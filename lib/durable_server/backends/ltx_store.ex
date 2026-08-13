defmodule DurableServer.Backends.LTXStore do
  @moduledoc """
  Storage backend that persists each key as a log of LTX segments instead of
  one full-state object, so a sync writes only the pages of the encoded state
  that changed — the Litestream persistence model applied to
  DurableServer state.

  Configure it with a wrapped backend that provides transport and CAS:

      {DurableServer.Backends.LTXStore,
       backend: {DurableServer.Backends.ObjectStore, object_store_opts}}

  ## Layout

  The object at the key itself is the **head**: a small marker map carrying
  the current transaction id, the rolling checksum, and the segment manifest.
  All ownership semantics — etag CAS, `try_claim`, conditional delete,
  ambiguous-conflict recovery — operate on the head exactly as they would on
  a full-state object, so supervisor fencing logic is unchanged. Segments are
  immutable objects under `__ltx/<key>/`, named `<min>-<max>.<nonce>.ltx`;
  the random nonce means two concurrent writers can never overwrite each
  other's segment bytes, and the manifest decides which bytes are real.

  ## Write paths

    * **Inline** — states encoding to at most `:inline_threshold_pages` pages
      are embedded in the head as a single base64 LTX snapshot: one
      conditional PUT, the same cost as today's full-state write.
    * **Delta** — when the previous image for the key is cached (populated by
      any get or put) and the caller's etag matches it, only changed pages
      are written as one delta segment, then the head is CAS'd. A write that
      changes nothing still appends a zero-page segment so the TXID chain
      stays contiguous. A lost head CAS leaves an orphan segment that no
      manifest references — harmless, and collected later.
    * **Snapshot** — on a cache miss, an etag mismatch, or when the manifest
      has reached `:max_segments`, the full image is written as one snapshot
      segment and the manifest resets to it, bounding restore cost.

  ## Reads

  A get fetches the head, then the manifest's segments, verifies every file
  checksum, the TXID chain, and the final rolling checksum against the head,
  and decodes the term — anything inconsistent fails closed. A restore that
  fails is retried once against a freshly (consistently) read head, closing
  the race with a concurrent snapshot reset that deleted a segment mid-read.

  An object written before this backend was introduced (no marker) is
  returned exactly as the wrapped backend decodes it, and converts to LTX
  form on its next write.

  ## Limits

  Subscriptions are not supported (heartbeat tracking is forced to `:poll`),
  and prefixes that cover the global `__ltx/` namespace must not be used for
  child keys. Segment bodies are base64-wrapped so they survive any wrapped
  backend's codec (including ObjectStore's JSON).
  """

  @behaviour DurableServer.StorageBackend

  alias DurableServer.{Meta, StorageBackend, StoredState}
  alias DurableServer.LTX
  alias DurableServer.LTX.{Decoder, Encoder, PagedTerm}

  @head_marker "__durable_server_ltx__"
  @segment_marker "__durable_server_ltx_segment__"
  @format_version 1
  @segment_root "__ltx/"

  @valid_opts [:backend, :page_size, :inline_threshold_pages, :max_segments]
  @default_page_size 4096
  @default_inline_threshold_pages 4
  @default_max_segments 16
  @update_max_retries 5

  defstruct backend: nil,
            page_size: @default_page_size,
            inline_threshold_pages: @default_inline_threshold_pages,
            max_segments: @default_max_segments,
            cache: nil

  @type state :: %__MODULE__{}

  @impl true
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

  def init_backend(opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    backend = Keyword.fetch!(opts, :backend)
    page_size = Keyword.get(opts, :page_size, @default_page_size)
    inline_threshold_pages = Keyword.get(opts, :inline_threshold_pages, @default_inline_threshold_pages)
    max_segments = Keyword.get(opts, :max_segments, @default_max_segments)

    cond do
      not match?(%StorageBackend{}, backend) ->
        raise ArgumentError,
              "ltx backend :backend must be an initialized DurableServer.StorageBackend"

      not LTX.valid_page_size?(page_size) ->
        raise ArgumentError,
              "ltx backend :page_size must be a power of two between 512 and 65536"

      not (is_integer(inline_threshold_pages) and inline_threshold_pages >= 0) ->
        raise ArgumentError, "ltx backend :inline_threshold_pages must be a non-negative integer"

      not (is_integer(max_segments) and max_segments >= 1) ->
        raise ArgumentError, "ltx backend :max_segments must be a positive integer"

      true ->
        {:ok,
         %{
           state: %__MODULE__{
             backend: backend,
             page_size: page_size,
             inline_threshold_pages: inline_threshold_pages,
             max_segments: max_segments,
             cache: :ets.new(__MODULE__, [:set, :public])
           },
           defaults: Map.put(StorageBackend.defaults(backend), :heartbeat_tracking_mode, :poll),
           features:
             backend
             |> StorageBackend.features()
             |> Map.put(:heartbeat_subscribe?, false)
         }}
    end
  end

  @impl true
  def ensure_ready(%{backend: backend}), do: StorageBackend.ensure_ready(backend)

  @impl true
  def get_object(%__MODULE__{} = state, key, opts) do
    case StorageBackend.get_object(state.backend, key, opts) do
      {:ok, %{body: %{@head_marker => _version} = head, etag: etag}} ->
        case restore(state, key, head, etag) do
          {:ok, object} ->
            {:ok, object}

          {:error, _reason} ->
            # A concurrent snapshot reset may have deleted a segment we were
            # reading. One retry against a freshly read head resolves it.
            retry_restore(state, key)
        end

      {:ok, %{body: _legacy} = object} ->
        {:ok, object}

      other ->
        other
    end
  end

  defp retry_restore(state, key) do
    case StorageBackend.get_object(state.backend, key, consistent: true) do
      {:ok, %{body: %{@head_marker => _version} = head, etag: etag}} ->
        restore(state, key, head, etag)

      {:ok, %{body: _legacy} = object} ->
        {:ok, object}

      other ->
        other
    end
  end

  @impl true
  def put_object(%__MODULE__{} = state, key, data, opts) do
    caller_etag = Keyword.get(opts, :etag)
    cached = cache_get(state, key)

    previous_image =
      case cached do
        %{etag: ^caller_etag, image: image} when not is_nil(caller_etag) -> image
        _other -> nil
      end

    with {:ok, image} <- PagedTerm.encode(data, state.page_size, previous_image) do
      cond do
        image.commit <= state.inline_threshold_pages ->
          put_inline(state, key, data, image, opts, cached)

        # Deltas may only extend a manifest that already starts with a
        # snapshot segment; an inline-origin entry (empty manifest) must
        # transition through the snapshot path.
        previous_image != nil and cached.manifest != [] and
            length(cached.manifest) < state.max_segments ->
          put_delta(state, key, data, image, opts, cached)

        true ->
          put_snapshot(state, key, data, image, opts, cached)
      end
    end
  end

  @impl true
  def try_claim(%__MODULE__{} = state, key, body) do
    # Claims always inline the body — regardless of size — so two racing
    # claimers never write segment objects that could collide. The next
    # ordinary put converts an oversized inline head to segment form.
    with {:ok, image} <- PagedTerm.encode(body, state.page_size),
         {:ok, head} <- build_inline_head(image, 1) do
      case StorageBackend.try_claim(state.backend, key, head) do
        {:ok, {:claimed, etag}} = result ->
          cache_put(state, key, etag, 1, image, [])
          result

        other ->
          other
      end
    end
  end

  @impl true
  def update_object(%__MODULE__{} = state, key, update_fn, opts) do
    do_update_object(state, key, update_fn, opts, 0)
  end

  defp do_update_object(state, key, update_fn, opts, attempt) do
    with {:ok, %{body: body, etag: etag}} <- get_object(state, key, consistent: true),
         {:ok, new_body} <- update_fn.(%{body: body, etag: etag}) do
      case put_object(state, key, new_body, Keyword.put(opts, :etag, etag)) do
        {:ok, object} ->
          {:ok, object}

        {:error, :conflict} when attempt < @update_max_retries ->
          Process.sleep(round(min(100 * :math.pow(2, attempt), 1_000)))
          do_update_object(state, key, update_fn, opts, attempt + 1)

        {:error, :conflict} ->
          {:error, :max_retries_exceeded}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def delete_object(%__MODULE__{} = state, key), do: delete_object(state, key, [])

  @impl true
  def delete_object(%__MODULE__{} = state, key, opts) do
    case StorageBackend.delete_object(state.backend, key, opts) do
      :ok ->
        cache_delete(state, key)
        delete_segments(state, key)
        :ok

      other ->
        other
    end
  end

  @impl true
  def list_all_objects_stream(%__MODULE__{} = state, prefix, opts) do
    error_handler = Keyword.get(opts, :error_handler, fn reason -> raise inspect(reason) end)

    state.backend
    |> StorageBackend.list_all_objects_stream(prefix, opts)
    |> Stream.transform(:ok, fn
      %{key: @segment_root <> _rest}, :ok ->
        {[], :ok}

      %{key: key, body: %{@head_marker => _version} = head, etag: etag} = object, :ok ->
        case restore(state, key, head, etag) do
          {:ok, %{body: body}} ->
            {[%{object | body: body}], :ok}

          {:error, reason} ->
            case error_handler.({:restore_failed, key, reason}) do
              :halt -> {:halt, :ok}
              _other -> {[], :ok}
            end
        end

      object, :ok ->
        {[object], :ok}
    end)
  end

  @impl true
  def encode(%__MODULE__{} = state, data) do
    with {:ok, image} <- PagedTerm.encode(data, state.page_size) do
      build_inline_head(image, 1)
    end
  end

  @impl true
  def decode(%__MODULE__{} = state, data) do
    case data do
      %{@head_marker => _version, "inline" => inline} when is_binary(inline) ->
        with {:ok, image} <- decode_inline(inline, state.page_size),
             {:ok, term} <- PagedTerm.decode(image) do
          {:ok, term}
        end

      %{@head_marker => _version} ->
        {:error, :head_requires_segment_restore}

      _other ->
        StorageBackend.decode(state.backend, data)
    end
  end

  # -- Write paths ----------------------------------------------------------

  defp put_inline(state, key, data, image, opts, cached) do
    txid = next_txid(state, key, opts, cached)

    with {:ok, txid} <- txid,
         {:ok, head} <- build_inline_head(image, txid) do
      case StorageBackend.put_object(state.backend, key, head, opts) do
        {:ok, %{etag: etag}} ->
          cache_put(state, key, etag, txid, image, [])
          release_segments(state, key, manifest_names(cached))
          {:ok, %{body: data, etag: etag}}

        {:error, :conflict} ->
          resolve_ambiguous_put(state, key, data, opts)

        other ->
          other
      end
    end
  end

  defp put_delta(state, key, data, image, opts, cached) do
    txid = cached.txid + 1
    changed = PagedTerm.changed_pages(cached.image.page_checksums, image)

    header = %LTX.Header{
      page_size: state.page_size,
      commit: image.commit,
      min_txid: txid,
      max_txid: txid,
      timestamp: System.system_time(:millisecond),
      pre_apply_checksum: cached.image.checksum
    }

    entry_name = segment_name(txid, txid)

    with {:ok, segment} <- encode_segment(header, changed, image.checksum),
         {:ok, _object} <-
           StorageBackend.put_object(state.backend, segment_key(key, entry_name), segment, []) do
      manifest = cached.manifest ++ [%{"name" => entry_name, "min" => txid, "max" => txid}]
      head = build_segment_head(image, txid, manifest)

      case StorageBackend.put_object(state.backend, key, head, opts) do
        {:ok, %{etag: etag}} ->
          cache_put(state, key, etag, txid, image, manifest)
          {:ok, %{body: data, etag: etag}}

        {:error, :conflict} ->
          # The delta segment is now an orphan no manifest references; the
          # sweeper collects it.
          cache_delete(state, key)
          resolve_ambiguous_put(state, key, data, opts)

        other ->
          other
      end
    end
  end

  defp put_snapshot(state, key, data, image, opts, cached) do
    with {:ok, txid} <- next_txid(state, key, opts, cached) do
      header = %LTX.Header{
        page_size: state.page_size,
        commit: image.commit,
        min_txid: 1,
        max_txid: txid,
        timestamp: System.system_time(:millisecond)
      }

      entry_name = segment_name(1, txid)
      pages = Enum.sort(image.pages)

      with {:ok, segment} <- encode_segment(header, pages, image.checksum),
           {:ok, _object} <-
             StorageBackend.put_object(state.backend, segment_key(key, entry_name), segment, []) do
        manifest = [%{"name" => entry_name, "min" => 1, "max" => txid}]
        head = build_segment_head(image, txid, manifest)

        case StorageBackend.put_object(state.backend, key, head, opts) do
          {:ok, %{etag: etag}} ->
            cache_put(state, key, etag, txid, image, manifest)
            release_segments(state, key, manifest_names(cached) -- [entry_name])
            {:ok, %{body: data, etag: etag}}

          {:error, :conflict} ->
            cache_delete(state, key)
            resolve_ambiguous_put(state, key, data, opts)

          other ->
            other
        end
      end
    end
  end

  # The next TXID comes from the cache when we hold any knowledge of the key,
  # otherwise from the stored head; a key with no head starts at 1.
  defp next_txid(_state, _key, _opts, %{txid: txid}), do: {:ok, txid + 1}

  defp next_txid(state, key, _opts, nil) do
    case StorageBackend.get_object(state.backend, key, consistent: true) do
      {:ok, %{body: %{@head_marker => _version} = head}} ->
        case head do
          %{"txid" => txid} when is_integer(txid) and txid >= 1 -> {:ok, txid + 1}
          _other -> {:error, :invalid_ltx_head}
        end

      {:ok, %{body: _legacy}} ->
        {:ok, 1}

      {:error, :not_found} ->
        {:ok, 1}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # -- Restore --------------------------------------------------------------

  defp restore(state, key, head, etag) do
    with {:ok, image, txid, manifest} <- restore_image(state, key, head),
         {:ok, term} <- PagedTerm.decode(image) do
      cache_put(state, key, etag, txid, image, manifest)
      {:ok, %{body: term, etag: etag}}
    end
  end

  defp restore_image(state, _key, %{
         @head_marker => @format_version,
         "txid" => txid,
         "inline" => inline
       })
       when is_binary(inline) and is_integer(txid) do
    with {:ok, image} <- decode_inline(inline, state.page_size) do
      {:ok, image, txid, []}
    end
  end

  defp restore_image(state, key, %{
         @head_marker => @format_version,
         "txid" => txid,
         "post_apply_checksum" => checksum_hex,
         "commit" => commit,
         "segments" => manifest
       })
       when is_integer(txid) and is_list(manifest) and manifest != [] do
    with {:ok, expected_checksum} <- parse_checksum(checksum_hex),
         {:ok, image} <- apply_manifest(state, key, manifest),
         :ok <- verify_restored(image, expected_checksum, commit, txid) do
      {:ok, image, txid, manifest}
    end
  end

  defp restore_image(_state, _key, %{@head_marker => @format_version}),
    do: {:error, :invalid_ltx_head}

  defp restore_image(_state, _key, %{@head_marker => version}),
    do: {:error, {:unsupported_ltx_head_version, version}}

  defp apply_manifest(state, key, manifest) do
    manifest
    |> Enum.sort_by(&Map.get(&1, "min"))
    |> Enum.reduce_while({:ok, nil, nil}, fn entry, {:ok, image, prev} ->
      with %{"name" => name} when is_binary(name) <- entry,
           {:ok, %{body: body}} <-
             StorageBackend.get_object(state.backend, segment_key(key, name), []),
           {:ok, binary} <- unwrap_segment(body),
           {:ok, decoded} <- Decoder.decode(binary),
           :ok <- verify_chain(prev, decoded, image),
           {:ok, image} <-
             PagedTerm.apply_pages(
               image,
               decoded.header.commit,
               decoded.pages,
               state.page_size
             ) do
        {:cont, {:ok, image, decoded}}
      else
        {:error, reason} -> {:halt, {:error, {:segment_restore_failed, entry["name"], reason}}}
        other -> {:halt, {:error, {:segment_restore_failed, entry["name"], other}}}
      end
    end)
    |> case do
      {:ok, nil, _prev} -> {:error, :empty_manifest}
      {:ok, image, _prev} -> {:ok, image}
      {:error, _reason} = error -> error
    end
  end

  # The first segment must be a snapshot; each later segment must be exactly
  # TXID-adjacent and agree across the checksum chain.
  defp verify_chain(nil, decoded, _image) do
    if LTX.snapshot?(decoded.header), do: :ok, else: {:error, :first_segment_not_snapshot}
  end

  defp verify_chain(prev, decoded, image) do
    cond do
      decoded.header.min_txid != prev.header.max_txid + 1 ->
        {:error, :txid_chain_broken}

      decoded.header.pre_apply_checksum != image.checksum ->
        {:error, :checksum_chain_broken}

      true ->
        :ok
    end
  end

  defp verify_restored(image, expected_checksum, commit, _txid) do
    cond do
      image.commit != commit -> {:error, :restored_commit_mismatch}
      image.checksum != expected_checksum -> {:error, :restored_checksum_mismatch}
      true -> :ok
    end
  end

  # -- Head/segment encoding -------------------------------------------------

  defp build_inline_head(image, txid) do
    header = %LTX.Header{
      page_size: image.page_size,
      commit: image.commit,
      min_txid: 1,
      max_txid: txid,
      timestamp: System.system_time(:millisecond)
    }

    with {:ok, enc} <- Encoder.new(header),
         {:ok, enc} <- encode_all_pages(enc, Enum.sort(image.pages)),
         {:ok, iodata} <- Encoder.finish(enc, image.checksum) do
      {:ok,
       %{
         @head_marker => @format_version,
         "txid" => txid,
         "post_apply_checksum" => LTX.format_txid(image.checksum),
         "commit" => image.commit,
         "page_size" => image.page_size,
         "segments" => [],
         "inline" => Base.encode64(IO.iodata_to_binary(iodata))
       }}
    end
  end

  defp build_segment_head(image, txid, manifest) do
    %{
      @head_marker => @format_version,
      "txid" => txid,
      "post_apply_checksum" => LTX.format_txid(image.checksum),
      "commit" => image.commit,
      "page_size" => image.page_size,
      "segments" => manifest,
      "inline" => nil
    }
  end

  defp encode_segment(header, pages, post_apply_checksum) do
    with {:ok, enc} <- Encoder.new(header),
         {:ok, enc} <- encode_all_pages(enc, pages),
         {:ok, iodata} <- Encoder.finish(enc, post_apply_checksum) do
      {:ok,
       %{
         @segment_marker => @format_version,
         "data" => Base.encode64(IO.iodata_to_binary(iodata))
       }}
    end
  end

  defp encode_all_pages(enc, pages) do
    Enum.reduce_while(pages, {:ok, enc}, fn {pgno, data}, {:ok, enc} ->
      case Encoder.encode_page(enc, pgno, data) do
        {:ok, enc} -> {:cont, {:ok, enc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp decode_inline(inline, page_size) do
    with {:ok, binary} <- decode_base64(inline),
         {:ok, decoded} <- Decoder.decode(binary),
         true <- LTX.snapshot?(decoded.header) or {:error, :inline_not_snapshot},
         {:ok, image} <-
           PagedTerm.apply_pages(nil, decoded.header.commit, decoded.pages, page_size) do
      if image.checksum == decoded.trailer.post_apply_checksum do
        {:ok, image}
      else
        {:error, :inline_checksum_mismatch}
      end
    end
  end

  defp unwrap_segment(%{@segment_marker => @format_version, "data" => data})
       when is_binary(data),
       do: decode_base64(data)

  defp unwrap_segment(_body), do: {:error, :invalid_segment_body}

  defp decode_base64(encoded) do
    case Base.decode64(encoded) do
      {:ok, binary} -> {:ok, binary}
      :error -> {:error, :invalid_base64}
    end
  end

  defp parse_checksum(hex) when is_binary(hex) do
    case LTX.parse_txid(hex) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :invalid_head_checksum}
    end
  end

  defp parse_checksum(_other), do: {:error, :invalid_head_checksum}

  defp segment_key(key, name), do: @segment_root <> key <> "/" <> name

  defp segment_name(min_txid, max_txid) do
    nonce = Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
    LTX.format_txid(min_txid) <> "-" <> LTX.format_txid(max_txid) <> "." <> nonce <> ".ltx"
  end

  defp manifest_names(%{manifest: manifest}), do: Enum.map(manifest, & &1["name"])
  defp manifest_names(_cached), do: []

  # Best-effort removal of segments a new head no longer references
  # (retention :none). Failures are ignored; the sweeper collects leftovers.
  defp release_segments(_state, _key, []), do: :ok

  defp release_segments(state, key, names) do
    Enum.each(names, fn name ->
      _ = StorageBackend.delete_object(state.backend, segment_key(key, name))
    end)
  end

  defp delete_segments(state, key) do
    state.backend
    |> StorageBackend.list_all_objects_stream(@segment_root <> key <> "/",
      error_handler: fn _reason -> :halt end
    )
    |> Enum.each(fn %{key: segment_key} ->
      _ = StorageBackend.delete_object(state.backend, segment_key)
    end)
  rescue
    _error -> :ok
  end

  # -- Ambiguous conditional-write recovery ----------------------------------

  # Mirrors ObjectStore/EncryptedStore: a conditional head PUT can commit
  # while its response is lost. Adopt storage only when the persisted state
  # restores to exactly the attempted term and the same boot owner.
  defp resolve_ambiguous_put(state, key, %StoredState{meta: %Meta{} = attempted_meta} = data, opts) do
    if Keyword.has_key?(opts, :etag) do
      case retry_restore(state, key) do
        {:ok, %{body: %StoredState{meta: %Meta{} = persisted_meta} = persisted, etag: etag}} ->
          if same_boot_owner?(attempted_meta, persisted_meta) and persisted == data do
            {:ok, %{body: data, etag: etag}}
          else
            {:error, :conflict}
          end

        _other ->
          {:error, :conflict}
      end
    else
      {:error, :conflict}
    end
  end

  defp resolve_ambiguous_put(_state, _key, _data, _opts), do: {:error, :conflict}

  defp same_boot_owner?(
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str},
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str}
       )
       when is_pid(pid) and not is_nil(node_ref) and is_binary(node_str),
       do: true

  defp same_boot_owner?(%Meta{}, %Meta{}), do: false

  # -- Cache -----------------------------------------------------------------

  defp cache_get(%{cache: cache}, key) do
    case :ets.lookup(cache, key) do
      [{^key, entry}] -> entry
      [] -> nil
    end
  end

  defp cache_put(%{cache: cache}, key, etag, txid, image, manifest) do
    :ets.insert(cache, {key, %{etag: etag, txid: txid, image: image, manifest: manifest}})
    :ok
  end

  defp cache_delete(%{cache: cache}, key) do
    :ets.delete(cache, key)
    :ok
  end
end
