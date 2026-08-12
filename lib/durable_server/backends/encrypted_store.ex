defmodule DurableServer.Backends.EncryptedStore do
  @moduledoc """
  Storage backend wrapper that encrypts values before delegating persistence.

  This wrapper can front any configured DurableServer backend. Existing
  plaintext objects remain readable and are encrypted the next time they are
  written.

  Configure it with a nested backend specification, one or more recipient
  public keys, and the private key for this deployment:

      {DurableServer.Backends.EncryptedStore,
       backend: {DurableServer.Backends.ObjectStore, object_store_opts},
       recipient_public_keys: [recipient_public_key],
       decryption_key: recipient_private_key}

  Recipient public keys can include both old and new keys during rotation. A
  write creates one wrapped content key per recipient, while any matching
  private key can read the value.

  Existing plaintext objects are readable by default (`plaintext_compat:
  :permissive`), which preserves the migration promise above but means
  anything with write access to the wrapped backend can inject unencrypted
  data with no cryptographic check. Every plaintext read is logged so the
  exposure is observable. Pass `plaintext_compat: :strict` once a deployment
  no longer needs to read pre-encryption data to reject unmarked objects
  outright.

  Encryption seals a canonical, backend-agnostic encoding of the exact term a
  caller stores: atom keys, pids, and references all survive unchanged,
  regardless of which backend this wrapper is configured to front. Only the
  sealed envelope (ciphertext, wrapped per-recipient keys, nonce, and tag) is
  passed to the wrapped backend's own codec for storage; the plaintext it
  protects is never interpreted by that codec. The one case this does not
  cover is an object written before encryption was enabled: that legacy
  plaintext is read through the wrapped backend's own codec exactly as if
  this wrapper were absent, and gains full fidelity the next time it's
  written.
  """

  @behaviour DurableServer.StorageBackend

  alias DurableServer.{Encryption, Meta, StorageBackend, StoredState}

  require Logger

  @valid_opts [:backend, :recipient_public_keys, :decryption_key, :plaintext_compat]
  @plaintext_compat_modes [:permissive, :strict]
  @subscribe_ready_timeout_ms 5_000
  @unsubscribe_ready_timeout_ms @subscribe_ready_timeout_ms * 2
  @max_decoded_payload_bytes 16_777_216

  # Tags the inner plaintext shape so it can never be confused with a wrapped
  # backend's own encoding (the pre-fix shape) or with a legacy plaintext
  # object's raw bytes. Distinct from Encryption's own envelope/marker
  # versioning, which this does not touch.
  @canonical_payload_format :dse_payload_v1

  @derive {Inspect, only: [:backend, :recipient_public_keys, :plaintext_compat]}
  defstruct backend: nil,
            recipient_public_keys: nil,
            decryption_key: nil,
            plaintext_compat: :permissive

  @type state :: %__MODULE__{
          backend: struct(),
          recipient_public_keys: [binary()],
          decryption_key: binary(),
          plaintext_compat: :permissive | :strict
        }

  @impl true
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

  def init_backend(opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    backend = Keyword.fetch!(opts, :backend)
    recipient_public_keys = Keyword.fetch!(opts, :recipient_public_keys)
    decryption_key = Keyword.fetch!(opts, :decryption_key)
    plaintext_compat = Keyword.get(opts, :plaintext_compat, :permissive)

    cond do
      not match?(%StorageBackend{}, backend) ->
        raise ArgumentError,
              "encrypted backend :backend must be an initialized DurableServer.StorageBackend"

      plaintext_compat not in @plaintext_compat_modes ->
        raise ArgumentError,
              "encrypted backend :plaintext_compat must be one of #{inspect(@plaintext_compat_modes)}"

      not Encryption.supported?() ->
        {:error, :encryption_not_supported}

      true ->
        with {:ok, probe} <- Encryption.seal(<<>>, recipient_public_keys, <<>>),
             {:ok, <<>>} <- Encryption.open(probe, decryption_key, <<>>) do
          {:ok,
           %{
             state: %__MODULE__{
               backend: backend,
               recipient_public_keys: recipient_public_keys,
               decryption_key: decryption_key,
               plaintext_compat: plaintext_compat
             },
             defaults: StorageBackend.defaults(backend),
             features: StorageBackend.features(backend)
           }}
        end
    end
  end

  @impl true
  def ensure_ready(%{backend: backend}), do: StorageBackend.ensure_ready(backend)

  @impl true
  def get_object(%{} = state, key, opts) do
    case StorageBackend.get_object(state.backend, key, opts) do
      {:ok, %{body: body} = object} ->
        with {:ok, decoded} <- decode_stored_body(state, body, key) do
          {:ok, %{object | body: decoded}}
        end

      other ->
        other
    end
  end

  @impl true
  def list_all_objects_stream(%{} = state, prefix, opts) do
    error_handler = Keyword.get(opts, :error_handler, fn reason -> raise inspect(reason) end)

    state.backend
    |> StorageBackend.list_all_objects_stream(prefix, opts)
    |> Stream.transform(:ok, fn
      %{key: key, body: body} = object, :ok ->
        case decode_stored_body(state, body, key) do
          {:ok, decoded} ->
            {[%{object | body: decoded}], :ok}

          {:error, reason} ->
            case error_handler.({:decode_failed, key, reason}) do
              :halt -> {:halt, :ok}
              _other -> {[], :ok}
            end
        end

      object, :ok ->
        {[object], :ok}
    end)
  end

  @impl true
  def put_object(%{} = state, key, data, opts) do
    with {:ok, encrypted} <- encode_body(state, data, key) do
      case StorageBackend.put_object(state.backend, key, encrypted, opts) do
        {:ok, object} ->
          {:ok, %{object | body: data}}

        {:error, :conflict} ->
          resolve_ambiguous_conditional_put(state, key, data, opts)

        other ->
          other
      end
    end
  end

  @impl true
  def delete_object(%{} = state, key), do: StorageBackend.delete_object(state.backend, key)

  @impl true
  def delete_object(%{} = state, key, opts),
    do: StorageBackend.delete_object(state.backend, key, opts)

  @impl true
  def try_claim(%{} = state, key, body) do
    with {:ok, encrypted} <- encode_body(state, body, key) do
      StorageBackend.try_claim(state.backend, key, encrypted)
    end
  end

  @impl true
  def update_object(%{} = state, key, update_fn, opts) do
    result =
      StorageBackend.update_object(
        state.backend,
        key,
        fn %{body: encrypted, etag: etag} ->
          with {:ok, body} <- decode_stored_body(state, encrypted, key),
               {:ok, new_body} <- update_fn.(%{body: body, etag: etag}),
               {:ok, new_encrypted} <- encode_body(state, new_body, key) do
            {:ok, new_encrypted}
          end
        end,
        opts
      )

    case result do
      {:ok, %{body: encrypted} = object} ->
        with {:ok, body} <- decode_stored_body(state, encrypted, key) do
          {:ok, %{object | body: body}}
        end

      other ->
        other
    end
  end

  @impl true
  def encode(%{} = state, data), do: encode_body(state, data, <<>>)

  @impl true
  def decode(%{} = state, data) do
    if Encryption.encrypted?(data) do
      decode_encrypted_body(state, data, <<>>)
    else
      StorageBackend.decode(state.backend, data)
    end
  end

  @impl true
  def subscribe(%{} = state, subscriber, prefix, opts) do
    caller = self()

    relay_pid =
      spawn(fn ->
        subscription_relay_init(caller, subscriber, state, prefix, opts)
      end)

    monitor_ref = Process.monitor(relay_pid)

    receive do
      {:encrypted_store_subscribed, ^relay_pid, {:ok, inner_ref}} ->
        Process.demonitor(monitor_ref, [:flush])
        {:ok, {relay_pid, inner_ref}}

      {:encrypted_store_subscribed, ^relay_pid, {:error, reason}} ->
        Process.demonitor(monitor_ref, [:flush])
        {:error, reason}

      {:DOWN, ^monitor_ref, :process, ^relay_pid, reason} ->
        {:error, {:subscription_exit, reason}}
    after
      @subscribe_ready_timeout_ms ->
        abandon_subscribe(state, relay_pid, monitor_ref)
    end
  end

  @impl true
  def unsubscribe(%{} = _state, {relay_pid, inner_ref}) when is_pid(relay_pid) do
    if Process.alive?(relay_pid) do
      ref = make_ref()
      send(relay_pid, {:encrypted_store_unsubscribe, self(), ref, inner_ref})

      receive do
        {:encrypted_store_unsubscribed, ^ref} -> :ok
      after
        @unsubscribe_ready_timeout_ms -> {:error, :unsubscribe_timeout}
      end
    else
      :ok
    end
  end

  def unsubscribe(%{} = _state, _subscription_ref), do: :ok

  # The relay can register the inner subscription and send its ready message
  # in the same instant this receive's timeout fires, racing the caller into
  # giving up right before the confirmation lands. A final non-blocking drain
  # catches that message if it is already in the mailbox, so the inner
  # subscription can be torn down here instead of leaking because the
  # untrappable :kill below never gives the relay a chance to do it itself.
  defp abandon_subscribe(state, relay_pid, monitor_ref) do
    receive do
      {:encrypted_store_subscribed, ^relay_pid, {:ok, inner_ref}} ->
        _ = StorageBackend.unsubscribe(state.backend, inner_ref)

      {:encrypted_store_subscribed, ^relay_pid, {:error, _reason}} ->
        :ok
    after
      0 -> :ok
    end

    Process.demonitor(monitor_ref, [:flush])
    Process.exit(relay_pid, :kill)
    {:error, :subscribe_timeout}
  end

  defp encode_body(state, data, context) do
    with {:ok, plaintext} <- serialize(data) do
      Encryption.seal(plaintext, state.recipient_public_keys, context)
    end
  end

  # Encryption.encrypted?/1 only tests for marker presence, so it must gate
  # exactly one branch here: any body carrying the marker goes to
  # decode_encrypted_body/3, which performs the cryptographic check and
  # returns an error for a malformed envelope. Nothing marked as encrypted
  # may reach decode_plaintext_body/3.
  defp decode_stored_body(state, body, context) do
    if Encryption.encrypted?(body) do
      decode_encrypted_body(state, body, context)
    else
      decode_plaintext_body(state, body, context)
    end
  end

  # No :telemetry.execute/3 call accompanies these warnings: :telemetry is
  # pulled in only transitively (via req/finch), not declared as a dependency
  # of this library in mix.exs, so nothing here can rely on it being present.
  defp decode_plaintext_body(%{plaintext_compat: :strict}, _body, context) do
    Logger.warning(
      "DurableServer.Backends.EncryptedStore rejected an unmarked plaintext object at #{inspect(context)} (plaintext_compat: :strict)"
    )

    {:error, :plaintext_rejected}
  end

  defp decode_plaintext_body(%{plaintext_compat: :permissive}, body, context) do
    Logger.warning(
      "DurableServer.Backends.EncryptedStore read an unmarked plaintext object at #{inspect(context)}; it will be encrypted on the next write"
    )

    {:ok, body}
  end

  defp decode_encrypted_body(state, envelope, context) do
    with {:ok, plaintext} <- Encryption.open(envelope, state.decryption_key, context) do
      deserialize(plaintext)
    end
  end

  # The wrapped backend's own codec never sees this plaintext (see moduledoc):
  # the tag exists only to disambiguate this shape from the pre-fix one, which
  # ran the caller's term through the wrapped backend's encode/1 first, and
  # from a legacy plaintext object's own encoding.
  defp serialize(term) do
    binary = :erlang.term_to_binary({@canonical_payload_format, term})

    with :ok <- validate_payload_size(binary) do
      {:ok, binary}
    end
  rescue
    error in ArgumentError -> {:error, {:serialization_failed, error}}
  end

  # Persisted values carry live pids/refs whose ETF embeds the originating
  # node atom (a %Meta{} routed through EKVStore is one example), so `:safe`
  # would refuse to intern that atom on a freshly booted node and make
  # otherwise-valid durable state unreadable after a deploy -- see meta.ex.
  # This layer never emits compressed ETF (serialize/1 above), so the
  # compression tag is rejected outright rather than decompressed: a small
  # compressed blob can expand by two to three orders of magnitude, and
  # nothing here needs that tradeoff. The size bound below is checked before
  # binary_to_term runs, not after.
  defp deserialize(binary) do
    with :ok <- validate_decodable_term(binary) do
      case :erlang.binary_to_term(binary) do
        {@canonical_payload_format, term} -> {:ok, term}
        _other -> {:error, :unrecognized_payload_format}
      end
    end
  rescue
    error in ArgumentError -> {:error, {:invalid_encrypted_payload, error}}
  end

  defp validate_decodable_term(<<131, 80, _uncompressed_size::32, _rest::binary>>),
    do: {:error, :compressed_payload_rejected}

  defp validate_decodable_term(binary), do: validate_payload_size(binary)

  # Shared by serialize/1 and validate_decodable_term/1 so the write-side and
  # read-side bounds cannot drift apart: an oversized write fails fast here
  # instead of succeeding and leaving an object only deserialize/1 will ever
  # refuse to return.
  defp validate_payload_size(binary) when byte_size(binary) <= @max_decoded_payload_bytes,
    do: :ok

  defp validate_payload_size(binary),
    do: {:error, {:payload_too_large, byte_size(binary)}}

  defp resolve_ambiguous_conditional_put(
         state,
         key,
         %StoredState{meta: %Meta{} = attempted_meta} = attempted,
         opts
       ) do
    if Keyword.has_key?(opts, :etag) do
      case StorageBackend.get_object(state.backend, key, consistent: true) do
        {:ok, %{body: persisted_body, etag: etag}} ->
          with {:ok, persisted} <- decode_stored_body(state, persisted_body, key),
               %StoredState{meta: %Meta{} = persisted_meta} <- persisted,
               true <- same_boot_owner?(attempted_meta, persisted_meta),
               {:ok, attempted_encoded} <- serialize(attempted),
               {:ok, persisted_encoded} <- serialize(persisted),
               true <- attempted_encoded == persisted_encoded do
            {:ok, %{body: attempted, etag: etag}}
          else
            _other -> {:error, :conflict}
          end

        _other ->
          {:error, :conflict}
      end
    else
      {:error, :conflict}
    end
  end

  defp resolve_ambiguous_conditional_put(_state, _key, _data, _opts),
    do: {:error, :conflict}

  defp same_boot_owner?(
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str},
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str}
       )
       when is_pid(pid) and not is_nil(node_ref) and is_binary(node_str),
       do: true

  defp same_boot_owner?(%Meta{}, %Meta{}), do: false

  defp subscription_relay_init(caller, subscriber, state, prefix, opts) do
    monitor_ref = Process.monitor(subscriber)

    case StorageBackend.subscribe(state.backend, self(), prefix, opts) do
      {:ok, inner_ref} ->
        send(caller, {:encrypted_store_subscribed, self(), {:ok, inner_ref}})
        subscription_relay_loop(subscriber, state, inner_ref, monitor_ref)

      {:error, reason} ->
        send(caller, {:encrypted_store_subscribed, self(), {:error, reason}})
    end
  end

  defp subscription_relay_loop(subscriber, state, inner_ref, monitor_ref) do
    receive do
      {:durable_server_storage_events, events} when is_list(events) ->
        decoded_events = Enum.flat_map(events, &decode_event(state, &1))

        if decoded_events != [] do
          send(subscriber, {:durable_server_storage_events, decoded_events})
        end

        subscription_relay_loop(subscriber, state, inner_ref, monitor_ref)

      {:encrypted_store_unsubscribe, caller, ref, ^inner_ref} ->
        _ = StorageBackend.unsubscribe(state.backend, inner_ref)
        send(caller, {:encrypted_store_unsubscribed, ref})

      {:DOWN, ^monitor_ref, :process, ^subscriber, _reason} ->
        _ = StorageBackend.unsubscribe(state.backend, inner_ref)
        :ok

      _other ->
        subscription_relay_loop(subscriber, state, inner_ref, monitor_ref)
    end
  end

  defp decode_event(state, %{key: key, value: value} = event) when is_binary(key) do
    case decode_stored_body(state, value, key) do
      {:ok, decoded} -> [%{event | value: decoded}]
      {:error, _reason} -> []
    end
  end

  defp decode_event(_state, event), do: [event]
end
