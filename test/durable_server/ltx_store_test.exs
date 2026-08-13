defmodule DurableServer.LTXStoreTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.{LTXStore, ObjectStore}
  alias DurableServer.{Meta, StorageBackend, StoredState}

  @head_marker "__durable_server_ltx__"

  defmodule MemoryBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(table) do
      {:ok,
       %{
         state: %{table: table},
         defaults: %{heartbeat_tracking_mode: :subscribe},
         features: %{heartbeat_subscribe?: true, conditional_delete?: true}
       }}
    end

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(%{table: table}, key, _opts) do
      case :ets.lookup(table, {:object, key}) do
        [{{:object, ^key}, body, etag}] -> {:ok, %{body: body, etag: etag}}
        [] -> {:error, :not_found}
      end
    end

    @impl true
    def list_all_objects_stream(%{table: table}, prefix, opts) do
      include_objects = Keyword.get(opts, :include_objects, false)

      table
      |> :ets.tab2list()
      |> Enum.sort()
      |> Stream.flat_map(fn
        {{:object, key}, body, etag} ->
          if String.starts_with?(key, prefix) do
            object = %{key: key, etag: etag}
            [if(include_objects, do: Map.put(object, :body, body), else: object)]
          else
            []
          end

        _other ->
          []
      end)
    end

    @impl true
    def put_object(%{table: table}, key, body, opts) do
      mode = lookup(table, {:mode, key}, :normal)
      current = lookup(table, {:object, key}, nil)

      with :ok <- check_etag(current, Keyword.fetch(opts, :etag)) do
        etag = next_etag(current)

        case mode do
          :commit_then_conflict ->
            :ets.insert(table, {{:object, key}, body, etag})
            {:error, :conflict}

          :conflict ->
            {:error, :conflict}

          :normal ->
            :ets.insert(table, {{:object, key}, body, etag})
            {:ok, %{body: body, etag: etag}}
        end
      end
    end

    @impl true
    def delete_object(%{table: table}, key), do: delete_from_table(table, key)

    @impl true
    def delete_object(%{table: table}, key, _opts), do: delete_from_table(table, key)

    @impl true
    def try_claim(%{table: table}, key, body) do
      case :ets.insert_new(table, {{:object, key}, body, "1"}) do
        true -> {:ok, {:claimed, "1"}}
        false -> {:error, :already_claimed}
      end
    end

    @impl true
    def update_object(%{} = state, key, update_fn, opts) do
      with {:ok, %{body: body, etag: etag}} <- get_object(state, key, consistent: true),
           {:ok, new_body} <- update_fn.(%{body: body, etag: etag}) do
        put_object(state, key, new_body, Keyword.put(opts, :etag, etag))
      end
    end

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    defp check_etag(nil, :error), do: :ok
    defp check_etag(nil, {:ok, _etag}), do: {:error, :conflict}
    defp check_etag({_body, _etag}, :error), do: :ok
    defp check_etag({_body, etag}, {:ok, etag}), do: :ok
    defp check_etag(_current, _etag), do: {:error, :conflict}

    defp next_etag(nil), do: "1"
    defp next_etag({_body, etag}), do: Integer.to_string(String.to_integer(etag) + 1)

    defp delete_from_table(table, key) do
      case :ets.take(table, {:object, key}) do
        [] -> {:error, :not_found}
        [_object] -> :ok
      end
    end

    defp lookup(table, key, default) do
      case :ets.lookup(table, key) do
        [{^key, value}] -> value
        [{{:object, _key}, body, etag}] -> {body, etag}
        [] -> default
      end
    end
  end

  # Small states inline (<= 4 pages of 512); big_state forces segments.
  defp big_state(tag) do
    %{tag: tag, blob: :binary.copy(<<rem(tag, 251)>>, 8_000), list: Enum.to_list(1..100)}
  end

  defp backend!(adapter, opts) do
    {:ok, backend} = StorageBackend.init_backend(adapter, opts)
    backend
  end

  defp ltx_store!(table, opts \\ []) do
    backend!(
      LTXStore,
      Keyword.merge(
        [backend: backend!(MemoryBackend, table), page_size: 512, inline_threshold_pages: 4],
        opts
      )
    )
  end

  setup do
    table = :ets.new(:ltx_store_test, [:set, :public])
    {:ok, table: table, store: ltx_store!(table)}
  end

  # A second store instance over the same table has a cold cache, forcing
  # every read through the real restore path.
  defp cold_reader(context), do: ltx_store!(context.table)

  defp underlying_head(context, key) do
    [{{:object, ^key}, head, _etag}] = :ets.lookup(context.table, {:object, key})
    head
  end

  defp segment_keys(context, key) do
    context.table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{:object, object_key}, _body, _etag} ->
        if String.starts_with?(object_key, "__ltx/" <> key <> "/"), do: [object_key], else: []

      _other ->
        []
    end)
    |> Enum.sort()
  end

  test "small states round trip through an inline head", context do
    state = %{count: 1, pid: self(), ref: make_ref()}

    assert {:ok, %{body: ^state, etag: etag}} =
             StorageBackend.put_object(context.store, "server/1", state)

    head = underlying_head(context, "server/1")
    assert %{@head_marker => 1, "txid" => 1, "segments" => [], "inline" => inline} = head
    assert is_binary(inline)
    assert segment_keys(context, "server/1") == []

    assert {:ok, %{body: ^state, etag: ^etag}} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "large states write a snapshot segment", context do
    state = big_state(1)

    assert {:ok, %{body: ^state}} = StorageBackend.put_object(context.store, "server/1", state)

    head = underlying_head(context, "server/1")
    assert %{@head_marker => 1, "txid" => 1, "inline" => nil, "segments" => [entry]} = head
    assert %{"min" => 1, "max" => 1, "name" => name} = entry
    assert name =~ ~r/^0{15}1-0{15}1\.[0-9a-f]{8}\.ltx$/
    assert segment_keys(context, "server/1") == ["__ltx/server/1/" <> name]

    assert {:ok, %{body: ^state}} = StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "a CAS write after a warm read ships only a delta segment", context do
    state1 = big_state(1)
    state2 = %{state1 | list: state1.list ++ [101]}

    assert {:ok, %{etag: etag1}} = StorageBackend.put_object(context.store, "server/1", state1)

    assert {:ok, %{body: ^state2, etag: _etag2}} =
             StorageBackend.put_object(context.store, "server/1", state2, etag: etag1)

    head = underlying_head(context, "server/1")
    assert %{"txid" => 2, "segments" => [%{"min" => 1}, %{"min" => 2, "max" => 2}]} = head

    [snapshot_key, delta_key] =
      Enum.sort_by(segment_keys(context, "server/1"), &String.contains?(&1, "-0{15}2"))

    # The delta is materially smaller than the snapshot.
    [{_k1, snapshot_body, _e1}] = :ets.lookup(context.table, {:object, snapshot_key})
    [{_k2, delta_body, _e2}] = :ets.lookup(context.table, {:object, delta_key})
    assert byte_size(delta_body["data"]) < byte_size(snapshot_body["data"]) / 2

    assert {:ok, %{body: ^state2}} = StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "a rehomed owner deltas after its first read", context do
    state1 = big_state(1)
    state2 = %{state1 | tag: 2}

    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", state1)

    # A different node (cold cache) reads, then writes with the etag it saw.
    reader = cold_reader(context)
    assert {:ok, %{etag: etag}} = StorageBackend.get_object(reader, "server/1", [])
    assert {:ok, _object} = StorageBackend.put_object(reader, "server/1", state2, etag: etag)

    assert %{"txid" => 2, "segments" => [_snapshot, _delta]} = underlying_head(context, "server/1")
    assert {:ok, %{body: ^state2}} = StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "an unchanged CAS write appends a zero-page delta and keeps the chain contiguous",
       context do
    state = big_state(1)

    assert {:ok, %{etag: etag1}} = StorageBackend.put_object(context.store, "server/1", state)
    assert {:ok, %{etag: etag2}} = StorageBackend.put_object(context.store, "server/1", state, etag: etag1)
    assert etag2 != etag1

    assert %{"txid" => 2, "segments" => [_snapshot, _empty_delta]} =
             underlying_head(context, "server/1")

    assert {:ok, %{body: ^state}} = StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "the manifest resets to a snapshot at max_segments and releases old segments", context do
    store = ltx_store!(context.table, max_segments: 4)
    state = big_state(1)

    {:ok, %{etag: etag}} = StorageBackend.put_object(store, "server/1", state)

    final_etag =
      Enum.reduce(2..6, etag, fn tag, etag ->
        {:ok, %{etag: etag}} =
          StorageBackend.put_object(store, "server/1", %{state | tag: tag}, etag: etag)

        etag
      end)

    # Writes: snapshot(1), deltas(2..4), then the manifest is full — write 5
    # resets to a snapshot; write 6 deltas on top of it.
    assert %{"txid" => 6, "segments" => segments} = underlying_head(context, "server/1")
    assert [%{"min" => 1, "max" => 5}, %{"min" => 6, "max" => 6}] =
             Enum.map(segments, &Map.take(&1, ["min", "max"]))

    # Retention :none — only referenced segments remain on disk.
    assert length(segment_keys(context, "server/1")) == 2

    assert {:ok, %{body: %{tag: 6}, etag: ^final_etag}} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "shrinking below the inline threshold returns to an inline head", context do
    assert {:ok, %{etag: etag}} =
             StorageBackend.put_object(context.store, "server/1", big_state(1))

    assert segment_keys(context, "server/1") != []

    assert {:ok, _object} =
             StorageBackend.put_object(context.store, "server/1", %{small: true}, etag: etag)

    assert %{"inline" => inline, "segments" => []} = underlying_head(context, "server/1")
    assert is_binary(inline)
    assert segment_keys(context, "server/1") == []

    assert {:ok, %{body: %{small: true}}} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "stale etags conflict", context do
    assert {:ok, %{etag: etag1}} =
             StorageBackend.put_object(context.store, "server/1", big_state(1))

    assert {:ok, _object} =
             StorageBackend.put_object(context.store, "server/1", big_state(2), etag: etag1)

    assert {:error, :conflict} =
             StorageBackend.put_object(context.store, "server/1", big_state(3), etag: etag1)
  end

  test "try_claim inlines the body and loses cleanly to an existing claim", context do
    body = stored_state(%{count: 0})

    assert {:ok, {:claimed, etag}} = StorageBackend.try_claim(context.store, "server/1", body)
    assert {:error, :already_claimed} = StorageBackend.try_claim(context.store, "server/1", body)

    assert %{"inline" => inline} = underlying_head(context, "server/1")
    assert is_binary(inline)

    assert {:ok, %{body: ^body, etag: ^etag}} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])

    # An oversized claim body still inlines — no segment objects at claim time.
    big = stored_state(big_state(9))
    assert {:ok, {:claimed, _etag}} = StorageBackend.try_claim(context.store, "server/9", big)
    assert segment_keys(context, "server/9") == []

    assert {:ok, %{body: ^big}} = StorageBackend.get_object(cold_reader(context), "server/9", [])
  end

  test "preserves ambiguous conditional-write recovery on the head", context do
    body1 = stored_state(%{count: 1})

    assert {:ok, %{etag: etag1}} = StorageBackend.put_object(context.store, "server/1", body1)

    :ets.insert(context.table, {{:mode, "server/1"}, :commit_then_conflict})
    body2 = %{body1 | state: %{count: 2}}

    assert {:ok, %{body: ^body2}} =
             StorageBackend.put_object(context.store, "server/1", body2, etag: etag1)
  end

  test "legacy objects read through unchanged and convert on first write", context do
    legacy = %{"count" => 41}
    :ets.insert(context.table, {{:object, "server/legacy"}, legacy, "7"})

    assert {:ok, %{body: ^legacy, etag: "7"}} =
             StorageBackend.get_object(context.store, "server/legacy", [])

    assert {:ok, _object} =
             StorageBackend.put_object(context.store, "server/legacy", %{"count" => 42}, etag: "7")

    assert %{@head_marker => 1, "txid" => 1} = underlying_head(context, "server/legacy")

    assert {:ok, %{body: %{"count" => 42}}} =
             StorageBackend.get_object(cold_reader(context), "server/legacy", [])
  end

  test "a corrupted segment fails the read closed, never as :not_found", context do
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))

    [segment_key] = segment_keys(context, "server/1")
    [{key_tuple, body, etag}] = :ets.lookup(context.table, {:object, segment_key})
    corrupted = Map.update!(body, "data", fn data -> "AAAA" <> data end)
    :ets.insert(context.table, {key_tuple, corrupted, etag})

    assert {:error, {:segment_restore_failed, _name, _reason}} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "a head whose checksum disagrees with its segments fails closed", context do
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))

    head = underlying_head(context, "server/1")
    tampered = Map.put(head, "post_apply_checksum", "8000000000000bad")
    [{key_tuple, _body, etag}] = :ets.lookup(context.table, {:object, "server/1"})
    :ets.insert(context.table, {key_tuple, tampered, etag})

    assert {:error, :restored_checksum_mismatch} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "update_object round trips through get and CAS put", context do
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))

    assert {:ok, %{body: %{tag: 100}}} =
             StorageBackend.update_object(context.store, "server/1", fn %{body: body} ->
               {:ok, %{body | tag: 100}}
             end)

    assert {:ok, %{body: %{tag: 100}}} =
             StorageBackend.get_object(cold_reader(context), "server/1", [])
  end

  test "delete removes the head and its segments", context do
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))
    assert segment_keys(context, "server/1") != []

    assert :ok = StorageBackend.delete_object(context.store, "server/1")
    assert {:error, :not_found} = StorageBackend.get_object(context.store, "server/1", [])
    assert segment_keys(context, "server/1") == []
  end

  test "list_all_objects_stream hides segments and restores head bodies", context do
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/2", %{small: true})

    listed =
      context.store
      |> StorageBackend.list_all_objects_stream("server/", include_objects: true)
      |> Enum.to_list()

    assert [%{key: "server/1", body: %{tag: 1}}, %{key: "server/2", body: %{small: true}}] =
             Enum.sort_by(listed, & &1.key)

    # Listing everything still hides the segment namespace.
    all =
      context.store
      |> StorageBackend.list_all_objects_stream("", include_objects: true)
      |> Enum.map(& &1.key)

    refute Enum.any?(all, &String.starts_with?(&1, "__ltx/"))
  end

  test "encode/decode round trip terms with full fidelity", context do
    term = %{pid: self(), ref: make_ref(), atom: :value}

    assert {:ok, encoded} = StorageBackend.encode(context.store, term)
    assert %{@head_marker => 1} = encoded
    assert {:ok, ^term} = StorageBackend.decode(context.store, encoded)
  end

  test "round trips heads and segments through the ObjectStore JSON codec" do
    underlying = backend!(ObjectStore, DurableServer.TestHelper.test_object_store())

    {:ok, store} =
      StorageBackend.init_backend(LTXStore,
        backend: underlying,
        page_size: 512,
        inline_threshold_pages: 4
      )

    key = "ltx-store/#{System.unique_integer([:positive, :monotonic])}"
    state = big_state(1)

    assert {:ok, %{etag: etag}} = StorageBackend.put_object(store, key, state)
    assert {:ok, %{body: ^state}} = StorageBackend.get_object(store, key, [])

    # Delta through JSON as well.
    state2 = %{state | tag: 2}
    assert {:ok, _object} = StorageBackend.put_object(store, key, state2, etag: etag)

    {:ok, cold} =
      StorageBackend.init_backend(LTXStore,
        backend: underlying,
        page_size: 512,
        inline_threshold_pages: 4
      )

    assert {:ok, %{body: ^state2}} = StorageBackend.get_object(cold, key, [])
    assert :ok = StorageBackend.delete_object(store, key)
  end

  test "a DurableServer child persists and restores through LTX segments", context do
    supervisor_name = :"ltx_lifecycle_#{System.unique_integer([:positive, :monotonic])}"
    prefix = "ltx_lifecycle/#{System.unique_integer([:positive, :monotonic])}/"

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: context.store,
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    key = "counter-1"

    assert {:ok, {pid, _meta}} =
             DurableServer.Supervisor.ensure_started_child(
               supervisor_name,
               {DurableServer.TestCounterServer, key: key, initial_state: %{count: 0}}
             )

    assert GenServer.call(pid, :increment_and_sync) == 1
    assert GenServer.call(pid, :increment_and_sync) == 2

    # The child's state reached storage in LTX head form.
    storage_key = prefix <> key
    assert %{@head_marker => 1} = underlying_head(context, storage_key)

    # Kill without graceful shutdown, then restart: state must restore.
    Process.exit(pid, :kill)

    wait_until(fn -> DurableServer.Supervisor.lookup(supervisor_name, key) == nil end)

    assert {:ok, {new_pid, _meta}} =
             DurableServer.Supervisor.ensure_started_child(
               supervisor_name,
               {DurableServer.TestCounterServer, key: key, initial_state: %{count: 0}}
             )

    assert new_pid != pid
    assert GenServer.call(new_pid, :get_count) == 2
  end

  defp wait_until(fun, attempts \\ 50) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition never became true")
      true ->
        Process.sleep(20)
        wait_until(fun, attempts - 1)
    end
  end

  defp stored_state(state) do
    %StoredState{
      vsn: 1,
      state: state,
      meta: %Meta{
        status: :running,
        pid: self(),
        node_str: Atom.to_string(Node.self()),
        node_ref: System.unique_integer([:positive, :monotonic]),
        last_heartbeat_at: System.system_time(:millisecond),
        crash_history: []
      }
    }
  end
end
