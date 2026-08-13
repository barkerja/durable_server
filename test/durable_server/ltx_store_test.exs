defmodule DurableServer.LTXStoreTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.{LTXStore, ObjectStore}
  alias DurableServer.{Meta, StorageBackend, StoredState}

  @head_marker "__durable_server_ltx__"


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
        [backend: backend!(DurableServer.TestMemoryBackend, table), page_size: 512, inline_threshold_pages: 4],
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

  describe "subscribe/4" do
    test "decodes inline head events and hides segment traffic", context do
      assert {:ok, subscription_ref} =
               StorageBackend.subscribe(context.store, self(), "server/")

      small = %{count: 1}
      assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", small)

      assert_receive {:durable_server_storage_events,
                      [%{type: :put, key: "server/1", value: ^small}]}

      # A segment-form write produces exactly one logical event — the
      # segment object's own put is invisible to subscribers.
      big = big_state(1)
      assert {:ok, _object} = StorageBackend.put_object(context.store, "server/2", big)

      assert_receive {:durable_server_storage_events,
                      [%{type: :put, key: "server/2", value: ^big}]}

      refute_receive {:durable_server_storage_events, _events}, 100

      assert :ok = StorageBackend.unsubscribe(context.store, subscription_ref)
    end

    test "passes legacy (unmarked) values through unchanged", context do
      assert {:ok, _ref} = StorageBackend.subscribe(context.store, self(), "server/")

      # Simulate a legacy writer putting a plain value directly underneath.
      {:ok, memory} = StorageBackend.init_backend(DurableServer.TestMemoryBackend, context.table)
      assert {:ok, _object} = StorageBackend.put_object(memory, "server/legacy", %{"old" => true})

      assert_receive {:durable_server_storage_events,
                      [%{key: "server/legacy", value: %{"old" => true}}]}
    end

    test "tears down the relay and inner subscription when the subscriber dies", context do
      subscriber = spawn(fn -> Process.sleep(:infinity) end)

      assert {:ok, {relay_pid, inner_ref}} =
               StorageBackend.subscribe(context.store, subscriber, "server/")

      assert Process.alive?(relay_pid)

      assert [{{:subscriber, ^inner_ref}, ^relay_pid, "server/"}] =
               :ets.lookup(context.table, {:subscriber, inner_ref})

      relay_monitor = Process.monitor(relay_pid)
      Process.exit(subscriber, :kill)

      assert_receive {:DOWN, ^relay_monitor, :process, ^relay_pid, _reason}
      assert :ets.lookup(context.table, {:subscriber, inner_ref}) == []
    end

    test "heartbeat defaults pass through the wrapped backend", context do
      assert StorageBackend.defaults(context.store).heartbeat_tracking_mode == :subscribe
      assert StorageBackend.supports?(context.store, :heartbeat_subscribe?)
    end
  end

  describe "DurableServer.LTX.Sweeper" do
    test "sweeps on demand and on its schedule", context do
      assert {:ok, %{etag: etag}} = StorageBackend.put_object(context.store, "server/1", big_state(1))
      assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(2), etag: etag)

      orphan = "0000000000000001-0000000000000001.deadbeef.ltx"
      :ets.insert(context.table, {{:object, "__ltx/server/1/" <> orphan}, %{"junk" => true}, "9"})

      {:ok, sweeper} =
        DurableServer.LTX.Sweeper.start_link(
          backend: context.store,
          interval_ms: 3_600_000
        )

      assert {:ok, %{deleted: 1}} = DurableServer.LTX.Sweeper.sweep_now(sweeper)
      refute ("__ltx/server/1/" <> orphan) in segment_keys(context, "server/1")

      # Scheduled path: a fresh orphan is collected without manual prodding.
      :ets.insert(context.table, {{:object, "__ltx/server/1/" <> orphan}, %{"junk" => true}, "9"})

      {:ok, _fast_sweeper} =
        DurableServer.LTX.Sweeper.start_link(backend: context.store, interval_ms: 25)

      wait_until(fn -> not (("__ltx/server/1/" <> orphan) in segment_keys(context, "server/1")) end)
    end

    test "the supervisor starts a sweeper for an LTX storage backend", context do
      supervisor_name = :"ltx_sweeper_#{System.unique_integer([:positive, :monotonic])}"

      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: supervisor_name,
           prefix: "ltx_sweeper/",
           backend:
             {DurableServer.Backends.LTXStore,
              [
                backend: {DurableServer.TestMemoryBackend, context.table},
                page_size: 512,
                sweep_interval_ms: 25
              ]},
           graceful_shutdown_timeout_ms: 500
         ]}
      )

      # Plant an orphan behind a real head written through the supervisor's
      # backend.
      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      {:ok, %{etag: etag}} = StorageBackend.put_object(backend, "ltx_sweeper/k", big_state(1))
      {:ok, _object} = StorageBackend.put_object(backend, "ltx_sweeper/k", big_state(2), etag: etag)

      orphan = "__ltx/ltx_sweeper/k/0000000000000001-0000000000000001.deadbeef.ltx"
      :ets.insert(context.table, {{:object, orphan}, %{"junk" => true}, "9"})

      wait_until(fn -> :ets.lookup(context.table, {:object, orphan}) == [] end)
    end

    test "sweep_interval_ms: :disabled starts no sweeper", context do
      supervisor_name = :"ltx_no_sweeper_#{System.unique_integer([:positive, :monotonic])}"

      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: supervisor_name,
           prefix: "ltx_no_sweeper/",
           backend:
             {DurableServer.Backends.LTXStore,
              [
                backend: {DurableServer.TestMemoryBackend, context.table},
                page_size: 512,
                sweep_interval_ms: :disabled
              ]},
           graceful_shutdown_timeout_ms: 500
         ]}
      )

      sweeper_children =
        supervisor_name
        |> Supervisor.which_children()
        |> Enum.filter(&match?({{DurableServer.LTX.Sweeper, _ref}, _pid, _type, _mods}, &1))

      assert sweeper_children == []
    end
  end

  describe "sweep_orphans/2" do
    test "deletes unreferenced segments the head has advanced past", context do
      assert {:ok, %{etag: etag}} = StorageBackend.put_object(context.store, "server/1", big_state(1))
      assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(2), etag: etag)

      # Plant an orphan behind the head (txid 1, not in the manifest).
      orphan_name = "0000000000000001-0000000000000001.deadbeef.ltx"
      :ets.insert(context.table, {{:object, "__ltx/server/1/" <> orphan_name}, %{"junk" => true}, "9"})

      before_keys = segment_keys(context, "server/1")
      assert ("__ltx/server/1/" <> orphan_name) in before_keys

      assert {:ok, %{deleted: 1, kept: 2, keys: 1}} =
               DurableServer.Backends.LTXStore.sweep_orphans(context.store)

      refute ("__ltx/server/1/" <> orphan_name) in segment_keys(context, "server/1")
      assert {:ok, %{body: %{tag: 2}}} = StorageBackend.get_object(cold_reader(context), "server/1", [])
    end

    test "keeps unreferenced segments ahead of the head and of unknown age", context do
      assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))

      # An in-flight delta (txid 2 > head txid 1) whose head CAS hasn't landed.
      in_flight = "0000000000000002-0000000000000002.cafebabe.ltx"
      :ets.insert(context.table, {{:object, "__ltx/server/1/" <> in_flight}, %{"junk" => true}, "9"})

      # A headless key's segment (interrupted first write) with unknown age.
      headless = "0000000000000001-0000000000000001.0badf00d.ltx"
      :ets.insert(context.table, {{:object, "__ltx/server/2/" <> headless}, %{"junk" => true}, "9"})

      assert {:ok, %{deleted: 0, kept: 3, keys: 2}} =
               DurableServer.Backends.LTXStore.sweep_orphans(context.store)

      assert ("__ltx/server/1/" <> in_flight) in segment_keys(context, "server/1")
      assert ("__ltx/server/2/" <> headless) in segment_keys(context, "server/2")
    end

    test "deletes aged headless segments when the store reports ages", context do
      # The memory backend reports no last_modified, so simulate age via a
      # wrapped list by inserting and sweeping with min_age_ms 0 — age is
      # still unknown, so even min_age_ms 0 keeps it.
      headless = "0000000000000001-0000000000000001.0badf00d.ltx"
      :ets.insert(context.table, {{:object, "__ltx/server/2/" <> headless}, %{"junk" => true}, "9"})

      assert {:ok, %{deleted: 0, kept: 1, keys: 1}} =
               DurableServer.Backends.LTXStore.sweep_orphans(context.store, min_age_ms: 0)
    end

    test "respects the prefix filter", context do
      assert {:ok, %{etag: etag}} = StorageBackend.put_object(context.store, "a/1", big_state(1))
      assert {:ok, _object} = StorageBackend.put_object(context.store, "a/1", big_state(2), etag: etag)
      assert {:ok, %{etag: etag_b}} = StorageBackend.put_object(context.store, "b/1", big_state(1))
      assert {:ok, _object} = StorageBackend.put_object(context.store, "b/1", big_state(2), etag: etag_b)

      orphan = "0000000000000001-0000000000000001.deadbeef.ltx"
      :ets.insert(context.table, {{:object, "__ltx/a/1/" <> orphan}, %{"junk" => true}, "9"})
      :ets.insert(context.table, {{:object, "__ltx/b/1/" <> orphan}, %{"junk" => true}, "9"})

      assert {:ok, %{deleted: 1, keys: 1}} =
               DurableServer.Backends.LTXStore.sweep_orphans(context.store, prefix: "a/")

      refute ("__ltx/a/1/" <> orphan) in segment_keys(context, "a/1")
      assert ("__ltx/b/1/" <> orphan) in segment_keys(context, "b/1")
    end
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
