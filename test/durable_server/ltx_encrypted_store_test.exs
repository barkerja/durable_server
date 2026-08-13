defmodule DurableServer.LTXEncryptedStoreTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.{EncryptedStore, LTXStore}
  alias DurableServer.{Encryption, StorageBackend}
  alias DurableServer.TestMemoryBackend, as: MemoryBackend

  # Encryption composes UNDER the segment layer: LTXStore diffs plaintext
  # pages, EncryptedStore seals each head and segment object it stores. The
  # reverse order (sealing before paging) would make every sync's bytes
  # differ (fresh content key and nonce per seal), defeating deltas entirely.

  defp stack!(table) do
    {:ok, memory} = StorageBackend.init_backend(MemoryBackend, table)
    {public_key, private_key} = Encryption.generate_key_pair()

    {:ok, encrypted} =
      StorageBackend.init_backend(EncryptedStore,
        backend: memory,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    {:ok, store} =
      StorageBackend.init_backend(LTXStore,
        backend: encrypted,
        page_size: 512,
        inline_threshold_pages: 4
      )

    {store, encrypted}
  end

  defp big_state(tag) do
    %{tag: tag, blob: :binary.copy(<<rem(tag, 251)>>, 8_000), pid: self(), ref: make_ref()}
  end

  setup do
    table = :ets.new(:ltx_encrypted_test, [:set, :public])
    {store, encrypted} = stack!(table)
    {:ok, table: table, store: store, encrypted: encrypted}
  end

  defp raw_bodies(context) do
    context.table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{:object, key}, body, _etag} -> [{key, body}]
      _other -> []
    end)
  end

  test "round trips inline and segment states with everything sealed at rest", context do
    small = %{count: 1, pid: self()}
    big = big_state(1)

    assert {:ok, %{etag: small_etag}} =
             StorageBackend.put_object(context.store, "server/small", small)

    assert {:ok, %{etag: big_etag}} = StorageBackend.put_object(context.store, "server/big", big)

    # Every object in the underlying store — heads and segments — is a sealed
    # envelope; no LTX head marker or segment marker is visible in plaintext.
    for {_key, body} <- raw_bodies(context) do
      assert Encryption.encrypted?(body)
    end

    # Cold stack (fresh caches) restores both.
    {cold, _encrypted} = stack_with_same_keys(context)

    assert {:ok, %{body: ^small, etag: ^small_etag}} =
             StorageBackend.get_object(cold, "server/small", [])

    assert {:ok, %{body: ^big, etag: ^big_etag}} = StorageBackend.get_object(cold, "server/big", [])
  end

  test "deltas still work through the sealed transport", context do
    state1 = big_state(1)
    state2 = %{state1 | tag: 2}

    assert {:ok, %{etag: etag1}} = StorageBackend.put_object(context.store, "server/1", state1)
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", state2, etag: etag1)

    segment_keys =
      raw_bodies(context)
      |> Enum.map(&elem(&1, 0))
      |> Enum.filter(&String.starts_with?(&1, "__ltx/server/1/"))

    assert length(segment_keys) == 2

    {cold, _encrypted} = stack_with_same_keys(context)
    assert {:ok, %{body: ^state2}} = StorageBackend.get_object(cold, "server/1", [])
  end

  test "a sealed segment moved to another storage key fails authentication", context do
    assert {:ok, _object} = StorageBackend.put_object(context.store, "server/1", big_state(1))

    [{segment_key, sealed_body}] =
      raw_bodies(context)
      |> Enum.filter(fn {key, _body} -> String.starts_with?(key, "__ltx/server/1/") end)

    moved_key = String.replace(segment_key, "server/1", "server/2")
    :ets.insert(context.table, {{:object, moved_key}, sealed_body, "1"})

    assert {:error, :authentication_failed} =
             StorageBackend.get_object(context.encrypted, moved_key, [])
  end

  test "the supervisor accepts the triple-nested spec" do
    supervisor_name = :"ltx_encrypted_#{System.unique_integer([:positive, :monotonic])}"
    prefix = "ltx_encrypted/#{System.unique_integer([:positive, :monotonic])}/"
    {public_key, private_key} = Encryption.generate_key_pair()
    table = :ets.new(:ltx_encrypted_supervisor_test, [:set, :public])

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend:
           {LTXStore,
            [
              backend:
                {EncryptedStore,
                 [
                   backend: {MemoryBackend, table},
                   recipient_public_keys: [public_key],
                   decryption_key: private_key
                 ]},
              page_size: 512
            ]},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

    assert storage_backend.adapter == LTXStore
    assert storage_backend.state.backend.adapter == EncryptedStore

    assert {:ok, {pid, _meta}} =
             DurableServer.Supervisor.ensure_started_child(
               supervisor_name,
               {DurableServer.TestCounterServer, key: "c1", initial_state: %{count: 0}}
             )

    assert GenServer.call(pid, :increment_and_sync) == 1

    # Whatever reached the raw table is sealed.
    sealed =
      table
      |> :ets.tab2list()
      |> Enum.filter(&match?({{:object, _key}, _body, _etag}, &1))

    assert sealed != []
    assert Enum.all?(sealed, fn {{:object, _key}, body, _etag} -> Encryption.encrypted?(body) end)
  end

  # Rebuild the stack over the same table with the same key pair — the pair
  # is embedded in the existing EncryptedStore state.
  defp stack_with_same_keys(context) do
    encrypted_state = context.encrypted.state

    {:ok, store} =
      StorageBackend.init_backend(LTXStore,
        backend: context.encrypted,
        page_size: 512,
        inline_threshold_pages: 4
      )

    _ = encrypted_state
    {store, context.encrypted}
  end
end
