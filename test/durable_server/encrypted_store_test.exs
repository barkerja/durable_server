defmodule DurableServer.EncryptedStoreTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.{EncryptedStore, ObjectStore}
  alias DurableServer.{Encryption, Meta, StorageBackend, StoredState}

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
            store(table, key, body, etag)
            {:error, :conflict}

          :conflict ->
            {:error, :conflict}

          :normal ->
            store(table, key, body, etag)
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
        true ->
          notify_subscribers(table, key, body)
          {:ok, {:claimed, "1"}}

        false ->
          {:error, :already_claimed}
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

    @impl true
    def subscribe(%{table: table}, subscriber, prefix, _opts) do
      ref = make_ref()
      :ets.insert(table, {{:subscriber, ref}, subscriber, prefix})
      {:ok, ref}
    end

    @impl true
    def unsubscribe(%{table: table}, ref) do
      :ets.delete(table, {:subscriber, ref})
      :ok
    end

    defp check_etag(nil, :error), do: :ok
    defp check_etag({_body, etag}, {:ok, etag}), do: :ok
    defp check_etag(_current, _etag), do: {:error, :conflict}

    defp next_etag(nil), do: "1"
    defp next_etag({_body, etag}), do: Integer.to_string(String.to_integer(etag) + 1)

    defp store(table, key, body, etag) do
      :ets.insert(table, {{:object, key}, body, etag})
      notify_subscribers(table, key, body)
    end

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

    defp notify_subscribers(table, key, body) do
      table
      |> :ets.tab2list()
      |> Enum.each(fn
        {{:subscriber, _ref}, subscriber, prefix} ->
          if String.starts_with?(key, prefix) do
            send(
              subscriber,
              {:durable_server_storage_events, [%{type: :put, key: key, value: body}]}
            )
          end

        _other ->
          :ok
      end)
    end
  end

  setup do
    table = :ets.new(:encrypted_store_test, [:set, :public])
    underlying = backend!(MemoryBackend, table)
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    %{table: table, underlying: underlying, encrypted: encrypted}
  end

  test "encrypts stored values and decrypts them on read", context do
    assert {:ok, %{body: %{secret: "value"}, etag: "1"}} =
             StorageBackend.put_object(context.encrypted, "server/1", %{secret: "value"})

    assert {:ok, %{body: raw}} = StorageBackend.get_object(context.underlying, "server/1")
    assert Encryption.encrypted?(raw)
    refute %{secret: "value"} in Map.values(raw)

    assert {:ok, %{body: %{secret: "value"}, etag: "1"}} =
             StorageBackend.get_object(context.encrypted, "server/1")
  end

  test "reads legacy plaintext and encrypts it on the next write", context do
    assert {:ok, %{etag: "1"}} =
             StorageBackend.put_object(context.underlying, "server/1", %{legacy: true})

    assert {:ok, %{body: %{legacy: true}, etag: "1"}} =
             StorageBackend.get_object(context.encrypted, "server/1")

    assert {:ok, %{etag: "2"}} =
             StorageBackend.put_object(
               context.encrypted,
               "server/1",
               %{legacy: false},
               etag: "1"
             )

    assert {:ok, %{body: raw}} = StorageBackend.get_object(context.underlying, "server/1")
    assert Encryption.encrypted?(raw)
  end

  test "supports encode/decode, update, listing, claims, and deletes", context do
    assert {:ok, encoded} = StorageBackend.encode(context.encrypted, %{count: 1})
    assert Encryption.encrypted?(encoded)
    assert {:ok, %{count: 1}} = StorageBackend.decode(context.encrypted, encoded)

    assert {:ok, {:claimed, "1"}} =
             StorageBackend.try_claim(context.encrypted, "server/1", %{count: 1})

    assert {:ok, %{body: %{count: 2}, etag: "2"}} =
             StorageBackend.update_object(context.encrypted, "server/1", fn %{body: body} ->
               {:ok, %{body | count: body.count + 1}}
             end)

    assert [%{key: "server/1", body: %{count: 2}}] =
             context.encrypted
             |> StorageBackend.list_all_objects_stream("server/", include_objects: true)
             |> Enum.to_list()

    assert :ok = StorageBackend.delete_object(context.encrypted, "server/1")
    assert {:error, :not_found} = StorageBackend.get_object(context.encrypted, "server/1")
  end

  test "binds ciphertext to its storage key", context do
    assert {:ok, _object} =
             StorageBackend.put_object(context.encrypted, "server/1", %{secret: true})

    assert {:ok, %{body: raw}} = StorageBackend.get_object(context.underlying, "server/1")
    assert {:ok, _object} = StorageBackend.put_object(context.underlying, "server/2", raw)

    assert {:error, :authentication_failed} =
             StorageBackend.get_object(context.encrypted, "server/2")
  end

  test "decrypts subscription events before forwarding them", context do
    assert {:ok, subscription_ref} =
             StorageBackend.subscribe(context.encrypted, self(), "server/")

    assert {:ok, _object} =
             StorageBackend.put_object(context.encrypted, "server/1", %{status: :running})

    assert_receive {:durable_server_storage_events,
                    [%{type: :put, key: "server/1", value: %{status: :running}}]}

    assert :ok = StorageBackend.unsubscribe(context.encrypted, subscription_ref)
  end

  test "preserves ambiguous conditional-write recovery", context do
    stored_state = stored_state(%{count: 1})

    assert {:ok, %{etag: "1"}} =
             StorageBackend.put_object(context.encrypted, "server/1", stored_state)

    :ets.insert(context.table, {{:mode, "server/1"}, :commit_then_conflict})
    updated = %{stored_state | state: %{count: 2}}

    assert {:ok, %{body: ^updated, etag: "2"}} =
             StorageBackend.put_object(context.encrypted, "server/1", updated, etag: "1")
  end

  test "allows any configured recipient to decrypt" do
    table = :ets.new(:encrypted_store_recipients_test, [:set, :public])
    underlying = backend!(MemoryBackend, table)
    {public_key1, _private_key1} = Encryption.generate_key_pair()
    {public_key2, private_key2} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [public_key1, public_key2],
        decryption_key: private_key2
      )

    assert {:ok, _object} = StorageBackend.put_object(encrypted, "server/1", %{secret: true})
    assert {:ok, %{body: %{secret: true}}} = StorageBackend.get_object(encrypted, "server/1")
  end

  test "round trips StoredState through the ObjectStore JSON codec" do
    underlying = backend!(ObjectStore, DurableServer.TestHelper.test_object_store())
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    key = "encrypted-store/#{System.unique_integer([:positive, :monotonic])}"
    stored_state = stored_state(%{secret: "value"})

    assert {:ok, _object} = StorageBackend.put_object(encrypted, key, stored_state)
    assert {:ok, %{body: raw}} = StorageBackend.get_object(underlying, key)
    assert Encryption.encrypted?(raw)
    refute "value" in Map.values(raw)

    assert {:ok, %{body: %StoredState{state: %{"secret" => "value"}}}} =
             StorageBackend.get_object(encrypted, key)
  end

  defp backend!(adapter, opts) do
    {:ok, backend} = StorageBackend.init_backend(adapter, opts)
    backend
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
