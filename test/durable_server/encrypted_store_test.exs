defmodule DurableServer.EncryptedStoreTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

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

  # Never replies to the relay's inner subscribe call, so the outer
  # subscribe/4 always runs out its @subscribe_ready_timeout_ms and falls
  # through to abandon_subscribe/3's untrapped-message branch.
  defmodule HangingSubscribeBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(_opts), do: {:ok, %{state: %{}}}

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(_state, _key, _opts), do: {:error, :not_found}

    @impl true
    def list_all_objects_stream(_state, _prefix, _opts), do: []

    @impl true
    def put_object(_state, _key, data, _opts), do: {:ok, %{body: data, etag: "1"}}

    @impl true
    def delete_object(_state, _key), do: :ok

    @impl true
    def try_claim(_state, _key, _body), do: {:error, :already_claimed}

    @impl true
    def update_object(_state, _key, _update_fn, _opts), do: {:error, :not_found}

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    @impl true
    def subscribe(_state, _subscriber, _prefix, _opts), do: Process.sleep(:infinity)
  end

  # A real KV store (shared ETS table) whose own encode/2 and decode/2 are
  # each wired to return a value distinct from whatever they were called
  # with, so any test relying on them silently fails instead of coincidentally
  # passing. Two EncryptedStore backends fronting the same table but
  # different codec tags stand in for two legs of a MirrorStore migration
  # that see the same physical bytes at different times.
  defmodule OpaqueCodecBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(opts) do
      {:ok, %{state: %{table: Keyword.fetch!(opts, :table), codec: Keyword.fetch!(opts, :codec)}}}
    end

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(%{table: table}, key, _opts) do
      case :ets.lookup(table, key) do
        [{^key, body, etag}] -> {:ok, %{body: body, etag: etag}}
        [] -> {:error, :not_found}
      end
    end

    @impl true
    def list_all_objects_stream(%{table: table}, prefix, _opts) do
      table
      |> :ets.tab2list()
      |> Stream.filter(fn {key, _body, _etag} -> String.starts_with?(key, prefix) end)
      |> Stream.map(fn {key, _body, etag} -> %{key: key, etag: etag} end)
    end

    @impl true
    def put_object(%{table: table}, key, body, _opts) do
      etag = System.unique_integer([:positive, :monotonic]) |> Integer.to_string()
      :ets.insert(table, {key, body, etag})
      {:ok, %{body: body, etag: etag}}
    end

    @impl true
    def delete_object(%{table: table}, key) do
      :ets.delete(table, key)
      :ok
    end

    @impl true
    def try_claim(%{table: table}, key, body) do
      case :ets.insert_new(table, {key, body, "1"}) do
        true -> {:ok, {:claimed, "1"}}
        false -> {:error, :already_claimed}
      end
    end

    @impl true
    def update_object(%{} = state, key, update_fn, opts) do
      with {:ok, %{body: body, etag: etag}} <- get_object(state, key, []),
           {:ok, new_body} <- update_fn.(%{body: body, etag: etag}) do
        put_object(state, key, new_body, opts)
      end
    end

    @impl true
    def encode(%{codec: :a}, _data), do: {:ok, "encoded-by-backend-a"}
    def encode(%{codec: :b}, _data), do: {:ok, "encoded-by-backend-b"}

    @impl true
    def decode(%{codec: :a}, _data), do: {:ok, "decoded-by-backend-a"}
    def decode(%{codec: :b}, _data), do: {:ok, "decoded-by-backend-b"}
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

    assert {:ok, %{body: %StoredState{state: %{secret: "value"}}}} =
             StorageBackend.get_object(encrypted, key)
  end

  test "a value sealed while wrapping one backend decodes correctly while wrapping another (MirrorStore cutover)" do
    table = :ets.new(:encrypted_store_codec_independence_test, [:set, :public])
    {public_key, private_key} = Encryption.generate_key_pair()

    leg_a = backend!(OpaqueCodecBackend, table: table, codec: :a)
    leg_b = backend!(OpaqueCodecBackend, table: table, codec: :b)

    encrypted_a =
      backend!(EncryptedStore,
        backend: leg_a,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    encrypted_b =
      backend!(EncryptedStore,
        backend: leg_b,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    key = "server/cutover"

    value = %{
      atom_key: "value",
      nested: %{pid: self(), ref: make_ref()},
      list: [:one, :two, 3]
    }

    assert {:ok, _object} = StorageBackend.put_object(encrypted_a, key, value)
    assert {:ok, %{body: decoded}} = StorageBackend.get_object(encrypted_b, key)

    assert decoded == value
    assert is_atom(Map.keys(decoded) |> List.first())
    assert is_pid(decoded.nested.pid)
    assert is_reference(decoded.nested.ref)
  end

  test "keeps the decryption key out of inspect/1 output" do
    table = :ets.new(:encrypted_store_inspect_test, [:set, :public])
    underlying = backend!(MemoryBackend, table)
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    refute inspect(encrypted) =~ byte_list(private_key)
  end

  test "decodes a payload carrying a pid whose node atom this VM has not interned", context do
    key = "server/pid-roundtrip"

    atom_name =
      "durable_server_f3_fresh_atom_#{System.unique_integer([:positive, :monotonic])}@nohost"

    canonical_bytes = atom_name |> fresh_pid_term_bytes() |> wrap_in_canonical_payload_format()

    # Precondition: the atom genuinely is not interned yet, so the vulnerable
    # `:safe` decode this fix replaces would reject these exact bytes.
    assert_raise ArgumentError, fn -> :erlang.binary_to_term(canonical_bytes, [:safe]) end

    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: context.underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    assert {:ok, envelope} = Encryption.seal(canonical_bytes, [public_key], key)
    :ets.insert(context.table, {{:object, key}, envelope, "1"})

    assert {:ok, %{body: pid, etag: "1"}} = StorageBackend.get_object(encrypted, key, [])
    assert is_pid(pid)
    assert Atom.to_string(node(pid)) == atom_name
  end

  test "rejects an oversized payload instead of decoding it", context do
    key = "server/oversized"
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: context.underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    oversized = :erlang.term_to_binary(:crypto.strong_rand_bytes(16_777_216))
    assert byte_size(oversized) > 16_777_216

    assert {:ok, envelope} = Encryption.seal(oversized, [public_key], key)
    :ets.insert(context.table, {{:object, key}, envelope, "1"})

    assert {:error, {:payload_too_large, size}} = StorageBackend.get_object(encrypted, key, [])
    assert size == byte_size(oversized)
  end

  test "rejects an oversized write instead of orphaning the object", context do
    key = "server/oversized-write"
    oversized = :crypto.strong_rand_bytes(18_000_000)

    assert {:error, {:payload_too_large, size}} =
             StorageBackend.put_object(context.encrypted, key, oversized)

    assert size > 16_777_216

    assert {:error, :not_found} = StorageBackend.get_object(context.underlying, key)
    assert {:error, :not_found} = StorageBackend.get_object(context.encrypted, key)
  end

  test "a write that succeeds near the size boundary always remains readable", context do
    key = "server/near-boundary"
    near_boundary = :crypto.strong_rand_bytes(16_000_000)

    assert {:ok, %{body: ^near_boundary}} =
             StorageBackend.put_object(context.encrypted, key, near_boundary)

    assert {:ok, %{body: ^near_boundary}} = StorageBackend.get_object(context.encrypted, key)
  end

  test "rejects a compressed payload before decompressing it", context do
    key = "server/compression-bomb"
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: context.underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    # 20 MB of zeros compresses to a few KB but must never be inflated back.
    bomb = :erlang.term_to_binary(:binary.copy(<<0>>, 20_000_000), compressed: 9)
    assert byte_size(bomb) < 16_777_216

    assert {:ok, envelope} = Encryption.seal(bomb, [public_key], key)
    :ets.insert(context.table, {{:object, key}, envelope, "1"})

    assert {:error, :compressed_payload_rejected} =
             StorageBackend.get_object(encrypted, key, [])
  end

  test "tears down the relay and inner subscription when the subscriber dies", context do
    subscriber = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, {relay_pid, inner_ref}} =
             StorageBackend.subscribe(context.encrypted, subscriber, "server/")

    assert Process.alive?(relay_pid)

    assert [{{:subscriber, ^inner_ref}, ^relay_pid, "server/"}] =
             :ets.lookup(context.table, {:subscriber, inner_ref})

    relay_monitor = Process.monitor(relay_pid)
    Process.exit(subscriber, :kill)

    assert_receive {:DOWN, ^relay_monitor, :process, ^relay_pid, _reason}
    assert :ets.lookup(context.table, {:subscriber, inner_ref}) == []
  end

  test "leaves no stray :DOWN message in the caller's mailbox after a subscribe timeout" do
    underlying = backend!(HangingSubscribeBackend, [])
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key
      )

    assert {:error, :subscribe_timeout} = StorageBackend.subscribe(encrypted, self(), "server/")

    refute_receive {:DOWN, _ref, :process, _pid, _reason}, 200
  end

  test "rejects a marker-present but malformed body instead of treating it as plaintext",
       context do
    key = "server/malformed"
    :ets.insert(context.table, {{:object, key}, %{"__durable_server_encrypted__" => 1}, "1"})

    assert {:error, :invalid_envelope} = StorageBackend.get_object(context.encrypted, key, [])
  end

  test "rejects unmarked bodies when plaintext_compat is strict" do
    table = :ets.new(:encrypted_store_strict_test, [:set, :public])
    underlying = backend!(MemoryBackend, table)
    {public_key, private_key} = Encryption.generate_key_pair()

    encrypted =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [public_key],
        decryption_key: private_key,
        plaintext_compat: :strict
      )

    :ets.insert(table, {{:object, "server/1"}, %{legacy: true}, "1"})

    assert {:error, :plaintext_rejected} = StorageBackend.get_object(encrypted, "server/1", [])
  end

  test "logs a warning when returning legacy plaintext under the default permissive mode",
       context do
    assert {:ok, %{etag: "1"}} =
             StorageBackend.put_object(context.underlying, "server/1", %{legacy: true})

    log =
      capture_log(fn ->
        assert {:ok, %{body: %{legacy: true}}} =
                 StorageBackend.get_object(context.encrypted, "server/1", [])
      end)

    assert log =~ "read an unmarked plaintext object"
  end

  test "a node in an earlier rotation phase can read what the next phase writes" do
    table = :ets.new(:encrypted_store_rotation_test, [:set, :public])
    underlying = backend!(MemoryBackend, table)

    {old_public_key, old_private_key} = Encryption.generate_key_pair()
    {new_public_key, new_private_key} = Encryption.generate_key_pair()

    phase1 =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [old_public_key, new_public_key],
        decryption_key: old_private_key
      )

    phase2 =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [old_public_key, new_public_key],
        decryption_key: new_private_key
      )

    phase3 =
      backend!(EncryptedStore,
        backend: underlying,
        recipient_public_keys: [new_public_key],
        decryption_key: new_private_key
      )

    assert {:ok, _object} = StorageBackend.put_object(phase1, "server/1", %{written_by: :phase1})

    assert {:ok, %{body: %{written_by: :phase1}}} =
             StorageBackend.get_object(phase2, "server/1")

    assert {:ok, _object} = StorageBackend.put_object(phase2, "server/2", %{written_by: :phase2})

    assert {:ok, %{body: %{written_by: :phase2}}} =
             StorageBackend.get_object(phase1, "server/2")

    assert {:ok, %{body: %{written_by: :phase2}}} =
             StorageBackend.get_object(phase3, "server/2")
  end

  defp backend!(adapter, opts) do
    {:ok, backend} = StorageBackend.init_backend(adapter, opts)
    backend
  end

  defp byte_list(binary), do: binary |> :binary.bin_to_list() |> Enum.join(", ")

  defp fresh_pid_term_bytes(atom_name) when is_binary(atom_name) do
    <<131, 88, 118, byte_size(atom_name)::16, atom_name::binary, 1::32, 0::32, 1::32>>
  end

  # Wraps hand-built term bytes (as produced by fresh_pid_term_bytes/1, version
  # byte included) in the same {:dse_payload_v1, term} 2-tuple serialize/1
  # produces, using the real atom encoding for the current OTP release rather
  # than a hardcoded tag byte.
  defp wrap_in_canonical_payload_format(<<131, inner::binary>>) do
    <<131, tag_atom::binary>> = :erlang.term_to_binary(:dse_payload_v1)
    <<131, 104, 2>> <> tag_atom <> inner
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
