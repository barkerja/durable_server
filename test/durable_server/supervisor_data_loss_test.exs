defmodule DurableServer.SupervisorDataLossTest do
  use ExUnit.Case, async: true

  alias DurableServer.StorageBackend

  defmodule InMemoryBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(_opts) do
      {:ok,
       %{
         state: %{table: :ets.new(__MODULE__, [:set, :public])},
         defaults: %{
           heartbeat_tracking_mode: :poll,
           discovery_interval_ms: 60_000,
           heartbeat_interval_ms: 10_000,
           heartbeat_reconcile_interval_ms: 10_000
         }
       }}
    end

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(%{table: table}, key, _opts) do
      case :ets.lookup(table, key) do
        [{^key, %{body: body, etag: etag}}] -> {:ok, %{body: body, etag: etag}}
        [] -> {:error, :not_found}
      end
    end

    @impl true
    def list_all_objects_stream(%{table: table}, prefix, _opts) do
      table
      |> :ets.tab2list()
      |> Stream.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
      |> Stream.map(fn {key, %{etag: etag}} -> %{key: key, etag: etag} end)
    end

    @impl true
    def put_object(%{table: table}, key, data, _opts) do
      etag = next_etag()
      :ets.insert(table, {key, %{body: data, etag: etag}})
      {:ok, %{body: data, etag: etag}}
    end

    @impl true
    def delete_object(%{table: table}, key) do
      :ets.delete(table, key)
      :ok
    end

    @impl true
    def delete_object(%{table: table}, key, _opts) do
      :ets.delete(table, key)
      :ok
    end

    @impl true
    def try_claim(%{table: table}, key, body) do
      case :ets.lookup(table, key) do
        [] ->
          etag = next_etag()
          :ets.insert(table, {key, %{body: body, etag: etag}})
          {:ok, {:claimed, etag}}

        [_existing] ->
          {:error, :taken}
      end
    end

    @impl true
    def update_object(%{table: table} = state, key, update_fn, _opts) do
      with {:ok, %{body: body, etag: etag}} <- get_object(state, key, []),
           {:ok, new_body} <- update_fn.(%{body: body, etag: etag}) do
        put_object(%{table: table}, key, new_body, [])
      end
    end

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    defp next_etag, do: System.unique_integer([:positive, :monotonic]) |> Integer.to_string()
  end

  # Stands in for an EncryptedStore whose get_object fails to decrypt an
  # object that genuinely exists, rather than for a key that never existed --
  # the distinction do_ensure_started_child/check_existing must preserve.
  defmodule FailingGetBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      fail_keys = opts |> Keyword.get(:fail_keys, []) |> MapSet.new()

      {:ok,
       %{
         state: %{delegate: delegate, fail_keys: fail_keys},
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate, fail_keys: fail_keys}, key, opts) do
      if MapSet.member?(fail_keys, key) do
        {:error, :simulated_decrypt_failure}
      else
        StorageBackend.get_object(delegate, key, opts)
      end
    end

    @impl true
    def list_all_objects_stream(%{delegate: delegate}, prefix, opts),
      do: StorageBackend.list_all_objects_stream(delegate, prefix, opts)

    @impl true
    def put_object(%{delegate: delegate}, key, data, opts),
      do: StorageBackend.put_object(delegate, key, data, opts)

    @impl true
    def delete_object(%{delegate: delegate}, key), do: StorageBackend.delete_object(delegate, key)

    @impl true
    def delete_object(%{delegate: delegate}, key, opts),
      do: StorageBackend.delete_object(delegate, key, opts)

    @impl true
    def try_claim(%{delegate: delegate}, key, body),
      do: StorageBackend.try_claim(delegate, key, body)

    @impl true
    def update_object(%{delegate: delegate}, key, update_fn, opts),
      do: StorageBackend.update_object(delegate, key, update_fn, opts)

    @impl true
    def encode(%{delegate: delegate}, data), do: StorageBackend.encode(delegate, data)

    @impl true
    def decode(%{delegate: delegate}, data), do: StorageBackend.decode(delegate, data)
  end

  defmodule DataLossTestServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: state
    def load_state(_old_vsn, persisted_state), do: persisted_state

    def init(loaded_state, info) do
      {:ok, Map.put(loaded_state, :key, info.key), auto_sync: false}
    end
  end

  test "a get_object decrypt failure never starts a fresh child over intact persisted state" do
    supervisor_name = :"data_loss_#{System.unique_integer([:positive, :monotonic])}"
    prefix = "data_loss/#{System.unique_integer([:positive, :monotonic])}/"
    key = "server-#{System.unique_integer([:positive, :monotonic])}"
    storage_key = prefix <> key

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: {FailingGetBackend, delegate: {InMemoryBackend, []}, fail_keys: [storage_key]},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
    delegate = storage_backend.state.delegate

    assert {:ok, _object} = StorageBackend.put_object(delegate, storage_key, %{"count" => 999})

    assert {:error, :simulated_decrypt_failure} =
             DurableServer.Supervisor.ensure_started_child(
               supervisor_name,
               {DataLossTestServer, key: key, initial_state: %{"count" => 0}}
             )

    assert DurableServer.Supervisor.lookup(supervisor_name, key) == nil

    assert {:ok, %{body: %{"count" => 999}}} = StorageBackend.get_object(delegate, storage_key)
  end

  test "start_child(existing: true) surfaces the real reason instead of :not_found" do
    supervisor_name = :"data_loss_existing_#{System.unique_integer([:positive, :monotonic])}"
    prefix = "data_loss_existing/#{System.unique_integer([:positive, :monotonic])}/"
    key = "server-#{System.unique_integer([:positive, :monotonic])}"
    storage_key = prefix <> key

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: {FailingGetBackend, delegate: {InMemoryBackend, []}, fail_keys: [storage_key]},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
    delegate = storage_backend.state.delegate

    assert {:ok, _object} = StorageBackend.put_object(delegate, storage_key, %{"count" => 999})

    assert {:error, :simulated_decrypt_failure} =
             DurableServer.Supervisor.start_child(
               supervisor_name,
               {DataLossTestServer, key: key, initial_state: %{"count" => 0}},
               existing: true
             )

    assert DurableServer.Supervisor.lookup(supervisor_name, key) == nil
  end
end
