defmodule DurableServer.TestMemoryBackend do
  @moduledoc """
  In-memory ETS-backed StorageBackend for tests, with per-key failure modes:
  insert `{{:mode, key}, :conflict}` or `{{:mode, key}, :commit_then_conflict}`
  into the table to simulate CAS failures.
  """

  @behaviour DurableServer.StorageBackend

  alias DurableServer.StorageBackend

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
