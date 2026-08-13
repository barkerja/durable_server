defmodule DurableServer.StubEKV do
  @moduledoc """
  Stand-in for the real `EKV` module in managed-backend tests: starts a
  registered no-op process per instance (so supervisors can supervise it and
  tests can assert it is running) and answers the `EKV` API surface
  `DurableServer.Backends.EKVStore` uses with inert defaults. Inject with
  `ekv_mod: DurableServer.StubEKV, ekv_supervisor_mod: DurableServer.StubEKV.Config`.
  """

  use GenServer

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :name)},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: :"#{name}_ekv_sup")
  end

  @impl true
  def init(opts), do: {:ok, opts}

  # EKV API surface used by EKVStore, all inert.
  def keys(_name, _prefix), do: []
  def scan(_name, _prefix), do: []
  def lookup(_name, _key), do: nil
  def row_state(_name, _key, _opts), do: {:ok, :absent}
  def put(_name, _key, _value, _opts), do: {:ok, {1, :node@stub}}
  def update(_name, _key, _fun, _opts), do: {:ok, :updated, {1, :node@stub}}
  def get(_name, _key, _opts), do: :ok
  def delete(_name, _key, _opts), do: {:ok, {1, :node@stub}}
  def subscribe(_name, _prefix), do: :ok
  def unsubscribe(_name, _prefix), do: :ok

  defmodule Config do
    @moduledoc false
    def get_config(_name), do: %{cluster_size: 1}
  end
end
