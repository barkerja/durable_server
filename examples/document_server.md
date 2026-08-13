# Large Document Server

The canonical delta shape: a large body plus append-only history,
synced on every edit.

All examples assume a supervisor running the LTX segment backend — see the
"LTX Segment Backend" section of the [README](../README.md) for the full
option reference, storage layout, and failure modes.

Supervisor configuration:

```elixir
children = [
  {DurableServer.Supervisor,
   name: MyDurableSup,
   prefix: "my_app/",
   backend:
     {DurableServer.Backends.LTXStore,
      backend: {DurableServer.Backends.ObjectStore, object_store_opts}}}
]
```

A representative server — a document holding a large body plus append-only
history, the ideal delta shape:

```elixir
defmodule MyApp.DocumentServer do
  use DurableServer, vsn: 1

  def dump_state(state), do: state
  def load_state(_old_vsn, persisted), do: persisted

  def init(state, _info), do: {:ok, state, auto_sync: true, sync_every_ms: 5_000}

  def handle_call({:edit, patch}, _from, state) do
    state =
      state
      |> Map.update!(:body, &apply_patch(&1, patch))
      |> Map.update!(:history, &[patch | &1])

    {:reply, :ok, state, :sync}
  end
end

DurableServer.Supervisor.ensure_started_child(
  MyDurableSup,
  {MyApp.DocumentServer, key: "doc_42", initial_state: %{body: "", history: []}}
)
```

With a 1 MiB body, each edit syncs a delta segment of roughly the touched
pages (a few KiB) instead of re-uploading the full megabyte; every ~16th
sync writes a fresh snapshot and resets the log.
