# CI/CD Pipeline Orchestration

An event-sourced pipeline-run object that survives coordinator deploys
and crashes — and deliberately *not* an artifact store.

All examples assume a supervisor running the LTX segment backend — see the
"LTX Segment Backend" section of the [README](../README.md) for the full
option reference, storage layout, and failure modes.

A pipeline run is a durable object whose state is an event log plus the
derived job DAG — append-only, synced on every transition, so a coordinator
deploy or crash resumes every in-flight pipeline exactly where it stopped
(on any node, thanks to rehoming).

One thing this is deliberately **not**: artifact or cache storage. Durable
state is memory-resident and delta-synced — wrong on both counts for large
write-once blobs, which need neither CAS fencing nor deltas. Jobs PUT
artifacts straight to the bucket as plain objects; the run state tracks
references only.

```elixir
defmodule MyApp.PipelineRun do
  use DurableServer, vsn: 1

  def dump_state(state), do: state
  def load_state(_old_vsn, persisted), do: persisted
  def init(state, _info), do: {:ok, state}

  # Every transition appends to the event log and checkpoints. The reply
  # tells the caller which jobs became runnable.
  def handle_call({:event, event}, _from, state) do
    state =
      state
      |> Map.update!(:events, &(&1 ++ [event]))
      |> apply_event(event)

    {:reply, runnable_jobs(state), state, :sync}
  end

  defp apply_event(state, {:job_finished, job, :ok, artifact_refs}) do
    # References to bucket objects the job already uploaded — never the
    # artifact bytes themselves.
    %{state | jobs: Map.put(state.jobs, job, {:succeeded, artifact_refs})}
  end

  defp apply_event(state, {:job_finished, job, {:error, reason}, _refs}) do
    %{state | jobs: Map.update!(state.jobs, job, fn _ -> {:failed, reason} end)}
  end

  # ... {:job_started, ...}, {:job_retried, ...}, dependency resolution ...
  defp runnable_jobs(state), do: MyApp.DAG.ready(state.jobs, state.dag)
end

DurableServer.Supervisor.ensure_started_child(
  MyDurableSup,
  {MyApp.PipelineRun,
   key: "run:" <> run_id,
   initial_state: %{dag: dag, jobs: initial_jobs(dag), events: []}}
)
```

The event log is the ideal delta shape — each transition ships the pages
holding the new event and the touched job entry — and a run's full history
remains queryable for as long as the run object lives.
