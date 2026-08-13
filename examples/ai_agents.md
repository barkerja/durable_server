# AI Agent State

Per-agent durable objects: conversation memory synced after every turn,
and an agent-framework (Jido) struct checkpointed after every instruction
step.

All examples assume a supervisor running the LTX segment backend — see the
"LTX Segment Backend" section of the [README](../README.md) for the full
option reference, storage layout, and failure modes.

Agent state is close to the ideal delta shape: a durable object per agent or
conversation, whose state is a growing transcript, tool-call log, and
accumulated memory — append-heavy, byte-stable, and synced after every turn.
With the plain backend, a long-running agent re-uploads its entire history
on each turn; with LTXStore each turn ships only the new messages.

```elixir
defmodule MyApp.AgentMemoryServer do
  use DurableServer, vsn: 1

  # Persist the transcript and distilled memory; drop runtime-only handles
  # (in-flight LLM request tasks, streaming pids) that must not survive a
  # rehome.
  def dump_state(state), do: Map.drop(state, [:inflight])
  def load_state(_old_vsn, persisted), do: Map.put(persisted, :inflight, nil)

  def init(state, _info), do: {:ok, state}

  def handle_call({:turn, user_message, assistant_reply, tool_calls}, _from, state) do
    state =
      state
      |> Map.update!(:messages, &(&1 ++ [user_message, assistant_reply]))
      |> Map.update!(:tool_log, &(&1 ++ tool_calls))

    # Durable after every turn: a crash or rehome resumes the conversation
    # with nothing lost, and the sync cost is the new messages, not the
    # whole transcript.
    {:reply, :ok, state, :sync}
  end

  def handle_call({:remember, fact}, _from, state) do
    {:reply, :ok, Map.update!(state, :memory, &Map.merge(&1, fact)), :sync}
  end
end

DurableServer.Supervisor.ensure_started_child(
  MyDurableSup,
  {MyApp.AgentMemoryServer,
   key: "agent:" <> conversation_id,
   initial_state: %{messages: [], tool_log: [], memory: %{}, inflight: nil}}
)
```

A 500 KiB transcript grows by one page or two per turn, so each `:sync`
ships a few KiB. Combined with the encrypted composition (see the Encrypted Backend
section of the README), per-user
agent memory is also sealed and key-bound at rest.

The same shape hosts an agent-framework struct — for example a
[Jido](https://github.com/agentjido/jido) agent, where the framework's
agent state (schema fields, instruction results, queued directives) lives
inside the durable state and every instruction step is checkpointed:

```elixir
defmodule MyApp.DurableJidoServer do
  use DurableServer, vsn: 1

  def dump_state(state), do: state
  def load_state(_old_vsn, persisted), do: persisted

  def init(%{agent: nil} = state, info) do
    # First boot: create the Jido agent; restarts restore it as-is — the
    # LTX image preserves the struct exactly (atoms, nested structs, refs).
    {:ok, %{state | agent: MyApp.PlannerAgent.new(info.key)}}
  end

  def init(state, _info), do: {:ok, state}

  def handle_call({:instruct, instruction, params}, _from, %{agent: agent} = state) do
    case MyApp.PlannerAgent.cmd(agent, instruction, params) do
      {:ok, agent, directives} ->
        state = %{state | agent: agent, steps: state.steps + 1}
        # Checkpoint after every instruction: a killed node resumes the
        # plan mid-flight instead of restarting it.
        {:reply, {:ok, directives}, state, :sync}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end
end
```

Only the fields the instruction touched change in the encoded state, so
checkpoint-per-step stays cheap even as the agent's working memory grows.
The usual persistence rules apply unchanged: keep runtime-only resources
(sockets, tasks, framework supervisor pids) out of `dump_state/1`, exactly
as with any DurableServer.
