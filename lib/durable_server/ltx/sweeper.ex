defmodule DurableServer.LTX.Sweeper do
  @moduledoc """
  Periodically runs `DurableServer.Backends.LTXStore.sweep_orphans/2` against
  one LTX-backed storage backend.

  Started automatically by `DurableServer.Supervisor` when its storage
  backend is an `LTXStore` whose `:sweep_interval_ms` is not `:disabled`
  (default: every 6 hours). Runs the first sweep after a random delay within
  one interval and jitters subsequent runs by ±10%, so a fleet's sweeps
  spread out; sweeping is idempotent and safe to run from any node at any
  time, so overlapping sweeps from several nodes merely duplicate work.

  Options:

    * `:backend` (required) — the initialized `LTXStore`
      `DurableServer.StorageBackend`;
    * `:interval_ms` (required) — target interval between sweeps;
    * `:min_age_ms` — passed through to `sweep_orphans/2`;
    * `:prefix` — passed through to `sweep_orphans/2` (default `""`).
  """

  use GenServer

  alias DurableServer.Backends.LTXStore

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Runs a sweep immediately, synchronously. Intended for tests and consoles."
  def sweep_now(pid, timeout \\ :infinity), do: GenServer.call(pid, :sweep_now, timeout)

  @impl true
  def init(opts) do
    opts =
      Keyword.validate!(opts, [:backend, :interval_ms, :min_age_ms, prefix: ""])

    state = %{
      backend: Keyword.fetch!(opts, :backend),
      interval_ms: Keyword.fetch!(opts, :interval_ms),
      sweep_opts:
        Keyword.take(opts, [:prefix, :min_age_ms])
    }

    # First sweep lands at a random point within one interval so a fleet
    # restart doesn't synchronize every node's sweep schedule.
    schedule(:rand.uniform(state.interval_ms))
    {:ok, state}
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    {:reply, run_sweep(state), state}
  end

  @impl true
  def handle_info(:sweep, state) do
    _ = run_sweep(state)
    schedule(jittered(state.interval_ms))
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp run_sweep(state) do
    LTXStore.sweep_orphans(state.backend, state.sweep_opts)
  rescue
    error ->
      Logger.warning("DurableServer.LTX.Sweeper sweep failed: #{Exception.message(error)}")
      {:error, error}
  end

  defp schedule(delay_ms), do: Process.send_after(self(), :sweep, delay_ms)

  defp jittered(interval_ms) do
    jitter = div(interval_ms, 10)
    interval_ms - jitter + :rand.uniform(2 * jitter)
  end
end
