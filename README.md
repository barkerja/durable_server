# DurableServer

DurableServer provides durable, distributed GenServer processes backed by pluggable storage backends.

It implements fault-tolerant, stateful processes that can survive node failures, restarts, and deployments by automatically persisting state and coordinating across a distributed cluster.

## Key Features

- **Durable state**: Automatically persists state to storage with configurable sync intervals
- **Cluster coordination**: Uses distributed registry for process discovery and health monitoring
- **Capacity-aware placement**: Monitors CPU, memory, and disk usage to route new processes to nodes with available capacity
- **Sticky placement**: Environment variable-based placement preferences (e.g., same machine, same region, etc.) with time-gated fallback to ensure servers restart on preferred nodes when possible
- **Automatic recovery**: Failed processes are detected and restarted across the cluster
- **Graceful shutdown**: Ensures state is synchronized before termination
- **Administrative cordon**: Stop a server and block all automatic or explicit restarts until uncordoned
- **Lifecycle monitoring & dispatch**: Monitor lifecycle events and dispatch messages between DurableServers and other processes
- **Pluggable backends**: Run with object storage, EKV, or a dual-backend migration adapter

## Installation

Add `durable_server` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:durable_server, "~> 0.1.0"}
  ]
end
```

For releases, add `:os_mon` to `extra_applications`:

```elixir
def application do
  [
    mod: {MyApp.Application, []},
    extra_applications: [:logger, :runtime_tools, :os_mon]
  ]
end
```

## Basic Usage

```elixir
defmodule MyCounterServer do
  use DurableServer, vsn: 1

  def dump_state(state), do: %{count: state.count}

  def load_state(_old_vsn, %{"count" => count}), do: %{count: count}

  def init(%{count: count} = state) do
    {:ok, Map.merge(state, %{started_at: DateTime.utc_now()})}
  end

  def handle_call(:increment, _from, state) do
    new_state = %{state | count: state.count + 1}
    {:reply, new_state.count, new_state}
  end

  def handle_call(:get_count, _from, state) do
    {:reply, state.count, state}
  end
end
```

Start the supervisor (typically in your application supervision tree):

```elixir
children = [
  {DurableServer.Supervisor,
   name: MyDurableSup,
   prefix: "my_app/",
   object_store: [
     bucket: "my-bucket",
     access_key_id: System.fetch_env!("DURABLE_AWS_ACCESS_KEY_ID"),
     secret_access_key: System.fetch_env!("DURABLE_AWS_SECRET_ACCESS_KEY"),
     s3_endpoint: System.fetch_env!("DURABLE_AWS_ENDPOINT_URL_S3"),
     default_region: System.fetch_env!("DURABLE_AWS_REGION")
   ]}
]
```

Start and use individual servers:

```elixir
{:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(
  MyDurableSup,
  {MyCounterServer, key: "user_123", initial_state: %{count: 0}}
)

GenServer.call(pid, :increment)  # => 1
GenServer.call(pid, :increment)  # => 2
GenServer.call(pid, :get_count)  # => 2
```

`:initial_state` is required and must be a map. On first boot, DurableServer
passes it through `dump_state/1`, the configured backend's encode/decode path,
and then `load_state/2` before `init/1` or `init/2`. The dumped initial state
must therefore be encodable by your configured backend.

## Administrative Cordon

Use `terminate_and_cordon_child/3` when you need to stop a DurableServer and
make it ineligible for any future starts until an operator explicitly clears the
cordon.

```elixir
# Stop a running child and persist status: :cordoned
:ok = DurableServer.Supervisor.terminate_and_cordon_child(MyDurableSup, pid)

# Or cordon by key. If the child is not running, storage is updated directly.
:ok = DurableServer.Supervisor.terminate_and_cordon_child(
  MyDurableSup,
  "user_123",
  timeout: 10_000
)

# Later, allow explicit starts and LifecycleManager recovery again.
:ok = DurableServer.Supervisor.uncordon_child(MyDurableSup, "user_123")
```

A cordoned object stores `status: :cordoned`. While cordoned:

- `start_child/3` and `ensure_started_child/3` return `{:error, :cordoned}`
- LifecycleManager skips the object, even when it is permanent
- restart claims treat the object as not eligible

When cordoning by key, DurableServer uses the same lock/orphan checks as normal
startup before updating storage. A live owner is asked to persist the cordoned
status itself; an orphaned owner is only replaced after the lock is expired.

## Storage Backends

`DurableServer` includes two built-in backends:

### Object Storage Backend

```elixir
{DurableServer.Supervisor,
 name: MyDurableSup,
 prefix: "my_app/",
 backend: {DurableServer.Backends.ObjectStore,
  [
    bucket: "my-bucket",
    access_key_id: "...",
    secret_access_key: "...",
    s3_endpoint: "...",
    default_region: "..."
  ]}}
```

### EKV Backend

Start EKV in your application tree (CAS config is required for DurableServer lock semantics):

```elixir
ekv_config = [
  name: :durable_ekv,
  data_dir: "/path/to/ekv_store",
  cluster_size: 3
]

children = [
  {EKV,
   name: :durable_ekv,
   data_dir: "/data/ekv/durable",
   cluster_size: 3,
   node_id: System.fetch_env!("EKV_NODE_ID")},
  {DurableServer.Supervisor,
   name: MyDurableSup,
   prefix: "my_app/",
   backend: {DurableServer.Backends.EKVStore, ekv_config}}
]
```

If you use EKV backend, add EKV to your app's dependencies.

### Mirror Backend (Object Storage -> EKV)

Use the mirror backend to dual-write while you cut over reads/writes in phases.

See `DurableServer.Backends.MirrorStore` for usage and an example rollout.

### Encrypted Backend

Wrap any backend with `DurableServer.Backends.EncryptedStore` to encrypt state
before it reaches the underlying store. Encryption uses a fresh content key per
object and supports up to 32 X25519 recipients, following the envelope model
used by `superfly/ltx`. The encrypted store seals a canonical, backend-agnostic
encoding of the exact term you store — atom keys, pids, and references all
survive unchanged no matter which backend it wraps — and never asks the
wrapped backend to interpret that plaintext; only the sealed envelope
(ciphertext, wrapped per-recipient keys, nonce, and tag) reaches the wrapped
backend's own codec. The one exception is an object written before encryption
was enabled: it is read through the wrapped backend's own codec exactly as if
this wrapper were absent, and gains the same fidelity guarantee the next time
it's written.

Both the write and the read path bound the encoded payload at 16 MiB
(16,777,216 bytes), enforced from the same constant so the two sides cannot
drift apart. A write larger than the bound fails immediately with
`{:error, {:payload_too_large, size}}` instead of succeeding and leaving
behind an object nothing can ever read back; a read rejects anything over the
bound, or anything claiming ETF compression, before attempting to decode it.

Generate a key pair once and store the private key in your secret manager:

```elixir
{public_key, private_key} = DurableServer.Encryption.generate_key_pair()

Base.url_encode64(public_key, padding: false)
Base.url_encode64(private_key, padding: false)
```

Decode the keys in runtime configuration and wrap your normal backend:

```elixir
public_key =
  "DURABLE_ENCRYPTION_PUBLIC_KEY"
  |> System.fetch_env!()
  |> Base.url_decode64!(padding: false)

private_key =
  "DURABLE_ENCRYPTION_PRIVATE_KEY"
  |> System.fetch_env!()
  |> Base.url_decode64!(padding: false)

children = [
  {DurableServer.Supervisor,
   name: MyDurableSup,
   prefix: "my_app/",
   backend:
     {DurableServer.Backends.EncryptedStore,
      backend: {DurableServer.Backends.ObjectStore, object_store_opts},
      recipient_public_keys: [public_key],
      decryption_key: private_key}}
]
```

Existing plaintext objects remain readable by default
(`plaintext_compat: :permissive`, the default) and are encrypted the next time
they are written. Permissive mode means anything with write access to the
wrapped backend can inject an unmarked object and have it accepted with no
cryptographic check; every such read is logged with `Logger.warning/1` so the
exposure is observable — there is no accompanying `:telemetry` event, since
`:telemetry` is only a transitive dependency here, not one this library
declares. Pass `plaintext_compat: :strict` once a deployment no longer needs
to read pre-encryption data — reads of an unmarked object then fail with
`{:error, :plaintext_rejected}` instead of being returned.

If you let the supervisor manage the EKV process directly — passing
`data_dir`/`cluster_size`/`node_id` to `DurableServer.Backends.EKVStore`
instead of starting your own `EKV` — wrapping that spec in `EncryptedStore`
also derives an encrypted heartbeat store automatically when you don't supply
one, using an `EKVStore` under a `heartbeats/` subdirectory of the same
`data_dir`. The derived store always carries forward the primary store's
`recipient_public_keys`, `decryption_key`, and `plaintext_compat`, so a
primary store configured `:strict` cannot end up with a silently
`:permissive` heartbeat store.

The storage key is authenticated with the ciphertext, so moving an encrypted
value to a different key causes decryption to fail.

### Rotating Keys

Rotation changes two independent things — the recipient set and which private
key this deployment holds — and they must be rolled out as separate,
fleet-wide steps. Changing the decryption key and shrinking the recipient set
in the same deploy means a mid-rollout node running the new config writes
objects that a node still on the old config cannot decrypt; those objects
also disappear entirely from `list_all_objects_stream` for the old-config
node, since it cannot even decode their metadata.

1. **Add the new recipient, fleet-wide.** Deploy
   `recipient_public_keys: [old_pub, new_pub]` with `decryption_key: old_priv`
   to every node. Every node can still decrypt with the old key. Writing an
   object — any `put_object`/`update_object` — is what re-encrypts it for the
   new recipient; reading one never does. Rewrite or backfill the objects you
   want protected under the new key, then verify the fleet is healthy on this
   config before continuing.
2. **Switch the decryption key, fleet-wide.** Deploy the same
   `recipient_public_keys: [old_pub, new_pub]` (unchanged) with
   `decryption_key: new_priv` to every node. Every node can now decrypt
   objects sealed for either recipient, so this is safe regardless of which
   objects were rewritten in step 1.
3. **Drop the old recipient.** Only once every node is confirmed running step
   2's config, deploy `recipient_public_keys: [new_pub]`. Any object still
   sealed only for `old_pub` (because it was never rewritten) becomes
   unrecoverable at this point.

A decryption-key change and a recipient-set shrink must never land in the
same deploy — that is the two-step version of this procedure, and it is
unsafe.

Rotation does not complete on its own. Nothing here re-encrypts objects your
application never rewrites, and no tooling ships to force a rewrite of
everything under a prefix. If your workload doesn't naturally touch every
object, step 1 requires you to write your own backfill (read and rewrite each
key, for example via `list_all_objects_stream`) before step 3 is safe.

### LTX Segment Backend

Wrap any backend with `DurableServer.Backends.LTXStore` to persist each key
as a log of [LTX](https://github.com/superfly/ltx) segments instead of one
full-state object — the persistence model Litestream uses for SQLite,
applied to DurableServer state. A sync then writes only the pages of the
encoded state that actually changed, which matters for large states:
benchmarked point mutations of a 1 MiB state write ~3% of the full-state
bytes, and appends under 1%.

#### When to use it — and when not to

LTXStore pays off when these hold:

- **States are large** (tens of KiB to hundreds of MiB encoded). Below the
  inline threshold (16 KiB by default) it behaves exactly like the plain
  backend, so it is harmless — but also pointless — for uniformly tiny
  states.
- **Syncs are frequent relative to state size.** The win is per-sync bytes;
  a server that syncs once at shutdown gains little.
- **Mutations are byte-stable.** Appending to logs/queues, updating
  fixed-size fields, or touching a few entries of a large map all produce
  small deltas. AI-agent state — per-conversation transcripts, tool-call
  logs, accumulated memory, checkpoint-per-step plans — is a natural fit
  (see the agent examples below). A length-changing edit early in the encoded term shifts
  every later page and degrades that sync toward full-snapshot cost — never
  *worse* than the plain backend's every-sync full write, but no better.
- **You can spare the read amplification.** A cold restore fetches the head
  plus up to `:max_segments` objects instead of one. Rehome and cold-start
  latency grows accordingly (bounded by snapshot resets).

Prefer the plain backend when:

- states are small (session stubs, counters, presence records) — the inline
  path makes LTXStore equivalent, so the extra moving parts buy nothing;
- you rely on **inspecting stored JSON** in the bucket — LTX heads and
  segments are opaque envelopes, not human-readable state (the trade that
  buys full term fidelity: atom keys, pids, and refs survive);
- the payload is **large write-once blobs** (build artifacts, caches, media)
  — those don't belong in *any* DurableServer state, which is
  memory-resident; PUT them to the bucket directly and keep references in
  state (see the CI/CD example below);
- your mutation pattern is a wholesale state replacement each sync — every
  delta would be a full rewrite plus segment overhead;
- you need rock-bottom operational surface: one object per key, no
  manifests, no sweeper.

#### Usage

Configuration is one wrapper in the supervisor spec — server modules,
`dump_state/1`/`load_state/2`, and every `DurableServer.Supervisor` API are
completely unchanged:

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

##### AI agents

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
ships a few KiB. Combined with the encrypted composition below, per-user
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

##### A sharded BM25 search engine — encrypted and compacted

Inverted-index shards are large, long-lived, rehomeable state: exactly what
the durable-object model plus segment persistence is for. Each shard is one
DurableServer holding postings, document lengths, and corpus statistics;
documents route to a shard by hash, and queries fan out and merge scores.
With the encrypted composition, index contents are sealed and key-bound at
rest, and compaction (snapshot resets at `:max_segments`) is automatic.

```elixir
# Supervisor: segments over sealed objects, as in the composition above.
backend:
  {DurableServer.Backends.LTXStore,
   backend:
     {DurableServer.Backends.EncryptedStore,
      backend: {DurableServer.Backends.ObjectStore, object_store_opts},
      recipient_public_keys: [public_key],
      decryption_key: private_key}}
```

```elixir
defmodule MyApp.SearchShard do
  use DurableServer, vsn: 1

  @k1 1.2
  @b 0.75

  def dump_state(state), do: state
  def load_state(_old_vsn, persisted), do: persisted
  def init(state, _info), do: {:ok, state}

  def handle_call({:index, docs}, _from, state) do
    {:reply, :ok, Enum.reduce(docs, state, &index_doc/2), :sync}
  end

  # Queries are read-only — no sync, no storage traffic.
  def handle_call({:search, terms, limit}, _from, state) do
    {:reply, top_k(state, terms, limit), state}
  end

  defp index_doc({id, text}, state) do
    terms = tokenize(text)

    postings =
      Enum.reduce(Enum.frequencies(terms), state.postings, fn {term, tf}, acc ->
        Map.update(acc, term, %{id => tf}, &Map.put(&1, id, tf))
      end)

    %{
      state
      | postings: postings,
        doc_lens: Map.put(state.doc_lens, id, length(terms)),
        total_len: state.total_len + length(terms)
    }
  end

  defp top_k(state, terms, limit) do
    n = map_size(state.doc_lens)
    avg_len = if n > 0, do: state.total_len / n, else: 1.0

    terms
    |> Enum.reduce(%{}, fn term, scores ->
      docs = Map.get(state.postings, term, %{})
      idf = :math.log(1 + (n - map_size(docs) + 0.5) / (map_size(docs) + 0.5))

      Enum.reduce(docs, scores, fn {id, tf}, scores ->
        len = Map.fetch!(state.doc_lens, id)
        score = idf * (tf * (@k1 + 1)) / (tf + @k1 * (1 - @b + @b * len / avg_len))
        Map.update(scores, id, score, &(&1 + score))
      end)
    end)
    |> Enum.sort_by(fn {_id, score} -> -score end)
    |> Enum.take(limit)
  end

  defp tokenize(text),
    do: text |> String.downcase() |> String.split(~r/[^a-z0-9]+/, trim: true)
end

defmodule MyApp.Search do
  @shards 8

  def index(id, text) do
    {:ok, {pid, _meta}} = shard(:erlang.phash2(id, @shards))
    GenServer.call(pid, {:index, [{id, text}]})
  end

  def search(query, limit \\ 10) do
    terms = query |> String.downcase() |> String.split()

    0..(@shards - 1)
    |> Task.async_stream(fn n ->
      {:ok, {pid, _meta}} = shard(n)
      GenServer.call(pid, {:search, terms, limit})
    end)
    |> Enum.flat_map(fn {:ok, hits} -> hits end)
    |> Enum.sort_by(fn {_id, score} -> -score end)
    |> Enum.take(limit)
  end

  defp shard(n) do
    DurableServer.Supervisor.ensure_started_child(
      MyDurableSup,
      {MyApp.SearchShard,
       key: "search:shard:#{n}",
       initial_state: %{postings: %{}, doc_lens: %{}, total_len: 0}}
    )
  end
end
```

An honest cost note: posting lists are length-changing map values, so
indexing one document shifts every encoded byte after the earliest touched
term — per-document syncs degrade toward snapshot cost. Index in batches
(one `:sync` per batch, as above) and the rewrite amortizes; queries cost
nothing. What the segment model buys the search shard is cheap durability
for *incremental* progress, bounded-restore rehoming of multi-megabyte
shards, and encryption at rest — not free single-document indexing.

##### CI/CD pipeline orchestration

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

Options (all set on the `LTXStore` spec):

| Option | Default | Meaning |
|---|---|---|
| `:backend` | required | The wrapped, transport-providing backend spec |
| `:page_size` | `4096` | LTX page size; power of two, 512–65536 |
| `:inline_threshold_pages` | `4` | States at or below this many pages embed in the head (one PUT) |
| `:max_segments` | `16` | Manifest length that triggers a snapshot reset |
| `:sweep_interval_ms` | `21_600_000` (6 h) | Automatic orphan-sweep interval; `:disabled` opts out |

#### Migrating an existing deployment

Switching an existing prefix to LTXStore is in-place: objects written before
the wrapper read through unchanged and convert to LTX form on their next
write. Rolling back is the reverse migration *only for keys still in inline
or legacy form* — once a key has segment form, the plain backend cannot read
it, so treat the cutover as forward-only (or run a `MirrorStore` phase
first, as with any backend migration).

#### How it stores data

The object at the key itself remains the **head**: it carries the current
transaction id, rolling checksum, and segment manifest, and it is the sole
CAS/fencing point — ownership semantics are identical to the plain backend.
Segments are immutable objects under the global `__ltx/` namespace, named
with a random nonce so concurrent writers can never overwrite each other's
bytes. Three write paths are chosen automatically:

- states encoding to at most `:inline_threshold_pages` pages (default 4, so
  16 KiB at the default 4096-byte `:page_size`) are embedded in the head
  directly — one PUT, the same cost as the plain backend;
- when the writer holds the previous image (populated by any read or write,
  so a rehomed owner deltas immediately after its first restore) and the
  caller's etag matches, only changed pages ship as one delta segment
  followed by the head CAS;
- otherwise — and whenever the manifest reaches `:max_segments` (default
  16) — the full image ships as one snapshot segment and the log resets,
  bounding restore cost. Segments the new head no longer references are
  deleted eagerly (there is no retention window; superseded history is not
  kept).

Reads restore the head's manifest: every segment's file checksum, the
transaction-id chain, the checksum chain, and the final rolling checksum are
all verified, and anything inconsistent fails closed with an error — never
`{:error, :not_found}`, so a damaged log can never be mistaken for a fresh
key and silently overwritten. Objects written before LTXStore was introduced
read through unchanged and convert to segment form on their next write.

A lost head CAS can leave an orphan segment no manifest references. The
supervisor automatically runs `DurableServer.LTX.Sweeper` against an LTX
storage backend every `:sweep_interval_ms` (default 6 hours, jittered across
the fleet; pass `:disabled` to opt out). Sweeping is idempotent and safe to
run from any node at any time, concurrently with live traffic, so you can
also invoke it manually:

```elixir
%{storage_backend: backend} = DurableServer.Supervisor.__get_config__(MyDurableSup)
DurableServer.Backends.LTXStore.sweep_orphans(backend, prefix: "my_app/")
```

To encrypt, compose with the encrypted backend **under** the segment layer,
so heads and segments are sealed per object and bound to their storage keys:

```elixir
backend:
  {DurableServer.Backends.LTXStore,
   backend:
     {DurableServer.Backends.EncryptedStore,
      backend: {DurableServer.Backends.ObjectStore, object_store_opts},
      recipient_public_keys: [public_key],
      decryption_key: private_key}}
```

The reverse order would seal the term before paging it, and a fresh content
key per seal means no two syncs ever share bytes — deltas would never match.
Note the encrypted backend's 16 MiB payload bound applies per sealed object,
which caps practical state size around 10 MiB for this composition
(segment bytes carry base64 overhead through the wrapped codec).

Storage subscriptions relay through the wrapper: subscribers observe logical
keys with decoded values, segment traffic under `__ltx/` is invisible to
them, and the wrapped backend's heartbeat tracking mode passes through — so
an LTX-wrapped EKV backend keeps subscribe-based heartbeats. Wrapping a
*managed* EKV backend (`data_dir`/`cluster_size`/`node_id`) also derives the
split heartbeat store automatically; the derived store is deliberately not
LTX-wrapped (heartbeats are tiny, constantly-rewritten values that gain
nothing from segment form), while an encrypted layer under the LTX wrapper
is carried forward so heartbeats stay encrypted.

#### Failure modes to expect

- A read of a damaged or partially deleted log returns a descriptive error
  (`{:segment_restore_failed, name, reason}`,
  `:restored_checksum_mismatch`, ...) after one retry against a freshly read
  head, and logs a warning. It never returns `{:error, :not_found}`, so
  `ensure_started_child/3` will surface the failure instead of starting a
  fresh child over intact data.
- A crash between a segment PUT and the head CAS leaves an orphan segment.
  It is invisible to readers (the manifest defines the log) and the sweeper
  deletes it once the head has provably advanced past its TXID range.
- An unchanged state synced with CAS still advances the head (a zero-page
  segment keeps the transaction chain contiguous), so etag-based fencing
  behaves exactly as with the plain backend.

Known limits: child keys must not live under the reserved `__ltx/`
namespace, and encoded states are capped at 1 GiB.

## Configuration Options

DurableServer supports these options in the `init/1` return tuple:

- `:auto_sync` - Enable automatic periodic syncing (default: false)
- `:sync_every_ms` - Sync interval in milliseconds (default: 30_000)
- `:meta` - Optional metadata included in the global registry

## State Synchronization

State is synchronized to storage in these scenarios:

1. **Manual sync**: Return `:sync` from any callback: `{:noreply, state, :sync}`
2. **Automatic sync**: When `:auto_sync` is enabled, changes sync on the `:sync_every_ms` interval
3. **Graceful shutdown**: State is always synced before termination

## Group

`Group` provides distributed process groups, registry, lifecycle monitoring, and isolated subclusters.

### Monitoring Events

Monitor lifecycle events for DurableServers:

```elixir
# Monitor a specific key
:ok = Group.monitor(MyDurableSup, "user/123")

# Monitor all keys with a prefix
:ok = Group.monitor(MyDurableSup, "user/")

# Monitor all events
:ok = Group.monitor(MyDurableSup, :all)
```

Monitors receive `{:group, events, info}` tuples in their mailbox:

```elixir
def handle_info({:group, events, _info}, state) do
  Enum.each(events, fn
    %Group.Event{type: :registered, key: key, pid: pid, previous_meta: nil} ->
      # A DurableServer started (previous_meta is nil for first registration)
      :ok
    %Group.Event{type: :unregistered, key: key, reason: reason} ->
      # A DurableServer stopped
      :ok
    _ -> :ok
  end)
  {:noreply, state}
end
```

Event types: `:registered`, `:unregistered`, `:joined`, `:left`

`:registered` and `:joined` events include a `previous_meta` field (`nil` for new, old meta for re-register/re-join). Single operations produce one event per tuple; bulk operations (nodedown, process death) batch all events together.

### Joining as a Member

Non-DurableServer processes can join keys to be discoverable and receive dispatched messages:

```elixir
# Join a key (e.g., from a Phoenix Channel)
:ok = Group.join(MyDurableSup, "room/123", %{type: :channel})

# Re-joining updates metadata in place
:ok = Group.join(MyDurableSup, "room/123", %{type: :channel, status: :active})

# Query all members of a key (DurableServers + joined processes)
members = Group.members(MyDurableSup, "room/123")
# => [{#PID<0.150.0>, %{...}}, {#PID<0.200.0>, %{type: :channel, status: :active}}]

# Leave when done (also happens automatically on process death)
:ok = Group.leave(MyDurableSup, "room/123")
```

### Dispatching to Members

Send messages to all members of a key:

```elixir
# From a DurableServer, broadcast to all connected channels
Group.dispatch(MyDurableSup, state.key, {:new_message, message})
```

### Named Clusters

For advanced use cases, you can create isolated subclusters where only connected nodes receive events:

```elixir
# Connect this node to a named cluster
:ok = Group.connect(MyDurableSup, :game_servers)

# Join/monitor/dispatch with the cluster: option
:ok = Group.join(MyDurableSup, "room/123", %{}, cluster: :game_servers)
:ok = Group.monitor(MyDurableSup, :all, cluster: :game_servers)
```

Note: DurableServers always register in the default cluster to ensure global uniqueness. Named clusters are purely for the pub/sub layer.

### Monitor vs Join

- **`monitor/2`**: Receive lifecycle events (`:registered`, `:unregistered`, `:joined`, `:left`) - system-generated
- **`join/3`**: Be discoverable via `members/2` and receive `dispatch/3` messages - application-level

These are independent - joining does not monitor events, and monitoring does not make you discoverable.

## Running Tests

### Unit Tests (with LocalStack)

Start LocalStack for S3-compatible storage:

```bash
docker run -d --name localstack -p 4566:4566 localstack/localstack
```

Run the tests:

```bash
mix test
```

### Integration Tests (with Tigris)

Set the required environment variables:
> *Note*: You can add these to a gitignored .env in this project and they will be loaded
automatically in `test_helper.exs`

```bash
export DURABLE_AWS_ACCESS_KEY_ID=<your-tigris-access-key>
export DURABLE_AWS_SECRET_ACCESS_KEY=<your-tigris-secret-key>
export DURABLE_AWS_ENDPOINT_URL_S3=https://t3.storage.dev
export DURABLE_AWS_ENDPOINT_URL_IAM=https://iam.storage.dev
export DURABLE_AWS_REGION=<your-region>
export DURABLE_BUCKET=<your-bucket-name>
```

Run integration tests (which hit t3.storage.dev directly):

```bash
mix test --include integration
```
