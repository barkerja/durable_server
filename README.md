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
