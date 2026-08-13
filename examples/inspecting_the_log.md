# Inspecting the LTX Log

The other examples treat the segment backend as transparent — which it is.
This one goes underneath: what actually lands in the bucket, how to watch a
delta versus a snapshot write, how to decode a segment with the
`DurableServer.LTX` codec, and how to sweep orphans by hand. Useful when
you're debugging, auditing storage, or just verifying the machinery does
what the README claims.

All examples assume a supervisor running the LTX segment backend — see the
"LTX Segment Backend" section of the [README](../README.md) for the full
option reference, storage layout, and failure modes.

## Getting at the layers

The supervisor's storage backend is the LTX wrapper; its `state.backend` is
the wrapped transport (ObjectStore, EKV, or an EncryptedStore over either):

```elixir
alias DurableServer.StorageBackend

%{storage_backend: ltx} = DurableServer.Supervisor.__get_config__(MyDurableSup)
transport = ltx.state.backend
```

Reads through `ltx` restore and decode; reads through `transport` show the
stored bytes as the bucket sees them (modulo the transport's own codec).

## The head object

Write something larger than the inline threshold, then fetch the object at
the key through the *transport*:

```elixir
{:ok, %{body: head}} = StorageBackend.get_object(transport, "my_app/doc_42")

head["__durable_server_ltx__"]  #=> 1                (format version)
head["txid"]                    #=> 3                (current transaction id)
head["post_apply_checksum"]     #=> "8f3a..."        (rolling checksum, 16 hex)
head["commit"]                  #=> 261              (image size in pages)
head["segments"]
#=> [
#     %{"min" => 1, "max" => 1, "name" => "0000...01-0000...01.9dc41f22.ltx"},
#     %{"min" => 2, "max" => 2, "name" => "0000...02-0000...02.5e77a010.ltx"},
#     %{"min" => 3, "max" => 3, "name" => "0000...03-0000...03.c2b90d4e.ltx"}
#   ]
head["inline"]                  #=> nil              (small states inline here)
```

The manifest *is* the log: only files it names exist as far as readers are
concerned. Segment objects live under the global namespace, at
`"__ltx/" <> key <> "/" <> name` — note the random nonce in each filename,
which is why concurrent writers can never overwrite each other's bytes:

```elixir
transport
|> StorageBackend.list_all_objects_stream("__ltx/my_app/doc_42/")
|> Enum.map(& &1.key)
```

(Listing through `ltx` instead would show you nothing — the wrapper hides
segment traffic from its own listings and subscriptions.)

## Decoding a segment

Segment bodies are base64-wrapped LTX files; the codec is public API:

```elixir
alias DurableServer.LTX

[entry | _rest] = head["segments"]

{:ok, %{body: wrapped}} =
  StorageBackend.get_object(transport, "__ltx/my_app/doc_42/" <> entry["name"])

{:ok, decoded} = wrapped["data"] |> Base.decode64!() |> LTX.Decoder.decode()

decoded.header.min_txid            #=> 1
decoded.header.max_txid            #=> 1
decoded.header.commit              #=> 260
LTX.snapshot?(decoded.header)      #=> true   (min_txid == 1)
length(decoded.pages)              #=> 260    (a snapshot carries every page)
decoded.trailer.post_apply_checksum
#=> the rolling checksum the head claimed at txid 1
```

`Decoder.decode/1` verifies the file checksum and, for snapshots, the
post-apply checksum — the same fail-closed verification restores run. The
files are standard LTX v3, wire-compatible with `superfly/ltx`, so Go
tooling can read them too (once unwrapped from base64, and only in the
unencrypted composition).

## Watching a delta versus a snapshot

```elixir
# Small edit against the current etag:
{:ok, %{body: doc, etag: etag}} = StorageBackend.get_object(ltx, "my_app/doc_42")
{:ok, _} = StorageBackend.put_object(ltx, "my_app/doc_42", small_edit(doc), etag: etag)

# The new manifest entry is a one-txid delta; decode it:
{:ok, %{body: head}} = StorageBackend.get_object(transport, "my_app/doc_42")
last = List.last(head["segments"])
{:ok, %{body: wrapped}} =
  StorageBackend.get_object(transport, "__ltx/my_app/doc_42/" <> last["name"])
{:ok, delta} = wrapped["data"] |> Base.decode64!() |> LTX.Decoder.decode()

length(delta.pages)          #=> 2      (only the touched pages)
LTX.snapshot?(delta.header)  #=> false
delta.header.pre_apply_checksum
#=> the previous head's post_apply_checksum — the checksum chain
```

Keep writing: at `:max_segments` (default 16) the manifest collapses back
to a single snapshot entry and the superseded segment objects disappear —
that's compaction, observable as `length(head["segments"])` sawtoothing
between 1 and 16. An *unchanged* CAS write appends a zero-page delta
(`delta.pages == []`) so the TXID chain stays contiguous.

## Sweeping orphans by hand

A crash between a segment PUT and its head CAS strands a segment no
manifest references. The supervisor sweeps automatically, but the sweep is
ordinary public API — idempotent and safe to run anywhere, anytime:

```elixir
DurableServer.Backends.LTXStore.sweep_orphans(ltx, prefix: "my_app/")
#=> {:ok, %{deleted: 1, kept: 47, keys: 12}}
```

A segment is deleted only when its key's head has provably advanced past
its TXID range; anything possibly in-flight is kept until it ages out.

## With encryption underneath

In the `{LTXStore, backend: {EncryptedStore, ...}}` composition everything
above still works — but read through the *encrypted* backend
(`ltx.state.backend`), which unseals; the raw bucket objects are
`__durable_server_encrypted__` envelopes bound to their storage keys, and
nothing about the log's structure is visible without a recipient key.
