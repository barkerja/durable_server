# Sharded BM25 Search Engine — Encrypted and Compacted

An inverted index sharded across durable objects: hash-routed indexing,
fan-out/merge queries, contents sealed at rest, compaction automatic.

All examples assume a supervisor running the LTX segment backend — see the
"LTX Segment Backend" section of the [README](../README.md) for the full
option reference, storage layout, and failure modes.

Inverted-index shards are large, long-lived, rehomeable state: exactly what
the durable-object model plus segment persistence is for. Each shard is one
DurableServer holding postings, document lengths, and corpus statistics;
documents route to a shard by hash, and queries fan out and merge scores.
With the encrypted composition, index contents are sealed and key-bound at
rest, and compaction (snapshot resets at `:max_segments`) is automatic.

```elixir
# Supervisor: segments over sealed objects, as in the README's encrypted composition.
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
