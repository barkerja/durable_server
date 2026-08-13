defmodule DurableServer.LTX.Compactor do
  @moduledoc """
  Compacts multiple LTX files into one.

      {:ok, iodata} = Compactor.compact([oldest_binary, ..., newest_binary])

  Inputs must be ordered oldest-to-newest by TXID range, share one page size,
  and have contiguous TXID ranges. The output file spans
  `[first.min_txid, last.max_txid]`: for each page number the newest input's
  version wins, pages beyond the final commit size are dropped, the pre-apply
  checksum comes from the first input, and the post-apply checksum and commit
  come from the last — matching the Go reference compactor. Compacting a
  snapshot with its subsequent deltas therefore yields a new snapshot.

  Every input is fully verified (file checksum included) before any output is
  produced. Two checks are stricter than the reference implementation, both
  failing closed on corruption the reference would pass through:

    * where two inputs are exactly adjacent (`next.min_txid == prev.max_txid
      + 1`), the next file's pre-apply checksum must equal the previous
      file's post-apply checksum;
    * when the output is a snapshot with checksum tracking, the merged pages'
      rolling checksum must equal the claimed post-apply checksum.

  Options:

    * `:allow_non_contiguous_txids` (default `false`) — permit gaps between
      input TXID ranges, for rebuilds with known-missing transactions.
  """

  alias DurableServer.LTX
  alias DurableServer.LTX.{Decoder, Encoder}

  @spec compact([binary()], keyword()) :: {:ok, iodata()} | {:error, term()}
  def compact(inputs, opts \\ [])

  def compact([], _opts), do: {:error, :no_inputs}

  def compact(inputs, opts) when is_list(inputs) do
    opts = Keyword.validate!(opts, allow_non_contiguous_txids: false)

    with {:ok, decoded} <- decode_inputs(inputs),
         :ok <- validate_inputs(decoded, opts[:allow_non_contiguous_txids]) do
      first = List.first(decoded)
      last = List.last(decoded)

      header = %LTX.Header{
        flags: first.header.flags,
        page_size: first.header.page_size,
        commit: last.header.commit,
        min_txid: first.header.min_txid,
        max_txid: last.header.max_txid,
        timestamp: last.header.timestamp,
        pre_apply_checksum: first.header.pre_apply_checksum
      }

      pages = merge_pages(decoded, header.commit)
      post_apply = last.trailer.post_apply_checksum

      with :ok <- verify_snapshot_consistency(header, pages, post_apply),
           {:ok, enc} <- Encoder.new(header),
           {:ok, enc} <- encode_pages(enc, pages) do
        Encoder.finish(enc, post_apply)
      end
    end
  end

  defp decode_inputs(inputs) do
    inputs
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {input, index}, {:ok, acc} ->
      case Decoder.decode(input) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_input, index, reason}}}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_inputs(decoded, allow_non_contiguous?) do
    checksum_modes = decoded |> Enum.map(&LTX.no_checksum?(&1.header)) |> Enum.uniq()

    if length(checksum_modes) > 1 do
      {:error, :mixed_checksum_modes}
    else
      decoded
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.with_index(1)
      |> Enum.reduce_while(:ok, fn {[prev, next], index}, :ok ->
        case validate_pair(prev, next, index, allow_non_contiguous?) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp validate_pair(prev, next, index, allow_non_contiguous?) do
    exactly_adjacent? = next.header.min_txid == prev.header.max_txid + 1
    tracked? = not LTX.no_checksum?(prev.header)

    cond do
      prev.header.page_size != next.header.page_size ->
        {:error,
         {:page_size_mismatch, index, prev.header.page_size, next.header.page_size}}

      not allow_non_contiguous? and
          not LTX.contiguous?(prev.header.max_txid, next.header.min_txid, next.header.max_txid) ->
        {:error,
         {:non_contiguous_txids, index, {prev.header.min_txid, prev.header.max_txid},
          {next.header.min_txid, next.header.max_txid}}}

      # Only exactly-adjacent files promise this equality; overlapping ranges
      # legitimately have a pre-apply checksum from inside the previous range.
      exactly_adjacent? and tracked? and
          next.header.pre_apply_checksum != prev.trailer.post_apply_checksum ->
        {:error, {:checksum_chain_broken, index}}

      true ->
        :ok
    end
  end

  # The newest input wins for each page number; pages beyond the final commit
  # are dropped (the database shrank past them).
  defp merge_pages(decoded, commit) do
    decoded
    |> Enum.reduce(%{}, fn %{pages: pages}, acc ->
      Enum.reduce(pages, acc, fn {pgno, data}, acc -> Map.put(acc, pgno, data) end)
    end)
    |> Enum.reject(fn {pgno, _data} -> pgno > commit end)
    |> Enum.sort()
  end

  defp encode_pages(enc, pages) do
    Enum.reduce_while(pages, {:ok, enc}, fn {pgno, data}, {:ok, enc} ->
      case Encoder.encode_page(enc, pgno, data) do
        {:ok, enc} -> {:cont, {:ok, enc}}
        {:error, reason} -> {:halt, {:error, {:encode_page, pgno, reason}}}
      end
    end)
  end

  # A compacted snapshot must reproduce the database image whose rolling
  # checksum the newest input claims; anything else means a page was lost or
  # substituted somewhere in the chain. The reference implementation defers
  # this to whoever eventually decodes the snapshot — we refuse to emit it.
  # A deletion output (commit 0) is excluded: its post-apply checksum is the
  # bare checksum flag by convention, which Encoder.finish/2 enforces.
  defp verify_snapshot_consistency(%LTX.Header{commit: 0}, _pages, _post_apply), do: :ok

  defp verify_snapshot_consistency(header, pages, post_apply) do
    if LTX.snapshot?(header) and not LTX.no_checksum?(header) and
         LTX.checksum_pages(pages, header.page_size) != post_apply do
      {:error, :compacted_snapshot_checksum_mismatch}
    else
      :ok
    end
  end
end
