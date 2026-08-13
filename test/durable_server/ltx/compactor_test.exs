defmodule DurableServer.LTX.CompactorTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias DurableServer.LTX
  alias DurableServer.LTX.{Compactor, Decoder, Encoder}

  @checksum_flag 1 <<< 63
  @page_size 512

  defp page(tag), do: for(i <- 0..(@page_size - 1), into: <<>>, do: <<rem(tag * 31 + div(i, 16) * 7, 251)>>)

  defp image_checksum(image), do: image |> Enum.sort() |> LTX.checksum_pages(@page_size)

  defp encode!(header, pages, post_apply) do
    {:ok, enc} = Encoder.new(header)

    enc =
      Enum.reduce(Enum.sort(pages), enc, fn {pgno, data}, enc ->
        {:ok, enc} = Encoder.encode_page(enc, pgno, data)
        enc
      end)

    {:ok, iodata} = Encoder.finish(enc, post_apply)
    IO.iodata_to_binary(iodata)
  end

  # Encodes a snapshot of `image` (a pgno => data map) at txid 1.
  defp snapshot!(image) do
    header = %LTX.Header{
      page_size: @page_size,
      commit: map_size(image),
      min_txid: 1,
      max_txid: 1,
      timestamp: 1_000
    }

    encode!(header, Enum.sort(image), image_checksum(image))
  end

  # Applies `changes` (pgno => data) to `image`, truncating/growing to
  # `commit`, and encodes the delta for txid range [txid, txid].
  defp delta!(image, changes, commit, txid) do
    updated =
      image
      |> Map.merge(changes)
      |> Map.filter(fn {pgno, _data} -> pgno <= commit end)

    header = %LTX.Header{
      page_size: @page_size,
      commit: commit,
      min_txid: txid,
      max_txid: txid,
      timestamp: 1_000 + txid,
      pre_apply_checksum: image_checksum(image)
    }

    {encode!(header, Enum.sort(changes), image_checksum(updated)), updated}
  end

  test "compacting a snapshot with deltas produces the final snapshot" do
    image1 = Map.new(1..4, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    {delta2, image2} = delta!(image1, %{2 => page(102)}, 4, 2)
    {delta3, image3} = delta!(image2, %{1 => page(201), 5 => page(205)}, 5, 3)

    assert {:ok, iodata} = Compactor.compact([snapshot, delta2, delta3])
    compacted = IO.iodata_to_binary(iodata)

    assert {:ok, decoded} = Decoder.decode(compacted)
    assert decoded.header.min_txid == 1
    assert decoded.header.max_txid == 3
    assert decoded.header.commit == 5
    assert decoded.header.timestamp == 1_003
    assert decoded.header.pre_apply_checksum == 0
    assert LTX.snapshot?(decoded.header)

    assert {:ok, database} = Decoder.decode_database(compacted)

    expected =
      image3 |> Enum.sort() |> Enum.map(fn {_pgno, data} -> data end) |> IO.iodata_to_binary()

    assert database == expected
  end

  test "a single input compacts to an equivalent file" do
    image = Map.new(1..3, fn pgno -> {pgno, page(pgno)} end)
    {delta, _updated} = delta!(image, %{2 => page(42)}, 3, 5)

    assert {:ok, iodata} = Compactor.compact([delta])
    assert {:ok, decoded} = Decoder.decode(IO.iodata_to_binary(iodata))

    assert decoded.header.min_txid == 5
    assert decoded.header.max_txid == 5
    assert decoded.pages == [{2, page(42)}]
  end

  test "a deletion as the newest input compacts to a deletion file" do
    image1 = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    deletion =
      encode!(
        %LTX.Header{
          page_size: @page_size,
          commit: 0,
          min_txid: 2,
          max_txid: 2,
          pre_apply_checksum: image_checksum(image1)
        },
        [],
        @checksum_flag
      )

    assert {:ok, iodata} = Compactor.compact([snapshot, deletion])
    assert {:ok, decoded} = Decoder.decode(IO.iodata_to_binary(iodata))

    assert decoded.header.commit == 0
    assert decoded.pages == []
    assert decoded.trailer.post_apply_checksum == @checksum_flag
  end

  test "pages beyond a shrunken commit are dropped" do
    image1 = Map.new(1..5, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    {delta2, image2} = delta!(image1, %{1 => page(11)}, 3, 2)

    assert {:ok, iodata} = Compactor.compact([snapshot, delta2])
    assert {:ok, decoded} = Decoder.decode(IO.iodata_to_binary(iodata))

    assert decoded.header.commit == 3
    assert Enum.map(decoded.pages, &elem(&1, 0)) == [1, 2, 3]
    assert {:ok, database} = Decoder.decode_database(IO.iodata_to_binary(iodata))
    assert byte_size(database) == 3 * @page_size
    assert decoded.trailer.post_apply_checksum == image_checksum(image2)
  end

  test "rejects an empty input list" do
    assert {:error, :no_inputs} = Compactor.compact([])
  end

  test "rejects mismatched page sizes" do
    small = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(small)

    big_page = :binary.copy(<<7>>, 1024)

    big =
      encode!(
        %LTX.Header{page_size: 1024, commit: 1, min_txid: 2, max_txid: 2,
          pre_apply_checksum: @checksum_flag ||| 1},
        [{1, big_page}],
        LTX.checksum_pages([{1, big_page}], 1024)
      )

    assert {:error, {:page_size_mismatch, 1, @page_size, 1024}} =
             Compactor.compact([snapshot, big])
  end

  test "rejects non-contiguous TXIDs unless explicitly allowed" do
    image1 = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    {delta2, image2} = delta!(image1, %{1 => page(11)}, 2, 2)
    {_delta3, image3} = delta!(image2, %{2 => page(12)}, 2, 3)
    {delta4, _image4} = delta!(image3, %{1 => page(13)}, 2, 4)

    assert {:error, {:non_contiguous_txids, 1, {2, 2}, {4, 4}}} =
             Compactor.compact([delta2, delta4])

    assert {:ok, iodata} =
             Compactor.compact([delta2, delta4], allow_non_contiguous_txids: true)

    assert {:ok, decoded} = Decoder.decode(IO.iodata_to_binary(iodata))
    assert decoded.header.min_txid == 2
    assert decoded.header.max_txid == 4
  end

  test "rejects a broken checksum chain between exactly adjacent inputs" do
    image1 = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    # A delta claiming a pre-apply checksum that is not the snapshot's
    # post-apply checksum, despite being TXID-adjacent.
    bogus_delta =
      encode!(
        %LTX.Header{
          page_size: @page_size,
          commit: 2,
          min_txid: 2,
          max_txid: 2,
          pre_apply_checksum: @checksum_flag ||| 0xBAD
        },
        [{1, page(11)}],
        @checksum_flag ||| 0xBAD2
      )

    assert {:error, {:checksum_chain_broken, 1}} = Compactor.compact([snapshot, bogus_delta])
  end

  test "rejects a corrupted input with its index" do
    image1 = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)
    {delta2, _image2} = delta!(image1, %{1 => page(11)}, 2, 2)

    flip_at = LTX.header_size() + 25
    <<prefix::binary-size(^flip_at), byte, rest::binary>> = delta2
    corrupted = <<prefix::binary, bxor(byte, 0x01), rest::binary>>

    assert {:error, {:invalid_input, 1, _reason}} = Compactor.compact([snapshot, corrupted])
  end

  test "rejects mixed checksum modes" do
    image1 = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    untracked =
      encode!(
        %LTX.Header{
          flags: LTX.header_flag_no_checksum(),
          page_size: @page_size,
          commit: 2,
          min_txid: 2,
          max_txid: 2
        },
        [{1, page(11)}],
        0
      )

    assert {:error, :mixed_checksum_modes} = Compactor.compact([snapshot, untracked])
  end

  test "compacts no-checksum inputs, inheriting the flag" do
    header = %LTX.Header{
      flags: LTX.header_flag_no_checksum(),
      page_size: @page_size,
      commit: 4,
      min_txid: 2,
      max_txid: 2
    }

    first = encode!(header, [{1, page(1)}], 0)
    second = encode!(%{header | min_txid: 3, max_txid: 3}, [{2, page(2)}], 0)

    assert {:ok, iodata} = Compactor.compact([first, second])
    assert {:ok, decoded} = Decoder.decode(IO.iodata_to_binary(iodata))

    assert LTX.no_checksum?(decoded.header)
    assert Enum.map(decoded.pages, &elem(&1, 0)) == [1, 2]
  end

  test "refuses to emit a snapshot whose pages do not match the claimed checksum" do
    image1 = Map.new(1..2, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    # A delta whose checksums describe a page-2 change its frames omit — an
    # invalid log that the reference compactor would pass through.
    updated = Map.put(image1, 2, page(99))

    lying_delta =
      encode!(
        %LTX.Header{
          page_size: @page_size,
          commit: 2,
          min_txid: 2,
          max_txid: 2,
          pre_apply_checksum: image_checksum(image1)
        },
        [],
        image_checksum(updated)
      )

    assert {:error, :compacted_snapshot_checksum_mismatch} =
             Compactor.compact([snapshot, lying_delta])
  end

  test "randomized history: compaction equals sequential application" do
    :rand.seed(:exsss, {42, 43, 44})

    image1 = Map.new(1..4, fn pgno -> {pgno, page(pgno)} end)
    snapshot = snapshot!(image1)

    {files, final_image, _txid} =
      Enum.reduce(2..21, {[snapshot], image1, 2}, fn _step, {files, image, txid} ->
        commit = :rand.uniform(8)

        # Pages the database grows into must be written by the transaction;
        # existing pages change with probability 1/3.
        changes =
          1..commit
          |> Enum.flat_map(fn pgno ->
            cond do
              not Map.has_key?(image, pgno) -> [{pgno, page(pgno * 1000 + txid)}]
              :rand.uniform(3) == 1 -> [{pgno, page(pgno * 100 + txid)}]
              true -> []
            end
          end)
          |> Map.new()

        {delta, updated} = delta!(image, changes, commit, txid)
        {files ++ [delta], updated, txid + 1}
      end)

    # Compact everything at once.
    assert {:ok, iodata} = Compactor.compact(files)
    full = IO.iodata_to_binary(iodata)
    assert {:ok, database} = Decoder.decode_database(full)

    expected =
      final_image
      |> Enum.sort()
      |> Enum.map(fn {_pgno, data} -> data end)
      |> IO.iodata_to_binary()

    assert database == expected

    # Compact in two levels: [1..10] and [11..21], then compact the results.
    {first_half, second_half} = Enum.split(files, 10)
    assert {:ok, left} = Compactor.compact(first_half)
    assert {:ok, right} = Compactor.compact(second_half)

    assert {:ok, iodata2} =
             Compactor.compact([IO.iodata_to_binary(left), IO.iodata_to_binary(right)])

    assert {:ok, database2} = Decoder.decode_database(IO.iodata_to_binary(iodata2))
    assert database2 == expected

    # Compacting the compaction is a no-op on content.
    assert {:ok, iodata3} = Compactor.compact([full])
    assert {:ok, database3} = Decoder.decode_database(IO.iodata_to_binary(iodata3))
    assert database3 == expected
  end
end
