defmodule DurableServer.LTX.CodecTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias DurableServer.LTX
  alias DurableServer.LTX.{Decoder, Encoder}

  @checksum_flag 1 <<< 63

  defp page(pgno, page_size) do
    for i <- 0..(page_size - 1), into: <<>>, do: <<rem(pgno * 31 + div(i, 16) * 7, 251)>>
  end

  defp encode!(header, pages, post_apply_checksum) do
    {:ok, enc} = Encoder.new(header)

    enc =
      Enum.reduce(pages, enc, fn {pgno, data}, enc ->
        {:ok, enc} = Encoder.encode_page(enc, pgno, data)
        enc
      end)

    {:ok, iodata} = Encoder.finish(enc, post_apply_checksum)
    IO.iodata_to_binary(iodata)
  end

  test "round trips a snapshot file" do
    page_size = 512
    pages = for pgno <- 1..3, do: {pgno, page(pgno, page_size)}
    post_apply = LTX.checksum_pages(pages, page_size)

    header = %LTX.Header{
      page_size: page_size,
      commit: 3,
      min_txid: 1,
      max_txid: 1,
      timestamp: 1_700_000_000_000
    }

    binary = encode!(header, pages, post_apply)

    assert {:ok, decoded} = Decoder.decode(binary)
    assert decoded.header.page_size == page_size
    assert decoded.header.commit == 3
    assert decoded.header.min_txid == 1
    assert decoded.header.max_txid == 1
    assert decoded.pages == pages
    assert decoded.trailer.post_apply_checksum == post_apply
    assert map_size(decoded.page_index) == 3

    assert {:ok, database} = Decoder.decode_database(binary)
    assert database == IO.iodata_to_binary(Enum.map(pages, fn {_pgno, data} -> data end))
  end

  test "round trips a non-snapshot delta file" do
    page_size = 1024
    base = for pgno <- 1..6, do: {pgno, page(pgno, page_size)}
    pre_apply = LTX.checksum_pages(base, page_size)

    changed = [{2, page(102, page_size)}, {5, page(105, page_size)}]

    updated =
      Enum.map(base, fn {pgno, data} ->
        {pgno, :proplists.get_value(pgno, changed, data)}
      end)

    post_apply = LTX.checksum_pages(updated, page_size)

    header = %LTX.Header{
      page_size: page_size,
      commit: 6,
      min_txid: 2,
      max_txid: 3,
      timestamp: 1_700_000_000_000,
      pre_apply_checksum: pre_apply
    }

    binary = encode!(header, changed, post_apply)

    assert {:ok, decoded} = Decoder.decode(binary)
    assert decoded.pages == changed
    assert decoded.header.pre_apply_checksum == pre_apply
    assert decoded.trailer.post_apply_checksum == post_apply
    assert {:error, :not_a_snapshot} = Decoder.decode_database(binary)
  end

  test "round trips a deletion file (commit 0, zero pages)" do
    header = %LTX.Header{
      page_size: 4096,
      commit: 0,
      min_txid: 7,
      max_txid: 7,
      pre_apply_checksum: @checksum_flag ||| 0x1234
    }

    binary = encode!(header, [], @checksum_flag)

    assert {:ok, decoded} = Decoder.decode(binary)
    assert decoded.pages == []
    assert decoded.header.commit == 0
    assert decoded.trailer.post_apply_checksum == @checksum_flag
  end

  test "rejects a deletion file whose post-apply checksum is not the bare flag" do
    header = %LTX.Header{
      page_size: 4096,
      commit: 0,
      min_txid: 7,
      max_txid: 7,
      pre_apply_checksum: @checksum_flag ||| 0x1234
    }

    {:ok, enc} = Encoder.new(header)

    assert {:error, :deletion_post_apply_checksum_must_be_empty} =
             Encoder.finish(enc, @checksum_flag ||| 0x9999)
  end

  test "round trips a file with checksum tracking disabled" do
    page_size = 512
    pages = [{3, page(3, page_size)}, {4, page(4, page_size)}]

    header = %LTX.Header{
      flags: LTX.header_flag_no_checksum(),
      page_size: page_size,
      commit: 8,
      min_txid: 3,
      max_txid: 4
    }

    binary = encode!(header, pages, 0)

    assert {:ok, decoded} = Decoder.decode(binary)
    assert decoded.pages == pages
    assert decoded.trailer.post_apply_checksum == 0
    assert LTX.no_checksum?(decoded.header)
  end

  test "header validation rejects invalid shapes" do
    valid = %LTX.Header{page_size: 512, commit: 1, min_txid: 1, max_txid: 1}
    assert :ok = LTX.validate_header(valid)

    assert {:error, {:invalid_page_size, 513}} =
             LTX.validate_header(%{valid | page_size: 513})

    assert {:error, :min_txid_required} = LTX.validate_header(%{valid | min_txid: 0})

    assert {:error, {:txid_out_of_order, 3, 2}} =
             LTX.validate_header(%{valid | min_txid: 3, max_txid: 2})

    assert {:error, :snapshot_pre_apply_checksum_must_be_zero} =
             LTX.validate_header(%{valid | pre_apply_checksum: @checksum_flag ||| 1})

    assert {:error, :pre_apply_checksum_required} =
             LTX.validate_header(%{valid | min_txid: 2, max_txid: 2})

    assert {:error, :invalid_pre_apply_checksum_format} =
             LTX.validate_header(%{valid | min_txid: 2, max_txid: 2, pre_apply_checksum: 7})

    assert {:error, :wal_offset_required_with_salt} =
             LTX.validate_header(%{valid | wal_salt1: 5})

    assert {:error, {:invalid_header_flags, 0xFF}} = LTX.validate_header(%{valid | flags: 0xFF})
  end

  test "encoder enforces page ordering and bounds" do
    page_size = 512
    data = page(1, page_size)

    snapshot = %LTX.Header{page_size: page_size, commit: 4, min_txid: 1, max_txid: 1}
    {:ok, enc} = Encoder.new(snapshot)

    assert {:error, {:snapshot_must_start_at_page_1, 2}} = Encoder.encode_page(enc, 2, data)

    {:ok, enc} = Encoder.encode_page(enc, 1, data)
    assert {:error, {:nonsequential_snapshot_pages, 1, 3}} = Encoder.encode_page(enc, 3, data)

    delta = %LTX.Header{
      page_size: page_size,
      commit: 4,
      min_txid: 2,
      max_txid: 2,
      pre_apply_checksum: @checksum_flag ||| 1
    }

    {:ok, enc} = Encoder.new(delta) |> then(fn {:ok, e} -> Encoder.encode_page(e, 3, data) end)
    assert {:error, {:out_of_order_pages, 3, 3}} = Encoder.encode_page(enc, 3, data)
    assert {:error, {:out_of_order_pages, 3, 2}} = Encoder.encode_page(enc, 2, data)
    assert {:error, {:pgno_out_of_bounds, 5, 4}} = Encoder.encode_page(enc, 5, data)

    assert {:error, {:invalid_page_buffer_size, 3, 512}} =
             Encoder.encode_page(enc, 4, "abc")
  end

  test "encoder refuses the lock page" do
    page_size = 512
    lock_pgno = LTX.lock_pgno(page_size)

    header = %LTX.Header{
      page_size: page_size,
      commit: lock_pgno,
      min_txid: 2,
      max_txid: 2,
      pre_apply_checksum: @checksum_flag ||| 1
    }

    {:ok, enc} = Encoder.new(header)

    assert {:error, {:cannot_encode_lock_page, ^lock_pgno}} =
             Encoder.encode_page(enc, lock_pgno, page(1, page_size))
  end

  test "decoder rejects corruption, truncation, and trailing bytes" do
    page_size = 512
    pages = for pgno <- 1..2, do: {pgno, page(pgno, page_size)}

    header = %LTX.Header{page_size: page_size, commit: 2, min_txid: 1, max_txid: 1}
    binary = encode!(header, pages, LTX.checksum_pages(pages, page_size))

    assert :ok = Decoder.verify(binary)

    # Flip one byte inside the page block.
    flip_at = LTX.header_size() + 20
    <<prefix::binary-size(^flip_at), byte, rest::binary>> = binary
    corrupted = <<prefix::binary, bxor(byte, 0x01), rest::binary>>
    assert {:error, _reason} = Decoder.decode(corrupted)

    # Truncate the trailer.
    truncated = binary_part(binary, 0, byte_size(binary) - 4)
    assert {:error, _reason} = Decoder.decode(truncated)

    # Trailing garbage.
    assert {:error, {:trailing_bytes, 3}} = Decoder.decode(binary <> "xyz")

    # Bad magic.
    <<_magic::binary-size(4), rest::binary>> = binary
    assert {:error, :invalid_magic} = Decoder.decode("NOPE" <> rest)
  end

  test "decoder rejects a snapshot whose post-apply checksum is wrong" do
    page_size = 512
    pages = [{1, page(1, page_size)}]
    header = %LTX.Header{page_size: page_size, commit: 1, min_txid: 1, max_txid: 1}

    binary = encode!(header, pages, @checksum_flag ||| 0xDEAD)
    assert {:error, :post_apply_checksum_mismatch} = Decoder.decode(binary)
  end

  test "txid and filename formatting round trip" do
    assert LTX.format_txid(0x1234ABCD) == "000000001234abcd"
    assert {:ok, 0x1234ABCD} = LTX.parse_txid("000000001234abcd")
    assert LTX.format_filename(1, 0xFF) == "0000000000000001-00000000000000ff.ltx"
    assert {:ok, 1, 255} = LTX.parse_filename("0000000000000001-00000000000000ff.ltx")
    assert :error = LTX.parse_filename("nope.ltx")
  end

  test "contiguity check matches the reference semantics" do
    assert LTX.contiguous?(5, 6, 7)
    assert LTX.contiguous?(5, 3, 7)
    refute LTX.contiguous?(5, 7, 8)
    refute LTX.contiguous?(5, 3, 5)
  end
end
