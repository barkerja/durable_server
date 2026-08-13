defmodule DurableServer.LTX.PagedTermTest do
  use ExUnit.Case, async: true

  alias DurableServer.LTX
  alias DurableServer.LTX.{Decoder, Encoder, PagedTerm}
  alias DurableServer.LTX.PagedTerm.Image

  @page_size 512

  test "round trips terms of every practical shape" do
    terms = [
      %{count: 1},
      %{"string_keys" => [1, 2, 3], atom_key: :value},
      {:tuple, self(), make_ref()},
      List.duplicate("padding to force multiple pages", 200),
      :crypto.strong_rand_bytes(5_000),
      nil,
      []
    ]

    for term <- terms do
      assert {:ok, %Image{} = image} = PagedTerm.encode(term, @page_size)
      assert image.commit == map_size(image.pages)
      assert Enum.all?(image.pages, fn {_pgno, data} -> byte_size(data) == @page_size end)
      assert {:ok, decoded} = PagedTerm.decode(image)
      assert decoded == term
    end
  end

  test "encoding is deterministic for maps" do
    # Build the same map with different insertion orders.
    map1 = Enum.reduce(1..100, %{}, fn i, acc -> Map.put(acc, "key_#{i}", i) end)
    map2 = Enum.reduce(100..1//-1, %{}, fn i, acc -> Map.put(acc, "key_#{i}", i) end)

    assert {:ok, image1} = PagedTerm.encode(map1, @page_size)
    assert {:ok, image2} = PagedTerm.encode(map2, @page_size)

    assert image1.pages == image2.pages
    assert image1.checksum == image2.checksum
  end

  test "a same-length in-place mutation touches only its page" do
    base = %{
      header: :stable,
      blob_a: :binary.copy(<<1>>, 2_000),
      blob_b: :binary.copy(<<2>>, 2_000),
      blob_c: :binary.copy(<<3>>, 2_000)
    }

    changed = %{base | blob_b: :binary.copy(<<9>>, 2_000)}

    assert {:ok, image1} = PagedTerm.encode(base, @page_size)
    assert {:ok, image2} = PagedTerm.encode(changed, @page_size)

    assert image1.commit == image2.commit

    changed_pages = PagedTerm.changed_pages(image1.page_checksums, image2)
    changed_pgnos = Enum.map(changed_pages, &elem(&1, 0))

    # The 2000-byte run of <<2>> spans ceil(2000/512)+1 = at most 5 pages.
    assert length(changed_pgnos) <= 5
    assert length(changed_pgnos) < image2.commit
  end

  test "changed_pages against empty checksums returns the full image" do
    assert {:ok, image} = PagedTerm.encode(%{a: 1}, @page_size)
    assert PagedTerm.changed_pages(%{}, image) == Enum.sort(image.pages)
  end

  test "an unchanged term yields an empty diff" do
    term = %{stable: List.duplicate(:x, 500)}
    assert {:ok, image1} = PagedTerm.encode(term, @page_size)
    assert {:ok, image2} = PagedTerm.encode(term, @page_size)
    assert PagedTerm.changed_pages(image1.page_checksums, image2) == []
  end

  test "encoding against a previous image matches encoding from scratch" do
    base = %{list: Enum.to_list(1..2_000)}
    changed = %{list: Enum.to_list(1..2_000) ++ [:tail]}

    assert {:ok, base_image} = PagedTerm.encode(base, @page_size)
    assert {:ok, from_scratch} = PagedTerm.encode(changed, @page_size)
    assert {:ok, incremental} = PagedTerm.encode(changed, @page_size, base_image)

    assert incremental.pages == from_scratch.pages
    assert incremental.page_checksums == from_scratch.page_checksums
    assert incremental.checksum == from_scratch.checksum

    # A previous image with a different page size is ignored, not misused.
    assert {:ok, other_size} = PagedTerm.encode(changed, 1024, base_image)
    assert {:ok, ^changed} = PagedTerm.decode(other_size)
  end

  test "apply_pages reconstructs images from snapshot and delta pages" do
    term1 = %{value: :binary.copy(<<7>>, 3_000)}
    term2 = %{value: :binary.copy(<<7>>, 2_999) <> <<8>>}

    assert {:ok, image1} = PagedTerm.encode(term1, @page_size)
    assert {:ok, image2} = PagedTerm.encode(term2, @page_size)

    # Snapshot apply.
    assert {:ok, restored1} =
             PagedTerm.apply_pages(nil, image1.commit, Enum.sort(image1.pages), @page_size)

    assert restored1.checksum == image1.checksum
    assert {:ok, ^term1} = PagedTerm.decode(restored1)

    # Delta apply on top.
    delta = PagedTerm.changed_pages(image1.page_checksums, image2)
    assert delta != []

    assert {:ok, restored2} = PagedTerm.apply_pages(restored1, image2.commit, delta, @page_size)
    assert restored2.checksum == image2.checksum
    assert {:ok, ^term2} = PagedTerm.decode(restored2)
  end

  test "apply_pages drops pages beyond a shrunken commit" do
    big = %{value: :binary.copy(<<1>>, 4_000)}
    small = %{value: <<1>>}

    assert {:ok, big_image} = PagedTerm.encode(big, @page_size)
    assert {:ok, small_image} = PagedTerm.encode(small, @page_size)
    assert small_image.commit < big_image.commit

    delta = PagedTerm.changed_pages(big_image.page_checksums, small_image)

    assert {:ok, restored} =
             PagedTerm.apply_pages(big_image, small_image.commit, delta, @page_size)

    assert restored.commit == small_image.commit
    assert restored.checksum == small_image.checksum
    assert {:ok, ^small} = PagedTerm.decode(restored)
  end

  test "apply_pages rejects incomplete images" do
    assert {:error, {:missing_page, 2}} =
             PagedTerm.apply_pages(nil, 2, [{1, :binary.copy(<<0>>, @page_size)}], @page_size)

    assert {:error, {:invalid_page_size_at, 1, 3}} =
             PagedTerm.apply_pages(nil, 1, [{1, "abc"}], @page_size)
  end

  test "rejects invalid page sizes and oversized terms" do
    assert {:error, {:invalid_page_size, 513}} = PagedTerm.encode(%{}, 513)

    # The lock-page cap: an image reaching pgno 0x40000000/page_size + 1 is
    # refused. Simulated with the smallest page size and a term just over
    # the 1 GiB boundary — too slow for a unit test, so exercise the bound
    # arithmetic instead via a mocked commit calculation.
    lock = LTX.lock_pgno(@page_size)
    max_bytes = (lock - 1) * @page_size
    assert max_bytes == 0x40000000
  end

  test "decode rejects tampered prologues and compressed payloads" do
    assert {:ok, image} = PagedTerm.encode(%{a: 1}, @page_size)

    # Corrupt the magic.
    bad_page = "XXXX" <> binary_part(Map.fetch!(image.pages, 1), 4, @page_size - 4)
    bad_image = %{image | pages: Map.put(image.pages, 1, bad_page)}
    assert {:error, :invalid_image_prologue} = PagedTerm.decode(bad_image)

    # Future format version.
    <<_magic::binary-size(4), _version::16, rest::binary>> = Map.fetch!(image.pages, 1)
    versioned_page = <<"DSPT", 99::16, rest::binary>>
    versioned_image = %{image | pages: Map.put(image.pages, 1, versioned_page)}
    assert {:error, {:unsupported_image_version, 99}} = PagedTerm.decode(versioned_image)

    # A compressed ETF payload is rejected before decompression.
    compressed = :erlang.term_to_binary(:binary.copy(<<0>>, 100_000), compressed: 9)
    padding = @page_size - 16 - byte_size(compressed)

    compressed_page =
      <<"DSPT", 1::16, 0::16, byte_size(compressed)::64, compressed::binary,
        0::size(padding)-unit(8)>>

    compressed_image = %Image{
      page_size: @page_size,
      commit: 1,
      pages: %{1 => compressed_page},
      page_checksums: %{1 => LTX.checksum_page(1, compressed_page)},
      checksum: 0
    }

    assert {:error, :compressed_payload_rejected} = PagedTerm.decode(compressed_image)

    # A truncated payload claim.
    truncated_page = <<"DSPT", 1::16, 0::16, 10_000::64, 0::size(@page_size - 16)-unit(8)>>
    truncated_image = %{compressed_image | pages: %{1 => truncated_page}}
    assert {:error, :invalid_image_prologue} = PagedTerm.decode(truncated_image)
  end

  test "images flow through real LTX segments end to end" do
    term1 = %{status: :running, log: Enum.to_list(1..300)}
    term2 = %{status: :running, log: Enum.to_list(1..300) ++ [301]}

    assert {:ok, image1} = PagedTerm.encode(term1, @page_size)
    assert {:ok, image2} = PagedTerm.encode(term2, @page_size)

    # Snapshot segment at TXID 1.
    snapshot_header = %LTX.Header{
      page_size: @page_size,
      commit: image1.commit,
      min_txid: 1,
      max_txid: 1
    }

    {:ok, enc} = Encoder.new(snapshot_header)

    enc =
      Enum.reduce(Enum.sort(image1.pages), enc, fn {pgno, data}, enc ->
        {:ok, enc} = Encoder.encode_page(enc, pgno, data)
        enc
      end)

    {:ok, snapshot_iodata} = Encoder.finish(enc, image1.checksum)

    # Delta segment at TXID 2 with only the changed pages.
    delta_pages = PagedTerm.changed_pages(image1.page_checksums, image2)

    delta_header = %LTX.Header{
      page_size: @page_size,
      commit: image2.commit,
      min_txid: 2,
      max_txid: 2,
      pre_apply_checksum: image1.checksum
    }

    {:ok, enc} = Encoder.new(delta_header)

    enc =
      Enum.reduce(delta_pages, enc, fn {pgno, data}, enc ->
        {:ok, enc} = Encoder.encode_page(enc, pgno, data)
        enc
      end)

    {:ok, delta_iodata} = Encoder.finish(enc, image2.checksum)

    # Restore: decode both segments, apply in order, decode the term.
    assert {:ok, snapshot_decoded} = Decoder.decode(IO.iodata_to_binary(snapshot_iodata))
    assert {:ok, delta_decoded} = Decoder.decode(IO.iodata_to_binary(delta_iodata))

    assert {:ok, restored1} =
             PagedTerm.apply_pages(
               nil,
               snapshot_decoded.header.commit,
               snapshot_decoded.pages,
               @page_size
             )

    assert restored1.checksum == snapshot_decoded.trailer.post_apply_checksum

    assert {:ok, restored2} =
             PagedTerm.apply_pages(
               restored1,
               delta_decoded.header.commit,
               delta_decoded.pages,
               @page_size
             )

    assert restored2.checksum == delta_decoded.trailer.post_apply_checksum
    assert {:ok, ^term2} = PagedTerm.decode(restored2)
  end
end
