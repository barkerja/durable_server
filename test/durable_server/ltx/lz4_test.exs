defmodule DurableServer.LTX.LZ4Test do
  use ExUnit.Case, async: true

  alias DurableServer.LTX.LZ4

  test "round trips short data" do
    data = "hello"
    assert {:ok, ^data} = data |> LZ4.compress_block() |> LZ4.decompress_block(byte_size(data))
  end

  test "round trips data requiring extended literal lengths" do
    for size <- [15, 16, 269, 270, 271, 4096, 65_536] do
      data = :crypto.strong_rand_bytes(size)
      assert {:ok, ^data} = data |> LZ4.compress_block() |> LZ4.decompress_block(size)
    end
  end

  test "round trips adversarial and structured inputs" do
    inputs = [
      # Uniform run (maximally overlapping matches).
      :binary.copy(<<0>>, 4096),
      # Short repeating period.
      :binary.copy("ab", 2048),
      # Period longer than min-match.
      :binary.copy("0123456789abcdef", 256),
      # Repetition starting exactly at the match-start margin boundary.
      :crypto.strong_rand_bytes(100) <> :binary.copy(<<7>>, 12),
      # Random head, compressible tail, random end.
      :crypto.strong_rand_bytes(500) <>
        :binary.copy("pattern!", 400) <> :crypto.strong_rand_bytes(500),
      # Match candidates further apart than the 65535 offset cap.
      "needle" <> :crypto.strong_rand_bytes(70_000) <> "needle" <> <<0::40>>,
      # Every byte value.
      IO.iodata_to_binary(Enum.map(0..255, &<<&1>>)),
      # Realistic page: deterministic term encoding.
      :erlang.term_to_binary(Enum.to_list(1..1_000), [:deterministic])
    ]

    for data <- inputs do
      size = byte_size(data)
      assert {:ok, ^data} = data |> LZ4.compress_block() |> LZ4.decompress_block(size)
    end
  end

  test "actually compresses repetitive data" do
    run = :binary.copy(<<0>>, 4096)
    assert byte_size(LZ4.compress_block(run)) < 64

    period = :binary.copy("0123456789abcdef", 256)
    assert byte_size(LZ4.compress_block(period)) < 256

    # A zero-padded page tail (the PagedTerm padding case) compresses away.
    padded = :crypto.strong_rand_bytes(1_000) <> :binary.copy(<<0>>, 3_096)
    assert byte_size(LZ4.compress_block(padded)) < 1_100
  end

  test "incompressible data expands only by the literal framing overhead" do
    data = :crypto.strong_rand_bytes(4096)
    compressed = LZ4.compress_block(data)
    # Token + extended length bytes: 1 + ceil((4096-15)/255) = 18 bytes.
    assert byte_size(compressed) <= 4096 + 20
  end

  test "decompresses a handcrafted block containing an overlapping match" do
    # 4 literals "abcd", then a match at offset 4 of length 396 = full 400
    # bytes of "abcd" repeated. Match length 396 = 4 + (15 ext) + 255 + 122.
    block = <<0x4F, "abcd", 4::16-little, 255, 122>>
    expected = :binary.copy("abcd", 100)

    assert {:ok, ^expected} = LZ4.decompress_block(block, 400)
  end

  test "decompresses a handcrafted block with a non-overlapping match" do
    # 8 literals "abcdefgh", match offset 8 length 8, then 2 trailing literals "xy".
    block = <<0x84, "abcdefgh", 8::16-little, 0x20, "xy">>
    assert {:ok, "abcdefghabcdefghxy"} = LZ4.decompress_block(block, 18)
  end

  test "rejects a match offset beyond the produced output" do
    block = <<0x14, "a", 5::16-little>>
    assert {:error, :lz4_invalid_offset} = LZ4.decompress_block(block, 10)
  end

  test "rejects a zero match offset" do
    block = <<0x14, "a", 0::16-little>>
    assert {:error, :lz4_invalid_offset} = LZ4.decompress_block(block, 10)
  end

  test "rejects truncated blocks" do
    assert {:error, :lz4_truncated_block} = LZ4.decompress_block(<<0xF0>>, 100)
    assert {:error, :lz4_truncated_block} = LZ4.decompress_block(<<0x50, "ab">>, 100)
  end

  test "rejects output exceeding the expected size before inflating it" do
    # 1 literal then a match that would expand far beyond the 8-byte bound.
    block = <<0x1F, "a", 1::16-little, 255, 255, 255, 0>>
    assert {:error, :lz4_output_overflow} = LZ4.decompress_block(block, 8)
  end

  test "rejects output smaller than the expected size" do
    block = LZ4.compress_block("abc")
    assert {:error, {:lz4_size_mismatch, 3, 4}} = LZ4.decompress_block(block, 4)
  end
end
