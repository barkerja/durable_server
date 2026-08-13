defmodule DurableServer.LTX.CRC64Test do
  use ExUnit.Case, async: true

  alias DurableServer.LTX.CRC64

  test "matches the CRC-64/GO-ISO known-answer vector" do
    # The canonical check value for CRC-64/GO-ISO (Go's crc64.MakeTable(crc64.ISO)).
    assert CRC64.checksum("123456789") == 0xB90956C775A41001
  end

  test "empty input hashes to zero" do
    assert CRC64.checksum(<<>>) == 0
  end

  test "incremental updates equal one-shot hashing" do
    data = :crypto.strong_rand_bytes(10_000)

    incremental =
      data
      |> :binary.bin_to_list()
      |> Enum.chunk_every(313)
      |> Enum.map(&:binary.list_to_bin/1)
      |> Enum.reduce(CRC64.new(), &CRC64.update(&2, &1))
      |> CRC64.final()

    assert incremental == CRC64.checksum(data)
  end

  test "is sensitive to every byte position" do
    base = :binary.copy(<<0xAB>>, 64)

    checksums =
      for index <- 0..63 do
        <<prefix::binary-size(^index), byte, rest::binary>> = base
        CRC64.checksum(<<prefix::binary, Bitwise.bxor(byte, 1), rest::binary>>)
      end

    assert length(Enum.uniq([CRC64.checksum(base) | checksums])) == 65
  end
end
