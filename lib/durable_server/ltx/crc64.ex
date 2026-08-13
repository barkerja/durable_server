defmodule DurableServer.LTX.CRC64 do
  @moduledoc false

  # CRC-64/GO-ISO, bit-for-bit compatible with Go's hash/crc64 using
  # crc64.MakeTable(crc64.ISO), which is what superfly/ltx uses for both the
  # file checksum and page checksums. Parameters: poly 0x000000000000001B
  # (reflected: 0xD800000000000000), init 0xFFFFFFFFFFFFFFFF, refin/refout
  # true, xorout 0xFFFFFFFFFFFFFFFF. Known-answer: crc("123456789") =
  # 0xB90956C775A41001 (asserted in tests).

  import Bitwise

  @mask 0xFFFFFFFFFFFFFFFF
  @reflected_poly 0xD800000000000000

  table =
    for index <- 0..255 do
      Enum.reduce(1..8, index, fn _bit, crc ->
        if (crc &&& 1) == 1 do
          bxor(crc >>> 1, @reflected_poly)
        else
          crc >>> 1
        end
      end)
    end

  @table List.to_tuple(table)

  @type state :: non_neg_integer()

  @doc "Returns a fresh hash state (the pre-inverted internal register)."
  @spec new() :: state()
  def new, do: @mask

  @doc "Folds `binary` into the hash state."
  @spec update(state(), binary()) :: state()
  def update(state, binary) when is_integer(state) and is_binary(binary) do
    do_update(state, binary)
  end

  defp do_update(state, <<>>), do: state

  defp do_update(state, <<byte, rest::binary>>) do
    do_update(bxor(elem(@table, bxor(state, byte) &&& 0xFF), state >>> 8), rest)
  end

  @doc "Finalizes the state into the externally visible checksum value."
  @spec final(state()) :: non_neg_integer()
  def final(state), do: bnot(state) &&& @mask

  @doc "One-shot checksum of `binary`."
  @spec checksum(binary()) :: non_neg_integer()
  def checksum(binary), do: new() |> update(binary) |> final()
end
