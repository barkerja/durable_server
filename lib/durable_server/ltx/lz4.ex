defmodule DurableServer.LTX.LZ4 do
  @moduledoc false

  # LZ4 *block* format (not frame format), as used by LTX v3 page frames.
  #
  # The decompressor is a full implementation of the block spec, since it must
  # read blocks produced by any conforming compressor (Go's pierrec/lz4 in
  # practice). The compressor is a greedy single-pass matcher: each 4-byte
  # substring is used directly as a map key mapping to its last-seen position
  # (the map is the hash table, collision-free by construction), matches are
  # extended with the :binary.longest_common_prefix/1 BIF, and the block
  # format's end rules are honored — no match starts within 12 bytes of the
  # end, no match extends into the last 5 bytes, and the final sequence is
  # literals-only. Incompressible input degrades to the literals-only block
  # that earlier versions always emitted, so compression never fails.

  import Bitwise

  @min_match 4
  @max_offset 65_535
  # A match may not start within 12 bytes of the end of the block.
  @match_start_margin 12
  # A match may not extend into the last 5 bytes of the block.
  @last_literals 5

  @doc """
  Compresses `data` into an LZ4 block. Never fails for non-empty input.
  """
  @spec compress_block(binary()) :: binary()
  def compress_block(data) when is_binary(data) and byte_size(data) > 0 do
    length = byte_size(data)

    if length < @match_start_margin + 1 do
      IO.iodata_to_binary(emit_literals(data, 0, length))
    else
      compress(data, length, 0, 0, %{}, [])
    end
  end

  # pos walks the input; anchor marks the start of pending literals.
  defp compress(data, length, pos, anchor, table, acc) when pos <= length - @match_start_margin do
    key = binary_part(data, pos, @min_match)

    case table do
      %{^key => candidate} when pos - candidate <= @max_offset ->
        table = Map.put(table, key, pos)
        max_match = length - @last_literals - pos

        # Comparing the two source regions directly also handles overlapping
        # matches (offset < length): the prefix length of data[candidate..]
        # vs data[pos..] is exactly the run the decoder will reproduce.
        match_length =
          :binary.longest_common_prefix([
            binary_part(data, pos, max_match),
            binary_part(data, candidate, max_match)
          ])

        if match_length >= @min_match do
          sequence =
            emit_sequence(data, anchor, pos - anchor, pos - candidate, match_length)

          compress(data, length, pos + match_length, pos + match_length, table, [
            acc | sequence
          ])
        else
          compress(data, length, pos + 1, anchor, table, acc)
        end

      _no_usable_candidate ->
        compress(data, length, pos + 1, anchor, Map.put(table, key, pos), acc)
    end
  end

  defp compress(data, length, _pos, anchor, _table, acc) do
    IO.iodata_to_binary([acc | emit_literals(data, anchor, length - anchor)])
  end

  # Final literals-only sequence (may be empty only if the whole block was
  # consumed by matches, which the end rules prevent: the last 5 bytes are
  # always literals).
  defp emit_literals(data, offset, count) do
    token_literals = min(count, 15)

    [
      <<token_literals <<< 4>>,
      extended_length(count, 15),
      binary_part(data, offset, count)
    ]
  end

  defp emit_sequence(data, literal_offset, literal_count, offset, match_length) do
    extra = match_length - @min_match
    token = bor(min(literal_count, 15) <<< 4, min(extra, 15))

    [
      <<token>>,
      extended_length(literal_count, 15),
      binary_part(data, literal_offset, literal_count),
      <<offset::16-little>>,
      extended_length(extra, 15)
    ]
  end

  # Emits the extension bytes for a length field whose nibble maxes at
  # `base`; nothing when the value fits in the nibble.
  defp extended_length(value, base) when value < base, do: []
  defp extended_length(value, base), do: encode_extended_length(value - base)

  defp encode_extended_length(n) when n >= 255, do: [255 | encode_extended_length(n - 255)]
  defp encode_extended_length(n), do: [n]

  @doc """
  Decompresses an LZ4 block, requiring the output to be exactly
  `expected_size` bytes. The bound is enforced during decompression, so a
  malicious block cannot expand past it before being rejected.
  """
  @spec decompress_block(binary(), pos_integer()) :: {:ok, binary()} | {:error, term()}
  def decompress_block(compressed, expected_size)
      when is_binary(compressed) and is_integer(expected_size) and expected_size > 0 do
    case decode_sequence(compressed, <<>>, expected_size) do
      {:ok, output} when byte_size(output) == expected_size -> {:ok, output}
      {:ok, output} -> {:error, {:lz4_size_mismatch, byte_size(output), expected_size}}
      {:error, _reason} = error -> error
    end
  end

  defp decode_sequence(<<>>, _acc, _max), do: {:error, :lz4_truncated_block}

  defp decode_sequence(<<token, rest::binary>>, acc, max) do
    with {:ok, literal_length, rest} <- read_length(token >>> 4, rest),
         {:ok, literals, rest} <- take_bytes(rest, literal_length),
         acc = <<acc::binary, literals::binary>>,
         :ok <- check_bound(acc, max) do
      case rest do
        # A block's final sequence is literals-only and ends the block.
        <<>> ->
          {:ok, acc}

        <<offset::16-little, rest::binary>> ->
          with :ok <- validate_offset(offset, acc),
               {:ok, extra, rest} <- read_length(token &&& 0x0F, rest),
               match_length = extra + 4,
               acc = copy_match(acc, offset, match_length),
               :ok <- check_bound(acc, max) do
            decode_sequence_continue(rest, acc, max)
          end

        _short ->
          {:error, :lz4_truncated_block}
      end
    end
  end

  # After a match, an empty remainder is a valid end only if the block is
  # complete; the spec's "last sequence is literals-only" rule means a block
  # never *ends* on a match, but pierrec and the reference decoder accept it
  # when the output is already complete. We accept it iff output == max
  # (checked by the caller); an empty rest here otherwise fails the size check.
  defp decode_sequence_continue(<<>>, acc, _max), do: {:ok, acc}
  defp decode_sequence_continue(rest, acc, max), do: decode_sequence(rest, acc, max)

  defp read_length(15, rest), do: read_extended_length(rest, 15)
  defp read_length(base, rest), do: {:ok, base, rest}

  defp read_extended_length(<<255, rest::binary>>, total),
    do: read_extended_length(rest, total + 255)

  defp read_extended_length(<<byte, rest::binary>>, total), do: {:ok, total + byte, rest}
  defp read_extended_length(<<>>, _total), do: {:error, :lz4_truncated_block}

  defp take_bytes(binary, count) when byte_size(binary) >= count do
    <<taken::binary-size(^count), rest::binary>> = binary
    {:ok, taken, rest}
  end

  defp take_bytes(_binary, _count), do: {:error, :lz4_truncated_block}

  defp check_bound(acc, max) when byte_size(acc) <= max, do: :ok
  defp check_bound(_acc, _max), do: {:error, :lz4_output_overflow}

  defp validate_offset(offset, acc) when offset >= 1 and offset <= byte_size(acc), do: :ok
  defp validate_offset(_offset, _acc), do: {:error, :lz4_invalid_offset}

  defp copy_match(acc, offset, length) do
    window = binary_part(acc, byte_size(acc) - offset, offset)

    copied =
      if length <= offset do
        binary_part(window, 0, length)
      else
        # Overlapping match: the window repeats.
        repeated = :binary.copy(window, div(length, offset))
        <<repeated::binary, binary_part(window, 0, rem(length, offset))::binary>>
      end

    <<acc::binary, copied::binary>>
  end
end
