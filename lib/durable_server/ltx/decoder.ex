defmodule DurableServer.LTX.Decoder do
  @moduledoc """
  Decoder/verifier for LTX v3 files.

      {:ok, decoded} = Decoder.decode(binary)
      decoded.header       #=> %LTX.Header{}
      decoded.trailer      #=> %LTX.Trailer{}
      decoded.pages        #=> [{pgno, page_data}] in file order
      decoded.page_index   #=> %{pgno => {offset, size}}

  `decode/1` verifies the file checksum and — for snapshot files with
  checksum tracking — the post-apply database checksum, exactly as the Go
  reference decoder's `Verify` does. Anything that fails verification is
  rejected; no partially decoded data is returned.

  Only the v3 block-compressed page format (`PageHeaderFlagSize`) is
  supported. Pages written by pre-v3 encoders in LZ4 *frame* format are
  rejected with `{:error, :unsupported_page_encoding}`.
  """

  import Bitwise

  alias DurableServer.LTX
  alias DurableServer.LTX.{CRC64, LZ4}

  @type decoded :: %{
          header: LTX.Header.t(),
          trailer: LTX.Trailer.t(),
          pages: [{LTX.pgno(), binary()}],
          page_index: %{LTX.pgno() => {non_neg_integer(), pos_integer()}}
        }

  @doc "Decodes and fully verifies an LTX file."
  @spec decode(binary()) :: {:ok, decoded()} | {:error, term()}
  def decode(binary) when is_binary(binary) do
    header_size = LTX.header_size()

    with {:ok, header} <- take_header(binary),
         :ok <- LTX.validate_header(header),
         <<header_bytes::binary-size(^header_size), rest::binary>> = binary,
         crc = CRC64.update(CRC64.new(), header_bytes),
         {:ok, pages, db_checksum, crc, rest} <- decode_pages(header, rest, crc),
         {:ok, page_index, crc, rest} <- decode_page_index(rest, crc),
         {:ok, trailer, crc, rest} <- decode_trailer(rest, crc),
         :ok <- expect_eof(rest),
         :ok <- LTX.validate_trailer(trailer, header),
         :ok <- verify_file_checksum(trailer, crc),
         :ok <- verify_snapshot_checksum(header, trailer, db_checksum) do
      {:ok, %{header: header, trailer: trailer, pages: pages, page_index: page_index}}
    end
  end

  @doc "Verifies an LTX file, discarding decoded data."
  @spec verify(binary()) :: :ok | {:error, term()}
  def verify(binary) do
    with {:ok, _decoded} <- decode(binary), do: :ok
  end

  @doc """
  Materializes a snapshot LTX file into a raw database image (the
  concatenation of pages 1..commit, with a zero lock page where applicable).
  """
  @spec decode_database(binary()) :: {:ok, binary()} | {:error, term()}
  def decode_database(binary) do
    with {:ok, %{header: header, pages: pages}} <- decode(binary) do
      if LTX.snapshot?(header) do
        build_database(header, pages)
      else
        {:error, :not_a_snapshot}
      end
    end
  end

  defp take_header(binary) when byte_size(binary) < 100, do: {:error, :short_header}

  defp take_header(binary) do
    binary |> binary_part(0, LTX.header_size()) |> LTX.unmarshal_header()
  end

  defp decode_pages(header, rest, crc) do
    initial_db_checksum =
      if LTX.no_checksum?(header), do: nil, else: LTX.checksum_flag()

    do_decode_pages(header, rest, crc, [], initial_db_checksum, 0)
  end

  defp do_decode_pages(header, rest, crc, pages, db_checksum, prev_pgno) do
    case rest do
      # Empty page header: end of the page block.
      <<0::48, rest::binary>> ->
        crc = CRC64.update(crc, <<0::48>>)
        {:ok, Enum.reverse(pages), db_checksum, crc, rest}

      <<pgno::32, flags::16, rest::binary>> ->
        with :ok <- validate_page_header(header, pgno, flags, prev_pgno),
             {:ok, {data, compressed_size}, rest} <- decode_page_data(header, rest) do
          # The file checksum covers the page header, the size field, and the
          # *uncompressed* page bytes — mirroring the encoder.
          crc =
            crc
            |> CRC64.update(<<pgno::32, flags::16>>)
            |> CRC64.update(<<compressed_size::32>>)
            |> CRC64.update(data)

          db_checksum = roll_db_checksum(header, db_checksum, pgno, data)

          do_decode_pages(header, rest, crc, [{pgno, data} | pages], db_checksum, pgno)
        end

      _short ->
        {:error, :truncated_page_block}
    end
  end

  defp roll_db_checksum(_header, nil, _pgno, _data), do: nil

  defp roll_db_checksum(header, db_checksum, pgno, data) do
    if LTX.snapshot?(header) and pgno != LTX.lock_pgno(header.page_size) do
      LTX.add_page_checksum(db_checksum, pgno, data)
    else
      db_checksum
    end
  end

  defp validate_page_header(header, pgno, flags, prev_pgno) do
    cond do
      pgno == 0 ->
        {:error, :page_number_required}

      (flags &&& bnot(LTX.page_header_flag_size())) != 0 ->
        {:error, {:invalid_page_header_flags, flags}}

      (flags &&& LTX.page_header_flag_size()) == 0 ->
        {:error, :unsupported_page_encoding}

      pgno <= prev_pgno ->
        {:error, {:out_of_order_pages, prev_pgno, pgno}}

      pgno > header.commit ->
        {:error, {:pgno_out_of_bounds, pgno, header.commit}}

      true ->
        :ok
    end
  end

  defp decode_page_data(header, <<size::32, rest::binary>>) when byte_size(rest) >= size do
    <<compressed::binary-size(^size), rest::binary>> = rest

    with {:ok, data} <- LZ4.decompress_block(compressed, header.page_size) do
      {:ok, {data, size}, rest}
    end
  end

  defp decode_page_data(_header, _rest), do: {:error, :truncated_page_block}

  defp decode_page_index(rest, crc), do: do_decode_page_index(rest, crc, %{}, 0)

  defp do_decode_page_index(rest, crc, index, consumed) do
    case LTX.decode_uvarint(rest) do
      {:ok, 0, after_marker} ->
        marker_size = byte_size(rest) - byte_size(after_marker)
        crc = CRC64.update(crc, binary_part(rest, 0, marker_size))
        consumed = consumed + marker_size

        case after_marker do
          <<index_size::64, rest::binary>> ->
            if index_size == consumed do
              {:ok, index, CRC64.update(crc, <<index_size::64>>), rest}
            else
              {:error, {:page_index_size_mismatch, index_size, consumed}}
            end

          _short ->
            {:error, :truncated_page_index}
        end

      {:ok, pgno, rest_after_pgno} ->
        with {:ok, offset, rest_after_offset} <- LTX.decode_uvarint(rest_after_pgno),
             {:ok, size, rest_after_size} <- LTX.decode_uvarint(rest_after_offset) do
          entry_size = byte_size(rest) - byte_size(rest_after_size)
          crc = CRC64.update(crc, binary_part(rest, 0, entry_size))

          do_decode_page_index(
            rest_after_size,
            crc,
            Map.put(index, pgno, {offset, size}),
            consumed + entry_size
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_trailer(<<post_apply::64, file_checksum::64, rest::binary>>, crc) do
    crc = CRC64.update(crc, <<post_apply::64>>)

    {:ok, %LTX.Trailer{post_apply_checksum: post_apply, file_checksum: file_checksum}, crc,
     rest}
  end

  defp decode_trailer(_binary, _crc), do: {:error, :truncated_trailer}

  defp expect_eof(<<>>), do: :ok
  defp expect_eof(rest), do: {:error, {:trailing_bytes, byte_size(rest)}}

  defp verify_file_checksum(trailer, crc) do
    if (LTX.checksum_flag() ||| CRC64.final(crc)) == trailer.file_checksum do
      :ok
    else
      {:error, :file_checksum_mismatch}
    end
  end

  defp verify_snapshot_checksum(header, trailer, db_checksum) do
    if LTX.snapshot?(header) and not LTX.no_checksum?(header) and
         trailer.post_apply_checksum != db_checksum do
      {:error, :post_apply_checksum_mismatch}
    else
      :ok
    end
  end

  defp build_database(header, pages) do
    lock_pgno = LTX.lock_pgno(header.page_size)
    zero_page = :binary.copy(<<0>>, header.page_size)
    page_map = Map.new(pages)

    result =
      Enum.reduce_while(1..max(header.commit, 0)//1, [], fn pgno, acc ->
        cond do
          pgno == lock_pgno ->
            {:cont, [acc, zero_page]}

          data = Map.get(page_map, pgno) ->
            {:cont, [acc, data]}

          true ->
            {:halt, {:error, {:missing_snapshot_page, pgno}}}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      iodata -> {:ok, IO.iodata_to_binary(iodata)}
    end
  end
end
