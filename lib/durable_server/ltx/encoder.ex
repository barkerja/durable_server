defmodule DurableServer.LTX.Encoder do
  @moduledoc """
  Streaming encoder for LTX v3 files.

      {:ok, enc} = Encoder.new(%LTX.Header{...})
      {:ok, enc} = Encoder.encode_page(enc, pgno, page_data)
      {:ok, iodata} = Encoder.finish(enc, post_apply_checksum)

  Pages must be appended in ascending page-number order (strictly sequential
  for snapshots, skipping the lock page). The returned iodata is the complete
  file; its checksum has already been embedded in the trailer.

  Validation rules and hash coverage mirror the Go reference encoder exactly,
  so emitted files verify under the Go decoder.
  """

  import Bitwise

  alias DurableServer.LTX
  alias DurableServer.LTX.{CRC64, LZ4}

  defstruct header: nil,
            crc: nil,
            out: [],
            n: 0,
            index: %{},
            prev_pgno: 0,
            pages_written: 0,
            finished?: false

  @type t :: %__MODULE__{}

  @doc "Validates `header` and starts an encoder."
  @spec new(LTX.Header.t()) :: {:ok, t()} | {:error, term()}
  def new(%LTX.Header{} = header) do
    with :ok <- LTX.validate_header(header) do
      bytes = LTX.marshal_header(header)

      {:ok,
       %__MODULE__{
         header: header,
         crc: CRC64.update(CRC64.new(), bytes),
         out: [bytes],
         n: byte_size(bytes)
       }}
    end
  end

  @doc "Appends one page frame. `data` must be exactly `header.page_size` bytes."
  @spec encode_page(t(), LTX.pgno(), binary()) :: {:ok, t()} | {:error, term()}
  def encode_page(%__MODULE__{finished?: true}, _pgno, _data), do: {:error, :encoder_closed}

  def encode_page(%__MODULE__{} = enc, pgno, data) when is_integer(pgno) and is_binary(data) do
    %{header: header, prev_pgno: prev_pgno} = enc
    lock_pgno = LTX.lock_pgno(header.page_size)

    with :ok <- validate_page(enc, pgno, data, lock_pgno),
         :ok <- validate_page_order(header, prev_pgno, pgno, lock_pgno) do
      offset = enc.n
      compressed = LZ4.compress_block(data)

      page_header = <<pgno::32, LTX.page_header_flag_size()::16>>
      size_field = <<byte_size(compressed)::32>>

      # The file checksum covers the page header, the size field, and the
      # *uncompressed* page data — never the compressed bytes.
      crc =
        enc.crc
        |> CRC64.update(page_header)
        |> CRC64.update(size_field)
        |> CRC64.update(data)

      n = enc.n + byte_size(page_header) + byte_size(size_field) + byte_size(compressed)

      {:ok,
       %{
         enc
         | crc: crc,
           out: [enc.out, page_header, size_field, compressed],
           n: n,
           index: Map.put(enc.index, pgno, {offset, n - offset}),
           prev_pgno: pgno,
           pages_written: enc.pages_written + 1
       }}
    end
  end

  @doc """
  Terminates the page block, writes the page index and trailer, and returns
  the complete file as iodata. `post_apply_checksum` is the rolling database
  checksum after this file is applied (or `0` when the header carries the
  no-checksum flag).
  """
  @spec finish(t(), LTX.checksum()) :: {:ok, iodata()} | {:error, term()}
  def finish(%__MODULE__{finished?: true}, _post_apply_checksum), do: {:error, :encoder_closed}

  def finish(%__MODULE__{} = enc, post_apply_checksum) when is_integer(post_apply_checksum) do
    # End-of-page-block marker: an empty page header (6 zero bytes).
    enc = append_hashed(enc, <<0::48>>)

    enc = encode_page_index(enc)

    trailer = %LTX.Trailer{post_apply_checksum: post_apply_checksum}
    crc = CRC64.update(enc.crc, <<post_apply_checksum::64>>)
    file_checksum = LTX.checksum_flag() ||| CRC64.final(crc)
    trailer = %{trailer | file_checksum: file_checksum}

    with :ok <- LTX.validate_trailer(trailer, enc.header),
         :ok <- validate_deletion(enc.header, trailer) do
      {:ok, [enc.out, LTX.marshal_trailer(trailer)]}
    end
  end

  defp encode_page_index(%__MODULE__{} = enc) do
    offset = enc.n

    entries =
      enc.index
      |> Map.keys()
      |> Enum.sort()
      |> Enum.map(fn pgno ->
        {page_offset, size} = Map.fetch!(enc.index, pgno)

        [
          LTX.encode_uvarint(pgno),
          LTX.encode_uvarint(page_offset),
          LTX.encode_uvarint(size)
        ]
      end)

    enc = append_hashed(enc, IO.iodata_to_binary([entries, LTX.encode_uvarint(0)]))
    index_size = enc.n - offset
    append_hashed(enc, <<index_size::64>>)
  end

  defp append_hashed(%__MODULE__{} = enc, bytes) do
    %{
      enc
      | crc: CRC64.update(enc.crc, bytes),
        out: [enc.out, bytes],
        n: enc.n + byte_size(bytes)
    }
  end

  defp validate_page(enc, pgno, data, lock_pgno) do
    cond do
      pgno <= 0 ->
        {:error, :page_number_required}

      pgno > enc.header.commit ->
        {:error, {:pgno_out_of_bounds, pgno, enc.header.commit}}

      byte_size(data) != enc.header.page_size ->
        {:error, {:invalid_page_buffer_size, byte_size(data), enc.header.page_size}}

      pgno == lock_pgno ->
        {:error, {:cannot_encode_lock_page, pgno}}

      true ->
        :ok
    end
  end

  # Snapshots must start at page 1 and be strictly sequential, skipping the
  # lock page; non-snapshots only require strictly ascending page numbers.
  defp validate_page_order(header, prev_pgno, pgno, lock_pgno) do
    if LTX.snapshot?(header) do
      cond do
        prev_pgno == 0 and pgno != 1 ->
          {:error, {:snapshot_must_start_at_page_1, pgno}}

        prev_pgno == lock_pgno - 1 and prev_pgno != 0 and pgno != prev_pgno + 2 ->
          {:error, {:nonsequential_snapshot_pages, prev_pgno, pgno}}

        prev_pgno != lock_pgno - 1 and prev_pgno != 0 and pgno != prev_pgno + 1 ->
          {:error, {:nonsequential_snapshot_pages, prev_pgno, pgno}}

        true ->
          :ok
      end
    else
      if prev_pgno >= pgno do
        {:error, {:out_of_order_pages, prev_pgno, pgno}}
      else
        :ok
      end
    end
  end

  # A deletion LTX file (commit == 0) must carry the bare checksum flag as its
  # post-apply checksum, mirroring the Go encoder's Close check.
  defp validate_deletion(%LTX.Header{commit: 0}, %LTX.Trailer{post_apply_checksum: post})
       when post != 1 <<< 63,
       do: {:error, :deletion_post_apply_checksum_must_be_empty}

  defp validate_deletion(_header, _trailer), do: :ok
end
