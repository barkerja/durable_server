defmodule DurableServer.LTX do
  @moduledoc """
  Reader/writer for the LTX (Lite Transaction File) format, version 3.

  LTX is the page-oriented transaction file format from `superfly/ltx`, used
  by LiteFS and Litestream to ship database state to object storage as
  immutable, compactable segments. This implementation is
  wire-compatible with the Go reference implementation: files it encodes
  verify under the Go decoder and vice versa.

  An LTX file is: a 100-byte header, a block of LZ4-block-compressed page
  frames (each a 6-byte page header, a 4-byte compressed size, and the
  compressed page bytes), an empty page header terminating the block, a
  varint page index, and a 16-byte trailer carrying the post-apply database
  checksum and the CRC-64-ISO file checksum. The file checksum covers the
  *uncompressed* page bytes plus all structural bytes, so it is independent
  of compressor choice.

  See `DurableServer.LTX.Encoder` and `DurableServer.LTX.Decoder`.
  """

  import Bitwise

  alias DurableServer.LTX.CRC64

  @magic "LTX1"
  @version 3
  @header_size 100
  @page_header_size 6
  @trailer_size 16
  @checksum_size 8

  @checksum_flag 1 <<< 63
  @checksum_mask 0xFFFFFFFFFFFFFFFF

  @header_flag_no_checksum 1 <<< 1
  @header_flag_mask @header_flag_no_checksum

  @page_header_flag_size 1 <<< 0

  @pending_byte 0x40000000
  @max_page_size 65_536
  @min_page_size 512

  defmodule Header do
    @moduledoc "LTX file header. Field semantics match the Go reference struct."
    defstruct version: 3,
              flags: 0,
              page_size: nil,
              commit: nil,
              min_txid: nil,
              max_txid: nil,
              timestamp: 0,
              pre_apply_checksum: 0,
              wal_offset: 0,
              wal_size: 0,
              wal_salt1: 0,
              wal_salt2: 0,
              node_id: 0

    @type t :: %__MODULE__{}
  end

  defmodule Trailer do
    @moduledoc "LTX file trailer: post-apply database checksum + file checksum."
    defstruct post_apply_checksum: 0, file_checksum: 0

    @type t :: %__MODULE__{}
  end

  @type txid :: pos_integer()
  @type checksum :: non_neg_integer()
  @type pgno :: pos_integer()

  def magic, do: @magic
  def version, do: @version
  def header_size, do: @header_size
  def page_header_size, do: @page_header_size
  def trailer_size, do: @trailer_size
  def checksum_size, do: @checksum_size
  def checksum_flag, do: @checksum_flag
  def header_flag_no_checksum, do: @header_flag_no_checksum
  def page_header_flag_size, do: @page_header_flag_size

  @doc "True if `page_size` is a power of two between 512 and 65536."
  @spec valid_page_size?(term()) :: boolean()
  def valid_page_size?(page_size)
      when is_integer(page_size) and page_size >= @min_page_size and
             page_size <= @max_page_size,
      do: (page_size &&& page_size - 1) == 0

  def valid_page_size?(_page_size), do: false

  @doc "The SQLite lock page number for `page_size` (never written to LTX files)."
  @spec lock_pgno(pos_integer()) :: pos_integer()
  def lock_pgno(page_size), do: div(@pending_byte, page_size) + 1

  @doc "True if the header describes a snapshot (contains the initial transaction)."
  @spec snapshot?(Header.t()) :: boolean()
  def snapshot?(%Header{min_txid: 1}), do: true
  def snapshot?(%Header{}), do: false

  @doc "True if the header disables database checksum tracking."
  @spec no_checksum?(Header.t()) :: boolean()
  def no_checksum?(%Header{flags: flags}), do: (flags &&& @header_flag_no_checksum) != 0

  @doc """
  Per-page checksum: `checksum_flag ||| crc64_iso(<<pgno::32>> <> data)`.
  """
  @spec checksum_page(pgno(), binary()) :: checksum()
  def checksum_page(pgno, data) when is_integer(pgno) and is_binary(data) do
    @checksum_flag ||| CRC64.checksum(<<pgno::32, data::binary>>)
  end

  @doc """
  Folds one page into a rolling database checksum:
  `checksum_flag ||| (acc xor checksum_page(pgno, data))`. Callers must skip
  the lock page, exactly as the reference implementation does.
  """
  @spec add_page_checksum(checksum(), pgno(), binary()) :: checksum()
  def add_page_checksum(acc, pgno, data) do
    @checksum_flag ||| (bxor(acc, checksum_page(pgno, data)) &&& @checksum_mask)
  end

  @doc "Rolling checksum of a full database image given as a list/stream of `{pgno, data}`."
  @spec checksum_pages(Enumerable.t(), pos_integer()) :: checksum()
  def checksum_pages(pages, page_size) do
    lock = lock_pgno(page_size)

    Enum.reduce(pages, 0, fn
      {^lock, _data}, acc -> acc
      {pgno, data}, acc -> add_page_checksum(acc, pgno, data)
    end)
  end

  @doc "True if `[min_txid, max_txid]` is contiguous after `prev_max_txid`."
  @spec contiguous?(non_neg_integer(), txid(), txid()) :: boolean()
  def contiguous?(prev_max_txid, min_txid, max_txid) do
    min_txid <= prev_max_txid + 1 and max_txid > prev_max_txid
  end

  @doc "Formats a TXID as fixed-width lowercase hex (Go `TXID.String()`)."
  @spec format_txid(txid() | 0) :: String.t()
  def format_txid(txid) when is_integer(txid) and txid >= 0 do
    txid |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(16, "0")
  end

  @doc "Parses a 16-character hex TXID."
  @spec parse_txid(String.t()) :: {:ok, non_neg_integer()} | :error
  def parse_txid(<<string::binary-size(16)>>) do
    case Integer.parse(String.downcase(string), 16) do
      {value, ""} -> {:ok, value}
      _other -> :error
    end
  end

  def parse_txid(_other), do: :error

  @doc "LTX segment filename for a TXID range: `<min>-<max>.ltx`."
  @spec format_filename(txid(), txid()) :: String.t()
  def format_filename(min_txid, max_txid) do
    format_txid(min_txid) <> "-" <> format_txid(max_txid) <> ".ltx"
  end

  @doc "Parses `<min>-<max>.ltx` into `{:ok, min_txid, max_txid}`."
  @spec parse_filename(String.t()) :: {:ok, non_neg_integer(), non_neg_integer()} | :error
  def parse_filename(
        <<min::binary-size(16), ?-, max::binary-size(16), ".ltx"::binary>> = _name
      ) do
    with {:ok, min_txid} <- parse_txid(min),
         {:ok, max_txid} <- parse_txid(max) do
      {:ok, min_txid, max_txid}
    end
  end

  def parse_filename(_name), do: :error

  @doc "Validates a header exactly as the Go reference `Header.Validate` does."
  @spec validate_header(Header.t()) :: :ok | {:error, term()}
  def validate_header(%Header{} = header) do
    no_checksum? = no_checksum?(header)

    cond do
      header.version != @version ->
        {:error, :invalid_version}

      (header.flags &&& bnot(@header_flag_mask)) != 0 ->
        {:error, {:invalid_header_flags, header.flags}}

      not valid_page_size?(header.page_size) ->
        {:error, {:invalid_page_size, header.page_size}}

      not is_integer(header.commit) or header.commit < 0 ->
        {:error, {:invalid_commit, header.commit}}

      not is_integer(header.min_txid) or header.min_txid == 0 ->
        {:error, :min_txid_required}

      not is_integer(header.max_txid) or header.max_txid == 0 ->
        {:error, :max_txid_required}

      header.min_txid > header.max_txid ->
        {:error, {:txid_out_of_order, header.min_txid, header.max_txid}}

      header.wal_offset < 0 ->
        {:error, {:negative_wal_offset, header.wal_offset}}

      header.wal_size < 0 ->
        {:error, {:negative_wal_size, header.wal_size}}

      (header.wal_salt1 != 0 or header.wal_salt2 != 0) and header.wal_offset == 0 ->
        {:error, :wal_offset_required_with_salt}

      header.wal_offset == 0 and header.wal_size != 0 ->
        {:error, :wal_offset_required_with_size}

      snapshot?(header) and header.pre_apply_checksum != 0 ->
        {:error, :snapshot_pre_apply_checksum_must_be_zero}

      not snapshot?(header) and no_checksum? and header.pre_apply_checksum != 0 ->
        {:error, :pre_apply_checksum_not_allowed}

      not snapshot?(header) and not no_checksum? and header.pre_apply_checksum == 0 ->
        {:error, :pre_apply_checksum_required}

      not snapshot?(header) and not no_checksum? and
          (header.pre_apply_checksum &&& @checksum_flag) == 0 ->
        {:error, :invalid_pre_apply_checksum_format}

      true ->
        :ok
    end
  end

  @doc "Validates a trailer against its header (Go `Trailer.Validate`)."
  @spec validate_trailer(Trailer.t(), Header.t()) :: :ok | {:error, term()}
  def validate_trailer(%Trailer{} = trailer, %Header{} = header) do
    no_checksum? = no_checksum?(header)

    cond do
      no_checksum? and trailer.post_apply_checksum != 0 ->
        {:error, :post_apply_checksum_not_allowed}

      not no_checksum? and trailer.post_apply_checksum == 0 ->
        {:error, :post_apply_checksum_required}

      not no_checksum? and (trailer.post_apply_checksum &&& @checksum_flag) == 0 ->
        {:error, :invalid_post_apply_checksum_format}

      trailer.file_checksum == 0 ->
        {:error, :file_checksum_required}

      (trailer.file_checksum &&& @checksum_flag) == 0 ->
        {:error, :invalid_file_checksum_format}

      true ->
        :ok
    end
  end

  @doc "Encodes a header into its 100-byte wire form."
  @spec marshal_header(Header.t()) :: binary()
  def marshal_header(%Header{} = h) do
    <<@magic, h.flags::32, h.page_size::32, h.commit::32, h.min_txid::64, h.max_txid::64,
      h.timestamp::64, h.pre_apply_checksum::64, h.wal_offset::64, h.wal_size::64,
      h.wal_salt1::32, h.wal_salt2::32, h.node_id::64, 0::size(20)-unit(8)>>
  end

  @doc "Decodes a 100-byte header."
  @spec unmarshal_header(binary()) :: {:ok, Header.t()} | {:error, term()}
  def unmarshal_header(
        <<@magic, flags::32, page_size::32, commit::32, min_txid::64, max_txid::64,
          timestamp::64, pre_apply_checksum::64, wal_offset::64, wal_size::64, wal_salt1::32,
          wal_salt2::32, node_id::64, _reserved::binary-size(20)>>
      ) do
    {:ok,
     %Header{
       version: @version,
       flags: flags,
       page_size: page_size,
       commit: commit,
       min_txid: min_txid,
       max_txid: max_txid,
       timestamp: timestamp,
       pre_apply_checksum: pre_apply_checksum,
       wal_offset: wal_offset,
       wal_size: wal_size,
       wal_salt1: wal_salt1,
       wal_salt2: wal_salt2,
       node_id: node_id
     }}
  end

  def unmarshal_header(binary) when is_binary(binary) and byte_size(binary) >= @header_size,
    do: {:error, :invalid_magic}

  def unmarshal_header(binary) when is_binary(binary), do: {:error, :short_header}

  @doc "Encodes a trailer into its 16-byte wire form."
  @spec marshal_trailer(Trailer.t()) :: binary()
  def marshal_trailer(%Trailer{} = t) do
    <<t.post_apply_checksum::64, t.file_checksum::64>>
  end

  @doc "Decodes a 16-byte trailer."
  @spec unmarshal_trailer(binary()) :: {:ok, Trailer.t()} | {:error, term()}
  def unmarshal_trailer(<<post_apply_checksum::64, file_checksum::64>>) do
    {:ok, %Trailer{post_apply_checksum: post_apply_checksum, file_checksum: file_checksum}}
  end

  def unmarshal_trailer(_binary), do: {:error, :short_trailer}

  @doc "Encodes an unsigned LEB128 varint (Go `binary.AppendUvarint`)."
  @spec encode_uvarint(non_neg_integer()) :: binary()
  def encode_uvarint(value) when value < 0x80, do: <<value>>

  def encode_uvarint(value),
    do: <<(value &&& 0x7F) ||| 0x80, encode_uvarint(value >>> 7)::binary>>

  @doc "Decodes an unsigned LEB128 varint, returning `{:ok, value, rest}`."
  @spec decode_uvarint(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_uvarint(binary), do: decode_uvarint(binary, 0, 0)

  defp decode_uvarint(<<byte, rest::binary>>, shift, acc) when byte < 0x80,
    do: {:ok, acc ||| byte <<< shift, rest}

  defp decode_uvarint(<<byte, rest::binary>>, shift, acc) when shift < 63,
    do: decode_uvarint(rest, shift + 7, acc ||| (byte &&& 0x7F) <<< shift)

  defp decode_uvarint(<<_byte, _rest::binary>>, _shift, _acc), do: {:error, :uvarint_overflow}
  defp decode_uvarint(<<>>, _shift, _acc), do: {:error, :uvarint_truncated}
end
