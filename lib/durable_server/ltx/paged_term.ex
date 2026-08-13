defmodule DurableServer.LTX.PagedTerm do
  @moduledoc """
  Maps arbitrary Elixir terms onto fixed-size page images so they can be
  shipped as LTX segments and diffed page-by-page between versions.

  A term is encoded with `:erlang.term_to_binary(term, [:deterministic])`,
  prefixed with a 16-byte prologue (magic, format version, payload size), and
  zero-padded to a whole number of pages. Page numbers follow the LTX/SQLite
  convention (1-based); an image large enough to reach the SQLite lock page
  (1 GiB) is rejected rather than emulating SQLite's zero-filled lock page.

  `changed_pages/2` diffs a new image against the previous image's per-page
  checksums, so a writer only ships pages whose bytes changed. Because the
  diff operates on the encoded byte stream, its efficiency depends on byte
  stability: a same-length in-place mutation touches one page, while a
  length-changing mutation shifts and rewrites every page after the edit
  point. The M3 benchmarks quantify this.

  Decoding mirrors `DurableServer.Backends.EncryptedStore`'s hardening: the
  ETF compression tag is rejected before `binary_to_term/1` runs (this module
  never emits compressed ETF, and a small compressed blob can expand by
  orders of magnitude), and `:safe` is deliberately not used because
  persisted values carry pids/refs whose ETF embeds originating node atoms —
  see the equivalent note in the encrypted backend.
  """

  import Bitwise

  alias DurableServer.LTX

  @magic "DSPT"
  @format_version 1
  @prologue_size 16

  defmodule Image do
    @moduledoc "A paged term image: pages, per-page checksums, and the rolling checksum."
    defstruct page_size: nil,
              commit: nil,
              pages: %{},
              page_checksums: %{},
              checksum: nil

    @type t :: %__MODULE__{
            page_size: pos_integer(),
            commit: pos_integer(),
            pages: %{DurableServer.LTX.pgno() => binary()},
            page_checksums: %{DurableServer.LTX.pgno() => DurableServer.LTX.checksum()},
            checksum: DurableServer.LTX.checksum()
          }
  end

  @doc """
  Encodes `term` into a paged image.

  When `previous` is the immediately preceding image (same page size),
  checksums are only computed for pages whose bytes changed; unchanged pages
  reuse the previous checksums, and the rolling checksum is updated
  incrementally. This turns the per-sync cost from O(state size) checksum
  work into O(changed bytes) — the byte comparison against the previous page
  is a plain binary equality check.
  """
  @spec encode(term(), pos_integer(), Image.t() | nil) :: {:ok, Image.t()} | {:error, term()}
  def encode(term, page_size, previous \\ nil)
      when is_nil(previous) or is_struct(previous, Image) do
    previous =
      case previous do
        %Image{page_size: ^page_size} = image -> image
        _other -> nil
      end

    if LTX.valid_page_size?(page_size) do
      payload = :erlang.term_to_binary(term, [:deterministic])
      total_size = @prologue_size + byte_size(payload)
      commit = div(total_size + page_size - 1, page_size)

      if commit >= LTX.lock_pgno(page_size) do
        {:error, {:term_too_large, total_size}}
      else
        padding = commit * page_size - total_size

        image_bytes =
          <<@magic, @format_version::16, 0::16, byte_size(payload)::64, payload::binary,
            0::size(padding)-unit(8)>>

        {:ok, build_image(image_bytes, commit, page_size, previous)}
      end
    else
      {:error, {:invalid_page_size, page_size}}
    end
  end

  @doc """
  Decodes the term stored in a complete image.
  """
  @spec decode(Image.t()) :: {:ok, term()} | {:error, term()}
  def decode(%Image{} = image) do
    with {:ok, bytes} <- image_bytes(image) do
      case bytes do
        <<@magic, @format_version::16, _reserved::16, payload_size::64, rest::binary>>
        when byte_size(rest) >= payload_size ->
          rest |> binary_part(0, payload_size) |> deserialize()

        <<@magic, version::16, _rest::binary>> when version != @format_version ->
          {:error, {:unsupported_image_version, version}}

        _other ->
          {:error, :invalid_image_prologue}
      end
    end
  end

  @doc """
  Pages of `image` whose checksums differ from `previous_checksums`
  (a `pgno => checksum` map, typically `previous_image.page_checksums`).
  Returns them sorted by page number, ready for `LTX.Encoder`. Pages the
  image no longer contains are handled by the shrunken commit, not by the
  diff.
  """
  @spec changed_pages(%{LTX.pgno() => LTX.checksum()}, Image.t()) :: [{LTX.pgno(), binary()}]
  def changed_pages(previous_checksums, %Image{} = image) when is_map(previous_checksums) do
    image.page_checksums
    |> Enum.filter(fn {pgno, checksum} -> Map.get(previous_checksums, pgno) != checksum end)
    |> Enum.map(fn {pgno, _checksum} -> {pgno, Map.fetch!(image.pages, pgno)} end)
    |> Enum.sort()
  end

  @doc """
  Applies decoded LTX segment pages on top of `base` (a previous image, or
  `nil` when the segment is a snapshot), producing the next image. Pages
  beyond `commit` are dropped; the result must be complete (every page
  `1..commit` present) or an error is returned.
  """
  @spec apply_pages(Image.t() | nil, pos_integer(), [{LTX.pgno(), binary()}], pos_integer()) ::
          {:ok, Image.t()} | {:error, term()}
  def apply_pages(base, commit, pages, page_size)
      when (is_struct(base, Image) or is_nil(base)) and is_integer(commit) and commit > 0 do
    base_pages =
      case base do
        nil -> %{}
        %Image{} = image -> image.pages
      end

    merged =
      pages
      |> Enum.reduce(base_pages, fn {pgno, data}, acc -> Map.put(acc, pgno, data) end)
      |> Map.filter(fn {pgno, _data} -> pgno <= commit end)

    with :ok <- validate_complete(merged, commit, page_size) do
      image_bytes =
        1..commit
        |> Enum.map(&Map.fetch!(merged, &1))
        |> IO.iodata_to_binary()

      {:ok, build_image(image_bytes, commit, page_size)}
    end
  end

  defp validate_complete(pages, commit, page_size) do
    Enum.reduce_while(1..commit, :ok, fn pgno, :ok ->
      case Map.fetch(pages, pgno) do
        {:ok, data} when byte_size(data) == page_size -> {:cont, :ok}
        {:ok, data} -> {:halt, {:error, {:invalid_page_size_at, pgno, byte_size(data)}}}
        :error -> {:halt, {:error, {:missing_page, pgno}}}
      end
    end)
  end

  defp build_image(image_bytes, commit, page_size, previous \\ nil) do
    pages =
      Map.new(1..commit, fn pgno ->
        {pgno, binary_part(image_bytes, (pgno - 1) * page_size, page_size)}
      end)

    previous_pages = if previous, do: previous.pages, else: %{}
    previous_checksums = if previous, do: previous.page_checksums, else: %{}

    page_checksums =
      Map.new(pages, fn {pgno, data} ->
        case previous_pages do
          %{^pgno => previous_data} when previous_data == data ->
            {pgno, Map.fetch!(previous_checksums, pgno)}

          _other ->
            {pgno, LTX.checksum_page(pgno, data)}
        end
      end)

    # Rolling checksum over all pages, skipping the lock page (unreachable
    # for encode-produced images, which cap out below it, but kept exact
    # for images rebuilt by apply_pages/4).
    lock_pgno = LTX.lock_pgno(page_size)

    checksum =
      Enum.reduce(page_checksums, 0, fn
        {^lock_pgno, _page_checksum}, acc -> acc
        {_pgno, page_checksum}, acc -> LTX.checksum_flag() ||| bxor(acc, page_checksum)
      end)

    %Image{
      page_size: page_size,
      commit: commit,
      pages: pages,
      page_checksums: page_checksums,
      checksum: checksum
    }
  end

  defp image_bytes(%Image{} = image) do
    with :ok <- validate_complete(image.pages, image.commit, image.page_size) do
      {:ok,
       1..image.commit
       |> Enum.map(&Map.fetch!(image.pages, &1))
       |> IO.iodata_to_binary()}
    end
  end

  defp deserialize(<<131, 80, _uncompressed_size::32, _rest::binary>>),
    do: {:error, :compressed_payload_rejected}

  defp deserialize(payload) do
    {:ok, :erlang.binary_to_term(payload)}
  rescue
    _error in ArgumentError -> {:error, :invalid_term_payload}
  end
end
