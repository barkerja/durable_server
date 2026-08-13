defmodule DurableServer.LTX.InteropTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias DurableServer.LTX
  alias DurableServer.LTX.{Decoder, Encoder}

  @moduledoc """
  Interop against the Go reference implementation (github.com/superfly/ltx).

  Decode side: golden fixtures under test/fixtures/ltx were produced by the
  Go encoder (see scripts/ltx_interop/main.go for regeneration instructions);
  decoding them with full checksum verification proves wire compatibility.

  Encode side: when a Go toolchain is available, files produced by the Elixir
  encoder are verified by the Go decoder via `go run . verify`.
  """

  @fixtures_dir Path.expand("../../fixtures/ltx", __DIR__)
  @interop_script_dir Path.expand("../../../scripts/ltx_interop", __DIR__)

  @checksum_flag 1 <<< 63
  @timestamp 1_700_000_000_000

  # Mirrors the deterministic page-content formula in scripts/ltx_interop/main.go.
  defp page(pgno, page_size) do
    for i <- 0..(page_size - 1), into: <<>>, do: <<rem(pgno * 31 + div(i, 16) * 7, 251)>>
  end

  defp fixture!(name) do
    path = Path.join(@fixtures_dir, name)
    assert File.exists?(path), "missing fixture #{path} — run: cd scripts/ltx_interop && go run . generate ../../test/fixtures/ltx"
    File.read!(path)
  end

  describe "decoding Go-encoded fixtures" do
    test "snapshot.ltx" do
      assert {:ok, decoded} = Decoder.decode(fixture!("snapshot.ltx"))

      assert %LTX.Header{
               page_size: 4096,
               commit: 3,
               min_txid: 1,
               max_txid: 1,
               timestamp: @timestamp,
               node_id: 7,
               pre_apply_checksum: 0
             } = decoded.header

      assert decoded.pages == for(pgno <- 1..3, do: {pgno, page(pgno, 4096)})

      expected_post = LTX.checksum_pages(decoded.pages, 4096)
      assert decoded.trailer.post_apply_checksum == expected_post

      assert {:ok, database} = Decoder.decode_database(fixture!("snapshot.ltx"))
      assert byte_size(database) == 3 * 4096
    end

    test "delta.ltx" do
      assert {:ok, decoded} = Decoder.decode(fixture!("delta.ltx"))

      assert %LTX.Header{page_size: 1024, commit: 6, min_txid: 2, max_txid: 3} =
               decoded.header

      assert decoded.pages == [{2, page(102, 1024)}, {5, page(105, 1024)}]

      base = for pgno <- 1..6, do: {pgno, page(pgno, 1024)}
      assert decoded.header.pre_apply_checksum == LTX.checksum_pages(base, 1024)

      updated =
        Enum.map(base, fn
          {2, _data} -> {2, page(102, 1024)}
          {5, _data} -> {5, page(105, 1024)}
          other -> other
        end)

      assert decoded.trailer.post_apply_checksum == LTX.checksum_pages(updated, 1024)
    end

    test "deletion.ltx" do
      assert {:ok, decoded} = Decoder.decode(fixture!("deletion.ltx"))

      assert %LTX.Header{commit: 0, min_txid: 7, max_txid: 7} = decoded.header
      assert decoded.pages == []
      assert decoded.trailer.post_apply_checksum == @checksum_flag
    end

    test "page512.ltx" do
      assert {:ok, decoded} = Decoder.decode(fixture!("page512.ltx"))

      assert %LTX.Header{page_size: 512, commit: 4, min_txid: 1, max_txid: 1} = decoded.header
      assert decoded.pages == for(pgno <- 1..4, do: {pgno, page(pgno, 512)})
    end

    test "nochecksum.ltx" do
      assert {:ok, decoded} = Decoder.decode(fixture!("nochecksum.ltx"))

      assert LTX.no_checksum?(decoded.header)
      assert %LTX.Header{page_size: 512, commit: 8, min_txid: 3, max_txid: 4} = decoded.header
      assert decoded.pages == [{3, page(3, 512)}, {4, page(4, 512)}]
      assert decoded.trailer.post_apply_checksum == 0
    end
  end

  describe "Go decoder verifies Elixir-encoded files" do
    @describetag :interop

    setup do
      case System.find_executable("go") do
        nil -> {:ok, skip: true}
        _go -> :ok
      end
    end

    test "snapshot, delta, deletion, and no-checksum files", context do
      if context[:skip] do
        :ok
      else
        dir = Path.join(System.tmp_dir!(), "ltx_interop_#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        on_exit(fn -> File.rm_rf!(dir) end)

        # Snapshot.
        snapshot_pages = for pgno <- 1..5, do: {pgno, page(pgno, 4096)}
        snapshot_post = LTX.checksum_pages(snapshot_pages, 4096)

        write_ltx!(
          Path.join(dir, "elixir_snapshot.ltx"),
          %LTX.Header{page_size: 4096, commit: 5, min_txid: 1, max_txid: 1, timestamp: @timestamp},
          snapshot_pages,
          snapshot_post
        )

        # Delta on top of the snapshot.
        updated =
          Enum.map(snapshot_pages, fn
            {3, _data} -> {3, page(203, 4096)}
            other -> other
          end)

        write_ltx!(
          Path.join(dir, "elixir_delta.ltx"),
          %LTX.Header{
            page_size: 4096,
            commit: 5,
            min_txid: 2,
            max_txid: 2,
            timestamp: @timestamp,
            pre_apply_checksum: snapshot_post
          },
          [{3, page(203, 4096)}],
          LTX.checksum_pages(updated, 4096)
        )

        # Deletion.
        write_ltx!(
          Path.join(dir, "elixir_deletion.ltx"),
          %LTX.Header{
            page_size: 4096,
            commit: 0,
            min_txid: 3,
            max_txid: 3,
            pre_apply_checksum: LTX.checksum_pages(updated, 4096)
          },
          [],
          @checksum_flag
        )

        # No-checksum.
        write_ltx!(
          Path.join(dir, "elixir_nochecksum.ltx"),
          %LTX.Header{
            flags: LTX.header_flag_no_checksum(),
            page_size: 512,
            commit: 9,
            min_txid: 4,
            max_txid: 6
          },
          [{7, page(7, 512)}, {9, page(9, 512)}],
          0
        )

        files = Enum.sort(Path.wildcard(Path.join(dir, "*.ltx")))
        assert files != []

        {output, status} =
          System.cmd("go", ["run", ".", "verify" | files],
            cd: @interop_script_dir,
            stderr_to_stdout: true
          )

        assert status == 0, "Go verification failed:\n#{output}"

        for file <- files do
          assert output =~ "ok #{file}"
        end
      end
    end
  end

  defp write_ltx!(path, header, pages, post_apply) do
    {:ok, enc} = Encoder.new(header)

    enc =
      Enum.reduce(pages, enc, fn {pgno, data}, enc ->
        {:ok, enc} = Encoder.encode_page(enc, pgno, data)
        enc
      end)

    {:ok, iodata} = Encoder.finish(enc, post_apply)
    File.write!(path, IO.iodata_to_binary(iodata))
  end
end
