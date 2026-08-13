// Interop harness for DurableServer.LTX against the Go reference
// implementation (github.com/superfly/ltx).
//
// Usage:
//
//	go run . generate <dir>   # write golden fixtures for the Elixir decoder
//	go run . verify <file...> # verify Elixir-encoded files with the Go decoder
//
// Regenerate the checked-in fixtures with:
//
//	cd scripts/ltx_interop && go run . generate ../../test/fixtures/ltx
//
// Page contents are deterministic: byte i of page pgno is
// (pgno*31 + (i/16)*7) % 251, mirrored by the Elixir interop test.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: main.go generate <dir> | verify <file...>")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "generate":
		if err := generate(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "generate:", err)
			os.Exit(1)
		}
	case "verify":
		for _, path := range os.Args[2:] {
			if err := verify(path); err != nil {
				fmt.Fprintf(os.Stderr, "verify %s: %v\n", path, err)
				os.Exit(1)
			}
			fmt.Printf("ok %s\n", path)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		os.Exit(2)
	}
}

func page(pgno uint32, pageSize int) []byte {
	data := make([]byte, pageSize)
	for i := range data {
		data[i] = byte((int(pgno)*31 + (i/16)*7) % 251)
	}
	return data
}

func rollingChecksum(pages map[uint32][]byte, commit uint32) ltx.Checksum {
	var chksum ltx.Checksum
	for pgno := uint32(1); pgno <= commit; pgno++ {
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(pgno, pages[pgno]))
	}
	return chksum
}

func encodeFile(path string, hdr ltx.Header, pages [][2]interface{}, postApply ltx.Checksum) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		return err
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return err
	}
	for _, p := range pages {
		pgno := p[0].(uint32)
		data := p[1].([]byte)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return fmt.Errorf("encode page %d: %w", pgno, err)
		}
	}
	enc.SetPostApplyChecksum(postApply)
	if err := enc.Close(); err != nil {
		return err
	}
	return f.Sync()
}

func generate(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	const ts = int64(1_700_000_000_000)

	// snapshot.ltx: 3 pages of 4096, TXID 1-1.
	{
		pageSize, commit := 4096, uint32(3)
		pages := map[uint32][]byte{}
		var frames [][2]interface{}
		for pgno := uint32(1); pgno <= commit; pgno++ {
			pages[pgno] = page(pgno, pageSize)
			frames = append(frames, [2]interface{}{pgno, pages[pgno]})
		}
		hdr := ltx.Header{Version: ltx.Version, PageSize: uint32(pageSize), Commit: commit,
			MinTXID: 1, MaxTXID: 1, Timestamp: ts, NodeID: 7}
		if err := encodeFile(filepath.Join(dir, "snapshot.ltx"), hdr, frames, rollingChecksum(pages, commit)); err != nil {
			return fmt.Errorf("snapshot.ltx: %w", err)
		}
	}

	// delta.ltx: pages 2 and 5 of a 6-page 1024-byte database, TXID 2-3.
	{
		pageSize, commit := 1024, uint32(6)
		base := map[uint32][]byte{}
		for pgno := uint32(1); pgno <= commit; pgno++ {
			base[pgno] = page(pgno, pageSize)
		}
		pre := rollingChecksum(base, commit)

		updated := map[uint32][]byte{}
		for pgno, data := range base {
			updated[pgno] = data
		}
		updated[2] = page(102, pageSize)
		updated[5] = page(105, pageSize)
		post := rollingChecksum(updated, commit)

		hdr := ltx.Header{Version: ltx.Version, PageSize: uint32(pageSize), Commit: commit,
			MinTXID: 2, MaxTXID: 3, Timestamp: ts, PreApplyChecksum: pre}
		frames := [][2]interface{}{{uint32(2), updated[2]}, {uint32(5), updated[5]}}
		if err := encodeFile(filepath.Join(dir, "delta.ltx"), hdr, frames, post); err != nil {
			return fmt.Errorf("delta.ltx: %w", err)
		}
	}

	// deletion.ltx: commit 0, zero pages, TXID 7-7.
	{
		pageSize, commit := 4096, uint32(2)
		prior := map[uint32][]byte{1: page(1, pageSize), 2: page(2, pageSize)}
		pre := rollingChecksum(prior, commit)

		hdr := ltx.Header{Version: ltx.Version, PageSize: uint32(pageSize), Commit: 0,
			MinTXID: 7, MaxTXID: 7, Timestamp: ts, PreApplyChecksum: pre}
		if err := encodeFile(filepath.Join(dir, "deletion.ltx"), hdr, nil, ltx.ChecksumFlag); err != nil {
			return fmt.Errorf("deletion.ltx: %w", err)
		}
	}

	// page512.ltx: snapshot with the minimum page size, TXID 1-1.
	{
		pageSize, commit := 512, uint32(4)
		pages := map[uint32][]byte{}
		var frames [][2]interface{}
		for pgno := uint32(1); pgno <= commit; pgno++ {
			pages[pgno] = page(pgno, pageSize)
			frames = append(frames, [2]interface{}{pgno, pages[pgno]})
		}
		hdr := ltx.Header{Version: ltx.Version, PageSize: uint32(pageSize), Commit: commit,
			MinTXID: 1, MaxTXID: 1, Timestamp: ts}
		if err := encodeFile(filepath.Join(dir, "page512.ltx"), hdr, frames, rollingChecksum(pages, commit)); err != nil {
			return fmt.Errorf("page512.ltx: %w", err)
		}
	}

	// nochecksum.ltx: non-snapshot delta with checksum tracking disabled.
	{
		pageSize := 512
		hdr := ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum,
			PageSize: uint32(pageSize), Commit: 8, MinTXID: 3, MaxTXID: 4, Timestamp: ts}
		frames := [][2]interface{}{{uint32(3), page(3, pageSize)}, {uint32(4), page(4, pageSize)}}
		if err := encodeFile(filepath.Join(dir, "nochecksum.ltx"), hdr, frames, 0); err != nil {
			return fmt.Errorf("nochecksum.ltx: %w", err)
		}
	}

	return nil
}

func verify(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return err
	}

	hdr, trailer := dec.Header(), dec.Trailer()
	fmt.Printf("  header: min=%s max=%s commit=%d pageSize=%d preApply=%s\n",
		hdr.MinTXID, hdr.MaxTXID, hdr.Commit, hdr.PageSize, hdr.PreApplyChecksum)
	fmt.Printf("  trailer: postApply=%s fileChecksum=%s\n",
		trailer.PostApplyChecksum, trailer.FileChecksum)
	return nil
}
