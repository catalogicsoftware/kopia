package blockdevice_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshots/blockdevice"
)

const testChunkSize = 128 << 10

func TestUploadFullThenIncremental(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	rep := env.RepositoryWriter

	// 3 full chunks + a short trailing chunk.
	size := int64(3*testChunkSize + 1000)
	data := makeData(size, 0)

	opts := blockdevice.Options{ChunkSize: testChunkSize, Description: "test-disk"}

	// Full backup: no previous object, everything uploaded.
	fullID, err := blockdevice.Upload(ctx, rep, bytes.NewReader(data), size,
		func(int) bool { return true }, object.EmptyID, opts)
	if err != nil {
		t.Fatalf("full Upload failed: %v", err)
	}

	if got := readAll(t, ctx, rep, fullID); !bytes.Equal(got, data) {
		t.Fatalf("full backup round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// Incremental backup: only chunk 1 changed.
	data2 := append([]byte(nil), data...)
	copy(data2[testChunkSize:2*testChunkSize], makeData(testChunkSize, 7))

	incID, err := blockdevice.Upload(ctx, rep, bytes.NewReader(data2), size,
		func(i int) bool { return i == 1 }, fullID, opts)
	if err != nil {
		t.Fatalf("incremental Upload failed: %v", err)
	}

	if got := readAll(t, ctx, rep, incID); !bytes.Equal(got, data2) {
		t.Fatalf("incremental backup round-trip mismatch")
	}

	// Unchanged chunks must reuse the previous chunk objects; chunk 1 must differ.
	prev := loadIndex(t, ctx, rep, fullID)
	cur := loadIndex(t, ctx, rep, incID)

	if len(prev) != 4 || len(cur) != 4 {
		t.Fatalf("expected 4 chunk entries, got prev=%d cur=%d", len(prev), len(cur))
	}

	for i := range cur {
		switch {
		case i == 1 && cur[i].Object == prev[i].Object:
			t.Errorf("dirty chunk %d was not re-uploaded (object unchanged)", i)
		case i != 1 && cur[i].Object != prev[i].Object:
			t.Errorf("clean chunk %d was not reused (object changed)", i)
		}
	}
}

func TestUploadRejectsBadChunkSize(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	if _, err := blockdevice.Upload(ctx, env.RepositoryWriter, bytes.NewReader([]byte("x")), 1,
		func(int) bool { return true }, object.EmptyID, blockdevice.Options{ChunkSize: 100}); err == nil {
		t.Fatal("expected error for unsupported chunk size, got nil")
	}
}

func makeData(n int64, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i) + seed
	}

	return b
}

func readAll(t *testing.T, ctx context.Context, rep repo.DirectRepositoryWriter, oid object.ID) []byte {
	t.Helper()

	r, err := rep.OpenObject(ctx, oid)
	if err != nil {
		t.Fatalf("OpenObject(%v) failed: %v", oid, err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read object %v failed: %v", oid, err)
	}

	return data
}

func loadIndex(t *testing.T, ctx context.Context, rep repo.DirectRepositoryWriter, oid object.ID) []object.IndirectObjectEntry {
	t.Helper()

	indexID, ok := oid.IndexObjectID()
	if !ok {
		t.Fatalf("object %v is not indirect", oid)
	}

	entries, err := object.LoadIndexObject(ctx, rep.ContentManager(), indexID)
	if err != nil {
		t.Fatalf("LoadIndexObject(%v) failed: %v", indexID, err)
	}

	return entries
}
