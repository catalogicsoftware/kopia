// Uploads a fixed-chunk block-device image into a repository, uploading only the chunks that changed.
package blockdevice

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
)

const DefaultParallelism = 8

var splitterForChunkSize = map[int]string{
	128 << 10: "FIXED-128K",
	256 << 10: "FIXED-256K",
	512 << 10: "FIXED-512K",
	1 << 20:   "FIXED-1M",
	2 << 20:   "FIXED-2M",
	4 << 20:   "FIXED-4M",
	8 << 20:   "FIXED-8M",
}

// Options configure a block-device upload.
type Options struct {
	ChunkSize   int // Supported sizes are 128K, 256K, 512K, 1M, 2M, 4M, 8M
	Parallelism int
	Description string
	Progress    func(uploadedBytes int64)
}

// Upload writes a block-device image into the repository as a single indirect object and returns its ID.
// If previousObjectID is object.EmptyID all blocks are read.
func Upload(
	ctx context.Context,
	repositoryWriter repo.DirectRepositoryWriter,
	sourceReader io.ReaderAt,
	size int64,
	isDirty func(chunkIndex int) bool,
	previousObjectID object.ID,
	options Options,
) (object.ID, error) {
	splitterName, ok := splitterForChunkSize[options.ChunkSize]
	if !ok {
		return object.EmptyID, errors.Errorf("unsupported chunk size %d; must be a fixed size (128K, 256K, 512K, 1M, 2M, 4M or 8M)", options.ChunkSize)
	}

	if size < 0 {
		return object.EmptyID, errors.Errorf("invalid size %d", size)
	}

	numChunks := int((size + int64(options.ChunkSize) - 1) / int64(options.ChunkSize))

	// Load the previous per-chunk entries for reuse
	var prevEntries []object.IndirectObjectEntry

	if previousObjectID != object.EmptyID {
		indexID, ok := previousObjectID.IndexObjectID()
		if !ok {
			return object.EmptyID, errors.Errorf("previous object %v is not an indirect object", previousObjectID)
		}

		var err error

		prevEntries, err = object.LoadIndexObject(ctx, repositoryWriter.ContentManager(), indexID)
		if err != nil {
			return object.EmptyID, errors.Wrap(err, "unable to load previous index object")
		}
	}

	indirectObjectEntries := make([]object.IndirectObjectEntry, numChunks)

	uploadChunk := func(ctx context.Context, chunkNum int) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		offset := int64(chunkNum) * int64(options.ChunkSize)

		// A chunk covers [offset, offset+ChunkSize), clamped to the device size, so the last chunk may be shorter
		end := min(offset+int64(options.ChunkSize), size)
		length := end - offset

		// Reuse the unchanged chunk from the previous backup when possible
		if previousObjectID != object.EmptyID && !isDirty(chunkNum) {
			if prev, ok := reusablePrevEntry(prevEntries, chunkNum, offset, length); ok {
				indirectObjectEntries[chunkNum] = object.IndirectObjectEntry{Start: offset, Length: length, Object: prev.Object}
				return nil
			}
		}

		objectWriter := repositoryWriter.NewObjectWriter(ctx, object.WriterOptions{
			Splitter:    splitterName,
			Description: options.Description,
		})
		defer objectWriter.Close() //nolint:errcheck

		buffer := make([]byte, options.ChunkSize)
		chunkReader := io.NewSectionReader(sourceReader, offset, length)

		if _, err := io.CopyBuffer(objectWriter, chunkReader, buffer); err != nil {
			return errors.Wrapf(err, "unable to read chunk %d", chunkNum)
		}

		objectID, err := objectWriter.Result()
		if err != nil {
			return errors.Wrapf(err, "unable to write chunk %d", chunkNum)
		}

		indirectObjectEntries[chunkNum] = object.IndirectObjectEntry{
			Start:  offset,
			Length: length,
			Object: objectID,
		}

		if options.Progress != nil {
			options.Progress(length)
		}

		return nil
	}

	workers := options.Parallelism
	if workers <= 0 {
		workers = DefaultParallelism
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workers)

	for i := range numChunks {
		group.Go(func() error {
			return uploadChunk(groupCtx, i)
		})
	}

	if err := group.Wait(); err != nil {
		return object.EmptyID, err
	}

	// Write the index object that points to all the chunks
	objectWriter := repositoryWriter.NewObjectWriter(ctx, object.WriterOptions{
		Prefix:      object.IndirectContentPrefix,
		Description: options.Description,
	})
	defer objectWriter.Close() //nolint:errcheck

	objectID, err := object.WriteIndirectIndex(objectWriter, indirectObjectEntries)
	if err != nil {
		return object.EmptyID, errors.Wrap(err, "unable to write block device index")
	}

	return objectID, nil
}

// reusablePrevEntry returns the previous entry for chunk
func reusablePrevEntry(prevEntries []object.IndirectObjectEntry, chunkNum int, offset int64, length int64) (object.IndirectObjectEntry, bool) {
	if chunkNum >= len(prevEntries) {
		return object.IndirectObjectEntry{}, false
	}

	prevEntry := prevEntries[chunkNum]
	if prevEntry.Start != offset || prevEntry.Length != length {
		return object.IndirectObjectEntry{}, false
	}

	return prevEntry, true
}
