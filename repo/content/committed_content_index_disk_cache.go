package content

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/public/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

const (
	simpleIndexSuffix                      = ".sndx"
	unusedCommittedContentIndexCleanupTime = 1 * time.Hour // delete unused committed index blobs after 1 hour
)

type diskCommittedContentIndexCache struct {
	dirname              string
	timeNow              func() time.Time
	v1PerContentOverhead uint32
	log                  logging.Logger
}

func (c *diskCommittedContentIndexCache) indexBlobPath(indexBlobID blob.ID) string {
	return filepath.Join(c.dirname, string(indexBlobID)+simpleIndexSuffix)
}

func (c *diskCommittedContentIndexCache) openIndex(ctx context.Context, indexBlobID blob.ID) (packIndex, error) {
	fullpath := c.indexBlobPath(indexBlobID)

	f, err := c.mmapOpenWithRetry(fullpath)
	if err != nil {
		return nil, err
	}

	return openPackIndex(f, c.v1PerContentOverhead)
}

// mmapOpenWithRetry attempts mmap.Open() with exponential back-off to work around rare issue specific to Windows where
// we can't open the file right after it has been written.
func (c *diskCommittedContentIndexCache) mmapOpenWithRetry(path string) (*mmap.ReaderAt, error) {
	const (
		maxRetries    = 8
		startingDelay = 10 * time.Millisecond
	)

	// retry milliseconds: 10, 20, 40, 80, 160, 320, 640, 1280, total ~2.5s
	f, err := mmap.Open(path)
	nextDelay := startingDelay

	retryCount := 0
	for err != nil && retryCount < maxRetries {
		retryCount++
		c.log.Debugf("retry #%v unable to mmap.Open(): %v", retryCount, err)
		time.Sleep(nextDelay)
		nextDelay *= 2
		f, err = mmap.Open(path)
	}

	return f, errors.Wrap(err, "mmap() error")
}

func (c *diskCommittedContentIndexCache) hasIndexBlobID(ctx context.Context, indexBlobID blob.ID) (bool, error) {
	_, err := os.Stat(c.indexBlobPath(indexBlobID))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, errors.Wrapf(err, "error checking %v", indexBlobID)
}

func (c *diskCommittedContentIndexCache) addContentToCache(ctx context.Context, indexBlobID blob.ID, data gather.Bytes) error {
	exists, err := c.hasIndexBlobID(ctx, indexBlobID)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	tmpFile, err := writeTempFileAtomic(c.dirname, data.ToByteSlice())
	if err != nil {
		return err
	}

	// rename() is atomic, so one process will succeed, but the other will fail
	if err := os.Rename(tmpFile, c.indexBlobPath(indexBlobID)); err != nil {
		// verify that the content exists
		exists, err := c.hasIndexBlobID(ctx, indexBlobID)
		if err != nil {
			return err
		}

		if !exists {
			return errors.Errorf("unsuccessful index write of content %q", indexBlobID)
		}
	}

	return nil
}

func writeTempFileAtomic(dirname string, data []byte) (string, error) {
	// write to a temp file to avoid race where two processes are writing at the same time.
	tf, err := os.CreateTemp(dirname, "tmp")
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirname, cache.DirMode) //nolint:errcheck
			tf, err = os.CreateTemp(dirname, "tmp")
		}
	}

	if err != nil {
		return "", errors.Wrap(err, "can't create tmp file")
	}

	if _, err := tf.Write(data); err != nil {
		return "", errors.Wrap(err, "can't write to temp file")
	}

	if err := tf.Close(); err != nil {
		return "", errors.Errorf("can't close tmp file")
	}

	return tf.Name(), nil
}

func (c *diskCommittedContentIndexCache) expireUnused(ctx context.Context, used []blob.ID) error {
	c.log.Debugf("expireUnused (except %v)", used)

	entries, err := ioutil.ReadDir(c.dirname)
	if err != nil {
		return errors.Wrap(err, "can't list cache")
	}

	remaining := map[blob.ID]os.FileInfo{}

	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), simpleIndexSuffix) {
			n := strings.TrimSuffix(ent.Name(), simpleIndexSuffix)
			remaining[blob.ID(n)] = ent
		}
	}

	for _, u := range used {
		delete(remaining, u)
	}

	for _, rem := range remaining {
		if c.timeNow().Sub(rem.ModTime()) > unusedCommittedContentIndexCleanupTime {
			c.log.Debugf("removing unused %v %v", rem.Name(), rem.ModTime())

			if err := os.Remove(filepath.Join(c.dirname, rem.Name())); err != nil {
				c.log.Errorf("unable to remove unused index file: %v", err)
			}
		} else {
			c.log.Debugf("keeping unused %v because it's too new %v", rem.Name(), rem.ModTime())
		}
	}

	return nil
}
