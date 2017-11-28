package block

import (
	"time"

	"github.com/kopia/kopia/storage"
)

type blockCache interface {
	getBlock(blockID string, offset, length int64) ([]byte, error)
	putBlock(blockID string, data []byte) error
	listIndexBlocks() ([]Info, error)
}

// CachingOptions specifies configuration of local cache.
type CachingOptions struct {
	CacheDirectory          string `json:"cacheDirectory,omitempty"`
	MaxCacheSizeBytes       int64  `json:"maxCacheSize,omitempty"`
	MaxListCacheDurationSec int    `json:"maxListCacheDuration,omitempty"`
	HMACSecret              []byte `json:"-"`
}

func newBlockCache(st storage.Storage, caching CachingOptions) blockCache {
	if caching.MaxCacheSizeBytes == 0 || caching.CacheDirectory == "" {
		return nullBlockCache{st}
	}

	c := &diskBlockCache{
		st:                st,
		directory:         caching.CacheDirectory,
		maxSizeBytes:      caching.MaxCacheSizeBytes,
		hmacSecret:        append([]byte(nil), caching.HMACSecret...),
		listCacheDuration: time.Duration(caching.MaxListCacheDurationSec) * time.Second,
		closed:            make(chan struct{}),
	}

	c.sweepDirectory()
	go c.sweepDirectoryPeriodically()

	return c
}
