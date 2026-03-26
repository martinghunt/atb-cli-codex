package store

import (
	"errors"
	"os"

	"github.com/martin/atb-cli-codex/internal/cache"
)

// BuildQueryCache builds or refreshes lookup.sqlite from cached parquet data.
// It is a no-op when the minimum parquet inputs are not present yet.
func BuildQueryCache(layout cache.Layout, logf func(string, ...any)) error {
	err := ensureLookupIndex(layout, logf)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
