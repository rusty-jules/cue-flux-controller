package controllers

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/tidwall/buntdb"

	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	cuev1alpha1 "github.com/phoban01/cue-flux-controller/api/v1alpha1"
)

var cache *CueInstanceCache

var (
	dbPersistence = ":memory:"
	// revision:*
	idxFilterFmt = "%s:*"
	// revision:ts
	tsKeyFmt = "%s:ts"
	// revision:cueinstanceName:cueinstanceNamespace
	cacheKeyFmt = "%s:%s:%s"
)

func init() {
	db, _ := buntdb.Open(dbPersistence)
	cache = &CueInstanceCache{db}
}

type CueInstanceCache struct {
	db *buntdb.DB
}

type CueInstanceIndex struct {
	revision  string
	timestamp time.Time
}

func (i *CueInstanceIndex) name() string {
	return i.revision
}

func (i CueInstanceIndex) filter() string {
	return fmt.Sprintf(idxFilterFmt, i.revision)
}

func (i *CueInstanceIndex) ts() (string, string) {
	tsKey := fmt.Sprintf(tsKeyFmt, i.revision)
	tsVal := i.timestamp.String()
	return tsKey, tsVal
}

func (c *CueInstanceCache) indexExists(revision string) bool {
	var indexExists bool

	// check if index exists with a view tx first so we don't lock db
	c.db.View(func(tx *buntdb.Tx) error {
		idxs, err := tx.Indexes()
		if err != nil {
			indexExists = false
			return err
		}
		for _, idx := range idxs {
			if idx == revision {
				indexExists = true
				break
			}
		}
		return nil
	})

	return indexExists
}

func (c *CueInstanceCache) cacheKey(revision string, instance *cuev1alpha1.CueInstance) string {
	return fmt.Sprintf(cacheKeyFmt, revision, instance.GetName(), instance.GetNamespace())
}

func (c *CueInstanceCache) createIndex(artifact *sourcev1.Artifact) error {
	if !c.indexExists(artifact.Revision) {
		idx := &CueInstanceIndex{
			revision:  artifact.Revision,
			timestamp: artifact.LastUpdateTime.Time,
		}
		return c.db.Update(func(tx *buntdb.Tx) error {
			// create index
			if err := tx.CreateIndex(idx.name(), idx.filter(), buntdb.IndexString); err != nil {
				return err
			}

			// insert timestamp
			tsKey, tsVal := idx.ts()
			_, _, err := tx.Set(tsKey, tsVal, nil)
			if err != nil {
				return err
			}

			return nil
		})
	}

	return nil
}

// dropIndex drops an index and all values that match it
func (c *CueInstanceCache) dropIndexes(idxs []*CueInstanceIndex) error {
	return c.db.Update(func(tx *buntdb.Tx) error {
		var delkeys []string

		for _, idx := range idxs {
			tx.AscendKeys(idx.filter(), func(k, v string) bool {
				delkeys = append(delkeys, k)
				return true
			})

			if err := tx.DropIndex(idx.name()); err != nil {
				if !errors.Is(err, buntdb.ErrNotFound) {
					return err
				}
			}
		}

		for _, k := range delkeys {
			if _, err := tx.Delete(k); err != nil {
				if !errors.Is(err, buntdb.ErrNotFound) {
					return err
				}
			}
		}

		return nil
	})
}

func (c *CueInstanceCache) gc(keepLast int) error {
	// get indexes
	var indexes []*CueInstanceIndex

	err := c.db.View(func(tx *buntdb.Tx) error {
		idxs, err := tx.Indexes()
		if err != nil {
			return err
		}

		for _, idxName := range idxs {
			tss, err := tx.Get(fmt.Sprintf(tsKeyFmt, idxName))
			if err != nil {
				return err
			}
			ts, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", tss)
			if err != nil {
				return err
			}
			indexes = append(indexes, &CueInstanceIndex{
				revision:  idxName,
				timestamp: ts,
			})
		}

		return nil
	})
	if err != nil {
		return err
	}

	if len(indexes) <= keepLast {
		return nil
	}

	// sort in ascending order
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].timestamp.After(indexes[j].timestamp)
	})

	// select the oldest indexes by skipping the most recent ones
	return c.dropIndexes(indexes[keepLast:])
}

func (c *CueInstanceCache) getInstance(cacheKey string) ([]byte, error) {
	var cachedResources []byte

	err := c.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(cacheKey)
		if err != nil {
			return err
		}

		cachedResources = []byte(val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return cachedResources, nil
}

func (c *CueInstanceCache) insertInstance(result *bytes.Buffer, cacheKey string) error {
	c.db.Update(func(tx *buntdb.Tx) error {
		// could optionally add an expiration configuration for ensuring cue is re-evaluated
		tx.Set(cacheKey, result.String(), nil)
		return nil
	})

	return nil
}
