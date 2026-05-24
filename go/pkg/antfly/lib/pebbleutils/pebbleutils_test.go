// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package pebbleutils

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

func TestNewCacheDefaultsNonPositiveSize(t *testing.T) {
	cache := NewCache(0)
	defer cache.Close()

	require.NotNil(t, cache.Get())
}

func TestCacheApplyDefaultsNonPositiveFallback(t *testing.T) {
	opts := &pebble.Options{FS: vfs.NewMem()}
	var cache *Cache

	cache.Apply(opts, 0)
	require.NotNil(t, opts.Cache)

	db, err := pebble.Open("", opts)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("k"), []byte("v"), pebble.NoSync))
	require.NoError(t, db.Close())
}

func TestRecoverPebbleClosed_RecognizesRawPebbleClosedError(t *testing.T) {
	var err error

	func() {
		defer RecoverPebbleClosed(&err)
		panic(errors.New("pebble: closed"))
	}()

	require.ErrorIs(t, err, pebble.ErrClosed)
}

func TestRecoverPebbleClosed_RecognizesClosedLogWriterPanic(t *testing.T) {
	var err error

	func() {
		defer RecoverPebbleClosed(&err)
		panic(errors.New("pebble/record: closed LogWriter"))
	}()

	require.ErrorIs(t, err, pebble.ErrClosed)
}

func TestRecoverPebbleClosed_RepanicsUnknownErrors(t *testing.T) {
	require.Panics(t, func() {
		var err error
		func() {
			defer RecoverPebbleClosed(&err)
			panic(errors.New("boom"))
		}()
	})
}
