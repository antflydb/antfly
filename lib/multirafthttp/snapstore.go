// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multirafthttp

import (
	"context"
	"io"
)

// SnapStore is a local interface defining the snapshot storage operations
// required by the transport layer. This is a subset of the full SnapStore
// interface from src/snapstore, allowing multirafthttp to avoid importing src/.
type SnapStore interface {
	// Get returns a reader for the snapshot with the given ID.
	// Returns os.ErrNotExist if the snapshot doesn't exist.
	Get(ctx context.Context, snapID string) (io.ReadCloser, error)

	// Put stores a snapshot with the given ID from the reader.
	// The implementation should handle atomic writes (e.g., temp file + rename).
	Put(ctx context.Context, snapID string, r io.Reader) error

	// Exists checks if a snapshot with the given ID exists.
	Exists(ctx context.Context, snapID string) (bool, error)
}
