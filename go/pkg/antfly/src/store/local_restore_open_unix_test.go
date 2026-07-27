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

//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package store

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenLocalRestoreArtifactRejectsFIFOWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	const artifactName = "backup-1-1.afb"
	require.NoError(t, syscall.Mkfifo(filepath.Join(root, artifactName), 0o600))

	file, err := openLocalRestoreArtifact(root, artifactName)
	if file != nil {
		_ = file.Close()
	}
	require.ErrorContains(t, err, "not a regular file")
}
