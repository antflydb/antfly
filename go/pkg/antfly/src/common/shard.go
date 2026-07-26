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

package common

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"go.etcd.io/raft/v3"
)

// BackupFormat selects the backup serialization format.
type BackupFormat string

const (
	// DefaultBackupFormat is used when callers omit a backup format.
	DefaultBackupFormat BackupFormat = BackupFormatPortable
	// BackupFormatNative uses engine-specific physical snapshots (fast, same-backend only).
	BackupFormatNative BackupFormat = "native"
	// BackupFormatPortable uses the cross-backend AFB logical format (slower restore, any backend).
	BackupFormatPortable BackupFormat = "portable"
)

var ErrBackupAlreadyExists = errors.New("backup already exists")

var ErrBackupArtifactIntegrityMismatch = errors.New("backup artifact integrity mismatch")

const backupArtifactVerificationBufferBytes = 256 * 1024

var backupArtifactVerificationBufferPool = sync.Pool{
	New: func() any {
		buffer := make([]byte, backupArtifactVerificationBufferBytes)
		return &buffer
	},
}

func NormalizeBackupFormat(format BackupFormat) BackupFormat {
	if format == "" {
		return DefaultBackupFormat
	}
	return format
}

func ValidateBackupFormat(format BackupFormat) (BackupFormat, error) {
	normalized := NormalizeBackupFormat(format)
	switch normalized {
	case BackupFormatNative, BackupFormatPortable:
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported backup format %q", normalized)
	}
}

// EffectiveFilesystemLocation returns the coordinator-authorized location for
// a file backup or restore. A named connection must never fall back to the
// user-supplied logical URI when its resolved path is missing.
func EffectiveFilesystemLocation(config BackupConfig) (string, error) {
	if !strings.HasPrefix(config.Location, "file://") {
		return config.Location, nil
	}
	if config.ResolvedLocation != "" {
		if !strings.HasPrefix(config.ResolvedLocation, "file://") {
			return "", errors.New("resolved filesystem location must use file://")
		}
		return config.ResolvedLocation, nil
	}
	if config.Connection != "" {
		return "", errors.New("filesystem operation requires a coordinator-resolved location")
	}
	return config.Location, nil
}

type BackupConfig struct {
	BackupID         string       `json:"backup_id"`
	Connection       string       `json:"connection,omitempty"`
	Location         string       `json:"location"`
	Format           BackupFormat `json:"format,omitempty"`
	ResolvedLocation string       `json:"-"`
	// Artifact binds a portable restore request to the exact shard payload
	// declared by the immutable table backup metadata.
	Artifact *BackupArtifactIntegrity `json:"artifact,omitempty"`
}

// BackupArtifactIntegrity binds an immutable backup object to the bytes
// produced by the shard. Metadata publishes these identities only after every
// shard reports durable success, allowing cross-runtime restore admission to
// verify artifacts without trusting provider-specific ETags.
type BackupArtifactIntegrity struct {
	Name      string `json:"name"`
	SizeBytes uint64 `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

func ValidateBackupArtifactIntegrity(artifact *BackupArtifactIntegrity) error {
	if artifact == nil ||
		artifact.Name == "" ||
		artifact.SizeBytes == 0 {
		return errors.New("backup artifact identity is incomplete")
	}
	decoded, err := hex.DecodeString(artifact.SHA256)
	if err != nil || len(decoded) != sha256.Size ||
		hex.EncodeToString(decoded) != artifact.SHA256 {
		return fmt.Errorf("backup artifact %q has an invalid SHA-256 digest", artifact.Name)
	}
	return nil
}

// VerifyBackupArtifact streams an artifact through SHA-256 with bounded,
// pooled memory and checks cancellation between reads.
func VerifyBackupArtifact(
	ctx context.Context,
	reader io.Reader,
	artifact BackupArtifactIntegrity,
) error {
	if err := ValidateBackupArtifactIntegrity(&artifact); err != nil {
		return err
	}
	bufferPtr := backupArtifactVerificationBufferPool.Get().(*[]byte)
	defer backupArtifactVerificationBufferPool.Put(bufferPtr)
	buffer := *bufferPtr
	hasher := sha256.New()
	var total uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := reader.Read(buffer)
		if n > 0 {
			next := total + uint64(n)
			if next < total || next > artifact.SizeBytes {
				return fmt.Errorf(
					"%w: %s has an unexpected size",
					ErrBackupArtifactIntegrityMismatch,
					artifact.Name,
				)
			}
			total = next
			_, _ = hasher.Write(buffer[:n])
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if total != artifact.SizeBytes ||
		hex.EncodeToString(hasher.Sum(nil)) != artifact.SHA256 {
		return fmt.Errorf(
			"%w: %s",
			ErrBackupArtifactIntegrityMismatch,
			artifact.Name,
		)
	}
	return nil
}

const (
	BackupArtifactNameHeader   = "X-Antfly-Backup-Artifact-Name"
	BackupArtifactSizeHeader   = "X-Antfly-Backup-Artifact-Size"
	BackupArtifactSHA256Header = "X-Antfly-Backup-Artifact-Sha256"
)

func (rc *BackupConfig) Equal(other *BackupConfig) bool {
	return rc == nil && other == nil || rc != nil && other != nil &&
		rc.BackupID == other.BackupID &&
		rc.Connection == other.Connection &&
		rc.Location == other.Location &&
		NormalizeBackupFormat(rc.Format) == NormalizeBackupFormat(other.Format) &&
		rc.Artifact.Equal(other.Artifact)
}

func (a *BackupArtifactIntegrity) Equal(other *BackupArtifactIntegrity) bool {
	return a == nil && other == nil || a != nil && other != nil &&
		a.Name == other.Name &&
		a.SizeBytes == other.SizeBytes &&
		a.SHA256 == other.SHA256
}

type PeerSet map[types.ID]struct{}

func NewPeerSet(ids ...types.ID) PeerSet {
	peerSet := make(map[types.ID]struct{})
	for _, id := range ids {
		peerSet[id] = struct{}{}
	}
	return peerSet
}

func (ps PeerSet) Equal(other PeerSet) bool {
	return maps.Equal(ps, other)
}

func (ps PeerSet) Add(id types.ID) {
	ps[id] = struct{}{}
}

func (ps PeerSet) Remove(id types.ID) {
	delete(ps, id)
}

func (ps PeerSet) Contains(id types.ID) bool {
	_, ok := ps[id]
	return ok
}

func (ps PeerSet) IDSlice() types.IDSlice {
	if len(ps) == 0 {
		return make(types.IDSlice, 0) // Return empty non-nil slice
	}
	ids := make(types.IDSlice, 0, len(ps))
	for id := range ps {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (ps PeerSet) MarshalJSON() ([]byte, error) {
	ids := ps.IDSlice()
	return json.Marshal(ids)
}

// Copy creates a new copy of a PeerSet.
func (ps PeerSet) Copy() PeerSet {
	return maps.Clone(ps)
}

func (ps *PeerSet) UnmarshalJSON(b []byte) error {
	var idSlice types.IDSlice
	if err := json.Unmarshal(b, &idSlice); err != nil {
		return err
	}
	// Ensure the map is initialized before adding elements.
	// Using *ps = make(PeerSet) dereferences the pointer and assigns a new map to it.
	*ps = make(PeerSet)
	for _, id := range idSlice {
		(*ps)[id] = struct{}{}
	}
	return nil
}

type RaftStatus struct {
	Lead   types.ID `json:"leader_id,omitzero"`
	Voters PeerSet  `json:"voters,omitempty"`
	// RaftState string   `json:"raft_state"`
}

func NewRaftStatus(rs raft.Status) *RaftStatus {
	ids := rs.Config.Voters.IDs()
	voters := make(PeerSet)
	for id := range ids {
		voters.Add(types.ID(id))
	}
	return &RaftStatus{
		Lead:   types.ID(rs.Lead),
		Voters: voters,
		// RaftState: rs.SoftState.RaftState.String(),
	}
}

func (rs *RaftStatus) Equal(other *RaftStatus) bool {
	if rs == nil || other == nil {
		return rs == other
	}
	return rs.Lead == other.Lead &&
		rs.Voters.Equal(other.Voters) // && rs.RaftState == other.RaftState
}
