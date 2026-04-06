// Copyright 2025 Antfly, Inc.
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
	"encoding/json"
	"maps"
	"slices"

	"github.com/antflydb/antfly/lib/types"
	"go.etcd.io/raft/v3"
)

// BackupFormat selects the backup serialization format.
type BackupFormat string

const (
	// BackupFormatNative uses engine-specific physical snapshots (fast, same-backend only).
	BackupFormatNative BackupFormat = "native"
	// BackupFormatPortable uses the cross-backend AFB logical format (slower restore, any backend).
	BackupFormatPortable BackupFormat = "portable"
)

type BackupConfig struct {
	BackupID string       `json:"backup_id"`
	Location string       `json:"location"`
	Format   BackupFormat `json:"format,omitempty"`
}

func (rc *BackupConfig) Equal(other *BackupConfig) bool {
	return rc == nil && other == nil || rc != nil && other != nil &&
		rc.BackupID == other.BackupID && rc.Location == other.Location
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
