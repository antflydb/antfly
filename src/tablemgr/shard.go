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

package tablemgr

import (
	"fmt"

	"go.uber.org/zap/zapcore"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
)

// ShardTransition represents a planned change for a shard
type ShardTransition struct {
	ShardID     types.ID      // ID of the shard to transition
	AddPeers    types.IDSlice // Peers to add to this shard
	RemovePeers types.IDSlice // Peers to remove from this shard
}

func (s ShardTransition) String() string {
	builder := fmt.Sprintf("ShardTransition{ShardID: %s", s.ShardID)
	if len(s.AddPeers) > 0 {
		builder += fmt.Sprintf(", AddPeers: %s", s.AddPeers)
	}
	if len(s.RemovePeers) > 0 {
		builder += fmt.Sprintf(", RemovePeers: %s", s.RemovePeers)
	}
	return builder + "}"
}

type SplitTransition struct {
	ShardID      types.ID // ID of the shard to transition
	SplitShardID types.ID // Split this shard into two and use this for the new shard's ID
	SplitKey     []byte   // Key to split the shard at
	TableName    string   // Name of the table to split
}

func (s SplitTransition) String() string {
	return fmt.Sprintf(
		"SplitTransition{ShardID: %s, SplitShardID: %s, SplitKey: %s, TableName: %s}",
		s.ShardID,
		s.SplitShardID,
		types.FormatKey(s.SplitKey),
		s.TableName,
	)
}

// MarshalLogObject implements zapcore.ObjectMarshaler for structured logging.
func (s SplitTransition) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("ShardID", s.ShardID.String())
	enc.AddString("SplitShardID", s.SplitShardID.String())
	enc.AddString("SplitKey", types.FormatKey(s.SplitKey))
	enc.AddString("TableName", s.TableName)
	return nil
}

type MergeTransition struct {
	ShardID      types.ID // ID of the shard to transition
	MergeShardID types.ID // Merge this shard into the other shard
	TableName    string   // Name of the table to merge
}

func (s MergeTransition) String() string {
	return fmt.Sprintf(
		"MergeTransition{ShardID: %s, MergeShardID: %s, TableName: %s}",
		s.ShardID,
		s.MergeShardID,
		s.TableName,
	)
}

// createShardConfig generates a map of shard configurations with evenly distributed
// byte ranges for the specified number of shards. Supports up to 65535 shards.
func createShardConfig(tc TableConfig) map[types.ID]*store.ShardConfig {
	if tc.NumShards == 0 {
		return nil
	}

	result := make(map[types.ID]*store.ShardConfig, tc.NumShards)
	for i := range tc.NumShards {
		shardID := tc.StartID + types.ID(i)

		var byteRange types.Range
		if i == 0 {
			byteRange[0] = []byte{}
		} else {
			byteRange[0] = shardBoundary(i, tc.NumShards)
		}
		if i == tc.NumShards-1 {
			byteRange[1] = []byte{0xFF}
		} else {
			byteRange[1] = shardBoundary(i+1, tc.NumShards)
		}

		result[shardID] = &store.ShardConfig{
			ByteRange: byteRange,
			Schema:    tc.Schema,
			Indexes:   tc.Indexes,
		}
	}
	return result
}

// shardBoundary computes the byte-key for the i-th boundary when dividing the
// key space into n shards. It maps i to a position in a 16-bit space (0–65535)
// and returns a 1- or 2-byte key. Single-byte keys are preferred when the low
// byte is zero (e.g. boundary 1 of 2 → {0x80}).
func shardBoundary(i, n uint) []byte {
	pos := (uint32(i) * 65536) / uint32(n) //nolint:gosec // G115: i and n are small shard counts, no overflow
	hi := byte(pos >> 8)                   //nolint:gosec // G115: intentional byte extraction from uint32
	lo := byte(pos & 0xFF)
	if lo == 0 {
		return []byte{hi}
	}
	return []byte{hi, lo}
}
