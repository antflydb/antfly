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

//go:generate protoc --go_out=. --go_opt=paths=source_relative common.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative transaction.proto
package common

import (
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/types"
)

func SplitArchive(shardID types.ID) string {
	return fmt.Sprintf("antfly-split-%s", shardID)
}

const RootAntflyDir = "antflydb"

func NodeDir(dataDir string, nodeID types.ID) string {
	return fmt.Sprintf("%s/store/%s", dataDir, nodeID)
}

func ShardDir(dataDir string, shardID, nodeID types.ID) string {
	return fmt.Sprintf("%s/store/%s/%s", dataDir, nodeID, shardID)
}

func RaftLogDir(dataDir string, shardID, nodeID types.ID) string {
	return fmt.Sprintf("%s/store/%s/%s/log", dataDir, nodeID, shardID)
}

func SnapDir(dataDir string, shardID, nodeID types.ID) string {
	return fmt.Sprintf("%s/store/%s/%s/snap", dataDir, nodeID, shardID)
}

func StorageDBDir(dataDir string, shardID, nodeID types.ID) string {
	return fmt.Sprintf("%s/store/%s/%s/storage", dataDir, nodeID, shardID)
}

func ParseStorageDBDir(dbDir string) (types.ID, types.ID, error) {
	// Path format: {dataDir}/store/{nodeID}/{shardID}/storage
	// Find "store" marker and extract nodeID/shardID relative to it
	parts := strings.Split(dbDir, "/")

	// Find the "store" marker
	storeIdx := -1
	for i, part := range parts {
		if part == "store" {
			storeIdx = i
			break
		}
	}

	// Need at least: store/{nodeID}/{shardID}/storage (4 parts after store marker)
	if storeIdx == -1 || storeIdx+3 >= len(parts) {
		return 0, 0, fmt.Errorf("invalid storage db dir format %s", dbDir)
	}

	// Verify last part is "storage"
	if parts[len(parts)-1] != "storage" {
		return 0, 0, fmt.Errorf("invalid storage db dir format %s", dbDir)
	}

	nodeID, err := types.IDFromString(parts[storeIdx+1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid storage db dir %s: %w", dbDir, err)
	}
	shardID, err := types.IDFromString(parts[storeIdx+2])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid storage db dir %s: %w", dbDir, err)
	}
	return shardID, nodeID, nil
}

func ShardBackupFileName(backupID string, shardID types.ID) string {
	return fmt.Sprintf("%s-%s.tar.zst", backupID, shardID)
}

// ShardPortableBackupFileName returns the AFB file name for a shard's portable backup.
func ShardPortableBackupFileName(backupID string, shardID types.ID) string {
	return fmt.Sprintf("%s-%s.afb", backupID, shardID)
}

func MetadataSnapDir(dataDir string, peerID types.ID) string {
	return fmt.Sprintf("%s/metadata/%s/snap", dataDir, peerID)
}

func MetadataStorageDir(dataDir string, peerID types.ID) string {
	return fmt.Sprintf("%s/metadata/%s/storage", dataDir, peerID)
}

func MetadataRaftLogDir(dataDir string, peerID types.ID) string {
	return fmt.Sprintf("%s/metadata/%s/log", dataDir, peerID)
}
