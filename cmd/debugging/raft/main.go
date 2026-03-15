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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	// Using NoopLogger
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft" // Adjust import path if needed
	etcdRaft "go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

func main() {
	nodeID := flag.Uint64("node", 1, "Node ID")
	shard := flag.String("shard", "", "Shard ID")
	flag.Parse()

	if *nodeID == 0 || *shard == "" {
		log.Fatal("Node ID and Shard ID must be set")
	}

	shardID, err := types.IDFromString(*shard)
	if err != nil {
		log.Fatalf("Failed to parse Shard ID: %v", err)
	}
	dir := common.RaftLogDir(common.RootAntflyDir, shardID, types.ID(*nodeID))
	log.Printf("Attempting to open Pebble storage at: %s", dir)

	// Check if directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Fatalf("Directory does not exist: %s", dir)
	}

	zaplog, _ := zap.NewDevelopment()
	ps, err := raft.NewPebbleStorage(zaplog, dir, nil)
	if err != nil {
		log.Fatalf("Failed to open Pebble storage at %s: %v", dir, err)
	}
	defer func() {
		if err := ps.Close(); err != nil {
			log.Printf("Error closing Pebble storage: %v", err)
		}
	}()

	log.Println("Successfully opened Pebble storage.")

	// Call methods and print results
	fmt.Println("--- Calling PebbleStorage Methods ---")

	// InitialState
	hs, cs, err := ps.InitialState()
	if err != nil {
		log.Printf("Error getting InitialState: %v", err)
	} else {
		fmt.Printf("InitialState HardState: %+v\n", hs)
		fmt.Printf("InitialState ConfState: %+v\n", cs)
	}

	// LastIndex
	lastIndex, err := ps.LastIndex()
	if err != nil {
		log.Printf("Error getting LastIndex: %v", err)
	} else {
		fmt.Printf("LastIndex: %d\n", lastIndex)
	}

	// FirstIndex
	firstIndex, err := ps.FirstIndex()
	if err != nil {
		log.Printf("Error getting FirstIndex: %v", err)
	} else {
		fmt.Printf("FirstIndex: %d\n", firstIndex)
	}

	// Term
	if lastIndex > 0 { // Only call Term if there are entries
		term, err := ps.Term(lastIndex)
		if err != nil {
			log.Printf("Error getting Term(%d): %v", lastIndex, err)
		} else {
			fmt.Printf("Term(%d): %d\n", lastIndex, term)
		}
	} else {
		fmt.Println("Skipping Term call as LastIndex is 0.")
	}

	entries, err := ps.GetAllEntries()
	if err != nil {
		log.Printf("Error getting AllEntries: %v", err)
	} else {
		if len(entries) > 0 {
			fmt.Printf("First Entry: %v\n", entries[0])
			fmt.Printf("Last Entry: %v\n", entries[len(entries)-1])
		}
	}

	// Entries (example: get last 5 entries up to lastIndex)
	if lastIndex > 0 {
		lo := uint64(0)
		if lastIndex >= 5 {
			lo = lastIndex - 4
		} else {
			lo = firstIndex // Ensure lo >= firstIndex
		}
		hi := lastIndex + 1
		maxSize := uint64(10 * 1024 * 1024) // Example maxSize 10MB

		fmt.Printf("Attempting to get Entries (lo=%d, hi=%d, maxSize=%d)\n", lo, hi, maxSize)
		entries, err := ps.Entries(lo, hi, maxSize)
		if err != nil {
			log.Printf("Error getting Entries(%d, %d, %d): %v", lo, hi, maxSize, err)
		} else {
			fmt.Printf("Entries (%d total):\n", len(entries))
			for i, entry := range entries {
				entryType := entry.Type.String()
				dataSize := len(entry.Data)
				fmt.Printf("  [%d] Index: %d, Term: %d, Type: %s, Data Size: %d\n", i, entry.Index, entry.Term, entryType, dataSize)
				// Avoid printing large data payloads
				// if dataSize > 0 && dataSize < 100 {
				// 	fmt.Printf("    Data: %x\n", entry.Data)
				// } else if dataSize >= 100 {
				//  fmt.Printf("    Data: (omitted, %d bytes)\n", dataSize)
				// }
			}
		}
	} else {
		fmt.Println("Skipping Entries call as LastIndex is 0.")
	}

	// Snapshot
	snapshot, err := ps.Snapshot()
	if err != nil {
		// ErrSnapshotTemporarilyUnavailable is common if no snapshot exists or compacting
		if err == etcdRaft.ErrSnapshotTemporarilyUnavailable {
			fmt.Println("Snapshot: Temporarily Unavailable (might be normal)")
		} else {
			log.Printf("Error getting Snapshot: %v", err)
		}
	} else if etcdRaft.IsEmptySnap(snapshot) {
		fmt.Println("Snapshot: Is Empty")
	} else {
		fmt.Printf("Snapshot Metadata Index: %d\n", snapshot.Metadata.Index)
		fmt.Printf("Snapshot Metadata Term: %d\n", snapshot.Metadata.Term)
		fmt.Printf("Snapshot Metadata ConfState: %+v\n", snapshot.Metadata.ConfState)
		fmt.Printf("Snapshot Data Size: %d bytes\n", len(snapshot.Data))
	}

	fmt.Println("--- Finished ---")
}
