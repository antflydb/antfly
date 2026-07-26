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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/goccy/go-json"
)

func TestBackupConfigDoesNotSerializeResolvedLocation(t *testing.T) {
	config := BackupConfig{
		BackupID:         "backup-1",
		Connection:       "filesystem",
		Location:         "file:///logical",
		ResolvedLocation: "file:///private/authorized/logical",
		Format:           BackupFormatPortable,
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal backup config: %v", err)
	}
	if strings.Contains(string(data), "private/authorized") ||
		strings.Contains(string(data), "resolved") {
		t.Fatalf("resolved location leaked into serialized backup config: %s", data)
	}
}

func TestVerifyBackupArtifactRejectsSameSizeCorruption(t *testing.T) {
	body := []byte("portable-artifact")
	digest := sha256.Sum256(body)
	artifact := BackupArtifactIntegrity{
		Name:      "backup-1-1.afb",
		SizeBytes: uint64(len(body)),
		SHA256:    hex.EncodeToString(digest[:]),
	}
	if err := VerifyBackupArtifact(
		context.Background(),
		bytes.NewReader(body),
		artifact,
	); err != nil {
		t.Fatalf("verify valid artifact: %v", err)
	}

	corrupt := append([]byte(nil), body...)
	corrupt[0] ^= 0xff
	err := VerifyBackupArtifact(
		context.Background(),
		bytes.NewReader(corrupt),
		artifact,
	)
	if !errors.Is(err, ErrBackupArtifactIntegrityMismatch) {
		t.Fatalf("expected integrity mismatch, got %v", err)
	}
}

func TestPeerSet_JSONRoundtrip(t *testing.T) {
	original := NewPeerSet()
	original.Add(types.ID(1))
	original.Add(types.ID(2))
	original.Add(types.ID(3))

	jsonData, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PeerSet: %v", err)
	}

	var unmarshalled PeerSet
	err = json.Unmarshal(jsonData, &unmarshalled)
	if err != nil {
		t.Fatalf("Failed to unmarshal PeerSet: %v", err)
	}

	if len(original) != len(unmarshalled) {
		t.Errorf("Expected length %d, got %d", len(original), len(unmarshalled))
	}

	if !reflect.DeepEqual(original, unmarshalled) {
		t.Errorf("Expected %v, got %v", original, unmarshalled)
	}

	// Test with an empty PeerSet
	emptyOriginal := NewPeerSet()
	jsonDataEmpty, err := json.Marshal(emptyOriginal)
	if err != nil {
		t.Fatalf("Failed to marshal empty PeerSet: %v", err)
	}

	var unmarshalledEmpty PeerSet
	err = json.Unmarshal(jsonDataEmpty, &unmarshalledEmpty)
	if err != nil {
		t.Fatalf("Failed to unmarshal empty PeerSet: %v", err)
	}

	if len(unmarshalledEmpty) != 0 {
		t.Errorf("Expected empty PeerSet, got %v", unmarshalledEmpty)
	}
	if !reflect.DeepEqual(emptyOriginal, unmarshalledEmpty) {
		t.Errorf("Expected %v, got %v for empty PeerSet", emptyOriginal, unmarshalledEmpty)
	}

	// Test unmarshalling into an existing PeerSet
	existing := NewPeerSet()
	existing.Add(types.ID(10))
	err = json.Unmarshal(jsonData, &existing) // jsonData contains {1,2,3}
	if err != nil {
		t.Fatalf("Failed to unmarshal into existing PeerSet: %v", err)
	}
	if len(existing) != 3 { // Should be overwritten
		t.Errorf(
			"Expected length 3 after unmarshalling into existing, got %d. Got: %v",
			len(existing),
			existing,
		)
	}
	if !existing.Contains(types.ID(1)) || !existing.Contains(types.ID(2)) ||
		!existing.Contains(types.ID(3)) {
		t.Errorf(
			"Existing PeerSet does not contain expected elements after unmarshal. Got: %v",
			existing,
		)
	}
	if existing.Contains(types.ID(10)) {
		t.Errorf(
			"Existing PeerSet should not contain old elements after unmarshal. Got: %v",
			existing,
		)
	}
}
