// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type contentReceipt struct {
	Version  int    `json:"version"`
	Identity string `json:"identity"`
	SHA256   string `json:"sha256"`
	MAC      string `json:"mac"`
}

func (s *Service) signMedia(domain string, raw []byte) string {
	mac := hmac.New(sha256.New, s.mediaKey)
	_, _ = mac.Write([]byte("antfly-source-" + domain + "\x00"))
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *Service) contentIdentity(connection Connection, item Item) string {
	// Credential references and source permissions are not byte identity. Access
	// is rechecked before reuse; rotating a key or changing a reader must not hash
	// a multi-gigabyte immutable object again.
	raw, _ := json.Marshal(struct {
		Provider, Endpoint, URI, Revision, VersionID, Representation string
		Insecure                                                     bool
		Length                                                       int64
	}{connection.Provider, connection.S3Endpoint, item.URL, item.Revision, configString(item.Metadata, "version_id"), configString(item.Metadata, "representation_sha256"), connection.S3Insecure, item.Size})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (s *Service) digestPath(key string) string {
	return filepath.Join(s.directory, "media-digests", key+".json")
}
func (s *Service) receiptMAC(receipt contentReceipt) string {
	receipt.MAC = ""
	raw, _ := json.Marshal(receipt)
	return s.signMedia("content-receipt-v1", raw)
}
func validContentSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}
func (s *Service) verifiedMediaDigest(ctx context.Context, source Source, connection Connection, request Request, item Item) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	key := s.contentIdentity(connection, item)
	connector, err := s.providerConnector(ctx, connection)
	if err != nil {
		return "", err
	}
	connector, err = publicationConnector(connector, source)
	if err != nil {
		return "", err
	}
	media, ok := connector.(MediaConnector)
	if !ok {
		return "", fmt.Errorf("%w: provider does not support streaming media", ErrInvalidInput)
	}
	if err := checkMediaAccess(ctx, connector, media, request, item); err != nil {
		return "", mediaProviderError(err)
	}
	var receipt contentReceipt
	if loadJSON(s.digestPath(key), &receipt, 4096) == nil && receipt.Version == 1 && receipt.Identity == key && validContentSHA(receipt.SHA256) && hmac.Equal([]byte(receipt.MAC), []byte(s.receiptMAC(receipt))) {
		return receipt.SHA256, nil
	}
	digest, err := VerifyMediaDigest(ctx, media, request, item)
	if err != nil {
		return "", err
	}
	receipt = contentReceipt{Version: 1, Identity: key, SHA256: digest}
	receipt.MAC = s.receiptMAC(receipt)
	if err := writeJSON(s.digestPath(key), receipt, 4096); err != nil {
		return "", err
	}
	return receipt.SHA256, nil
}
func sourceMediaBinding(source Source) string {
	source.Alias = ""
	source.Streams = nil // Producer/search bindings do not revoke original bytes.
	source.Knowledge = nil
	source.SearchVersion = 0
	source.Artifacts = nil
	source.Processing = nil
	source.PermissionTTLSeconds = 0
	raw, _ := json.Marshal(source)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func connectionMediaBinding(connection Connection) string {
	raw, _ := json.Marshal(connection)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type mediaPinIdentity struct {
	SourceID          string `json:"source_id"`
	SourceBinding     string `json:"source_binding"`
	Connection        string `json:"connection"`
	ConnectionBinding string `json:"connection_binding"`
	Item              Item   `json:"item"`
}
type mediaPinV2 struct {
	Version int `json:"version"`
	mediaPinIdentity
	SHA256          string `json:"sha256"`
	ContentIdentity string `json:"content_identity"`
	MAC             string `json:"mac"`
}

func pinIdentityID(identity mediaPinIdentity) string {
	raw, _ := json.Marshal(identity)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (s *Service) pinReceiptMAC(pin mediaPinV2) string {
	pin.MAC = ""
	raw, _ := json.Marshal(pin)
	return s.signMedia("media-pin-v2", raw)
}
func (s *Service) readMediaPin(id string) (mediaPinV2, error) {
	var pin mediaPinV2
	if err := loadJSON(s.pinPath(id), &pin, 1<<20); err != nil {
		return pin, err
	}
	if pin.Version != 2 || pinIdentityID(pin.mediaPinIdentity) != id || !validContentSHA(pin.SHA256) || !hmac.Equal([]byte(pin.MAC), []byte(s.pinReceiptMAC(pin))) {
		return pin, fmt.Errorf("invalid verified media pin")
	}
	return pin, nil
}
func (s *Service) removeMediaAuthority(sourceID string) error {
	s.authorities.mu.Lock()
	defer s.authorities.mu.Unlock()
	s.authorities.generation++
	if old, ok := s.authorities.entries[sourceID]; ok {
		s.authorities.bytes -= old.size
		delete(s.authorities.entries, sourceID)
	}
	err := os.Remove(s.authorityPath(sourceID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Only receipts reachable from successfully published pins survive a full sync.
// This bounds disk use across provider revisions without retaining media bytes.
func (s *Service) pruneMediaDigests(keep map[string]bool) error {
	digests := map[string]bool{}
	for name := range keep {
		pin, err := s.readMediaPin(strings.TrimSuffix(name, ".json"))
		if err != nil {
			return err
		}
		digests[pin.ContentIdentity+".json"] = true
	}
	return s.pruneUnreferencedMediaDigests(digests)
}

func (s *Service) pruneUnreferencedMediaDigests(digests map[string]bool) error {
	directory := filepath.Join(s.directory, "media-digests")
	dir, err := os.Open(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, err := dir.ReadDir(128)
		if err != nil && err != io.EOF {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || digests[name] || !strings.HasSuffix(name, ".json") || !validContentSHA(strings.TrimSuffix(name, ".json")) {
				continue
			}
			if err := os.Remove(filepath.Join(directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err == io.EOF {
			return dir.Sync()
		}
	}
}
