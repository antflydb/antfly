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
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Secrets are separate from profiles, pins and catalogs. A domain-separated
// encryption key is derived from the operator's stable bootstrap media key.
// Key names are authenticated too, so ciphertext cannot move between entries.
type encryptedSourceSecret struct {
	Version           int
	Nonce, Ciphertext []byte
}

func validSecretKey(key string) bool { return validConnectionName(strings.ReplaceAll(key, ".", "_")) }
func (s *Service) secretPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.directory, "secrets", hex.EncodeToString(sum[:])+".json")
}
func (s *Service) secretCipher() (cipher.AEAD, error) {
	mac := hmac.New(sha256.New, s.mediaKey)
	_, _ = mac.Write([]byte("antfly-source-secret-encryption-v1"))
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (s *Service) putSecret(key, value string) error {
	aead, err := s.secretCipher()
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	record := encryptedSourceSecret{Version: 1, Nonce: nonce, Ciphertext: aead.Seal(nil, nonce, []byte(value), []byte(key))}
	if err := os.MkdirAll(filepath.Join(s.directory, "secrets"), 0700); err != nil {
		return err
	}
	if err := writeJSON(s.secretPath(key), record, 256<<10); err != nil {
		return err
	}
	s.invalidateSecret(key)
	return nil
}
func (s *Service) resolveSecret(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !validSecretKey(key) {
		return "", fmt.Errorf("invalid secret key")
	}
	var record encryptedSourceSecret
	if err := loadJSON(s.secretPath(key), &record, 256<<10); err != nil {
		return "", fmt.Errorf("source secret unavailable")
	}
	aead, err := s.secretCipher()
	if err != nil {
		return "", err
	}
	if record.Version != 1 || len(record.Nonce) != aead.NonceSize() {
		return "", fmt.Errorf("invalid source secret")
	}
	plaintext, err := aead.Open(nil, record.Nonce, record.Ciphertext, []byte(key))
	if err != nil {
		return "", fmt.Errorf("source secret authentication failed")
	}
	return string(plaintext), nil
}

func (s *Service) checkBootstrapKey() error {
	mac := hmac.New(sha256.New, s.mediaKey)
	_, _ = mac.Write([]byte("antfly-source-bootstrap-key-check-v1"))
	expected := hex.EncodeToString(mac.Sum(nil))
	path := filepath.Join(s.directory, "bootstrap-key-check.json")
	var persisted string
	if err := loadJSON(path, &persisted, 4096); err == nil {
		if !hmac.Equal([]byte(persisted), []byte(expected)) {
			return fmt.Errorf("source bootstrap media key changed; migrate the keystore before restarting")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeJSON(path, expected, 4096)
}

func removeSourceSecret(path string) error {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
