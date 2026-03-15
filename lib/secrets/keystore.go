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

package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"golang.org/x/crypto/pbkdf2"
)

const (
	// DefaultKeystorePath is the default location for the keystore file
	DefaultKeystorePath = "/etc/antfly/keystore"

	// KeystoreVersion is the current keystore format version
	KeystoreVersion = 1

	// Encryption parameters
	saltSize     = 32
	nonceSize    = 12
	keySize      = 32 // AES-256
	pbkdf2Rounds = 100000
)

// Entry represents a single secret entry in the keystore
type Entry struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value"` // encrypted value
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Keystore represents the encrypted secrets storage
type Keystore struct {
	Version int               `json:"version"`
	Salt    []byte            `json:"salt"`
	Entries map[string]*Entry `json:"entries"`

	mu            sync.RWMutex
	path          string
	password      string
	encryptionKey []byte
}

// keystoreFile represents the on-disk format
type keystoreFile struct {
	Version int               `json:"version"`
	Salt    string            `json:"salt"` // base64 encoded
	Entries map[string]*entry `json:"entries"`
}

type entry struct {
	Key       string `json:"key"`
	Value     string `json:"value"` // base64 encoded encrypted value
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// NewKeystore creates a new empty keystore
func NewKeystore(path string, password string) (*Keystore, error) {
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	key := deriveKey(password, salt)

	return &Keystore{
		Version:       KeystoreVersion,
		Salt:          salt,
		Entries:       make(map[string]*Entry),
		path:          path,
		password:      password,
		encryptionKey: key,
	}, nil
}

// LoadKeystore loads an existing keystore from disk
func LoadKeystore(path string, password string) (*Keystore, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: internal file I/O, not user-controlled; G703: internal path with traversal protection
	if err != nil {
		return nil, fmt.Errorf("failed to read keystore: %w", err)
	}

	var kf keystoreFile
	if err := json.Unmarshal(data, &kf); err != nil {
		return nil, fmt.Errorf("failed to parse keystore: %w", err)
	}

	if kf.Version != KeystoreVersion {
		return nil, fmt.Errorf("unsupported keystore version: %d", kf.Version)
	}

	salt, err := base64.StdEncoding.DecodeString(kf.Salt)
	if err != nil {
		return nil, fmt.Errorf("failed to decode salt: %w", err)
	}

	key := deriveKey(password, salt)

	entries := make(map[string]*Entry)
	for k, e := range kf.Entries {
		value, err := base64.StdEncoding.DecodeString(e.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode entry %s: %w", k, err)
		}

		createdAt, err := time.Parse(time.RFC3339, e.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse created_at for %s: %w", k, err)
		}

		updatedAt, err := time.Parse(time.RFC3339, e.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse updated_at for %s: %w", k, err)
		}

		entries[k] = &Entry{
			Key:       e.Key,
			Value:     value,
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}
	}

	return &Keystore{
		Version:       kf.Version,
		Salt:          salt,
		Entries:       entries,
		path:          path,
		password:      password,
		encryptionKey: key,
	}, nil
}

// Save writes the keystore to disk
func (k *Keystore) Save() error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entries := make(map[string]*entry)
	for key, e := range k.Entries {
		entries[key] = &entry{
			Key:       e.Key,
			Value:     base64.StdEncoding.EncodeToString(e.Value),
			CreatedAt: e.CreatedAt.Format(time.RFC3339),
			UpdatedAt: e.UpdatedAt.Format(time.RFC3339),
		}
	}

	kf := keystoreFile{
		Version: k.Version,
		Salt:    base64.StdEncoding.EncodeToString(k.Salt),
		Entries: entries,
	}

	data, err := json.MarshalIndent(kf, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal keystore: %w", err)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(k.path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create keystore directory: %w", err)
	}

	// Write to temporary file first, then rename (atomic)
	tmpPath := k.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write keystore: %w", err)
	}

	if err := os.Rename(tmpPath, k.path); err != nil {
		_ = os.Remove(tmpPath) // Best effort cleanup, ignore error
		return fmt.Errorf("failed to rename keystore: %w", err)
	}

	return nil
}

// Add adds or updates a secret in the keystore
func (k *Keystore) Add(key string, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	encryptedValue, err := k.encrypt(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt value: %w", err)
	}

	now := time.Now()

	if existing, ok := k.Entries[key]; ok {
		existing.Value = encryptedValue
		existing.UpdatedAt = now
	} else {
		k.Entries[key] = &Entry{
			Key:       key,
			Value:     encryptedValue,
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	return nil
}

// Get retrieves and decrypts a secret from the keystore
func (k *Keystore) Get(key string) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entry, ok := k.Entries[key]
	if !ok {
		return nil, fmt.Errorf("secret not found: %s", key)
	}

	value, err := k.decrypt(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt value: %w", err)
	}

	return value, nil
}

// GetString retrieves a secret as a string
func (k *Keystore) GetString(key string) (string, error) {
	value, err := k.Get(key)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// Has checks if a key exists in the keystore
func (k *Keystore) Has(key string) bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	_, ok := k.Entries[key]
	return ok
}

// Remove removes a secret from the keystore
func (k *Keystore) Remove(key string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, ok := k.Entries[key]; !ok {
		return fmt.Errorf("secret not found: %s", key)
	}

	delete(k.Entries, key)
	return nil
}

// List returns all secret keys (not values)
func (k *Keystore) List() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()

	keys := make([]string, 0, len(k.Entries))
	for key := range k.Entries {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// encrypt encrypts data using AES-256-GCM
func (k *Keystore) encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(k.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// decrypt decrypts data using AES-256-GCM
func (k *Keystore) decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(k.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// deriveKey derives an encryption key from password and salt using PBKDF2
func deriveKey(password string, salt []byte) []byte {
	return pbkdf2.Key([]byte(password), salt, pbkdf2Rounds, keySize, sha256.New)
}

// KeystoreExists checks if a keystore file exists at the given path
func KeystoreExists(path string) bool {
	_, err := os.Stat(path) //nolint:gosec // G703: internal path with traversal protection
	return err == nil
}
