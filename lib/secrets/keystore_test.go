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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewKeystore(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)
	require.NotNil(t, ks)

	assert.Equal(t, KeystoreVersion, ks.Version)
	assert.Len(t, ks.Salt, saltSize)
	assert.Empty(t, ks.Entries)
}

func TestKeystoreAddAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	// Add a secret
	err = ks.Add("test.key", []byte("secret-value"))
	require.NoError(t, err)

	// Get the secret back
	value, err := ks.Get("test.key")
	require.NoError(t, err)
	assert.Equal(t, "secret-value", string(value))

	// GetString variant
	strValue, err := ks.GetString("test.key")
	require.NoError(t, err)
	assert.Equal(t, "secret-value", strValue)
}

func TestKeystoreUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	// Add initial value
	err = ks.Add("test.key", []byte("initial"))
	require.NoError(t, err)

	// Update value
	err = ks.Add("test.key", []byte("updated"))
	require.NoError(t, err)

	// Verify updated value
	value, err := ks.GetString("test.key")
	require.NoError(t, err)
	assert.Equal(t, "updated", value)
}

func TestKeystoreRemove(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	// Add a secret
	err = ks.Add("test.key", []byte("value"))
	require.NoError(t, err)

	// Verify it exists
	assert.True(t, ks.Has("test.key"))

	// Remove it
	err = ks.Remove("test.key")
	require.NoError(t, err)

	// Verify it's gone
	assert.False(t, ks.Has("test.key"))

	// Try to get removed key
	_, err = ks.Get("test.key")
	assert.Error(t, err)

	// Try to remove non-existent key
	err = ks.Remove("non.existent")
	assert.Error(t, err)
}

func TestKeystoreList(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	// Empty keystore
	assert.Empty(t, ks.List())

	// Add multiple secrets
	err = ks.Add("aws.access_key", []byte("key1"))
	require.NoError(t, err)
	err = ks.Add("aws.secret_key", []byte("key2"))
	require.NoError(t, err)
	err = ks.Add("openai.api_key", []byte("key3"))
	require.NoError(t, err)

	// List should return sorted keys
	keys := ks.List()
	assert.Equal(t, []string{"aws.access_key", "aws.secret_key", "openai.api_key"}, keys)
}

func TestKeystoreSaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")
	password := "test-password"

	// Create and populate keystore
	ks1, err := NewKeystore(path, password)
	require.NoError(t, err)

	err = ks1.Add("secret1", []byte("value1"))
	require.NoError(t, err)
	err = ks1.Add("secret2", []byte("value2"))
	require.NoError(t, err)

	// Save to disk
	err = ks1.Save()
	require.NoError(t, err)

	// Verify file exists
	assert.True(t, KeystoreExists(path))

	// Load from disk
	ks2, err := LoadKeystore(path, password)
	require.NoError(t, err)

	// Verify loaded data
	value1, err := ks2.GetString("secret1")
	require.NoError(t, err)
	assert.Equal(t, "value1", value1)

	value2, err := ks2.GetString("secret2")
	require.NoError(t, err)
	assert.Equal(t, "value2", value2)

	assert.Equal(t, ks1.List(), ks2.List())
}

func TestKeystoreWrongPassword(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	// Create with one password
	ks1, err := NewKeystore(path, "correct-password")
	require.NoError(t, err)

	err = ks1.Add("secret", []byte("value"))
	require.NoError(t, err)

	err = ks1.Save()
	require.NoError(t, err)

	// Try to load with wrong password
	ks2, err := LoadKeystore(path, "wrong-password")
	require.NoError(t, err) // Loading succeeds

	// But decryption should fail
	_, err = ks2.Get("secret")
	assert.Error(t, err)
}

func TestKeystoreEncryption(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	plaintext := []byte("sensitive-data")

	// Encrypt
	ciphertext, err := ks.encrypt(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, ciphertext)

	// Decrypt
	decrypted, err := ks.decrypt(ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)

	// Each encryption should produce different ciphertext (due to nonce)
	ciphertext2, err := ks.encrypt(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, ciphertext, ciphertext2)

	// But both should decrypt to same plaintext
	decrypted2, err := ks.decrypt(ciphertext2)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted2)
}

func TestKeystoreFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	err = ks.Add("secret", []byte("value"))
	require.NoError(t, err)

	err = ks.Save()
	require.NoError(t, err)

	// Check file permissions (should be 0600)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestKeystoreMultilineValues(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	// Test with multiline JSON (like service account keys)
	jsonValue := `{
  "type": "service_account",
  "project_id": "my-project",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n"
}`

	err = ks.Add("gcp.credentials", []byte(jsonValue))
	require.NoError(t, err)

	err = ks.Save()
	require.NoError(t, err)

	// Load and verify
	ks2, err := LoadKeystore(path, "test-password")
	require.NoError(t, err)

	retrieved, err := ks2.GetString("gcp.credentials")
	require.NoError(t, err)
	assert.JSONEq(t, jsonValue, retrieved)
}
