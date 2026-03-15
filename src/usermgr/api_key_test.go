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

package usermgr

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestUser(t *testing.T, um *UserManager, username, password string) {
	t.Helper()
	_, err := um.CreateUser(username, password, nil)
	require.NoError(t, err, "CreateUser(%s) failed", username)
}

func TestCreateApiKey_Success(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	keyID, keySecret, err := um.CreateApiKey("alice", "test key", nil, time.Time{})
	require.NoError(err)

	assert.Len(keyID, apiKeyIDLength)

	// key secret is base64url-encoded 16 bytes = 22 chars
	secretRaw, err := base64.RawURLEncoding.DecodeString(keySecret)
	require.NoError(err, "key secret is not valid base64url")
	assert.Len(secretRaw, apiKeySecretBytes)
}

func TestValidateApiKey_CorrectSecret(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	keyID, keySecret, err := um.CreateApiKey("alice", "test key", nil, time.Time{})
	require.NoError(err)

	username, perms, err := um.ValidateApiKey(keyID, keySecret)
	require.NoError(err)
	assert.Equal("alice", username)
	assert.Empty(perms)
}

func TestValidateApiKey_WrongSecret(t *testing.T) {
	require := require.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	keyID, _, err := um.CreateApiKey("alice", "test key", nil, time.Time{})
	require.NoError(err)

	_, _, err = um.ValidateApiKey(keyID, "wrongsecretvalue12345")
	require.ErrorIs(err, ErrApiKeyInvalid)
}

func TestValidateApiKey_NonexistentKey(t *testing.T) {
	um, _ := newTestUserManager(t)

	_, _, err := um.ValidateApiKey("nonexistentkeyid12345", "somesecret")
	require.ErrorIs(t, err, ErrApiKeyNotFound)
}

func TestValidateApiKey_ExpiredKey(t *testing.T) {
	require := require.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	expiresAt := time.Now().Add(-1 * time.Hour)
	keyID, keySecret, err := um.CreateApiKey("alice", "expired key", nil, expiresAt)
	require.NoError(err)

	// Hash verification happens before expiration check (timing safety),
	// but the result should still be ErrApiKeyExpired.
	_, _, err = um.ValidateApiKey(keyID, keySecret)
	require.ErrorIs(err, ErrApiKeyExpired)
}

func TestValidateApiKey_HashCheckedBeforeExpiry(t *testing.T) {
	require := require.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	expiresAt := time.Now().Add(-1 * time.Hour)
	keyID, _, err := um.CreateApiKey("alice", "expired key", nil, expiresAt)
	require.NoError(err)

	// Wrong secret on an expired key should return ErrApiKeyInvalid (not ErrApiKeyExpired)
	// because hash is checked first.
	_, _, err = um.ValidateApiKey(keyID, "wrongsecretvalue12345")
	require.ErrorIs(err, ErrApiKeyInvalid)
}

func TestListApiKeys_OmitsSensitiveFields(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	_, _, err := um.CreateApiKey("alice", "test key 1", nil, time.Time{})
	require.NoError(err)
	_, _, err = um.CreateApiKey("alice", "test key 2", nil, time.Time{})
	require.NoError(err)

	keys, err := um.ListApiKeys("alice")
	require.NoError(err)
	require.Len(keys, 2)

	for _, key := range keys {
		assert.Empty(key.SecretHash, "SecretHash should be empty in list response")
		assert.Empty(key.SecretSalt, "SecretSalt should be empty in list response")
		assert.Equal("alice", key.Username)
	}
}

func TestDeleteApiKey_Success(t *testing.T) {
	require := require.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	keyID, keySecret, err := um.CreateApiKey("alice", "test key", nil, time.Time{})
	require.NoError(err)

	_, _, err = um.ValidateApiKey(keyID, keySecret)
	require.NoError(err, "should succeed before deletion")

	require.NoError(um.DeleteApiKey("alice", keyID))

	_, _, err = um.ValidateApiKey(keyID, keySecret)
	require.ErrorIs(err, ErrApiKeyNotFound)
}

func TestDeleteApiKey_NotFound(t *testing.T) {
	um, _ := newTestUserManager(t)

	err := um.DeleteApiKey("anyone", "nonexistent")
	require.ErrorIs(t, err, ErrApiKeyNotFound)
}

func TestCreateApiKey_NonexistentUser(t *testing.T) {
	um, _ := newTestUserManager(t)

	_, _, err := um.CreateApiKey("nonexistent", "test key", nil, time.Time{})
	require.ErrorIs(t, err, ErrUserNotFound)
}

func TestCreateApiKey_WithPermissions(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	initialPerms := []Permission{
		{Resource: "orders", ResourceType: ResourceTypeTable, Type: PermissionTypeRead},
		{Resource: "orders", ResourceType: ResourceTypeTable, Type: PermissionTypeWrite},
	}
	for _, perm := range initialPerms {
		require.NoError(um.AddPermissionToUser("alice", perm))
	}

	keyPerms := []Permission{
		{Resource: "orders", ResourceType: ResourceTypeTable, Type: PermissionTypeRead},
	}
	keyID, keySecret, err := um.CreateApiKey("alice", "read-only key", keyPerms, time.Time{})
	require.NoError(err)

	_, perms, err := um.ValidateApiKey(keyID, keySecret)
	require.NoError(err)
	require.Len(perms, 1)
	assert.Equal("orders", perms[0].Resource)
	assert.Equal(PermissionTypeRead, perms[0].Type)
}

func TestCreateApiKey_PrivilegeEscalationPrevented(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	escalatedPerms := []Permission{
		{Resource: "*", ResourceType: "*", Type: PermissionTypeAdmin},
	}
	_, _, err := um.CreateApiKey("alice", "escalated key", escalatedPerms, time.Time{})
	require.ErrorIs(t, err, ErrPrivilegeEscalation)
}

func TestApiKey_PersistenceAcrossReload(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(err)
	t.Cleanup(func() { db.Close() })

	kvDB := &kv.PebbleDB{DB: db}

	um1, err := NewUserManager(kvDB)
	require.NoError(err)
	_, err = um1.CreateUser("alice", "password123", nil)
	require.NoError(err)

	keyID, keySecret, err := um1.CreateApiKey("alice", "persistent key", nil, time.Time{})
	require.NoError(err)

	// Simulate restart by creating new UserManager from same DB
	um2, err := NewUserManager(kvDB)
	require.NoError(err)

	username, _, err := um2.ValidateApiKey(keyID, keySecret)
	require.NoError(err)
	assert.Equal("alice", username)
}
