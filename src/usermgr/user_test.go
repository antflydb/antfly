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
	"sort"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// newTestUserManager creates a new UserManager with a test DB and Enforcer.
func newTestUserManager(t *testing.T) (*UserManager, *pebble.DB) {
	t.Helper()
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	um, err := NewUserManager(&kv.PebbleDB{DB: db})
	require.NoError(t, err, "NewUserManager() failed")
	return um, db
}

// setupTestDB creates a new temporary PebbleDB for testing.
func setupTestDB(t *testing.T) *pebble.DB {
	t.Helper()
	// Use an in-memory FS for faster tests
	db, err := pebble.Open("", pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err, "Failed to open pebble DB")
	return db
}

func TestNewUserManager(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, db := newTestUserManager(t)
	require.NotNil(um, "NewUserManager() returned nil UserManager")
	assert.NotNil(um.passwordHashes, "UserManager.passwordHashes is nil")
	assert.Empty(um.passwordHashes, "UserManager.passwordHashes not initialized correctly")
	assert.NotNil(um.enforcer, "UserManager.enforcer not initialized")

	// Test loading existing users (password hashes)
	// 1. Create a user password info directly in DB
	testUserPI := &UserPasswordInfo{
		Username:     "testload",
		PasswordHash: []byte("hash"),
	}
	key := []byte(userPasswordPrefix + testUserPI.Username)
	data, _ := json.Marshal(testUserPI)
	require.NoError(
		db.Set(key, data, pebble.Sync),
		"Failed to set up test user password info in DB",
	)

	// 2. Create new UserManager, it should load the user's password hash
	um2, err := NewUserManager(&kv.PebbleDB{DB: db})
	require.NoError(err, "NewUserManager() with existing data failed")

	err = um2.AddPermissionToUser("testload", Permission{
		ResourceType: "table", Resource: "table1", Type: PermissionTypeRead,
	})
	require.NoError(err, "AddPermissionToUser() failed")
	loadedHash, ok := um2.passwordHashes["testload"]
	require.True(ok, "NewUserManager() failed to load existing user password hash")
	assert.Equal(testUserPI.PasswordHash, loadedHash, "Loaded password hash mismatch")

	// Check if Casbin policies were loaded by the enforcer associated with um2
	roles, err := um2.GetPermissionsForUser("testload")
	require.NoError(err, "GetPermissionsForUser for loaded user failed")
	expectedRole := Permission{Resource: "table1", ResourceType: "table", Type: PermissionTypeRead}
	require.Len(roles, 1, "Loaded user roles count mismatch")
	assert.Equal(expectedRole, roles[0], "Loaded user roles mismatch")
}

func TestUserManager_CreateUser(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, db := newTestUserManager(t)
	initialRoles := []Permission{
		{Resource: "table1", ResourceType: ResourceTypeTable, Type: PermissionTypeRead},
		{Resource: "globalres", ResourceType: ResourceTypeTable, Type: PermissionTypeAdmin},
	}
	user, err := um.CreateUser("testuser", "password123", initialRoles)
	require.NoError(err, "CreateUser() failed")
	require.NotNil(user, "CreateUser() returned nil user")
	assert.Equal("testuser", user.Username, "CreateUser() username mismatch")
	assert.NotEmpty(user.PasswordHash, "CreateUser() password hash is empty")

	// Verify user password hash is in memory
	_, exists := um.passwordHashes["testuser"]
	assert.True(exists, "User password hash not found in UserManager.passwordHashes after creation")

	// Verify user password info is in DB
	key := []byte(userPasswordPrefix + "testuser")
	val, closer, errDb := db.Get(key)
	require.NoError(errDb, "Failed to get user password info from DB")
	var pi UserPasswordInfo
	errUnmarshal := json.Unmarshal(val, &pi)
	require.NoError(closer.Close())
	require.NoError(errUnmarshal, "Failed to unmarshal UserPasswordInfo")
	assert.Equal("testuser", pi.Username, "UserPasswordInfo in DB username mismatch")
	assert.NoError(
		bcrypt.CompareHashAndPassword(pi.PasswordHash, []byte("password123")),
		"UserPasswordInfo in DB password hash mismatch",
	)

	// Verify roles in Casbin
	retrievedRoles, err := um.GetPermissionsForUser("testuser")
	require.NoError(err, "GetPermissionsForUser() after CreateUser() failed")
	// Sort for stable comparison
	sortRoles(retrievedRoles)
	sortRoles(initialRoles)
	assert.Equal(initialRoles, retrievedRoles, "CreateUser() roles in Casbin mismatch")
	// Direct check with enforcer
	// for _, r := range retrievedRoles {
	// 	if typ, obj, act := r.toCasbinPolicyComponents(); !enforcer.HasPolicy("testuser", typ, obj, act) {
	// 		t.Errorf("Enforcer missing policy for role: user=testuser, obj=%s, act=%s", obj, act)
	// 	}
	// }

	// Test creating existing user
	_, err = um.CreateUser("testuser", "anotherpassword", nil)
	assert.ErrorIs(err, ErrUserExists, "CreateUser() with existing user error mismatch")
}

func sortRoles(roles []Permission) {
	sort.Slice(roles, func(i, j int) bool {
		if roles[i].ResourceType != roles[j].ResourceType {
			return roles[i].ResourceType < roles[j].ResourceType
		}
		if roles[i].Resource != roles[j].Resource {
			return roles[i].Resource < roles[j].Resource
		}
		return roles[i].Type < roles[j].Type
	})
}

func TestUserManager_GetUser(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	// CreateUser now returns a User struct with PasswordHash populated.
	// No roles are attached to the User struct itself.
	createdUser, _ := um.CreateUser("testuser", "password123", nil)

	// Test get existing user
	user, err := um.GetUser("testuser")
	require.NoError(err, "GetUser() failed")
	// Compare relevant fields (Username, PasswordHash), not the whole struct if it might contain unexported fields or pointers.
	assert.Equal(createdUser.Username, user.Username, "GetUser() username mismatch")
	assert.Equal(createdUser.PasswordHash, user.PasswordHash, "GetUser() password hash mismatch")

	// Test get non-existent user
	_, err = um.GetUser("nonexistentuser")
	assert.ErrorIs(err, ErrUserNotFound, "GetUser() for non-existent user error mismatch")
}

func TestUserManager_AuthenticateUser(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	createdUser, _ := um.CreateUser("authuser", "securepassword", nil)

	// Test successful authentication
	authedUser, err := um.AuthenticateUser("authuser", "securepassword")
	require.NoError(err, "AuthenticateUser() successful auth failed")
	assert.Equal(createdUser.Username, authedUser.Username, "AuthenticateUser() username mismatch")
	assert.Equal(
		createdUser.PasswordHash,
		authedUser.PasswordHash,
		"AuthenticateUser() password hash mismatch",
	)

	// Test authentication with wrong password
	_, err = um.AuthenticateUser("authuser", "wrongpassword")
	assert.ErrorIs(err, ErrInvalidPassword, "AuthenticateUser() with wrong password error mismatch")

	// Test authentication for non-existent user
	_, err = um.AuthenticateUser("nonexistentuser", "password")
	assert.ErrorIs(err, ErrUserNotFound, "AuthenticateUser() for non-existent user error mismatch")
}

func TestUserManager_AddPermissionToUser(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	initialRole := Permission{
		Resource:     "table1",
		ResourceType: ResourceTypeTable,
		Type:         PermissionTypeRead,
	}
	_, _ = um.CreateUser("roleuser", "password", []Permission{initialRole})

	// Add new role
	newRole := Permission{
		Resource:     "table2",
		ResourceType: ResourceTypeTable,
		Type:         PermissionTypeWrite,
	}
	err := um.AddPermissionToUser("roleuser", newRole)
	require.NoError(err, "AddPermissionToUser() new role failed")

	// Verify with GetPermissionsForUser
	roles, _ := um.GetPermissionsForUser("roleuser")
	expectedRoles := []Permission{initialRole, newRole}
	sortRoles(roles)
	sortRoles(expectedRoles)
	assert.Equal(
		expectedRoles,
		roles,
		"AddPermissionToUser() GetPermissionsForUser() roles mismatch",
	)
	// Verify directly with enforcer
	// if typ, obj, act := newRole.toCasbinPolicyComponents(); !enforcer.HasPolicy("roleuser", typ, obj, act) {
	// 	t.Errorf("Enforcer missing policy for new role: user=roleuser, obj=%s, act=%s", obj, act)
	// }

	// Update existing role (Casbin's AddPolicy is idempotent for existing exact policy,
	// so adding the same role again does nothing. To "update" a role type,
	// one might remove the old and add the new, or Casbin's model might handle this differently.
	// Our AddRoleToUser simply adds a new policy. If the "resource+type" is the same but "roletype"
	// is different, it's a new distinct policy in Casbin.)
	// Let's test adding another permission for the same resource but different action.
	updatedRole := Permission{
		Resource:     "table1",
		ResourceType: ResourceTypeTable,
		Type:         PermissionTypeAdmin,
	}
	err = um.AddPermissionToUser("roleuser", updatedRole)
	require.NoError(err, "AddPermissionToUser() adding another role type for same resource failed")
	roles, _ = um.GetPermissionsForUser("roleuser")
	expectedRolesAfterUpdate := []Permission{initialRole, newRole, updatedRole}
	sortRoles(roles)
	sortRoles(expectedRolesAfterUpdate)
	assert.Equal(expectedRolesAfterUpdate, roles, "AddPermissionToUser() updated roles mismatch")

	// if typUpd, objUpd, actUpd := updatedRole.toCasbinPolicyComponents(); !enforcer.HasPolicy("roleuser", typUpd, objUpd, actUpd) {
	// 	t.Errorf("Enforcer missing policy for updated role: user=roleuser, obj=%s, act=%s", objUpd, actUpd)
	// }

	// Add role to non-existent user
	err = um.AddPermissionToUser(
		"nonexistentuser",
		Permission{Resource: "table1", ResourceType: ResourceTypeTable, Type: PermissionTypeRead},
	)
	assert.ErrorIs(
		err,
		ErrUserNotFound,
		"AddPermissionToUser() for non-existent user error mismatch",
	)
}

func TestUserManager_RemoveRoleFromUser(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, _ := newTestUserManager(t)
	role1 := Permission{
		Resource:     "table1",
		ResourceType: ResourceTypeTable,
		Type:         PermissionTypeRead,
	}
	role2 := Permission{
		Resource:     "table2",
		ResourceType: ResourceTypeTable,
		Type:         PermissionTypeWrite,
	}
	// Add a more specific role for table1 to test removal of all permissions for a resource
	role1Admin := Permission{
		Resource:     "table1",
		ResourceType: ResourceTypeTable,
		Type:         PermissionTypeAdmin,
	}
	_, _ = um.CreateUser("roleuser", "password", []Permission{role1, role2, role1Admin})

	// Remove existing role for "table1" (should remove both RoleReadOnly and RoleAdmin for table1)
	err := um.RemovePermissionFromUser("roleuser", "table1", ResourceTypeTable)
	require.NoError(err, "RemovePermissionFromUser() failed")

	roles, _ := um.GetPermissionsForUser("roleuser")
	expectedRolesAfterRemove := []Permission{role2} // Only role2 should remain
	sortRoles(roles)
	sortRoles(expectedRolesAfterRemove)
	assert.Equal(expectedRolesAfterRemove, roles, "RemovePermissionFromUser() roles mismatch")
	// Verify with enforcer
	// if typ1, obj1, act1 := role1.toCasbinPolicyComponents(); enforcer.HasPolicy("roleuser", typ1, obj1, act1) {
	// 	t.Errorf("Enforcer still has policy for removed role: user=roleuser, obj=%s, act=%s", obj1, act1)
	// }
	// if typ1Admin, obj1Admin, act1Admin := role1Admin.toCasbinPolicyComponents(); enforcer.HasPolicy("roleuser", typ1Admin, obj1Admin, act1Admin) {
	// 	t.Errorf("Enforcer still has policy for removed admin role: user=roleuser, obj=%s, act=%s", obj1Admin, act1Admin)
	// }
	// if typ2, obj2, act2 := role2.toCasbinPolicyComponents(); !enforcer.HasPolicy("roleuser", typ2, obj2, act2) {
	// 	t.Errorf("Enforcer missing policy for remaining role: user=roleuser, obj=%s, act=%s", obj2, act2)
	// }

	// Remove non-existent role from user
	err = um.RemovePermissionFromUser("roleuser", "tableNonExistent", ResourceTypeTable)
	assert.ErrorIs(
		err,
		ErrRoleNotFound,
		"RemovePermissionFromUser() for non-existent role error mismatch",
	) // This error might change based on Casbin's behavior or how we check

	// Remove role from non-existent user
	err = um.RemovePermissionFromUser("nonexistentuser", "table1", ResourceTypeTable)
	assert.ErrorIs(
		err,
		ErrUserNotFound,
		"RemovePermissionFromUser() for non-existent user error mismatch",
	)
}

func TestUserManager_DeleteUser(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, db := newTestUserManager(t)

	initialRoles := []Permission{
		{Resource: "table1", ResourceType: ResourceTypeTable, Type: PermissionTypeRead},
	}
	_, _ = um.CreateUser("deleteuser", "password", initialRoles)

	// Delete existing user
	err := um.DeleteUser("deleteuser")
	require.NoError(err, "DeleteUser() failed")

	// Verify user password hash is removed from memory
	_, exists := um.passwordHashes["deleteuser"]
	assert.False(
		exists,
		"User password hash still exists in UserManager.passwordHashes after deletion",
	)

	// Verify user password info is removed from DB
	keyDB := []byte(userPasswordPrefix + "deleteuser")
	_, closer, errDB := db.Get(keyDB)
	if errDB == nil {
		require.NoError(closer.Close())
	}
	assert.ErrorIs(
		errDB,
		pebble.ErrNotFound,
		"UserPasswordInfo not removed from DB or unexpected error",
	)

	// Verify Casbin policies are removed
	deletedUserRoles, err := um.GetPermissionsForUser("deleteuser")
	assert.ErrorIs(
		err,
		ErrUserNotFound,
		"GetPermissionsForUser after DeleteUser() error mismatch",
	) // GetPermissionsForUser checks for user existence first
	assert.Empty(deletedUserRoles, "Casbin policies not removed for deleted user")
	// More direct check
	// if len(enforcer.GetPermissionsForUser("deleteuser")) > 0 {
	// 	t.Error("Enforcer still has permissions for deleted user")
	// }

	// Attempt to load users again from DB to ensure password hash is gone
	// And enforcer policies associated with a *new* enforcer instance on same DB
	// should also be gone if the adapter worked correctly during delete.
	um2, err := NewUserManager(&kv.PebbleDB{DB: db})
	require.NoError(err, "NewUserManager after delete failed")
	assert.NotContains(
		um2.passwordHashes,
		"deleteuser",
		"User password hash still loaded from DB after deletion and re-initialization",
	)
	// if len(enforcer2.GetPermissionsForUser("deleteuser")) > 0 {
	// 	t.Error("User policies still loaded by new enforcer after deletion and re-initialization")
	// }

	// Delete non-existent user
	err = um.DeleteUser("nonexistentuser")
	assert.ErrorIs(err, ErrUserNotFound, "DeleteUser() for non-existent user error mismatch")
}

func TestUserManager_UpdatePassword(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	um, db := newTestUserManager(t)
	_, _ = um.CreateUser("updatepassuser", "oldpassword", nil)

	// Update password for existing user
	err := um.UpdatePassword("updatepassuser", "newpassword")
	require.NoError(err, "UpdatePassword() failed")

	// Verify new password works
	_, err = um.AuthenticateUser("updatepassuser", "newpassword")
	assert.NoError(err, "AuthenticateUser() with new password failed")

	// Verify old password no longer works
	_, err = um.AuthenticateUser("updatepassuser", "oldpassword")
	assert.ErrorIs(err, ErrInvalidPassword, "AuthenticateUser() with old password error mismatch")

	// Verify password hash in DB is updated
	key := []byte(userPasswordPrefix + "updatepassuser")
	val, closer, errDb := db.Get(key)
	require.NoError(errDb, "Failed to get user password info from DB after update")
	var pi UserPasswordInfo
	errUnmarshal := json.Unmarshal(val, &pi)
	require.NoError(closer.Close())
	require.NoError(errUnmarshal, "Failed to unmarshal UserPasswordInfo after update")
	assert.NoError(
		bcrypt.CompareHashAndPassword(pi.PasswordHash, []byte("newpassword")),
		"UserPasswordInfo in DB password hash not updated correctly",
	)

	// Update password for non-existent user
	err = um.UpdatePassword("nonexistentuser", "somepassword")
	assert.ErrorIs(err, ErrUserNotFound, "UpdatePassword() for non-existent user error mismatch")
}

// TestUserManager_Persistence checks if users persist across UserManager instances
func TestUserManager_Persistence(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	// This test needs a persistent DB, not an in-memory one for the core check.
	// However, setupTestDB uses vfs.NewMem(). For true file persistence test,
	// t.TempDir() should be used for pebble.Open's path without FS option.
	// For this test, let's assume vfs.NewMem() with a named path within that VFS
	// and reopening achieves a similar logical goal for unit testing UserManager.
	// If we want to test actual file system persistence, setupTestDB should change.
	// Given the current setup, we test if reopening the VFS-backed DB reloads state.

	dbPath := "test_persistence_db" // A "path" within the VFS
	vfsMem := vfs.NewMem()
	opts := pebbleutils.NewMemPebbleOpts()
	opts.FS = vfsMem // Override to use the same VFS instance for both opens

	// Setup first UserManager and create a user
	db1, err := pebble.Open(dbPath, opts)
	require.NoError(err, "Failed to open pebble DB (db1)")

	um1, err := NewUserManager(&kv.PebbleDB{DB: db1})
	require.NoError(err, "NewUserManager (um1) error")

	roles := []Permission{
		{Resource: "persist_table", ResourceType: ResourceTypeTable, Type: PermissionTypeWrite},
	}
	_, _ = um1.CreateUser("persistuser", "password", roles)
	// um1.enforcer.SavePolicy() // Ensure policies are saved if adapter requires explicit save

	require.NoError(db1.Close(), "Failed to close db1")

	// Setup second UserManager using the same DB "path" and VFS
	db2, err := pebble.Open(dbPath, opts) // Reopen with same VFS and "path"
	require.NoError(err, "Failed to open pebble DB (db2)")
	defer db2.Close()

	um2, err := NewUserManager(&kv.PebbleDB{DB: db2})
	require.NoError(err, "NewUserManager() for persistence test failed")

	// Check if user password hash exists in the second UserManager
	user, err := um2.GetUser("persistuser") // GetUser only gives User struct (no roles)
	require.NoError(err, "GetUser() from um2 failed")
	assert.Equal("persistuser", user.Username, "Expected user 'persistuser'")

	// Check if roles (policies) exist in the second enforcer
	retrievedRoles, err := um2.GetPermissionsForUser("persistuser")
	require.NoError(err, "GetPermissionsForUser() from um2 failed")
	sortRoles(retrievedRoles)
	sortRoles(roles)
	assert.Equal(roles, retrievedRoles, "User roles mismatch")

	// Authenticate to be sure password hash was persisted
	_, err = um2.AuthenticateUser("persistuser", "password")
	assert.NoError(err, "Failed to authenticate user in um2")
}
