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
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/antflydb/antfly/src/metadata/kv"
	metadatakvadapter "github.com/antflydb/antfly/src/metadata/kv/casbin-adapter"
	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/model"
	"github.com/cockroachdb/pebble/v2"
	"github.com/goccy/go-json"
	"golang.org/x/crypto/bcrypt"
)

const (
	userPasswordPrefix = "userpass:"
	apiKeyPrefix       = "apikey:"
	MinPasswordCost    = bcrypt.DefaultCost

	apiKeyIDLength    = 20 // alphanumeric characters (~119 bits)
	apiKeySecretBytes = 16 // 128 bits of randomness
)

// alphanumeric characters used for generating API key IDs.
const alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// ApiKeyRecord stores an API key's metadata and hashed secret.
type ApiKeyRecord struct {
	KeyID       string                       `json:"key_id"`
	SecretHash  []byte                       `json:"secret_hash"` // SHA-256(salt + secret)
	SecretSalt  []byte                       `json:"secret_salt"` // 16-byte random salt
	Username    string                       `json:"username"`    // owner
	Name        string                       `json:"name"`
	Permissions []Permission                 `json:"permissions,omitempty"`  // optional scoping
	RowFilter   map[string]json.RawMessage   `json:"row_filter,omitempty"`  // per-table bleve query filter
	CreatedAt   time.Time                    `json:"created_at"`
	ExpiresAt   time.Time                    `json:"expires_at"` // zero = never
}

const rbacModelConf = `
[request_definition]
r = sub, typ, obj, act

[policy_definition]
p = sub, typ, obj, act
p2 = sub, obj, filter

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
`

// Predefined errors
var (
	ErrUserExists      = errors.New("user already exists")
	ErrUserNotFound    = errors.New("user not found")
	ErrInvalidPassword = errors.New("invalid password")
	ErrRoleNotFound    = errors.New(
		"role not found for this table",
	) // May change depending on how we query Casbin
)

// UserPasswordInfo is used for storing/retrieving only username and hash.
type UserPasswordInfo struct {
	Username     string `json:"username"`
	PasswordHash []byte `json:"password_hash"`
}

// UserManager handles user-related operations.
type UserManager struct {
	mu             sync.RWMutex
	mux            *http.ServeMux
	passwordHashes map[string][]byte        // Stores username -> passwordHash
	apiKeys        map[string]*ApiKeyRecord // Stores keyID -> record
	db             kv.DB                    // For persistence of password hashes
	enforcer       casbin.IEnforcer         // For managing roles/permissions
}

// NewUserManager creates a new UserManager instance and loads user password hashes from the database.
// It requires a Casbin enforcer instance for role management.
func NewUserManager(db kv.DB) (*UserManager, error) {
	adapter, err := metadatakvadapter.NewAdapter(db, "casbin::")
	if err != nil {
		return nil, fmt.Errorf("failed to create casbin adapter: %w", err)
	}

	model, err := model.NewModelFromString(rbacModelConf) // Load the RBAC model from string
	if err != nil {
		return nil, fmt.Errorf("failed to create casbin model: %w", err)
	}
	enforcer, err := casbin.NewEnforcer(model, adapter)
	enforcer.EnableAutoSave(true)
	if err != nil {
		return nil, fmt.Errorf("failed to create casbin enforcer: %w", err)
	}
	um := &UserManager{
		mux:            http.NewServeMux(),
		db:             db,
		passwordHashes: make(map[string][]byte),
		apiKeys:        make(map[string]*ApiKeyRecord),
		enforcer:       enforcer,
	}
	if err := um.loadUserPasswordHashes(); err != nil {
		return nil, fmt.Errorf("failed to load user password hashes: %w", err)
	}
	if err := um.loadApiKeys(); err != nil {
		return nil, fmt.Errorf("failed to load api keys: %w", err)
	}
	return um, nil
}

// Helper to convert to Casbin policy components (object, action)
func (r *Permission) toCasbinPolicyComponents() (typ, object, action string) {
	typ = string(r.ResourceType) // e.g., "table", "user", "global"
	object = r.Resource
	action = string(r.Type)
	return
}

func (um *UserManager) Enforce(
	username string,
	resourceType ResourceType,
	resource string,
	permissionType PermissionType,
) (bool, error) {
	// Ensure the user exists
	um.mu.RLock()
	_, exists := um.passwordHashes[username]
	um.mu.RUnlock()
	if !exists {
		return false, ErrUserNotFound
	}

	// Convert to Casbin policy components
	typ := string(resourceType)
	act := string(permissionType)

	// Enforce the policy using Casbin
	ok, err := um.enforcer.Enforce(username, typ, resource, act)
	if err != nil {
		return false, fmt.Errorf("failed to enforce policy for user %s: %w", username, err)
	}
	return ok, nil
}

// CreateUser creates a new user, hashes their password, saves hash to DB, and sets initial roles in Casbin.
func (um *UserManager) CreateUser(
	username string,
	password string,
	initialPolicies []Permission,
) (*User, error) {
	um.mu.Lock()
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; exists {
		return nil, ErrUserExists
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), MinPasswordCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	userInfo := &UserPasswordInfo{
		Username:     username,
		PasswordHash: hashedPassword,
	}

	if err := um.saveUserPasswordInfo(userInfo); err != nil {
		return nil, fmt.Errorf("failed to save user password info: %w", err)
	}

	um.passwordHashes[username] = hashedPassword

	// Add initial roles to Casbin
	if len(initialPolicies) > 0 {
		casbinPolicies := make([][]string, len(initialPolicies))
		for i, role := range initialPolicies {
			typ, obj, act := role.toCasbinPolicyComponents()
			casbinPolicies[i] = []string{username, typ, obj, act}
		}
		// AddPoliciesEx to avoid duplicates if any (though should not happen for new user)
		// This also assumes the casbin adapter handles persistence on AddPolicies.
		// If not, um.enforcer.SavePolicy() might be needed here.
		_, err := um.enforcer.AddPolicies(casbinPolicies)
		if err != nil {
			// Potentially rollback user creation from DB if Casbin fails.
			// For now, log and return error.
			// um.db.Delete([]byte(userPasswordPrefix+username), pebble.Sync) // Example rollback
			// delete(um.passwordHashes, username)
			return nil, fmt.Errorf(
				"failed to add initial roles to Casbin for user %s: %w",
				username,
				err,
			)
		}
	}
	// um.enforcer.SavePolicy() // If auto-save is not enabled for the adapter

	// Return a User struct (without roles populated from Casbin here for simplicity)
	// The caller can use GetRolesForUser if needed.
	return &User{Username: username, PasswordHash: hashedPassword}, nil
}

// GetUser retrieves a user's basic info (username, hash) by their username.
// Roles are not populated in the returned User struct; use GetRolesForUser for that.
func (um *UserManager) GetUser(username string) (*User, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	passwordHash, exists := um.passwordHashes[username]
	if !exists {
		return nil, ErrUserNotFound
	}
	return &User{Username: username, PasswordHash: passwordHash}, nil
}

// AuthenticateUser checks if the provided username and password are valid.
func (um *UserManager) AuthenticateUser(username string, password string) (*User, error) {
	um.mu.RLock() // RLock initially
	passwordHash, exists := um.passwordHashes[username]
	um.mu.RUnlock() // Unlock after reading map

	if !exists {
		return nil, ErrUserNotFound
	}

	if err := bcrypt.CompareHashAndPassword(passwordHash, []byte(password)); err != nil {
		return nil, ErrInvalidPassword
	}

	// Return User struct without roles, consistent with GetUser
	return &User{Username: username, PasswordHash: passwordHash}, nil
}

// AddPermissionToUser adds a role (permission) for a user in Casbin.
func (um *UserManager) AddPermissionToUser(username string, perm Permission) error {
	um.mu.Lock() // Lock to check user existence, then Casbin op (which might have its own sync)
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return ErrUserNotFound
	}

	typ, obj, act := perm.toCasbinPolicyComponents()

	// AddPolicyEx returns (affected, error). True if policy was added, false if already exists.
	// We don't strictly need ErrRoleExists if Casbin handles idempotency.
	_, err := um.enforcer.AddPolicy(username, typ, obj, act)
	if err != nil {
		return fmt.Errorf("failed to add role to Casbin for user %s: %w", username, err)
	}
	// if !added {
	// This means the policy already existed. Depending on desired behavior,
	// this might not be an error. For now, let's not treat it as an error.
	// return ErrRoleExists // Or some other specific error/status
	//}
	// um.enforcer.SavePolicy() // If auto-save is not enabled
	return nil
}

// RemovePermissionFromUser removes all permissions for a user on a specific resource (identified by resourceName and resourceType).
func (um *UserManager) RemovePermissionFromUser(
	username, resourceName string,
	resourceType ResourceType,
) error {
	um.mu.Lock() // Lock for user check + Casbin operation
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return ErrUserNotFound
	}

	var foundAndRemoved bool
	var err error
	if resourceName == "*" {
		foundAndRemoved, err = um.enforcer.RemoveFilteredPolicy(0, username, string(resourceType))
	} else {
		foundAndRemoved, err = um.enforcer.RemoveFilteredPolicy(0, username, string(resourceType), resourceName)
	}
	if err != nil {
		return fmt.Errorf(
			"failed to remove role from for user %s resource %s/%s: %w",
			username,
			resourceType,
			resourceName,
			err,
		)
	}
	if !foundAndRemoved {
		return ErrRoleNotFound
	}

	// um.enforcer.SavePolicy() // If auto-save is not enabled
	return nil
}

// ListUsers returns a list of all usernames in the system.
func (um *UserManager) ListUsers() ([]string, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	users := make([]string, 0, len(um.passwordHashes))
	for username := range um.passwordHashes {
		users = append(users, username)
	}
	return users, nil
}

// GetPermissionsForUser retrieves all roles (permissions) for a user from Casbin.
func (um *UserManager) GetPermissionsForUser(username string) ([]Permission, error) {
	um.mu.RLock()
	_, exists := um.passwordHashes[username]
	um.mu.RUnlock()
	if !exists {
		// Not strictly necessary to lock for Casbin, but good for user check.
		return nil, ErrUserNotFound
	}

	casbinPermissions, err := um.enforcer.GetPermissionsForUser(
		username,
	) // Returns [][]string{{object, action}, ...}
	if err != nil {
		return nil, fmt.Errorf("failed to get permissions for user %s: %w", username, err)
	}
	roles := make([]Permission, 0, len(casbinPermissions))
	for _, p := range casbinPermissions {
		if len(p) < 4 {
			continue // Should not happen for valid permissions
		}
		roles = append(roles, Permission{
			ResourceType: ResourceType(p[1]),
			Resource:     p[2],
			Type:         PermissionType(p[3]),
		})
	}
	return roles, nil
}

func (um *UserManager) saveUserPasswordInfo(userInfo *UserPasswordInfo) error {
	key := []byte(userPasswordPrefix + userInfo.Username)
	data, err := json.Marshal(userInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal user password info: %w", err)
	}
	if err := um.db.Batch(context.Background(), [][2][]byte{{key, data}}, nil); err != nil {
		return fmt.Errorf("failed to save user password info to database: %w", err)
	}
	return nil
}

func (um *UserManager) loadUserPasswordHashes() error {
	iterOpt := &pebble.IterOptions{
		LowerBound: []byte(userPasswordPrefix),
		UpperBound: []byte(userPasswordPrefix + "~"),
	}
	iter, err := um.db.NewIter(context.TODO(), iterOpt)
	if err != nil {
		return fmt.Errorf("creating iterator for user passwords: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		var userInfo UserPasswordInfo
		if err := json.Unmarshal(iter.Value(), &userInfo); err != nil {
			return fmt.Errorf("unmarshalling user password info %s: %w", string(iter.Key()), err)
		}
		um.passwordHashes[userInfo.Username] = userInfo.PasswordHash
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error while loading user password hashes: %w", err)
	}
	return nil
}

// DeleteUser removes a user from the system (password hash from DB, all Casbin policies).
func (um *UserManager) DeleteUser(username string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return ErrUserNotFound
	}

	if err := um.db.Batch(context.Background(), nil, [][]byte{
		[]byte(userPasswordPrefix + username),
	}); err != nil {
		return fmt.Errorf("failed to delete user %s password info from database: %w", username, err)
	}

	delete(um.passwordHashes, username)

	// Remove all policies associated with this user from Casbin
	// This includes direct permissions (p lines) and group/role memberships (g lines) if any.
	// RemoveFilteredPolicy removes 'p' policies. Args: fieldIndex, fieldValues...
	// To remove all policies where user is the subject (fieldIndex 0):
	_, errP := um.enforcer.RemoveFilteredPolicy(0, username)
	if errP != nil {
		// Log error but continue to try removing grouping policies
		// Consider how to handle partial failures.
		fmt.Printf("Error removing direct policies for user %s from Casbin: %v\n", username, errP)
	}

	// RemoveFilteredGroupingPolicy removes 'g' policies.
	// To remove all grouping policies where user is the first element (e.g., user in a role):
	_, errG := um.enforcer.RemoveFilteredGroupingPolicy(0, username)
	if errG != nil {
		fmt.Printf("Error removing grouping policies for user %s from Casbin: %v\n", username, errG)
	}

	// Remove all p2 (row filter) policies for this user.
	_, errP2 := um.enforcer.RemoveFilteredNamedPolicy("p2", 0, username)
	if errP2 != nil {
		fmt.Printf("Error removing row filter policies for user %s from Casbin: %v\n", username, errP2)
	}

	// if (errP != nil || errG != nil) && !(removedPolicies || removedGroupingPolicies) {
	// If there was an error AND nothing was removed, it might be a more significant issue.
	// For now, just logging. A more robust error handling might be needed.
	// }
	// um.enforcer.SavePolicy() // If auto-save is not enabled

	return nil // Or return a combined error if any Casbin operation failed
}

// UpdatePassword updates the password for an existing user.
func (um *UserManager) UpdatePassword(username string, newPassword string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	currentHash, exists := um.passwordHashes[username]
	if !exists {
		return ErrUserNotFound
	}

	newPasswordHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), MinPasswordCost)
	if err != nil {
		return fmt.Errorf("failed to hash new password for user %s: %w", username, err)
	}

	userInfo := &UserPasswordInfo{
		Username:     username,
		PasswordHash: newPasswordHash,
	}
	if err := um.saveUserPasswordInfo(userInfo); err != nil {
		// If save fails, revert in-memory change
		um.passwordHashes[username] = currentHash // Revert
		return fmt.Errorf("failed to save updated password for user %s: %w", username, err)
	}
	um.passwordHashes[username] = newPasswordHash
	return nil
}

// SetRowFilter stores a row filter for a user on a specific table using p2 named policy.
func (um *UserManager) SetRowFilter(username, table string, filterJSON json.RawMessage) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return ErrUserNotFound
	}

	// Validate that the filter is valid JSON.
	var parsed map[string]interface{}
	if err := json.Unmarshal(filterJSON, &parsed); err != nil {
		return fmt.Errorf("invalid row filter JSON for table %q: %w", table, err)
	}

	// Remove any existing filter for this user+table, then add the new one.
	_, _ = um.enforcer.RemoveFilteredNamedPolicy("p2", 0, username, table)
	_, err := um.enforcer.AddNamedPolicy("p2", username, table, string(filterJSON))
	if err != nil {
		return fmt.Errorf("failed to set row filter for user %s table %s: %w", username, table, err)
	}
	return nil
}

// RemoveRowFilter removes the row filter for a user on a specific table.
func (um *UserManager) RemoveRowFilter(username, table string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return ErrUserNotFound
	}

	removed, err := um.enforcer.RemoveFilteredNamedPolicy("p2", 0, username, table)
	if err != nil {
		return fmt.Errorf("failed to remove row filter for user %s table %s: %w", username, table, err)
	}
	if !removed {
		return ErrRowFilterNotFound
	}
	return nil
}

// GetRowFilters returns the effective row filters for a user across all p2 policies.
// When multiple filters apply to the same table, they are conjuncted.
func (um *UserManager) GetRowFilters(username string) (map[string]json.RawMessage, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	// Get all p2 policies for this user (fieldIndex 0 = subject).
	policies, err := um.enforcer.GetFilteredNamedPolicy("p2", 0, username)
	if err != nil {
		return nil, fmt.Errorf("failed to get row filters for user %s: %w", username, err)
	}

	if len(policies) == 0 {
		return nil, nil
	}

	// p2 format: [sub, obj, filter] → policies[i] = [username, table, filterJSON]
	filters := make(map[string]json.RawMessage)
	for _, p := range policies {
		if len(p) < 3 {
			continue
		}
		table := p[1]
		filterJSON := json.RawMessage(p[2])

		existing, ok := filters[table]
		if !ok {
			filters[table] = filterJSON
		} else {
			// Conjunct multiple filters for the same table.
			conjunction, _ := json.Marshal(map[string]interface{}{
				"conjuncts": []json.RawMessage{existing, filterJSON},
			})
			filters[table] = conjunction
		}
	}
	return filters, nil
}

// ListRowFilters returns individual row filter entries for a user (not merged).
func (um *UserManager) ListRowFilters(username string) ([]rowFilterPolicy, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return nil, ErrUserNotFound
	}

	policies, err := um.enforcer.GetFilteredNamedPolicy("p2", 0, username)
	if err != nil {
		return nil, fmt.Errorf("failed to list row filters for user %s: %w", username, err)
	}

	entries := make([]rowFilterPolicy, 0, len(policies))
	for _, p := range policies {
		if len(p) < 3 {
			continue
		}
		entries = append(entries, rowFilterPolicy{
			Table:  p[1],
			Filter: json.RawMessage(p[2]),
		})
	}
	return entries, nil
}

// GetRowFilter returns the row filter for a user on a specific table.
func (um *UserManager) GetRowFilter(username, table string) (json.RawMessage, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return nil, ErrUserNotFound
	}

	policies, err := um.enforcer.GetFilteredNamedPolicy("p2", 0, username, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get row filter for user %s table %s: %w", username, table, err)
	}
	if len(policies) == 0 {
		return nil, ErrRowFilterNotFound
	}
	// Return the first match's filter JSON.
	if len(policies[0]) < 3 {
		return nil, ErrRowFilterNotFound
	}
	return json.RawMessage(policies[0][2]), nil
}

// rowFilterPolicy represents a single row filter policy for a user+table (internal type).
type rowFilterPolicy struct {
	Table  string
	Filter json.RawMessage
}

// Predefined row filter errors.
var ErrRowFilterNotFound = errors.New("row filter not found")

// Predefined API key errors.
var (
	ErrApiKeyNotFound      = errors.New("api key not found")
	ErrApiKeyExpired       = errors.New("api key expired")
	ErrApiKeyInvalid       = errors.New("invalid api key secret")
	ErrPrivilegeEscalation = errors.New("privilege escalation")
)

// generateRandomAlphanumeric generates a cryptographically random string of the given length
// using alphanumeric characters.
func generateRandomAlphanumeric(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("reading random bytes: %w", err)
	}
	result := make([]byte, n)
	for i := range b {
		result[i] = alphanumeric[int(b[i])%len(alphanumeric)]
	}
	return string(result), nil
}

// hashApiKeySecret computes SHA-256(salt + secretRawBytes).
func hashApiKeySecret(salt, secretRaw []byte) []byte {
	h := sha256.New()
	h.Write(salt)
	h.Write(secretRaw)
	return h.Sum(nil)
}

// CreateApiKey creates a new API key for the given user.
// It returns the key ID and cleartext secret (shown once, never stored).
func (um *UserManager) CreateApiKey(username, name string, permissions []Permission, rowFilter map[string]json.RawMessage, expiresAt time.Time) (keyID, keySecret string, err error) {
	um.mu.Lock()
	defer um.mu.Unlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return "", "", ErrUserNotFound
	}

	// Validate each requested permission is held by the creator (prevent escalation)
	for _, perm := range permissions {
		ok, enforceErr := um.enforcer.Enforce(username, string(perm.ResourceType), perm.Resource, string(perm.Type))
		if enforceErr != nil {
			return "", "", fmt.Errorf("checking permission for api key creation: %w", enforceErr)
		}
		if !ok {
			return "", "", fmt.Errorf("%w: permission (%s, %s, %s) not held by creator", ErrPrivilegeEscalation, perm.ResourceType, perm.Resource, perm.Type)
		}
	}

	// Validate row filter values are valid bleve query JSON
	for table, filterJSON := range rowFilter {
		var parsed map[string]interface{}
		if err := json.Unmarshal(filterJSON, &parsed); err != nil {
			return "", "", fmt.Errorf("invalid row_filter for table %q: %w", table, err)
		}
	}

	keyID, err = generateRandomAlphanumeric(apiKeyIDLength)
	if err != nil {
		return "", "", fmt.Errorf("generating key ID: %w", err)
	}

	secretRaw := make([]byte, apiKeySecretBytes)
	if _, err := rand.Read(secretRaw); err != nil {
		return "", "", fmt.Errorf("generating key secret: %w", err)
	}
	keySecret = base64.RawURLEncoding.EncodeToString(secretRaw)

	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", "", fmt.Errorf("generating salt: %w", err)
	}

	secretHash := hashApiKeySecret(salt, secretRaw)

	record := &ApiKeyRecord{
		KeyID:       keyID,
		SecretHash:  secretHash,
		SecretSalt:  salt,
		Username:    username,
		Name:        name,
		Permissions: permissions,
		RowFilter:   rowFilter,
		CreatedAt:   time.Now(),
		ExpiresAt:   expiresAt,
	}

	if err := um.saveApiKeyRecord(record); err != nil {
		return "", "", fmt.Errorf("saving api key record: %w", err)
	}

	um.apiKeys[keyID] = record
	return keyID, keySecret, nil
}

// ValidateApiKey validates an API key credential, returning the owner, permissions, and row filter.
// Hash is verified before expiration to prevent timing-based enumeration of valid key IDs.
func (um *UserManager) ValidateApiKey(keyID, keySecret string) (username string, permissions []Permission, rowFilter map[string]json.RawMessage, err error) {
	um.mu.RLock()
	record, exists := um.apiKeys[keyID]
	um.mu.RUnlock()

	if !exists {
		return "", nil, nil, ErrApiKeyNotFound
	}

	secretRaw, err := base64.RawURLEncoding.DecodeString(keySecret)
	if err != nil {
		return "", nil, nil, ErrApiKeyInvalid
	}

	computedHash := hashApiKeySecret(record.SecretSalt, secretRaw)
	if subtle.ConstantTimeCompare(computedHash, record.SecretHash) != 1 {
		return "", nil, nil, ErrApiKeyInvalid
	}

	if !record.ExpiresAt.IsZero() && time.Now().After(record.ExpiresAt) {
		return "", nil, nil, ErrApiKeyExpired
	}

	return record.Username, record.Permissions, record.RowFilter, nil
}

// ListApiKeys returns all API keys owned by the given user, with hash/salt omitted.
func (um *UserManager) ListApiKeys(username string) ([]*ApiKeyRecord, error) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	if _, exists := um.passwordHashes[username]; !exists {
		return nil, ErrUserNotFound
	}

	var keys []*ApiKeyRecord
	for _, record := range um.apiKeys {
		if record.Username == username {
			keys = append(keys, &ApiKeyRecord{
				KeyID:       record.KeyID,
				Username:    record.Username,
				Name:        record.Name,
				Permissions: record.Permissions,
				RowFilter:   record.RowFilter,
				CreatedAt:   record.CreatedAt,
				ExpiresAt:   record.ExpiresAt,
			})
		}
	}
	return keys, nil
}

// DeleteApiKey removes an API key by its ID, verifying it belongs to the given user.
func (um *UserManager) DeleteApiKey(username, keyID string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	record, exists := um.apiKeys[keyID]
	if !exists {
		return ErrApiKeyNotFound
	}
	if record.Username != username {
		return ErrApiKeyNotFound
	}

	if err := um.db.Batch(context.Background(), nil, [][]byte{
		[]byte(apiKeyPrefix + keyID),
	}); err != nil {
		return fmt.Errorf("deleting api key from database: %w", err)
	}

	delete(um.apiKeys, keyID)
	return nil
}

func (um *UserManager) saveApiKeyRecord(record *ApiKeyRecord) error {
	key := []byte(apiKeyPrefix + record.KeyID)
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshalling api key record: %w", err)
	}
	if err := um.db.Batch(context.Background(), [][2][]byte{{key, data}}, nil); err != nil {
		return fmt.Errorf("saving api key record to database: %w", err)
	}
	return nil
}

func (um *UserManager) loadApiKeys() error {
	iterOpt := &pebble.IterOptions{
		LowerBound: []byte(apiKeyPrefix),
		UpperBound: []byte(apiKeyPrefix + "~"),
	}
	iter, err := um.db.NewIter(context.TODO(), iterOpt)
	if err != nil {
		return fmt.Errorf("creating iterator for api keys: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		var record ApiKeyRecord
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return fmt.Errorf("unmarshalling api key record %s: %w", string(iter.Key()), err)
		}
		um.apiKeys[record.KeyID] = &record
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error while loading api keys: %w", err)
	}
	return nil
}
