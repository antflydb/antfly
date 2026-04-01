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

//go:build go1.24

//go:generate go tool oapi-codegen --config=cfg.yaml ./api.yaml
package usermgr

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)


type UserApi struct {
	logger *zap.Logger
	um     *UserManager
}

func NewUserApi(logger *zap.Logger, um *UserManager) http.Handler {
	api := &UserApi{
		logger: logger,
		um:     um,
	}
	return Handler(api)
}

func (um *UserApi) httpError(w http.ResponseWriter, message string, code int, err error) {
	if err != nil {
		um.logger.Warn("UserManager HTTP Error", zap.String("message", message), zap.Error(err))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff") // Prevents MIME-sniffing
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (um *UserApi) jsonResponse(w http.ResponseWriter, data any, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if data != nil {
		if err := json.NewEncoder(w).Encode(data); err != nil {
			um.logger.Warn("Failed to encode JSON response", zap.Error(err))
			// Headers might have already been written. Avoid writing another error if possible.
		}
	}
}

func (um *UserApi) GetCurrentUser(
	w http.ResponseWriter,
	r *http.Request,
) {
	// Get the authenticated username from the header set by authnMiddleware
	username := r.Header.Get("X-Authenticated-User")
	if username == "" {
		um.httpError(w, "User not authenticated", http.StatusUnauthorized, nil)
		return
	}

	permissions, err := um.um.GetPermissionsForUser(username)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to get user permissions", http.StatusInternalServerError, err)
		}
		return
	}

	response := map[string]any{
		"username":    username,
		"permissions": permissions,
	}
	um.jsonResponse(w, response, http.StatusOK)
}

func (um *UserApi) ListUsers(
	w http.ResponseWriter,
	r *http.Request,
) {
	users, err := um.um.ListUsers()
	if err != nil {
		um.httpError(w, "Failed to list users", http.StatusInternalServerError, err)
		return
	}

	// Return array of user objects with just username
	userList := make([]map[string]string, len(users))
	for i, username := range users {
		userList[i] = map[string]string{"username": username}
	}
	um.jsonResponse(w, userList, http.StatusOK)
}

func (um *UserApi) CreateUser(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	var req CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		um.httpError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	if userName != "" {
		if req.Username != nil && *req.Username != userName {
			um.httpError(
				w,
				"Username in path does not match request body",
				http.StatusBadRequest,
				nil,
			)
			return
		}
		req.Username = &userName // Override if username is in path
	}

	if req.Username == nil || *req.Username == "" || req.Password == "" {
		um.httpError(w, "Username and password are required", http.StatusBadRequest, nil)
		return
	}

	var initialPolicies []Permission
	if req.InitialPolicies != nil {
		initialPolicies = *req.InitialPolicies
	}
	// MVP (ajr) Contains side-effects for raft log
	user, err := um.um.CreateUser(*req.Username, req.Password, initialPolicies)
	if err != nil {
		if errors.Is(err, ErrUserExists) {
			um.httpError(w, ErrUserExists.Error(), http.StatusConflict, err)
		} else {
			um.httpError(w, "Failed to create user", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, user, http.StatusCreated)
}

func (um *UserApi) GetUserByName(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	user, err := um.um.GetUser(userName)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to get user", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, user, http.StatusOK)
}

func (um *UserApi) DeleteUser(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	// MVP (ajr) Contains side-effects for raft log
	if err := um.um.DeleteUser(userName); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to delete user", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, nil, http.StatusNoContent)
}

func (um *UserApi) UpdateUserPassword(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	var req UpdatePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		um.httpError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	if req.NewPassword == "" {
		um.httpError(w, "New password cannot be empty", http.StatusBadRequest, nil)
		return
	}

	// MVP (ajr) Contains side-effects for raft log
	err := um.um.UpdatePassword(userName, req.NewPassword)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to update password", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, map[string]string{"message": "Password updated successfully"}, http.StatusOK)
}

func (um *UserApi) GetUserPermissions(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	permissions, err := um.um.GetPermissionsForUser(userName)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to get permissions", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, permissions, http.StatusOK)
}

func (um *UserApi) AddPermissionToUser(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	var perm Permission
	if err := json.NewDecoder(r.Body).Decode(&perm); err != nil {
		um.httpError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	if perm.Resource == "" || perm.ResourceType == "" || perm.Type == "" {
		um.httpError(
			w,
			"Permission resource, resource_type, and permission_type are required",
			http.StatusBadRequest,
			nil,
		)
		return
	}
	// Validate ResourceType and PermissionType
	switch perm.ResourceType {
	case ResourceTypeTable, ResourceTypeUser, ResourceTypeAsterisk:
		// valid
	default:
		um.httpError(
			w,
			fmt.Sprintf("Invalid resource_type: %s", perm.ResourceType),
			http.StatusBadRequest,
			nil,
		)
		return
	}
	switch perm.Type {
	case PermissionTypeRead, PermissionTypeWrite, PermissionTypeAdmin:
		// valid
	default:
		um.httpError(
			w,
			fmt.Sprintf("Invalid permission_type: %s", perm.Type),
			http.StatusBadRequest,
			nil,
		)
		return
	}

	// MVP (ajr) Contains side-effects for raft log
	err := um.um.AddPermissionToUser(userName, perm)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to add permission", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(
		w,
		map[string]string{"message": "Permission added successfully"},
		http.StatusCreated,
	)
}

func (um *UserApi) RemovePermissionFromUser(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
	params RemovePermissionFromUserParams,
) {
	// MVP (ajr) Contains side-effects for raft log
	err := um.um.RemovePermissionFromUser(userName, params.Resource, params.ResourceType)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else if errors.Is(err, ErrRoleNotFound) {
			um.httpError(w, ErrRoleNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to remove permission", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, nil, http.StatusNoContent)
}

func (um *UserApi) ListApiKeys(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	keys, err := um.um.ListApiKeys(userName)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to list API keys", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, keys, http.StatusOK)
}

func (um *UserApi) CreateApiKey(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	var req CreateApiKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		um.httpError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	if req.Name == "" {
		um.httpError(w, "Name is required", http.StatusBadRequest, nil)
		return
	}

	var expiresAt time.Time
	if req.ExpiresIn != nil && *req.ExpiresIn != "" {
		duration, err := time.ParseDuration(*req.ExpiresIn)
		if err != nil {
			um.httpError(w, "Invalid expires_in duration: "+err.Error(), http.StatusBadRequest, err)
			return
		}
		expiresAt = time.Now().Add(duration)
	}

	var permissions []Permission
	if req.Permissions != nil {
		permissions = *req.Permissions
	}

	var rowFilter map[string]json.RawMessage
	if req.RowFilter != nil {
		rowFilter = make(map[string]json.RawMessage)
		for k, v := range *req.RowFilter {
			raw, marshalErr := json.Marshal(v)
			if marshalErr != nil {
				um.httpError(w, "Invalid row_filter value for table "+k, http.StatusBadRequest, marshalErr)
				return
			}
			rowFilter[k] = raw
		}
	}

	keyID, keySecret, err := um.um.CreateApiKey(userName, req.Name, permissions, rowFilter, expiresAt)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
			return
		}
		if errors.Is(err, ErrPrivilegeEscalation) {
			um.httpError(w, err.Error(), http.StatusForbidden, err)
			return
		}
		um.httpError(w, "Failed to create API key: "+err.Error(), http.StatusInternalServerError, err)
		return
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(keyID + ":" + keySecret))

	response := ApiKeyWithSecret{
		KeyId:     keyID,
		KeySecret: keySecret,
		Encoded:   encoded,
		Name:      req.Name,
		Username:  userName,
		CreatedAt: time.Now(),
	}
	if len(permissions) > 0 {
		response.Permissions = &permissions
	}
	if len(rowFilter) > 0 {
		rf := make(map[string]interface{})
		for k, v := range rowFilter {
			var parsed interface{}
			_ = json.Unmarshal(v, &parsed)
			rf[k] = parsed
		}
		response.RowFilter = &rf
	}
	if !expiresAt.IsZero() {
		response.ExpiresAt = &expiresAt
	}

	um.jsonResponse(w, response, http.StatusCreated)
}

func (um *UserApi) ListRowFilters(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
) {
	entries, err := um.um.ListRowFilters(userName)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to list row filters", http.StatusInternalServerError, err)
		}
		return
	}
	result := make([]RowFilterEntry, len(entries))
	for i, e := range entries {
		result[i] = RowFilterEntry{
			Table:  e.Table,
			Filter: rawToMap(e.Filter),
		}
	}
	um.jsonResponse(w, result, http.StatusOK)
}

func (um *UserApi) GetRowFilter(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
	table string,
) {
	filterJSON, err := um.um.GetRowFilter(userName, table)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else if errors.Is(err, ErrRowFilterNotFound) {
			um.httpError(w, ErrRowFilterNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to get row filter", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, RowFilterEntry{
		Table:  table,
		Filter: rawToMap(filterJSON),
	}, http.StatusOK)
}

func (um *UserApi) SetRowFilter(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
	table string,
) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		um.httpError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	filterJSON, err := json.Marshal(body)
	if err != nil {
		um.httpError(w, "Failed to marshal filter", http.StatusBadRequest, err)
		return
	}

	if err := um.um.SetRowFilter(userName, table, filterJSON); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to set row filter: "+err.Error(), http.StatusBadRequest, err)
		}
		return
	}
	um.jsonResponse(w, RowFilterEntry{
		Table:  table,
		Filter: body,
	}, http.StatusOK)
}

func (um *UserApi) RemoveRowFilter(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
	table string,
) {
	if err := um.um.RemoveRowFilter(userName, table); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			um.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound, err)
		} else if errors.Is(err, ErrRowFilterNotFound) {
			um.httpError(w, ErrRowFilterNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to remove row filter", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, nil, http.StatusNoContent)
}

func (um *UserApi) DeleteApiKey(
	w http.ResponseWriter,
	r *http.Request,
	userName UserNamePathParameter,
	keyId KeyIdPathParameter,
) {
	if err := um.um.DeleteApiKey(userName, keyId); err != nil {
		if errors.Is(err, ErrApiKeyNotFound) {
			um.httpError(w, ErrApiKeyNotFound.Error(), http.StatusNotFound, err)
		} else {
			um.httpError(w, "Failed to delete API key", http.StatusInternalServerError, err)
		}
		return
	}
	um.jsonResponse(w, nil, http.StatusNoContent)
}

// rawToMap converts json.RawMessage to map[string]interface{} for the generated types.
func rawToMap(raw json.RawMessage) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	return m
}
