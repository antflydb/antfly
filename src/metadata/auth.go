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

package metadata

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/antflydb/antfly/src/usermgr"
	"go.uber.org/zap"
)

type apiKeyPermissionsKey struct{}

func apiKeyPermissionsFromContext(r *http.Request) []usermgr.Permission {
	perms, _ := r.Context().Value(apiKeyPermissionsKey{}).([]usermgr.Permission)
	return perms
}

// checkPermission performs the two-step auth check (Casbin RBAC + API key scope)
// for a single resource. Returns true if authorized.
func (ms *MetadataStore) checkPermission(
	w http.ResponseWriter,
	username string,
	keyPerms []usermgr.Permission,
	resourceType usermgr.ResourceType,
	resource string,
	permType usermgr.PermissionType,
) bool {
	ok, err := ms.um.Enforce(username, resourceType, resource, permType)
	if err != nil {
		ms.logger.Error("Error during enforcement check",
			zap.Error(err),
			zap.String("username", username),
			zap.String("resourceType", string(resourceType)),
			zap.String("resource", resource),
			zap.String("permissionType", string(permType)))
		errorResponse(w, "Forbidden", http.StatusForbidden)
		return false
	}
	if !ok {
		errorResponse(w, "Forbidden", http.StatusForbidden)
		return false
	}
	if len(keyPerms) > 0 {
		if !matchesApiKeyPermission(keyPerms, resourceType, resource, permType) {
			errorResponse(w, "Forbidden", http.StatusForbidden)
			return false
		}
	}
	return true
}

func (ms *MetadataStore) ensureAuth(
	w http.ResponseWriter,
	r *http.Request,
	resourceType usermgr.ResourceType,
	resource string,
	permissionType usermgr.PermissionType,
) bool {
	if !ms.config.EnableAuth {
		return true
	}
	username := r.Header.Get("X-Authenticated-User")
	if resource != "*" {
		resource = r.PathValue(resource)
	}
	return ms.checkPermission(w, username, apiKeyPermissionsFromContext(r), resourceType, resource, permissionType)
}

// ensureMultiTableAuth checks Casbin RBAC and API key scope permissions for
// each table in the provided map. It returns false (and writes an HTTP error)
// if any check fails. This should be used by endpoints that operate on
// multiple tables specified in the request body (e.g. MultiBatchWrite,
// CommitTransaction) where the standard path-based authMiddleware cannot apply.
func (ms *MetadataStore) ensureMultiTableAuth(
	w http.ResponseWriter,
	r *http.Request,
	tables map[string]usermgr.PermissionType,
) bool {
	if !ms.config.EnableAuth {
		return true
	}
	username := r.Header.Get("X-Authenticated-User")
	keyPerms := apiKeyPermissionsFromContext(r)
	for tableName, permType := range tables {
		if !ms.checkPermission(w, username, keyPerms, usermgr.ResourceTypeTable, tableName, permType) {
			return false
		}
	}
	return true
}

// matchesApiKeyPermission checks if the requested action matches at least one
// of the API key's explicit permissions, using the same wildcard matching as Casbin.
func matchesApiKeyPermission(
	perms []usermgr.Permission,
	resourceType usermgr.ResourceType,
	resource string,
	permissionType usermgr.PermissionType,
) bool {
	for _, p := range perms {
		typMatch := p.ResourceType == resourceType || p.ResourceType == "*"
		resMatch := p.Resource == resource || p.Resource == "*"
		actMatch := p.Type == permissionType || p.Type == "*"
		if typMatch && resMatch && actMatch {
			return true
		}
	}
	return false
}

func (ms *MetadataStore) authMiddleware(
	resourceType usermgr.ResourceType,
	resource string,
	permissionType usermgr.PermissionType,
	next http.HandlerFunc,
) http.Handler {
	if !ms.config.EnableAuth {
		// If auth is disabled, just call the next handler directly
		return next
	}
	return ms.authnMiddleware(ms.authzMiddleware(resourceType, resource, permissionType, next))
}

func (ms *MetadataStore) authzMiddleware(
	resourceType usermgr.ResourceType,
	resource string,
	permissionType usermgr.PermissionType,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ms.ensureAuth(w, r, resourceType, resource, permissionType) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (ms *MetadataStore) authnMiddleware(next http.Handler) http.Handler {
	if !ms.config.EnableAuth {
		// If auth is disabled, just call the next handler directly
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			w.Header().Set("WWW-Authenticate", `Basic, ApiKey, Bearer`)
			errorResponse(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		authParts := strings.SplitN(authHeader, " ", 2)
		if len(authParts) != 2 {
			errorResponse(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		switch authParts[0] {
		case "Basic":
			decoded, err := base64.StdEncoding.DecodeString(authParts[1])
			if err != nil {
				errorResponse(w, "Unauthorized", http.StatusBadRequest)
				return
			}
			creds := strings.SplitN(string(decoded), ":", 2)
			if len(creds) != 2 {
				errorResponse(w, "Unauthorized", http.StatusBadRequest)
				return
			}
			username, password := creds[0], creds[1]
			if _, err := ms.um.AuthenticateUser(username, password); err != nil {
				errorResponse(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			r.Header.Set("X-Authenticated-User", username)

		case "ApiKey", "Bearer":
			// Both use base64(id:secret) format
			decoded, err := base64.StdEncoding.DecodeString(authParts[1])
			if err != nil {
				errorResponse(w, "Unauthorized", http.StatusBadRequest)
				return
			}
			creds := strings.SplitN(string(decoded), ":", 2)
			if len(creds) != 2 {
				errorResponse(w, "Unauthorized", http.StatusBadRequest)
				return
			}
			keyID, keySecret := creds[0], creds[1]
			username, permissions, err := ms.um.ValidateApiKey(keyID, keySecret)
			if err != nil {
				errorResponse(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			r.Header.Set("X-Authenticated-User", username)
			if len(permissions) > 0 {
				ctx := context.WithValue(r.Context(), apiKeyPermissionsKey{}, permissions)
				r = r.WithContext(ctx)
			}

		default:
			errorResponse(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
