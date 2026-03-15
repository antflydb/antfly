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
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/usermgr"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const testDBPath = "test_leader_auth.db"

func newTestLeaderNode(t *testing.T) *MetadataStore {
	t.Helper()

	db, err := pebble.Open(testDBPath, pebbleutils.NewMemPebbleOpts())
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close DB: %v", err)
		}
	})
	require.NoError(t, err)

	um, err := usermgr.NewUserManager(&kv.PebbleDB{DB: db})
	require.NoError(t, err)

	// Create a test user and permissions
	_, err = um.CreateUser("testuser", "testpassword", []usermgr.Permission{
		{
			Resource:     "testtable",
			ResourceType: usermgr.ResourceTypeTable,
			Type:         usermgr.PermissionTypeRead,
		},
		{
			Resource:     "another_table",
			ResourceType: usermgr.ResourceTypeTable,
			Type:         usermgr.PermissionTypeWrite,
		},
		{
			Resource:     "*",
			ResourceType: usermgr.ResourceTypeAsterisk,
			Type:         usermgr.PermissionTypeAdmin,
		}, // Policy: p, testuser, *, *, admin
		{
			Resource:     "*",
			ResourceType: usermgr.ResourceTypeTable,
			Type:         usermgr.PermissionTypeAdmin,
		}, // Policy: p, testuser, table, *, admin
	})
	require.NoError(t, err)

	zapLogger, _ := zap.NewDevelopment()
	return &MetadataStore{
		logger: zapLogger,
		um:     um,
	}
}

func TestAuthMiddleware(t *testing.T) {
	ln := newTestLeaderNode(t)
	ln.config = &common.Config{
		EnableAuth: true, // Enable auth for testing
	}

	// Dummy handler to be protected by middleware
	dummyHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			t.Logf("failed to write response: %v", err)
		}
	}

	tests := []struct {
		name               string
		resourceType       usermgr.ResourceType
		resourceNameInPath string // if "*", means not from path. If a name, it's the path param name
		permissionType     usermgr.PermissionType
		path               string // path for the request, including any params
		authHeader         string
		expectedStatus     int
		expectedBody       string
	}{
		{
			name:               "No Authorization Header",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeRead,
			path:               "/tables/testtable",
			authHeader:         "",
			expectedStatus:     http.StatusUnauthorized,
			expectedBody:       "Unauthorized",
		},
		{
			name:               "Invalid Authorization Header Format",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeRead,
			path:               "/tables/testtable",
			authHeader:         "Bearer sometoken",
			expectedStatus:     http.StatusBadRequest, // Bearer is a valid scheme; "sometoken" is invalid base64
			expectedBody:       "Unauthorized",
		},
		{
			name:               "Invalid Base64 Encoding",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeRead,
			path:               "/tables/testtable",
			authHeader:         "Basic invalid-base64",
			expectedStatus:     http.StatusBadRequest, // Corrected based on typical basic auth parsing
			expectedBody:       "Unauthorized",        // Error message might vary based on exact authn failure point
		},
		{
			name:               "Malformed Credentials",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeRead,
			path:               "/tables/testtable",
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser"),
			), // Missing colon
			expectedStatus: http.StatusBadRequest, // Corrected based on typical basic auth parsing
			expectedBody:   "Unauthorized",        // Error message might vary
		},
		{
			name:               "Incorrect Password",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeRead,
			path:               "/tables/testtable",
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser:wrongpassword"),
			),
			expectedStatus: http.StatusUnauthorized,
			expectedBody:   "Unauthorized",
		},
		{
			name:               "Correct Credentials, Insufficient Permissions",
			resourceType:       usermgr.ResourceTypeTable,   // r.typ = table
			resourceNameInPath: "tableName",                 // r.obj = testtable (from path)
			permissionType:     usermgr.PermissionTypeWrite, // r.act = readwrite
			path:               "/tables/testtable",         // testuser has "p, testuser, table, testtable, readonly"
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser:testpassword"),
			),
			expectedStatus: http.StatusForbidden, // No direct "readwrite" for "testtable", and "p, testuser, table, *, admin" doesn't grant "readwrite"
			expectedBody:   "Forbidden",
		},
		{
			name:               "Correct Credentials, Sufficient Permissions (specific resource)",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeRead,
			path:               "/tables/testtable",
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser:testpassword"),
			),
			expectedStatus: http.StatusOK,
			expectedBody:   "OK",
		},
		{
			name:               "Correct Credentials, Sufficient Permissions (path variable resource)",
			resourceType:       usermgr.ResourceTypeTable,
			resourceNameInPath: "tableName",
			permissionType:     usermgr.PermissionTypeWrite, // testuser has readwrite for another_table
			path:               "/tables/another_table",
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser:testpassword"),
			),
			expectedStatus: http.StatusOK,
			expectedBody:   "OK",
		},
		{
			name:               "Correct Credentials, Sufficient Permissions (wildcard resource in policy)",
			resourceType:       usermgr.ResourceTypeTable, // r.typ = "table"
			resourceNameInPath: "*",                       // r.obj = "*"
			permissionType:     "admin",                   // r.act = "admin" -> matches "p, testuser, table, *, admin"
			path:               "/tables/any_table_name_really",
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser:testpassword"),
			),
			expectedStatus: http.StatusOK,
			expectedBody:   "OK",
		},
		{
			name:               "Correct Credentials, Path resource not matching any specific rule, but wildcard matches",
			resourceType:       usermgr.ResourceTypeTable, // r.typ = "table"
			resourceNameInPath: "tableName",
			permissionType:     "admin",                                    // r.act = "admin" -> matches "p, testuser, table, *, admin"
			path:               "/tables/unknown_table_for_specific_rules", // r.obj = "unknown_table_for_specific_rules"
			authHeader: "Basic " + base64.StdEncoding.EncodeToString(
				[]byte("testuser:testpassword"),
			),
			expectedStatus: http.StatusOK,
			expectedBody:   "OK",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://localhost"+tt.path, nil)
			if tt.resourceNameInPath != "*" &&
				strings.Contains(tt.path, "{"+tt.resourceNameInPath+"}") {
				parts := strings.Split(tt.path, "/")
				if len(parts) > 2 {
					req.SetPathValue(tt.resourceNameInPath, parts[len(parts)-1])
				}
			} else if tt.resourceNameInPath != "*" && !strings.Contains(tt.path, "{") {
				pathParts := strings.Split(strings.Trim(tt.path, "/"), "/")
				if len(pathParts) > 0 {
					req.SetPathValue(tt.resourceNameInPath, pathParts[len(pathParts)-1])
				}
			}

			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rr := httptest.NewRecorder()

			// The authMiddleware itself calls authnMiddleware and then authzMiddleware
			// So we test the combined effect here by calling the outer ln.authMiddleware
			handler := ln.authMiddleware(
				tt.resourceType,
				tt.resourceNameInPath,
				tt.permissionType,
				http.HandlerFunc(dummyHandler),
			)
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tt.expectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v. Body: %s",
					status, tt.expectedStatus, rr.Body.String())
			}

			if body := rr.Body.String(); !strings.Contains(body, tt.expectedBody) {
				// For debugging body mismatches when status is also wrong
				if rr.Code == tt.expectedStatus {
					t.Errorf("handler returned unexpected body: got %q want %q",
						body, tt.expectedBody)
				}
			}
		})
	}
}
