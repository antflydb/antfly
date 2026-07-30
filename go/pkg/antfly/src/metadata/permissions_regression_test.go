// Copyright 2026 Antfly, Inc.
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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	antflymcp "github.com/antflydb/antfly/go/pkg/antfly/src/mcp"
	"github.com/antflydb/antfly/go/pkg/antfly/src/usermgr"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type authorizationTransport struct {
	base  http.RoundTripper
	value string
}

func (t authorizationTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set("Authorization", t.value)
	return t.base.RoundTrip(clone)
}

func TestMCPWriteDeniedForReadOnlyPrincipal(t *testing.T) {
	metadataStore := newTestLeaderNode(t)
	metadataStore.config = &common.Config{EnableAuth: true}
	_, err := metadataStore.um.CreateUser("readonly", "password", []usermgr.Permission{{
		Resource:     "documents",
		ResourceType: usermgr.ResourceTypeTable,
		Type:         usermgr.PermissionTypeRead,
	}})
	require.NoError(t, err)

	adapter := newMCPAdapter(&TableApi{ln: metadataStore})
	ctx := context.WithValue(context.Background(), authenticatedPrincipalKey{}, authenticatedPrincipal{
		username: "readonly",
	})

	_, err = adapter.Batch(ctx, "documents", map[string]any{
		"doc-1": map[string]any{"body": "must not be written"},
	}, nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "write permission"), err.Error())
}

func TestMCPPermissionChecksFailClosedWithoutPrincipal(t *testing.T) {
	metadataStore := newTestLeaderNode(t)
	metadataStore.config = &common.Config{EnableAuth: true}
	adapter := newMCPAdapter(&TableApi{ln: metadataStore})

	_, err := adapter.Batch(context.Background(), "documents", nil, nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "write permission"), err.Error())
}

func TestMCPHTTPTransportPreservesAuthenticatedPrincipal(t *testing.T) {
	metadataStore := newTestLeaderNode(t)
	metadataStore.config = &common.Config{EnableAuth: true}
	_, err := metadataStore.um.CreateUser("readonly-http", "password", []usermgr.Permission{{
		Resource:     "documents",
		ResourceType: usermgr.ResourceTypeTable,
		Type:         usermgr.PermissionTypeRead,
	}})
	require.NoError(t, err)

	adapter := newMCPAdapter(&TableApi{ln: metadataStore})
	handler := antflymcp.NewMCPHandler(antflymcp.NewMCPServer(adapter))
	server := httptest.NewServer(metadataStore.authnMiddleware(handler))
	t.Cleanup(server.Close)

	credentials := base64.StdEncoding.EncodeToString([]byte("readonly-http:password"))
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "permissions-test", Version: "v0.0.1"}, nil)
	session, err := client.Connect(context.Background(), &sdkmcp.StreamableClientTransport{
		Endpoint: server.URL,
		HTTPClient: &http.Client{Transport: authorizationTransport{
			base:  http.DefaultTransport,
			value: "Basic " + credentials,
		}},
		DisableStandaloneSSE: true,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	result, err := session.CallTool(context.Background(), &sdkmcp.CallToolParams{
		Name: "batch",
		Arguments: map[string]any{
			"tableName": "documents",
			"writes": map[string]any{
				"doc-1": map[string]any{"body": "must not be written"},
			},
			"deletes": []string{},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)
	text, ok := result.Content[0].(*sdkmcp.TextContent)
	require.True(t, ok)
	require.Contains(t, text.Text, "write permission")
}

func TestQueryReadPermissionsIncludesNestedJoinTables(t *testing.T) {
	req := QueryRequest{
		Table: "orders",
		Join: JoinClause{
			RightTable: "customers",
			NestedJoin: &JoinClause{
				RightTable: "regions",
			},
		},
	}

	required := queryReadPermissions(&req)
	require.Equal(t, map[string]usermgr.PermissionType{
		"orders":    usermgr.PermissionTypeRead,
		"customers": usermgr.PermissionTypeRead,
		"regions":   usermgr.PermissionTypeRead,
	}, required)
}
