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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommunityInventoryAndRevisionBoundRead(t *testing.T) {
	edited := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer installation-token", r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/repos/org/repo/issues":
			fmt.Fprint(w, `[{"id":1,"node_id":"I_1","number":1,"html_url":"https://github.com/org/repo/issues/1","title":"Retry design","body":"Keep a budget","user":{"login":"alice"},"labels":[{"name":"design"}],"state":"open"}]`)
		case "/repos/org/repo/issues/1":
			body := "Keep a budget"
			if edited {
				body = "Use a deadline"
			}
			fmt.Fprintf(w, `{"id":1,"node_id":"I_1","number":1,"html_url":"https://github.com/org/repo/issues/1","title":"Retry design","body":%q,"user":{"login":"alice"},"labels":[{"name":"design"}],"state":"open"}`, body)
		case "/repos/org/repo/issues/comments":
			fmt.Fprint(w, `[{"id":2,"node_id":"IC_2","html_url":"https://github.com/org/repo/issues/1#issuecomment-2","body":"Agreed","issue_url":"https://api.github.com/repos/org/repo/issues/1"}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := testGitHubClient(server)
	items, err := c.ListCommunity(context.Background(), "installation-token", "org/repo", []string{"issues", "comments"}, 10)
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.Equal(t, "Retry design\n\nKeep a budget", items[0].Text)
	require.Equal(t, "alice", items[0].Metadata["author"])
	item, err := c.ReadCommunity(context.Background(), "installation-token", "org/repo", items[0].Kind, items[0].Endpoint)
	require.NoError(t, err)
	require.Equal(t, items[0].Revision, item.Revision)
	edited = true
	item, err = c.ReadCommunity(context.Background(), "installation-token", "org/repo", items[0].Kind, items[0].Endpoint)
	require.NoError(t, err)
	require.NotEqual(t, items[0].Revision, item.Revision)
	_, err = c.ListCommunity(context.Background(), "installation-token", "org/repo", []string{"issues", "comments"}, 1)
	require.Error(t, err)
	_, err = c.ReadCommunity(context.Background(), "installation-token", "org/repo", "issue", "repos/other/repo/issues/1")
	require.Error(t, err)
}
func TestReviewContextAndThreadRelationships(t *testing.T) {
	raw := map[string]any{"node_id": "review-comment", "html_url": "https://github.com/org/repo/pull/4#discussion_r5", "body": "This races", "path": "src/retry.go", "line": 12, "side": "RIGHT", "commit_id": "abc", "in_reply_to_id": 42, "diff_hunk": "@@ -10 +10 @@\n+ retry()"}
	item, err := NormalizeCommunity("org/repo", "review_comment", "repos/org/repo/pulls/comments/5", raw)
	require.NoError(t, err)
	require.Contains(t, item.Text, "src/retry.go")
	require.Contains(t, item.Text, "retry()")
	require.Equal(t, 42, item.Metadata["in_reply_to_id"])
}

func TestPullInventoryDoesNotPageMixedIssues(t *testing.T) {
	issuesRead := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/org/repo/issues":
			issuesRead = true
			t.Error("PR/review inventory must not use the mutable mixed issue feed")
			fmt.Fprint(w, `[]`)
		case "/repos/org/repo/pulls/comments":
			fmt.Fprint(w, `[]`)
		case "/repos/org/repo/pulls":
			require.Equal(t, "all", r.URL.Query().Get("state"))
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			first, last := 200, 101
			if page == 2 {
				first, last = 100, 1
			} else {
				w.Header().Set("Link", "<http://"+r.Host+"/repos/org/repo/pulls?state=all&per_page=100&page=2>; rel=\"next\"")
			}
			rows := []map[string]any{}
			for n := first; n >= last; n-- {
				rows = append(rows, map[string]any{"number": n})
			}
			require.NoError(t, json.NewEncoder(w).Encode(rows))
		default:
			parts := strings.Split(r.URL.Path, "/")
			number := parts[5]
			if len(parts) == 7 && parts[6] == "reviews" {
				fmt.Fprintf(w, `[{"node_id":"R_%s","id":%s,"html_url":"https://github.com/org/repo/pull/%s#review","body":"review"}]`, number, number, number)
			} else {
				fmt.Fprintf(w, `{"node_id":"P_%s","number":%s,"html_url":"https://github.com/org/repo/pull/%s","body":"PR"}`, number, number, number)
			}
		}
	}))
	defer server.Close()
	c := testGitHubClient(server)
	for _, kinds := range [][]string{{"prs"}, {"reviews"}} {
		items, err := c.ListCommunity(t.Context(), "token", "org/repo", kinds, 1000)
		require.NoError(t, err)
		require.Len(t, items, 200)
		found := false
		for _, item := range items {
			if item.ID == "P_101" || item.ID == "R_101" {
				found = true
			}
		}
		require.True(t, found, "the page boundary item must remain selected")
	}
	require.False(t, issuesRead)
}

func TestCommunityAbsenceRequiresProviderEvidence(t *testing.T) {
	for _, test := range []struct {
		name, body   string
		exists, fail bool
	}{
		{"accessible", `{"data":{"nodes":[{"id":"I_1"}]}}`, true, false},
		{"deleted", `{"data":{"nodes":[null]}}`, false, false},
		{"not_found", `{"data":{"nodes":[null]},"errors":[{"type":"NOT_FOUND","path":["nodes",0]}]}`, false, false},
		{"permission", `{"data":{"nodes":[null]},"errors":[{"type":"FORBIDDEN","path":["nodes",0]}]}`, false, true},
		{"rate_limit", `{"errors":[{"type":"RATE_LIMITED"}]}`, false, true},
		{"wrong_identity", `{"data":{"nodes":[{"id":"other"}]}}`, false, true},
		{"missing", `{"data":{}}`, false, true},
		{"partial", `{"data":{"nodes":[]}}`, false, true},
		{"bad_not_found_path", `{"data":{"nodes":[null]},"errors":[{"type":"NOT_FOUND","path":["nodes",2]}]}`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/graphql", r.URL.Path)
				var request struct {
					Variables map[string][]string `json:"variables"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Equal(t, []string{"I_1"}, request.Variables["ids"])
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			c := testGitHubClient(server)
			exists, err := c.CommunityNodesExist(t.Context(), "token", []string{"I_1"})
			if !test.fail {
				require.Equal(t, []bool{test.exists}, exists)
			}
			if test.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func testGitHubClient(server *httptest.Server) *GitHubClient {
	base, _ := url.Parse(server.URL + "/")
	return &GitHubClient{HTTPClient: server.Client(), APIBase: base}
}
