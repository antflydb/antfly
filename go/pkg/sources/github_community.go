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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	gh "github.com/google/go-github/v78/github"
)

// CommunityItem uses the provider's stable item identity, independent of URLs
// and presentation paths. Revision covers all normalized text and metadata.
const maxCommunityInventoryBytes = 32 << 20

type CommunityItem struct {
	ID       string
	Kind     string
	URL      string
	Endpoint string
	Revision string
	Text     string
	Metadata map[string]any
}

func NormalizeCommunity(repository, kind, endpoint string, raw map[string]any) (CommunityItem, error) {
	id := stringValue(raw["node_id"])
	if id == "" {
		id = stringValue(raw["id"])
	}
	if id == "" {
		return CommunityItem{}, fmt.Errorf("GitHub community item has no identity")
	}
	uri := stringValue(raw["html_url"])
	if uri == "" {
		uri = stringValue(raw["url"])
	}
	if !strings.HasPrefix(uri, "https://github.com/") {
		return CommunityItem{}, fmt.Errorf("GitHub community item has invalid canonical URL")
	}
	metadata := map[string]any{"repository": repository, "resource_kind": kind, "provider_id": id}
	// Keep relationship and review location evidence. Exclude volatile response
	// counts, API URLs and installation tokens from revision identity/provenance.
	for _, key := range []string{"number", "state", "title", "created_at", "updated_at", "submitted_at", "author_association", "commit_id", "original_commit_id", "path", "line", "original_line", "side", "start_line", "start_side", "position", "original_position", "diff_hunk", "in_reply_to_id", "issue_url", "pull_request_url", "base", "head", "closed_at", "merged_at", "locked"} {
		if v, ok := raw[key]; ok && v != nil {
			metadata[key] = v
		}
	}
	if user, ok := raw["user"].(map[string]any); ok {
		metadata["author"] = user["login"]
	} else if user, ok := raw["author"].(map[string]any); ok {
		metadata["author"] = user["login"]
	}
	if labels, ok := raw["labels"].([]any); ok {
		names := []string{}
		for _, v := range labels {
			if label, ok := v.(map[string]any); ok {
				names = append(names, stringValue(label["name"]))
			}
		}
		metadata["labels"] = names
	}
	for _, key := range []string{"parent_id", "discussion_id", "reply_to_id"} {
		if v, ok := raw[key]; ok {
			metadata[key] = v
		}
	}
	title := stringValue(raw["title"])
	body := stringValue(raw["body"])
	text := body
	if title != "" {
		text = title + "\n\n" + body
	}
	if diff := stringValue(raw["diff_hunk"]); diff != "" {
		text += "\n\nReview context (" + stringValue(raw["path"]) + "):\n" + diff
	}
	canonical, _ := json.Marshal([]any{kind, uri, text, metadata})
	digest := sha256.Sum256(canonical)
	return CommunityItem{ID: id, Kind: kind, URL: uri, Endpoint: endpoint, Revision: hex.EncodeToString(digest[:]), Text: text, Metadata: metadata}, nil
}
func stringValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return ""
}

func (c *GitHubClient) communityGET(ctx context.Context, token, path string, out any) (*gh.Response, error) {
	client := c.newClient(token)
	req, err := client.NewRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(ctx, req, out)
}

// ListCommunity returns a complete bounded inventory or an error. No caller may
// turn a page/rate-limit/scope failure into authority to delete absent items.
func (c *GitHubClient) ListCommunity(ctx context.Context, token, repository string, kinds []string, limit int) ([]CommunityItem, error) {
	if _, _, err := ParseRepository(repository); err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, kind := range kinds {
		selected[kind] = true
	}
	items := []CommunityItem{}
	inventoryBytes := 0
	requests := 0
	add := func(kind, endpoint string, raw map[string]any) error {
		item, err := NormalizeCommunity(repository, kind, endpoint, raw)
		if err != nil {
			return err
		}
		if len(items) >= limit {
			return fmt.Errorf("GitHub community inventory exceeds %d items", limit)
		}
		metadataBytes, _ := json.Marshal(item.Metadata)
		inventoryBytes += len(item.Text) + len(metadataBytes)
		if inventoryBytes > maxCommunityInventoryBytes {
			return fmt.Errorf("GitHub community inventory exceeds byte budget; narrow the selected resources")
		}
		items = append(items, item)
		return nil
	}
	var list func(string, func(map[string]any) error) error
	list = func(route string, consume func(map[string]any) error) error {
		page := 1
		for {
			requests++
			if requests > 2048 {
				return fmt.Errorf("GitHub community request budget exhausted")
			}
			sep := "?"
			if strings.Contains(route, "?") {
				sep = "&"
			}
			var values []map[string]any
			response, err := c.communityGET(ctx, token, route+sep+"per_page=100&page="+strconv.Itoa(page), &values)
			if err != nil {
				return err
			}
			for _, raw := range values {
				if err := consume(raw); err != nil {
					return err
				}
			}
			if response == nil || response.NextPage == 0 {
				return nil
			}
			if response.NextPage <= page {
				return fmt.Errorf("GitHub repeated community page")
			}
			page = response.NextPage
		}
	}
	prefix := "repos/" + repository
	if selected["issues"] {
		if err := list(prefix+"/issues?state=all", func(raw map[string]any) error {
			if _, pr := raw["pull_request"]; pr {
				return nil
			}
			number := stringValue(raw["number"])
			if number == "" {
				return fmt.Errorf("GitHub issue has no number")
			}
			return add("issue", prefix+"/issues/"+number, raw)
		}); err != nil {
			return nil, err
		}
	}
	// PR and review inventory must not depend on the mixed issues feed.
	if selected["prs"] || selected["reviews"] {
		if err := list(prefix+"/pulls?state=all", func(raw map[string]any) error {
			number := stringValue(raw["number"])
			if number == "" {
				return fmt.Errorf("GitHub pull request has no number")
			}
			if selected["prs"] {
				var pull map[string]any
				requests++
				if requests > 2048 {
					return fmt.Errorf("GitHub community request budget exhausted")
				}
				if _, err := c.communityGET(ctx, token, prefix+"/pulls/"+number, &pull); err != nil {
					return err
				}
				if err := add("pr", prefix+"/pulls/"+number, pull); err != nil {
					return err
				}
			}
			if selected["reviews"] {
				return list(prefix+"/pulls/"+number+"/reviews", func(review map[string]any) error {
					review["pull_request_url"] = "https://github.com/" + repository + "/pull/" + number
					return add("review", prefix+"/pulls/"+number+"/reviews/"+stringValue(review["id"]), review)
				})
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if selected["comments"] {
		if err := list(prefix+"/issues/comments", func(raw map[string]any) error {
			return add("comment", prefix+"/issues/comments/"+stringValue(raw["id"]), raw)
		}); err != nil {
			return nil, err
		}
	}
	if selected["reviews"] {
		if err := list(prefix+"/pulls/comments", func(raw map[string]any) error {
			return add("review_comment", prefix+"/pulls/comments/"+stringValue(raw["id"]), raw)
		}); err != nil {
			return nil, err
		}
	}
	if selected["discussions"] {
		values, err := c.listDiscussions(ctx, token, repository, limit-len(items))
		if err != nil {
			return nil, err
		}
		for _, item := range values {
			metadataBytes, _ := json.Marshal(item.Metadata)
			inventoryBytes += len(item.Text) + len(metadataBytes)
		}
		if inventoryBytes > maxCommunityInventoryBytes {
			return nil, fmt.Errorf("GitHub community inventory exceeds byte budget")
		}
		items = append(items, values...)
	}
	return items, nil
}
func (c *GitHubClient) ReadCommunity(ctx context.Context, token, repository, kind, endpoint string) (CommunityItem, error) {
	if strings.HasPrefix(endpoint, "graphql:") {
		return c.readDiscussionNode(ctx, token, repository, kind, strings.TrimPrefix(endpoint, "graphql:"))
	}
	if !strings.HasPrefix(endpoint, "repos/"+repository+"/") || strings.ContainsAny(endpoint, "?#") {
		return CommunityItem{}, fmt.Errorf("GitHub community endpoint is outside selected repository")
	}
	var raw map[string]any
	if _, err := c.communityGET(ctx, token, endpoint, &raw); err != nil {
		return CommunityItem{}, err
	}
	// Review GET doesn't contain its parent URL; derive it from the pinned route.
	if kind == "review" {
		parts := strings.Split(endpoint, "/")
		if len(parts) != 7 {
			return CommunityItem{}, fmt.Errorf("invalid review endpoint")
		}
		raw["pull_request_url"] = "https://github.com/" + repository + "/pull/" + parts[4]
	}
	return NormalizeCommunity(repository, kind, endpoint, raw)
}

func discussionFields(kind string) string {
	common := `id url body createdAt updatedAt author { login } `
	if kind == "discussion" {
		return common + `title number closed locked category { name } repository { nameWithOwner }`
	}
	return common + `discussion { id repository { nameWithOwner } } replyTo { id }`
}

func (c *GitHubClient) graph(ctx context.Context, token, query string, variables map[string]any, out any) error {
	client := c.newClient(token)
	req, err := client.NewRequest("POST", "graphql", map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	var result struct {
		Data   json.RawMessage   `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if _, err := client.Do(ctx, req, &result); err != nil {
		return err
	}
	if len(result.Errors) > 0 || len(result.Data) == 0 {
		return fmt.Errorf("GitHub Discussions query failed; verify installation Discussions scope")
	}
	return json.Unmarshal(result.Data, out)
}
func discussionRaw(node map[string]any) map[string]any {
	raw := map[string]any{"node_id": node["id"], "html_url": node["url"], "body": node["body"], "title": node["title"], "number": node["number"], "created_at": node["createdAt"], "updated_at": node["updatedAt"], "author": node["author"], "locked": node["locked"]}
	if closed, ok := node["closed"].(bool); ok {
		if closed {
			raw["state"] = "closed"
		} else {
			raw["state"] = "open"
		}
	}
	if parent, ok := node["discussion"].(map[string]any); ok {
		raw["discussion_id"] = parent["id"]
	}
	if reply, ok := node["replyTo"].(map[string]any); ok {
		raw["reply_to_id"] = reply["id"]
	}
	return raw
}
func (c *GitHubClient) readDiscussionNode(ctx context.Context, token, repository, kind, id string) (CommunityItem, error) {
	var result struct {
		Node map[string]any `json:"node"`
	}
	if err := c.graph(ctx, token, `query($id:ID!){node(id:$id){... on Discussion {`+discussionFields("discussion")+`} ... on DiscussionComment {`+discussionFields("discussion_comment")+`}}}`, map[string]any{"id": id}, &result); err != nil {
		return CommunityItem{}, err
	}
	repo, _ := result.Node["repository"].(map[string]any)
	if kind != "discussion" {
		parent, _ := result.Node["discussion"].(map[string]any)
		repo, _ = parent["repository"].(map[string]any)
	}
	if repo["nameWithOwner"] != repository {
		return CommunityItem{}, fmt.Errorf("Discussion node is outside selected repository")
	}
	return NormalizeCommunity(repository, kind, "graphql:"+id, discussionRaw(result.Node))
}
func (c *GitHubClient) listDiscussions(ctx context.Context, token, repository string, limit int) ([]CommunityItem, error) {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid repository")
	}
	items := []CommunityItem{}
	inventoryBytes := 0
	requests := 0
	type connection struct {
		Nodes    []map[string]any `json:"nodes"`
		PageInfo struct {
			More   bool   `json:"hasNextPage"`
			Cursor string `json:"endCursor"`
		} `json:"pageInfo"`
	}
	var walk func(string, string, string) error
	walk = func(id, field, kind string) error {
		cursor := any(nil)
		seen := map[string]bool{}
		for {
			requests++
			if requests > 2048 {
				return fmt.Errorf("Discussion request budget exhausted")
			}
			var conn connection
			if field == "discussions" {
				var data struct {
					Repository struct {
						Discussions connection `json:"discussions"`
					} `json:"repository"`
				}
				query := `query($owner:String!,$repo:String!,$cursor:String){repository(owner:$owner,name:$repo){discussions(first:100,after:$cursor){nodes{` + discussionFields(kind) + `} pageInfo{hasNextPage endCursor}}}}`
				if err := c.graph(ctx, token, query, map[string]any{"owner": parts[0], "repo": parts[1], "cursor": cursor}, &data); err != nil {
					return err
				}
				conn = data.Repository.Discussions
			} else {
				typ := "Discussion"
				if field == "replies" {
					typ = "DiscussionComment"
				}
				var data struct {
					Node map[string]json.RawMessage `json:"node"`
				}
				query := `query($id:ID!,$cursor:String){node(id:$id){... on ` + typ + ` {` + field + `(first:100,after:$cursor){nodes{` + discussionFields(kind) + `} pageInfo{hasNextPage endCursor}}}}}`
				if err := c.graph(ctx, token, query, map[string]any{"id": id, "cursor": cursor}, &data); err != nil {
					return err
				}
				if err := json.Unmarshal(data.Node[field], &conn); err != nil {
					return err
				}
			}
			for _, node := range conn.Nodes {
				if len(items) >= limit {
					return fmt.Errorf("GitHub Discussions inventory exceeds item budget")
				}
				item, err := NormalizeCommunity(repository, kind, "graphql:"+stringValue(node["id"]), discussionRaw(node))
				if err != nil {
					return err
				}
				metadataBytes, _ := json.Marshal(item.Metadata)
				inventoryBytes += len(item.Text) + len(metadataBytes)
				if inventoryBytes > maxCommunityInventoryBytes {
					return fmt.Errorf("GitHub Discussions inventory exceeds byte budget")
				}
				items = append(items, item)
				if field == "discussions" {
					if err := walk(item.ID, "comments", "discussion_comment"); err != nil {
						return err
					}
				} else if field == "comments" {
					if err := walk(item.ID, "replies", "discussion_reply"); err != nil {
						return err
					}
				}
			}
			if !conn.PageInfo.More {
				return nil
			}
			next := conn.PageInfo.Cursor
			if next == "" || seen[next] {
				return fmt.Errorf("GitHub repeated Discussion cursor")
			}
			seen[next] = true
			cursor = next
		}
	}
	if err := walk("", "discussions", "discussion"); err != nil {
		return nil, err
	}
	return items, nil
}

// CommunityNodesExist checks provider identities independently of mutable list
// offsets. Batches keep large deletion sets within the provider's rate budget.
// Unavailable nodes (deleted or no longer shared) can leave this accessible
// dataset; transport, rate-limit and permission errors are not absence evidence.
func (c *GitHubClient) CommunityNodesExist(ctx context.Context, token string, ids []string) ([]bool, error) {
	if len(ids) == 0 || len(ids) > 100 {
		return nil, fmt.Errorf("GitHub absence batch must contain 1..100 IDs")
	}
	client := c.newClient(token)
	req, err := client.NewRequest("POST", "graphql", map[string]any{
		"query":     `query($ids: [ID!]!) { nodes(ids: $ids) { id } }`,
		"variables": map[string]any{"ids": ids},
	})
	if err != nil {
		return nil, err
	}
	var result struct {
		Data struct {
			Nodes []json.RawMessage `json:"nodes"`
		} `json:"data"`
		Errors []struct {
			Type string `json:"type"`
			Path []any  `json:"path"`
		} `json:"errors"`
	}
	if _, err := client.Do(ctx, req, &result); err != nil {
		return nil, err
	}
	for _, failure := range result.Errors {
		if failure.Type != "NOT_FOUND" || len(failure.Path) != 2 || failure.Path[0] != "nodes" {
			return nil, fmt.Errorf("GitHub community absence check failed")
		}
		index, ok := failure.Path[1].(float64)
		if !ok || index < 0 || index >= float64(len(ids)) || index != float64(int(index)) {
			return nil, fmt.Errorf("GitHub community absence error has invalid node index")
		}
	}
	if len(result.Data.Nodes) != len(ids) {
		return nil, fmt.Errorf("GitHub community absence lacks complete node evidence")
	}
	exists := make([]bool, len(ids))
	for i, raw := range result.Data.Nodes {
		if string(raw) == "null" {
			continue
		}
		var node struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &node); err != nil {
			return nil, err
		}
		if node.ID != ids[i] {
			return nil, fmt.Errorf("GitHub community absence identity mismatch")
		}
		exists[i] = true
	}
	return exists, nil
}
