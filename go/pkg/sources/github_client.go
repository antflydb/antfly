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
	"errors"
	"fmt"
	gh "github.com/google/go-github/v78/github"
	"golang.org/x/oauth2"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

type GitHubClient struct {
	HTTPClient *http.Client
	APIBase    *url.URL
}
type TreeEntry struct {
	Path string `json:"path"`
	SHA  string `json:"sha"`
	Type string `json:"type"`
	Mode string `json:"mode"`
	Size int64  `json:"size"`
}

func (c *GitHubClient) ResolveCommit(ctx context.Context, accessToken, repository, ref string) (string, error) {
	owner, repo, err := ParseRepository(repository)
	if err != nil {
		return "", err
	}
	client := c.newClient(accessToken)
	ref = strings.TrimSpace(ref)
	if ref == "" {
		repositoryInfo, _, err := client.Repositories.Get(ctx, owner, repo)
		if err != nil {
			return "", fmt.Errorf("get GitHub repository default branch: %w", err)
		}
		ref = strings.TrimSpace(repositoryInfo.GetDefaultBranch())
		if ref == "" {
			return "", fmt.Errorf("GitHub repository has no default branch")
		}
	}
	sha, _, err := client.Repositories.GetCommitSHA1(ctx, owner, repo, ref, "")
	if err != nil {
		return "", fmt.Errorf("resolve GitHub ref %q: %w", ref, err)
	}
	if strings.TrimSpace(sha) == "" {
		return "", fmt.Errorf("GitHub returned an empty commit SHA")
	}
	return sha, nil
}

func (c *GitHubClient) ListTree(ctx context.Context, accessToken, repository, commitSHA string) ([]TreeEntry, error) {
	owner, repo, err := ParseRepository(repository)
	if err != nil {
		return nil, err
	}
	client := c.newClient(accessToken)
	tree, _, err := client.Git.GetTree(ctx, owner, repo, commitSHA, true)
	if err != nil {
		return nil, fmt.Errorf("list recursive GitHub tree: %w", err)
	}
	if !tree.GetTruncated() {
		entries := mapTreeEntries(tree.Entries, "")
		sortTreeEntries(entries)
		return entries, nil
	}

	type queuedTree struct {
		sha    string
		prefix string
	}
	queue := []queuedTree{{sha: commitSHA}}
	entries := make([]TreeEntry, 0, len(tree.Entries))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		subtree, _, err := client.Git.GetTree(ctx, owner, repo, current.sha, false)
		if err != nil {
			return nil, fmt.Errorf("walk GitHub tree %q: %w", current.prefix, err)
		}
		if subtree.GetTruncated() {
			return nil, fmt.Errorf("GitHub returned a truncated non-recursive tree")
		}
		for _, entry := range subtree.Entries {
			path := joinGitHubTreePath(current.prefix, entry.GetPath())
			switch entry.GetType() {
			case "tree":
				queue = append(queue, queuedTree{sha: entry.GetSHA(), prefix: path})
			case "blob":
				if entry.GetMode() != "120000" {
					entries = append(entries, treeEntry(entry, path))
				}
			}
		}
	}
	sortTreeEntries(entries)
	return entries, nil
}

var ErrBlobTooLarge = errors.New("GitHub blob exceeds read byte limit")

func (c *GitHubClient) GetBlob(ctx context.Context, accessToken, repository, blobSHA string, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 || maxBytes == math.MaxInt64 {
		return nil, fmt.Errorf("invalid GitHub blob read byte limit %d", maxBytes)
	}
	reader, err := c.OpenBlob(ctx, accessToken, repository, blobSHA)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	content, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read GitHub blob %q: %w", blobSHA, err)
	}
	if int64(len(content)) > maxBytes {
		return nil, fmt.Errorf("%w: blob %q, limit %d bytes", ErrBlobTooLarge, blobSHA, maxBytes)
	}
	return content, nil
}

// OpenBlob streams a pinned Git object; callers must bound the read and close it.
func (c *GitHubClient) OpenBlob(ctx context.Context, accessToken, repository, blobSHA string) (io.ReadCloser, error) {
	return c.blobRequest(ctx, accessToken, repository, blobSHA, http.MethodGet)
}

// CheckBlobAccess rechecks Contents permission on the actual pinned blob
// without downloading its response body. Metadata/repository listing alone
// does not prove that a token can read repository content.
func (c *GitHubClient) CheckBlobAccess(ctx context.Context, accessToken, repository, blobSHA string) error {
	reader, err := c.blobRequest(ctx, accessToken, repository, blobSHA, http.MethodHead)
	if err != nil {
		return err
	}
	return reader.Close()
}

func (c *GitHubClient) blobRequest(ctx context.Context, accessToken, repository, blobSHA, method string) (io.ReadCloser, error) {
	owner, repo, err := ParseRepository(repository)
	if err != nil {
		return nil, err
	}
	client := c.newClient(accessToken)
	path := fmt.Sprintf("repos/%s/%s/git/blobs/%s", url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(blobSHA))
	req, err := client.NewRequest(method, path, nil)
	if err != nil {
		return nil, fmt.Errorf("create GitHub blob request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3.raw")
	resp, err := client.BareDo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get GitHub blob %q: %w", blobSHA, err)
	}
	return resp.Body, nil
}

func mapTreeEntries(entries []*gh.TreeEntry, prefix string) []TreeEntry {
	mapped := make([]TreeEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.GetType() != "blob" || entry.GetMode() == "120000" {
			continue
		}
		mapped = append(mapped, treeEntry(entry, joinGitHubTreePath(prefix, entry.GetPath())))
	}
	return mapped
}

func treeEntry(entry *gh.TreeEntry, path string) TreeEntry {
	return TreeEntry{Path: path, SHA: entry.GetSHA(), Type: entry.GetType(), Mode: entry.GetMode(), Size: int64(entry.GetSize())}
}

func sortTreeEntries(entries []TreeEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
}

func joinGitHubTreePath(prefix, path string) string {
	if prefix == "" {
		return path
	}
	return prefix + "/" + path
}

func (c *GitHubClient) newClient(accessToken string) *gh.Client {
	configured := c.HTTPClient
	if configured == nil {
		configured = http.DefaultClient
	}
	baseTransport := configured.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	transport := &oauth2.Transport{Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken}), Base: baseTransport}
	httpClient := &http.Client{Transport: transport, Timeout: configured.Timeout}
	client := gh.NewClient(httpClient)
	if c.APIBase != nil {
		client.BaseURL = c.APIBase
		client.UploadURL = c.APIBase
	}
	return client
}

// ParseRepository accepts owner/repo and canonical github.com repository URLs.
func ParseRepository(repository string) (string, string, error) {
	repository = strings.TrimSpace(repository)
	if parsed, err := url.Parse(repository); err == nil && parsed.Scheme != "" {
		if !strings.EqualFold(parsed.Scheme, "https") || !strings.EqualFold(parsed.Host, "github.com") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", "", fmt.Errorf("invalid GitHub repository %q", repository)
		}
		repository = strings.Trim(parsed.Path, "/")
	}
	repository = strings.TrimSuffix(repository, ".git")
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" || parts[0] == "." || parts[0] == ".." || parts[1] == "." || parts[1] == ".." {
		return "", "", fmt.Errorf("invalid GitHub repository %q", repository)
	}
	return parts[0], parts[1], nil
}
