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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
)

type GitHubRepositorySelection struct {
	Repository string   `json:"repository"`
	Ref        string   `json:"ref,omitempty"`
	Prefix     string   `json:"prefix,omitempty"`
	Include    []string `json:"include,omitempty"`
}
type GitHubConfig struct {
	Repositories    []GitHubRepositorySelection `json:"repositories,omitempty"`
	Repository      string                      `json:"repository,omitempty"`
	Ref             string                      `json:"ref,omitempty"`
	Prefix          string                      `json:"prefix,omitempty"`
	Include         []string                    `json:"include,omitempty"`
	Patterns        []string                    `json:"patterns,omitempty"`
	ExcludePatterns []string                    `json:"exclude_patterns,omitempty"`
	Suffixes        []string                    `json:"suffixes,omitempty"`
}

func GitHubSelections(raw SourceConfig) (GitHubConfig, []GitHubRepositorySelection, error) {
	config, err := decodeSourceConfig[GitHubConfig](raw)
	if err != nil {
		return config, nil, err
	}
	selections := config.Repositories
	if config.Repository != "" {
		if len(selections) > 0 {
			return config, nil, fmt.Errorf("%w: choose repository or repositories", ErrInvalidInput)
		}
		selections = []GitHubRepositorySelection{{Repository: config.Repository, Ref: config.Ref, Prefix: config.Prefix, Include: config.Include}}
	}
	if len(selections) == 0 || len(selections) > 100 {
		return config, nil, fmt.Errorf("%w: select 1..100 repositories", ErrInvalidInput)
	}
	seen := map[string]bool{}
	for i := range selections {
		selected := &selections[i]
		parts := strings.Split(selected.Repository, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(selected.Repository, "?#\\\x00 ") {
			return config, nil, fmt.Errorf("%w: invalid repository", ErrInvalidInput)
		}
		// Connected code selection obeys the same path contract as ordinary
		// imports. Validate before any provider enumeration; an invalid path
		// must never be interpreted by the selector as an empty scope.
		prefix, err := normalizeGitHubSourcePath(selected.Prefix, true)
		if err != nil {
			return config, nil, err
		}
		selected.Prefix = prefix
		identity := strings.ToLower(selected.Repository)
		if seen[identity] {
			return config, nil, fmt.Errorf("%w: duplicate repository selection", ErrInvalidInput)
		}
		seen[identity] = true
		if len(selected.Include) == 0 {
			selected.Include = []string{"code", "issues", "prs", "comments", "reviews", "discussions"}
		}
		for _, kind := range selected.Include {
			switch kind {
			case "code", "issues", "prs", "comments", "reviews", "discussions":
			default:
				return config, nil, fmt.Errorf("%w: unsupported GitHub resource kind %q", ErrInvalidInput, kind)
			}
		}
	}
	return config, selections, nil
}
func (c *GitHubConnector) Validate(ctx context.Context, r Request) error {
	_, selections, err := GitHubSelections(r.Config)
	if err != nil {
		return err
	}
	access, err := c.access(ctx)
	if err != nil {
		return err
	}
	for _, selection := range selections {
		if _, ok := findInstallationRepository(access.Repositories, selection.Repository); access.Repositories != nil && !ok {
			return fmt.Errorf("%w: selected repository is not accessible", ErrInvalidInput)
		}
	}
	return nil
}
func (c *GitHubConnector) Enumerate(ctx context.Context, r Request) (Inventory, error) {
	config, selections, err := GitHubSelections(r.Config)
	if err != nil {
		return Inventory{}, err
	}
	if err := c.Validate(ctx, r); err != nil {
		return Inventory{}, err
	}
	access, err := c.access(ctx)
	if err != nil {
		return Inventory{}, err
	}
	snapshot := Inventory{Authoritative: !r.Preview && len(r.RetrySourceKeys) == 0, BatchObjects: 1, BatchBytes: c.byteLimit()}
	retry := exactSourceKeySet(r.RetrySourceKeys)
	add := func(o Item) error {
		if len(retry) > 0 || len(r.RetrySourceKeys) > 0 {
			if _, ok := retry[o.Path]; !ok {
				return nil
			}
			delete(retry, o.Path)
		}
		if len(snapshot.Objects) >= maxImportSourceObjects {
			return fmt.Errorf("%w: GitHub source item budget exceeded", ErrInvalidInput)
		}
		o.ReadMode = sourceReadBytes
		o.ObjectIndex = len(snapshot.Objects) + 1
		snapshot.Objects = append(snapshot.Objects, o)
		return nil
	}
	for _, selection := range selections {
		repository, ok := findInstallationRepository(access.Repositories, selection.Repository)
		if !ok && access.Repositories == nil {
			repository = Repository{FullName: selection.Repository}
		}
		for _, kind := range selection.Include {
			if kind != "code" {
				continue
			}
			commit, err := c.Client.ResolveCommit(ctx, access.Token, repository.FullName, selection.Ref)
			if err != nil {
				return Inventory{}, err
			}
			tree, err := c.Client.ListTree(ctx, access.Token, repository.FullName, commit)
			if err != nil {
				return Inventory{}, err
			}
			local := codeSelection{Repository: repository.FullName, Ref: selection.Ref, Prefix: selection.Prefix, Patterns: config.Patterns, ExcludePatterns: config.ExcludePatterns, Suffixes: config.Suffixes}
			for _, entry := range selectGitHubTreeEntries(tree, local, ImportModeSource, nil) {
				if KnownBinarySource(entry.Path) {
					snapshot.Diagnostics = append(snapshot.Diagnostics, Diagnostic{Level: "warning", Code: "source_binary_not_selected", Detail: "Binary repository content requires document extraction/rendering and is not part of code selection.", Source: &DiagnosticSource{Key: repository.FullName + "/code/" + entry.Path, SourceKind: "github", Revision: entry.SHA}})
					continue
				}
				object := githubSourceObject(r.SourceNamespace, repository.FullName, selection.Ref, commit, entry, len(snapshot.Objects)+1)
				object.ID = GitHubIdentity(r.SourceNamespace, repository.FullName, "code", entry.Path)
				object.Path = repository.FullName + "/code/" + entry.Path
				object.Metadata["resource_kind"] = "code"
				if err := add(object); err != nil {
					return Inventory{}, err
				}
				if r.Preview && len(snapshot.Objects) > 0 {
					return snapshot, nil
				}
			}
		}
		kinds := []string{}
		for _, kind := range selection.Include {
			if kind != "code" {
				kinds = append(kinds, kind)
			}
		}
		if len(kinds) == 0 {
			continue
		}
		client, ok := c.Client.(interface {
			ListCommunity(context.Context, string, string, []string, int) ([]CommunityItem, error)
		})
		if !ok {
			return Inventory{}, fmt.Errorf("GitHub client lacks community ingestion")
		}
		items, err := client.ListCommunity(ctx, access.Token, repository.FullName, kinds, maxImportSourceObjects-len(snapshot.Objects))
		if err != nil {
			return Inventory{}, err
		}
		for _, item := range items {
			metadata := cloneSourceMetadata(item.Metadata)
			metadata["canonical_url"] = item.URL
			metadata["community_endpoint"] = item.Endpoint
			metadata["resource_kind"] = item.Kind
			metadata["repository"] = repository.FullName
			object := Item{ID: GitHubIdentity(r.SourceNamespace, repository.FullName, "community/"+item.Kind, item.ID), Path: repository.FullName + "/" + item.Kind + "/" + item.ID + ".txt", Filename: item.Kind + ".txt", Revision: item.Revision, URL: item.URL, SourceKind: "github", Size: int64(len(item.Text)), Metadata: metadata}
			if err := add(object); err != nil {
				return Inventory{}, err
			}
			if r.Preview && len(snapshot.Objects) > 0 {
				return snapshot, nil
			}
		}
	}
	if len(retry) > 0 {
		return Inventory{}, fmt.Errorf("%w: retry items are no longer accessible", ErrInvalidInput)
	}
	return snapshot, nil
}
func (c *GitHubConnector) ReadBatch(ctx context.Context, r Request, objects []Item) ([]Input, error) {
	if len(objects) > 1 {
		return nil, fmt.Errorf("connected GitHub reads one item per batch")
	}
	if err := c.Validate(ctx, r); err != nil {
		return nil, err
	}
	access, err := c.access(ctx)
	if err != nil {
		return nil, err
	}
	inputs := []Input{}
	for _, object := range objects {
		input := inputFromObject(object)
		repo := configString(object.Metadata, "repository")
		if _, ok := findInstallationRepository(access.Repositories, repo); access.Repositories != nil && !ok {
			return nil, fmt.Errorf("repository access revoked")
		}
		ceiling := min(int64(maxImportSourceObjectBytes), c.byteLimit())
		if endpoint := configString(object.Metadata, "community_endpoint"); endpoint != "" {
			client, ok := c.Client.(interface {
				ReadCommunity(context.Context, string, string, string, string) (CommunityItem, error)
			})
			if !ok {
				return nil, fmt.Errorf("GitHub client lacks community reads")
			}
			item, readErr := client.ReadCommunity(ctx, access.Token, repo, configString(object.Metadata, "resource_kind"), endpoint)
			if readErr == nil && item.Revision != object.Revision {
				readErr = &RevisionChangedError{Expected: object.Revision, Actual: item.Revision}
			}
			if readErr == nil && int64(len(item.Text)) > ceiling {
				readErr = fmt.Errorf("%w: community item exceeds byte budget", ErrInvalidInput)
			}
			input.ReadError = readErr
			if readErr == nil {
				input.Bytes = []byte(item.Text)
			}
		} else {
			input.Bytes, input.ReadError = c.Client.GetBlob(ctx, access.Token, repo, object.Revision, ceiling)
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}
func (c *GitHubConnector) OpenSource(ctx context.Context, r Request, o Item) (io.ReadCloser, error) {
	inputs, err := c.ReadBatch(ctx, r, []Item{o})
	if err != nil {
		return nil, err
	}
	if inputs[0].ReadError != nil {
		return nil, inputs[0].ReadError
	}
	return io.NopCloser(bytes.NewReader(inputs[0].Bytes)), nil
}

func KnownBinarySource(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".pdf", ".docx", ".pptx", ".xlsx", ".zip", ".gz", ".xz", ".zst", ".wasm", ".so", ".dylib", ".dll", ".exe", ".mp4", ".mp3", ".wav", ".woff", ".woff2", ".ttf", ".otf", ".parquet", ".sqlite", ".db":
		return true
	}
	return false
}

// The resource kind is part of identity, not just a display-path prefix. Code
// paths are arbitrary repository content and cannot share a namespace with
// provider-issued community IDs (or IDs from different community resources).
func GitHubIdentity(namespace, repository, resource, id string) string {
	return "github://" + strings.Trim(namespace, "/") + "/" + repository + "/" + resource + "/" + id
}

func (c *GitHubConnector) ValidateAbsentSources(ctx context.Context, r Request, ids []string) error {
	if r.Mode != ImportModeSource {
		return fmt.Errorf("%w: canonical source mode required", ErrInvalidInput)
	}
	if err := c.Validate(ctx, r); err != nil {
		return err
	}
	access, err := c.access(ctx)
	if err != nil {
		return err
	}
	client, ok := c.Client.(interface {
		CommunityNodesExist(context.Context, string, []string) ([]bool, error)
	})
	_, selections, err := GitHubSelections(r.Config)
	if err != nil {
		return err
	}
	selected := map[string]map[string]bool{}
	for _, selection := range selections {
		kinds := map[string]bool{}
		for _, kind := range selection.Include {
			kinds[kind] = true
		}
		selected[strings.ToLower(selection.Repository)] = kinds
	}
	prefix := "github://" + strings.Trim(r.SourceNamespace, "/") + "/"
	if len(ids) > maxImportSourceObjects {
		return fmt.Errorf("GitHub absence check budget exceeded")
	}
	communityIDs := []string{}
	for _, id := range ids {
		if !strings.HasPrefix(id, prefix) {
			return fmt.Errorf("GitHub absence identity outside source namespace")
		}
		parts := strings.SplitN(strings.TrimPrefix(id, prefix), "/", 5)
		if len(parts) < 4 {
			return fmt.Errorf("invalid GitHub absence identity")
		}
		if parts[2] == "code" {
			continue
		}
		if parts[2] != "community" || len(parts) != 5 || parts[4] == "" {
			return fmt.Errorf("invalid GitHub community absence identity")
		}
		kind := map[string]string{"issue": "issues", "pr": "prs", "comment": "comments", "review": "reviews", "review_comment": "reviews", "discussion": "discussions", "discussion_comment": "discussions", "discussion_reply": "discussions"}[parts[3]]
		if kind == "" {
			return fmt.Errorf("unsupported GitHub community absence kind")
		}
		if !selected[strings.ToLower(parts[0]+"/"+parts[1])][kind] {
			continue
		} // Explicit configuration removal.
		if !ok {
			return fmt.Errorf("GitHub client lacks community absence validation")
		}
		communityIDs = append(communityIDs, parts[4])
	}
	for start := 0; start < len(communityIDs); start += 100 {
		batch := communityIDs[start:min(start+100, len(communityIDs))]
		exists, err := client.CommunityNodesExist(ctx, access.Token, batch)
		if err != nil {
			return err
		}
		if len(exists) != len(batch) {
			return fmt.Errorf("GitHub community absence lacks complete node evidence")
		}
		for i, found := range exists {
			if found {
				return fmt.Errorf("%w: GitHub inventory omitted accessible community item %s", ErrInventoryIncomplete, batch[i])
			}
		}
	}
	return nil
}

func selectGitHubTreeEntries(entries []TreeEntry, config codeSelection, mode string, retrySourceKeys []string) []TreeEntry {
	exactKey, _ := normalizeGitHubSourcePath(config.Key, true)
	prefix, _ := normalizeGitHubSourcePath(config.Prefix, true)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	retryKeys := exactSourceKeySet(retrySourceKeys)
	selected := make([]TreeEntry, 0)
	for _, entry := range entries {
		if entry.Type != "blob" || entry.Mode == "120000" {
			continue
		}
		if exactKey != "" && entry.Path != exactKey {
			continue
		}
		if prefix != "" && !strings.HasPrefix(entry.Path, prefix) {
			continue
		}
		if !sourceObjectAllowed(entry.Path, config.Suffixes, config.Patterns, config.ExcludePatterns) {
			continue
		}

		if len(retrySourceKeys) > 0 {
			if _, ok := retryKeys[entry.Path]; !ok {
				continue
			}
		}
		selected = append(selected, entry)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Path < selected[j].Path })
	return selected
}

func githubSourceObject(sourceNamespace, repository, requestedRef, commitSHA string, entry TreeEntry, objectIndex int) Item {
	blobURL := "https://api.github.com/repos/" + repository + "/git/blobs/" + url.PathEscape(entry.SHA)
	canonicalURL := "https://github.com/" + repository + "/blob/" + url.PathEscape(commitSHA) + "/" + escapeSourcePath(entry.Path)
	idPrefix := "github://"
	if sourceNamespace = strings.Trim(sourceNamespace, "/"); sourceNamespace != "" {
		idPrefix += sourceNamespace + "/"
	}
	return Item{
		ID:          idPrefix + repository + "/" + entry.Path,
		URL:         blobURL,
		Path:        entry.Path,
		Filename:    filepath.Base(entry.Path),
		Revision:    entry.SHA,
		Size:        entry.Size,
		SourceKind:  ProviderGitHub,
		ObjectIndex: objectIndex,
		Metadata: map[string]any{
			"source_namespace": sourceNamespace,
			"repository":       repository,
			"requested_ref":    requestedRef,
			"commit_sha":       commitSHA,
			"blob_sha":         entry.SHA,
			"path":             entry.Path,
			"size":             entry.Size,
			"remote_url":       blobURL,
			"canonical_url":    canonicalURL,
			"object_index":     objectIndex,
		},
	}
}

func findInstallationRepository(repositories []Repository, requested string) (Repository, bool) {
	owner, repo, err := ParseRepository(requested)
	if err != nil {
		return Repository{}, false
	}
	fullName := owner + "/" + repo
	for _, repository := range repositories {
		if strings.EqualFold(repository.FullName, fullName) {
			return repository, true
		}
	}
	return Repository{}, false
}

func normalizeGitHubSourcePath(path string, allowEmpty bool) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" && allowEmpty {
		return "", nil
	}
	if path == "" || strings.HasPrefix(path, "/") || strings.Contains(path, "\\") {
		return "", fmt.Errorf("%w: invalid GitHub source path", ErrInvalidInput)
	}
	path = strings.TrimSuffix(path, "/")
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("%w: invalid GitHub source path", ErrInvalidInput)
		}
	}
	return path, nil
}

type Repository struct{ FullName string }
type GitHubAccess struct {
	Token        string
	Repositories []Repository
}
type GitHubContentClient interface {
	ResolveCommit(context.Context, string, string, string) (string, error)
	ListTree(context.Context, string, string, string) ([]TreeEntry, error)
	GetBlob(context.Context, string, string, string, int64) ([]byte, error)
}
type GitHubConnector struct {
	Client     GitHubContentClient
	Access     func(context.Context) (GitHubAccess, error)
	BatchBytes int64
}

func (c *GitHubConnector) access(ctx context.Context) (GitHubAccess, error) {
	if c.Access == nil {
		return GitHubAccess{}, fmt.Errorf("GitHub credential resolver required")
	}
	access, err := c.Access(ctx)
	if err != nil {
		return access, err
	}
	if access.Repositories == nil {
		return access, nil
	}
	return access, nil
}
func (c *GitHubConnector) byteLimit() int64 {
	if c.BatchBytes > 0 {
		return c.BatchBytes
	}
	return maxImportSourceObjectBytes
}
func (c *GitHubConnector) Read(ctx context.Context, r Request, o Item) ([]byte, error) {
	inputs, err := c.ReadBatch(ctx, r, []Item{o})
	if err != nil {
		return nil, err
	}
	return inputs[0].Bytes, inputs[0].ReadError
}
func (c *GitHubConnector) CheckAccess(ctx context.Context, r Request, o Item) error {
	_, err := c.Read(ctx, r, o)
	return err
}

type codeSelection struct {
	Repository, Ref, Key, Prefix        string
	Suffixes, Patterns, ExcludePatterns []string
}
