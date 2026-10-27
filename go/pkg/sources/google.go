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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

type GoogleConfig struct {
	Bucket          string   `json:"bucket"`
	Prefix          string   `json:"prefix,omitempty"`
	Key             string   `json:"key,omitempty"`
	FolderID        string   `json:"folder_id,omitempty"`
	SharedDriveID   string   `json:"shared_drive_id,omitempty"`
	Suffixes        []string `json:"suffixes,omitempty"`
	Patterns        []string `json:"patterns,omitempty"`
	ExcludePatterns []string `json:"exclude_patterns,omitempty"`
	MaxObjects      int      `json:"max_objects,omitempty"`
}

// GoogleConnector receives an already authorized HTTP client. Credential storage
// and renewal policy belong to its caller, not to acquisition.
type GoogleConnector struct {
	ProviderName string
	HTTPClient   *http.Client
	APIBase      string
}

func (c *GoogleConnector) Provider() string { return c.ProviderName }
func (c *GoogleConnector) client(context.Context, Request) (*http.Client, error) {
	if c.HTTPClient == nil {
		return nil, fmt.Errorf("Google HTTP client required")
	}
	return c.HTTPClient, nil
}
func (c *GoogleConnector) base() string {
	if c.APIBase != "" {
		return c.APIBase
	}
	if c.ProviderName == ProviderGCS {
		return "https://storage.googleapis.com/storage/v1"
	}
	return "https://www.googleapis.com/drive/v3"
}
func (c *GoogleConnector) Validate(_ context.Context, r Request) error {
	config, err := decodeSourceConfig[GoogleConfig](r.Config)
	if err != nil {
		return err
	}
	if r.Mode != ImportModeSource {
		return fmt.Errorf("%w: Google sources require source mode and a matching connection", ErrInvalidInput)
	}
	if config.MaxObjects < 0 || config.MaxObjects > maxImportSourceObjects {
		return fmt.Errorf("%w: invalid max_objects", ErrInvalidInput)
	}
	if c.ProviderName == ProviderGCS {
		if strings.TrimSpace(config.Bucket) == "" || strings.ContainsAny(config.Bucket, "/\\\x00") {
			return fmt.Errorf("%w: allowed bucket is required", ErrInvalidInput)
		}
	} else if !googleFileID(config.FolderID) {
		return fmt.Errorf("%w: folder_id is required", ErrInvalidInput)
	}
	if config.SharedDriveID != "" && !googleFileID(config.SharedDriveID) {
		return fmt.Errorf("%w: invalid shared_drive_id", ErrInvalidInput)
	}
	return nil
}
func googleFileID(s string) bool {
	if s == "" || len(s) > 512 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func (c *GoogleConnector) get(ctx context.Context, client *http.Client, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base()+path, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return googleRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return googleResponseError(resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if err != nil {
		return err
	}
	if len(body) > 8<<20 {
		return fmt.Errorf("%w: Google metadata response too large", ErrInvalidInput)
	}
	return json.Unmarshal(body, out)
}
func (c *GoogleConnector) Enumerate(ctx context.Context, r Request) (Inventory, error) {
	if err := c.Validate(ctx, r); err != nil {
		return Inventory{}, err
	}
	client, err := c.client(ctx, r)
	if err != nil {
		return Inventory{}, err
	}
	config, _ := decodeSourceConfig[GoogleConfig](r.Config)
	snapshot := Inventory{Authoritative: !r.Preview && len(r.RetrySourceKeys) == 0 && config.MaxObjects == 0, BatchObjects: 1, BatchBytes: 32 << 20}
	limit := maxImportSourceObjects
	if config.MaxObjects > 0 {
		limit = config.MaxObjects
	}
	if r.Preview {
		limit = 1
	}
	requested := exactSourceKeySet(r.RetrySourceKeys)
	retry := exactSourceKeySet(r.RetrySourceKeys)
	legacyDrivePaths := map[string]int{}
	add := func(o Item) (bool, error) {
		if !sourceObjectAllowed(o.Path, config.Suffixes, config.Patterns, config.ExcludePatterns) {
			return false, nil
		}
		if len(requested) > 0 {
			key := sourceObjectRetryKey(o)
			_, selected := requested[key]
			if c.ProviderName == ProviderGoogleDrive {
				// Historical runs used display paths. Accept a legacy path only
				// after the complete inventory establishes that it is unambiguous.
				if _, legacy := requested[o.Path]; legacy {
					legacyDrivePaths[o.Path]++
					if legacyDrivePaths[o.Path] > 1 {
						return false, fmt.Errorf("%w: ambiguous legacy Drive retry path; start a full sync to establish stable file identities", ErrInvalidInput)
					}
					delete(retry, o.Path)
					selected = true
				}
			}
			if !selected {
				return false, nil
			}
			delete(retry, key)
		}
		if len(snapshot.Objects) >= limit {
			return true, fmt.Errorf("%w: source inventory exceeds %d items", ErrInvalidInput, limit)
		}
		o.ObjectIndex = len(snapshot.Objects) + 1
		snapshot.Objects = append(snapshot.Objects, o)
		return (r.Preview || (config.MaxObjects > 0 && len(requested) == 0)) && len(snapshot.Objects) >= limit, nil
	}
	if c.ProviderName == ProviderGCS {
		err = c.enumerateGCS(ctx, client, r, config, add)
	} else {
		err = c.enumerateDrive(ctx, client, r, config, add)
	}
	if err != nil {
		return Inventory{}, err
	}
	if len(retry) > 0 {
		return Inventory{}, fmt.Errorf("%w: selected retry sources are no longer accessible", ErrInvalidInput)
	}
	return snapshot, nil
}
func (c *GoogleConnector) enumerateGCS(ctx context.Context, client *http.Client, r Request, config GoogleConfig, add func(Item) (bool, error)) error {
	type object struct {
		Name        string `json:"name"`
		Generation  string `json:"generation"`
		Size        string `json:"size"`
		ContentType string `json:"contentType"`
	}
	token := ""
	seen := map[string]bool{}
	for pages := 0; pages < maxImportSourceObjects; pages++ {
		q := url.Values{"prefix": {config.Prefix}, "maxResults": {"1000"}, "fields": {"nextPageToken,items(name,generation,size,contentType)"}}
		if config.Key != "" {
			q.Set("prefix", config.Key)
		}
		if token != "" {
			q.Set("pageToken", token)
		}
		var result struct {
			Items []object `json:"items"`
			Next  string   `json:"nextPageToken"`
		}
		if err := c.get(ctx, client, "/b/"+url.PathEscape(config.Bucket)+"/o?"+q.Encode(), &result); err != nil {
			return err
		}
		for _, item := range result.Items {
			if config.Key != "" && item.Name != config.Key {
				continue
			}
			size, err := strconv.ParseInt(item.Size, 10, 64)
			if err != nil || size < 0 {
				return fmt.Errorf("invalid GCS object size")
			}
			if _, err := strconv.ParseInt(item.Generation, 10, 64); err != nil {
				return fmt.Errorf("invalid GCS generation")
			}
			canonical := "gs://" + config.Bucket + "/" + escapeSourcePath(item.Name)
			o := Item{ID: "gcs://" + r.SourceNamespace + "/" + config.Bucket + "/" + item.Name, Path: item.Name, Filename: filepath.Base(item.Name), URL: canonical, Revision: item.Generation, Size: size, ReadMode: sourceReadBytes, SourceKind: c.ProviderName, Metadata: map[string]any{"bucket": config.Bucket, "key": item.Name, "generation": item.Generation, "mime_type": item.ContentType, "canonical_url": canonical}}
			stop, err := add(o)
			if err != nil || stop {
				return err
			}
		}
		if result.Next == "" {
			return nil
		}
		if seen[result.Next] {
			return fmt.Errorf("Google source repeated page token")
		}
		seen[result.Next] = true
		token = result.Next
	}
	return fmt.Errorf("Google source pagination budget exhausted")
}

type driveSourceFile struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MIME     string `json:"mimeType"`
	Version  string `json:"version"`
	Size     string `json:"size"`
	URL      string `json:"webViewLink"`
	Modified string `json:"modifiedTime"`
	MD5      string `json:"md5Checksum"`
}

const driveSourceFields = "id,name,mimeType,version,size,webViewLink,modifiedTime,md5Checksum"

func (c *GoogleConnector) enumerateDrive(ctx context.Context, client *http.Client, r Request, config GoogleConfig, add func(Item) (bool, error)) error {
	type folder struct{ id, path string }
	queue := []folder{{config.FolderID, ""}}
	visited := map[string]bool{}
	requests := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visited[current.id] {
			continue
		}
		visited[current.id] = true
		var parent driveSourceFile
		if err := c.get(ctx, client, "/files/"+url.PathEscape(current.id)+"?fields="+url.QueryEscape(driveSourceFields)+"&supportsAllDrives=true", &parent); err != nil {
			return err
		}
		if parent.MIME != "application/vnd.google-apps.folder" {
			return fmt.Errorf("%w: selected Drive parent is not a folder", ErrInvalidInput)
		}
		token := ""
		seen := map[string]bool{}
		for {
			requests++
			if requests > maxImportSourceObjects {
				return fmt.Errorf("Drive enumeration budget exhausted")
			}
			q := url.Values{"q": {"'" + current.id + "' in parents and trashed = false"}, "pageSize": {"1000"}, "fields": {"nextPageToken,incompleteSearch,files(" + driveSourceFields + ")"}, "supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"}}
			if config.SharedDriveID != "" {
				q.Set("corpora", "drive")
				q.Set("driveId", config.SharedDriveID)
			}
			if token != "" {
				q.Set("pageToken", token)
			}
			var result struct {
				Files      []driveSourceFile `json:"files"`
				Next       string            `json:"nextPageToken"`
				Incomplete bool              `json:"incompleteSearch"`
			}
			if err := c.get(ctx, client, "/files?"+q.Encode(), &result); err != nil {
				return err
			}
			if result.Incomplete {
				return fmt.Errorf("Drive returned an incomplete inventory")
			}
			for _, item := range result.Files {
				if !googleFileID(item.ID) || strings.ContainsRune(item.Name, 0) {
					return fmt.Errorf("Drive returned invalid file identity/path")
				}
				path := current.path + DrivePathSegment(item.Name)
				if item.MIME == "application/vnd.google-apps.folder" {
					queue = append(queue, folder{item.ID, path + "/"})
					if len(queue)+len(visited) > maxImportSourceObjects {
						return fmt.Errorf("Drive folder budget exhausted")
					}
					continue
				}
				export := googleWorkspaceTextExport(item.MIME)
				size := int64(maxImportSourceObjectBytes)
				if item.Size != "" && export == "" {
					var err error
					size, err = strconv.ParseInt(item.Size, 10, 64)
					if err != nil || size < 0 {
						return fmt.Errorf("invalid Drive file size")
					}
				}
				canonical := item.URL
				if canonical == "" {
					canonical = "https://drive.google.com/file/d/" + item.ID + "/view"
				}
				o := Item{ID: "drive://" + r.SourceNamespace + "/" + item.ID, Path: path, Filename: item.Name, URL: canonical, Revision: item.Version, Size: size, SourceKind: c.ProviderName, ReadMode: sourceReadBytes, Metadata: map[string]any{"file_id": item.ID, "mime_type": item.MIME, "export_mime_type": export, "canonical_url": canonical, "modified_at": item.Modified, "md5_checksum": item.MD5, "shared_drive_id": config.SharedDriveID, "size_is_upper_bound": export != "" || item.Size == ""}}
				stop, err := add(o)
				if err != nil || stop {
					return err
				}
			}
			if result.Next == "" {
				break
			}
			if seen[result.Next] {
				return fmt.Errorf("Drive repeated page token")
			}
			seen[result.Next] = true
			token = result.Next
		}
	}
	return nil
}
func (c *GoogleConnector) read(ctx context.Context, client *http.Client, r Request, o Item) ([]byte, error) {
	return c.readWithin(ctx, client, r, o, maxImportSourceObjectBytes)
}
func (c *GoogleConnector) readWithin(ctx context.Context, client *http.Client, r Request, o Item, limit int) ([]byte, error) {
	if limit < 0 || limit > maxImportSourceObjectBytes {
		return nil, ErrInvalidInput
	}
	config, _ := decodeSourceConfig[GoogleConfig](r.Config)
	if configString(o.Metadata, "mime_type") == "application/vnd.google-apps.spreadsheet" {
		return nil, fmt.Errorf("%w: whole-workbook extraction is not supported; first-sheet CSV must not be presented as a complete workbook", ErrInvalidInput)
	}
	var path string
	if c.ProviderName == ProviderGCS {
		if configString(o.Metadata, "bucket") != config.Bucket {
			return nil, fmt.Errorf("GCS source is outside configured bucket")
		}
		path = "/b/" + url.PathEscape(config.Bucket) + "/o/" + url.PathEscape(o.Path) + "?alt=media&generation=" + url.QueryEscape(o.Revision) + "&ifGenerationMatch=" + url.QueryEscape(o.Revision)
	} else {
		id := configString(o.Metadata, "file_id")
		if !googleFileID(id) {
			return nil, fmt.Errorf("invalid Drive identity")
		}
		if err := c.checkDriveRevision(ctx, client, id, o.Revision); err != nil {
			return nil, err
		}
		path = "/files/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
		if mime := configString(o.Metadata, "export_mime_type"); mime != "" {
			path = "/files/" + url.PathEscape(id) + "/export?mimeType=" + url.QueryEscape(mime)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base()+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, googleRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, googleResponseError(resp)
	}
	if c.ProviderName == ProviderGCS && resp.Header.Get("X-Goog-Generation") != o.Revision {
		return nil, &RevisionChangedError{Expected: o.Revision, Actual: resp.Header.Get("X-Goog-Generation")}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("%w: source exceeds byte limit", ErrInvalidInput)
	}
	if c.ProviderName == ProviderGCS && int64(len(data)) != o.Size {
		return nil, fmt.Errorf("GCS source length changed")
	}
	if c.ProviderName == ProviderGoogleDrive {
		if upper, _ := o.Metadata["size_is_upper_bound"].(bool); !upper && int64(len(data)) != o.Size {
			return nil, fmt.Errorf("Drive source length changed")
		}
		if expected := configString(o.Metadata, "md5_checksum"); expected != "" && configString(o.Metadata, "export_mime_type") == "" {
			sum := md5.Sum(data)
			if hex.EncodeToString(sum[:]) != expected {
				return nil, fmt.Errorf("Drive source checksum mismatch")
			}
		}
		if err := c.checkDriveRevision(ctx, client, configString(o.Metadata, "file_id"), o.Revision); err != nil {
			return nil, err
		}
	}
	return data, nil
}
func (c *GoogleConnector) checkDriveRevision(ctx context.Context, client *http.Client, id, revision string) error {
	var f driveSourceFile
	if err := c.get(ctx, client, "/files/"+url.PathEscape(id)+"?fields="+url.QueryEscape(driveSourceFields)+"&supportsAllDrives=true", &f); err != nil {
		return err
	}
	if revision == "" || f.Version != revision {
		return &RevisionChangedError{Expected: revision, Actual: f.Version}
	}
	return nil
}
func (c *GoogleConnector) ReadBatch(ctx context.Context, r Request, objects []Item) ([]Input, error) {
	if len(objects) > 1 {
		return nil, fmt.Errorf("Google sources read one object per batch")
	}
	client, err := c.client(ctx, r)
	if err != nil {
		return nil, err
	}
	inputs := make([]Input, 0, len(objects))
	for _, o := range objects {
		input := inputFromObject(o)
		input.Bytes, input.ReadError = c.read(ctx, client, r, o)
		inputs = append(inputs, input)
	}
	return inputs, nil
}
func (c *GoogleConnector) ValidateCachedSnapshot(ctx context.Context, r Request, _ Inventory) error {
	client, err := c.client(ctx, r)
	if err != nil {
		return err
	}
	config, _ := decodeSourceConfig[GoogleConfig](r.Config)
	var response json.RawMessage
	if c.ProviderName == ProviderGCS {
		return c.get(ctx, client, "/b/"+url.PathEscape(config.Bucket)+"/o?maxResults=1&prefix="+url.QueryEscape(config.Prefix), &response)
	}
	pinned, _, err := driveContentClient(client)
	if err != nil {
		return err
	}
	return c.get(ctx, pinned, "/files/"+url.PathEscape(config.FolderID)+"?fields=id&supportsAllDrives=true", &response)
}
func (c *GoogleConnector) OpenSource(ctx context.Context, r Request, o Item) (io.ReadCloser, error) {
	client, err := c.client(ctx, r)
	if err != nil {
		return nil, err
	}
	data, err := c.read(ctx, client, r, o)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (c *GoogleConnector) ValidateStagedSource(ctx context.Context, r Request, o Item) error {
	client, err := c.client(ctx, r)
	if err != nil {
		return err
	}
	if c.ProviderName == ProviderGoogleDrive {
		if err := c.checkDriveContentAccess(ctx, client, configString(o.Metadata, "file_id"), ""); err != nil {
			return fmt.Errorf("Drive content access cannot be established for cached export: %w", err)
		}
		return nil
	}
	config, err := decodeSourceConfig[GoogleConfig](r.Config)
	if err != nil {
		return err
	}
	if o.Revision == "" || configString(o.Metadata, "bucket") != config.Bucket {
		return fmt.Errorf("invalid pinned GCS source")
	}
	if _, err := strconv.ParseUint(o.Revision, 10, 64); err != nil {
		return fmt.Errorf("invalid pinned GCS generation")
	}
	var object struct {
		Generation string `json:"generation"`
	}
	path := "/b/" + url.PathEscape(config.Bucket) + "/o/" + url.PathEscape(o.Path) + "?generation=" + url.QueryEscape(o.Revision) + "&ifGenerationMatch=" + url.QueryEscape(o.Revision)
	if err := c.get(ctx, client, path, &object); err != nil {
		return err
	}
	if object.Generation != o.Revision {
		return &RevisionChangedError{Expected: o.Revision, Actual: object.Generation}
	}
	return nil
}
func (*GoogleConnector) RuntimeAccess(context.Context, Request) (string, error) {
	return "", nil
}
func (*GoogleConnector) IsRetryable(err error) bool {
	var e *GoogleHTTPError
	if errors.As(err, &e) {
		return e.Retryable()
	}
	return false
}

// Drive names are metadata, not local filesystem components. Encode literal
// slashes and percent signs so a file named "Design/2026" remains distinct from
// a file "2026" in a folder named "Design". Stable provider IDs remain identity.
func DrivePathSegment(name string) string {
	return strings.NewReplacer("%", "%25", "/", "%2F").Replace(name)
}

func googleWorkspaceTextExport(mime string) string {
	switch mime {
	case "application/vnd.google-apps.document", "application/vnd.google-apps.presentation":
		return "text/plain"
	default:
		return ""
	}
}

// Enumeration establishes this provider's authoritative selection; no
// additional identity checks are required before retiring omitted records.
func (*GoogleConnector) ValidateAbsentSources(context.Context, Request, []string) error {
	return nil
}

func (c *GoogleConnector) Read(ctx context.Context, r Request, o Item) ([]byte, error) {
	client, err := c.client(ctx, r)
	if err != nil {
		return nil, err
	}
	return c.read(ctx, client, r, o)
}
func (c *GoogleConnector) CheckAccess(ctx context.Context, r Request, o Item) error {
	return c.ValidateStagedSource(ctx, r, o)
}

// Resource capabilities express user permissions; OAuth scopes separately limit
// the app. drive.file additionally requires this file to be authorized to the app.
func (c *GoogleConnector) checkDriveContentAccess(ctx context.Context, client *http.Client, id, revision string) error {
	if !googleFileID(id) {
		return ErrInvalidInput
	}
	pinned, grant, err := driveContentClient(client)
	if err != nil {
		return err
	}
	var file struct {
		ID            string `json:"id"`
		Version       string `json:"version"`
		AppAuthorized bool   `json:"isAppAuthorized"`
		Capabilities  struct {
			CanDownload bool `json:"canDownload"`
		} `json:"capabilities"`
	}
	if err := c.get(ctx, pinned, "/files/"+url.PathEscape(id)+"?fields=id,version,isAppAuthorized,capabilities(canDownload)&supportsAllDrives=true", &file); err != nil {
		return err
	}
	if file.ID != id || !file.Capabilities.CanDownload {
		return fmt.Errorf("%w: Drive content download access revoked", ErrInvalidInput)
	}
	if !grant.allFiles && !file.AppAuthorized {
		return &GoogleHTTPError{Status: 403, Reason: "file_not_authorized"}
	}
	if revision != "" && file.Version != revision {
		return &RevisionChangedError{Expected: revision, Actual: file.Version}
	}
	return nil
}
