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
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// Normalized content is private and content addressed. SQL/page checkpoints keep
// references and item metadata only; hosted runtimes use private object storage.
const MaxAcquisitionContentBytes = 64 << 20
const PreviewItemLimit = 9 // one parent plus at most eight attachment samples

type AcquisitionContentStore interface {
	Load(context.Context, string) ([]byte, bool, error)
	Save(context.Context, string, []byte) error
}

type FileAcquisitionContentStore struct{ Directory string }

func ValidContentRef(ref string) bool {
	if len(ref) != 64 {
		return false
	}
	_, err := hex.DecodeString(ref)
	return err == nil
}
func (s *FileAcquisitionContentStore) Load(_ context.Context, ref string) ([]byte, bool, error) {
	if !ValidContentRef(ref) {
		return nil, false, ErrInvalidInput
	}
	f, err := os.Open(filepath.Join(s.Directory, acquisitionKey(ref)+".json"))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxAcquisitionContentBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(b) > MaxAcquisitionContentBytes {
		return nil, false, ErrInvalidInput
	}
	return b, true, nil
}
func (s *FileAcquisitionContentStore) Save(ctx context.Context, ref string, data []byte) error {
	sum := sha256.Sum256(data)
	if !ValidContentRef(ref) || hex.EncodeToString(sum[:]) != ref || len(data) > MaxAcquisitionContentBytes {
		return ErrInvalidInput
	}
	return (&FileAcquisitionCache{Directory: s.Directory, maxBytes: MaxAcquisitionContentBytes}).Save(ctx, ref, data)
}
func (s *FileAcquisitionContentStore) Prune(items []Item) error {
	keep := map[string]bool{}
	for _, item := range items {
		keep[acquisitionKey(item.ContentRef)] = true
	}
	files, err := os.ReadDir(s.Directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, file := range files {
		name := file.Name()
		if !file.IsDir() && len(name) == 69 && name[64:] == ".json" && ValidContentRef(name[:64]) && !keep[name[:64]] {
			if err := os.Remove(filepath.Join(s.Directory, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

type hydrationEnvelope struct {
	Version                        int
	Namespace, Selection, Provider string
	Inputs                         []Input
}
type hydrationCheckpoint struct {
	Version   int
	Selection string
	Items     []Item
}

func (c *CollaborationConnector) hydrationSelection(r Request) string {
	version := c.ExtractorVersion
	if version == "" {
		version = "attachment-text-v1"
	}
	return acquisitionKey(c.ProviderName + "\x00" + workspaceSelectionKey(r) + "\x00" + version)
}
func (c *CollaborationConnector) hydrationKey(r Request, ref collaborationRef) string {
	ref.Attachment = ""
	b, _ := json.Marshal(ref)
	return "hydrated/" + c.hydrationSelection(r) + "/" + acquisitionKey(string(b))
}
func (c *CollaborationConnector) hydrate(ctx context.Context, r Request, config CollaborationConfig, ref collaborationRef, message *slackMessage) ([]Input, error) {
	key := c.hydrationKey(r, ref)
	if !r.Preview && c.ContentStore != nil {
		if items, ok := c.hydratedItems[key]; ok {
			return checkpointInputs(items), nil
		}
	}
	if !r.Preview && c.ContentStore != nil && c.Cache != nil {
		raw, found, err := c.Cache.Load(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			var checkpoint hydrationCheckpoint
			if json.Unmarshal(raw, &checkpoint) != nil || checkpoint.Version != 1 || checkpoint.Selection != c.hydrationSelection(r) || len(checkpoint.Items) == 0 || len(checkpoint.Items) > 1001 {
				return nil, ErrInvalidInput
			}
			out := make([]Input, 0, len(checkpoint.Items))
			for _, item := range checkpoint.Items {
				if !ownedID(item.ID, r.SourceNamespace) || !ValidContentRef(item.ContentRef) || item.Revision == "" || item.Size < 0 || item.Size > maxImportSourceObjectBytes {
					return nil, ErrInvalidInput
				}
				out = append(out, Input{Item: item})
			}
			c.rememberHydration(key, checkpoint.Items)
			return out, nil
		}
	}
	var m collaborationMaterial
	var err error
	if message != nil && message.hasBody {
		m, err = c.slackMaterial(ctx, config, ref, *message)
	} else {
		m, err = c.load(ctx, config, ref)
	}
	if err != nil {
		return nil, err
	}
	inputs, err := c.materialItems(ctx, r, config, ref, m)
	if err != nil {
		return nil, err
	}
	if r.Preview || c.ContentStore == nil {
		return inputs, nil
	}
	if c.Extractor != nil && c.ExtractorVersion == "" {
		return nil, fmt.Errorf("%w: cached custom extractors require a version", ErrInvalidInput)
	}
	payload, err := json.Marshal(hydrationEnvelope{Version: 1, Namespace: r.SourceNamespace, Selection: c.hydrationSelection(r), Provider: c.ProviderName, Inputs: inputs})
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxAcquisitionContentBytes {
		return nil, fmt.Errorf("normalized acquisition exceeds content budget")
	}
	sum := sha256.Sum256(payload)
	contentRef := hex.EncodeToString(sum[:])
	if err := c.ContentStore.Save(ctx, contentRef, payload); err != nil {
		return nil, err
	}
	checkpoint := hydrationCheckpoint{Version: 1, Selection: c.hydrationSelection(r)}
	for i := range inputs {
		inputs[i].ContentRef = contentRef
		checkpoint.Items = append(checkpoint.Items, inputs[i].Item)
	}
	c.rememberHydration(key, checkpoint.Items)
	if c.Cache != nil {
		metadata, _ := json.Marshal(checkpoint)
		if err := c.Cache.Save(ctx, key, metadata); err != nil {
			return nil, err
		}
	}
	return inputs, nil
}
func (c *CollaborationConnector) retainedInputs(ctx context.Context, r Request, ref string) ([]Input, bool, error) {
	if !ValidContentRef(ref) {
		return nil, false, ErrInvalidInput
	}
	scope := r.SourceNamespace + "/" + c.hydrationSelection(r)
	if c.retainedRef == ref && c.retainedScope == scope && c.retainedBundle != nil {
		return c.retainedBundle, true, nil
	}
	data, found, err := c.ContentStore.Load(ctx, ref)
	if err != nil || !found {
		return nil, found, err
	}
	sum := sha256.Sum256(data)
	if len(data) > MaxAcquisitionContentBytes || hex.EncodeToString(sum[:]) != ref {
		return nil, false, fmt.Errorf("acquisition content digest mismatch")
	}
	var envelope hydrationEnvelope
	if json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 || envelope.Namespace != r.SourceNamespace || envelope.Selection != c.hydrationSelection(r) || envelope.Provider != c.ProviderName || len(envelope.Inputs) == 0 || len(envelope.Inputs) > 1001 {
		return nil, false, ErrInvalidInput
	}
	var total int
	for _, input := range envelope.Inputs {
		total += len(input.Bytes)
		if !ownedID(input.ID, r.SourceNamespace) || input.SourceKind != c.ProviderName || input.ReadError != nil || input.Size != int64(len(input.Bytes)) || total > maxImportSourceObjectBytes {
			return nil, false, ErrInvalidInput
		}
	}
	c.retainedScope = scope
	c.retainedRef = ref
	c.retainedBundle = envelope.Inputs
	return envelope.Inputs, true, nil
}

// A chat message inherits its channel/thread permissions. Reusing a pinned
// normalized snapshot does not need another rate-limited history request. Calendar
// events and independently authorized Drive files retain revision/access checks.
func (c *CollaborationConnector) authorizeRetained(ctx context.Context, config CollaborationConfig, ref collaborationRef, item Item) error {
	key := refKey(ref.Resource, ref.Container, "")
	if c.scopeChecks == nil {
		c.scopeChecks = map[string]time.Time{}
	}
	if time.Since(c.scopeChecks[key]) > 30*time.Second {
		if err := c.validateRef(ctx, config, ref); err != nil {
			return err
		}
		c.scopeChecks[key] = time.Now()
	}
	if c.observedScopes == nil {
		c.observedScopes = map[string]collaborationRef{}
	}
	c.observedScopes[key] = ref
	if ref.Resource == "calendar" {
		eventKey := refKey(ref.Resource, ref.Container, ref.ID) + configString(item.Metadata, "etag")
		if time.Since(c.scopeChecks[eventKey]) > 30*time.Second {
			var event calendarEvent
			if err := c.get(ctx, "calendar", "/calendars/"+url.PathEscape(ref.Container)+"/events/"+url.PathEscape(ref.ID)+"?fields=id,etag,status,recurringEventId", &event); err != nil {
				return err
			}
			if event.ID != ref.ID || event.ETag != configString(item.Metadata, "etag") {
				return &RevisionChangedError{Expected: configString(item.Metadata, "etag"), Actual: event.ETag}
			}
			c.scopeChecks[eventKey] = time.Now()
		}
		if ref.Attachment != "" {
			if err := c.drive().checkDriveRevision(ctx, c.HTTPClient, ref.Attachment, configString(item.Metadata, "drive_revision")); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkpointInputs(items []Item) []Input {
	inputs := make([]Input, len(items))
	for i, item := range items {
		inputs[i] = Input{Item: item}
	}
	return inputs
}
func (c *CollaborationConnector) rememberHydration(key string, items []Item) {
	if c.hydratedItems == nil {
		c.hydratedItems = make(map[string][]Item)
	}
	c.hydratedItems[key] = items
}
