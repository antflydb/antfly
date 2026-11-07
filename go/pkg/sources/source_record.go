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
	"encoding/json"
	"fmt"
)

const SourceRecordVersion = 3

type SourceIdentity struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
	ItemID   string `json:"item_id"`
	// RelativeKey is this row's key relative to its source's own root (see
	// Item.RelativeKey). Stored alongside ItemID so a row's identity can be
	// recomputed and verified under IdentityKey without a second, separate
	// copy of the value living only in free-form Metadata.
	RelativeKey string `json:"relative_key,omitempty"`
	URI         string `json:"uri"`
	Path        string `json:"path"`
	Revision    string `json:"revision"`
	IndexedAt   int64  `json:"indexed_at"`
}

type AcquiredContent struct {
	MIMEType string          `json:"mime_type"`
	Text     *string         `json:"text,omitempty"`
	Resource *MediaReference `json:"resource,omitempty"`
}

type sourceRecord struct {
	SchemaVersion int             `json:"schema_version"`
	Source        SourceIdentity  `json:"source"`
	Content       AcquiredContent `json:"content"`
	Permissions   Permissions     `json:"permissions"`
	Metadata      map[string]any  `json:"metadata"`
}

// Document is the in-process acquisition value. New wire records are neutral
// source/content envelopes. Flat values are decoded only for state migration.
func (d Document) MarshalJSON() ([]byte, error) {
	if d.Version < SourceRecordVersion {
		type flat Document
		return json.Marshal(flat(d))
	}
	content := AcquiredContent{MIMEType: d.MIMEType, Resource: d.Media}
	if d.Media != nil {
		content.MIMEType = d.Media.MIMEType
	} else {
		content.Text = &d.Content
		if content.MIMEType == "" {
			content.MIMEType = "text/plain"
		}
	}
	return json.Marshal(sourceRecord{SchemaVersion: SourceRecordVersion,
		Source:  SourceIdentity{ID: configString(d.Metadata, "source_id"), Provider: configString(d.Metadata, "provider"), ItemID: configString(d.Metadata, "identity"), RelativeKey: configString(d.Metadata, "relative_key"), URI: d.URI, Path: d.Path, Revision: d.Revision, IndexedAt: d.IndexedAt},
		Content: content, Permissions: d.Permissions, Metadata: d.Metadata})
}

func (d *Document) UnmarshalJSON(raw []byte) error {
	var header struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return err
	}
	if header.SchemaVersion == 0 {
		type flat Document
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		return decoder.Decode((*flat)(d))
	}
	if header.SchemaVersion != SourceRecordVersion {
		return fmt.Errorf("unsupported source record version %d", header.SchemaVersion)
	}
	var record sourceRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&record); err != nil {
		return err
	}
	if record.Content.Text != nil && record.Content.Resource != nil {
		return fmt.Errorf("source content cannot contain both text and a resource")
	}
	kind, text := "text", ""
	if record.Content.Resource != nil {
		kind = "media"
	} else if record.Content.Text != nil {
		text = *record.Content.Text
	}
	metadata := record.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	for key, value := range map[string]string{"source_id": record.Source.ID, "provider": record.Source.Provider, "identity": record.Source.ItemID, "relative_key": record.Source.RelativeKey} {
		if existing := configString(metadata, key); existing != "" && existing != value {
			return fmt.Errorf("source identity differs from metadata")
		}
		metadata[key] = value
	}
	*d = Document{Version: SourceRecordVersion, Kind: kind, MIMEType: record.Content.MIMEType, URI: record.Source.URI, Path: record.Source.Path, Revision: record.Source.Revision, IndexedAt: record.Source.IndexedAt, Content: text, Media: record.Content.Resource, Permissions: record.Permissions, Metadata: metadata}
	return nil
}

// SourceRow projects document into its stored row, after verifying that id is
// exactly the key IdentityKey computes for it under namespace (the owning
// source's identity.namespace, or "" for a source that has not set one, in
// which case this is exactly the legacy RecordKey check).
func SourceRow(id, sourceID, namespace string, document Document) (map[string]any, error) {
	if document.Version != SourceRecordVersion {
		return nil, fmt.Errorf("source publication requires neutral record v%d", SourceRecordVersion)
	}
	if _, present := document.Metadata["relative_key"].(string); namespace != "" && !present {
		return nil, fmt.Errorf("record is missing its relative key")
	}
	key, err := IdentityKey(sourceID, namespace, configString(document.Metadata, "relative_key"), configString(document.Metadata, "identity"))
	if err != nil || key != id {
		return nil, fmt.Errorf("record identity differs from source ownership")
	}
	if configString(document.Metadata, "source_id") != sourceID {
		return nil, fmt.Errorf("source identity differs from row ownership")
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	var row map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&row); err != nil {
		return nil, err
	}
	row["id"], row["source_id"], row["doc_type"] = id, sourceID, "source_item"
	return row, nil
}

func (h *syncHit) UnmarshalJSON(raw []byte) error {
	var hit struct {
		ID     string          `json:"_id"`
		Source json.RawMessage `json:"_source"`
	}
	if err := json.Unmarshal(raw, &hit); err != nil {
		return err
	}
	h.ID = hit.ID
	if len(hit.Source) == 0 {
		return nil
	}
	var identity struct {
		ID       string          `json:"id"`
		SourceID string          `json:"source_id"`
		DocType  string          `json:"doc_type"`
		Grep     json.RawMessage `json:"grep"`
	}
	if err := json.Unmarshal(hit.Source, &identity); err != nil {
		return err
	}
	h.Source.ID, h.Source.SourceID, h.Source.DocType = identity.ID, identity.SourceID, identity.DocType
	value := hit.Source
	if len(identity.Grep) > 0 {
		value = identity.Grep
	}
	return json.Unmarshal(value, &h.Source.Document)
}

// DocumentFromRecord decodes the neutral acquisition envelope from a table row.
// It is shared by hosted and standalone publication/readiness adapters.
func DocumentFromRecord(record any) (Document, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return Document{}, err
	}
	var document Document
	if err = json.Unmarshal(raw, &document); err != nil {
		return Document{}, err
	}
	if document.Version != SourceRecordVersion {
		return Document{}, fmt.Errorf("expected neutral source record v%d", SourceRecordVersion)
	}
	return document, nil
}
