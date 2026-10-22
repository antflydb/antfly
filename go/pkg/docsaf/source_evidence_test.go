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

package docsaf

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func evidenceFixture() (SourceDocument, SourceEvidenceOptions) {
	d := SourceDocument{ID: "drive/file-id", SourceID: "file-id", SourceURI: "https://drive.google.com/file/d/file-id/view", SourcePath: "Reports/Quarterly.pdf", SHA256: strings.Repeat("a", 64), Version: "7"}
	o := SourceEvidenceOptions{IndexedAt: time.Unix(123, 0), Access: &SourceAccess{Readers: []string{"user:alice"}}, Units: []EvidenceUnit{{ID: "page-1", Kind: "page", Location: EvidenceLocation{Page: 1}, Asset: EvidenceAsset{Field: "payloads.page1", MIMEType: "image/png", SHA256: strings.Repeat("b", 64), Recipe: "render-150dpi-v1", ByteLength: 68}}}}
	return d, o
}
func TestEvidenceRecordContractAndOwnership(t *testing.T) {
	d, o := evidenceFixture()
	record, err := d.ToEvidenceRecord(o)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	source := decoded["source"].(map[string]any)
	if source["version"] != float64(2) || source["content"] != "" || source["revision"] != "7:"+d.SHA256 || source["uri"] != d.SourceURI {
		t.Fatalf("invalid contract: %s", b)
	}
	o.Access.Readers[0] = "user:bob"
	o.Units[0].ID = "changed"
	if source["permissions"].(map[string]any)["public"] != false || record["source"].(map[string]any)["permissions"].(SourceAccess).Readers[0] != "user:alice" || record["source"].(map[string]any)["evidence"].([]EvidenceUnit)[0].ID != "page-1" {
		t.Fatal("record aliases caller state")
	}
}
func TestEvidenceRejectsIncompleteUnsafeAndAmbiguousRecords(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SourceDocument, *SourceEvidenceOptions)
	}{
		{"missing access", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Access = nil }},
		{"timestamp", func(d *SourceDocument, o *SourceEvidenceOptions) { o.IndexedAt = time.Time{} }},
		{"inline identity", func(d *SourceDocument, o *SourceEvidenceOptions) { o.SourceURI = "data:image/png;base64,AA==" }},
		{"credentials", func(d *SourceDocument, o *SourceEvidenceOptions) {
			o.SourceURI = "https://secret:token@example.com/file"
		}},
		{"external payload", func(d *SourceDocument, o *SourceEvidenceOptions) {
			o.Units[0].Asset.Field = "https://example.com/image"
		}},
		{"reserved payload", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Units[0].Asset.Field = "_private.image" }},
		{"digest", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Units[0].Asset.SHA256 = strings.Repeat("B", 64) }},
		{"duplicate", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Units = append(o.Units, o.Units[0]) }},
		{"location", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Units[0].Location.Slide = 1 }},
		{"size", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Units[0].Asset.ByteLength = 0 }},
		{"field collision", func(d *SourceDocument, o *SourceEvidenceOptions) { o.Field = "url" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, o := evidenceFixture()
			tc.mutate(&d, &o)
			if _, err := d.ToEvidenceRecord(o); err == nil {
				t.Fatal("accepted invalid evidence")
			}
		})
	}
}
func TestDriveIdentitySurvivesRenameAndInlineMaterialization(t *testing.T) {
	item := ContentItem{Path: "Reports/old.pdf", SourceURL: "https://drive.google.com/file/d/drive-id/view", Content: []byte("exported"), ContentType: "application/pdf", Metadata: map[string]any{"drive_file_id": "drive-id", "version": "8", "source_type": "google_drive"}}
	opts := SourceDocumentOptions{InlineContent: true, PreferProviderID: true, IDPrefix: "org/source"}
	first, err := SourceDocumentFromContentItem(item, opts)
	if err != nil {
		t.Fatal(err)
	}
	item.Path = "Archive/new.pdf"
	renamed, err := SourceDocumentFromContentItem(item, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "org/source/drive-id" || renamed.ID != first.ID || renamed.SourcePath == first.SourcePath || first.SourceURI != item.SourceURL || first.SourceID != "drive-id" || !strings.HasPrefix(first.URL, "data:") || first.Version != "8" {
		t.Fatalf("identity lost: %#v %#v", first, renamed)
	}
}
