// Copyright 2025 Antfly, Inc.
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

package termite

import (
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
)

func TestValidateContentTypes(t *testing.T) {
	caps := embeddings.EmbedderCapabilities{
		SupportedMIMETypes: []embeddings.MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/jpeg"},
			{MIMEType: "image/png"},
			{MIMEType: "image/*"},
		},
	}

	tests := []struct {
		name     string
		contents [][]ai.ContentPart
		wantErr  bool
	}{
		{
			name: "text/plain accepted",
			contents: [][]ai.ContentPart{
				{ai.TextContent{Text: "hello"}},
			},
		},
		{
			name: "image/jpeg accepted (exact)",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "image/jpeg", Data: []byte{0xFF}}},
			},
		},
		{
			name: "image/png accepted (exact)",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "image/png", Data: []byte{0x89}}},
			},
		},
		{
			name: "image/gif accepted via wildcard",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "image/gif", Data: []byte{0x47}}},
			},
		},
		{
			name: "image/webp accepted via wildcard",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "image/webp", Data: []byte{0x52}}},
			},
		},
		{
			name: "image/bmp accepted via wildcard",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "image/bmp", Data: []byte{0x42}}},
			},
		},
		{
			name: "audio/wav rejected (no audio wildcard)",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "audio/wav", Data: []byte{0x52}}},
			},
			wantErr: true,
		},
		{
			name: "application/pdf rejected",
			contents: [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: "application/pdf", Data: []byte{0x25}}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateContentTypes(tt.contents, caps)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateContentTypes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateContentTypes_AudioWildcard(t *testing.T) {
	caps := embeddings.EmbedderCapabilities{
		SupportedMIMETypes: []embeddings.MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "audio/wav"},
			{MIMEType: "audio/*"},
		},
	}

	tests := []struct {
		name     string
		mimeType string
		wantErr  bool
	}{
		{"audio/wav exact", "audio/wav", false},
		{"audio/mp3 via wildcard", "audio/mp3", false},
		{"audio/ogg via wildcard", "audio/ogg", false},
		{"image/png rejected", "image/png", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contents := [][]ai.ContentPart{
				{ai.BinaryContent{MIMEType: tt.mimeType, Data: []byte{0x00}}},
			}
			err := validateContentTypes(contents, caps)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateContentTypes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateContentTypes_NoWildcard(t *testing.T) {
	// Without wildcard, only exact matches work
	caps := embeddings.EmbedderCapabilities{
		SupportedMIMETypes: []embeddings.MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/jpeg"},
			{MIMEType: "image/png"},
		},
	}

	contents := [][]ai.ContentPart{
		{ai.BinaryContent{MIMEType: "image/gif", Data: []byte{0x47}}},
	}
	err := validateContentTypes(contents, caps)
	if err == nil {
		t.Error("expected error for image/gif without wildcard, got nil")
	}
}
