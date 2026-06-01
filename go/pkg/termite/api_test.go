// Copyright 2026 Antfly, Inc.
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

	"github.com/antflydb/antfly/go/pkg/libaf/ai"
	"github.com/antflydb/antfly/go/pkg/libaf/embeddings"
)

func TestConvertChatMessagePreservesMediaImageParts(t *testing.T) {
	textPart := ContentPart{}
	if err := textPart.FromTextContentPart(TextContentPart{
		Type: TextContentPartTypeText,
		Text: "describe",
	}); err != nil {
		t.Fatalf("text part: %v", err)
	}

	mediaURLPart := ContentPart{}
	if err := mediaURLPart.FromMediaContentPart(MediaContentPart{
		Type:     MediaContentPartTypeMedia,
		Url:      "https://example.test/image.png",
		MimeType: "image/png",
	}); err != nil {
		t.Fatalf("media url part: %v", err)
	}

	inlineMediaPart := ContentPart{}
	if err := inlineMediaPart.FromMediaContentPart(MediaContentPart{
		Type:     MediaContentPartTypeMedia,
		Data:     []byte{1, 2, 3},
		MimeType: "image/jpeg",
	}); err != nil {
		t.Fatalf("inline media part: %v", err)
	}

	audioPart := ContentPart{}
	if err := audioPart.FromMediaContentPart(MediaContentPart{
		Type:     MediaContentPartTypeMedia,
		Url:      "https://example.test/audio.wav",
		MimeType: "audio/wav",
	}); err != nil {
		t.Fatalf("audio media part: %v", err)
	}

	content := ChatMessageContent{}
	if err := content.FromChatMessageContent1([]ContentPart{textPart, mediaURLPart, inlineMediaPart, audioPart}); err != nil {
		t.Fatalf("content: %v", err)
	}

	msg := convertChatMessage(ChatMessage{
		Role:    RoleUser,
		Content: content,
	})

	if msg.Content != "describe" {
		t.Fatalf("content = %q", msg.Content)
	}
	if len(msg.Parts) != 3 {
		t.Fatalf("got %d parts, want 3", len(msg.Parts))
	}
	if msg.Parts[0].Type != "text" || msg.Parts[0].Text != "describe" {
		t.Fatalf("text part = %#v", msg.Parts[0])
	}
	if msg.Parts[1].Type != "image_url" || msg.Parts[1].ImageURL != "https://example.test/image.png" {
		t.Fatalf("media url image part = %#v", msg.Parts[1])
	}
	if msg.Parts[2].Type != "image_url" || msg.Parts[2].ImageURL != "data:image/jpeg;base64,AQID" {
		t.Fatalf("inline media image part = %#v", msg.Parts[2])
	}
}

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
