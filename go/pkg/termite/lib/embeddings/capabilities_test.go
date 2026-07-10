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

package embeddings

import (
	"testing"

	modelcaps "github.com/antflydb/antfly/go/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/go/pkg/termite/lib/pipelines"
)

func TestBuildCapabilities_VisualImagesAreExact(t *testing.T) {
	caps := buildCapabilities(nil, &pipelines.EmbeddingPipeline{}, nil)

	assertMIMESupported(t, caps, "image/jpeg")
	assertMIMESupported(t, caps, "image/png")
	assertMIMESupported(t, caps, "image/gif")
	assertMIMESupported(t, caps, "image/bmp")
	assertMIMESupported(t, caps, "image/webp")
	assertMIMENotSupported(t, caps, "image/*")
}

func assertMIMESupported(t *testing.T, caps modelcaps.EmbedderCapabilities, mimeType string) {
	t.Helper()
	if !caps.SupportsMIMEType(mimeType) {
		t.Fatalf("expected %s to be supported by %#v", mimeType, caps.SupportedMIMETypes)
	}
}

func assertMIMENotSupported(t *testing.T, caps modelcaps.EmbedderCapabilities, mimeType string) {
	t.Helper()
	if caps.SupportsMIMEType(mimeType) {
		t.Fatalf("expected %s to be unsupported by %#v", mimeType, caps.SupportedMIMETypes)
	}
}
