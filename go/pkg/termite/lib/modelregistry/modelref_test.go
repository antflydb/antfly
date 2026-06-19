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

package modelregistry

import "testing"

func TestParseModelRefOwnerRepoFormatVariant(t *testing.T) {
	ref, err := ParseModelRef("antflydb/clipclap:gguf:Q4_K")
	if err != nil {
		t.Fatalf("ParseModelRef: %v", err)
	}
	if ref.Owner != "antflydb" || ref.Name != "clipclap" || ref.Format != "gguf" || ref.Variant != "Q4_K" {
		t.Fatalf("ref = %+v", ref)
	}
	if got, want := ref.String(), "antflydb/clipclap:gguf:Q4_K"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestParseModelRefHuggingFaceFormatVariant(t *testing.T) {
	ref, err := ParseModelRef("hf:antflydb/gliner2-base-v1:gguf:Q4_K")
	if err != nil {
		t.Fatalf("ParseModelRef: %v", err)
	}
	if !ref.IsHuggingFace || ref.Owner != "antflydb" || ref.Name != "gliner2-base-v1" || ref.Format != "gguf" || ref.Variant != "Q4_K" {
		t.Fatalf("ref = %+v", ref)
	}
	if got, want := ref.String(), "hf:antflydb/gliner2-base-v1:gguf:Q4_K"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestParseModelRefRejectsUnknownUntaggedVariant(t *testing.T) {
	if _, err := ParseModelRef("antflydb/clipclap:unknown"); err == nil {
		t.Fatal("expected invalid untagged variant error")
	}
}
