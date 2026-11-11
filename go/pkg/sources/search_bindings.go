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
	"reflect"

	"github.com/antflydb/antfly/go/pkg/sources/internal/declarations"
	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type SearchStream = management.SearchStream
type ArtifactBinding = management.ArtifactBinding
type RetrievalBindings = management.RetrievalBindings
type VectorSpace = management.VectorSpace

const SearchVersion = declarations.SearchVersion

func validBindingName(value string) bool { return declarations.ValidBindingName(value) }

// ValidateSearchBindings checks the shared search declaration contract.
func ValidateSearchBindings(version int, streams []SearchStream, artifacts []ArtifactBinding) error {
	return declarations.ValidateSearchBindings(version, streams, artifacts)
}

// ValidateVectorSpace checks a complete pinned vector-space declaration.
func ValidateVectorSpace(v VectorSpace) error { return declarations.ValidateVectorSpace(v) }

// CompatibleVectorSpace compares the entire recipe, not just name/dimension.
func CompatibleVectorSpace(expected, supplied VectorSpace) bool {
	return ValidateVectorSpace(expected) == nil && ValidateVectorSpace(supplied) == nil && reflect.DeepEqual(expected, supplied)
}

// LegacyStream is a borrowed text-consumer adapter. Semantic-only streams are
// deliberately excluded, rather than becoming fabricated empty text matches.
func LegacyStream(stream SearchStream) (TextStream, bool) {
	if stream.Retrieval == nil {
		return stream, stream.Field != ""
	}
	r := stream.Retrieval
	if r.Exact == nil {
		return TextStream{}, false
	}
	stream.Default = r.Exact.Default
	if stream.Artifact != "" {
		if r.FullText == nil {
			return TextStream{}, false
		}
		stream.Index = r.FullText.Index
	}
	stream.Retrieval = nil
	return stream, true
}

func DefaultSearchStreams(version int) []SearchStream {
	if version < 2 {
		return DefaultTextStreams()
	}
	return []SearchStream{{Name: "source", Field: "content.text", Purpose: "source", Boundary: "record", Retrieval: &RetrievalBindings{Exact: &management.ExactRetrieval{Default: true}}}}
}
