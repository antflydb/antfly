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
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type SearchStream = management.SearchStream
type ArtifactBinding = management.ArtifactBinding
type RetrievalBindings = management.RetrievalBindings
type VectorSpace = management.VectorSpace

const SearchVersion = 2

func validBindingName(value string) bool {
	return value != "" && len(value) <= 4096 && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

// ValidateSearchBindings validates desired declarations, never native readiness
// or authorization. Version zero is the original text-only wire contract.
func ValidateSearchBindings(version int, streams []SearchStream, artifacts []ArtifactBinding) error {
	if version < 0 || version > SearchVersion {
		return fmt.Errorf("unsupported search version %d", version)
	}
	if len(streams) > 64 || len(artifacts) > 64 {
		return fmt.Errorf("at most 64 streams and artifacts are supported")
	}
	names := map[string]bool{}
	for _, stream := range streams {
		if !validBindingName(stream.Name) || names[stream.Name] {
			return fmt.Errorf("invalid or duplicate stream name")
		}
		names[stream.Name] = true
		for _, value := range []string{stream.Field, stream.ResourceField, stream.Artifact, stream.Index, stream.Purpose, stream.LocationField} {
			if value != "" && !validBindingName(value) {
				return fmt.Errorf("invalid stream binding")
			}
		}
		if stream.Boundary != "" && stream.Boundary != "record" && stream.Boundary != "unit" {
			return fmt.Errorf("unsupported text matching boundary")
		}
		if version < 2 {
			if stream.Retrieval != nil || stream.ResourceField != "" || stream.Field == "" || (stream.Artifact == "") != (stream.Index == "") {
				return fmt.Errorf("legacy streams require text and an artifact/full-text pair; new bindings require search_version=2")
			}
			continue
		}
		if stream.Index != "" || stream.Default {
			return fmt.Errorf("version 2 uses retrieval bindings and per-mode defaults")
		}
		if stream.Retrieval == nil {
			return fmt.Errorf("stream requires retrieval")
		}
		r := stream.Retrieval
		if r.Exact == nil && r.FullText == nil && r.Semantic == nil {
			return fmt.Errorf("stream requires a retrieval operation")
		}
		if stream.ResourceField != "" && (stream.Field != "" || stream.Artifact != "") {
			return fmt.Errorf("resource representation cannot also name text or an artifact")
		}
		if stream.Field == "" && stream.ResourceField == "" && stream.Artifact == "" {
			return fmt.Errorf("stream requires a representation")
		}
		if (r.Exact != nil || r.FullText != nil) && stream.Field == "" {
			return fmt.Errorf("text retrieval requires a textual field")
		}
		if r.Exact != nil && stream.Artifact != "" && r.FullText == nil {
			return fmt.Errorf("exact artifact enumeration requires a full-text member index")
		}
		if r.FullText != nil && !validBindingName(r.FullText.Index) {
			return fmt.Errorf("full-text retrieval requires an index")
		}
		if semantic := r.Semantic; semantic != nil {
			if !validBindingName(semantic.Index) {
				return fmt.Errorf("semantic retrieval requires an index")
			}
			if err := ValidateVectorSpace(semantic.VectorSpace); err != nil {
				return err
			}
			inputs := map[management.SemanticRetrievalQueryInputs]bool{}
			for _, input := range semantic.QueryInputs {
				if input != "text" && input != "vector" || inputs[input] {
					return fmt.Errorf("invalid or duplicate semantic query input")
				}
				inputs[input] = true
			}
			if inputs["text"] && semantic.VectorSpace.QueryModality != "text" && semantic.VectorSpace.QueryModality != "multimodal" {
				return fmt.Errorf("text query input is incompatible with the declared query modality")
			}
			if len(inputs) == 0 || semantic.External && inputs["text"] {
				return fmt.Errorf("external vectors accept supplied query vectors; managed bindings require supported query inputs")
			}
			if len(semantic.MemberArtifacts) > 64 {
				return fmt.Errorf("too many semantic member artifacts")
			}
			members := map[string]bool{}
			for _, member := range semantic.MemberArtifacts {
				if !validBindingName(member) || members[member] {
					return fmt.Errorf("invalid or duplicate member artifact")
				}
				members[member] = true
			}
		}
	}
	names = map[string]bool{}
	if version < 2 && len(artifacts) > 0 {
		return fmt.Errorf("artifact discovery requires search_version=2")
	}
	for _, artifact := range artifacts {
		if !validBindingName(artifact.Name) || !validBindingName(artifact.Artifact) || !validBindingName(artifact.ContentType) || names[artifact.Name] {
			return fmt.Errorf("invalid or duplicate artifact binding")
		}
		names[artifact.Name] = true
		for _, value := range []string{artifact.Purpose, artifact.LocationField, artifact.SchemaUri} {
			if value != "" && !validBindingName(value) {
				return fmt.Errorf("invalid artifact metadata")
			}
		}
	}
	return nil
}

func ValidateVectorSpace(v VectorSpace) error {
	for _, value := range []string{v.Id, v.Model, v.Revision, v.QueryPreprocessing, v.DocumentPreprocessing} {
		if !validBindingName(value) {
			return fmt.Errorf("complete pinned vector-space identity is required")
		}
	}
	if v.Dimension < 1 || v.Dimension > 65536 {
		return fmt.Errorf("invalid vector dimension")
	}
	if v.Metric != "cosine" && v.Metric != "inner_product" && v.Metric != "l2_squared" {
		return fmt.Errorf("unsupported vector metric")
	}
	for _, modality := range []string{string(v.QueryModality), string(v.DocumentModality)} {
		if modality != "text" && modality != "image" && modality != "audio" && modality != "multimodal" {
			return fmt.Errorf("unsupported vector modality")
		}
	}
	return nil
}

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
