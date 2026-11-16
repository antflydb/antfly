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

package chunking

import (
	"encoding/json"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"testing"
)

func TestSpeakerIndexPresence(t *testing.T) {
	for _, input := range []string{
		`{"start_ns":0,"end_ns":1500000000,"speaker_index":0}`,
		`{"start_ns":0,"end_ns":1500000000,"speaker_index":3}`,
		`{"start_ns":0,"end_ns":1500000000}`,
	} {
		t.Run(input, func(t *testing.T) {
			var original map[string]json.RawMessage
			if err := json.Unmarshal([]byte(input), &original); err != nil {
				t.Fatal(err)
			}
			// Both direct /chunk responses and nested inference SDK responses use
			// generated types; each must distinguish the first voice from silence.
			var span ChunkSpeakerSpan
			if err := json.Unmarshal([]byte(input), &span); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(span)
			if err != nil {
				t.Fatal(err)
			}
			var roundtrip map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &roundtrip); err != nil {
				t.Fatal(err)
			}
			want, present := original["speaker_index"]
			got, gotPresent := roundtrip["speaker_index"]
			if present != gotPresent || string(want) != string(got) {
				t.Fatalf("speaker presence changed: %s -> %s", input, encoded)
			}
			for _, value := range []any{&ChunkCoordinates{}, &ChunkTranscriptSpan{}, &oapi.ChunkSpeakerSpan{}, &oapi.ChunkCoordinates{}, &oapi.ChunkTranscriptSpan{}} {
				if err := json.Unmarshal([]byte(input), value); err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				roundtrip = nil
				if err := json.Unmarshal(encoded, &roundtrip); err != nil {
					t.Fatal(err)
				}
				got, gotPresent := roundtrip["speaker_index"]
				if present != gotPresent || string(want) != string(got) {
					t.Fatalf("%T lost speaker presence: %s -> %s", value, input, encoded)
				}
			}
			if present != (span.SpeakerIndex != nil) {
				t.Fatalf("presence lost on decode: %s", input)
			}
		})
	}
}
