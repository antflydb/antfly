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
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNeutralSourceWireRoundTripAndOwnership(t *testing.T) {
	item := Item{ID: "s3://job/" + testSourceID + "/file.txt", Path: "file.txt", URL: "s3://bucket/file.txt", Revision: "v1", SourceKind: ProviderS3, Metadata: map[string]any{"generation": int64(9007199254740993)}}
	doc, err := Normalize(testSourceID, item, []byte("provider text"), Permissions{Readers: []string{"user:self"}}, time.Now())
	require.NoError(t, err)
	key, err := RecordKey(testSourceID, item.ID)
	require.NoError(t, err)
	row, err := SourceRow(key, testSourceID, doc)
	require.NoError(t, err)
	require.NotContains(t, row, "grep")
	raw, err := json.Marshal(row)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"text":"provider text"`)
	require.Contains(t, string(raw), `9007199254740993`)
	var decoded Document
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, doc.Content, decoded.Content)
	require.Equal(t, doc.URI, decoded.URI)
	require.Equal(t, json.Number("9007199254740993"), decoded.Metadata["generation"])
	_, err = SourceRow(key, "another-source", doc)
	require.Error(t, err)
	require.Error(t, json.Unmarshal([]byte(`{"schema_version":3,"source":{},"content":{"text":"x","resource":{}}}`), &decoded))
}

func TestStreamChangesDoNotReacquireSources(t *testing.T) {
	source := Source{ID: testSourceID, Streams: DefaultTextStreams()}
	before, err := sourceConfigurationHash(source, Connection{Provider: ProviderS3})
	require.NoError(t, err)
	source.Streams = []TextStream{{Name: "transcript", Artifact: "speech", Index: "speech_text", Field: "text", Purpose: "transcription", Boundary: "unit", Default: true}}
	after, err := sourceConfigurationHash(source, Connection{Provider: ProviderS3})
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, ValidateTextStreams(source.Streams))
	source.Streams[0].Index = ""
	require.Error(t, ValidateTextStreams(source.Streams))
}
