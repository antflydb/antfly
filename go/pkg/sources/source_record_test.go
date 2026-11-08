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
	"strings"
	"testing"
	"time"
)

func TestNeutralSourceWireRoundTripAndOwnership(t *testing.T) {
	item := Item{ID: "s3://job/" + testSourceID + "/file.txt", Path: "file.txt", URL: "s3://bucket/file.txt", Revision: "v1", SourceKind: ProviderS3, Metadata: map[string]any{"generation": int64(9007199254740993)}}
	doc, err := Normalize(testSourceID, item, []byte("provider text"), Permissions{Readers: []string{"user:self"}}, time.Now())
	require.NoError(t, err)
	key, err := RecordKey(testSourceID, item.ID)
	require.NoError(t, err)
	row, err := SourceRow(key, testSourceID, "", doc)
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
	_, err = SourceRow(key, "another-source", "", doc)
	require.Error(t, err)
	require.Error(t, json.Unmarshal([]byte(`{"schema_version":3,"source":{},"content":{"text":"x","resource":{}}}`), &decoded))
}

// TestIdentityKeyPreservesLegacyFormulaWithoutANamespace pins that a source
// which has never set identity.namespace keys its rows exactly as before
// namespaces existed: IdentityKey with an empty namespace must equal
// RecordKey byte for byte, for every input RecordKey accepts or rejects.
func TestIdentityKeyPreservesLegacyFormulaWithoutANamespace(t *testing.T) {
	providerID := "s3://job/" + testSourceID + "/docs/file.txt"
	legacy, err := RecordKey(testSourceID, providerID)
	require.NoError(t, err)
	withEmptyNamespace, err := IdentityKey(testSourceID, "", "docs/file.txt", providerID)
	require.NoError(t, err)
	require.Equal(t, legacy, withEmptyNamespace)

	// A relative key is never consulted when namespace is empty, so garbage
	// there changes nothing.
	alsoEmptyNamespace, err := IdentityKey(testSourceID, "", "", providerID)
	require.NoError(t, err)
	require.Equal(t, legacy, alsoEmptyNamespace)

	_, err = RecordKey(testSourceID, "not-owned")
	require.Error(t, err)
	_, err = IdentityKey(testSourceID, "", "docs/file.txt", "not-owned")
	require.Error(t, err)
}

// TestIdentityKeyNamespaceFollowsContentNotLocation pins the actual point of
// namespace: once set, the key depends only on namespace+relativeKey, so two
// completely different provider IDs (different providers, different
// locations) produce the same row identity as long as they share a namespace
// and relative key, and the same provider ID's row identity changes the
// moment either namespace or relative key changes.
func TestIdentityKeyNamespaceFollowsContentNotLocation(t *testing.T) {
	fromS3, err := IdentityKey(testSourceID, "ns", "docs/file.txt", "s3://job/"+testSourceID+"/bucket/docs/file.txt")
	require.NoError(t, err)
	fromGCS, err := IdentityKey(testSourceID, "ns", "docs/file.txt", "gcs://job/"+testSourceID+"/other-bucket/docs/file.txt")
	require.NoError(t, err)
	require.Equal(t, fromS3, fromGCS, "identical namespace+relativeKey must key the same row regardless of provider or location")

	differentNamespace, err := IdentityKey(testSourceID, "other-ns", "docs/file.txt", "s3://job/"+testSourceID+"/bucket/docs/file.txt")
	require.NoError(t, err)
	require.NotEqual(t, fromS3, differentNamespace)

	differentRelativeKey, err := IdentityKey(testSourceID, "ns", "docs/other.txt", "s3://job/"+testSourceID+"/bucket/docs/file.txt")
	require.NoError(t, err)
	require.NotEqual(t, fromS3, differentRelativeKey)

	// The object whose name equals a literal bucket prefix has an explicit
	// empty suffix. It is a distinct identity, not a missing/native-ID key.
	rootKey, err := IdentityKey(testSourceID, "ns", "", "s3://job/"+testSourceID+"/bucket/docs/file.txt")
	require.NoError(t, err)
	require.NotEqual(t, fromS3, rootKey)
}

// TestIdentityKeyCurrentPrefixInvariant pins the construction this task's
// design relies on: setting namespace to a source's own current provider-ID
// prefix (TrimSuffix(providerID, "/"+relativeKey)) reconstructs providerID
// byte for byte, for any path-like provider ID. This is what lets a source
// "adopt" a namespace derived from its own existing rows without having to
// invent a new relative-key convention.
func TestIdentityKeyCurrentPrefixInvariant(t *testing.T) {
	for _, providerID := range []string{
		"s3://job/" + testSourceID + "/bucket/docs/file.txt",
		"gcs://job/" + testSourceID + "/bucket/a/b/c.pdf",
		"local://job/" + testSourceID + "/prefix/objects/sha256/abc.bin",
	} {
		relativeKey := "docs/file.txt"
		switch {
		case strings.Contains(providerID, "a/b/c.pdf"):
			relativeKey = "a/b/c.pdf"
		case strings.Contains(providerID, "objects/sha256"):
			relativeKey = "objects/sha256/abc.bin"
		}
		namespace := strings.TrimSuffix(providerID, "/"+relativeKey)
		require.Equal(t, providerID, namespace+"/"+relativeKey, providerID)
	}
}

// TestLegacyNamespaceReproducesExistingKeys is the end-to-end proof that
// LegacyNamespace's whole point works: for every item of a source,
// IdentityKey(sourceID, LegacyNamespace(items), relativeKey, item.ID) must
// equal RecordKey(sourceID, item.ID) -- the legacy key that item already
// has -- for both a connector that reports an explicit RelativeKey (S3-like)
// and one that falls back to its native ID (Drive/GitHub-like).
func TestLegacyNamespaceReproducesExistingKeys(t *testing.T) {
	pathLike := []Item{
		{ID: "s3://job/" + testSourceID + "/bucket/docs/a.txt", RelativeKey: "docs/a.txt"},
		{ID: "s3://job/" + testSourceID + "/bucket/docs/b.txt", RelativeKey: "docs/b.txt"},
	}
	nativeIDFallback := []Item{
		{ID: "drive://job/" + testSourceID + "/file-id-one"},
		{ID: "drive://job/" + testSourceID + "/file-id-two"},
	}
	for _, items := range [][]Item{pathLike, nativeIDFallback} {
		namespace, err := LegacyNamespace(testSourceID, items)
		require.NoError(t, err)
		for _, item := range items {
			legacy, err := RecordKey(testSourceID, item.ID)
			require.NoError(t, err)
			adopted, err := IdentityKey(testSourceID, namespace, relativeKeyFor(testSourceID, item), item.ID)
			require.NoError(t, err)
			require.Equal(t, legacy, adopted, item.ID)
		}
	}
}

func TestLegacyNamespaceRejectsDisagreementAndEmptyInput(t *testing.T) {
	_, err := LegacyNamespace(testSourceID, nil)
	require.Error(t, err)

	// Two items whose provider IDs genuinely disagree on their namespace
	// prefix (different buckets) must error rather than silently adopt one
	// and re-key the other.
	disagreeing := []Item{
		{ID: "s3://job/" + testSourceID + "/bucket-one/docs/a.txt", RelativeKey: "docs/a.txt"},
		{ID: "s3://job/" + testSourceID + "/bucket-two/docs/b.txt", RelativeKey: "docs/b.txt"},
	}
	_, err = LegacyNamespace(testSourceID, disagreeing)
	require.Error(t, err)

	// An item whose ID does not actually end with its relative key is a
	// malformed input, not a namespace disagreement, but must also error
	// rather than derive a garbage prefix.
	malformed := []Item{{ID: "s3://job/" + testSourceID + "/bucket/docs/a.txt", RelativeKey: "not-a-suffix"}}
	_, err = LegacyNamespace(testSourceID, malformed)
	require.Error(t, err)
}

func TestSourceRowRecomputesIdentityUnderTheOwningNamespace(t *testing.T) {
	item := Item{ID: "s3://job/" + testSourceID + "/bucket/docs/file.txt", RelativeKey: "docs/file.txt", Path: "docs/file.txt", URL: "s3://bucket/docs/file.txt", Revision: "v1", SourceKind: ProviderS3}
	doc, err := Normalize(testSourceID, item, []byte("text"), Permissions{Readers: []string{"user:self"}}, time.Now())
	require.NoError(t, err)
	require.Equal(t, "docs/file.txt", doc.Metadata["relative_key"])

	key, err := IdentityKey(testSourceID, "ns", "docs/file.txt", item.ID)
	require.NoError(t, err)
	row, err := SourceRow(key, testSourceID, "ns", doc)
	require.NoError(t, err)
	require.NotNil(t, row)

	// The legacy key is wrong once a namespace is in play.
	legacyKey, err := RecordKey(testSourceID, item.ID)
	require.NoError(t, err)
	_, err = SourceRow(legacyKey, testSourceID, "ns", doc)
	require.Error(t, err)

	// Round trip through the wire format preserves relative_key so a later
	// re-verification (e.g. inside SyncWriter.Upsert) can still recompute it.
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	var decoded Document
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, "docs/file.txt", decoded.Metadata["relative_key"])
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

func TestExplicitEmptyBucketRelativeKeySurvivesPublication(t *testing.T) {
	item := Item{ID: "s3://job/" + testSourceID + "/bucket/prefix", Path: "prefix", URL: "s3://bucket/prefix", Revision: "v1", SourceKind: ProviderS3, RelativeKeySet: true}
	doc, err := Normalize(testSourceID, item, []byte("content"), Permissions{Public: true}, time.Now())
	require.NoError(t, err)
	require.Equal(t, "", doc.Metadata["relative_key"])
	key, err := IdentityKey(testSourceID, "ns", "", item.ID)
	require.NoError(t, err)
	_, err = SourceRow(key, testSourceID, "ns", doc)
	require.NoError(t, err)
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	var decoded Document
	require.NoError(t, json.Unmarshal(raw, &decoded))
	_, err = SourceRow(key, testSourceID, "ns", decoded)
	require.NoError(t, err)
	delete(doc.Metadata, "relative_key")
	_, err = SourceRow(key, testSourceID, "ns", doc)
	require.Error(t, err, "missing metadata must not impersonate an explicit empty key")
}
