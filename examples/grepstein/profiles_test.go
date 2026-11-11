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

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

func TestProcessingProfilesSelectInputsBeforePreparation(t *testing.T) {
	source := t.TempDir()
	writeCorpusFixture(t, filepath.Join(source, "evidence.pdf"), nativeGCSFixturePDF())
	writeCorpusFixture(t, filepath.Join(source, "recording.wav"), nativeTranscriptFixtureAudio())
	writeCorpusFixture(t, filepath.Join(source, "video.mp4"), []byte("raw-only video"))
	for _, name := range []string{"pdf", "transcription"} {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			if err := corpusPrepareCmd([]string{"--profile", name, "--state", state, "--source", "test=" + source}); err != nil {
				t.Fatal(err)
			}
			records := testCorpusRecords(t, state)
			profile, _ := processingProfile(name)
			if len(records) != 1 || !profile.accepts(records[0].Document["mime_type"].(string)) {
				t.Fatalf("wrong selection: %+v", records)
			}
			if err := corpusPrepareCmd([]string{"--state", state, "--resume", "--profile", name}); err == nil {
				t.Fatal("resume accepted profile mutation")
			}
		})
	}
}

func TestTranscriptionUsesSharedNativeRecipeAndServerCredentials(t *testing.T) {
	ordinary, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	pdf, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", "", false, false, corpusIndexOptions{Profile: "pdf"})
	if err != nil {
		t.Fatal(err)
	}
	if corpusDescription(ordinary, 25) != corpusDescription(pdf, 25) {
		t.Fatal("default changed the PDF recipe")
	}
	audio, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "https://inference.example/ai/v1", "", false, false, corpusIndexOptions{Profile: "transcription", InferenceSecret: "shared.token", SourceConnection: "originals"})
	if err != nil {
		t.Fatal(err)
	}
	if corpusDescription(pdf, 25) == corpusDescription(audio, 25) {
		t.Fatal("profile table identity was not fenced")
	}
	enrichments := audio[corpusTextIndex].(map[string]any)["enrichments"].([]any)
	var producer map[string]any
	if err := json.Unmarshal([]byte(enrichments[0].(map[string]any)["producer_json"].(string)), &producer); err != nil {
		t.Fatal(err)
	}
	config := producer["config"].(map[string]any)
	transcriber := config["transcription"].(map[string]any)["transcriber"].(map[string]any)
	if config["route_preset"] != "explicit_only" || config["credentials"] != "originals" || transcriber["model"] != defaultTranscriberModel || transcriber["api_key"] != "${secret:shared.token}" || transcriber["api_url"] != "https://inference.example/ai/v1" {
		t.Fatalf("wrong recipe: %v", producer)
	}
	profile, _ := processingProfile("transcription")
	raw, _ := json.Marshal(nativeGrepConfigForProfile("https://instance.example", "speech", profile))
	if !strings.Contains(string(raw), `"purpose":"transcription"`) || !strings.Contains(string(raw), `"name":"transcript"`) {
		t.Fatal(string(raw))
	}
	for _, value := range []map[string]any{
		{"provider": "antfly", "model": "whisper", "timestamps": false},
		{"provider": "antfly", "model": "whisper", "api_key": "private-value"},
		{"provider": "antfly", "api_key": "${secret:safe}"},
		{"provider": "antfly", "model": "whisper", "api_url": "https://other.example/ai/v1"},
	} {
		file := filepath.Join(t.TempDir(), "transcriber.json")
		if err := atomicCorpusJSON(file, value); err != nil {
			t.Fatal(err)
		}
		if _, err := nativeTranscriptionConfig(file, "https://inference.example/ai/v1", "${secret:shared.token}"); err == nil {
			t.Fatalf("accepted unsafe/incomplete transcriber: %v", value)
		}
	}
	if _, err := processingProfile("caption"); err == nil {
		t.Fatal("enabled an unimplemented profile")
	}
}

func TestTranscriptQualificationChecksEveryUnitAndTimeSpan(t *testing.T) {
	source := "gs://originals/objects/sha256/" + strings.Repeat("a", 64) + ".wav?generation=123"
	for _, problem := range []string{"", "missing-time", "empty-text", "missing-unit", "missing-hits", "provider-error"} {
		t.Run(problem, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if requests == 2 {
					cursor, ok := body["search_after"].([]any)
					if !ok || len(cursor) != 2 || cursor[1] != "unit99" {
						t.Errorf("bad continuation: %v", body)
					}
				}
				start, end := 0, 100
				if requests == 2 {
					start, end = 100, 129
				}
				hits := []any{}
				for i := start; i < end; i++ {
					span := map[string]any{"start_ms": 0, "end_ms": 1000, "char_start": 0, "char_end": 5}
					text := "hello"
					if problem == "missing-time" {
						delete(span, "start_ms")
					}
					if problem == "empty-text" {
						text = ""
					}
					hits = append(hits, map[string]any{"_id": fmt.Sprintf("unit%d", i), "_sort": []string{fmt.Sprint(i), fmt.Sprintf("unit%d", i)}, "_source": map[string]any{"text": text, "provenance": map[string]any{"source_url": source, "transcript_used": true, "transcript_spans": []any{span}}}})
				}
				if problem == "missing-unit" && requests == 2 {
					hits = nil
				}
				status := 200
				if problem == "provider-error" {
					status = 503
				}
				page := map[string]any{"status": status, "hits": map[string]any{"hits": hits}}
				if problem == "missing-hits" {
					delete(page, "hits")
				}
				json.NewEncoder(w).Encode(map[string]any{"responses": []any{page}})
			}))
			defer server.Close()
			segments, err := qualifyTranscriptUnits(context.Background(), server.Client(), server.URL, "recording", source, 129)
			if problem == "" {
				if err != nil || segments != 129 || requests != 2 {
					t.Fatalf("qualification segments=%d requests=%d err=%v", segments, requests, err)
				}
			} else if err == nil {
				t.Fatal("accepted incomplete timestamp inventory")
			}
		})
	}
}

func TestPublishAudioLocatorUsesImmutableGCSIdentity(t *testing.T) {
	source, state := t.TempDir(), t.TempDir()
	data := nativeTranscriptFixtureAudio()
	writeCorpusFixture(t, filepath.Join(source, "recording.wav"), data)
	if err := corpusPrepareCmd([]string{"--profile", "transcription", "--state", state, "--source", "test=" + source}); err != nil {
		t.Fatal(err)
	}
	records := testCorpusRecords(t, state)
	raw, _ := json.Marshal(records[0].Document["metadata"].(map[string]any)["source_locator"])
	var loc sourceLocator
	if err := json.Unmarshal(raw, &loc); err != nil {
		t.Fatal(err)
	}
	var cfg corpusConfig
	if err := readCorpusJSON(filepath.Join(state, "sources.json"), &cfg); err != nil {
		t.Fatal(err)
	}
	store := newCorpusStore(cfg)
	defer store.Close()
	checksum := md5.Sum(data)
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Method == "GET" {
			return response(404, ""), nil
		}
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Type") != "audio/wav" || !bytes.Equal(body, data) || r.URL.Query().Get("ifGenerationMatch") != "0" {
			t.Error("audio source was transformed or uploaded without its precondition")
		}
		return response(200, fmt.Sprintf(`{"generation":"77","size":%q,"md5Hash":%q}`, fmt.Sprint(len(data)), base64.StdEncoding.EncodeToString(checksum[:]))), nil
	})}
	locator, version, err := publishLocator(client, store, "originals", loc)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(locator)
	if err != nil || !validProcessingLocator(uri) || !strings.HasSuffix(version, ":77") || !strings.Contains(locator, ".wav?generation=77") {
		t.Fatalf("invalid immutable audio identity: %s %s %v", locator, version, err)
	}
	if validOriginalLocator(uri, "originals") {
		t.Fatal("audio widened the public PDF-only delivery gate")
	}
}
