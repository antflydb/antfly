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
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources"
	"github.com/antflydb/antfly/go/pkg/sources/management"
)

const defaultTranscriberModel = "openai/whisper-small:safetensors@973afd24965f72e36ca33b3055d56a652f456b4d"

// Profiles select native recipes and coverage policies. Acquisition remains
// independent. Each profile gets a separate corpus state/table so installing a
// new recipe cannot invalidate an accepted PDF table's producers or artifacts.
type unitEvidence uint8

const (
	pdfPageEvidence unitEvidence = iota
	transcriptEvidence
	groundedImageEvidence
)

type corpusProfile struct {
	Name, Purpose, Route string
	MIMEs                []string
	Evidence             unitEvidence
}

func processingProfile(name string) (corpusProfile, error) {
	switch name {
	case "", "pdf":
		return corpusProfile{Name: "pdf", Purpose: "extraction", Route: "pdf", MIMEs: []string{"application/pdf"}, Evidence: pdfPageEvidence}, nil
	case "images":
		return corpusProfile{Name: "images", Purpose: "caption", Route: "image", MIMEs: []string{"image/png", "image/jpeg", "image/webp"}, Evidence: groundedImageEvidence}, nil
	case "transcription":
		// Raw preservation accepts more formats than the native Whisper profile.
		// Add processing formats only with decoder and temporal-evidence support.
		return corpusProfile{Name: "transcription", Purpose: "transcription", Route: "audio", MIMEs: []string{"audio/wav", "audio/mpeg", "audio/mp4", "audio/ogg", "audio/aac", "audio/aiff", "audio/x-caf", "audio/flac"}, Evidence: transcriptEvidence}, nil
	default:
		return corpusProfile{}, fmt.Errorf("unknown processing profile %q (available: pdf, transcription, images)", name)
	}
}
func (p corpusProfile) accepts(mime string) bool {
	return slices.Contains(p.MIMEs, mime)
}

func nativeTranscriptionConfig(filename, inferenceURL, secretRef string) (map[string]any, error) {
	transcriber := map[string]any{"provider": "antfly", "model": defaultTranscriberModel}
	if filename != "" {
		transcriber = nil
		raw, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &transcriber); err != nil {
			return nil, err
		}
	}
	provider, _ := transcriber["provider"].(string)
	model, _ := transcriber["model"].(string)
	if provider == "" || (provider == "antfly" && model == "") {
		return nil, fmt.Errorf("transcriber requires a provider and an explicit model for Antfly")
	}
	if timestamps, exists := transcriber["timestamps"]; exists && timestamps != true {
		return nil, fmt.Errorf("transcription profile requires timestamps")
	}
	transcriber["timestamps"] = true
	if transcriber["provider"] == "antfly" && inferenceURL != "" {
		if transcriber["api_url"] == nil && transcriber["url"] == nil {
			endpoint, err := inferenceMLBaseURL(inferenceURL)
			if err != nil {
				return nil, err
			}
			transcriber["api_url"] = endpoint
		}
		if secretRef != "" {
			if err := validateCorpusInferenceEndpoint(transcriber, inferenceURL); err != nil {
				return nil, err
			}
		}
	}
	// Validate author-supplied credentials before adding the authorized server
	// secret reference. Reuse the same preset validation as antfly-sources.
	if _, err := sources.PreviewProcessing(management.ProcessingPreset{Name: "speech", Kind: "transcription", ProducerConfig: transcriber}); err != nil {
		return nil, err
	}
	if transcriber["provider"] == "antfly" && secretRef != "" {
		transcriber["api_key"] = secretRef
		delete(transcriber, "bearer_token")
	}
	producer := sources.NativeTranscriptProducer(transcriber)
	return producer["config"].(map[string]any), nil
}

// Apply the same generation and content-address constraints to a profile's
// supported suffix. The public PDF explorer keeps its narrower PDF-only gate.
func validProcessingLocator(uri *url.URL) bool {
	if uri == nil || uri.Scheme != "gs" || uri.Fragment != "" {
		return false
	}
	mime := corpusMIME(uri.Path)
	if mime == "" {
		return false
	}
	copy := *uri
	copy.Path = strings.TrimSuffix(copy.Path, path.Ext(copy.Path)) + ".pdf"
	return validOriginalLocator(&copy, copy.Host)
}
