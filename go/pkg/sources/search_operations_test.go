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
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestProcessingPreviewAndScopedJobContract(t *testing.T) {
	preview, err := PreviewProcessing(management.ProcessingPreset{Name: "content", Kind: "content"})
	require.NoError(t, err)
	require.Len(t, preview.Processing.Indexes, 1)
	require.NoError(t, ValidateProcessing(&preview.Processing))
	summary, err := PreviewProcessing(management.ProcessingPreset{Name: "summary", Kind: "summary", ProducerConfig: map[string]any{"prompt": "Summarize", "api_key": "${secret:inference-key}"}})
	require.NoError(t, err)
	require.True(t, summary.Stream.Generated)
	require.False(t, summary.Stream.Retrieval.Exact.Default)
	_, err = PreviewProcessing(management.ProcessingPreset{Name: "summary", Kind: "summary", ProducerConfig: map[string]any{"api_key": "plaintext"}})
	require.Error(t, err)
	source := management.Source{Table: "shared", Artifacts: []ArtifactBinding{preview.Artifact}}
	method, path, body, err := ProcessingJobPath(source, management.ProcessingJobRequest{Artifact: preview.Artifact.Artifact, Action: "start"})
	require.NoError(t, err)
	require.Equal(t, "POST", method)
	require.True(t, strings.HasSuffix(path, "/reprocess-jobs"))
	require.Equal(t, false, body.(map[string]any)["advance"])
	_, _, _, err = ProcessingJobPath(source, management.ProcessingJobRequest{Artifact: "another-source-artifact", Action: "start"})
	require.Error(t, err)
	_, _, _, err = ProcessingJobPath(source, management.ProcessingJobRequest{Artifact: preview.Artifact.Artifact, Action: "advance", JobId: "../other"})
	require.Error(t, err)
	job, err := DecodeProcessingJob([]byte(`{"job_id":18446744073709551615,"phase":"queued"}`))
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", job["job_id"])
}
