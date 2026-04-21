// Copyright 2025 Antfly, Inc.
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

package pipelines

import (
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"
)

// TokenizeTexts tokenizes a batch of text strings and returns padded ModelInputs
// ready for model inference. Sequences are truncated to maxLength and padded to
// the longest sequence in the batch.
func TokenizeTexts(tokenizer tokenizers.Tokenizer, texts []string, maxLength int) *backends.ModelInputs {
	batchSize := len(texts)
	allInputIDs := make([][]int32, batchSize)
	allAttentionMask := make([][]int32, batchSize)
	maxLen := 0

	for i, text := range texts {
		tokens := tokenizer.Encode(text)
		if len(tokens) > maxLength {
			tokens = tokens[:maxLength]
		}
		if len(tokens) > maxLen {
			maxLen = len(tokens)
		}
		allInputIDs[i] = IntToInt32(tokens)
	}

	// Pad to max length and create attention masks
	for i := range allInputIDs {
		origLen := len(allInputIDs[i])
		allAttentionMask[i] = make([]int32, maxLen)
		for j := range origLen {
			allAttentionMask[i][j] = 1
		}
		// Pad input IDs
		if origLen < maxLen {
			padded := make([]int32, maxLen)
			copy(padded, allInputIDs[i])
			allInputIDs[i] = padded
		}
	}

	return &backends.ModelInputs{
		InputIDs:      allInputIDs,
		AttentionMask: allAttentionMask,
	}
}
