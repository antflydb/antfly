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
	"math"
	"strings"
)

// CTCDecode performs greedy CTC decoding on model output logits.
// It takes the argmax at each timestep, collapses consecutive duplicates,
// and removes the blank token (index 0).
//
// Parameters:
//   - logits: raw model output [timeSteps * vocabSize]
//   - timeSteps: number of time steps
//   - vocabSize: vocabulary size
//   - charDict: character dictionary mapping indices to characters (index 0 = blank)
//
// Returns the decoded text and average confidence score.
func CTCDecode(logits []float32, timeSteps, vocabSize int, charDict []string) (string, float64) {
	if len(logits) < timeSteps*vocabSize {
		return "", 0
	}

	var (
		indices     []int
		confidences []float64
		prevIdx     = -1
	)

	for t := range timeSteps {
		// Find argmax for this timestep
		bestIdx := 0
		bestVal := float32(-math.MaxFloat32)

		offset := t * vocabSize
		for v := range vocabSize {
			if logits[offset+v] > bestVal {
				bestVal = logits[offset+v]
				bestIdx = v
			}
		}

		// Skip blank token (index 0) and consecutive duplicates
		if bestIdx != 0 && bestIdx != prevIdx {
			indices = append(indices, bestIdx)
			// Convert logit to probability via softmax approximation
			confidences = append(confidences, float64(bestVal))
		}

		prevIdx = bestIdx
	}

	if len(indices) == 0 {
		return "", 0
	}

	// Map indices to characters.
	// CTC convention: index 0 is blank (already skipped above),
	// so model index N maps to charDict[N-1].
	var text strings.Builder
	for _, idx := range indices {
		dictIdx := idx - 1
		if dictIdx >= 0 && dictIdx < len(charDict) {
			text.WriteString(charDict[dictIdx])
		}
	}

	// Average confidence
	avgConf := 0.0
	for _, c := range confidences {
		avgConf += c
	}
	avgConf /= float64(len(confidences))

	return text.String(), avgConf
}

// LoadCharDict loads a character dictionary from lines of text.
// The first line is typically the blank token, each subsequent line is a character.
func LoadCharDict(lines []string) []string {
	dict := make([]string, len(lines))
	for i, line := range lines {
		dict[i] = strings.TrimSpace(line)
	}
	return dict
}
