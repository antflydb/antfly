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
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Grounding is the model's native textual representation, not pixel-space
// OCR regions. Preserve it verbatim in the unit artifact: coordinates use
// LightOn's normalized 0–1000 space and must never be relabeled as pixels.
var groundingHeader = regexp.MustCompile(`^!\[([a-z_]+\+?)\]\(([0-9]+),\s*([0-9]+),\s*([0-9]+),\s*([0-9]+)\)$`)

func validateGroundedImage(text string) (int, error) {
	blocks := 0
	content := false
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "![") {
			if blocks > 0 && !content {
				return 0, fmt.Errorf("empty grounding block")
			}
			match := groundingHeader.FindStringSubmatch(line)
			if len(match) != 6 {
				return 0, fmt.Errorf("invalid grounding header")
			}
			switch strings.TrimSuffix(match[1], "+") {
			case "text", "title", "list", "header", "footer", "page_number", "footnote", "caption", "formula", "code", "table", "image", "chart", "header_image", "footer_image", "aside_text":
			default:
				return 0, fmt.Errorf("unknown grounding label")
			}
			var box [4]int
			for i := range box {
				n, err := strconv.Atoi(match[i+2])
				if err != nil || n > 1000 {
					return 0, fmt.Errorf("invalid normalized grounding coordinates")
				}
				box[i] = n
			}
			if box[2] <= box[0] || box[3] <= box[1] {
				return 0, fmt.Errorf("empty grounding rectangle")
			}
			blocks++
			content = false
		} else {
			if blocks == 0 {
				return 0, fmt.Errorf("text precedes grounding header")
			}
			content = true
		}
	}
	if blocks == 0 || !content {
		return 0, fmt.Errorf("missing grounding content")
	}
	return blocks, nil
}
