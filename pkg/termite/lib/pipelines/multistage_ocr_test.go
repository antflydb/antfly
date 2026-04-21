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

import "testing"

func TestAssembleFullText(t *testing.T) {
	// Helper to build a RecognizedRegion at a given bbox position.
	region := func(text string, x1, y1, x2, y2 float64) RecognizedRegion {
		return RecognizedRegion{
			TextRegion: TextRegion{
				BBox: [4]float64{x1, y1, x2, y2},
			},
			Text: text,
		}
	}

	tests := []struct {
		name    string
		regions []RecognizedRegion
		want    string
	}{
		{
			name:    "empty",
			regions: nil,
			want:    "",
		},
		{
			name: "single region",
			regions: []RecognizedRegion{
				region("Hello", 10, 10, 100, 30),
			},
			want: "Hello",
		},
		{
			name: "same line space separated",
			regions: []RecognizedRegion{
				region("This", 10, 10, 50, 30),
				region("is", 60, 10, 80, 30),
				region("a", 90, 10, 100, 30),
				region("heading", 110, 10, 200, 30),
			},
			want: "This is a heading",
		},
		{
			name: "different lines newline separated",
			regions: []RecognizedRegion{
				region("Line one", 10, 10, 200, 30),
				region("Line two", 10, 60, 200, 80),
			},
			want: "Line one\nLine two",
		},
		{
			name: "mixed same line and different lines",
			regions: []RecognizedRegion{
				// Line 1: two words at y=10..30 (height=20)
				region("Hello", 10, 10, 80, 30),
				region("world", 90, 10, 180, 30),
				// Line 2: one word at y=60..80
				region("Goodbye", 10, 60, 120, 80),
			},
			want: "Hello world\nGoodbye",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := assembleFullText(tt.regions)
			if got != tt.want {
				t.Errorf("assembleFullText() = %q, want %q", got, tt.want)
			}
		})
	}
}
