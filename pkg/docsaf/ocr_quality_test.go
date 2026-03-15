package docsaf

import (
	"strings"
	"testing"
)

func TestNeedsOCRFallback(t *testing.T) {
	tests := []struct {
		name          string
		text          string
		minContentLen int
		want          bool
	}{
		{
			name:          "empty text",
			text:          "",
			minContentLen: 50,
			want:          true,
		},
		{
			name:          "short text",
			text:          "Page 1",
			minContentLen: 50,
			want:          true,
		},
		{
			name:          "normal text above threshold",
			text:          "The court was in session today for the trial of the defendant who was charged with multiple counts of fraud and conspiracy.",
			minContentLen: 50,
			want:          false,
		},
		{
			name:          "whitespace only",
			text:          "   \n\n\t  ",
			minContentLen: 50,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NeedsOCRFallback(tt.text, tt.minContentLen)
			if got != tt.want {
				t.Errorf("NeedsOCRFallback() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasGarbledPatterns(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{
			name: "normal text",
			text: "The court was in session today for the trial of the defendant who was charged with multiple counts of fraud and conspiracy in the southern district of New York.",
			want: false,
		},
		{
			name: "too few words",
			text: "Hello world",
			want: false,
		},
		{
			name: "garbled single-char words",
			text: strings.Repeat("$ J U H Q % O D Q G R I ", 5),
			want: true,
		},
		{
			name: "legal text with some single chars",
			text: "Case No. 1:15-cv-07433-LAP v Maxwell Document Filed in U.S. District Court for the Southern District of New York on January 5 2024",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasGarbledPatterns(tt.text)
			if got != tt.want {
				t.Errorf("HasGarbledPatterns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplacementCharRatio(t *testing.T) {
	tests := []struct {
		name string
		text string
		want float64
	}{
		{
			name: "no replacements",
			text: "clean text",
			want: 0,
		},
		{
			name: "empty",
			text: "",
			want: 0,
		},
		{
			name: "all replacements",
			text: "\uFFFD\uFFFD\uFFFD\uFFFD",
			want: 1.0,
		},
		{
			name: "mixed",
			text: "ab\uFFFDcd",
			want: 0.2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReplacementCharRatio(tt.text)
			if got != tt.want {
				t.Errorf("ReplacementCharRatio() = %v, want %v", got, tt.want)
			}
		})
	}
}
