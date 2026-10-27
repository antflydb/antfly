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

package docsaf

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestFilesystemSourceIncludePatterns(t *testing.T) {
	baseDir := t.TempDir()
	for _, path := range []string{
		"README.md",
		"notes.txt",
		"docs/guide.md",
		"docs/api/reference.md",
		"other/skip.md",
		"other/guide/file.md",
	} {
		fullPath := filepath.Join(baseDir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte("# "+path), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{
			name:     "top-level wildcard",
			patterns: []string{"*.md"},
			want:     []string{"README.md"},
		},
		{
			name:     "nested wildcard",
			patterns: []string{"docs/*.md"},
			want:     []string{"docs/guide.md"},
		},
		{
			name:     "directory wildcard",
			patterns: []string{"*/api/*.md"},
			want:     []string{"docs/api/reference.md"},
		},
		{
			name:     "recursive wildcard",
			patterns: []string{"docs/**/*.md"},
			want:     []string{"docs/api/reference.md", "docs/guide.md"},
		},
		{
			name:     "brace alternatives with different depths",
			patterns: []string{"{docs/api,other}/guide/*.md"},
			want:     []string{"other/guide/file.md"},
		},
		{
			name:     "brace alternatives with escaped slash",
			patterns: []string{`{docs\/api,other}/guide/*.md`},
			want:     []string{"other/guide/file.md"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := NewFilesystemSource(FilesystemSourceConfig{
				BaseDir:         baseDir,
				IncludePatterns: tt.patterns,
			})
			items, errs := source.Traverse(context.Background())

			var got []string
			for item := range items {
				got = append(got, filepath.ToSlash(item.Path))
			}
			if err := <-errs; err != nil {
				t.Fatalf("Traverse() error = %v", err)
			}

			slices.Sort(got)
			if !slices.Equal(got, tt.want) {
				t.Errorf("Traverse() paths = %v, want %v", got, tt.want)
			}
		})
	}
}
