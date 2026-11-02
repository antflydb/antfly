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
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/docsaf"
	"github.com/antflydb/antfly/go/pkg/embedded"
)

// corpusIncludes selects Antfly's own documentation relative to the repository
// root (the -repo flag): user guides and reference, subsystem design docs,
// plans, and dated history.
var corpusIncludes = []string{
	"docs/*.md",
	"docs/*.mdx",
	"docs/guides/**",
	"docs/reference/**",
	"docs/operations/**",
	"docs/design/**",
	"docs/plans/**/*.md",
	"zig/*.md",
	"zig/pkg/**/*.md",
	"zig/lib/**/*.md",
}

// corpusExcludes trims generated, vendored, and non-prose files out of the
// corpus.
var corpusExcludes = []string{
	"**/node_modules/**",
	"**/testdata/**",
	"**/CHANGELOG.md",
	"**/CHANGELOG*.md",
	"**/LICENSE*",
}

// docSection is this example's document shape: one row per markdown section,
// classified by path (see docKind). This is
// deliberately narrower than docsaf.DocumentSection.ToDocument(): the example
// wants a stable, example-specific field set (title/heading_path/body/
// source/kind) rather than docsaf's generic content-source document shape.
type docSection struct {
	Key         string
	Title       string
	HeadingPath []string
	Body        string
	Source      string
	Kind        string
}

// docKind classifies a corpus-relative path. Dated records under history/
// are "work-log", proposed work is "plan", user-facing guides and reference
// pages are "guide" and "reference", and living subsystem docs are "design".
func docKind(relPath string) string {
	switch {
	case strings.HasPrefix(relPath, "docs/plans/"):
		return "plan"
	case strings.HasPrefix(relPath, "docs/") && strings.Contains(relPath, "/history/"):
		return "work-log"
	case strings.HasPrefix(relPath, "docs/guides/"),
		strings.HasPrefix(relPath, "docs/") && strings.Count(relPath, "/") == 1:
		return "guide"
	case strings.HasPrefix(relPath, "docs/reference/"), strings.HasPrefix(relPath, "docs/operations/"):
		return "reference"
	default:
		return "design"
	}
}

// collectDocSections walks the corpus under repoRoot and splits every
// markdown file into sections using docsaf's heading-aware MarkdownProcessor.
func collectDocSections(ctx context.Context, repoRoot string) ([]docSection, error) {
	source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
		BaseDir:         repoRoot,
		IncludePatterns: corpusIncludes,
		ExcludePatterns: corpusExcludes,
	})

	processor := &docsaf.MarkdownProcessor{}
	items, errs := source.Traverse(ctx)

	var sections []docSection
	for item := range items {
		if !processor.CanProcess(item.ContentType, item.Path) {
			continue
		}
		parsed, err := processor.Process(item.Path, "", "", item.Content)
		if err != nil {
			log.Printf("skip %s: %v", item.Path, err)
			continue
		}
		relPath := item.Path
		kind := docKind(relPath)
		for _, section := range parsed {
			if strings.TrimSpace(section.Content) == "" {
				continue
			}
			sections = append(sections, docSection{
				Key:         "doc:" + section.ID,
				Title:       section.Title,
				HeadingPath: section.SectionPath,
				Body:        section.Content,
				Source:      relPath,
				Kind:        kind,
			})
		}
	}
	if err := <-errs; err != nil {
		return sections, fmt.Errorf("walk corpus under %s: %w", repoRoot, err)
	}
	return sections, nil
}

// toDocument converts a docSection to the JSON document body the example writes.
func (s docSection) toDocument() map[string]any {
	doc := map[string]any{
		"title":  s.Title,
		"body":   s.Body,
		"source": s.Source,
		"kind":   s.Kind,
	}
	if len(s.HeadingPath) > 0 {
		doc["heading_path"] = s.HeadingPath
	}
	return doc
}

// ingestBatchSize bounds how many documents the example writes per Batch call.
const ingestBatchSize = 200

// ingestSections writes docSections to db in bounded batches, returning the
// number of documents written.
func ingestSections(db *embedded.DB, sections []docSection) (int, error) {
	written := 0
	for start := 0; start < len(sections); start += ingestBatchSize {
		end := min(start+ingestBatchSize, len(sections))
		batch := sections[start:end]

		writes := make([]embedded.WriteIntent, 0, len(batch))
		for _, section := range batch {
			value, err := json.Marshal(section.toDocument())
			if err != nil {
				return written, fmt.Errorf("marshal %s: %w", section.Key, err)
			}
			writes = append(writes, embedded.WriteIntent{
				Key:   section.Key,
				Value: value,
			})
		}

		timestampNS := uint64(time.Now().UnixNano())
		if err := db.Batch(writes, timestampNS); err != nil {
			return written, fmt.Errorf("write batch [%d:%d): %w", start, end, err)
		}
		written += len(writes)
		fmt.Printf("  wrote %d/%d documents\n", written, len(sections))
	}
	return written, nil
}
