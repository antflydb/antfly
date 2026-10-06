// Copyright 2026 Antfly, Inc.
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
	"github.com/antflydb/antfly/go/pkg/lite"
)

// corpusIncludes selects Antfly's own design docs and work log, relative to
// the repository root (the -repo flag).
var corpusIncludes = []string{
	"zig/*.md",
	"zig/pkg/**/*.md",
	"zig/lib/**/*.md",
	"docs/design/**",
	"work-log/**/*.md",
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

// docSection is dogfood's document shape: one row per markdown section,
// classified as a design doc or a work-log entry by path prefix. This is
// deliberately narrower than docsaf.DocumentSection.ToDocument(): dogfood
// wants a stable, dogfood-specific field set (title/heading_path/body/
// source/kind) rather than docsaf's generic content-source document shape.
type docSection struct {
	Key         string
	Title       string
	HeadingPath []string
	Body        string
	Source      string
	Kind        string
}

// docKind classifies a corpus-relative path as a design doc or a work-log
// entry. Everything under work-log/ is "work-log"; everything else in the
// corpus (zig/*.md, zig/pkg/**, zig/lib/**, docs/design/**) is "design".
func docKind(relPath string) string {
	if strings.HasPrefix(relPath, "work-log/") {
		return "work-log"
	}
	return "design"
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

// toDocument converts a docSection to the JSON document body dogfood writes.
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

// ingestBatchSize bounds how many documents dogfood writes per Batch call.
const ingestBatchSize = 200

// ingestSections writes docSections to db in bounded batches, returning the
// number of documents written.
func ingestSections(db *lite.DB, sections []docSection) (int, error) {
	written := 0
	for start := 0; start < len(sections); start += ingestBatchSize {
		end := min(start+ingestBatchSize, len(sections))
		batch := sections[start:end]

		writes := make([]lite.WriteIntent, 0, len(batch))
		for _, section := range batch {
			value, err := json.Marshal(section.toDocument())
			if err != nil {
				return written, fmt.Errorf("marshal %s: %w", section.Key, err)
			}
			writes = append(writes, lite.WriteIntent{
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
