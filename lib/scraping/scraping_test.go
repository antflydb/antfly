// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package scraping

import (
	"testing"
)

func TestExtractHTMLText(t *testing.T) {
	html := `<!DOCTYPE html>
<html>
<head><title>Test Article</title></head>
<body>
<article>
<h1>Hello World</h1>
<p>This is a test paragraph with some content that should be extracted by the readability library.</p>
<p>Another paragraph here with more meaningful text to ensure the extraction works properly.</p>
</article>
<aside>This is sidebar content that should be filtered out.</aside>
</body>
</html>`

	result, err := extractHTMLText([]byte(html))
	if err != nil {
		t.Fatalf("extractHTMLText failed: %v", err)
	}

	if result.Title != "Test Article" {
		t.Errorf("expected title 'Test Article', got %q", result.Title)
	}

	if result.Format != "text" {
		t.Errorf("expected format 'text', got %q", result.Format)
	}

	if len(result.Data) == 0 {
		t.Error("expected non-empty data")
	}

	t.Logf("Title: %s", result.Title)
	t.Logf("Content: %s", string(result.Data))
}
