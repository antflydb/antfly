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

package template

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/alpkeskin/gotoon"
	"github.com/jellydator/ttlcache/v3"
	"github.com/mbleigh/raymond"
	"github.com/samber/go-singleflightx"
)

var (
	// templateCache is a TTL cache for parsed templates
	templateCache     *ttlcache.Cache[string, *template.Template]
	templateCacheOnce sync.Once
	templateSfg       singleflightx.Group[string, *template.Template]

	// handlebarsCache is a TTL cache for parsed handlebars templates
	handlebarsCache     *ttlcache.Cache[string, *raymond.Template]
	handlebarsCacheOnce sync.Once
	handlebarsSfg       singleflightx.Group[string, *raymond.Template]

	// Default TTL for cached templates
	defaultTemplateTTL = 5 * time.Minute
)

// scrubHtml removes script and style tags from HTML and returns plain text.
// This is useful for extracting clean text content from HTML documents.
// It also adds newlines after block-level elements to maintain readability.
func scrubHtml(html string) string {
	// Create a new reader from the HTML string
	r := strings.NewReader(html)

	// Parse the HTML
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		// Log the error but don't crash - return empty string
		log.Printf("Error parsing HTML in scrubHtml helper: %v", err)
		return ""
	}

	// Remove script and style tags
	doc.Find("script, style").Remove()

	// Add newlines after block-level elements to maintain readability
	blockElements := []string{
		"p", "div", "h1", "h2", "h3", "h4", "h5", "h6",
		"li", "tr", "br", "hr", "blockquote", "pre",
		"article", "section", "header", "footer", "nav",
		"aside", "main", "figure", "figcaption",
	}

	for _, elem := range blockElements {
		doc.Find(elem).Each(func(i int, s *goquery.Selection) {
			// Add a newline marker after each block element
			s.AfterHtml("\n")
		})
	}

	// Get the text from the entire document
	cleanText := doc.Text()

	// Trim surrounding whitespace
	return strings.TrimSpace(cleanText)
}

// init registers custom helpers for both Go templates and Handlebars
func init() {
	// Register the scrub helper for Handlebars (raymond)
	raymond.RegisterHelper("scrubHtml", scrubHtml)

	// Register the eq helper for equality comparison
	// Usage: {{#if (eq @key "value")}}...{{/if}}
	raymond.RegisterHelper("eq", func(a, b any) bool {
		return a == b
	})

	// Register the media helper for GenKit dotprompt compatibility
	// Usage: {{media url=imageDataURI}} or {{media url=this}}
	// Returns: <<<dotprompt:media:url data:image/png;base64,...>>>
	raymond.RegisterHelper("media", func(options *raymond.Options) raymond.SafeString {
		url := options.HashStr("url")
		if url == "" {
			return raymond.SafeString("")
		}
		// Return GenKit dotprompt media directive
		return raymond.SafeString(fmt.Sprintf("<<<dotprompt:media:url %s>>>", url))
	})

	// Register the encodeToon helper for TOON format encoding
	// Usage: {{encodeToon this.fields}} or {{encodeToon this.fields lengthMarker=false indent=4}}
	// Options:
	//   - lengthMarker (bool): Add # prefix to array counts (default: true)
	//   - indent (int): Indentation spacing (default: 2)
	//   - delimiter (string): Field separator for arrays (default: none)
	raymond.RegisterHelper("encodeToon", func(value any, options *raymond.Options) raymond.SafeString {
		// Build gotoon encoding options
		var opts []gotoon.EncodeOption

		// Parse lengthMarker option (default: true)
		lengthMarker := true
		if lm, ok := options.Hash()["lengthMarker"].(bool); ok {
			lengthMarker = lm
		}
		if lengthMarker {
			opts = append(opts, gotoon.WithLengthMarker())
		}

		// Parse indent option (default: 2)
		indent := 2
		if i, ok := options.Hash()["indent"].(int); ok {
			indent = i
		}
		opts = append(opts, gotoon.WithIndent(indent))

		// Parse delimiter option (optional)
		if delim, ok := options.Hash()["delimiter"].(string); ok {
			opts = append(opts, gotoon.WithDelimiter(delim))
		}

		// Encode the value using gotoon
		encoded, err := gotoon.Encode(value, opts...)
		if err != nil {
			log.Printf("Error encoding TOON in encodeToon helper: %v", err)
			return raymond.SafeString("")
		}

		return raymond.SafeString(encoded)
	})
}

// initTemplateCache initializes the template cache with default settings
func initTemplateCache() {
	templateCacheOnce.Do(func() {
		templateCache = ttlcache.New(
			ttlcache.WithTTL[string, *template.Template](defaultTemplateTTL),
			ttlcache.WithCapacity[string, *template.Template](1000),
		)

		// Start the cache cleanup goroutine
		go templateCache.Start()
	})
}

// initHandlebarsCache initializes the handlebars template cache with default settings
func initHandlebarsCache() {
	handlebarsCacheOnce.Do(func() {
		handlebarsCache = ttlcache.New(
			ttlcache.WithTTL[string, *raymond.Template](defaultTemplateTTL),
			ttlcache.WithCapacity[string, *raymond.Template](1000),
		)

		// Start the cache cleanup goroutine
		go handlebarsCache.Start()
	})
}

// SetTemplateCacheTTL allows configuration of the template cache TTL
func SetTemplateCacheTTL(ttl time.Duration) {
	defaultTemplateTTL = ttl
	// Re-initialize cache with new TTL if it was already created
	if templateCache != nil {
		templateCache.Stop()
		templateCacheOnce = sync.Once{}
		initTemplateCache()
	}
	if handlebarsCache != nil {
		handlebarsCache.Stop()
		handlebarsCacheOnce = sync.Once{}
		initHandlebarsCache()
	}
}

// ClearTemplateCache clears all cached templates (both Go templates and Handlebars)
func ClearTemplateCache() {
	if templateCache != nil {
		templateCache.DeleteAll()
	}
	if handlebarsCache != nil {
		handlebarsCache.DeleteAll()
	}
}

// Render takes a Handlebars template string and a map of values, then renders the template
// with the provided context. Templates are cached to improve performance.
// This is now the default template implementation using Handlebars.
func Render(tmplStr string, context map[string]any) (string, error) {
	return RenderHandlebars(tmplStr, context)
}

// RenderGoTemplate takes a Go template string and a map of values, then renders the template
// with the provided context. Templates are cached to improve performance.
// This function is maintained for backward compatibility but Handlebars is now the default.
func RenderGoTemplate(tmplStr string, context map[string]any) (string, error) {
	// Ensure cache is initialized
	initTemplateCache()

	// Try to get template from cache
	item := templateCache.Get(tmplStr)
	var tmpl *template.Template

	if item == nil {
		// Parse the template if not in cache
		var err error
		tmpl, err, _ = templateSfg.Do(tmplStr, func() (*template.Template, error) {
			// Parse the template string with custom functions
			tmpl, err = template.New("template").Funcs(template.FuncMap{
				"scrubHtml": scrubHtml,
			}).Parse(tmplStr)
			if err != nil {
				return nil, err
			}
			tmpl = tmpl.Option("missingkey=zero")

			// Store in cache
			templateCache.Set(tmplStr, tmpl, defaultTemplateTTL)
			return tmpl, nil
		})
		if err != nil {
			return "", fmt.Errorf("failed to parse template: %w", err)
		}
	} else {
		tmpl = item.Value()
	}

	// Create a buffer to capture the output
	var buf bytes.Buffer

	// Execute the template with the provided context
	if err := tmpl.Execute(&buf, context); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}
	return buf.String(), nil
}

// RenderNoCache renders a Handlebars template without using the cache.
// Useful for one-off templates or when caching is not desired.
// This is now the default template implementation using Handlebars.
func RenderNoCache(tmplStr string, context map[string]any) (string, error) {
	return RenderHandlebarsNoCache(tmplStr, context)
}

// RenderGoTemplateNoCache renders a Go template without using the cache.
// Useful for one-off templates or when caching is not desired.
// This function is maintained for backward compatibility but Handlebars is now the default.
func RenderGoTemplateNoCache(tmplStr string, context map[string]any) (string, error) {
	// Parse the template with custom functions
	tmpl, err := template.New("template").Funcs(template.FuncMap{
		"scrubHtml": scrubHtml,
	}).Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	// Create a buffer to capture the output
	var buf bytes.Buffer

	// Execute the template with the provided context
	if err := tmpl.Execute(&buf, context); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// RenderHandlebars takes a handlebars template string and a map of values,
// then renders the template with the provided context. Templates are cached to improve performance.
func RenderHandlebars(tmplStr string, context map[string]any) (string, error) {
	// Ensure cache is initialized
	initHandlebarsCache()

	// Try to get template from cache
	item := handlebarsCache.Get(tmplStr)
	var tmpl *raymond.Template

	if item == nil {
		// Parse the template if not in cache
		var err error
		tmpl, err, _ = handlebarsSfg.Do(tmplStr, func() (*raymond.Template, error) {
			// Parse the handlebars template string
			tmpl, err := raymond.Parse(tmplStr)
			if err != nil {
				return nil, err
			}

			// Store in cache
			handlebarsCache.Set(tmplStr, tmpl, defaultTemplateTTL)
			return tmpl, nil
		})
		if err != nil {
			return "", fmt.Errorf("failed to parse handlebars template: %w", err)
		}
	} else {
		tmpl = item.Value()
	}

	// Execute the template with the provided context
	result, err := tmpl.Exec(context)
	if err != nil {
		return "", fmt.Errorf("failed to execute handlebars template: %w", err)
	}

	return result, nil
}

// RenderHandlebarsNoCache renders a handlebars template without using the cache.
// Useful for one-off templates or when caching is not desired.
func RenderHandlebarsNoCache(tmplStr string, context map[string]any) (string, error) {
	// Parse the handlebars template
	tmpl, err := raymond.Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse handlebars template: %w", err)
	}

	// Execute the template with the provided context
	result, err := tmpl.Exec(context)
	if err != nil {
		return "", fmt.Errorf("failed to execute handlebars template: %w", err)
	}

	return result, nil
}
