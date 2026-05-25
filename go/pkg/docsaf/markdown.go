package docsaf

import (
	"bytes"
	"fmt"
	"maps"
	"mime"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"gopkg.in/yaml.v3"
)

// MarkdownProcessor processes Markdown (.md) and MDX (.mdx) content using goldmark.
// It chunks content into sections by headings and extracts YAML frontmatter.
// Sections are merged if they would be too small (under MinTokensPerSection tokens).
type MarkdownProcessor struct {
	// MinTokensPerSection is the minimum token count before splitting into a new section.
	// If 0, defaults to 500 tokens. Set to 1 to split on every heading (original behavior).
	MinTokensPerSection int
}

// estimateTokens provides a rough token count estimate.
// Uses ~1.3 tokens per word as a heuristic for English text.
func estimateTokens(text string) int {
	words := len(strings.Fields(text))
	return int(float64(words) * 1.3)
}

// codeBlockRegex matches fenced code blocks (```...```)
var codeBlockRegex = regexp.MustCompile("(?s)```[^`]*```")

// calculateCodeRatio returns the ratio of code block content to total content (0.0 to 1.0).
// A ratio > 0.85 indicates the section is predominantly code.
func calculateCodeRatio(content string) float64 {
	if len(content) == 0 {
		return 0.0
	}

	matches := codeBlockRegex.FindAllString(content, -1)
	if len(matches) == 0 {
		return 0.0
	}

	codeChars := 0
	for _, m := range matches {
		codeChars += len(m)
	}

	return float64(codeChars) / float64(len(content))
}

// CanProcess returns true for markdown content types or .md/.mdx extensions.
func (mp *MarkdownProcessor) CanProcess(contentType, path string) bool {
	// Check MIME type first
	if strings.Contains(contentType, "text/markdown") ||
		strings.Contains(contentType, "text/x-markdown") {
		return true
	}
	// Fall back to extension
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".md") || strings.HasSuffix(lower, ".mdx")
}

// headingStackEntry represents a heading in the hierarchy stack
type headingStackEntry struct {
	level int
	title string
}

// Process processes markdown content and returns document sections.
func (mp *MarkdownProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	isMDX := strings.HasSuffix(strings.ToLower(path), ".mdx")

	// Extract frontmatter if present
	frontmatter, contentWithoutFrontmatter := extractFrontmatter(content)

	// Parse markdown using goldmark
	md := goldmark.New()
	reader := text.NewReader(contentWithoutFrontmatter)
	doc := md.Parser().Parse(reader)

	var sections []DocumentSection
	var currentSection *DocumentSection
	var contentBuffer bytes.Buffer
	var preambleBuffer bytes.Buffer // Buffer for content before first heading
	seenHeading := false

	// Track heading hierarchy for section_path
	var headingStack []headingStackEntry

	// Walk the AST and extract sections by headings
	err := ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			if heading, ok := n.(*ast.Heading); ok {
				seenHeading = true

				// Save previous section
				if currentSection != nil {
					currentSection.Content = strings.TrimSpace(contentBuffer.String())
					if currentSection.Content != "" {
						sections = append(sections, *currentSection)
					}
					contentBuffer.Reset()
				}

				// Extract heading text
				headingText := extractText(heading, contentWithoutFrontmatter)

				// Update heading stack: pop entries with level >= current
				for len(headingStack) > 0 && headingStack[len(headingStack)-1].level >= heading.Level {
					headingStack = headingStack[:len(headingStack)-1]
				}
				// Push current heading onto stack
				headingStack = append(headingStack, headingStackEntry{
					level: heading.Level,
					title: headingText,
				})

				// Build section path from stack
				sectionPath := make([]string, len(headingStack))
				for i, entry := range headingStack {
					sectionPath[i] = entry.title
				}

				// Use frontmatter title if available for first section (only if no preamble content)
				sectionTitle := headingText
				if len(sections) == 0 && preambleBuffer.Len() == 0 && frontmatter != nil {
					if title, ok := frontmatter["title"].(string); ok && title != "" {
						sectionTitle = title
					}
				}

				// Create new section
				docType := "markdown_section"
				if isMDX {
					docType = "mdx_section"
				}

				metadata := map[string]any{
					"heading_level": heading.Level,
					"is_mdx":        isMDX,
				}
				if frontmatter != nil {
					metadata["frontmatter"] = frontmatter
				}
				if sourceURL != "" {
					metadata["source_url"] = sourceURL
				}

				// Generate URL if baseURL is provided
				url := ""
				if baseURL != "" {
					slug := generateSlug(headingText)
					cleanPath := transformURLPath(path)
					url = baseURL + "/" + cleanPath + "#" + slug
				}

				// Use full section path for ID to ensure uniqueness (e.g., "Overview" may appear multiple times)
				sectionPathID := strings.Join(sectionPath, " > ")

				currentSection = &DocumentSection{
					ID:          generateID(path, sectionPathID),
					FilePath:    path,
					Title:       sectionTitle,
					Type:        docType,
					URL:         url,
					SectionPath: sectionPath,
					Metadata:    metadata,
				}

				// Skip appending heading text to content - it's already the Title
				return ast.WalkSkipChildren, nil
			}

			// Append content - use preambleBuffer before first heading, contentBuffer after
			// Only extract from leaf text nodes to avoid duplicating text from container nodes
			if textNode, ok := n.(*ast.Text); ok {
				targetBuffer := &preambleBuffer
				if seenHeading && currentSection != nil {
					targetBuffer = &contentBuffer
				}
				targetBuffer.Write(textNode.Segment.Value(contentWithoutFrontmatter))
				if textNode.SoftLineBreak() {
					targetBuffer.WriteString("\n")
				}
			}
			// Also extract content from HTML blocks (e.g., MDX components like <Questions>)
			if htmlBlock, ok := n.(*ast.HTMLBlock); ok {
				targetBuffer := &preambleBuffer
				if seenHeading && currentSection != nil {
					targetBuffer = &contentBuffer
				}
				for i := 0; i < htmlBlock.Lines().Len(); i++ {
					line := htmlBlock.Lines().At(i)
					targetBuffer.Write(line.Value(contentWithoutFrontmatter))
				}
			}
			// Extract fenced code blocks with language metadata
			if codeBlock, ok := n.(*ast.FencedCodeBlock); ok {
				targetBuffer := &preambleBuffer
				if seenHeading && currentSection != nil {
					targetBuffer = &contentBuffer
				}
				lang := string(codeBlock.Language(contentWithoutFrontmatter))
				targetBuffer.WriteString("\n```")
				if lang != "" {
					targetBuffer.WriteString(lang)
				}
				targetBuffer.WriteString("\n")
				for i := 0; i < codeBlock.Lines().Len(); i++ {
					line := codeBlock.Lines().At(i)
					targetBuffer.Write(line.Value(contentWithoutFrontmatter))
				}
				targetBuffer.WriteString("```\n")
			}
			// Extract indented code blocks (no language info)
			if codeBlock, ok := n.(*ast.CodeBlock); ok {
				targetBuffer := &preambleBuffer
				if seenHeading && currentSection != nil {
					targetBuffer = &contentBuffer
				}
				targetBuffer.WriteString("\n```\n")
				for i := 0; i < codeBlock.Lines().Len(); i++ {
					line := codeBlock.Lines().At(i)
					targetBuffer.Write(line.Value(contentWithoutFrontmatter))
				}
				targetBuffer.WriteString("```\n")
			}
		}
		return ast.WalkContinue, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse markdown: %w", err)
	}

	// Save last section
	if currentSection != nil {
		currentSection.Content = strings.TrimSpace(contentBuffer.String())
		if currentSection.Content != "" {
			sections = append(sections, *currentSection)
		}
	}

	// Merge preamble content into the first section (if any)
	preambleContent := strings.TrimSpace(preambleBuffer.String())
	if preambleContent != "" && len(sections) > 0 {
		// Prepend preamble content to the first section's content
		sections[0].Content = preambleContent + "\n\n" + sections[0].Content
		sections[0].Content = strings.TrimSpace(sections[0].Content)

		// Mark that this section contains preamble content
		sections[0].Metadata["has_preamble"] = true

		// Use frontmatter title if available for the first section
		if frontmatter != nil {
			if fmTitle, ok := frontmatter["title"].(string); ok && fmTitle != "" {
				sections[0].Title = fmTitle
			}
		}
	}

	// Merge small sections to ensure minimum token count and merge pure-code sections
	// with their preceding prose sections for better search context
	minTokens := mp.MinTokensPerSection
	if minTokens == 0 {
		minTokens = 500 // default
	}
	if len(sections) > 1 {
		sections = mergeSmallerSections(sections, minTokens)
	}

	// If no sections found (no headings), create one section for the entire content
	if len(sections) == 0 {
		docType := "markdown_section"
		if isMDX {
			docType = "mdx_section"
		}

		// Use frontmatter title if available, otherwise use filename
		title := filepath.Base(path)
		if frontmatter != nil {
			if fmTitle, ok := frontmatter["title"].(string); ok && fmTitle != "" {
				title = fmTitle
			}
		}

		metadata := map[string]any{
			"is_mdx":      isMDX,
			"no_headings": true,
		}
		if frontmatter != nil {
			metadata["frontmatter"] = frontmatter
		}
		if sourceURL != "" {
			metadata["source_url"] = sourceURL
		}

		url := ""
		if baseURL != "" {
			cleanPath := transformURLPath(path)
			url = baseURL + "/" + cleanPath
		}

		sections = append(sections, DocumentSection{
			ID:       generateID(path, filepath.Base(path)),
			FilePath: path,
			Title:    title,
			Content:  string(contentWithoutFrontmatter),
			Type:     docType,
			URL:      url,
			Metadata: metadata,
		})
	}

	// Extract questions and associate them with sections
	questions := mp.ExtractQuestionsWithSections(path, sourceURL, content)
	sections = mp.addQuestionsToSections(sections, questions)

	return sections, nil
}

// ExtractQuestionsWithSections extracts questions with section path information.
func (mp *MarkdownProcessor) ExtractQuestionsWithSections(path, sourceURL string, content []byte) []Question {
	var questions []Question

	// Extract frontmatter
	frontmatter, contentWithoutFrontmatter := extractFrontmatter(content)

	// Get document title for context
	context := ""
	if frontmatter != nil {
		if title, ok := frontmatter["title"].(string); ok {
			context = title
		}
	}

	// Extract from frontmatter (applies to the whole document, no section path)
	if frontmatter != nil {
		extractor := &QuestionsExtractor{}
		questions = append(questions, extractor.extractFromFrontmatter(path, sourceURL, frontmatter)...)
	}

	// Extract <Questions> components with section awareness
	questions = append(questions, mp.extractQuestionsWithSectionPaths(path, sourceURL, context, contentWithoutFrontmatter)...)

	return questions
}

// extractQuestionsWithSectionPaths extracts questions from <Questions> components,
// tracking which section each component belongs to based on preceding headings.
func (mp *MarkdownProcessor) extractQuestionsWithSectionPaths(path, sourceURL, context string, content []byte) []Question {
	var questions []Question

	// Parse markdown to find headings and their positions
	md := goldmark.New()
	reader := text.NewReader(content)
	doc := md.Parser().Parse(reader)

	// Build a list of heading positions and their section paths
	type headingInfo struct {
		startPos    int
		level       int
		sectionPath []string
	}
	var headings []headingInfo
	var headingStack []headingStackEntry

	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			if heading, ok := n.(*ast.Heading); ok {
				headingText := extractText(heading, content)

				// Update heading stack
				for len(headingStack) > 0 && headingStack[len(headingStack)-1].level >= heading.Level {
					headingStack = headingStack[:len(headingStack)-1]
				}
				headingStack = append(headingStack, headingStackEntry{
					level: heading.Level,
					title: headingText,
				})

				// Build section path
				sectionPath := make([]string, len(headingStack))
				for i, entry := range headingStack {
					sectionPath[i] = entry.title
				}

				// Get the position of this heading in the content
				lines := heading.Lines()
				if lines.Len() > 0 {
					startPos := lines.At(0).Start
					headings = append(headings, headingInfo{
						startPos:    startPos,
						level:       heading.Level,
						sectionPath: sectionPath,
					})
				}
			}
		}
		return ast.WalkContinue, nil
	})

	// Find all <Questions> components and their positions
	contentStr := string(content)
	matches := questionsComponentRegex.FindAllStringSubmatchIndex(contentStr, -1)

	for _, match := range matches {
		if len(match) < 4 {
			continue
		}

		componentStart := match[0]
		componentContentStart := match[2]
		componentContentEnd := match[3]
		componentContent := contentStr[componentContentStart:componentContentEnd]

		// Find the section path for this component position
		var sectionPath []string
		for i := len(headings) - 1; i >= 0; i-- {
			if headings[i].startPos < componentStart {
				sectionPath = headings[i].sectionPath
				break
			}
		}

		// Parse list items
		lines := strings.SplitSeq(componentContent, "\n")
		for line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") {
				questionText := strings.TrimSpace(line[2:])
				if questionText != "" {
					questions = append(questions, Question{
						ID:          generateID(path, "mdx_component_q_"+questionText),
						Text:        questionText,
						SourcePath:  path,
						SourceURL:   sourceURL,
						SourceType:  "mdx_component",
						Context:     context,
						SectionPath: sectionPath,
					})
				}
			}
		}
	}

	return questions
}

// addQuestionsToSections associates questions with their containing sections.
func (mp *MarkdownProcessor) addQuestionsToSections(sections []DocumentSection, questions []Question) []DocumentSection {
	if len(questions) == 0 {
		return sections
	}

	// Initialize Questions slices
	for i := range sections {
		sections[i].Questions = nil
	}

	for _, q := range questions {
		bestIdx := -1
		bestMatchLen := -1

		for i, section := range sections {
			matchLen := mp.matchSectionPath(section.SectionPath, q.SectionPath)
			if matchLen > bestMatchLen {
				bestMatchLen = matchLen
				bestIdx = i
			}
		}

		if bestIdx >= 0 {
			sections[bestIdx].Questions = append(sections[bestIdx].Questions, q.Text)
		} else if len(sections) > 0 {
			// If no match (e.g., frontmatter questions), add to first section
			sections[0].Questions = append(sections[0].Questions, q.Text)
		}
	}

	return sections
}

// matchSectionPath returns the length of the matching prefix between two section paths.
func (mp *MarkdownProcessor) matchSectionPath(sectionPath, questionPath []string) int {
	if len(questionPath) == 0 {
		return 0
	}
	if len(sectionPath) == 0 {
		return -1
	}

	matchLen := 0
	minLen := min(len(questionPath), len(sectionPath))

	for i := range minLen {
		if sectionPath[i] == questionPath[i] {
			matchLen++
		} else {
			break
		}
	}

	if matchLen == 0 {
		return -1
	}

	return matchLen
}

// extractText extracts text content from an AST node.
func extractText(node ast.Node, source []byte) string {
	var buf bytes.Buffer
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		buf.Write(child.Text(source)) //nolint:staticcheck // ast.Node.Text is the standard way to extract text
	}
	return strings.TrimSpace(buf.String())
}

// extractFrontmatter extracts YAML frontmatter from markdown content.
func extractFrontmatter(content []byte) (map[string]any, []byte) {
	if !bytes.HasPrefix(content, []byte("---\n")) && !bytes.HasPrefix(content, []byte("---\r\n")) {
		return nil, content
	}

	remaining := content[4:]
	endIdx := bytes.Index(remaining, []byte("\n---\n"))
	if endIdx == -1 {
		endIdx = bytes.Index(remaining, []byte("\n---\r\n"))
		if endIdx == -1 {
			return nil, content
		}
	}

	frontmatterYAML := remaining[:endIdx]
	var frontmatter map[string]any
	if err := yaml.Unmarshal(frontmatterYAML, &frontmatter); err != nil {
		return nil, content
	}

	contentStart := 4 + endIdx + 5
	if contentStart >= len(content) {
		return frontmatter, []byte{}
	}
	return frontmatter, content[contentStart:]
}

// GenerateID creates a deterministic, path-based ID for a section.
// The ID format is "path#slug" which sorts lexicographically by file path,
// enabling streaming LinearMerge without buffering all records for sorting.
// This is exported so SDK users can compose with it via WithIDTransform.
func GenerateID(path, identifier string) string {
	slug := generateSlug(identifier)
	if slug == "" {
		slug = "doc"
	}
	return path + "#" + slug
}

// generateID is the internal alias used by content processors.
func generateID(path, identifier string) string {
	return GenerateID(path, identifier)
}

// generateSlug creates a GitHub-style URL slug from a heading.
func generateSlug(heading string) string {
	slug := strings.ToLower(heading)

	var result strings.Builder
	for _, ch := range slug {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			result.WriteRune(ch)
		} else if ch == ' ' || ch == '-' || ch == '_' {
			result.WriteRune('-')
		}
	}

	slugStr := result.String()
	slugStr = strings.ReplaceAll(slugStr, "--", "-")
	slugStr = strings.Trim(slugStr, "-")

	return slugStr
}

// mergeSmallerSections merges consecutive sections that are below the minimum token threshold.
// It combines small sections with the next section to ensure each chunk has enough context.
// It also merges sections that are predominantly code (>85% code) with the previous section,
// since explanatory prose typically precedes code examples.
func mergeSmallerSections(sections []DocumentSection, minTokens int) []DocumentSection {
	if len(sections) <= 1 {
		return sections
	}

	var merged []DocumentSection
	var accumulator *DocumentSection
	var accumulatedContent strings.Builder

	for i, section := range sections {
		codeRatio := calculateCodeRatio(section.Content)
		isPureCode := codeRatio > 0.85

		// If this section is predominantly code and we have a previous merged section,
		// append it to the previous section instead of starting fresh.
		// This ensures code examples stay with their explanatory prose.
		if isPureCode && len(merged) > 0 && accumulator == nil {
			// Append to the last merged section
			lastIdx := len(merged) - 1
			merged[lastIdx].Content = merged[lastIdx].Content + "\n\n" + section.Content
			merged[lastIdx].Content = strings.TrimSpace(merged[lastIdx].Content)
			// Mark that this section contains code
			if merged[lastIdx].Metadata == nil {
				merged[lastIdx].Metadata = make(map[string]any)
			}
			merged[lastIdx].Metadata["has_code_blocks"] = true
			continue
		}

		if accumulator == nil {
			// Start a new accumulator
			accumulator = &DocumentSection{
				ID:          section.ID,
				FilePath:    section.FilePath,
				Title:       section.Title,
				Type:        section.Type,
				URL:         section.URL,
				SectionPath: section.SectionPath,
				Metadata:    copyMetadata(section.Metadata),
			}
			accumulatedContent.WriteString(section.Content)
		} else {
			// Append to accumulator
			accumulatedContent.WriteString("\n\n")
			accumulatedContent.WriteString(section.Content)
		}

		// Track code blocks in metadata
		if codeRatio > 0 && accumulator.Metadata != nil {
			accumulator.Metadata["has_code_blocks"] = true
		}

		accumulatedTokens := estimateTokens(accumulatedContent.String())
		accumulatedCodeRatio := calculateCodeRatio(accumulatedContent.String())

		// Flush if we've reached the threshold AND the section isn't predominantly code,
		// or if this is the last section
		shouldFlush := (accumulatedTokens >= minTokens && accumulatedCodeRatio <= 0.85) || i == len(sections)-1

		if shouldFlush {
			accumulator.Content = strings.TrimSpace(accumulatedContent.String())
			// Record the final code ratio in metadata
			if accumulator.Metadata != nil && accumulatedCodeRatio > 0 {
				accumulator.Metadata["code_ratio"] = accumulatedCodeRatio
			}
			merged = append(merged, *accumulator)
			accumulator = nil
			accumulatedContent.Reset()
		}
	}

	return merged
}

// copyMetadata creates a shallow copy of the metadata map.
func copyMetadata(m map[string]any) map[string]any {
	if m == nil {
		return make(map[string]any)
	}
	copy := make(map[string]any, len(m))
	maps.Copy(copy, m)
	return copy
}

// transformURLPath removes .md/.mdx extensions from the path for cleaner URLs.
func transformURLPath(path string) string {
	path = strings.TrimSuffix(path, ".mdx")
	path = strings.TrimSuffix(path, ".md")
	return path
}

// ExtractQuestions extracts questions from markdown/MDX content.
// It looks for questions in:
// 1. Frontmatter "questions" field
// 2. <Questions> MDX components inline in the content
func (mp *MarkdownProcessor) ExtractQuestions(path, sourceURL string, content []byte) []Question {
	extractor := &QuestionsExtractor{}

	// Extract frontmatter
	frontmatter, contentWithoutFrontmatter := extractFrontmatter(content)

	return extractor.ExtractFromMDXContent(path, sourceURL, contentWithoutFrontmatter, frontmatter)
}

// DetectContentType detects the MIME type from a file path.
func DetectContentType(path string, content []byte) string {
	ext := filepath.Ext(path)
	if ext != "" {
		mimeType := mime.TypeByExtension(ext)
		if mimeType != "" {
			return mimeType
		}
	}

	switch strings.ToLower(ext) {
	case ".md", ".markdown", ".mdx":
		return "text/markdown"
	case ".html", ".htm":
		return "text/html"
	case ".pdf":
		return "application/pdf"
	case ".yaml", ".yml":
		return "application/x-yaml"
	case ".json":
		return "application/json"
	case ".txt":
		return "text/plain"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	}

	return "application/octet-stream"
}
