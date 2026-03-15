package docsaf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// HTMLProcessor processes HTML (.html, .htm) content using goquery.
// It chunks content into sections by headings and extracts metadata from the document head.
type HTMLProcessor struct{}

// CanProcess returns true for HTML content types or .html/.htm extensions.
func (hp *HTMLProcessor) CanProcess(contentType, path string) bool {
	// Check MIME type first
	if strings.Contains(contentType, "text/html") {
		return true
	}
	// Fall back to extension
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".html") || strings.HasSuffix(lower, ".htm")
}

// Process processes HTML content and returns document sections.
// Questions found in the HTML are associated with their containing sections.
func (hp *HTMLProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	// Parse HTML with goquery
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	// Extract document-level metadata
	docMetadata := hp.extractMetadata(doc)
	if sourceURL != "" {
		docMetadata["source_url"] = sourceURL
	}

	// Extract sections by headings
	sections := hp.extractSections(doc, path, baseURL, docMetadata)

	// Extract questions and associate them with sections
	questions := hp.ExtractQuestions(path, sourceURL, content)
	sections = hp.addQuestionsToSections(sections, questions)

	return sections, nil
}

// addQuestionsToSections associates questions with their containing sections.
// Questions are matched to sections based on their SectionPath.
func (hp *HTMLProcessor) addQuestionsToSections(sections []DocumentSection, questions []Question) []DocumentSection {
	if len(questions) == 0 {
		return sections
	}

	// Build a map of section path (as string) to section index
	// Use the most specific match (longest matching prefix)
	for i := range sections {
		sections[i].Questions = nil // Initialize to avoid appending to nil
	}

	for _, q := range questions {
		// Find the best matching section for this question
		bestIdx := -1
		bestMatchLen := -1

		for i, section := range sections {
			matchLen := matchSectionPath(section.SectionPath, q.SectionPath)
			if matchLen > bestMatchLen {
				bestMatchLen = matchLen
				bestIdx = i
			}
		}

		// If we found a matching section, add the question text to it
		if bestIdx >= 0 {
			sections[bestIdx].Questions = append(sections[bestIdx].Questions, q.Text)
		} else if len(sections) > 0 {
			// If no match found but we have sections, add to the first section
			// (this handles questions before any heading)
			sections[0].Questions = append(sections[0].Questions, q.Text)
		}
	}

	return sections
}

// matchSectionPath returns the length of the matching prefix between two section paths.
// Returns -1 if the paths don't match at all, 0 if both are empty,
// or the number of matching elements from the start.
func matchSectionPath(sectionPath, questionPath []string) int {
	// If question has no section path, it matches any section
	if len(questionPath) == 0 {
		return 0
	}

	// If section has no path but question does, they don't match well
	if len(sectionPath) == 0 {
		return -1
	}

	// Count matching elements from the start
	matchLen := 0
	minLen := min(len(questionPath), len(sectionPath))

	for i := range minLen {
		if sectionPath[i] == questionPath[i] {
			matchLen++
		} else {
			break
		}
	}

	// If no elements match, return -1
	if matchLen == 0 {
		return -1
	}

	// For a full match, the section path should be equal to or a prefix of the question path
	// This ensures questions in deeper sections match those sections
	if matchLen == len(sectionPath) && matchLen <= len(questionPath) {
		return matchLen
	}

	return matchLen
}

// extractMetadata extracts metadata from the HTML <head> section.
func (hp *HTMLProcessor) extractMetadata(doc *goquery.Document) map[string]any {
	metadata := make(map[string]any)

	if title := doc.Find("title").First().Text(); title != "" {
		metadata["title"] = strings.TrimSpace(title)
	}

	doc.Find("meta").Each(func(i int, s *goquery.Selection) {
		if name, exists := s.Attr("name"); exists {
			if content, exists := s.Attr("content"); exists {
				metadata["meta_"+name] = content
			}
		}

		if property, exists := s.Attr("property"); exists {
			if content, exists := s.Attr("content"); exists {
				if strings.HasPrefix(property, "og:") || strings.HasPrefix(property, "twitter:") {
					metadata[property] = content
				}
			}
		}
	})

	return metadata
}

// extractSections extracts document sections from the HTML document.
func (hp *HTMLProcessor) extractSections(doc *goquery.Document, path, baseURL string,
	docMetadata map[string]any) []DocumentSection {

	var sections []DocumentSection
	slugs := newSlugCounter()

	headings := doc.Find("h1, h2, h3, h4, h5, h6")

	if headings.Length() == 0 {
		return []DocumentSection{hp.createFullDocSection(doc, path, baseURL, docMetadata)}
	}

	// Track heading hierarchy for section_path
	var headingStack []headingStackEntry

	headings.Each(func(i int, heading *goquery.Selection) {
		headingText := strings.TrimSpace(heading.Text())
		headingLevel := hp.getHeadingLevel(heading)
		headingID := heading.AttrOr("id", "")

		// Update heading stack: pop entries with level >= current
		for len(headingStack) > 0 && headingStack[len(headingStack)-1].level >= headingLevel {
			headingStack = headingStack[:len(headingStack)-1]
		}
		// Push current heading onto stack
		headingStack = append(headingStack, headingStackEntry{
			level: headingLevel,
			title: headingText,
		})

		// Build section path from stack
		sectionPath := make([]string, len(headingStack))
		for j, entry := range headingStack {
			sectionPath[j] = entry.title
		}

		slug := headingID
		if slug == "" {
			slug = generateSlug(headingText)
		}
		slug = slugs.unique(slug)

		content := hp.extractSectionContent(heading)

		title := headingText
		if i == 0 {
			if docTitle, ok := docMetadata["title"].(string); ok && docTitle != "" {
				title = docTitle
			}
		}

		url := ""
		if baseURL != "" {
			cleanPath := transformHTMLPath(path)
			url = baseURL + "/" + cleanPath + "#" + slug
		}

		// Use full section path for ID to ensure uniqueness (e.g., "Overview" may appear multiple times)
		sectionPathID := strings.Join(sectionPath, " > ")

		sections = append(sections, DocumentSection{
			ID:          generateID(path, sectionPathID),
			FilePath:    path,
			Title:       title,
			Content:     content,
			Type:        "html_section",
			URL:         url,
			SectionPath: sectionPath,
			Metadata: hp.mergeSectionMetadata(docMetadata, map[string]any{
				"heading_level": headingLevel,
				"heading_id":    headingID,
			}),
		})
	})

	return sections
}

// extractSectionContent extracts text content between a heading and the next heading.
func (hp *HTMLProcessor) extractSectionContent(heading *goquery.Selection) string {
	var content strings.Builder

	heading.NextAll().EachWithBreak(func(i int, s *goquery.Selection) bool {
		tagName := goquery.NodeName(s)

		if hp.isHeading(tagName) {
			return false
		}

		if tagName == "script" || tagName == "style" || tagName == "noscript" {
			return true
		}

		text := strings.TrimSpace(s.Text())
		if text != "" {
			content.WriteString(text)
			content.WriteString("\n\n")
		}

		return true
	})

	return strings.TrimSpace(content.String())
}

// createFullDocSection creates a single section for documents without headings.
func (hp *HTMLProcessor) createFullDocSection(doc *goquery.Document, path, baseURL string,
	docMetadata map[string]any) DocumentSection {

	docClone := doc.Clone()
	docClone.Find("script, style, noscript").Remove()

	content := ""
	if body := docClone.Find("body").First(); body.Length() > 0 {
		content = strings.TrimSpace(body.Text())
	} else {
		content = strings.TrimSpace(docClone.Text())
	}

	title := filepath.Base(path)
	if docTitle, ok := docMetadata["title"].(string); ok && docTitle != "" {
		title = docTitle
	}

	url := ""
	if baseURL != "" {
		url = baseURL + "/" + transformHTMLPath(path)
	}

	return DocumentSection{
		ID:       generateID(path, "document"),
		FilePath: path,
		Title:    title,
		Content:  content,
		Type:     "html_document",
		URL:      url,
		Metadata: hp.mergeSectionMetadata(docMetadata, map[string]any{
			"no_headings": true,
		}),
	}
}

func (hp *HTMLProcessor) isHeading(tagName string) bool {
	return tagName == "h1" || tagName == "h2" || tagName == "h3" ||
		tagName == "h4" || tagName == "h5" || tagName == "h6"
}

func (hp *HTMLProcessor) getHeadingLevel(s *goquery.Selection) int {
	tagName := goquery.NodeName(s)
	if len(tagName) == 2 && tagName[0] == 'h' && tagName[1] >= '1' && tagName[1] <= '6' {
		return int(tagName[1] - '0')
	}
	return 0
}

func (hp *HTMLProcessor) mergeSectionMetadata(docMeta, sectionMeta map[string]any) map[string]any {
	merged := make(map[string]any)
	maps.Copy(merged, docMeta)
	maps.Copy(merged, sectionMeta)
	return merged
}

// transformHTMLPath removes .html/.htm extensions from the path for cleaner URLs.
func transformHTMLPath(path string) string {
	path = strings.TrimSuffix(path, ".html")
	path = strings.TrimSuffix(path, ".htm")
	return path
}

// slugCounter tracks slug usage to handle duplicate heading slugs.
type slugCounter struct {
	counts map[string]int
}

func newSlugCounter() *slugCounter {
	return &slugCounter{counts: make(map[string]int)}
}

func (sc *slugCounter) unique(slug string) string {
	count, exists := sc.counts[slug]
	sc.counts[slug] = count + 1

	if !exists {
		return slug
	}
	return fmt.Sprintf("%s-%d", slug, count)
}

// ExtractQuestions extracts questions from HTML content.
// It looks for questions in:
// 1. data-docsaf-questions attributes (JSON array of strings or objects)
// 2. Elements with class "docsaf-questions" (extracts li text content)
// Questions are associated with the section they appear in based on preceding headings.
func (hp *HTMLProcessor) ExtractQuestions(path, sourceURL string, content []byte) []Question {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(content))
	if err != nil {
		return nil
	}

	var questions []Question

	// Get document title for context
	context := ""
	if title := doc.Find("title").First().Text(); title != "" {
		context = strings.TrimSpace(title)
	}

	// Track heading hierarchy as we traverse elements in document order
	var headingStack []headingStackEntry

	// Find all headings and question elements in document order
	// This ensures questions are associated with the correct section
	doc.Find("h1, h2, h3, h4, h5, h6, [data-docsaf-questions], .docsaf-questions").Each(func(i int, s *goquery.Selection) {
		tagName := goquery.NodeName(s)

		// If it's a heading, update the heading stack
		if hp.isHeading(tagName) {
			headingLevel := hp.getHeadingLevel(s)
			headingText := strings.TrimSpace(s.Text())

			// Pop entries with level >= current
			for len(headingStack) > 0 && headingStack[len(headingStack)-1].level >= headingLevel {
				headingStack = headingStack[:len(headingStack)-1]
			}
			// Push current heading onto stack
			headingStack = append(headingStack, headingStackEntry{
				level: headingLevel,
				title: headingText,
			})
			return
		}

		// Build current section path from heading stack
		sectionPath := make([]string, len(headingStack))
		for j, entry := range headingStack {
			sectionPath[j] = entry.title
		}

		// Check for data-docsaf-questions attribute
		if attr, exists := s.Attr("data-docsaf-questions"); exists {
			questions = append(questions, hp.parseDataAttribute(path, sourceURL, context, sectionPath, attr)...)
		}

		// Check for docsaf-questions class
		if s.HasClass("docsaf-questions") {
			s.Find("li").Each(func(j int, li *goquery.Selection) {
				questionText := strings.TrimSpace(li.Text())
				if questionText != "" {
					questions = append(questions, Question{
						ID:          generateID(path, "html_class_q_"+questionText),
						Text:        questionText,
						SourcePath:  path,
						SourceURL:   sourceURL,
						SourceType:  "html_class",
						Context:     context,
						SectionPath: sectionPath,
					})
				}
			})
		}
	})

	return questions
}

// parseDataAttribute parses questions from a data-docsaf-questions JSON attribute
// and associates them with the given section path.
func (hp *HTMLProcessor) parseDataAttribute(path, sourceURL, context string, sectionPath []string, attr string) []Question {
	var questions []Question

	// Try to parse as JSON array
	var items []any
	if err := json.Unmarshal([]byte(attr), &items); err != nil {
		return questions
	}

	for _, item := range items {
		var questionText string
		var metadata map[string]any

		switch q := item.(type) {
		case string:
			questionText = strings.TrimSpace(q)
		case map[string]any:
			if text, ok := q["text"].(string); ok {
				questionText = strings.TrimSpace(text)
			}
			// Copy other fields as metadata
			metadata = make(map[string]any)
			for k, v := range q {
				if k != "text" {
					metadata[k] = v
				}
			}
		}

		if questionText != "" {
			questions = append(questions, Question{
				ID:          generateID(path, "html_data_q_"+questionText),
				Text:        questionText,
				SourcePath:  path,
				SourceURL:   sourceURL,
				SourceType:  "html_data_attribute",
				Context:     context,
				SectionPath: sectionPath,
				Metadata:    metadata,
			})
		}
	}

	return questions
}
