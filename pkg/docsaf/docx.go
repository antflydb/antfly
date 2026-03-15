package docsaf

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"maps"
	"path/filepath"
	"strings"
)

// DocxProcessor processes Microsoft Word (.docx) files.
// It extracts text with heading-aware chunking, creating one section per heading.
type DocxProcessor struct{}

// CanProcess returns true for DOCX content types or .docx extensions.
func (dp *DocxProcessor) CanProcess(contentType, path string) bool {
	if strings.Contains(contentType, "application/vnd.openxmlformats-officedocument.wordprocessingml.document") {
		return true
	}
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".docx")
}

// Process extracts text from a DOCX file and returns document sections.
// Sections are chunked by heading styles (Heading1-Heading9).
func (dp *DocxProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	zr, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, fmt.Errorf("failed to open DOCX archive: %w", err)
	}

	docMetadata := extractOOXMLMetadata(zr, path)
	if sourceURL != "" {
		docMetadata["source_url"] = sourceURL
	}

	docXML, err := readZipFile(zr, "word/document.xml")
	if err != nil {
		return nil, fmt.Errorf("failed to read document.xml: %w", err)
	}

	paragraphs, err := parseDocxParagraphs(docXML)
	if err != nil {
		return nil, fmt.Errorf("failed to parse document.xml: %w", err)
	}

	sections := dp.buildSections(paragraphs, path, baseURL, docMetadata)
	return sections, nil
}

// docxParagraph holds the extracted text and optional heading level for a paragraph.
type docxParagraph struct {
	text         string
	headingLevel int // 0 = body text, 1-9 = heading level
}

// parseDocxParagraphs parses word/document.xml and extracts paragraphs with heading info.
func parseDocxParagraphs(data []byte) ([]docxParagraph, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	var paragraphs []docxParagraph

	// State tracking for XML traversal
	var inParagraph bool
	var inRun bool
	var inStyle bool
	var currentText strings.Builder
	var currentLevel int

	for {
		tok, err := decoder.Token()
		if err != nil {
			break
		}

		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "p":
				if t.Name.Space == nsWordprocessingML || t.Name.Space == "" {
					inParagraph = true
					currentText.Reset()
					currentLevel = 0
				}
			case "r":
				if inParagraph {
					inRun = true
				}
			case "pStyle":
				if inParagraph {
					inStyle = true
					for _, attr := range t.Attr {
						if attr.Name.Local == "val" {
							currentLevel = parseHeadingLevel(attr.Value)
						}
					}
				}
			case "tab":
				if inRun {
					currentText.WriteString("\t")
				}
			case "br":
				if inRun {
					currentText.WriteString("\n")
				}
			}

		case xml.EndElement:
			switch t.Name.Local {
			case "p":
				if inParagraph {
					text := strings.TrimSpace(currentText.String())
					if text != "" || currentLevel > 0 {
						paragraphs = append(paragraphs, docxParagraph{
							text:         text,
							headingLevel: currentLevel,
						})
					}
					inParagraph = false
				}
			case "r":
				inRun = false
			case "pStyle":
				inStyle = false
			}

		case xml.CharData:
			if inRun && !inStyle {
				currentText.Write(t)
			}
		}
	}

	return paragraphs, nil
}

// buildSections groups paragraphs into sections by heading, following the same
// heading stack pattern as HTMLProcessor.
func (dp *DocxProcessor) buildSections(paragraphs []docxParagraph, path, baseURL string,
	docMetadata map[string]any) []DocumentSection {

	// Check if any headings exist
	hasHeadings := false
	for _, p := range paragraphs {
		if p.headingLevel > 0 {
			hasHeadings = true
			break
		}
	}

	if !hasHeadings {
		return dp.createFullDocSection(paragraphs, path, baseURL, docMetadata)
	}

	var sections []DocumentSection
	var headingStack []headingStackEntry
	slugs := newSlugCounter()

	var currentContent strings.Builder
	var currentHeading string
	var currentLevel int

	flushSection := func() {
		content := strings.TrimSpace(currentContent.String())
		if content == "" && currentHeading == "" {
			return
		}

		sectionPath := make([]string, len(headingStack))
		for i, entry := range headingStack {
			sectionPath[i] = entry.title
		}

		title := currentHeading
		if title == "" {
			if docTitle, ok := docMetadata["title"].(string); ok && docTitle != "" {
				title = docTitle
			} else {
				title = filepath.Base(path)
			}
		}

		slug := slugs.unique(generateSlug(currentHeading))

		url := ""
		if baseURL != "" {
			cleanPath := transformDocxPath(path)
			url = baseURL + "/" + cleanPath + "#" + slug
		}

		sectionPathID := strings.Join(sectionPath, " > ")

		sections = append(sections, DocumentSection{
			ID:          generateID(path, sectionPathID),
			FilePath:    path,
			Title:       title,
			Content:     content,
			Type:        "docx_section",
			URL:         url,
			SectionPath: sectionPath,
			Metadata: dp.mergeSectionMetadata(docMetadata, map[string]any{
				"heading_level": currentLevel,
			}),
		})
	}

	for _, p := range paragraphs {
		if p.headingLevel > 0 {
			// Flush previous section
			flushSection()

			// Update heading stack: pop entries with level >= current
			for len(headingStack) > 0 && headingStack[len(headingStack)-1].level >= p.headingLevel {
				headingStack = headingStack[:len(headingStack)-1]
			}
			headingStack = append(headingStack, headingStackEntry{
				level: p.headingLevel,
				title: p.text,
			})

			currentHeading = p.text
			currentLevel = p.headingLevel
			currentContent.Reset()
		} else {
			if currentContent.Len() > 0 {
				currentContent.WriteString("\n")
			}
			currentContent.WriteString(p.text)
		}
	}

	// Flush last section
	flushSection()

	return sections
}

// createFullDocSection creates a single section for documents without headings.
func (dp *DocxProcessor) createFullDocSection(paragraphs []docxParagraph, path, baseURL string,
	docMetadata map[string]any) []DocumentSection {

	var content strings.Builder
	for i, p := range paragraphs {
		if i > 0 {
			content.WriteString("\n")
		}
		content.WriteString(p.text)
	}

	text := strings.TrimSpace(content.String())
	if text == "" {
		return nil
	}

	title := filepath.Base(path)
	if docTitle, ok := docMetadata["title"].(string); ok && docTitle != "" {
		title = docTitle
	}

	url := ""
	if baseURL != "" {
		url = baseURL + "/" + transformDocxPath(path)
	}

	return []DocumentSection{{
		ID:       generateID(path, "document"),
		FilePath: path,
		Title:    title,
		Content:  text,
		Type:     "docx_section",
		URL:      url,
		Metadata: dp.mergeSectionMetadata(docMetadata, map[string]any{
			"no_headings": true,
		}),
	}}
}

func (dp *DocxProcessor) mergeSectionMetadata(docMeta, sectionMeta map[string]any) map[string]any {
	merged := make(map[string]any)
	maps.Copy(merged, docMeta)
	maps.Copy(merged, sectionMeta)
	return merged
}

func transformDocxPath(path string) string {
	return strings.TrimSuffix(path, ".docx")
}

// parseHeadingLevel extracts a heading level (1-9) from a Word style name.
// Returns 0 if the style is not a heading.
func parseHeadingLevel(styleVal string) int {
	lower := strings.ToLower(styleVal)
	if !strings.HasPrefix(lower, "heading") {
		return 0
	}
	rest := strings.TrimPrefix(lower, "heading")
	rest = strings.TrimSpace(rest)
	if len(rest) == 1 && rest[0] >= '1' && rest[0] <= '9' {
		return int(rest[0] - '0')
	}
	return 0
}

// nsWordprocessingML is the OOXML WordprocessingML namespace.
const nsWordprocessingML = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
