package docsaf

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
)

// PptxProcessor processes Microsoft PowerPoint (.pptx) files.
// Each slide becomes a separate DocumentSection, similar to PDFProcessor pages.
type PptxProcessor struct {
	// IncludeNotes controls whether speaker notes are appended to slide content.
	// When true (default), notes are appended after a separator.
	IncludeNotes *bool
}

func (pp *PptxProcessor) includeNotes() bool {
	if pp.IncludeNotes != nil {
		return *pp.IncludeNotes
	}
	return true // default
}

// CanProcess returns true for PPTX content types or .pptx extensions.
func (pp *PptxProcessor) CanProcess(contentType, path string) bool {
	if strings.Contains(contentType, "application/vnd.openxmlformats-officedocument.presentationml.presentation") {
		return true
	}
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".pptx")
}

// Process extracts text from a PPTX file and returns one DocumentSection per slide.
func (pp *PptxProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	zr, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, fmt.Errorf("failed to open PPTX archive: %w", err)
	}

	docMetadata := extractOOXMLMetadata(zr, path)
	if sourceURL != "" {
		docMetadata["source_url"] = sourceURL
	}

	slides := pp.findSlides(zr)
	if len(slides) == 0 {
		return nil, nil
	}

	totalSlides := len(slides)
	docTitle := "Presentation"
	if title, ok := docMetadata["title"].(string); ok && title != "" {
		docTitle = title
	}

	var sections []DocumentSection

	for _, slide := range slides {
		slideData, err := readZipFile(zr, slide.path)
		if err != nil {
			continue
		}

		text := extractSlideText(slideData)

		// Optionally extract speaker notes
		if pp.includeNotes() && slide.notesPath != "" {
			notesData, err := readZipFile(zr, slide.notesPath)
			if err == nil {
				notes := extractSlideText(notesData)
				if notes != "" {
					text = text + "\n\n---\nSpeaker Notes:\n" + notes
				}
			}
		}

		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		title := fmt.Sprintf("%s - Slide %d", docTitle, slide.number)

		url := ""
		if baseURL != "" {
			cleanPath := transformPptxPath(path)
			url = fmt.Sprintf("%s/%s#slide-%d", baseURL, cleanPath, slide.number)
		}

		sections = append(sections, DocumentSection{
			ID:       generateID(path, fmt.Sprintf("slide_%d", slide.number)),
			FilePath: path,
			Title:    title,
			Content:  text,
			Type:     "pptx_slide",
			URL:      url,
			Metadata: pp.mergeSectionMetadata(docMetadata, map[string]any{
				"slide_number": slide.number,
				"total_slides": totalSlides,
			}),
		})
	}

	return sections, nil
}

// slideEntry holds the path and number of a slide in the archive.
type slideEntry struct {
	path      string
	notesPath string
	number    int
}

var slideFileRegex = regexp.MustCompile(`^ppt/slides/slide(\d+)\.xml$`)

// findSlides discovers slide files in the ZIP and returns them sorted by number.
func (pp *PptxProcessor) findSlides(zr *zip.Reader) []slideEntry {
	// Build a set of notes files for quick lookup
	notesFiles := make(map[int]string)
	notesRegex := regexp.MustCompile(`^ppt/notesSlides/notesSlide(\d+)\.xml$`)

	var slides []slideEntry

	for _, f := range zr.File {
		if m := slideFileRegex.FindStringSubmatch(f.Name); m != nil {
			num := 0
			for _, c := range m[1] {
				num = num*10 + int(c-'0')
			}
			slides = append(slides, slideEntry{path: f.Name, number: num})
		}
		if m := notesRegex.FindStringSubmatch(f.Name); m != nil {
			num := 0
			for _, c := range m[1] {
				num = num*10 + int(c-'0')
			}
			notesFiles[num] = f.Name
		}
	}

	// Sort by slide number
	sort.Slice(slides, func(i, j int) bool {
		return slides[i].number < slides[j].number
	})

	// Attach notes paths
	for i := range slides {
		if notesPath, ok := notesFiles[slides[i].number]; ok {
			slides[i].notesPath = notesPath
		}
	}

	return slides
}

// extractSlideText parses a PresentationML slide or notes XML and extracts all text.
func extractSlideText(data []byte) string {
	decoder := xml.NewDecoder(bytes.NewReader(data))

	var parts []string
	var inRun bool
	var currentPara strings.Builder
	var paraHasText bool

	for {
		tok, err := decoder.Token()
		if err != nil {
			break
		}

		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "p":
				// DrawingML paragraph <a:p>
				if t.Name.Space == nsDrawingML || t.Name.Space == "" {
					currentPara.Reset()
					paraHasText = false
				}
			case "r":
				// DrawingML run <a:r>
				inRun = true
			}

		case xml.EndElement:
			switch t.Name.Local {
			case "p":
				if paraHasText {
					text := strings.TrimSpace(currentPara.String())
					if text != "" {
						parts = append(parts, text)
					}
				}
			case "r":
				inRun = false
			}

		case xml.CharData:
			if inRun {
				currentPara.Write(t)
				paraHasText = true
			}
		}
	}

	return strings.Join(parts, "\n")
}

func (pp *PptxProcessor) mergeSectionMetadata(docMeta, sectionMeta map[string]any) map[string]any {
	merged := make(map[string]any)
	maps.Copy(merged, docMeta)
	maps.Copy(merged, sectionMeta)
	return merged
}

func transformPptxPath(path string) string {
	return strings.TrimSuffix(path, ".pptx")
}

// nsDrawingML is the OOXML DrawingML namespace used in PPTX text elements.
const nsDrawingML = "http://schemas.openxmlformats.org/drawingml/2006/main"
