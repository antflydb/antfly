package docsaf

import (
	"regexp"
	"strings"
)

// QuestionsExtractor extracts questions from various content sources.
type QuestionsExtractor struct{}

// ExtractFromMDXContent extracts questions from MDX/Markdown content.
// It looks for:
// 1. Questions in frontmatter (questions: [...])
// 2. <Questions> MDX components in the content
func (qe *QuestionsExtractor) ExtractFromMDXContent(path, sourceURL string, content []byte, frontmatter map[string]any) []Question {
	var questions []Question

	// Extract from frontmatter
	if frontmatter != nil {
		questions = append(questions, qe.extractFromFrontmatter(path, sourceURL, frontmatter)...)
	}

	// Extract from <Questions> components
	questions = append(questions, qe.extractFromQuestionsComponents(path, sourceURL, content)...)

	return questions
}

// extractFromFrontmatter extracts questions from frontmatter's "questions" field.
// Supports both array of strings and array of objects with "text" field.
func (qe *QuestionsExtractor) extractFromFrontmatter(path, sourceURL string, frontmatter map[string]any) []Question {
	var questions []Question

	questionsField, ok := frontmatter["questions"]
	if !ok {
		return questions
	}

	// Get document title for context
	context := ""
	if title, ok := frontmatter["title"].(string); ok {
		context = title
	}

	switch v := questionsField.(type) {
	case []any:
		for _, item := range v {
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
					ID:         generateID(path, "frontmatter_q_"+questionText),
					Text:       questionText,
					SourcePath: path,
					SourceURL:  sourceURL,
					SourceType: "frontmatter",
					Context:    context,
					Metadata:   metadata,
				})
			}
		}
	}

	return questions
}

// questionsComponentRegex matches <Questions> components and captures their content.
// Supports both self-closing and block-style components.
var questionsComponentRegex = regexp.MustCompile(`(?s)<Questions[^>]*>(.*?)</Questions>`)

// extractFromQuestionsComponents extracts questions from <Questions> MDX components.
// It parses the content looking for list items (- or *) within the component.
func (qe *QuestionsExtractor) extractFromQuestionsComponents(path, sourceURL string, content []byte) []Question {
	var questions []Question

	matches := questionsComponentRegex.FindAllSubmatch(content, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		componentContent := string(match[1])
		// Parse list items (- or * at start of line)
		lines := strings.SplitSeq(componentContent, "\n")
		for line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") {
				questionText := strings.TrimSpace(line[2:])
				if questionText != "" {
					questions = append(questions, Question{
						ID:         generateID(path, "mdx_component_q_"+questionText),
						Text:       questionText,
						SourcePath: path,
						SourceURL:  sourceURL,
						SourceType: "mdx_component",
						Metadata:   nil,
					})
				}
			}
		}
	}

	return questions
}

// ExtractFromOpenAPI extracts x-docsaf-questions from OpenAPI extensions.
// The extensions map should contain the questions as a string array.
func (qe *QuestionsExtractor) ExtractFromOpenAPI(path, sourceURL, sourceType, context string, extensions map[string]any) []Question {
	var questions []Question

	questionsExt, ok := extensions["x-docsaf-questions"]
	if !ok {
		return questions
	}

	switch v := questionsExt.(type) {
	case []any:
		for _, item := range v {
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
					ID:         generateID(path, sourceType+"_q_"+questionText),
					Text:       questionText,
					SourcePath: path,
					SourceURL:  sourceURL,
					SourceType: sourceType,
					Context:    context,
					Metadata:   metadata,
				})
			}
		}
	case []string:
		for _, questionText := range v {
			questionText = strings.TrimSpace(questionText)
			if questionText != "" {
				questions = append(questions, Question{
					ID:         generateID(path, sourceType+"_q_"+questionText),
					Text:       questionText,
					SourcePath: path,
					SourceURL:  sourceURL,
					SourceType: sourceType,
					Context:    context,
					Metadata:   nil,
				})
			}
		}
	}

	return questions
}
