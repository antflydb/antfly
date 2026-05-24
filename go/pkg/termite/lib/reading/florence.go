package reading

import (
	"fmt"
	"strings"
	"unicode"
)

// Florence-2 Task Types
// Florence-2 was trained on natural language prompts, not task tokens.
// The HuggingFace processor converts task tokens (like <OCR>) to natural language
// prompts internally before tokenization. In ONNX mode without that processor,
// we use the natural language prompts directly.

// FlorenceTask represents a Florence-2 task type
type FlorenceTask string

const (
	// FlorenceCaption generates a brief image caption
	FlorenceCaption FlorenceTask = "What does the image describe?"
	// FlorenceDetailedCaption generates a detailed image caption
	FlorenceDetailedCaption FlorenceTask = "Describe in detail what is shown in the image."
	// FlorenceMoreDetailedCaption generates a very detailed image caption
	FlorenceMoreDetailedCaption FlorenceTask = "Describe with a paragraph what is shown in the image."
	// FlorenceOCR extracts text from the image
	FlorenceOCR FlorenceTask = "What is the text in the image?"
	// FlorenceOCRWithRegion extracts text with bounding box regions
	// Note: Location tokens may not work in ONNX mode; use FlorenceOCR with line break reconstruction
	FlorenceOCRWithRegion FlorenceTask = "What is the text in the image, with regions?"
	// FlorenceObjectDetection detects objects in the image
	FlorenceObjectDetection FlorenceTask = "Locate the objects with category name in the image."
	// FlorenceDenseRegionCaption generates captions for dense regions
	FlorenceDenseRegionCaption FlorenceTask = "Locate the objects in the image, with their descriptions."
	// FlorenceRegionProposal generates region proposals
	FlorenceRegionProposal FlorenceTask = "Locate the region proposals in the image."
	// FlorenceCaptionToGrounding grounds a caption's phrases to image regions
	// Note: This task requires input, use FlorenceGroundingPrompt() instead
	FlorenceCaptionToGrounding FlorenceTask = "Locate the phrases in the caption:"
)

// FlorencePrompt returns the task prompt string for a Florence-2 task.
func FlorencePrompt(task FlorenceTask) string {
	return string(task)
}

// FlorenceGroundingPrompt creates a grounding prompt with a caption.
// Example: FlorenceGroundingPrompt("A green car parked in front of a yellow building.")
// Returns: "<CAPTION_TO_PHRASE_GROUNDING>A green car parked in front of a yellow building."
func FlorenceGroundingPrompt(caption string) string {
	return string(FlorenceCaptionToGrounding) + caption
}

// FlorenceDocVQAPrompt creates a document VQA prompt for Florence-2-DocVQA models.
// Note: This requires a model fine-tuned for DocVQA (e.g., HuggingFaceM4/Florence-2-DocVQA).
// Example: FlorenceDocVQAPrompt("What is the total amount?")
// Returns: "<DocVQA>What is the total amount?"
func FlorenceDocVQAPrompt(question string) string {
	return "<DocVQA>" + question
}

// FlorenceOutputParser parses Florence-2 model output into a Result.
func FlorenceOutputParser(text, prompt string) Result {
	return Result{Text: FlorenceParseOCR(text)}
}

// FlorenceParseOCR extracts text from Florence-2 OCR output and reconstructs line breaks.
// Florence-2 outputs concatenated text without newlines. This function uses heuristics
// to detect line breaks:
// - When a lowercase letter is immediately followed by an uppercase letter (e.g., "headingThis")
// - When sentence-ending punctuation is immediately followed by an uppercase letter (e.g., "end.Next")
func FlorenceParseOCR(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return text
	}

	// Insert newlines at likely line break positions
	var result strings.Builder
	runes := []rune(text)

	for i, r := range runes {
		result.WriteRune(r)

		if i < len(runes)-1 {
			next := runes[i+1]
			// Lowercase followed by uppercase (e.g., "textNext")
			if unicode.IsLower(r) && unicode.IsUpper(next) {
				result.WriteRune('\n')
			}
			// Sentence-ending punctuation followed by uppercase (e.g., "end.Next" or "done!Start")
			if (r == '.' || r == '!' || r == '?') && unicode.IsUpper(next) {
				result.WriteRune('\n')
			}
		}
	}

	return result.String()
}

// FlorenceOCRResult represents OCR output with optional bounding boxes
type FlorenceOCRResult struct {
	Text   string
	Boxes  []FlorenceBoundingBox
	Labels []string
}

// FlorenceBoundingBox represents a bounding box in normalized coordinates
type FlorenceBoundingBox struct {
	X1, Y1, X2, Y2 float64
}

// FlorenceParseOCRWithRegion parses OCR_WITH_REGION output.
// Florence returns text with location tokens like <loc_123><loc_456>...
// This is a simplified parser - full parsing requires the model's tokenizer.
func FlorenceParseOCRWithRegion(text string) FlorenceOCRResult {
	result := FlorenceOCRResult{
		Text: strings.TrimSpace(text),
	}
	// TODO: Parse location tokens to extract bounding boxes
	// Location tokens are model-specific and require tokenizer access
	return result
}

// FlorenceDetectionResult represents object detection output
type FlorenceDetectionResult struct {
	Objects []FlorenceDetectedObject
}

// FlorenceDetectedObject represents a detected object with bounding box
type FlorenceDetectedObject struct {
	Label string
	Box   FlorenceBoundingBox
	Score float64
}

// String returns the task prompt as a string
func (t FlorenceTask) String() string {
	return string(t)
}

// FlorenceTaskFromString converts a string to a FlorenceTask.
// Accepts task token formats (e.g., "<OCR>", "<ocr>") for convenience, but note
// that Florence-2 was trained on natural language prompts. The HuggingFace processor
// converts task tokens to prompts internally; in ONNX mode, use the natural language
// prompts from FlorencePrompt() directly.
func FlorenceTaskFromString(s string) (FlorenceTask, error) {
	switch s {
	case "<CAPTION>", "<cap>":
		return FlorenceCaption, nil
	case "<DETAILED_CAPTION>", "<dcap>":
		return FlorenceDetailedCaption, nil
	case "<MORE_DETAILED_CAPTION>", "<ncap>":
		return FlorenceMoreDetailedCaption, nil
	case "<OCR>", "<ocr>":
		return FlorenceOCR, nil
	case "<OCR_WITH_REGION>":
		return FlorenceOCRWithRegion, nil
	case "<OD>", "<od>":
		return FlorenceObjectDetection, nil
	case "<DENSE_REGION_CAPTION>", "<region_cap>":
		return FlorenceDenseRegionCaption, nil
	case "<REGION_PROPOSAL>", "<proposal>":
		return FlorenceRegionProposal, nil
	case "<CAPTION_TO_PHRASE_GROUNDING>", "<grounding>":
		return FlorenceCaptionToGrounding, nil
	default:
		return "", fmt.Errorf("unknown Florence-2 task: %s", s)
	}
}
