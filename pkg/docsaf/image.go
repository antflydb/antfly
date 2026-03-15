package docsaf

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/reading"
)

// ImageProcessor processes image files (PNG, JPEG, TIFF, WebP, BMP, GIF) by
// extracting text using an OCR Reader. Without a Reader, it cannot process
// any content.
type ImageProcessor struct {
	// Reader extracts text from images via OCR or vision models.
	Reader reading.Reader

	// ReadOptions configures the Reader call (prompt, max tokens).
	// Nil uses provider defaults.
	ReadOptions *reading.ReadOptions
}

// imageExtensions maps lowercase file extensions to MIME types for images
// that are reasonable candidates for OCR.
var imageExtensions = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".tiff": "image/tiff",
	".tif":  "image/tiff",
	".webp": "image/webp",
	".bmp":  "image/bmp",
	".gif":  "image/gif",
}

// imageMIMETypes is the set of MIME types this processor handles.
var imageMIMETypes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/tiff": true,
	"image/webp": true,
	"image/bmp":  true,
	"image/gif":  true,
}

// CanProcess returns true for image MIME types or image file extensions.
func (p *ImageProcessor) CanProcess(contentType, path string) bool {
	if p.Reader == nil {
		return false
	}

	if imageMIMETypes[contentType] {
		return true
	}

	ext := strings.ToLower(filepath.Ext(path))
	_, ok := imageExtensions[ext]
	return ok
}

// Process extracts text from an image using the configured Reader.
func (p *ImageProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	if p.Reader == nil {
		return nil, fmt.Errorf("ImageProcessor: no Reader configured")
	}

	// Determine MIME type from extension.
	mimeType := "image/png"
	ext := strings.ToLower(filepath.Ext(path))
	if mt, ok := imageExtensions[ext]; ok {
		mimeType = mt
	}

	results, err := p.Reader.Read(context.TODO(), []ai.BinaryContent{
		{MIMEType: mimeType, Data: content},
	}, p.ReadOptions)
	if err != nil {
		return nil, fmt.Errorf("reading image %s: %w", path, err)
	}

	text := ""
	if len(results) > 0 {
		text = strings.TrimSpace(results[0])
	}

	if text == "" {
		return nil, nil
	}

	title := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))

	url := ""
	if sourceURL != "" {
		url = sourceURL
	} else if baseURL != "" {
		url = strings.TrimSuffix(baseURL, "/") + "/" + path
	}

	return []DocumentSection{
		{
			ID:       generateID(path, "image"),
			FilePath: path,
			Title:    title,
			Content:  text,
			Type:     "image",
			URL:      url,
			Metadata: map[string]any{
				"extraction_method": "ocr",
				"mime_type":         mimeType,
			},
		},
	}, nil
}
