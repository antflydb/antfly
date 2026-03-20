package reading

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/ajroetker/pdf/render"
	libai "github.com/antflydb/antfly/pkg/libaf/ai"
)

const DefaultRenderDPI = 150

// RenderPDFPage renders a PDF page to PNG content for image-capable readers.
func RenderPDFPage(pdfData []byte, pageNum int, dpi float64) (libai.BinaryContent, error) {
	if pageNum <= 0 {
		return libai.BinaryContent{}, fmt.Errorf("page number must be greater than 0")
	}
	if dpi <= 0 {
		dpi = DefaultRenderDPI
	}

	renderer, err := render.NewRenderer(pdfData)
	if err != nil {
		return libai.BinaryContent{}, fmt.Errorf("create renderer: %w", err)
	}
	defer renderer.Close()

	pngBytes, err := renderer.RenderPageToPNG(pageNum, dpi)
	if err != nil {
		return libai.BinaryContent{}, fmt.Errorf("render page: %w", err)
	}

	return libai.BinaryContent{
		MIMEType: "image/png",
		Data:     pngBytes,
	}, nil
}

// EncodeDataURI encodes binary content as a data URI.
func EncodeDataURI(content libai.BinaryContent) (string, error) {
	mimeType := strings.TrimSpace(content.MIMEType)
	if mimeType == "" {
		return "", fmt.Errorf("mime type is required")
	}
	if len(content.Data) == 0 {
		return "", fmt.Errorf("content data is empty")
	}

	b64 := base64.StdEncoding.EncodeToString(content.Data)
	return "data:" + mimeType + ";base64," + b64, nil
}
