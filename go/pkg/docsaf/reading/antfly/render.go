// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package antfly

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/ajroetker/pdf/render"
	"github.com/antflydb/antfly/go/pkg/docsaf/reading"
)

const DefaultRenderDPI = 150

// RenderPDFPage renders a PDF page to PNG content for image-capable readers.
func RenderPDFPage(pdfData []byte, pageNum int, dpi float64) (reading.BinaryContent, error) {
	if pageNum <= 0 {
		return reading.BinaryContent{}, fmt.Errorf("page number must be greater than 0")
	}
	if dpi <= 0 {
		dpi = DefaultRenderDPI
	}

	renderer, err := render.NewRenderer(pdfData)
	if err != nil {
		return reading.BinaryContent{}, fmt.Errorf("create renderer: %w", err)
	}
	defer renderer.Close()

	pngBytes, err := renderer.RenderPageToPNG(pageNum, dpi)
	if err != nil {
		return reading.BinaryContent{}, fmt.Errorf("render page: %w", err)
	}

	return reading.BinaryContent{
		MIMEType: "image/png",
		Data:     pngBytes,
	}, nil
}

// EncodeDataURI encodes binary content as a data URI.
func EncodeDataURI(content reading.BinaryContent) (string, error) {
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
