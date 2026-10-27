// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// AttachmentExtractor must return complete, bounded UTF-8 text or an error.
// Applications may inject their document/OCR service without changing acquisition.
type AttachmentExtractor func(context.Context, string, string, []byte) ([]byte, error)

func htmlText(data []byte) []byte {
	z := html.NewTokenizer(bytes.NewReader(data))
	var out strings.Builder
	ignored := 0
	for {
		switch z.Next() {
		case html.ErrorToken:
			return []byte(strings.TrimSpace(out.String()))
		case html.StartTagToken:
			name, _ := z.TagName()
			switch string(name) {
			case "script", "style":
				ignored++
			case "br", "p", "div", "li", "tr":
				out.WriteByte('\n')
			}
		case html.EndTagToken:
			name, _ := z.TagName()
			if (string(name) == "script" || string(name) == "style") && ignored > 0 {
				ignored--
			}
		case html.TextToken:
			if ignored == 0 {
				out.Write(z.Text())
			}
		}
	}
}

// ExtractAttachmentText supports text/HTML, DOCX/PPTX, PDF (pdftotext), and
// images (tesseract). Missing tools and unsupported formats fail explicitly.
// Subprocesses receive private temporary files and inherit the caller's deadline.
func ExtractAttachmentText(ctx context.Context, name, mime string, data []byte) ([]byte, error) {
	if len(data) > maxImportSourceObjectBytes {
		return nil, fmt.Errorf("attachment exceeds byte limit")
	}
	mime = strings.ToLower(strings.TrimSpace(strings.Split(mime, ";")[0]))
	ext := strings.ToLower(filepath.Ext(name))
	var text []byte
	var err error
	switch {
	case mime == "text/html" || ext == ".html" || ext == ".htm":
		text = htmlText(data)
	case strings.HasPrefix(mime, "text/") || mime == "application/json" || mime == "application/xml" || ext == ".md" || ext == ".txt" || ext == ".csv":
		text = data
	case ext == ".docx" || ext == ".pptx" || mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" || mime == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
		text, err = officeAttachmentText(data)
	case mime == "application/pdf" || ext == ".pdf":
		text, err = attachmentCommand(ctx, "pdftotext", []string{"-layout", "-enc", "UTF-8"}, data, false)
	case strings.HasPrefix(mime, "image/"):
		text, err = attachmentCommand(ctx, "tesseract", nil, data, true)
	default:
		return nil, fmt.Errorf("%w: attachment %q (%s) requires a configured extractor", ErrInvalidInput, name, mime)
	}
	if err != nil {
		return nil, err
	}
	if len(text) > maxImportSourceObjectBytes || !utf8.Valid(text) || bytes.IndexByte(text, 0) >= 0 {
		return nil, fmt.Errorf("attachment extraction did not produce bounded UTF-8 text")
	}
	if len(bytes.TrimSpace(text)) == 0 && len(data) > 0 {
		return nil, fmt.Errorf("attachment has no extractable text; OCR or another extractor is required")
	}
	return text, nil
}

func officeAttachmentText(data []byte) ([]byte, error) {
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	var total int64
	found := false
	for _, file := range z.File {
		if file.Name != "word/document.xml" && !(strings.HasPrefix(file.Name, "ppt/slides/slide") && strings.HasSuffix(file.Name, ".xml")) {
			continue
		}
		found = true
		if file.UncompressedSize64 > maxImportSourceObjectBytes || total+int64(file.UncompressedSize64) > maxImportSourceObjectBytes {
			return nil, fmt.Errorf("office attachment exceeds expanded byte limit")
		}
		r, err := file.Open()
		if err != nil {
			return nil, err
		}
		b, err := readBounded(r)
		r.Close()
		if err != nil {
			return nil, err
		}
		total += int64(len(b))
		d := xml.NewDecoder(bytes.NewReader(b))
		for {
			t, err := d.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			switch t := t.(type) {
			case xml.StartElement:
				if t.Name.Local == "t" {
					var s string
					if err := d.DecodeElement(&s, &t); err != nil {
						return nil, err
					}
					out.WriteString(s)
				}
				if t.Name.Local == "tab" {
					out.WriteByte('\t')
				}
			case xml.EndElement:
				if t.Name.Local == "p" {
					out.WriteByte('\n')
				}
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("office attachment lacks document text")
	}
	return out.Bytes(), nil
}

// cappedOutput aborts producers before unbounded extracted text can accumulate.
type cappedOutput struct{ bytes.Buffer }

func (b *cappedOutput) Write(p []byte) (int, error) {
	if len(p) > maxImportSourceObjectBytes-b.Len() {
		return 0, fmt.Errorf("extracted text exceeds byte limit")
	}
	return b.Buffer.Write(p)
}
func attachmentCommand(ctx context.Context, executable string, args []string, data []byte, image bool) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	path, err := exec.LookPath(executable)
	if err != nil {
		return nil, fmt.Errorf("attachment extraction requires %s or an injected extractor", executable)
	}
	f, err := os.CreateTemp("", "antfly-attachment-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	args = append(args, f.Name())
	if image {
		args = append(args, "stdout")
	} else {
		args = append(args, "-")
	}
	command := exec.CommandContext(ctx, path, args...)
	var out cappedOutput
	command.Stdout = &out
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("%s attachment extraction failed: %w", executable, err)
	}
	return out.Bytes(), nil
}
