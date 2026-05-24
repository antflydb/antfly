package docsaf

import (
	"archive/zip"
	"encoding/xml"
	"fmt"
	"io"
	"path/filepath"
)

// readZipFile reads a named file from a ZIP archive.
func readZipFile(zr *zip.Reader, name string) ([]byte, error) {
	for _, f := range zr.File {
		if f.Name == name {
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s: %w", name, err)
			}
			defer rc.Close() //nolint:errcheck // best-effort close in deferred cleanup
			return io.ReadAll(rc)
		}
	}
	return nil, fmt.Errorf("file %s not found in archive", name)
}

// coreProperties represents the Dublin Core metadata in docProps/core.xml,
// shared by DOCX, PPTX, and other OOXML formats.
type coreProperties struct {
	Title          string `xml:"title"`
	Creator        string `xml:"creator"`
	Description    string `xml:"description"`
	Subject        string `xml:"subject"`
	Keywords       string `xml:"keywords"`
	Created        string `xml:"created"`
	Modified       string `xml:"modified"`
	LastModifiedBy string `xml:"lastModifiedBy"`
}

// extractOOXMLMetadata parses docProps/core.xml from a ZIP archive and returns
// document metadata. Falls back to the filename as title if core.xml is missing.
func extractOOXMLMetadata(zr *zip.Reader, path string) map[string]any {
	metadata := make(map[string]any)

	data, err := readZipFile(zr, "docProps/core.xml")
	if err != nil {
		metadata["title"] = filepath.Base(path)
		return metadata
	}

	var props coreProperties
	if err := xml.Unmarshal(data, &props); err != nil {
		metadata["title"] = filepath.Base(path)
		return metadata
	}

	if props.Title != "" {
		metadata["title"] = props.Title
	}
	if props.Creator != "" {
		metadata["author"] = props.Creator
	}
	if props.Subject != "" {
		metadata["subject"] = props.Subject
	}
	if props.Keywords != "" {
		metadata["keywords"] = props.Keywords
	}
	if props.Description != "" {
		metadata["description"] = props.Description
	}
	if props.Created != "" {
		metadata["creation_date"] = props.Created
	}
	if props.Modified != "" {
		metadata["mod_date"] = props.Modified
	}
	if props.LastModifiedBy != "" {
		metadata["last_modified_by"] = props.LastModifiedBy
	}

	if _, hasTitle := metadata["title"]; !hasTitle {
		metadata["title"] = filepath.Base(path)
	}

	return metadata
}
