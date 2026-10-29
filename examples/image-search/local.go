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

package main

import (
	"context"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// Local mode indexes screenshots, PDFs, and images from a folder. Documents
// store only a file:// URL; remoteMedia reads the file at enrichment time, so
// the image data is not copied into Antfly.

var supportedExts = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".gif":  "image/gif",
	".webp": "image/webp",
	".bmp":  "image/bmp",
	".tiff": "image/tiff",
	".tif":  "image/tiff",
	".pdf":  "application/pdf",
}

// searchLocalFolder indexes the supported files under folder into the
// "screenshots" table and runs each query against it.
func searchLocalFolder(ctx context.Context, client *antfly.AntflyClient, folder string, queries []string) {
	if len(queries) == 0 {
		queries = []string{"screenshot of a terminal", "chart or graph", "text document"}
	}
	absFolder, err := filepath.Abs(folder)
	if err != nil {
		log.Fatalf("Invalid folder path: %v", err)
	}
	fmt.Printf("Scanning folder: %s\n", absFolder)

	files, err := discoverFiles(absFolder)
	if err != nil {
		log.Fatalf("Failed to scan folder: %v", err)
	}
	fmt.Printf("Found %d supported files (.png, .jpg, .pdf, etc.)\n", len(files))
	if len(files) == 0 {
		fmt.Println("No supported files found. Put some screenshots or images in the folder and try again.")
		return
	}

	// Create the table with CLIP embeddings index
	// The template uses remoteMedia to fetch file content at enrichment time
	// via file:// URLs — the image data itself is NOT stored in Antfly.
	fmt.Println("\nCreating table 'screenshots' with CLIP embeddings index...")

	embedderConfig, err := antfly.NewIndexEmbedderConfig(antfly.AntflyEmbedderConfig{
		Model: "openai/clip-vit-base-patch32",
	})
	if err != nil {
		log.Fatal(err)
	}

	indexConfig, err := antfly.NewCreateIndexRequest(oapi.EmbeddingsIndexConfig{
		Dimension: 512,
		Template:  "{{remoteMedia url=file_url}}{{filename}}",
		Embedder:  *embedderConfig,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = client.CreateTable(ctx, "screenshots", antfly.CreateTableRequest{
		Indexes: map[string]antfly.CreateIndexRequest{
			"embeddings": *indexConfig,
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			fmt.Println("Table 'screenshots' already exists, continuing...")
		} else {
			log.Fatalf("Failed to create table: %v", err)
		}
	} else {
		fmt.Println("Created table 'screenshots'")
	}

	if err := client.WaitForTable(ctx, "screenshots", 30*time.Second); err != nil {
		log.Fatalf("Error waiting for shards: %v", err)
	}
	fmt.Println("Shards ready")

	// ANCHOR: local_ingest
	// Insert documents with file:// URLs pointing to local files.
	// Only metadata is stored — remoteMedia fetches content during enrichment.
	fmt.Printf("\nIngesting %d files...\n", len(files))
	startTime := time.Now()
	ingested := 0

	for _, f := range files {
		docID := sanitizeID(f.relPath)
		_, err = client.Batch(ctx, "screenshots", antfly.BatchRequest{
			Inserts: map[string]any{
				docID: map[string]any{
					"filename":  f.name,
					"path":      f.relPath,
					"mime_type": f.mime,
					"file_url":  "file://" + f.path,
				},
			},
		})
		if err != nil {
			log.Printf("  Failed to ingest %s: %v", f.name, err)
			continue
		}

		ingested++
		fmt.Printf("\r  Ingested: %d / %d", ingested, len(files))
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\n  Done: %d files in %.1fs\n", ingested, elapsed.Seconds())
	// ANCHOR_END: local_ingest

	// ANCHOR: local_search
	// Search with natural language
	fmt.Println("\n--- Search Results ---")

	for _, q := range queries {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          "screenshots",
			SemanticSearch: q,
			Indexes:        []string{"embeddings"},
			Limit:          5,
		})
		if err != nil {
			log.Printf("Query '%s' failed: %v", q, err)
			continue
		}

		fmt.Printf("\nQuery: %q\n", q)
		for _, resp := range results.Responses {
			if len(resp.Hits.Hits) == 0 {
				fmt.Println("  (no results)")
			}
			for _, hit := range resp.Hits.Hits {
				filename := ""
				if f, ok := hit.Source["filename"].(string); ok {
					filename = f
				}
				path := ""
				if p, ok := hit.Source["path"].(string); ok {
					path = p
				}
				display := path
				if display == "" {
					display = filename
				}
				fmt.Printf("  %.4f  %s\n", hit.Score, display)
			}
		}
	}
	// ANCHOR_END: local_search
}

type fileInfo struct {
	path    string // absolute path
	relPath string // relative to scan folder
	name    string // basename
	mime    string
}

func discoverFiles(root string) ([]fileInfo, error) {
	var files []fileInfo
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if info.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		mimeType, ok := supportedExts[ext]
		if !ok {
			// Try system MIME detection as fallback
			mimeType = mime.TypeByExtension(ext)
			if mimeType == "" || (!strings.HasPrefix(mimeType, "image/") && mimeType != "application/pdf") {
				return nil
			}
		}

		rel, _ := filepath.Rel(root, path)

		files = append(files, fileInfo{
			path:    path,
			relPath: rel,
			name:    info.Name(),
			mime:    mimeType,
		})
		return nil
	})
	return files, err
}

// sanitizeID turns a file path into a valid document ID.
func sanitizeID(path string) string {
	r := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		" ", "-",
		".", "-",
	)
	id := r.Replace(path)
	if len(id) > 128 {
		id = id[:128]
	}
	return id
}
