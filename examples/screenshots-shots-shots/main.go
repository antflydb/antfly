// Screenshots Shots Shots - Search your local screenshots, PDFs, and images with CLIP
//
// This example scans a local folder for screenshots, PDFs, and images,
// indexes them using CLIP embeddings via file:// URLs, and lets you search
// them with natural language queries. File content is NOT stored in Antfly —
// only the path is stored, and the server fetches files at enrichment time.
//
// Prerequisites:
// - Antfly running with inference and ONNX Runtime
// - CLIP model: antfly inference pull openai/clip-vit-base-patch32
//
// Run: go run main.go [folder] [query...]

// ANCHOR: imports
package main

import (
	"context"
	"fmt"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/oapi"
)

// ANCHOR_END: imports

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

func main() {
	// Parse args: [folder] [query...]
	folder := "."
	queries := []string{"screenshot of a terminal", "chart or graph", "text document"}

	args := os.Args[1:]
	if len(args) >= 1 {
		folder = args[0]
	}
	if len(args) >= 2 {
		queries = args[1:]
	}

	absFolder, err := filepath.Abs(folder)
	if err != nil {
		log.Fatalf("Invalid folder path: %v", err)
	}
	fmt.Printf("Scanning folder: %s\n", absFolder)

	// Discover files
	files, err := discoverFiles(absFolder)
	if err != nil {
		log.Fatalf("Failed to scan folder: %v", err)
	}
	fmt.Printf("Found %d supported files (.png, .jpg, .pdf, etc.)\n", len(files))

	if len(files) == 0 {
		fmt.Println("No supported files found. Put some screenshots or images in the folder and try again.")
		return
	}

	ctx := context.Background()

	antflyURL := os.Getenv("ANTFLY_URL")
	if antflyURL == "" {
		antflyURL = "http://localhost:8080/db/v1"
	}

	client, err := antfly.NewAntflyClient(antflyURL, http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	// ANCHOR: create_table
	// Step 1: Create the table with CLIP embeddings index
	// The template uses remoteMedia to fetch file content at enrichment time
	// via file:// URLs — the image data itself is NOT stored in Antfly.
	fmt.Println("\nCreating table 'screenshots' with CLIP embeddings index...")

	var embedderConfig oapi.EmbedderConfig
	embedderConfig.Provider = oapi.EmbedderProviderAntfly
	embedderConfig.FromAntflyEmbedderConfig(oapi.AntflyEmbedderConfig{
		Model: "openai/clip-vit-base-patch32",
	})

	var indexConfig oapi.IndexConfig
	indexConfig.Name = "embeddings"
	indexConfig.Type = oapi.IndexTypeEmbeddings
	indexConfig.FromEmbeddingsIndexConfig(oapi.EmbeddingsIndexConfig{
		Dimension: 512,
		Template:  "{{remoteMedia url=file_url}}{{filename}}",
		Embedder:  embedderConfig,
	})

	err = client.CreateTable(ctx, "screenshots", antfly.CreateTableRequest{
		Indexes: map[string]oapi.IndexConfig{
			"embeddings": indexConfig,
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
	// ANCHOR_END: create_table

	if err := client.WaitForTable(ctx, "screenshots", 30*time.Second); err != nil {
		log.Fatalf("Error waiting for shards: %v", err)
	}
	fmt.Println("Shards ready")

	// ANCHOR: ingest
	// Step 2: Insert documents with file:// URLs pointing to local files.
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
	// ANCHOR_END: ingest

	// ANCHOR: search
	// Step 3: Search with natural language
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
	// ANCHOR_END: search
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
