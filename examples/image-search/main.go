// Image Search with CLIP - Go Example
// This example demonstrates how to use Antfly's vector search capabilities
// with CLIP embeddings for image search.
//
// Prerequisites:
// - Antfly running with Termite and ONNX Runtime
// - CLIP model: antfly termite pull openai/clip-vit-base-patch32
//
// Run: go run main.go

// ANCHOR: imports
package main

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/oapi"
)

// ANCHOR_END: imports

func main() {
	ctx := context.Background()

	// Step 1: Create the client
	client, err := antfly.NewAntflyClient("http://localhost:8080/api/v1", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	// ANCHOR: create_table
	// Step 2: Create the table with embeddings index
	fmt.Println("Creating table 'images' with CLIP embeddings index...")

	// Build the embedder config (union type)
	var embedderConfig oapi.EmbedderConfig
	embedderConfig.Provider = oapi.EmbedderProviderTermite
	embedderConfig.FromTermiteEmbedderConfig(oapi.TermiteEmbedderConfig{
		Model: "openai/clip-vit-base-patch32",
	})

	// Build the index config (union type)
	var indexConfig oapi.IndexConfig
	indexConfig.Name = "embeddings"
	indexConfig.Type = oapi.IndexTypeEmbeddings
	indexConfig.FromEmbeddingsIndexConfig(oapi.EmbeddingsIndexConfig{
		Dimension: 512,
		Template:  "{{media url=image_url}}{{caption}}",
		Embedder:  embedderConfig,
	})

	err = client.CreateTable(ctx, "images", antfly.CreateTableRequest{
		Indexes: map[string]oapi.IndexConfig{
			"embeddings": indexConfig,
		},
	})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "already exists") {
			fmt.Println("Table 'images' already exists, continuing...")
		} else if strings.Contains(errStr, "model not found") {
			fmt.Printf("Warning: Embedder model not available (%v)\n", err)
			fmt.Println("Creating table without embeddings index...")
			err = client.CreateTable(ctx, "images", antfly.CreateTableRequest{})
			if err != nil {
				if strings.Contains(err.Error(), "already exists") {
					fmt.Println("Table 'images' already exists, continuing...")
				} else {
					log.Fatalf("Failed to create table: %v", err)
				}
			} else {
				fmt.Println("Created table 'images' (without embeddings)")
			}
		} else {
			log.Fatalf("Failed to create table: %v", err)
		}
	} else {
		fmt.Println("Created table 'images'")
	}
	// ANCHOR_END: create_table

	// Wait for shards to be ready before inserting
	if err := client.WaitForTable(ctx, "images", 30*time.Second); err != nil {
		log.Fatalf("Error waiting for shards: %v", err)
	}
	fmt.Println("Shards ready")

	// ANCHOR: add_image
	// Step 3: Add a sample image (Utah teapot)
	fmt.Println("\nAdding Utah teapot sample image...")
	_, err = client.Batch(ctx, "images", antfly.BatchRequest{
		Inserts: map[string]any{
			"utah_teapot": map[string]any{
				"caption":   "Utah teapot",
				"image_url": "https://upload.wikimedia.org/wikipedia/commons/e/e7/Utah_teapot_simple_2.png",
			},
		},
	})
	if err != nil {
		log.Printf("Warning: Failed to add teapot: %v", err)
	} else {
		fmt.Println("Added Utah teapot")
	}
	// ANCHOR_END: add_image

	// ANCHOR: search
	// Step 4: Search with text
	fmt.Println("\nSearching for '3D model teapot'...")
	results, err := client.Query(ctx, antfly.QueryRequest{
		Table:          "images",
		SemanticSearch: "3D model teapot",
		Indexes:        []string{"embeddings"},
		Limit:          5,
	})
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Println("\nSearch results:")
	for _, resp := range results.Responses {
		for _, hit := range resp.Hits.Hits {
			fmt.Printf("  Score: %.4f, ID: %s\n", hit.Score, hit.ID)
		}
	}
	// ANCHOR_END: search

	// Step 5: Batch import from MMIR dataset (optional - requires downloading the dataset)
	if _, err := os.Stat("mmir_dataset.tsv.gz"); err == nil {
		fmt.Println("\n--- Batch Import from MMIR Dataset ---")
		batchImport(ctx, client)
	} else {
		fmt.Println("\n--- Batch Import Skipped ---")
		fmt.Println("To run batch import, first download the MMIR dataset:")
		fmt.Println("  curl -o mmir_dataset.tsv.gz \"https://storage.googleapis.com/gresearch/wit-retrieval/mmir_dataset_train-00000-of-00005.tsv.gz\"")
	}

	// Step 6: Try some more queries
	fmt.Println("\n--- Additional Query Examples ---")
	queries := []string{
		"church cathedral",
		"map geography",
		"aircraft airplane",
	}

	for _, q := range queries {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          "images",
			SemanticSearch: q,
			Indexes:        []string{"embeddings"},
			Limit:          3,
		})
		if err != nil {
			log.Printf("Query '%s' failed: %v", q, err)
			continue
		}

		fmt.Printf("\nQuery: '%s'\n", q)
		for _, resp := range results.Responses {
			for _, hit := range resp.Hits.Hits {
				caption := ""
				if src, ok := hit.Source["caption"].(string); ok {
					caption = src
				}
				fmt.Printf("  %.4f: %s\n", hit.Score, caption)
			}
		}
	}
}

// ANCHOR: batch_import
func batchImport(ctx context.Context, client *antfly.AntflyClient) {
	numImages := 100
	fmt.Printf("Importing first %d images...\n", numImages)
	startTime := time.Now()
	successCount := 0

	f, err := os.Open("mmir_dataset.tsv.gz")
	if err != nil {
		log.Printf("Failed to open dataset: %v", err)
		return
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Printf("Failed to create gzip reader: %v", err)
		return
	}
	defer gz.Close()

	reader := csv.NewReader(gz)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	reader.Read() // Skip header

	for successCount < numImages {
		row, err := reader.Read()
		if err != nil {
			break
		}
		if len(row) < 5 {
			continue
		}

		imageURL, caption := row[0], row[4]
		// Handle b'...' format in dataset
		if strings.HasPrefix(imageURL, "b'") && strings.HasSuffix(imageURL, "'") {
			imageURL = imageURL[2 : len(imageURL)-1]
		}
		if strings.HasPrefix(caption, "b'") && strings.HasSuffix(caption, "'") {
			caption = caption[2 : len(caption)-1]
		}

		if !strings.HasPrefix(imageURL, "http") {
			continue
		}

		hash := md5.Sum([]byte(imageURL))
		docID := fmt.Sprintf("%x", hash[:6])

		_, err = client.Batch(ctx, "images", antfly.BatchRequest{
			Inserts: map[string]any{
				"mmir_" + docID: map[string]any{"caption": caption, "image_url": imageURL},
			},
		})
		if err == nil {
			successCount++
			fmt.Printf("\rImported: %d / %d", successCount, numImages)
		}
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("\nImported %d images in %.1fs (%.1f images/sec)\n", successCount, elapsed, float64(successCount)/elapsed)
}

// ANCHOR_END: batch_import

