// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/template"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	json "github.com/antflydb/antfly/pkg/libaf/json"
)

/*
Test extracting the description field

	go run lib/vector/testutils/generator/main.go \
	  -provider ollama \
	  -model nomic-embed-text \
	  -prompts lib/vector/testutils/generator/sample_prompts.jsonl \
	  -field description \
	  -output sampledesc-768d-10.gob

Test using a template

	go run lib/vector/testutils/generator/main.go \
	  -provider ollama \
	  -model nomic-embed-text \
	  -prompts lib/vector/testutils/generator/sample_prompts.jsonl \
	  -template "{{.title}}: {{.description}} [{{range .tags}}{{.}}, {{end}}]" \
	  -output sampletemp-768d-10.gob

Generate sparse embeddings via Termite

	go run lib/vector/testutils/generator/main.go \
	  -sparse \
	  -termite-url http://localhost:8850 \
	  -model splade-v3 \
	  -prompts wiki-articles-1000.json \
	  -field body \
	  -output sparse-wiki-1k.gob

Generate random sparse vectors

	go run lib/vector/testutils/generator/main.go \
	  -sparse \
	  -random 1000 \
	  -output random-sparse-1k.gob
*/
func main() {
	var (
		provider = flag.String(
			"provider",
			"ollama",
			"Embedding provider (openai, google, gemini, ollama, bedrock)",
		)
		model  = flag.String("model", "", "Model name (required)")
		output = flag.String(
			"output",
			"",
			"Output filename (required, will be saved in testdata folder)",
		)
		dimension = flag.Int("dimension", 0, "Vector dimension (optional, provider-specific)")
		random    = flag.Int(
			"random",
			0,
			"Generate random embeddings instead of using a model",
		)
		imagesFile = flag.String(
			"images",
			"",
			"File containing image paths (optional, for multimodal models)",
		)
		promptFile = flag.String(
			"prompts",
			"",
			"File containing prompts in JSONL format (required)",
		)
		promptField    = flag.String("field", "", "Field to extract from JSON objects (optional)")
		promptTemplate = flag.String(
			"template",
			"",
			"Go template to render with JSON objects (optional)",
		)
		sparse = flag.Bool(
			"sparse",
			false,
			"Generate sparse embeddings instead of dense (uses Termite SPLADE model)",
		)
		termiteURL = flag.String(
			"termite-url",
			"http://localhost:8850",
			"Termite service URL for sparse embedding generation",
		)
	)
	flag.Parse()

	// Sparse random generation
	if *sparse && *random > 0 {
		generateRandomSparse(*random, *output)
		return
	}

	// Sparse model-based generation
	if *sparse {
		generateSparseFromModel(*termiteURL, *model, *output, *promptFile, *promptField, *promptTemplate)
		return
	}

	if *random > 0 {
		dims := *dimension
		if dims <= 0 {
			log.Fatal("Dimension must be specified when generating random embeddings")
		}
		// Flatten embeddings into a single slice
		data := make([]float32, 0, dims*(*random))
		for i := 0; i < (*random); i++ {
			// Generate random embedding
			embedding := make([]float32, dims)
			for j := range dims {
				// Generate random float32 ranging from -14 to 14
				embedding[j] = rand.Float32()*28 - 14 //nolint:gosec // G404: non-security randomness for ML/jitter
			}
			data = append(data, embedding...)
		}
		vectorSet := &vector.Set_builder{
			Dims:  int64(dims),
			Count: int64(*random),
			Data:  data,
		}

		// Determine output path
		outputPath := *output
		if !strings.HasSuffix(outputPath, ".gob") {
			outputPath += ".gob"
		}

		var filePath string
		// Get the absolute path of this test file.
		_, testFile, _, ok := runtime.Caller(0)
		if !ok {
			log.Fatal("Failed to get current file path")
		}

		// Point to the dataset file.
		parentDir := filepath.Dir(testFile)
		filePath = filepath.Join(parentDir, "..", "..", "testdata", outputPath)

		// Create testdata directory if it doesn't exist
		testdataDir := filepath.Dir(filePath)
		if err := os.MkdirAll(testdataDir, 0o755); err != nil { //nolint:gosec // G301: standard permissions for data directory
			log.Fatalf("Failed to create testdata directory: %v", err)
		}

		// Save to file
		file, err := os.Create(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer func() { _ = file.Close() }()

		encoder := gob.NewEncoder(file)
		if err := encoder.Encode(vectorSet); err != nil {
			log.Fatalf("Failed to encode vector set: %v", err)
		}
		return
	}

	if *model == "" {
		log.Fatal("Model name is required. Use -model flag")
	}
	if *output == "" {
		log.Fatal("Output filename is required. Use -output flag")
	}
	if !strings.HasSuffix(*output, ".gob") {
		*output += ".gob"
	}

	// Build configuration for the embedder
	config := map[string]any{
		"provider": *provider,
		"model":    *model,
	}

	// Add optional configuration
	if *dimension > 0 {
		config["dimension"] = uint64(*dimension)
	}

	var modelConfig embeddings.EmbedderConfig
	// FIXME (ajr) Store the embedder config natively on the index config
	bytes, _ := json.Marshal(config)
	_ = json.Unmarshal(bytes, &modelConfig)

	// Create embedder
	embedder, err := embeddings.NewEmbedder(modelConfig)
	if err != nil {
		log.Fatalf("Failed to create embedder: %v", err)
	}
	// Generate embeddings in batches
	ctx := context.Background()
	batchSize := 100
	var allEmbeddings [][]float32

	if *imagesFile != "" {
		// Check if embedder supports image content
		caps := embedder.Capabilities()
		if !caps.SupportsModality("image/") {
			log.Fatal("Multimodal embedding is not supported by this embedder")
		}
		imagePaths, err := readImagePathsFromFile(*imagesFile)
		if err != nil {
			log.Fatalf("Failed to read image paths: %v", err)
		}
		if len(imagePaths) == 0 {
			log.Fatal("No image paths found in the provided file")
		}
		fmt.Printf("Using %d images for multimodal embedding\n", len(imagePaths))
		for i := 0; i < len(imagePaths); i += batchSize {
			end := min(i+batchSize, len(imagePaths))

			batch := imagePaths[i:end]
			contentBatch := make([][]ai.ContentPart, len(batch))
			for j, path := range batch {
				contentBatch[j] = []ai.ContentPart{
					ai.ImageURLContent{URL: "file://" + path},
				}
			}
			fmt.Printf("Processing batch %d-%d of %d...\n", i+1, end, len(imagePaths))

			batchEmbeddings, err := embedder.Embed(ctx, contentBatch)
			if err != nil {
				if strings.Contains(err.Error(), "resource exhausted") {
					time.Sleep(30 * time.Second)
					batchEmbeddings, err = embedder.Embed(ctx, contentBatch)
				}
				if err != nil {
					log.Fatalf("Failed to generate embeddings for batch %d-%d: %v", i+1, end, err)
				}
			}

			allEmbeddings = append(allEmbeddings, batchEmbeddings...)
			time.Sleep(100 * time.Millisecond)
		}
	} else {

		if *promptFile == "" {
			log.Fatal("Prompt file is required. Use -prompts flag")
		}
		if !strings.HasSuffix(*promptFile, ".jsonl") && !strings.HasSuffix(*promptFile, ".json") {
			log.Fatal("Prompt file must be in JSONL or JSON format. Use .jsonl")
		}

		if *promptField != "" && *promptTemplate != "" {
			log.Fatal("Cannot use both -field and -template flags at the same time. Choose one.")
		} else if *promptField == "" && *promptTemplate == "" {
			log.Fatal("Must specify either -field or -template flag to extract prompts from JSON objects")
		}
		// Read prompts from file if provided
		prompts, err := readPromptsFromFile(*promptFile, *promptField, *promptTemplate)
		if err != nil {
			log.Fatalf("Failed to read prompts: %v", err)
		}
		if len(prompts) == 0 {
			log.Fatal("No prompts found in the provided file")
		}

		fmt.Printf("Generating embeddings for %d prompts using %s/%s...\n", len(prompts), *provider, *model)

		for i := 0; i < len(prompts); i += batchSize {
			end := min(i+batchSize, len(prompts))

			batch := prompts[i:end]
			fmt.Printf("Processing batch %d-%d of %d...\n", i+1, end, len(prompts))

			batchEmbeddings, err := embeddings.EmbedText(ctx, embedder, batch)
			if err != nil {
				log.Fatalf("Failed to generate embeddings for batch %d-%d: %v", i+1, end, err)
			}

			allEmbeddings = append(allEmbeddings, batchEmbeddings...)

		}
	}

	if len(allEmbeddings) == 0 {
		log.Fatal("No embeddings were generated")
	}

	// Convert to vector.Set
	dims := len(allEmbeddings[0])
	count := len(allEmbeddings)

	fmt.Printf("Generated %d vectors of dimension %d\n", count, dims)

	// Flatten embeddings into a single slice
	data := make([]float32, 0, dims*count)
	for _, embedding := range allEmbeddings {
		data = append(data, embedding...)
	}

	vectorSet := &vector.Set_builder{
		Dims:  int64(dims),
		Count: int64(count),
		Data:  data,
	}

	// Determine output path
	outputPath := *output
	if !strings.HasSuffix(outputPath, ".gob") {
		outputPath += ".gob"
	}

	var filePath string
	// Get the absolute path of this test file.
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Failed to get current file path")
	}

	// Point to the dataset file.
	parentDir := filepath.Dir(testFile)
	filePath = filepath.Join(parentDir, "..", "..", "testdata", outputPath)

	// Create testdata directory if it doesn't exist
	testdataDir := filepath.Dir(filePath)
	if err := os.MkdirAll(testdataDir, 0o755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		log.Fatalf("Failed to create testdata directory: %v", err)
	}

	// Save to file
	file, err := os.Create(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer func() { _ = file.Close() }()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(vectorSet); err != nil {
		log.Fatalf("Failed to encode vector set: %v", err)
	}

	fmt.Printf("Successfully saved dataset to %s\n", filePath)
	fmt.Printf("Dataset summary:\n")
	fmt.Printf("  - Vectors: %d\n", count)
	fmt.Printf("  - Dimensions: %d\n", dims)
	fmt.Printf("  - Provider: %s\n", *provider)
	fmt.Printf("  - Model: %s\n", *model)
}

func generateRandomSparse(count int, output string) {
	if output == "" {
		log.Fatal("Output filename is required. Use -output flag")
	}

	dataset := testutils.SparseDataset{Count: int64(count)}
	for range count {
		// Random number of non-zero entries (10-100), typical for SPLADE
		nnz := rand.IntN(91) + 10 //nolint:gosec // G404: non-security randomness for ML/jitter
		indices := make([]uint32, nnz)
		values := make([]float32, nnz)

		// Random unique indices in BERT vocab space (0-30521)
		used := make(map[uint32]bool, nnz)
		for j := range nnz {
			for {
				idx := rand.Uint32N(30522) //nolint:gosec // G404: non-security randomness for ML/jitter
				if !used[idx] {
					used[idx] = true
					indices[j] = idx
					break
				}
			}
		}
		slices.Sort(indices)

		// Random positive weights (SPLADE values are always positive after ReLU + log)
		for j := range nnz {
			values[j] = rand.Float32() * 5.0 //nolint:gosec // G404: non-security randomness for ML/jitter
		}

		dataset.Vectors = append(dataset.Vectors, testutils.SparseDataEntry{
			Indices: indices,
			Values:  values,
		})
	}

	saveSparseDataset(dataset, output)
	fmt.Printf("Dataset summary:\n")
	fmt.Printf("  - Vectors: %d\n", count)
	fmt.Printf("  - Type: random sparse\n")
}

func generateSparseFromModel(termiteURL, model, output, promptFile, promptField, promptTemplate string) {
	if model == "" {
		log.Fatal("Model name is required for sparse generation. Use -model flag")
	}
	if output == "" {
		log.Fatal("Output filename is required. Use -output flag")
	}
	if promptFile == "" {
		log.Fatal("Prompt file is required for sparse generation. Use -prompts flag")
	}
	if promptField == "" && promptTemplate == "" {
		log.Fatal("Must specify either -field or -template flag to extract prompts from JSON objects")
	}

	prompts, err := readPromptsFromFile(promptFile, promptField, promptTemplate)
	if err != nil {
		log.Fatalf("Failed to read prompts: %v", err)
	}
	if len(prompts) == 0 {
		log.Fatal("No prompts found in the provided file")
	}

	// Create Termite client (implements SparseEmbedder)
	embedder, err := embeddings.NewTermiteClient(termiteURL, model)
	if err != nil {
		log.Fatalf("Failed to create Termite client: %v", err)
	}
	sparseEmbedder, ok := embedder.(embeddings.SparseEmbedder)
	if !ok {
		log.Fatal("Termite client does not support sparse embeddings")
	}

	fmt.Printf("Generating sparse embeddings for %d prompts using Termite/%s...\n", len(prompts), model)

	ctx := context.Background()
	batchSize := 100
	var allVecs []embeddings.SparseVector

	for i := 0; i < len(prompts); i += batchSize {
		end := min(i+batchSize, len(prompts))
		batch := prompts[i:end]
		fmt.Printf("Processing batch %d-%d of %d...\n", i+1, end, len(prompts))

		vecs, err := sparseEmbedder.SparseEmbed(ctx, batch)
		if err != nil {
			log.Fatalf("Failed to generate sparse embeddings for batch %d-%d: %v", i+1, end, err)
		}
		allVecs = append(allVecs, vecs...)
	}

	// Convert to SparseDataset
	dataset := testutils.SparseDataset{Count: int64(len(allVecs))}
	for _, v := range allVecs {
		dataset.Vectors = append(dataset.Vectors, testutils.SparseDataEntry{
			Indices: v.Indices,
			Values:  v.Values,
		})
	}

	saveSparseDataset(dataset, output)
	fmt.Printf("Dataset summary:\n")
	fmt.Printf("  - Vectors: %d\n", len(allVecs))
	fmt.Printf("  - Type: sparse (Termite/%s)\n", model)
}

func saveSparseDataset(dataset testutils.SparseDataset, output string) {
	if !strings.HasSuffix(output, ".gob") {
		output += ".gob"
	}

	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Failed to get current file path")
	}
	parentDir := filepath.Dir(testFile)
	filePath := filepath.Join(parentDir, "..", "..", "testdata", output)

	testdataDir := filepath.Dir(filePath)
	if err := os.MkdirAll(testdataDir, 0o755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		log.Fatalf("Failed to create testdata directory: %v", err)
	}

	file, err := os.Create(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer func() { _ = file.Close() }()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(dataset); err != nil {
		log.Fatalf("Failed to encode sparse dataset: %v", err)
	}

	fmt.Printf("Successfully saved sparse dataset to %s\n", filePath)
}

func readImagePathsFromFile(filename string) ([]string, error) {
	file, err := os.Open(filename) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer func() { _ = file.Close() }()

	var imagePaths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue // Skip empty lines and comments
		}
		imagePaths = append(imagePaths, string(line))
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return imagePaths, nil
}

// readPromptsFromFile reads prompts from a file, supporting both plain text and JSONL formats
func readPromptsFromFile(filename, field, tmpl string) ([]string, error) {
	file, err := os.Open(filename) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	var prompts []string
	scanner := bufio.NewScanner(file)

	// Check if the file appears to be JSONL by looking at the extension or first line
	isJSON := strings.HasSuffix(filename, ".jsonl") || strings.HasSuffix(filename, ".json")

	// If not obviously JSON, check the first line
	if !isJSON && scanner.Scan() {
		firstLine := scanner.Text()
		firstLine = strings.TrimSpace(firstLine)
		if firstLine != "" && !strings.HasPrefix(firstLine, "#") {
			// Try to parse as JSON
			var testObj map[string]any
			if err := json.Unmarshal([]byte(firstLine), &testObj); err == nil {
				isJSON = true
			}
			// Process the first line since we already read it
			if isJSON {
				prompt, err := extractPromptFromJSON([]byte(firstLine), field, tmpl)
				if err != nil {
					return nil, fmt.Errorf("failed to extract prompt from line 1: %w", err)
				}
				if prompt != "" {
					prompts = append(prompts, prompt)
				}
			} else if firstLine != "" && !strings.HasPrefix(firstLine, "#") {
				prompts = append(prompts, firstLine)
			}
		}
	}

	// Process remaining lines
	lineNum := 1
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		line = bytes.TrimSpace(line)

		if len(line) == 0 || bytes.HasPrefix(line, []byte{'#'}) {
			continue // Skip empty lines and comments
		}

		if isJSON {
			prompt, err := extractPromptFromJSON(line, field, tmpl)
			if err != nil {
				return nil, fmt.Errorf("failed to extract prompt from line %d: %w", lineNum, err)
			}
			if prompt != "" {
				prompts = append(prompts, prompt)
			}
		} else {
			prompts = append(prompts, string(line))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return prompts, nil
}

// extractPromptFromJSON extracts a prompt from a JSON object using a field or template
func extractPromptFromJSON(data []byte, field, tmpl string) (string, error) {
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return "", fmt.Errorf("failed to parse JSON: %w", err)
	}

	// If a template is specified, use it
	if tmpl != "" {
		rendered, err := template.Render(tmpl, obj)
		if err != nil {
			return "", fmt.Errorf("failed to render template: %w", err)
		}
		return rendered, nil
	}

	// If a field is specified, extract it
	if field != "" {
		value, ok := obj[field]
		if !ok {
			return "", fmt.Errorf("field %q not found in JSON object", field)
		}
		// Convert the value to string
		switch v := value.(type) {
		case string:
			return v, nil
		case float64, int, bool:
			return fmt.Sprintf("%v", v), nil
		default:
			// For complex types, marshal back to JSON
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("failed to convert field value to string: %w", err)
			}
			return string(jsonBytes), nil
		}
	}

	// If neither field nor template is specified, try common fields
	for _, commonField := range []string{"text", "content", "description", "prompt", "title"} {
		if value, ok := obj[commonField].(string); ok && value != "" {
			return value, nil
		}
	}

	// If no common fields found, convert the entire object to JSON string
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}
	return string(jsonBytes), nil
}
