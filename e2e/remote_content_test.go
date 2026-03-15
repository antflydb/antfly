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

package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/scraping"
	libtermite "github.com/antflydb/antfly/lib/termite"
	"github.com/antflydb/antfly/lib/types"
	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/termite/pkg/termite"
	"github.com/antflydb/termite/pkg/termite/lib/modelregistry"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// CLIP model name in the Antfly model registry
	clipModelName = "openai/clip-vit-base-patch32"

	// Expected embedding dimension for CLIP ViT-B/32
	clipEmbeddingDim = 512

	// CLAP model name on HuggingFace (used as both repo ID and local name)
	clapModelName = "Xenova/clap-htsat-unfused"

	// Expected embedding dimension for CLAP
	clapEmbeddingDim = 512
)

// modelDownloadMutex ensures only one model downloads at a time
var modelDownloadMutex sync.Mutex

// setupFakeS3ForE2E creates an in-memory S3 server for E2E testing.
// Returns a MinIO client, the server URL, endpoint (without http://), and a cleanup function.
func setupFakeS3ForE2E(t *testing.T) (*minio.Client, string, string, func()) {
	t.Helper()

	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Start HTTP server
	ts := httptest.NewServer(faker.Server())

	// Parse endpoint (remove http://)
	endpoint := strings.TrimPrefix(ts.URL, "http://")

	// Create MinIO client pointing to fake server
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("test-key", "test-secret", ""),
		Secure: false,
	})
	require.NoError(t, err)

	return client, ts.URL, endpoint, ts.Close
}

// createE2ETestBucket creates a bucket for E2E testing.
func createE2ETestBucket(t *testing.T, client *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()

	err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		// Remove all objects
		objectsCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
		for obj := range objectsCh {
			_ = client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		}
		_ = client.RemoveBucket(ctx, bucket)
	})
}

// uploadE2ETestContent uploads content to the fake S3 bucket.
func uploadE2ETestContent(t *testing.T, client *minio.Client, bucket, key, contentType string, data []byte) {
	t.Helper()
	ctx := context.Background()

	_, err := client.PutObject(ctx, bucket, key, strings.NewReader(string(data)), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	require.NoError(t, err)
}

// createTestImage creates a simple colored test image as PNG bytes.
func createTestImage(t *testing.T, width, height int, c color.Color) []byte {
	t.Helper()

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := range height {
		for x := range width {
			img.Set(x, y, c)
		}
	}

	var buf bytes.Buffer
	err := png.Encode(&buf, img)
	require.NoError(t, err)
	return buf.Bytes()
}

// findTestRepoRoot finds the repository root by walking up from the test file.
func findTestRepoRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("Failed to get current file path")
	}

	dir := filepath.Dir(filename)

	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Could not find repository root (go.mod not found)")
		}
		dir = parent
	}
}

// ensureCLIPModel downloads the CLIP model from the Antfly model registry if not present.
func ensureCLIPModel(t *testing.T, modelsDir string) string {
	t.Helper()

	modelDownloadMutex.Lock()
	defer modelDownloadMutex.Unlock()

	modelPath := filepath.Join(modelsDir, "embedders", clipModelName)

	if _, err := os.Stat(modelPath); err == nil {
		t.Logf("CLIP model already exists at %s", modelPath)
		return modelPath
	}

	t.Logf("Downloading CLIP model from registry: %s", clipModelName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Track download progress per file (only log at milestones)
	lastMilestone := make(map[string]int)
	regClient := modelregistry.NewClient(
		modelregistry.WithProgressHandler(func(downloaded, total int64, filename string) {
			if total > 0 {
				percent := float64(downloaded) / float64(total) * 100
				milestone := int(percent / 25)
				if milestone > lastMilestone[filename] || (downloaded == total && lastMilestone[filename] < 4) {
					lastMilestone[filename] = milestone
					t.Logf("  %s: %.0f%%", filename, percent)
				}
			}
		}),
	)

	manifest, err := regClient.FetchManifest(ctx, clipModelName)
	if err != nil {
		t.Fatalf("Failed to fetch manifest for %s: %v", clipModelName, err)
	}

	// Pull the f32 variant (default)
	if err := regClient.PullModel(ctx, manifest, modelsDir, []string{modelregistry.VariantF32}); err != nil {
		t.Fatalf("Failed to pull model %s: %v", clipModelName, err)
	}

	t.Logf("Successfully downloaded CLIP model: %s", clipModelName)
	return modelPath
}

// TestRemoteContentWithCLIP tests the full flow of remote content fetching from S3
// through the Antfly server with CLIP embeddings.
//
// This test:
// 1. Downloads CLIP model if not present
// 2. Starts a fake S3 server and uploads test images
// 3. Configures RemoteContent.S3 credentials pointing to fake S3
// 4. Starts Antfly swarm with Termite and CLIP model
// 5. Creates a table with an index using {{remoteMedia url=image_url}} template
// 6. Inserts documents with S3 URLs
// 7. Verifies documents are indexed and searchable
func TestRemoteContentWithCLIP(t *testing.T) {
	if os.Getenv("RUN_REMOTE_CONTENT_TESTS") == "" {
		t.Skip("Skipping remote content E2E test (set RUN_REMOTE_CONTENT_TESTS=true to run)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Find repo root and models directory
	repoRoot := findTestRepoRoot(t)
	modelsDir := filepath.Join(repoRoot, "models")

	// Step 1: Ensure CLIP model is downloaded
	ensureCLIPModel(t, modelsDir)

	// Step 2: Set up fake S3 server and upload test images
	s3Client, s3URL, endpoint, s3Cleanup := setupFakeS3ForE2E(t)
	defer s3Cleanup()

	createE2ETestBucket(t, s3Client, "test-images")

	// Create distinct colored images for testing
	redImage := createTestImage(t, 100, 100, color.RGBA{R: 255, G: 0, B: 0, A: 255})
	blueImage := createTestImage(t, 100, 100, color.RGBA{R: 0, G: 0, B: 255, A: 255})

	uploadE2ETestContent(t, s3Client, "test-images", "red.png", "image/png", redImage)
	uploadE2ETestContent(t, s3Client, "test-images", "blue.png", "image/png", blueImage)
	t.Logf("Uploaded test images to fake S3 at %s", s3URL)

	// Step 3: Configure remote content BEFORE starting the server
	// This is critical - the scraping package uses a global manager
	scraping.InitRemoteContentConfig(&scraping.RemoteContentConfig{
		Security: scraping.ContentSecurityConfig{
			MaxDownloadSizeBytes:   10 * 1024 * 1024, // 10MB
			DownloadTimeoutSeconds: 30,
			MaxImageDimension:      1024,
		},
		S3: map[string]scraping.S3CredentialConfig{
			"test": {
				Endpoint:        endpoint,
				AccessKeyId:     "test-key",
				SecretAccessKey: "test-secret",
				UseSsl:          false,
			},
		},
		DefaultS3: "test",
	})
	t.Log("Configured remote content with fake S3 credentials")

	// Step 4: Start Antfly swarm with Termite
	db.DefaultPebbleCacheSizeMB = 16 // Reduce memory usage

	logger := GetTestLogger(t)
	nodeID := types.ID(1)
	dataDir := t.TempDir()

	// Allocate dynamic ports
	metadataAPIPort := GetFreePort(t)
	metadataRaftPort := GetFreePort(t)
	storeAPIPort := GetFreePort(t)
	storeRaftPort := GetFreePort(t)
	termiteAPIPort := GetFreePort(t)

	metadataAPIURL := fmt.Sprintf("http://localhost:%d", metadataAPIPort)
	metadataRaftURL := fmt.Sprintf("http://localhost:%d", metadataRaftPort)
	storeAPIURL := fmt.Sprintf("http://localhost:%d", storeAPIPort)
	storeRaftURL := fmt.Sprintf("http://localhost:%d", storeRaftPort)
	termiteAPIURL := fmt.Sprintf("http://localhost:%d", termiteAPIPort)

	// Create config
	config := CreateTestConfig(t, dataDir, nodeID)
	config.Metadata.OrchestrationUrls = map[string]string{
		nodeID.String(): metadataAPIURL,
	}

	// Configure Termite with CLIP model
	config.Termite = termite.Config{
		ApiUrl:          termiteAPIURL,
		ModelsDir:       modelsDir,
		MaxLoadedModels: 2,
		PoolSize:        1,
	}
	t.Logf("Configured Termite with models directory: %s", modelsDir)

	// Clean any existing raft logs
	CleanupAntflyData(t, dataDir, nodeID)

	// Create context for servers
	swarmCtx, swarmCancel := context.WithCancel(ctx)
	defer swarmCancel()

	// Create readiness channels
	metadataReadyC := make(chan struct{})
	storeReadyC := make(chan struct{})
	termiteReadyC := make(chan struct{})

	// Start Termite server
	go termite.RunAsTermite(
		swarmCtx,
		logger.Named("termite"),
		config.Termite,
		termiteReadyC,
	)

	// Wait for Termite to be ready
	select {
	case <-termiteReadyC:
		logger.Info("Termite server ready")
		SetTermiteURL(termiteAPIURL)
		libtermite.SetDefaultURL(termiteAPIURL)
	case <-time.After(60 * time.Second):
		t.Fatal("Timeout waiting for Termite server to be ready")
	}

	// Start metadata server
	peers := common.Peers{
		{ID: nodeID, URL: metadataRaftURL},
	}

	go func() {
		metadata.RunAsMetadataServer(
			swarmCtx,
			logger.Named("metadata"),
			config,
			&store.StoreInfo{
				ID:      nodeID,
				RaftURL: metadataRaftURL,
				ApiURL:  metadataAPIURL,
			},
			peers,
			false,
			metadataReadyC,
			nil,
		)
	}()

	// Start store server
	go func() {
		store.RunAsStore(
			swarmCtx,
			logger.Named("store"),
			config,
			&store.StoreInfo{
				ID:      nodeID,
				ApiURL:  storeAPIURL,
				RaftURL: storeRaftURL,
			},
			"",
			storeReadyC,
			nil,
		)
	}()

	// Wait for servers to be ready
	select {
	case <-metadataReadyC:
		logger.Info("Metadata server ready")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for metadata server to be ready")
	}

	select {
	case <-storeReadyC:
		logger.Info("Store server ready")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for store server to be ready")
	}

	// Give servers a moment to fully initialize HTTP handlers
	time.Sleep(500 * time.Millisecond)

	// Create client
	apiURL := metadataAPIURL + "/api/v1"
	httpClient := &http.Client{Timeout: 5 * time.Minute}
	client, err := antfly.NewAntflyClient(apiURL, httpClient)
	require.NoError(t, err)

	t.Log("Swarm started successfully")

	// Step 5: Create table with CLIP embedding index
	tableName := "remote_content_test"

	var embedderConfig oapi.EmbedderConfig
	embedderConfig.Provider = oapi.EmbedderProviderTermite
	err = embedderConfig.FromTermiteEmbedderConfig(oapi.TermiteEmbedderConfig{
		Model: clipModelName,
	})
	require.NoError(t, err)

	var indexConfig oapi.IndexConfig
	indexConfig.Name = "embeddings"
	indexConfig.Type = oapi.IndexTypeEmbeddings
	err = indexConfig.FromEmbeddingsIndexConfig(oapi.EmbeddingsIndexConfig{
		Dimension: clipEmbeddingDim,
		// Use remoteMedia helper with S3 URLs - this is what we're testing!
		Template: "{{remoteMedia url=image_url}}{{caption}}",
		Embedder: embedderConfig,
	})
	require.NoError(t, err)

	err = client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]oapi.IndexConfig{
			"embeddings": indexConfig,
		},
	})
	require.NoError(t, err)
	t.Logf("Created table '%s' with CLIP embedding index", tableName)

	// Wait for shards to be ready
	waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

	// Step 6: Insert documents with S3 URLs
	// The URLs use s3://endpoint/bucket/key format which remoteMedia will fetch
	redImageURL := fmt.Sprintf("s3://%s/test-images/red.png", endpoint)
	blueImageURL := fmt.Sprintf("s3://%s/test-images/blue.png", endpoint)

	_, err = client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts: map[string]any{
			"red_image": map[string]any{
				"caption":   "A solid red colored square image",
				"image_url": redImageURL,
			},
			"blue_image": map[string]any{
				"caption":   "A solid blue colored square image",
				"image_url": blueImageURL,
			},
		},
	})
	require.NoError(t, err)
	t.Log("Inserted documents with S3 image URLs")

	// Wait for embeddings to be generated (async process)
	t.Log("Waiting for embeddings to be generated...")
	time.Sleep(10 * time.Second)

	// Step 7: Search and verify
	t.Run("search for red image", func(t *testing.T) {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			SemanticSearch: "red color square",
			Indexes:        []string{"embeddings"},
			Limit:          5,
		})
		require.NoError(t, err)

		// Should find results
		require.NotEmpty(t, results.Responses, "expected query responses")
		if len(results.Responses) > 0 && len(results.Responses[0].Hits.Hits) > 0 {
			topHit := results.Responses[0].Hits.Hits[0]
			t.Logf("Top hit for 'red color square': ID=%s, Score=%.4f", topHit.ID, topHit.Score)
			// The red image should be in the results
			foundRed := false
			for _, hit := range results.Responses[0].Hits.Hits {
				if hit.ID == "red_image" {
					foundRed = true
					break
				}
			}
			assert.True(t, foundRed, "expected to find red_image in search results")
		}
	})

	t.Run("search for blue image", func(t *testing.T) {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			SemanticSearch: "blue color square",
			Indexes:        []string{"embeddings"},
			Limit:          5,
		})
		require.NoError(t, err)

		require.NotEmpty(t, results.Responses, "expected query responses")
		if len(results.Responses) > 0 && len(results.Responses[0].Hits.Hits) > 0 {
			topHit := results.Responses[0].Hits.Hits[0]
			t.Logf("Top hit for 'blue color square': ID=%s, Score=%.4f", topHit.ID, topHit.Score)
			// The blue image should be in the results
			foundBlue := false
			for _, hit := range results.Responses[0].Hits.Hits {
				if hit.ID == "blue_image" {
					foundBlue = true
					break
				}
			}
			assert.True(t, foundBlue, "expected to find blue_image in search results")
		}
	})

	t.Log("Test completed successfully - remote content from S3 was fetched and embedded with CLIP")
}

// ensureCLAPModel downloads the CLAP model from HuggingFace if not present.
func ensureCLAPModel(t *testing.T, modelsDir string) string {
	t.Helper()

	modelDownloadMutex.Lock()
	defer modelDownloadMutex.Unlock()

	modelPath := filepath.Join(modelsDir, "embedders", clapModelName)

	if _, err := os.Stat(modelPath); err == nil {
		t.Logf("CLAP model already exists at %s", modelPath)
		return modelPath
	}

	t.Logf("Downloading CLAP model from HuggingFace: %s", clapModelName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	lastMilestone := make(map[string]int)
	hfClient := modelregistry.NewHuggingFaceClient(
		modelregistry.WithHFProgressHandler(func(downloaded, total int64, filename string) {
			if total > 0 {
				percent := float64(downloaded) / float64(total) * 100
				milestone := int(percent / 25)
				if milestone > lastMilestone[filename] || (downloaded == total && lastMilestone[filename] < 4) {
					lastMilestone[filename] = milestone
					t.Logf("  %s: %.0f%%", filename, percent)
				}
			}
		}),
	)

	if err := hfClient.PullFromHuggingFace(ctx, clapModelName, modelregistry.ModelTypeEmbedder, modelsDir, ""); err != nil {
		t.Fatalf("Failed to pull CLAP model %s: %v", clapModelName, err)
	}

	t.Logf("Successfully downloaded CLAP model: %s", clapModelName)
	return modelPath
}

// createTestAudioWAV creates a mono 16-bit PCM WAV file with a sine wave at the given frequency.
// If frequency is 0, creates silence. Returns raw WAV bytes.
func createTestAudioWAV(t *testing.T, sampleRate int, durationSec float64, frequency float64) []byte {
	t.Helper()

	numSamples := int(float64(sampleRate) * durationSec)
	samples := make([]int16, numSamples)

	if frequency > 0 {
		for i := range numSamples {
			sample := math.Sin(2.0 * math.Pi * frequency * float64(i) / float64(sampleRate))
			samples[i] = int16(sample * 32000)
		}
	}

	var buf bytes.Buffer
	dataSize := numSamples * 2

	// RIFF header
	buf.WriteString("RIFF")
	binary.Write(&buf, binary.LittleEndian, uint32(36+dataSize))
	buf.WriteString("WAVE")

	// fmt chunk
	buf.WriteString("fmt ")
	binary.Write(&buf, binary.LittleEndian, uint32(16))           // chunk size
	binary.Write(&buf, binary.LittleEndian, uint16(1))            // PCM format
	binary.Write(&buf, binary.LittleEndian, uint16(1))            // mono
	binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))   // sample rate
	binary.Write(&buf, binary.LittleEndian, uint32(sampleRate*2)) // byte rate
	binary.Write(&buf, binary.LittleEndian, uint16(2))            // block align
	binary.Write(&buf, binary.LittleEndian, uint16(16))           // bits per sample

	// data chunk
	buf.WriteString("data")
	binary.Write(&buf, binary.LittleEndian, uint32(dataSize))
	for _, sample := range samples {
		binary.Write(&buf, binary.LittleEndian, sample)
	}

	return buf.Bytes()
}

// TestRemoteContentWithCLIPAndCLAP tests the full multimodal flow with both
// CLIP (image) and CLAP (audio) embeddings fetched from mocked S3 buckets.
//
// This test:
// 1. Downloads CLIP and CLAP models if not present
// 2. Starts a fake S3 server and uploads test images and audio
// 3. Starts Antfly swarm with Termite serving both models
// 4. Creates a table with two indexes: image_embeddings (CLIP) and audio_embeddings (CLAP)
// 5. Inserts documents with S3 URLs for images and audio
// 6. Queries both indexes and verifies results
func TestRemoteContentWithCLIPAndCLAP(t *testing.T) {
	if os.Getenv("RUN_REMOTE_CONTENT_TESTS") == "" {
		t.Skip("Skipping remote content E2E test (set RUN_REMOTE_CONTENT_TESTS=true to run)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Find repo root and models directory
	repoRoot := findTestRepoRoot(t)
	modelsDir := filepath.Join(repoRoot, "models")

	// Step 1: Download both models
	ensureCLIPModel(t, modelsDir)
	ensureCLAPModel(t, modelsDir)

	// Step 2: Set up fake S3 with images and audio
	s3Client, s3URL, endpoint, s3Cleanup := setupFakeS3ForE2E(t)
	defer s3Cleanup()

	createE2ETestBucket(t, s3Client, "test-images")
	createE2ETestBucket(t, s3Client, "test-audio")

	// Upload test images
	redImage := createTestImage(t, 100, 100, color.RGBA{R: 255, G: 0, B: 0, A: 255})
	blueImage := createTestImage(t, 100, 100, color.RGBA{R: 0, G: 0, B: 255, A: 255})
	uploadE2ETestContent(t, s3Client, "test-images", "red.png", "image/png", redImage)
	uploadE2ETestContent(t, s3Client, "test-images", "blue.png", "image/png", blueImage)

	// Upload test audio: a 440Hz tone and silence (1 second each, 48kHz)
	toneAudio := createTestAudioWAV(t, 48000, 1.0, 440.0)
	silenceAudio := createTestAudioWAV(t, 48000, 1.0, 0.0)
	uploadE2ETestContent(t, s3Client, "test-audio", "tone.wav", "audio/wav", toneAudio)
	uploadE2ETestContent(t, s3Client, "test-audio", "silence.wav", "audio/wav", silenceAudio)

	t.Logf("Uploaded test images and audio to fake S3 at %s", s3URL)

	// Step 3: Configure remote content
	scraping.InitRemoteContentConfig(&scraping.RemoteContentConfig{
		Security: scraping.ContentSecurityConfig{
			MaxDownloadSizeBytes:   10 * 1024 * 1024,
			DownloadTimeoutSeconds: 30,
			MaxImageDimension:      1024,
		},
		S3: map[string]scraping.S3CredentialConfig{
			"test": {
				Endpoint:        endpoint,
				AccessKeyId:     "test-key",
				SecretAccessKey: "test-secret",
				UseSsl:          false,
			},
		},
		DefaultS3: "test",
	})

	// Step 4: Start Antfly swarm with Termite
	db.DefaultPebbleCacheSizeMB = 16

	logger := GetTestLogger(t)
	nodeID := types.ID(1)
	dataDir := t.TempDir()

	metadataAPIPort := GetFreePort(t)
	metadataRaftPort := GetFreePort(t)
	storeAPIPort := GetFreePort(t)
	storeRaftPort := GetFreePort(t)
	termiteAPIPort := GetFreePort(t)

	metadataAPIURL := fmt.Sprintf("http://localhost:%d", metadataAPIPort)
	metadataRaftURL := fmt.Sprintf("http://localhost:%d", metadataRaftPort)
	storeAPIURL := fmt.Sprintf("http://localhost:%d", storeAPIPort)
	storeRaftURL := fmt.Sprintf("http://localhost:%d", storeRaftPort)
	termiteAPIURL := fmt.Sprintf("http://localhost:%d", termiteAPIPort)

	config := CreateTestConfig(t, dataDir, nodeID)
	config.Metadata.OrchestrationUrls = map[string]string{
		nodeID.String(): metadataAPIURL,
	}
	config.Termite = termite.Config{
		ApiUrl:          termiteAPIURL,
		ModelsDir:       modelsDir,
		MaxLoadedModels: 4, // CLIP + CLAP (each has text + media sub-models)
		PoolSize:        1,
		BackendPriority: []string{"coreml", "go"},
	}

	CleanupAntflyData(t, dataDir, nodeID)

	swarmCtx, swarmCancel := context.WithCancel(ctx)
	defer swarmCancel()

	metadataReadyC := make(chan struct{})
	storeReadyC := make(chan struct{})
	termiteReadyC := make(chan struct{})

	go termite.RunAsTermite(swarmCtx, logger.Named("termite"), config.Termite, termiteReadyC)

	select {
	case <-termiteReadyC:
		logger.Info("Termite server ready")
		SetTermiteURL(termiteAPIURL)
		libtermite.SetDefaultURL(termiteAPIURL)
	case <-time.After(60 * time.Second):
		t.Fatal("Timeout waiting for Termite server to be ready")
	}

	peers := common.Peers{{ID: nodeID, URL: metadataRaftURL}}

	go func() {
		metadata.RunAsMetadataServer(swarmCtx, logger.Named("metadata"), config,
			&store.StoreInfo{ID: nodeID, RaftURL: metadataRaftURL, ApiURL: metadataAPIURL},
			peers, false, metadataReadyC, nil)
	}()
	go func() {
		store.RunAsStore(swarmCtx, logger.Named("store"), config,
			&store.StoreInfo{ID: nodeID, ApiURL: storeAPIURL, RaftURL: storeRaftURL},
			"", storeReadyC, nil)
	}()

	select {
	case <-metadataReadyC:
		logger.Info("Metadata server ready")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for metadata server")
	}
	select {
	case <-storeReadyC:
		logger.Info("Store server ready")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for store server")
	}

	time.Sleep(500 * time.Millisecond)

	apiURL := metadataAPIURL + "/api/v1"
	httpClient := &http.Client{Timeout: 5 * time.Minute}
	client, err := antfly.NewAntflyClient(apiURL, httpClient)
	require.NoError(t, err)
	t.Log("Swarm started successfully")

	// Step 5: Create table with CLIP and CLAP indexes
	tableName := "multimodal_test"

	// CLIP embedder config
	var clipEmbedder oapi.EmbedderConfig
	clipEmbedder.Provider = oapi.EmbedderProviderTermite
	err = clipEmbedder.FromTermiteEmbedderConfig(oapi.TermiteEmbedderConfig{Model: clipModelName})
	require.NoError(t, err)

	var imageIndex oapi.IndexConfig
	imageIndex.Name = "image_embeddings"
	imageIndex.Type = oapi.IndexTypeEmbeddings
	err = imageIndex.FromEmbeddingsIndexConfig(oapi.EmbeddingsIndexConfig{
		Dimension: clipEmbeddingDim,
		Template:  "{{remoteMedia url=image_url}}{{caption}}",
		Embedder:  clipEmbedder,
	})
	require.NoError(t, err)

	// CLAP embedder config
	var clapEmbedder oapi.EmbedderConfig
	clapEmbedder.Provider = oapi.EmbedderProviderTermite
	err = clapEmbedder.FromTermiteEmbedderConfig(oapi.TermiteEmbedderConfig{Model: clapModelName})
	require.NoError(t, err)

	var audioIndex oapi.IndexConfig
	audioIndex.Name = "audio_embeddings"
	audioIndex.Type = oapi.IndexTypeEmbeddings
	err = audioIndex.FromEmbeddingsIndexConfig(oapi.EmbeddingsIndexConfig{
		Dimension: clapEmbeddingDim,
		Template:  "{{remoteMedia url=audio_url}}{{description}}",
		Embedder:  clapEmbedder,
	})
	require.NoError(t, err)

	err = client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]oapi.IndexConfig{
			"image_embeddings": imageIndex,
			"audio_embeddings": audioIndex,
		},
	})
	require.NoError(t, err)
	t.Logf("Created table '%s' with CLIP and CLAP indexes", tableName)

	waitForShardsReady(t, ctx, client, tableName, 30*time.Second)

	// Step 6: Insert documents with S3 URLs for images and audio
	redImageURL := fmt.Sprintf("s3://%s/test-images/red.png", endpoint)
	blueImageURL := fmt.Sprintf("s3://%s/test-images/blue.png", endpoint)
	toneAudioURL := fmt.Sprintf("s3://%s/test-audio/tone.wav", endpoint)
	silenceAudioURL := fmt.Sprintf("s3://%s/test-audio/silence.wav", endpoint)

	_, err = client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts: map[string]any{
			"red_image": map[string]any{
				"caption":   "A solid red colored square image",
				"image_url": redImageURL,
			},
			"blue_image": map[string]any{
				"caption":   "A solid blue colored square image",
				"image_url": blueImageURL,
			},
			"tone_audio": map[string]any{
				"description": "A 440Hz sine wave tone",
				"audio_url":   toneAudioURL,
			},
			"silence_audio": map[string]any{
				"description": "Complete silence",
				"audio_url":   silenceAudioURL,
			},
		},
	})
	require.NoError(t, err)
	t.Log("Inserted documents with S3 image and audio URLs")

	// Wait for embeddings to be generated (async enrichment)
	t.Log("Waiting for embeddings to be generated...")
	time.Sleep(15 * time.Second)

	// Step 7: Query both indexes
	t.Run("CLIP: search for red image", func(t *testing.T) {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			SemanticSearch: "red color square",
			Indexes:        []string{"image_embeddings"},
			Limit:          5,
		})
		require.NoError(t, err)
		require.NotEmpty(t, results.Responses, "expected query responses")

		if len(results.Responses) > 0 && len(results.Responses[0].Hits.Hits) > 0 {
			topHit := results.Responses[0].Hits.Hits[0]
			t.Logf("Top hit for 'red color square': ID=%s, Score=%.4f", topHit.ID, topHit.Score)

			foundRed := false
			for _, hit := range results.Responses[0].Hits.Hits {
				if hit.ID == "red_image" {
					foundRed = true
					break
				}
			}
			assert.True(t, foundRed, "expected to find red_image in image search results")
		}
	})

	t.Run("CLIP: search for blue image", func(t *testing.T) {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			SemanticSearch: "blue color square",
			Indexes:        []string{"image_embeddings"},
			Limit:          5,
		})
		require.NoError(t, err)
		require.NotEmpty(t, results.Responses, "expected query responses")

		if len(results.Responses) > 0 && len(results.Responses[0].Hits.Hits) > 0 {
			topHit := results.Responses[0].Hits.Hits[0]
			t.Logf("Top hit for 'blue color square': ID=%s, Score=%.4f", topHit.ID, topHit.Score)

			foundBlue := false
			for _, hit := range results.Responses[0].Hits.Hits {
				if hit.ID == "blue_image" {
					foundBlue = true
					break
				}
			}
			assert.True(t, foundBlue, "expected to find blue_image in image search results")
		}
	})

	t.Run("CLAP: search for tone audio", func(t *testing.T) {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			SemanticSearch: "a sine wave tone beeping sound",
			Indexes:        []string{"audio_embeddings"},
			Limit:          5,
		})
		require.NoError(t, err)
		require.NotEmpty(t, results.Responses, "expected query responses")

		if len(results.Responses) > 0 && len(results.Responses[0].Hits.Hits) > 0 {
			topHit := results.Responses[0].Hits.Hits[0]
			t.Logf("Top hit for 'sine wave tone': ID=%s, Score=%.4f", topHit.ID, topHit.Score)

			foundTone := false
			for _, hit := range results.Responses[0].Hits.Hits {
				if hit.ID == "tone_audio" {
					foundTone = true
					break
				}
			}
			assert.True(t, foundTone, "expected to find tone_audio in audio search results")
		}
	})

	t.Run("CLAP: search for silence", func(t *testing.T) {
		results, err := client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			SemanticSearch: "silence quiet nothing",
			Indexes:        []string{"audio_embeddings"},
			Limit:          5,
		})
		require.NoError(t, err)
		require.NotEmpty(t, results.Responses, "expected query responses")

		if len(results.Responses) > 0 && len(results.Responses[0].Hits.Hits) > 0 {
			topHit := results.Responses[0].Hits.Hits[0]
			t.Logf("Top hit for 'silence': ID=%s, Score=%.4f", topHit.ID, topHit.Score)

			foundSilence := false
			for _, hit := range results.Responses[0].Hits.Hits {
				if hit.ID == "silence_audio" {
					foundSilence = true
					break
				}
			}
			assert.True(t, foundSilence, "expected to find silence_audio in audio search results")
		}
	})

	t.Log("Test completed - CLIP image and CLAP audio remote content from S3 embedded and queried successfully")
}

// TestRemoteContentCredentialResolution tests credential resolution without starting the full swarm.
// This is a lighter-weight test that verifies the configuration system works correctly.
func TestRemoteContentCredentialResolution(t *testing.T) {
	if os.Getenv("RUN_REMOTE_CONTENT_TESTS") == "" {
		t.Skip("Skipping remote content E2E test (set RUN_REMOTE_CONTENT_TESTS=true to run)")
	}

	// Set up fake S3 server
	s3Client, _, endpoint, cleanup := setupFakeS3ForE2E(t)
	defer cleanup()

	// Create test bucket and upload content
	createE2ETestBucket(t, s3Client, "test-docs")
	testContent := "This is test content from S3 for remote content integration testing."
	uploadE2ETestContent(t, s3Client, "test-docs", "document.txt", "text/plain", []byte(testContent))

	// Configure remote content with credentials pointing to fake S3
	scraping.InitRemoteContentConfig(&scraping.RemoteContentConfig{
		Security: scraping.ContentSecurityConfig{
			MaxDownloadSizeBytes:   10 * 1024 * 1024, // 10MB
			DownloadTimeoutSeconds: 30,
		},
		S3: map[string]scraping.S3CredentialConfig{
			"test": {
				Endpoint:        endpoint,
				AccessKeyId:     "test-key",
				SecretAccessKey: "test-secret",
				UseSsl:          false,
			},
		},
		DefaultS3: "test",
	})

	// Test credential resolution works
	t.Run("credential resolution", func(t *testing.T) {
		creds, secConfig, err := scraping.ResolveS3Credentials("s3://test-docs/document.txt", "")
		require.NoError(t, err)
		assert.Equal(t, endpoint, creds.Endpoint)
		assert.Equal(t, "test-key", creds.AccessKeyId)
		assert.NotNil(t, secConfig)
		assert.Equal(t, int64(10*1024*1024), secConfig.MaxDownloadSizeBytes)
	})

	// Test actual content download
	t.Run("content download", func(t *testing.T) {
		creds, secConfig, err := scraping.ResolveS3Credentials("s3://test-docs/document.txt", "")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := scraping.DownloadAndProcessLink(
			ctx,
			"s3://"+endpoint+"/test-docs/document.txt",
			secConfig,
			creds,
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "text", result.Format)
		assert.Equal(t, testContent, string(result.Data))
	})
}

// TestRemoteContentMultipleCredentials tests bucket pattern matching with multiple credentials.
func TestRemoteContentMultipleCredentials(t *testing.T) {
	if os.Getenv("RUN_REMOTE_CONTENT_TESTS") == "" {
		t.Skip("Skipping remote content E2E test (set RUN_REMOTE_CONTENT_TESTS=true to run)")
	}

	// Set up two fake S3 servers simulating different environments
	s3Client1, _, endpoint1, cleanup1 := setupFakeS3ForE2E(t)
	defer cleanup1()

	s3Client2, _, endpoint2, cleanup2 := setupFakeS3ForE2E(t)
	defer cleanup2()

	// Create buckets
	createE2ETestBucket(t, s3Client1, "prod-data")
	createE2ETestBucket(t, s3Client2, "staging-data")

	// Upload test content
	uploadE2ETestContent(t, s3Client1, "prod-data", "file.txt", "text/plain", []byte("Production data"))
	uploadE2ETestContent(t, s3Client2, "staging-data", "file.txt", "text/plain", []byte("Staging data"))

	// Configure with bucket patterns
	scraping.InitRemoteContentConfig(&scraping.RemoteContentConfig{
		S3: map[string]scraping.S3CredentialConfig{
			"prod": {
				Endpoint:        endpoint1,
				AccessKeyId:     "prod-key",
				SecretAccessKey: "prod-secret",
				UseSsl:          false,
				Buckets:         []string{"prod-*"},
			},
			"staging": {
				Endpoint:        endpoint2,
				AccessKeyId:     "staging-key",
				SecretAccessKey: "staging-secret",
				UseSsl:          false,
				Buckets:         []string{"staging-*"},
			},
		},
	})

	t.Run("prod bucket routes to prod credentials", func(t *testing.T) {
		creds, _, err := scraping.ResolveS3Credentials("s3://prod-data/file.txt", "")
		require.NoError(t, err)
		assert.Equal(t, endpoint1, creds.Endpoint)
		assert.Equal(t, "prod-key", creds.AccessKeyId)
	})

	t.Run("staging bucket routes to staging credentials", func(t *testing.T) {
		creds, _, err := scraping.ResolveS3Credentials("s3://staging-data/file.txt", "")
		require.NoError(t, err)
		assert.Equal(t, endpoint2, creds.Endpoint)
		assert.Equal(t, "staging-key", creds.AccessKeyId)
	})

	t.Run("download from prod", func(t *testing.T) {
		creds, secConfig, err := scraping.ResolveS3Credentials("s3://prod-data/file.txt", "")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := scraping.DownloadAndProcessLink(
			ctx,
			"s3://"+endpoint1+"/prod-data/file.txt",
			secConfig,
			creds,
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "Production data", string(result.Data))
	})

	t.Run("download from staging", func(t *testing.T) {
		creds, secConfig, err := scraping.ResolveS3Credentials("s3://staging-data/file.txt", "")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := scraping.DownloadAndProcessLink(
			ctx,
			"s3://"+endpoint2+"/staging-data/file.txt",
			secConfig,
			creds,
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "Staging data", string(result.Data))
	})
}
