// Copyright 2025 Antfly, Inc.
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

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package termite

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/libaf/s3"
	"github.com/antflydb/antfly/pkg/libaf/scraping"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/ner"
	"github.com/antflydb/antfly/pkg/termite/lib/reading"
	"github.com/antflydb/antfly/pkg/termite/lib/transcribing"
	"go.uber.org/zap"
)

type TermiteNode struct {
	logger *zap.Logger

	client *http.Client

	embedderRegistry      EmbedderRegistryInterface
	readerRegistry        ReaderRegistryInterface
	transcriberRegistry   TranscriberRegistryInterface
	chunker               ChunkerInterface
	rerankerRegistry      RerankerRegistryInterface
	generatorRegistry     GeneratorRegistryInterface
	nerRegistry           NERRegistryInterface
	seq2seqRegistry       Seq2SeqRegistryInterface
	classifierRegistry    ClassifierRegistryInterface
	contentSecurityConfig *scraping.ContentSecurityConfig
	s3Credentials         *s3.Credentials

	// Request queue for backpressure control
	requestQueue *RequestQueue

	// Result caches for inference deduplication
	embeddingCache       *ResultCache[[][]float32]
	sparseEmbeddingCache *ResultCache[[]embeddings.SparseVector]
	rerankingCache       *ResultCache[[]float32]
	nerCache             *ResultCache[[][]ner.Entity]
	readingCache         *ResultCache[[]reading.Result]
	transcriptionCache   *ResultCache[*transcribing.Result]

	// allowDownloads controls whether the dashboard shows model download commands
	allowDownloads bool

	// cleanups are invoked in LIFO order by Close().
	cleanups []func() error
}

// Close releases all resources held by the node. Safe to call multiple times.
func (n *TermiteNode) Close() error {
	var firstErr error
	for i := len(n.cleanups) - 1; i >= 0; i-- {
		if err := n.cleanups[i](); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	n.cleanups = nil
	return firstErr
}

// corsMiddleware adds permissive CORS headers for the Termite API
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept, Origin")
		w.Header().Set("Access-Control-Max-Age", "3600")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// DefaultShutdownTimeout is the default time to wait for graceful shutdown
const DefaultShutdownTimeout = 30 * time.Second

// RunAsTermite runs a standalone Termite HTTP server bound to config.ApiUrl.
// If readyC is non-nil, it will be closed when the server is ready to accept requests.
func RunAsTermite(ctx context.Context, zl *zap.Logger, config Config, readyC chan struct{}) {
	zl = zl.Named("termite")

	u, err := url.Parse(config.ApiUrl)
	if err != nil {
		zl.Fatal("Invalid API URL", zap.String("url", config.ApiUrl), zap.Error(err))
	}

	node := NewTermiteNode(ctx, zl, config)
	defer func() { _ = node.Close() }()

	srv := &http.Server{
		Handler:     node.APIHandler(),
		ReadTimeout: 540 * time.Second,
	}

	// Bind the socket before starting the server goroutine so readyC is only
	// closed after the port is actually listening.
	ln, err := net.Listen("tcp", u.Host)
	if err != nil {
		zl.Fatal("Failed to bind address", zap.String("address", u.Host), zap.Error(err))
	}

	// Signal readiness now that the socket is bound
	if readyC != nil {
		close(readyC)
	}

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		zl.Info("Termite's api server starting", zap.String("address", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	// Wait for context cancellation or server error
	select {
	case err := <-serverErr:
		if err != nil {
			zl.Fatal("HTTP server error", zap.Error(err))
		}
	case <-ctx.Done():
		zl.Info("Shutdown signal received, starting graceful shutdown...")
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
	defer shutdownCancel()

	// Stop accepting new connections
	srv.SetKeepAlivesEnabled(false)

	// Attempt graceful shutdown
	if err := srv.Shutdown(shutdownCtx); err != nil {
		zl.Warn("Graceful shutdown failed, forcing close",
			zap.Error(err),
			zap.Duration("timeout", DefaultShutdownTimeout))
		_ = srv.Close()
	} else {
		zl.Info("Graceful shutdown completed successfully")
	}

	zl.Info("HTTP server stopped")
}

// NewTermiteNode constructs a fully-initialized TermiteNode from config. All
// registries, caches, and the session manager are started eagerly; call
// node.Close() to release them. Fatal logs any initialization failure.
func NewTermiteNode(ctx context.Context, zl *zap.Logger, config Config) *TermiteNode {
	zl.Info("Starting termite node", zap.Any("config", config))

	var (
		err      error
		cleanups []func() error
	)

	// Parse backend priority (supports "backend" or "backend:device" format)
	var backendPriority []backends.BackendSpec
	if len(config.BackendPriority) > 0 {
		var err error
		backendPriority, err = backends.ParseBackendPriority(config.BackendPriority)
		if err != nil {
			zl.Fatal("Invalid backend_priority configuration", zap.Error(err))
		}
		// Also set global priority for backward compatibility
		globalPriority := make([]backends.BackendType, 0, len(backendPriority))
		for _, spec := range backendPriority {
			globalPriority = append(globalPriority, spec.Backend)
		}
		backends.SetPriority(globalPriority)
		zl.Info("Backend priority configured", zap.Any("priority", config.BackendPriority))
	}

	// Log available backends
	availableBackends := backends.ListAvailable()
	backendNames := make([]string, 0, len(availableBackends))
	for _, b := range availableBackends {
		backendNames = append(backendNames, b.Name())
	}
	zl.Info("Available inference backends", zap.Strings("backends", backendNames))

	// Detect and log GPU info, set metrics
	gpuInfo := backends.DetectGPU()
	zl.Info("GPU detection complete",
		zap.Bool("available", gpuInfo.Available),
		zap.String("type", gpuInfo.Type),
		zap.String("device", gpuInfo.DeviceName))

	// Parse keep_alive duration
	// Default to 5 minutes like Ollama - lazy loading is the default behavior.
	// Set keep_alive to "0" to explicitly enable eager loading (all models loaded at startup).
	const defaultKeepAlive = 5 * time.Minute
	var keepAlive time.Duration
	if config.KeepAlive == "0" {
		// Explicit eager loading
		keepAlive = 0
		zl.Info("Eager loading mode (all models loaded at startup)")
	} else if config.KeepAlive != "" {
		keepAlive, err = time.ParseDuration(config.KeepAlive)
		if err != nil {
			zl.Fatal("Invalid keep_alive duration", zap.String("keep_alive", config.KeepAlive), zap.Error(err))
		}
		zl.Info("Lazy loading enabled",
			zap.Duration("keep_alive", keepAlive),
			zap.Int("max_loaded_models", config.MaxLoadedModels))
	} else {
		// Default to lazy loading with 5 minute keep_alive (Ollama-compatible)
		keepAlive = defaultKeepAlive
		zl.Info("Lazy loading enabled (default)",
			zap.Duration("keep_alive", keepAlive),
			zap.Int("max_loaded_models", config.MaxLoadedModels))
	}

	// Compute model subdirectory paths from models_dir
	var embedderModelsDir, chunkerModelsDir, rerankerModelsDir, generatorModelsDir, readerModelsDir, transcriberModelsDir string
	if config.ModelsDir != "" {
		embedderModelsDir = filepath.Join(config.ModelsDir, "embedders")
		chunkerModelsDir = filepath.Join(config.ModelsDir, "chunkers")
		rerankerModelsDir = filepath.Join(config.ModelsDir, "rerankers")
		generatorModelsDir = filepath.Join(config.ModelsDir, "generators")
		readerModelsDir = filepath.Join(config.ModelsDir, "readers")
		transcriberModelsDir = filepath.Join(config.ModelsDir, "transcribers")
	}

	// Create session manager for multi-backend support
	// SessionManager handles backend selection per-model and manages sessions.
	// IMPORTANT: ONNX Runtime backend allows only ONE session at a time.
	// SessionManager enforces this by sharing sessions within each backend type.
	var sessionManager *backends.SessionManager
	hasModels := config.ModelsDir != ""

	if hasModels {
		sessionManager = backends.NewSessionManager()
		cleanups = append(cleanups, sessionManager.Close)

		// Configure session manager with backend priority (includes device preferences)
		if len(backendPriority) > 0 {
			sessionManager.SetPriority(backendPriority)
		}

		defaultBackend := backends.GetDefaultBackend()
		if defaultBackend != nil {
			zl.Info("Session manager initialized",
				zap.String("default_backend", defaultBackend.Name()))
		} else {
			zl.Warn("No inference backends available")
		}
	}

	// Create global model budget for cross-registry LRU eviction
	budget := NewModelBudget(uint64(config.MaxLoadedModels), zl.Named("budget"))

	// Initialize chunker with optional model directory support
	// If models_dir is set in config, Termite will discover and load chunker models
	// If not set, Termite falls back to semantic-only chunking
	cachedChunker, err := NewCachedChunker(chunkerModelsDir, sessionManager, config.PoolSize, keepAlive, uint64(config.MaxLoadedModels), budget, zl.Named("chunker"))
	if err != nil {
		zl.Fatal("Failed to initialize chunker", zap.Error(err))
	}
	cleanups = append(cleanups, cachedChunker.Close)

	// Initialize embedder registry (lazy loading with TTL-based unloading)
	embedderRegistry, err := NewEmbedderRegistry(
		EmbedderConfig{
			ModelsDir:       embedderModelsDir,
			KeepAlive:       keepAlive,
			MaxLoadedModels: uint64(config.MaxLoadedModels),
			PoolSize:        config.PoolSize,
		},
		sessionManager,
		budget,
		zl.Named("embedder"),
	)
	if err != nil {
		zl.Fatal("Failed to initialize embedder registry", zap.Error(err))
	}
	cleanups = append(cleanups, embedderRegistry.Close)

	// Apply per-model loading strategies
	// Models with "eager" strategy are pinned (never evicted)
	if len(config.ModelStrategies) > 0 {
		var eagerModels []string
		for modelName, strategy := range config.ModelStrategies {
			if strategy == ConfigModelStrategiesEager {
				eagerModels = append(eagerModels, modelName)
			}
		}
		if len(eagerModels) > 0 {
			zl.Info("Pinning eager models (will not be evicted)",
				zap.Strings("models", eagerModels))
			for _, modelName := range eagerModels {
				if err := embedderRegistry.Pin(modelName); err != nil {
					zl.Warn("Failed to pin model",
						zap.String("model", modelName),
						zap.Error(err))
				}
			}
		}
	}

	// Preload specified models at startup (Ollama-compatible)
	// Note: This preloads models that aren't already pinned
	if len(config.Preload) > 0 {
		if err := embedderRegistry.Preload(config.Preload); err != nil {
			zl.Warn("Some models failed to preload", zap.Error(err))
		}
	}

	// Initialize reranker registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	// Always create the registry so built-in rerankers are available
	rerankerRegistry, err := NewRerankerRegistry(
		RerankerConfig{
			ModelsDir:       rerankerModelsDir,
			KeepAlive:       keepAlive,
			MaxLoadedModels: uint64(config.MaxLoadedModels),
			PoolSize:        config.PoolSize,
		},
		sessionManager,
		budget,
		zl.Named("reranker"),
	)
	if err != nil {
		zl.Fatal("Failed to initialize reranker registry", zap.Error(err))
	}
	cleanups = append(cleanups, rerankerRegistry.Close)

	// If eager loading is requested, preload all models
	if keepAlive == 0 {
		if err := rerankerRegistry.PreloadAll(); err != nil {
			zl.Warn("Failed to preload some reranker models", zap.Error(err))
		}
	}

	// Initialize generator registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	var generatorRegistry *GeneratorRegistry
	if generatorModelsDir != "" {
		generatorRegistry, err = NewGeneratorRegistry(
			GeneratorConfig{
				ModelsDir:       generatorModelsDir,
				KeepAlive:       keepAlive,
				MaxLoadedModels: uint64(config.MaxLoadedModels),
			},
			sessionManager,
			budget,
			zl.Named("generator"),
		)
		if err != nil {
			zl.Fatal("Failed to initialize generator registry", zap.Error(err))
		}
		cleanups = append(cleanups, generatorRegistry.Close)

		// If eager loading is requested, preload all models
		if keepAlive == 0 {
			if err := generatorRegistry.PreloadAll(); err != nil {
				zl.Warn("Failed to preload some generator models", zap.Error(err))
			}
		}
	}

	// Initialize NER registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	var nerRegistry *NERRegistry
	var nerModelsDir string
	if config.ModelsDir != "" {
		nerModelsDir = filepath.Join(config.ModelsDir, "recognizers")
	}
	if nerModelsDir != "" {
		nerRegistry, err = NewNERRegistry(
			NERConfig{
				ModelsDir:       nerModelsDir,
				KeepAlive:       keepAlive,
				MaxLoadedModels: uint64(config.MaxLoadedModels),
				PoolSize:        config.PoolSize,
			},
			sessionManager,
			budget,
			zl.Named("ner"),
		)
		if err != nil {
			zl.Fatal("Failed to initialize NER registry", zap.Error(err))
		}
		cleanups = append(cleanups, nerRegistry.Close)

		// If eager loading is requested, preload all models
		if keepAlive == 0 {
			if err := nerRegistry.PreloadAll(); err != nil {
				zl.Warn("Failed to preload some NER models", zap.Error(err))
			}
		}
	}

	// Initialize Seq2Seq registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	var seq2seqRegistry *Seq2SeqRegistry
	var seq2seqModelsDir string
	if config.ModelsDir != "" {
		seq2seqModelsDir = filepath.Join(config.ModelsDir, "rewriters")
	}
	if seq2seqModelsDir != "" {
		seq2seqRegistry, err = NewSeq2SeqRegistry(
			Seq2SeqConfig{
				ModelsDir:       seq2seqModelsDir,
				KeepAlive:       keepAlive,
				MaxLoadedModels: uint64(config.MaxLoadedModels),
			},
			sessionManager,
			budget,
			zl.Named("seq2seq"),
		)
		if err != nil {
			zl.Fatal("Failed to initialize Seq2Seq registry", zap.Error(err))
		}
		cleanups = append(cleanups, seq2seqRegistry.Close)

		// If eager loading is requested, preload all models
		if keepAlive == 0 {
			if err := seq2seqRegistry.PreloadAll(); err != nil {
				zl.Warn("Failed to preload some Seq2Seq models", zap.Error(err))
			}
		}
	}

	// Initialize classifier registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	var classifierRegistry *ClassifierRegistry
	var classifierModelsDir string
	if config.ModelsDir != "" {
		classifierModelsDir = filepath.Join(config.ModelsDir, "classifiers")
	}
	if classifierModelsDir != "" {
		classifierRegistry, err = NewClassifierRegistry(
			ClassifierConfig{
				ModelsDir:       classifierModelsDir,
				KeepAlive:       keepAlive,
				MaxLoadedModels: uint64(config.MaxLoadedModels),
				PoolSize:        config.PoolSize,
			},
			sessionManager,
			budget,
			zl.Named("classifier"),
		)
		if err != nil {
			zl.Fatal("Failed to initialize classifier registry", zap.Error(err))
		}
		cleanups = append(cleanups, classifierRegistry.Close)

		// If eager loading is requested, preload all models
		if keepAlive == 0 {
			if err := classifierRegistry.Preload(classifierRegistry.List()); err != nil {
				zl.Warn("Failed to preload some classifier models", zap.Error(err))
			}
		}
	}

	// Initialize reader registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	var readerRegistry *ReaderRegistry
	if readerModelsDir != "" {
		readerRegistry, err = NewReaderRegistry(
			ReaderConfig{
				ModelsDir:       readerModelsDir,
				KeepAlive:       keepAlive,
				MaxLoadedModels: uint64(config.MaxLoadedModels),
				PoolSize:        config.PoolSize,
			},
			sessionManager,
			budget,
			zl.Named("reader"),
		)
		if err != nil {
			zl.Fatal("Failed to initialize reader registry", zap.Error(err))
		}
		cleanups = append(cleanups, readerRegistry.Close)

		// If eager loading is requested, preload all models
		if keepAlive == 0 {
			if err := readerRegistry.PreloadAll(); err != nil {
				zl.Warn("Failed to preload some reader models", zap.Error(err))
			}
		}
	}

	// Initialize transcriber registry with lazy loading
	// Models are discovered at startup but only loaded on first request
	var transcriberRegistry *TranscriberRegistry
	if transcriberModelsDir != "" {
		transcriberRegistry, err = NewTranscriberRegistry(
			TranscriberConfig{
				ModelsDir:       transcriberModelsDir,
				KeepAlive:       keepAlive,
				MaxLoadedModels: uint64(config.MaxLoadedModels),
				PoolSize:        config.PoolSize,
			},
			sessionManager,
			budget,
			zl.Named("transcriber"),
		)
		if err != nil {
			zl.Fatal("Failed to initialize transcriber registry", zap.Error(err))
		}
		cleanups = append(cleanups, transcriberRegistry.Close)

		// If eager loading is requested, preload all models
		if keepAlive == 0 {
			if err := transcriberRegistry.PreloadAll(); err != nil {
				zl.Warn("Failed to preload some transcriber models", zap.Error(err))
			}
		}
	}

	t := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     6 * time.Minute,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
	}
	client := &http.Client{
		Timeout:   time.Second * 540,
		Transport: t,
	}
	// Build content security config - use config value or fall back to defaults
	var contentSecurityConfig *scraping.ContentSecurityConfig
	if config.ContentSecurity.MaxDownloadSizeBytes != 0 || config.ContentSecurity.DownloadTimeoutSeconds != 0 || len(config.ContentSecurity.AllowedHosts) > 0 {
		contentSecurityConfig = &config.ContentSecurity
	} else {
		// Default secure settings
		contentSecurityConfig = &scraping.ContentSecurityConfig{
			BlockPrivateIps:        true,
			MaxDownloadSizeBytes:   104857600, // 100MB
			DownloadTimeoutSeconds: 30,
		}
	}

	// Initialize request queue for backpressure control
	var requestTimeout time.Duration
	if config.RequestTimeout != "" && config.RequestTimeout != "0" {
		requestTimeout, err = time.ParseDuration(config.RequestTimeout)
		if err != nil {
			zl.Fatal("Invalid request_timeout duration", zap.String("request_timeout", config.RequestTimeout), zap.Error(err))
		}
	}

	requestQueue := NewRequestQueue(RequestQueueConfig{
		MaxConcurrentRequests: config.MaxConcurrentRequests,
		MaxQueueSize:          config.MaxQueueSize,
		RequestTimeout:        requestTimeout,
	}, zl.Named("queue"))

	// Initialize result caches for inference deduplication
	embeddingCache := NewResultCache[[][]float32]("Embedding", 2*time.Minute, zl.Named("embedding-cache"))
	cleanups = append(cleanups, func() error { embeddingCache.Close(); return nil })

	sparseEmbeddingCache := NewResultCache[[]embeddings.SparseVector]("Sparse embedding", 2*time.Minute, zl.Named("sparse-embedding-cache"))
	cleanups = append(cleanups, func() error { sparseEmbeddingCache.Close(); return nil })

	rerankingCache := NewResultCache[[]float32]("Reranking", 2*time.Minute, zl.Named("reranking-cache"))
	cleanups = append(cleanups, func() error { rerankingCache.Close(); return nil })

	nerCache := NewResultCache[[][]ner.Entity]("NER", 2*time.Minute, zl.Named("ner-cache"))
	cleanups = append(cleanups, func() error { nerCache.Close(); return nil })

	readingCache := NewResultCache[[]reading.Result]("Reading", 5*time.Minute, zl.Named("reading-cache"))
	cleanups = append(cleanups, func() error { readingCache.Close(); return nil })

	transcriptionCache := NewResultCache[*transcribing.Result]("Transcription", 2*time.Minute, zl.Named("transcription-cache"))
	cleanups = append(cleanups, func() error { transcriptionCache.Close(); return nil })

	// Build S3 credentials from config (optional)
	var s3Creds *s3.Credentials
	if config.S3Credentials.Endpoint != "" {
		s3Creds = &config.S3Credentials
	}

	node := &TermiteNode{
		logger: zl,

		embedderRegistry:      embedderRegistry,
		chunker:               cachedChunker,
		rerankerRegistry:      rerankerRegistry,
		generatorRegistry:     generatorRegistry,
		nerRegistry:           nerRegistry,
		seq2seqRegistry:       seq2seqRegistry,
		classifierRegistry:    classifierRegistry,
		readerRegistry:        readerRegistry,
		transcriberRegistry:   transcriberRegistry,
		contentSecurityConfig: contentSecurityConfig,
		s3Credentials:         s3Creds,
		requestQueue:          requestQueue,
		embeddingCache:        embeddingCache,
		sparseEmbeddingCache:  sparseEmbeddingCache,
		rerankingCache:        rerankingCache,
		nerCache:              nerCache,
		readingCache:          readingCache,
		transcriptionCache:    transcriptionCache,
		allowDownloads:        config.AllowDownloads,

		client:   client,
		cleanups: cleanups,
	}

	return node
}

// APIHandler returns the full HTTP handler serving /ml/v1/*, /openai/v1/*,
// /anthropic/v1/*, /healthz, /readyz, the registry proxy, and the embedded
// dashboard. It includes CORS middleware.
//
// Callers embedding Termite in a shared mux should typically mount only the
// /ml/v1/ subtree via APIMLHandler() to avoid exposing OpenAI/Anthropic-compat
// and dashboard surfaces alongside another service.
func (n *TermiteNode) APIHandler() http.Handler {
	apiHandler := NewTermiteAPI(n.logger, n)

	rootMux := http.NewServeMux()

	// Health endpoints (outside /ml/v1 prefix for k8s compatibility)
	rootMux.HandleFunc("GET /healthz", n.handleHealthz)
	rootMux.HandleFunc("GET /readyz", n.handleReadyz)

	// Generate endpoint (manually registered until OpenAPI codegen is updated)
	rootMux.HandleFunc("POST /ml/v1/generate", n.handleApiGenerate)

	// Mount the OpenAPI-generated API handler (includes /ml/v1/version)
	rootMux.Handle("/ml/v1/", apiHandler)

	// OpenAI-compatible API at /openai/v1/* for standard SDK compatibility
	n.RegisterOpenAIRoutes(rootMux)

	// Anthropic-compatible API at /anthropic/v1/* for Anthropic SDK compatibility
	n.RegisterAnthropicRoutes(rootMux)

	// Registry proxy so the dashboard can fetch the model index
	addRegistryProxy(rootMux, defaultRegistryURL())

	// Serve the embedded dashboard at root (SPA with fallback to index.html)
	addDashboardRoutes(rootMux)

	return corsMiddleware(rootMux)
}

// APIMLHandler returns a handler that serves only the /ml/v1/* surface
// (plus /healthz, /readyz). Intended for embedding in an antfly metadata
// HTTP server where OpenAI/Anthropic-compat and dashboard surfaces are
// undesirable.
func (n *TermiteNode) APIMLHandler() http.Handler {
	apiHandler := NewTermiteAPI(n.logger, n)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", n.handleHealthz)
	mux.HandleFunc("GET /readyz", n.handleReadyz)
	mux.HandleFunc("POST /ml/v1/generate", n.handleApiGenerate)
	mux.Handle("/ml/v1/", apiHandler)

	return mux
}
