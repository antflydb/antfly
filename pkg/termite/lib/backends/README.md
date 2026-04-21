# backends

The `backends` package provides a unified interface for ML inference with multiple backend support:

- **ONNX Runtime** (`onnx`): Fastest inference, supports CPU/CUDA/CoreML (requires `onnx,ORT` build tags)
- **XLA** (`xla`): Hardware accelerated (CUDA, TPU, optimized CPU) via PJRT (requires `xla,XLA` build tags)
- **Go** (`go`): Pure Go inference, always available, no external dependencies

## Architecture

- **go-huggingface**: Hub download, tokenizers (SentencePiece, WordPiece, BPE), SafeTensors parsing
- **huggingface-gomlx**: Model architectures (BERT, LLaMA, DeBERTa), GoMLX inference
- **onnx-gomlx**: ONNX model execution via GoMLX
- **onnxruntime_go**: Direct ONNX Runtime inference

## Usage

### Basic Pipeline Usage

```go
import (
    "github.com/antflydb/antfly/pkg/termite/lib/backends"
    "github.com/gomlx/go-huggingface/hub"
    "github.com/gomlx/go-huggingface/tokenizers"
)

// Create tokenizer from HuggingFace repo
repo := hub.New("sentence-transformers/all-MiniLM-L6-v2")
tokenizer, err := tokenizers.New(repo)

// Get default backend and load model
backend := backends.GetDefaultBackend()
model, err := backend.Loader().Load(modelPath,
    backends.WithNormalization(true),
    backends.WithPooling("mean"))

// Create pipeline
pipeline := backends.NewPipeline(tokenizer, model, nil)
defer pipeline.Close()

// Generate embeddings
output, err := pipeline.Run(ctx, []string{"Hello world"})
```

### Using SessionManager

```go
manager := backends.NewSessionManager()
defer manager.Close()

// Configure priority
manager.SetPriority([]backends.BackendSpec{
    {Backend: backends.BackendONNX, Device: backends.DeviceCUDA},
    {Backend: backends.BackendXLA, Device: backends.DeviceAuto},
    {Backend: backends.BackendGo, Device: backends.DeviceCPU},
})

// Load model with backend selection
model, backendUsed, err := manager.LoadModel(modelPath,
    []string{"onnx", "xla", "go"}, // Supported backends
    backends.WithONNXFile("model_f16.onnx"))
```

### Feature Extraction Pipeline

```go
embedder, ok := model.(backends.FeatureExtractionModel)
if ok {
    embeddings, err := embedder.EmbedBatch(ctx, inputs)
}

// Or use the typed pipeline
pipeline := backends.NewFeatureExtractionPipeline(tokenizer, embedder, nil)
embeddings, err := pipeline.Embed(ctx, []string{"Hello", "World"})
```

## Build Tags

| Tags | Backends Available | Use Case |
|------|-------------------|----------|
| (none) | Go | Development, cross-platform |
| `xla,XLA` | Go + XLA | Hardware-accelerated inference |
| `onnx,ORT` | Go + ONNX | Production CPU/GPU |
| `onnx,ORT,xla,XLA` | Go + XLA + ONNX | Full backend support |

Build examples:
```bash
# Pure Go only (no CGO required)
go build ./cmd/termite

# With XLA acceleration
go build -tags="xla,XLA" ./cmd/termite

# With ONNX Runtime
go build -tags="onnx,ORT" ./cmd/termite

# All backends
go build -tags="onnx,ORT,xla,XLA" ./cmd/termite
```

## Model Interface

All models implement the base `Model` interface:

```go
type Model interface {
    Forward(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error)
    Close() error
    Name() string
    Backend() BackendType
}
```

Specialized interfaces extend this:

- `FeatureExtractionModel` - Embedding generation
- `TokenClassificationModel` - NER, chunking
- `SequenceClassificationModel` - Text classification
- `CrossEncoderModel` - Reranking
- `Seq2SeqModel` - Text generation
- `VisionModel` - Image processing
- `MultimodalModel` - Text + image

## Configuration Options

```go
// Functional options for model loading
model, err := loader.Load(path,
    backends.WithONNXFile("model_quantized.onnx"),
    backends.WithMaxLength(512),
    backends.WithNormalization(true),
    backends.WithPooling("mean"),      // "mean", "cls", "max", "none"
    backends.WithGPUMode(backends.GPUModeAuto),
    backends.WithBatchSize(32),
    backends.WithNumThreads(4),
)
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ONNXRUNTIME_ROOT` | ONNX Runtime library directory |
| `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` | Library search paths |
| `GOMLX_BACKEND` | GoMLX engine override (e.g., `xla`, `simplego`) - deprecated, use build tags |
| `PJRT_PLUGIN_LIBRARY_PATH` | PJRT plugin location for XLA |
| `TERMITE_FORCE_COREML` | Force CoreML on macOS (experimental) |
| `TERMITE_GPU` | GPU mode override (auto/cuda/tpu/coreml/off) |

## Directory Structure

```
lib/backends/
├── backend.go              # Registry and backend interface
├── backend_onnx.go         # ONNX Runtime (Linux/Windows)
├── backend_onnx_darwin.go  # ONNX Runtime (macOS/CoreML)
├── backend_gomlx.go        # Go backend (pure Go, always available)
├── backend_xla.go          # XLA backend (requires xla,XLA build tags)
├── gpu.go                  # GPU detection utilities
├── model.go                # Model interfaces, options, pooling utilities
├── pipeline.go             # Pipeline types (tokenizer + model)
├── session_manager.go      # Multi-backend session management
├── types.go                # Common types
└── README.md               # This file
```
