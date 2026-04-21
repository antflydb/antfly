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

package backends

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/gomlx/gomlx/backends"
	"github.com/gomlx/gomlx/pkg/core/dtypes"
	"github.com/gomlx/gomlx/pkg/core/graph"
	"github.com/gomlx/gomlx/pkg/core/tensors"
	"github.com/gomlx/gomlx/pkg/core/tensors/bucketing"
	mlctx "github.com/gomlx/gomlx/pkg/ml/context"
	"github.com/gomlx/onnx-gomlx/onnx"
	"github.com/gomlx/onnx-gomlx/onnx/parser"
	"golang.org/x/sync/errgroup"

	// Import Go backend - always available (pure Go, no CGO)
	_ "github.com/gomlx/gomlx/backends/simplego"
)

func init() {
	// Register Go backend (always available)
	// Note: The simplego package registers itself as "go" in the backends registry
	RegisterBackend(newGomlxBackend(BackendGo, "go"))
}

// defaultBucketStrategy is Exponential(1.4), which provides fine-grained bucketing
// with ~40% growth between adjacent buckets (e.g., 1,2,3,4,6,8,11,15,21,29,...).
// This balances cache efficiency against wasted padding.
var defaultBucketStrategy = bucketing.Exponential(1.4)

// bucketConfig holds resolved bucketing configuration for a model.
type bucketConfig struct {
	batchStrategy bucketing.Strategy
	seqStrategy   bucketing.Strategy
	maxCacheSize  int
	maxBatch      int // Max batch size for pre-compilation (0 = skip pre-compilation)
	maxSeq        int // Max sequence length for pre-compilation (0 = skip pre-compilation)
	enabled       bool
}

// resolveBucketConfig determines bucketing behavior based on the backend type and user config.
// The Go backend has no JIT compilation cost so bucketing is disabled by default.
// XLA and CoreML backends benefit from bucketed shapes.
func resolveBucketConfig(backendType BackendType, config *LoadConfig) bucketConfig {
	switch backendType {
	case BackendGo:
		// Pure Go backend: no JIT cost, so bucketing is unnecessary unless explicitly requested.
		if config.BatchBucketing != nil || config.SeqBucketing != nil {
			bc := bucketConfig{
				batchStrategy: config.BatchBucketing,
				seqStrategy:   config.SeqBucketing,
				enabled:       true,
				maxCacheSize:  config.MaxCacheSize,
			}
			if bc.batchStrategy == nil {
				bc.batchStrategy = defaultBucketStrategy
			}
			if bc.seqStrategy == nil {
				bc.seqStrategy = defaultBucketStrategy
			}
			return bc
		}
		return bucketConfig{enabled: false, maxCacheSize: -1}

	default:
		// XLA, CoreML, and future JIT backends: enable bucketing.
		bc := bucketConfig{
			batchStrategy: config.BatchBucketing,
			seqStrategy:   config.SeqBucketing,
			enabled:       true,
			maxCacheSize:  config.MaxCacheSize,
			maxBatch:      config.PreCompileMaxBatch,
			maxSeq:        config.PreCompileMaxSeq,
		}
		if bc.batchStrategy == nil {
			bc.batchStrategy = defaultBucketStrategy
		}
		if bc.seqStrategy == nil {
			bc.seqStrategy = defaultBucketStrategy
		}
		// Default pre-compilation bounds: batch from BatchSize, seq from MaxLength.
		if bc.maxBatch == 0 && config.BatchSize > 0 {
			bc.maxBatch = config.BatchSize
		}
		if bc.maxSeq == 0 {
			bc.maxSeq = config.MaxLength
		}
		return bc
	}
}

// enumerateBuckets returns all unique bucket values from 1 to max (inclusive)
// produced by the given strategy. Values exceeding max are excluded.
func enumerateBuckets(strategy bucketing.Strategy, max int) []int {
	if max <= 0 {
		return nil
	}
	seen := make(map[int]bool)
	var result []int
	for dim := 1; dim <= max; dim++ {
		b := strategy.Bucket(dim)
		if b > max {
			continue
		}
		if !seen[b] {
			seen[b] = true
			result = append(result, b)
		}
	}
	return result
}

// gomlxBackend implements Backend using GoMLX for inference.
//
// This backend supports ONNX model files (via onnx-gomlx).
//
// Two backends are registered:
//   - BackendGo: Pure Go engine (simplego), always available, slower
//   - BackendXLA: XLA engine, hardware accelerated (CUDA, TPU, optimized CPU), requires XLA/PJRT build tags
type gomlxBackend struct {
	backendType BackendType
	engineType  string // "go" or "xla"
	engineMgr   *engineManager
	available   *bool // cached availability check
}

// newGomlxBackend creates a new GoMLX backend with the specified type and engine.
func newGomlxBackend(backendType BackendType, engineType string) *gomlxBackend {
	return &gomlxBackend{
		backendType: backendType,
		engineType:  engineType,
	}
}

func (b *gomlxBackend) Type() BackendType {
	return b.backendType
}

func (b *gomlxBackend) Name() string {
	switch b.backendType {
	case BackendXLA:
		return "GoMLX (XLA)"
	case BackendCoreML:
		return "GoMLX (CoreML)"
	case BackendGo:
		return "GoMLX (Go)"
	default:
		return "GoMLX"
	}
}

func (b *gomlxBackend) Available() bool {
	if b.available != nil {
		return *b.available
	}

	// Test if we can create an engine of this type.
	// Use recover to catch panics from libraries that don't handle
	// missing dependencies gracefully (e.g., go-xla panics if PJRT
	// plugin fails to load due to GLIBC version mismatch).
	result := func() (available bool) {
		defer func() {
			if r := recover(); r != nil {
				available = false
			}
		}()
		_, err := backends.NewWithConfig(resolveXLABackendConfig(b.engineType))
		return err == nil
	}()

	b.available = &result
	return result
}

func (b *gomlxBackend) Priority() int {
	switch b.backendType {
	case BackendXLA:
		// XLA has higher priority than Go (lower number = higher priority)
		return 20
	case BackendCoreML:
		// CoreML is between XLA and Go (macOS only)
		return 25
	case BackendGo:
		// Go is always available fallback
		return 100
	default:
		return 50
	}
}

func (b *gomlxBackend) Loader() ModelLoader {
	if b.engineMgr == nil {
		b.engineMgr = newEngineManager()
	}
	return &gomlxModelLoader{backend: b}
}

// SessionFactory returns a SessionFactory for creating raw GoMLX sessions.
// This provides low-level access for building custom model types (e.g., seq2seq).
func (b *gomlxBackend) SessionFactory() SessionFactory {
	if b.engineMgr == nil {
		b.engineMgr = newEngineManager()
	}
	return &gomlxSessionFactory{backend: b}
}

// pjrtPluginPath holds the absolute path to a bundled PJRT CPU plugin file.
// Set by backend_xla.go init() when a bundled plugin is discovered.
// Used by getEngine() to pass the absolute path to GoMLX, bypassing auto-download.
var pjrtPluginPath string

// engineManager manages GoMLX backend engines.
type engineManager struct {
	mu            sync.RWMutex
	defaultEngine backends.Backend
}

func newEngineManager() *engineManager {
	return &engineManager{}
}

// getEngine returns the GoMLX backend engine, creating it if needed.
// If backendType is empty, auto-detects the best available backend.
func (m *engineManager) getEngine(backendType string) (backends.Backend, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If specific backend requested, create it
	if backendType != "" {
		return safeNewBackend(resolveXLABackendConfig(backendType))
	}

	// Use cached default engine
	if m.defaultEngine != nil {
		return m.defaultEngine, nil
	}

	// Auto-detect: try xla first, fall back to go (simplego)
	engine, err := safeNewBackend(resolveXLABackendConfig("xla"))
	if err != nil {
		// XLA not available, use go (simplego)
		engine, err = safeNewBackend("go")
		if err != nil {
			return nil, err
		}
	}

	m.defaultEngine = engine
	return engine, nil
}

// resolveXLABackendConfig returns the backend config string for GoMLX.
// If backendType is "xla" and a bundled PJRT plugin was discovered,
// returns "xla:/absolute/path/to/plugin.dylib" to bypass auto-download.
// For other backends, returns the type unchanged.
func resolveXLABackendConfig(backendType string) string {
	if backendType == "xla" && pjrtPluginPath != "" {
		return "xla:" + pjrtPluginPath
	}
	return backendType
}

// safeNewBackend creates a new backend, catching panics from libraries
// that don't handle missing dependencies gracefully.
func safeNewBackend(backendType string) (engine backends.Backend, err error) {
	defer func() {
		if r := recover(); r != nil {
			engine = nil
			err = fmt.Errorf("backend %q panicked during initialization: %v", backendType, r)
		}
	}()
	return backends.NewWithConfig(backendType)
}

// gomlxModelLoader implements ModelLoader for GoMLX inference.
type gomlxModelLoader struct {
	backend *gomlxBackend
}

func (l *gomlxModelLoader) Load(path string, opts ...LoadOption) (Model, error) {
	config := ApplyOptions(opts...)

	// Detect model format
	format := config.ModelFormat
	if format == ModelFormatAuto {
		format = detectGoMLXModelFormat(path)
	}

	// Get the inference backend using the backend's engine type (go, coreml, xla)
	// Use config.GoMLXBackend only as override if explicitly set
	engineType := l.backend.engineType
	if config.GoMLXBackend != "" {
		engineType = string(config.GoMLXBackend)
	}
	engine, err := l.backend.engineMgr.getEngine(engineType)
	if err != nil {
		return nil, fmt.Errorf("getting GoMLX engine %q: %w", engineType, err)
	}

	switch format {
	case ModelFormatONNX:
		return l.loadONNX(path, config, engine)
	default:
		return nil, fmt.Errorf("unknown model format at %s", path)
	}
}

// loadONNX loads an ONNX model via onnx-gomlx.
func (l *gomlxModelLoader) loadONNX(path string, config *LoadConfig, engine backends.Backend) (Model, error) {
	// Find the ONNX file
	onnxPath := filepath.Join(path, config.ONNXFilename)
	if _, err := os.Stat(onnxPath); os.IsNotExist(err) {
		// Try to find any .onnx file
		matches, _ := filepath.Glob(filepath.Join(path, "*.onnx"))
		if len(matches) == 0 {
			return nil, fmt.Errorf("no ONNX file found in %s", path)
		}
		onnxPath = matches[0]
	}

	buckets := resolveBucketConfig(l.backend.backendType, config)

	onnxModel, err := newONNXModel(onnxPath, engine, config.Pooling, config.Normalize, buckets)
	if err != nil {
		return nil, err
	}

	return &gomlxModelWrapper{
		onnxModel:   onnxModel,
		path:        path,
		config:      config,
		backendType: l.backend.backendType,
		buckets:     buckets,
	}, nil
}

func (l *gomlxModelLoader) SupportsModel(path string) bool {
	format := detectGoMLXModelFormat(path)
	return format != ModelFormatAuto
}

func (l *gomlxModelLoader) Backend() BackendType {
	return l.backend.backendType
}

// detectGoMLXModelFormat auto-detects the model format from files present.
func detectGoMLXModelFormat(path string) ModelFormat {
	// Check for ONNX format
	onnxFiles, _ := filepath.Glob(filepath.Join(path, "*.onnx"))
	if len(onnxFiles) > 0 {
		return ModelFormatONNX
	}

	return ModelFormatAuto // Unknown
}

// gomlxModelWrapper wraps an onnxModel to implement the backends.Model interface.
type gomlxModelWrapper struct {
	onnxModel   *onnxModel
	path        string
	config      *LoadConfig
	backendType BackendType
	buckets     bucketConfig
}

func (m *gomlxModelWrapper) Forward(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	if !m.buckets.enabled {
		return m.forwardDirect(ctx, inputs)
	}

	// Record original dimensions for trimming after inference.
	origBatch := len(inputs.InputIDs)
	if origBatch == 0 {
		return m.forwardDirect(ctx, inputs)
	}
	origSeq := len(inputs.InputIDs[0])

	// Round up to the nearest bucket.
	bucketedBatch := m.buckets.batchStrategy.Bucket(origBatch)
	bucketedSeq := m.buckets.seqStrategy.Bucket(origSeq)

	// Pad inputs to bucketed dimensions.
	padded := padModelInputs(inputs, bucketedBatch, bucketedSeq)

	// Run inference on the padded inputs.
	output, err := m.forwardDirect(ctx, padded)
	if err != nil {
		return nil, err
	}

	// Trim the output back to original dimensions.
	return trimModelOutput(output, origBatch, origSeq), nil
}

// forwardDirect runs inference without bucketing.
func (m *gomlxModelWrapper) forwardDirect(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	if m.onnxModel != nil {
		return m.onnxModel.forward(ctx, inputs)
	}
	return nil, fmt.Errorf("no model loaded")
}

// padModelInputs pads InputIDs, AttentionMask, and TokenTypeIDs to the target dimensions.
// Padding values are zero, which means attention mask zeros will exclude padded positions from pooling.
// Returns the original inputs unchanged if already at the target size.
func padModelInputs(inputs *ModelInputs, targetBatch, targetSeq int) *ModelInputs {
	origBatch := len(inputs.InputIDs)
	origSeq := 0
	if origBatch > 0 {
		origSeq = len(inputs.InputIDs[0])
	}

	// No padding needed.
	if origBatch == targetBatch && origSeq == targetSeq {
		return inputs
	}

	padded := *inputs // shallow copy

	padded.InputIDs = padInt32Slice(inputs.InputIDs, targetBatch, targetSeq)
	padded.AttentionMask = padInt32Slice(inputs.AttentionMask, targetBatch, targetSeq)
	if len(inputs.TokenTypeIDs) > 0 {
		padded.TokenTypeIDs = padInt32Slice(inputs.TokenTypeIDs, targetBatch, targetSeq)
	}

	return &padded
}

// padInt32Slice pads a [batch, seq] int32 slice to the target dimensions with zeros.
func padInt32Slice(src [][]int32, targetBatch, targetSeq int) [][]int32 {
	result := make([][]int32, targetBatch)
	for i := range targetBatch {
		row := make([]int32, targetSeq)
		if i < len(src) {
			copy(row, src[i])
		}
		result[i] = row
	}
	return result
}

// trimModelOutput trims inference output back to the original unpadded dimensions.
func trimModelOutput(output *ModelOutput, origBatch, origSeq int) *ModelOutput {
	if output == nil {
		return nil
	}

	trimmed := *output // shallow copy

	// LastHiddenState [batch, seq, hidden]: trim both batch and seq.
	if len(output.LastHiddenState) > origBatch {
		trimmed.LastHiddenState = output.LastHiddenState[:origBatch]
	}
	for i := range trimmed.LastHiddenState {
		if len(trimmed.LastHiddenState[i]) > origSeq {
			trimmed.LastHiddenState[i] = trimmed.LastHiddenState[i][:origSeq]
		}
	}

	// Embeddings [batch, hidden]: trim batch only.
	if len(output.Embeddings) > origBatch {
		trimmed.Embeddings = output.Embeddings[:origBatch]
	}

	// Logits [batch, classes]: trim batch only.
	if len(output.Logits) > origBatch {
		trimmed.Logits = output.Logits[:origBatch]
	}

	// EncoderOutput and PastKeyValues are passed through unchanged.

	return &trimmed
}

func (m *gomlxModelWrapper) Close() error {
	if m.onnxModel != nil {
		return m.onnxModel.close()
	}
	return nil
}

func (m *gomlxModelWrapper) Name() string {
	return m.path
}

func (m *gomlxModelWrapper) Backend() BackendType {
	return m.backendType
}

func (m *gomlxModelWrapper) InputInfo() []InputOutputInfo {
	if m.onnxModel != nil {
		return m.onnxModel.inputInfo
	}
	return nil
}

// =============================================================================
// ONNX Model (via onnx-gomlx)
// =============================================================================

// onnxModelType indicates the input type an ONNX model expects.
type onnxModelType int

const (
	// onnxModelText is a text encoder model (input_ids + attention_mask).
	onnxModelText onnxModelType = iota
	// onnxModelVision is a vision encoder model (pixel_values).
	onnxModelVision
	// onnxModelEmbeddings is a projection model (pre-computed embeddings).
	onnxModelEmbeddings
	// onnxModelAudio is an audio encoder model (input_features / audio_values).
	onnxModelAudio
)

// onnxModel implements inference for ONNX models using onnx-gomlx.
// This converts ONNX graphs to GoMLX for execution.
type onnxModel struct {
	path             string
	pooling          string
	normalize        bool
	maxCacheSize     int
	onnxModel        onnx.Model
	ctx              *mlctx.Context
	engine           backends.Backend
	exec             *mlctx.Exec // Cached compiled inference graph
	hasAttentionMask bool
	hasTokenTypeIds  bool
	modelType        onnxModelType
	inputNames       []string          // ONNX model input names
	inputInfo        []InputOutputInfo // Input tensor shape info

	mu sync.Mutex
}

// newONNXModel loads an ONNX model from a file path.
// buckets controls both the compiled graph cache and optional pre-compilation of bucket shapes.
func newONNXModel(onnxPath string, engine backends.Backend, pooling string, normalize bool, buckets bucketConfig) (*onnxModel, error) {
	// Load the ONNX model
	om, err := parser.ParseFile(onnxPath)
	if err != nil {
		return nil, fmt.Errorf("loading ONNX model: %w", err)
	}
	// Many PyTorch-exported models have mixed-precision ops (e.g. Int64 constants
	// with Float32 tensors), so enable automatic dtype promotion.
	om.AllowDTypePromotion()

	// Create GoMLX context and load variables
	ctx := mlctx.New()
	if err := om.VariablesToContext(ctx); err != nil {
		return nil, fmt.Errorf("loading ONNX variables: %w", err)
	}

	// Detect model type and input characteristics from ONNX input names.
	inputNames, inputShapes := om.Inputs()
	hasAttentionMask := false
	hasTokenTypeIds := false
	modelType := onnxModelText // default
	for _, name := range inputNames {
		switch name {
		case "attention_mask":
			hasAttentionMask = true
		case "token_type_ids":
			hasTokenTypeIds = true
		case "pixel_values":
			modelType = onnxModelVision
		case "input_features", "audio_values":
			modelType = onnxModelAudio
		}
	}
	// If no standard text or vision inputs found, treat as embeddings/projection model.
	if modelType == onnxModelText {
		hasStandardInput := slices.Contains(inputNames, "input_ids")
		if !hasStandardInput {
			modelType = onnxModelEmbeddings
		}
	}

	// Build input info from ONNX metadata.
	inputInfo := make([]InputOutputInfo, len(inputNames))
	for i, name := range inputNames {
		inputInfo[i] = InputOutputInfo{
			Name:       name,
			Dimensions: Shape(intsToInt64s(inputShapes[i].Dimensions)),
			DataType:   string(gomlxDataType(inputShapes[i].DType)),
		}
	}

	model := &onnxModel{
		path:             onnxPath,
		pooling:          pooling,
		normalize:        normalize,
		maxCacheSize:     buckets.maxCacheSize,
		onnxModel:        om,
		ctx:              ctx,
		engine:           engine,
		hasAttentionMask: hasAttentionMask,
		hasTokenTypeIds:  hasTokenTypeIds,
		modelType:        modelType,
		inputNames:       inputNames,
		inputInfo:        inputInfo,
	}

	// Pre-compile the inference graph for efficiency.
	// This compiles once and caches the executable, avoiding recompilation on every forward() call.
	// Vision and embeddings models use ExecOnceN (no pre-compilation) since their
	// input shapes are typically fixed or have few variations.
	if modelType == onnxModelText {
		if err := model.compile(buckets); err != nil {
			return nil, fmt.Errorf("compiling ONNX inference graph: %w", err)
		}
	}

	return model, nil
}

// compile pre-compiles the ONNX inference graph and optionally pre-compiles
// all bucket shapes to avoid JIT compilation during inference.
func (m *onnxModel) compile(buckets bucketConfig) error {
	graphFn := func(mlCtx *mlctx.Context, inputs []*graph.Node) []*graph.Node {
		inputMap := map[string]*graph.Node{
			"input_ids": inputs[0],
		}
		idx := 1
		if m.hasAttentionMask {
			inputMap["attention_mask"] = inputs[idx]
			idx++
		}
		if idx < len(inputs) {
			inputMap["token_type_ids"] = inputs[idx]
		}
		return m.onnxModel.CallGraph(mlCtx.Reuse(), inputs[0].Graph(), inputMap)
	}

	exec, err := mlctx.NewExecAny(m.engine, m.ctx, graphFn)
	if err != nil {
		return fmt.Errorf("creating exec: %w", err)
	}
	if m.maxCacheSize != 0 {
		exec.SetMaxCache(m.maxCacheSize)
	}
	m.exec = exec

	// Pre-compile all bucket shapes so JIT compilation happens at load time
	// rather than on the first inference for each shape.
	if buckets.enabled && buckets.maxBatch > 0 && buckets.maxSeq > 0 {
		batchBuckets := enumerateBuckets(buckets.batchStrategy, buckets.maxBatch)
		seqBuckets := enumerateBuckets(buckets.seqStrategy, buckets.maxSeq)

		var eg errgroup.Group
		for _, b := range batchBuckets {
			for _, s := range seqBuckets {
				b, s := b, s
				eg.Go(func() error {
					args := []any{tensors.FromFlatDataAndDimensions(make([]int64, b*s), b, s)}
					if m.hasAttentionMask {
						args = append(args, tensors.FromFlatDataAndDimensions(make([]int64, b*s), b, s))
					}
					if m.hasTokenTypeIds {
						args = append(args, tensors.FromFlatDataAndDimensions(make([]int64, b*s), b, s))
					}
					return m.exec.PreCompile(args...)
				})
			}
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("pre-compiling bucket shapes: %w", err)
		}
	}

	return nil
}

// forward dispatches to the appropriate execution path based on
// which ModelInputs fields are populated.
func (m *onnxModel) forward(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Audio path: CLAP audio encoder with mel spectrogram input.
	if len(inputs.AudioFeatures) > 0 {
		return m.execAudio(inputs)
	}

	// Text path uses cached compiled graph and has pooling/normalization.
	if len(inputs.Embeddings) == 0 && len(inputs.ImagePixels) == 0 {
		return m.execText(inputs.InputIDs, inputs.AttentionMask)
	}

	// Vision and projection paths build a single tensor and run once.
	var (
		names     []string
		tensor    *tensors.Tensor
		batchSize int
	)
	switch {
	case len(inputs.ImagePixels) > 0:
		batchSize = inputs.ImageBatch
		if batchSize == 0 {
			batchSize = 1
		}
		tensor = tensors.FromFlatDataAndDimensions(inputs.ImagePixels,
			batchSize, inputs.ImageChannels, inputs.ImageHeight, inputs.ImageWidth)
		names = []string{"pixel_values"}

	case len(inputs.Embeddings) > 0:
		batchSize = len(inputs.Embeddings)
		hiddenSize := len(inputs.Embeddings[0])
		flat := make([]float32, batchSize*hiddenSize)
		for i, emb := range inputs.Embeddings {
			copy(flat[i*hiddenSize:(i+1)*hiddenSize], emb)
		}
		tensor = tensors.FromFlatDataAndDimensions(flat, batchSize, hiddenSize)
		inputName := "input"
		if len(m.inputNames) > 0 {
			inputName = m.inputNames[0]
		}
		names = []string{inputName}
	}

	results, err := m.execOnce(names, tensor)
	if err != nil {
		return nil, fmt.Errorf("exec failed: %w", err)
	}
	return m.parseOutput(results, batchSize)
}

// execText runs the text encoder path (input_ids + optional attention_mask).
func (m *onnxModel) execText(inputIDs, attentionMask [][]int32) (*ModelOutput, error) {
	batchSize := len(inputIDs)
	if batchSize == 0 {
		return &ModelOutput{}, nil
	}

	seqLen := len(inputIDs[0])

	// Create input tensors (ONNX typically expects int64)
	flatInputIDs := make([]int64, batchSize*seqLen)
	for i := range batchSize {
		for j := range seqLen {
			flatInputIDs[i*seqLen+j] = int64(inputIDs[i][j])
		}
	}
	inputIDsTensor := tensors.FromFlatDataAndDimensions(flatInputIDs, batchSize, seqLen)

	var flatAttentionMask []int64
	var attentionMaskTensor *tensors.Tensor
	if m.hasAttentionMask {
		flatAttentionMask = make([]int64, batchSize*seqLen)
		for i := range batchSize {
			for j := range seqLen {
				flatAttentionMask[i*seqLen+j] = int64(attentionMask[i][j])
			}
		}
		attentionMaskTensor = tensors.FromFlatDataAndDimensions(flatAttentionMask, batchSize, seqLen)
	}

	// Execute using the cached compiled graph
	var results []*tensors.Tensor
	var err error
	if m.exec != nil {
		args := []any{inputIDsTensor}
		if m.hasAttentionMask {
			args = append(args, attentionMaskTensor)
		}
		if m.hasTokenTypeIds {
			flatTokenTypeIds := make([]int64, batchSize*seqLen)
			tokenTypeIdsTensor := tensors.FromFlatDataAndDimensions(flatTokenTypeIds, batchSize, seqLen)
			args = append(args, tokenTypeIdsTensor)
		}
		results, err = m.exec.Exec(args...)
	} else {
		names := []string{"input_ids"}
		inputTensors := []*tensors.Tensor{inputIDsTensor}
		if m.hasAttentionMask {
			names = append(names, "attention_mask")
			inputTensors = append(inputTensors, attentionMaskTensor)
		}
		results, err = m.execOnce(names, inputTensors...)
	}
	if err != nil {
		return nil, fmt.Errorf("exec failed: %w", err)
	}

	output, err := m.parseOutput(results, batchSize)
	if err != nil {
		return nil, err
	}

	// Text models with 3D output get attention-mask pooling.
	if output.LastHiddenState != nil {
		output.Embeddings = PoolHiddenStates(output.LastHiddenState, attentionMask, PoolingStrategy(m.pooling))
		if m.normalize {
			NormalizeEmbeddings(output.Embeddings)
		}
	}

	return output, nil
}

// execAudio runs the audio encoder path (e.g., CLAP audio encoder).
// Takes mel spectrogram features as [batch, channels, time, mels] and outputs embeddings.
func (m *onnxModel) execAudio(inputs *ModelInputs) (*ModelOutput, error) {
	batchSize := inputs.AudioBatch
	if batchSize == 0 {
		batchSize = 1
	}
	timeSteps := inputs.AudioTime
	nMels := inputs.AudioMels

	// CLAP expects 4D input: [batch, channels, time, mels]
	// The mel spectrogram is mono (1 channel).
	channels := 1
	tensor := tensors.FromFlatDataAndDimensions(inputs.AudioFeatures,
		batchSize, channels, timeSteps, nMels)

	inputName := m.inputNames[0]
	results, err := m.execOnce([]string{inputName}, tensor)
	if err != nil {
		return nil, fmt.Errorf("audio exec failed: %w", err)
	}
	return m.parseOutput(results, batchSize)
}

// execOnce runs the ONNX graph via ExecOnceN with the given named inputs.
func (m *onnxModel) execOnce(names []string, inputTensors ...*tensors.Tensor) ([]*tensors.Tensor, error) {
	graphFn := func(mlCtx *mlctx.Context, inputs []*graph.Node) []*graph.Node {
		inputMap := make(map[string]*graph.Node, len(names))
		for i, name := range names {
			inputMap[name] = inputs[i]
		}
		return m.onnxModel.CallGraph(mlCtx.Reuse(), inputs[0].Graph(), inputMap)
	}
	args := make([]any, len(inputTensors))
	for i, t := range inputTensors {
		args[i] = t
	}
	return mlctx.ExecOnceN(m.engine, m.ctx, graphFn, args...)
}

// parseOutput converts raw tensor results into a ModelOutput.
//
// Output shape interpretation depends on model type:
//   - 3D [batch, seq, hidden]: LastHiddenState + CLS-pooled Embeddings (vision)
//     or LastHiddenState only (text — caller applies attention-mask pooling).
//   - 2D [batch, dim]: Embeddings (vision/projection) or Logits (text/reranker).
func (m *onnxModel) parseOutput(results []*tensors.Tensor, batchSize int) (*ModelOutput, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("no output from ONNX model")
	}

	output := results[0]
	shape := output.Shape()

	switch len(shape.Dimensions) {
	case 3:
		data := output.Value().([][][]float32)
		lastHiddenState := data[:batchSize]

		mo := &ModelOutput{LastHiddenState: lastHiddenState}

		// Vision models get CLS-token pooling here; text models defer
		// pooling to the caller which has the attention mask.
		if m.modelType == onnxModelVision {
			hiddenSize := int(shape.Dimensions[2])
			embeddings := make([][]float32, batchSize)
			for i := range batchSize {
				embeddings[i] = make([]float32, hiddenSize)
				copy(embeddings[i], lastHiddenState[i][0])
			}
			mo.Embeddings = embeddings
		}

		return mo, nil

	case 2:
		data := output.Value().([][]float32)
		dim := int(shape.Dimensions[1])
		out := make([][]float32, batchSize)
		for i := range batchSize {
			out[i] = make([]float32, dim)
			copy(out[i], data[i])
		}

		// Text models set both Logits and Embeddings for 2D output;
		// the caller decides which to use (rerankers use Logits,
		// embedding models like CLAP text encoders use Embeddings).
		if m.modelType == onnxModelText {
			return &ModelOutput{Logits: out, Embeddings: out}, nil
		}
		return &ModelOutput{Embeddings: out}, nil

	default:
		return nil, fmt.Errorf("unexpected output shape: %v (expected 2D or 3D)", shape.Dimensions)
	}
}

func (m *onnxModel) close() error {
	if m.exec != nil {
		m.exec.Finalize()
		m.exec = nil
	}
	return nil
}

// =============================================================================
// SessionFactory Implementation (for seq2seq and other multi-model pipelines)
// =============================================================================

// gomlxSessionFactory creates sessions from ONNX model files using GoMLX.
type gomlxSessionFactory struct {
	backend *gomlxBackend
}

func (f *gomlxSessionFactory) CreateSession(modelPath string, opts ...SessionOption) (Session, error) {
	// Get the inference backend
	engine, err := f.backend.engineMgr.getEngine(f.backend.engineType)
	if err != nil {
		return nil, fmt.Errorf("getting GoMLX engine: %w", err)
	}

	// Load the ONNX model
	om, err := parser.ParseFile(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading ONNX model: %w", err)
	}
	om.AllowDTypePromotion()

	// Apply session options for input constants.
	cfg := ApplySessionOptions(opts...)
	if len(cfg.InputConstants) > 0 {
		om.WithInputsAsConstants(cfg.InputConstants)
	}

	// NOTE: DynamicAxes overrides are not applied here — the onnx.Model
	// interface does not expose internal shape mutation. This is safe because
	// antfly never calls ValidateInputs, and CallGraph uses runtime shapes.

	// Create context and load variables
	ctx := mlctx.New()
	if err := om.VariablesToContext(ctx); err != nil {
		return nil, fmt.Errorf("loading ONNX variables: %w", err)
	}

	// Get input/output info, filtering out compile-time constants.
	allInputNames, inputShapes := om.Inputs()
	outputNames, outputShapes := om.Outputs()

	var inputNames []string
	var inputInfo []TensorInfo
	for i, name := range allInputNames {
		if _, isConst := cfg.InputConstants[name]; isConst {
			continue
		}
		inputNames = append(inputNames, name)
		inputInfo = append(inputInfo, TensorInfo{
			Name:     name,
			Shape:    intsToInt64s(inputShapes[i].Dimensions),
			DataType: gomlxDataType(inputShapes[i].DType),
		})
	}

	outputInfo := make([]TensorInfo, len(outputNames))
	for i, name := range outputNames {
		outputInfo[i] = TensorInfo{
			Name:     name,
			Shape:    intsToInt64s(outputShapes[i].Dimensions),
			DataType: gomlxDataType(outputShapes[i].DType),
		}
	}

	sess := &gomlxSession{
		onnxModel:   om,
		ctx:         ctx,
		engine:      engine,
		inputInfo:   inputInfo,
		outputInfo:  outputInfo,
		inputNames:  inputNames,
		outputNames: outputNames,
	}

	// Pre-compile the session graph.
	if err := sess.compile(); err != nil {
		return nil, fmt.Errorf("compiling session graph: %w", err)
	}

	return sess, nil
}

func (f *gomlxSessionFactory) Backend() BackendType {
	return f.backend.backendType
}

// gomlxSession implements Session for raw tensor I/O using GoMLX.
type gomlxSession struct {
	onnxModel   onnx.Model
	ctx         *mlctx.Context
	engine      backends.Backend
	exec        *mlctx.Exec // Cached compiled graph
	inputInfo   []TensorInfo
	outputInfo  []TensorInfo
	inputNames  []string
	outputNames []string
	mu          sync.Mutex
}

// compile pre-compiles the session's ONNX graph for reuse across Run() calls.
func (s *gomlxSession) compile() error {
	graphFn := func(mlCtx *mlctx.Context, graphInputs []*graph.Node) []*graph.Node {
		inputNodeMap := make(map[string]*graph.Node, len(s.inputNames))
		for i, name := range s.inputNames {
			inputNodeMap[name] = graphInputs[i]
		}
		return s.onnxModel.CallGraph(mlCtx.Reuse(), graphInputs[0].Graph(), inputNodeMap)
	}

	exec, err := mlctx.NewExecAny(s.engine, s.ctx, graphFn)
	if err != nil {
		return fmt.Errorf("creating exec: %w", err)
	}

	// Unlimited cache for all backends. The default GoMLX cache of 32 is too
	// small for KV-cached autoregressive decoding, where each step has a
	// different past_key_values shape (1, 2, 3, ..., N). Compiled programs
	// are small enough that caching all of them is fine.
	exec.SetMaxCache(-1)

	s.exec = exec
	return nil
}

func (s *gomlxSession) Run(inputs []NamedTensor) ([]NamedTensor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.onnxModel == nil {
		return nil, fmt.Errorf("session is closed")
	}

	// Build a map of input name -> tensor for fast lookup
	inputMap := make(map[string]NamedTensor, len(inputs))
	for _, input := range inputs {
		inputMap[input.Name] = input
	}

	// Convert inputs to GoMLX tensors in the order expected by the model
	gomlxInputs := make([]*tensors.Tensor, len(s.inputNames))
	for i, name := range s.inputNames {
		input, ok := inputMap[name]
		if !ok {
			return nil, fmt.Errorf("missing input tensor: %s", name)
		}
		tensor, err := namedTensorToGoMLX(input)
		if err != nil {
			return nil, fmt.Errorf("converting input tensor %s: %w", name, err)
		}
		gomlxInputs[i] = tensor
	}

	// Execute using the cached compiled graph or fall back to ExecOnceN.
	args := make([]any, len(gomlxInputs))
	for i, t := range gomlxInputs {
		args[i] = t
	}

	var results []*tensors.Tensor
	var err error
	if s.exec != nil {
		results, err = s.exec.Exec(args...)
	} else {
		// Fallback: recompile each time (should not happen after compile()).
		graphFn := func(mlCtx *mlctx.Context, graphInputs []*graph.Node) []*graph.Node {
			inputNodeMap := make(map[string]*graph.Node, len(s.inputNames))
			for i, name := range s.inputNames {
				inputNodeMap[name] = graphInputs[i]
			}
			return s.onnxModel.CallGraph(mlCtx.Reuse(), graphInputs[0].Graph(), inputNodeMap)
		}
		results, err = mlctx.ExecOnceN(s.engine, s.ctx, graphFn, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("executing ONNX graph: %w", err)
	}

	// Convert outputs to NamedTensors
	outputs := make([]NamedTensor, len(results))
	for i, result := range results {
		name := ""
		if i < len(s.outputNames) {
			name = s.outputNames[i]
		}
		output, err := gomlxToNamedTensor(result, name)
		if err != nil {
			return nil, fmt.Errorf("converting output tensor %d: %w", i, err)
		}
		outputs[i] = output
	}

	return outputs, nil
}

func (s *gomlxSession) InputInfo() []TensorInfo {
	return s.inputInfo
}

func (s *gomlxSession) OutputInfo() []TensorInfo {
	return s.outputInfo
}

func (s *gomlxSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exec != nil {
		s.exec.Finalize()
		s.exec = nil
	}
	s.onnxModel = nil
	s.ctx = nil
	return nil
}

// =============================================================================
// Helper functions for tensor conversion
// =============================================================================

// intsToInt64s converts []int to []int64.
func intsToInt64s(dims []int) []int64 {
	result := make([]int64, len(dims))
	for i, d := range dims {
		result[i] = int64(d)
	}
	return result
}

// gomlxDataType converts GoMLX DType to our DataType.
func gomlxDataType(dt dtypes.DType) DataType {
	switch dt {
	case dtypes.Float32:
		return DataTypeFloat32
	case dtypes.Float64:
		return DataTypeFloat32 // Downgrade to float32 for compatibility
	case dtypes.Float16, dtypes.BFloat16:
		return DataTypeFloat16
	case dtypes.Int64:
		return DataTypeInt64
	case dtypes.Int32:
		return DataTypeInt32
	case dtypes.Int8, dtypes.Int16:
		return DataTypeInt32 // Upgrade to int32 for compatibility
	case dtypes.Bool:
		return DataTypeBool
	default:
		return DataTypeFloat32 // Default to float32
	}
}

// namedTensorToGoMLX converts a NamedTensor to a GoMLX tensor.
func namedTensorToGoMLX(nt NamedTensor) (*tensors.Tensor, error) {
	dims := make([]int, len(nt.Shape))
	for i, d := range nt.Shape {
		dims[i] = int(d)
	}

	switch data := nt.Data.(type) {
	case []float32:
		return tensors.FromFlatDataAndDimensions(data, dims...), nil
	case []float64:
		// Convert float64 to float32
		f32 := make([]float32, len(data))
		for i, v := range data {
			f32[i] = float32(v)
		}
		return tensors.FromFlatDataAndDimensions(f32, dims...), nil
	case []int64:
		return tensors.FromFlatDataAndDimensions(data, dims...), nil
	case []int32:
		// Convert int32 to int64
		i64 := make([]int64, len(data))
		for i, v := range data {
			i64[i] = int64(v)
		}
		return tensors.FromFlatDataAndDimensions(i64, dims...), nil
	case []int:
		// Convert int to int64
		i64 := make([]int64, len(data))
		for i, v := range data {
			i64[i] = int64(v)
		}
		return tensors.FromFlatDataAndDimensions(i64, dims...), nil
	case []bool:
		return tensors.FromFlatDataAndDimensions(data, dims...), nil
	default:
		return nil, fmt.Errorf("unsupported tensor data type: %T", data)
	}
}

// gomlxToNamedTensor converts a GoMLX tensor to a NamedTensor.
func gomlxToNamedTensor(t *tensors.Tensor, name string) (NamedTensor, error) {
	shape := t.Shape()
	dims := make([]int64, shape.Rank())
	for i := range shape.Rank() {
		dims[i] = int64(shape.Dimensions[i])
	}

	// Extract data based on dtype
	var data any
	switch shape.DType {
	case dtypes.Float32:
		data = extractFloat32Data(t)
	case dtypes.Float64:
		data = extractFloat64Data(t)
	case dtypes.Int64:
		data = extractInt64Data(t)
	case dtypes.Int32:
		data = extractInt32Data(t)
	case dtypes.Bool:
		data = extractBoolData(t)
	default:
		// Try float32 as default
		data = extractFloat32Data(t)
	}

	return NamedTensor{
		Name:  name,
		Shape: dims,
		Data:  data,
	}, nil
}

// extractFloat32Data extracts float32 data from a tensor as a flat slice.
func extractFloat32Data(t *tensors.Tensor) []float32 {
	val := t.Value()
	return flattenFloat32(val)
}

// extractFloat64Data extracts float64 data from a tensor as a flat slice.
func extractFloat64Data(t *tensors.Tensor) []float64 {
	val := t.Value()
	return flattenFloat64(val)
}

// extractInt64Data extracts int64 data from a tensor as a flat slice.
func extractInt64Data(t *tensors.Tensor) []int64 {
	val := t.Value()
	return flattenInt64(val)
}

// extractInt32Data extracts int32 data from a tensor as a flat slice.
func extractInt32Data(t *tensors.Tensor) []int32 {
	val := t.Value()
	return flattenInt32(val)
}

// extractBoolData extracts bool data from a tensor as a flat slice.
func extractBoolData(t *tensors.Tensor) []bool {
	val := t.Value()
	return flattenBool(val)
}

// flattenFloat32 recursively flattens multi-dimensional float32 data.
func flattenFloat32(val any) []float32 {
	switch v := val.(type) {
	case []float32:
		return v
	case [][]float32:
		var result []float32
		for _, row := range v {
			result = append(result, row...)
		}
		return result
	case [][][]float32:
		var result []float32
		for _, matrix := range v {
			for _, row := range matrix {
				result = append(result, row...)
			}
		}
		return result
	case [][][][]float32:
		var result []float32
		for _, cube := range v {
			for _, matrix := range cube {
				for _, row := range matrix {
					result = append(result, row...)
				}
			}
		}
		return result
	default:
		return nil
	}
}

// flattenFloat64 recursively flattens multi-dimensional float64 data.
func flattenFloat64(val any) []float64 {
	switch v := val.(type) {
	case []float64:
		return v
	case [][]float64:
		var result []float64
		for _, row := range v {
			result = append(result, row...)
		}
		return result
	case [][][]float64:
		var result []float64
		for _, matrix := range v {
			for _, row := range matrix {
				result = append(result, row...)
			}
		}
		return result
	default:
		return nil
	}
}

// flattenInt64 recursively flattens multi-dimensional int64 data.
func flattenInt64(val any) []int64 {
	switch v := val.(type) {
	case []int64:
		return v
	case [][]int64:
		var result []int64
		for _, row := range v {
			result = append(result, row...)
		}
		return result
	case [][][]int64:
		var result []int64
		for _, matrix := range v {
			for _, row := range matrix {
				result = append(result, row...)
			}
		}
		return result
	default:
		return nil
	}
}

// flattenInt32 recursively flattens multi-dimensional int32 data.
func flattenInt32(val any) []int32 {
	switch v := val.(type) {
	case []int32:
		return v
	case [][]int32:
		var result []int32
		for _, row := range v {
			result = append(result, row...)
		}
		return result
	case [][][]int32:
		var result []int32
		for _, matrix := range v {
			for _, row := range matrix {
				result = append(result, row...)
			}
		}
		return result
	default:
		return nil
	}
}

// flattenBool recursively flattens multi-dimensional bool data.
func flattenBool(val any) []bool {
	switch v := val.(type) {
	case []bool:
		return v
	case [][]bool:
		var result []bool
		for _, row := range v {
			result = append(result, row...)
		}
		return result
	default:
		return nil
	}
}
