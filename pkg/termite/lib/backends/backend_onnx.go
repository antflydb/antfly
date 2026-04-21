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

//go:build onnx && ORT

package backends

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	ort "github.com/yalue/onnxruntime_go"
)

func init() {
	RegisterBackend(&onnxBackend{})
}

// onnxBackend implements Backend using ONNX Runtime (Linux/Windows).
// This is the fastest backend for CPU and CUDA inference.
//
// Library Discovery (checked in order):
//  1. ONNXRUNTIME_ROOT environment variable
//  2. lib/ directory relative to the running binary (omni install layout)
//  3. LD_LIBRARY_PATH (Linux) or DYLD_LIBRARY_PATH (macOS)
//
// For CUDA: ensure CUDA libraries are also discoverable.
//
// Build Requirements:
//   - CGO must be enabled (CGO_ENABLED=1)
//   - ONNX Runtime libraries must be available at link time
type onnxBackend struct {
	// GPU mode configuration
	gpuMode   GPUMode
	gpuModeMu sync.RWMutex

	// Track whether CUDA is enabled (cached after first detection)
	cudaEnabled     bool
	cudaEnabledOnce sync.Once

	// Track initialization state
	initializedOnce sync.Once
	initErr         error

	// Shared arena-based allocator for all sessions
	sharedAllocator bool
}

func (b *onnxBackend) Type() BackendType {
	return BackendONNX
}

func (b *onnxBackend) Name() string {
	if b.useCUDA() {
		return "ONNX Runtime (CUDA)"
	}
	return "ONNX Runtime (CPU)"
}

func (b *onnxBackend) Available() bool {
	// ONNX is available if the build includes the ONNX Runtime
	// The build tags ensure this file is only included when ONNX is available
	return true
}

func (b *onnxBackend) Priority() int {
	// Highest priority when available
	return 10
}

func (b *onnxBackend) Loader() ModelLoader {
	return &ortModelLoader{backend: b}
}

// SessionFactory returns a SessionFactory for creating raw ONNX sessions.
// This provides low-level access for building custom model types.
func (b *onnxBackend) SessionFactory() SessionFactory {
	return &onnxSessionFactory{backend: b}
}

// initONNX initializes the ONNX Runtime library and registers a shared
// arena-based allocator so that all sessions share the same memory pool,
// reducing fragmentation and total memory usage.
func (b *onnxBackend) initONNX() error {
	b.initializedOnce.Do(func() {
		// Set library path if found
		if libPath := getOnnxLibraryPath(); libPath != "" {
			ort.SetSharedLibraryPath(filepath.Join(libPath, getOnnxLibraryName()))
		}

		// Initialize the environment
		b.initErr = ort.InitializeEnvironment()
		if b.initErr != nil {
			return
		}

		// Best-effort: register a shared arena-based allocator with the
		// environment so all sessions share one memory pool. If any step
		// fails, we silently fall back to per-session allocators.
		if memInfo, err := ort.GetMemoryInfo(); err == nil {
			if arenaCfg, err := ort.NewArenaCfg(0, -1, -1, -1); err == nil {
				if err := ort.CreateAndRegisterAllocator(memInfo, arenaCfg); err == nil {
					b.sharedAllocator = true
				}
				arenaCfg.Destroy()
			}
		}
	})
	return b.initErr
}

// enableSharedAllocator configures session options to use the shared
// environment allocator if one was registered during initialization.
func (b *onnxBackend) enableSharedAllocator(opts *ort.SessionOptions) error {
	if b.sharedAllocator {
		return opts.AddSessionConfigEntry("session.use_env_allocators", "1")
	}
	return nil
}

// getOnnxLibraryPath returns the directory containing libonnxruntime.
// Discovery order:
//  1. ONNXRUNTIME_ROOT environment variable (set by Makefile)
//  2. lib/ directory next to the running binary (omni install layout)
//  3. LD_LIBRARY_PATH or DYLD_LIBRARY_PATH
func getOnnxLibraryPath() string {
	platform := runtime.GOOS + "-" + runtime.GOARCH
	libName := getOnnxLibraryName()

	// Check ONNXRUNTIME_ROOT (set by Makefile)
	if root := os.Getenv("ONNXRUNTIME_ROOT"); root != "" {
		// Try platform-specific path first
		platformDir := filepath.Join(root, platform, "lib")
		if _, err := os.Stat(filepath.Join(platformDir, libName)); err == nil {
			return platformDir
		}
		// Try direct lib path
		directDir := filepath.Join(root, "lib")
		if _, err := os.Stat(filepath.Join(directDir, libName)); err == nil {
			return directDir
		}
	}

	// Check lib/ relative to the binary (omni install layout)
	if dir := findLibRelativeToBinary(libName); dir != "" {
		return dir
	}

	// Check library path environment variable (platform-specific)
	ldPath := os.Getenv("LD_LIBRARY_PATH")
	if runtime.GOOS == "darwin" {
		if dyldPath := os.Getenv("DYLD_LIBRARY_PATH"); dyldPath != "" {
			ldPath = dyldPath
		}
	}
	if ldPath != "" {
		for _, dir := range filepath.SplitList(ldPath) {
			if _, err := os.Stat(filepath.Join(dir, libName)); err == nil {
				return dir
			}
		}
	}

	return ""
}

// findLibRelativeToBinary checks for a library in lib/ directories relative
// to the running binary. This supports the omni install layout where bundled
// libraries are placed next to the binary or under a well-known lib path.
func findLibRelativeToBinary(libName string) string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return ""
	}
	binDir := filepath.Dir(exe)

	// Check <binary_dir>/lib/ (e.g. /opt/antfly/lib/)
	if dir := filepath.Join(binDir, "lib"); dirContains(dir, libName) {
		return dir
	}

	// Check <binary_dir>/../lib/antfly/ (e.g. /usr/local/lib/antfly/)
	if dir := filepath.Join(binDir, "..", "lib", "antfly"); dirContains(dir, libName) {
		return dir
	}

	// Check <binary_dir>/../lib/termite/ (e.g. /usr/local/lib/termite/)
	// This supports standalone termite omni installs.
	if dir := filepath.Join(binDir, "..", "lib", "termite"); dirContains(dir, libName) {
		return dir
	}

	return ""
}

// dirContains returns true if dir contains a file named name.
func dirContains(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

// getOnnxLibraryName returns the platform-specific library name.
func getOnnxLibraryName() string {
	switch runtime.GOOS {
	case "windows":
		return "onnxruntime.dll"
	case "darwin":
		return "libonnxruntime.dylib"
	default:
		return "libonnxruntime.so"
	}
}

// SetGPUMode sets the GPU mode for this backend.
// Must be called before any sessions are created to take effect.
func (b *onnxBackend) SetGPUMode(mode GPUMode) {
	b.gpuModeMu.Lock()
	defer b.gpuModeMu.Unlock()
	b.gpuMode = mode
}

// GetGPUMode returns the current GPU mode.
func (b *onnxBackend) GetGPUMode() GPUMode {
	b.gpuModeMu.RLock()
	defer b.gpuModeMu.RUnlock()
	if b.gpuMode == "" {
		return GPUModeAuto
	}
	return b.gpuMode
}

// useCUDA determines if CUDA should be used.
// Uses auto-detection by default, can be overridden via SetGPUMode().
func (b *onnxBackend) useCUDA() bool {
	b.cudaEnabledOnce.Do(func() {
		b.gpuModeMu.RLock()
		mode := b.gpuMode
		b.gpuModeMu.RUnlock()

		b.cudaEnabled = ShouldUseGPU(mode)
	})
	return b.cudaEnabled
}

// ortModelLoader implements ModelLoader for ONNX Runtime.
type ortModelLoader struct {
	backend *onnxBackend
}

func (l *ortModelLoader) Load(path string, opts ...LoadOption) (Model, error) {
	// Initialize ONNX Runtime if needed
	if err := l.backend.initONNX(); err != nil {
		return nil, fmt.Errorf("initializing ONNX Runtime: %w", err)
	}

	config := ApplyOptions(opts...)

	// Determine ONNX file path
	onnxPath := filepath.Join(path, config.ONNXFilename)
	if _, err := os.Stat(onnxPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("ONNX model not found: %s", onnxPath)
	}

	// Get input/output names from the model metadata
	inputs, outputs, err := ort.GetInputOutputInfo(onnxPath)
	if err != nil {
		return nil, fmt.Errorf("getting model info: %w", err)
	}

	// Extract input names - filter for known text model inputs
	inputNames := filterInputNames(inputs)
	if len(inputNames) == 0 {
		return nil, fmt.Errorf("no valid input names found in model")
	}

	// Build input info for the filtered inputs
	inputInfoMap := make(map[string]ort.InputOutputInfo, len(inputs))
	for _, info := range inputs {
		inputInfoMap[info.Name] = info
	}
	inputInfo := make([]InputOutputInfo, 0, len(inputNames))
	for _, name := range inputNames {
		if info, ok := inputInfoMap[name]; ok {
			inputInfo = append(inputInfo, InputOutputInfo{
				Name:       info.Name,
				Dimensions: Shape(info.Dimensions),
				DataType:   info.DataType.String(),
			})
		}
	}

	// Extract output names
	outputNames := make([]string, len(outputs))
	for i, info := range outputs {
		outputNames[i] = info.Name
	}
	if len(outputNames) == 0 {
		return nil, fmt.Errorf("no output names found in model")
	}

	// Create session options
	sessionOpts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, fmt.Errorf("creating session options: %w", err)
	}

	// Use shared environment allocator if available
	if err := l.backend.enableSharedAllocator(sessionOpts); err != nil {
		sessionOpts.Destroy()
		return nil, fmt.Errorf("enabling shared allocator: %w", err)
	}

	// Configure number of threads
	if config.NumThreads > 0 {
		if err := sessionOpts.SetIntraOpNumThreads(config.NumThreads); err != nil {
			sessionOpts.Destroy()
			return nil, fmt.Errorf("setting thread count: %w", err)
		}
	}

	// Enable CUDA if requested and available
	gpuMode := config.GPUMode
	if gpuMode == "" {
		gpuMode = l.backend.GetGPUMode()
	}
	useCUDA := gpuMode == GPUModeCuda || (gpuMode == GPUModeAuto && l.backend.useCUDA())
	if useCUDA {
		cudaOpts, err := ort.NewCUDAProviderOptions()
		if err == nil {
			if err := sessionOpts.AppendExecutionProviderCUDA(cudaOpts); err != nil {
				// CUDA not available, fall back to CPU
				cudaOpts.Destroy()
			} else {
				defer cudaOpts.Destroy()
			}
		}
	}

	// Create the session with dynamically detected input/output names
	session, err := ort.NewDynamicAdvancedSession(onnxPath,
		inputNames,
		outputNames,
		sessionOpts)
	if err != nil {
		sessionOpts.Destroy()
		return nil, fmt.Errorf("creating ONNX session: %w", err)
	}

	return &ortModel{
		path:        path,
		config:      config,
		session:     session,
		sessionOpts: sessionOpts,
		inputNames:  inputNames,
		outputNames: outputNames,
		inputInfo:   inputInfo,
	}, nil
}

// filterInputNames extracts relevant input names for text and vision models.
// Filters for common input patterns like input_ids, attention_mask, token_type_ids,
// and vision inputs like pixel_values.
func filterInputNames(inputs []ort.InputOutputInfo) []string {
	// Known input names for text and vision models
	knownInputs := map[string]bool{
		// Text model inputs
		"input_ids":      true,
		"attention_mask": true,
		"token_type_ids": true,
		// Vision model inputs
		"pixel_values": true,
	}

	var names []string
	for _, info := range inputs {
		if knownInputs[info.Name] {
			names = append(names, info.Name)
		}
	}

	// If no known inputs found, return all input names
	if len(names) == 0 {
		names = make([]string, len(inputs))
		for i, info := range inputs {
			names[i] = info.Name
		}
	}

	return names
}

func (l *ortModelLoader) SupportsModel(path string) bool {
	// Check if the model directory contains an ONNX file
	matches, _ := filepath.Glob(filepath.Join(path, "*.onnx"))
	return len(matches) > 0
}

func (l *ortModelLoader) Backend() BackendType {
	return BackendONNX
}

// ortModel implements Model using ONNX Runtime.
type ortModel struct {
	path        string
	config      *LoadConfig
	session     *ort.DynamicAdvancedSession
	sessionOpts *ort.SessionOptions
	inputNames  []string
	outputNames []string
	inputInfo   []InputOutputInfo
}

func (m *ortModel) Forward(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	if m.session == nil {
		return nil, fmt.Errorf("ONNX session not initialized")
	}

	// Determine input type and dispatch to appropriate forward method
	isVisionModel := inputs.ImagePixels != nil && len(inputs.ImagePixels) > 0
	isAudioModel := inputs.AudioFeatures != nil && len(inputs.AudioFeatures) > 0
	isEmbeddingsModel := inputs.Embeddings != nil && len(inputs.Embeddings) > 0

	if isEmbeddingsModel {
		return m.forwardEmbeddings(ctx, inputs)
	}
	if isVisionModel {
		return m.forwardVision(ctx, inputs)
	}
	if isAudioModel {
		return m.forwardAudio(ctx, inputs)
	}
	return m.forwardText(ctx, inputs)
}

// forwardEmbeddings handles inference for projection models (e.g., visual_projection.onnx).
// Takes pre-computed embeddings and projects them to a different dimensionality.
func (m *ortModel) forwardEmbeddings(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	batchSize := len(inputs.Embeddings)
	if batchSize == 0 {
		return &ModelOutput{}, nil
	}

	hiddenSize := len(inputs.Embeddings[0])

	// Flatten embeddings to 1D array for tensor creation
	flatData := make([]float32, batchSize*hiddenSize)
	for i, emb := range inputs.Embeddings {
		copy(flatData[i*hiddenSize:(i+1)*hiddenSize], emb)
	}

	// Create input tensor [batch, hidden_size]
	inputTensor, err := ort.NewTensor(ort.NewShape(int64(batchSize), int64(hiddenSize)), flatData)
	if err != nil {
		return nil, fmt.Errorf("creating embeddings tensor: %w", err)
	}
	defer inputTensor.Destroy()

	// For projection models, there's typically just one input
	inputTensors := []ort.Value{inputTensor}

	// Run inference
	outputTensors := make([]ort.Value, len(m.outputNames))
	for i := range outputTensors {
		outputTensors[i] = nil
	}
	err = m.session.Run(inputTensors, outputTensors)
	if err != nil {
		return nil, fmt.Errorf("running ONNX projection: %w", err)
	}
	defer func() {
		for _, t := range outputTensors {
			if t != nil {
				t.Destroy()
			}
		}
	}()

	if len(outputTensors) == 0 || outputTensors[0] == nil {
		return nil, fmt.Errorf("no output tensors returned")
	}

	// Get the output tensor
	outputTensor := outputTensors[0]
	outputShape := outputTensor.GetShape()

	// Type assert to get the data
	floatTensor, ok := outputTensor.(*ort.Tensor[float32])
	if !ok {
		return nil, fmt.Errorf("output tensor is not float32")
	}
	outputData := floatTensor.GetData()

	// Output should be 2D [batch, projection_dim]
	if len(outputShape) != 2 {
		return nil, fmt.Errorf("projection output has unexpected shape: %v (expected 2D)", outputShape)
	}

	projectionDim := int(outputShape[1])

	// Build output embeddings
	embeddings := make([][]float32, batchSize)
	for i := 0; i < batchSize; i++ {
		embeddings[i] = make([]float32, projectionDim)
		copy(embeddings[i], outputData[i*projectionDim:(i+1)*projectionDim])
	}

	return &ModelOutput{
		Embeddings: embeddings,
	}, nil
}

// forwardVision handles inference for vision models (e.g., CLIP visual encoder).
func (m *ortModel) forwardVision(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	batchSize := inputs.ImageBatch
	if batchSize == 0 {
		batchSize = 1
	}
	channels := inputs.ImageChannels
	height := inputs.ImageHeight
	width := inputs.ImageWidth

	// Create pixel_values tensor [batch, channels, height, width]
	pixelTensor, err := ort.NewTensor(ort.NewShape(int64(batchSize), int64(channels), int64(height), int64(width)), inputs.ImagePixels)
	if err != nil {
		return nil, fmt.Errorf("creating pixel_values tensor: %w", err)
	}
	defer pixelTensor.Destroy()

	// Build input tensors
	inputTensors := make([]ort.Value, 0, len(m.inputNames))
	for _, name := range m.inputNames {
		switch name {
		case "pixel_values":
			inputTensors = append(inputTensors, pixelTensor)
		}
	}

	if len(inputTensors) == 0 {
		return nil, fmt.Errorf("no valid input tensors created for vision model (expected pixel_values)")
	}

	// Run inference
	outputTensors := make([]ort.Value, len(m.outputNames))
	for i := range outputTensors {
		outputTensors[i] = nil
	}
	err = m.session.Run(inputTensors, outputTensors)
	if err != nil {
		return nil, fmt.Errorf("running ONNX inference: %w", err)
	}
	defer func() {
		for _, t := range outputTensors {
			if t != nil {
				t.Destroy()
			}
		}
	}()

	if len(outputTensors) == 0 || outputTensors[0] == nil {
		return nil, fmt.Errorf("no output tensors returned")
	}

	// Select best output tensor: prefer pooler_output by name over last_hidden_state.
	// CLIP visual encoder exports both ["last_hidden_state", "pooler_output"];
	// pooler_output is LayerNorm(CLS token) which is what visual_projection.onnx expects.
	outputTensor := outputTensors[0]
	outputShape := outputTensor.GetShape()
	for i, name := range m.outputNames {
		if name == "pooler_output" && i < len(outputTensors) && outputTensors[i] != nil {
			outputTensor = outputTensors[i]
			outputShape = outputTensor.GetShape()
			break
		}
	}

	// Type assert to get the data
	floatTensor, ok := outputTensor.(*ort.Tensor[float32])
	if !ok {
		return nil, fmt.Errorf("output tensor is not float32")
	}
	outputData := floatTensor.GetData()

	// Handle different output shapes:
	// - 3D [batch, seq, hidden]: hidden states (ViT outputs [batch, num_patches, hidden])
	// - 2D [batch, hidden]: pooled embeddings (pooler_output or models that pool internally)
	switch len(outputShape) {
	case 3:
		// Vision encoder output: [batch, num_patches, hidden]
		seqLen := int(outputShape[1])
		hiddenSize := int(outputShape[2])

		lastHiddenState := make([][][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			lastHiddenState[i] = make([][]float32, seqLen)
			for j := 0; j < seqLen; j++ {
				lastHiddenState[i][j] = make([]float32, hiddenSize)
				baseIdx := (i*seqLen + j) * hiddenSize
				copy(lastHiddenState[i][j], outputData[baseIdx:baseIdx+hiddenSize])
			}
		}

		// For vision models, use CLS token (first token) as embedding
		embeddings := make([][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			copy(embeddings[i], lastHiddenState[i][0])
		}

		return &ModelOutput{
			LastHiddenState: lastHiddenState,
			Embeddings:      embeddings,
		}, nil

	case 2:
		// Pooled output: [batch, hidden]
		hiddenSize := int(outputShape[1])

		embeddings := make([][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			baseIdx := i * hiddenSize
			copy(embeddings[i], outputData[baseIdx:baseIdx+hiddenSize])
		}

		return &ModelOutput{
			Embeddings: embeddings,
		}, nil

	default:
		return nil, fmt.Errorf("unexpected output shape: %v (expected 2D or 3D)", outputShape)
	}
}

// forwardAudio handles inference for audio models (e.g., CLAP audio encoder).
// Takes mel spectrogram features and outputs embeddings.
func (m *ortModel) forwardAudio(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	batchSize := inputs.AudioBatch
	if batchSize == 0 {
		batchSize = 1
	}
	timeSteps := inputs.AudioTime
	nMels := inputs.AudioMels

	// CLAP expects 4D input: [batch, channels, time, mels]
	// The mel spectrogram is mono (1 channel)
	channels := 1

	// Create input tensor for audio features [batch, channels, time, mels]
	inputTensor, err := ort.NewTensor(ort.NewShape(int64(batchSize), int64(channels), int64(timeSteps), int64(nMels)), inputs.AudioFeatures)
	if err != nil {
		return nil, fmt.Errorf("creating audio features tensor: %w", err)
	}
	defer inputTensor.Destroy()

	inputTensors := []ort.Value{inputTensor}

	// Run inference
	outputTensors := make([]ort.Value, len(m.outputNames))
	if err := m.session.Run(inputTensors, outputTensors); err != nil {
		return nil, fmt.Errorf("running audio inference: %w", err)
	}

	// Clean up output tensors when done
	defer func() {
		for _, t := range outputTensors {
			if t != nil {
				t.Destroy()
			}
		}
	}()

	if len(outputTensors) == 0 || outputTensors[0] == nil {
		return nil, fmt.Errorf("no output tensors returned")
	}

	// Select best output tensor: prefer pooler_output by name over last_hidden_state.
	// Models like CLAP export both last_hidden_state and pooler_output; we want the pooled one.
	outputTensor := outputTensors[0]
	outputShape := outputTensor.GetShape()
	for i, name := range m.outputNames {
		if name == "pooler_output" && i < len(outputTensors) && outputTensors[i] != nil {
			outputTensor = outputTensors[i]
			outputShape = outputTensor.GetShape()
			break
		}
	}

	// Type assert to get the data
	floatTensor, ok := outputTensor.(*ort.Tensor[float32])
	if !ok {
		return nil, fmt.Errorf("audio output tensor is not float32")
	}
	outputData := floatTensor.GetData()

	// Handle different output shapes:
	// - 3D [batch, seq, hidden]: hidden states (need pooling)
	// - 2D [batch, hidden]: pooled embeddings (CLAP style)
	switch len(outputShape) {
	case 3:
		// Audio encoder output: [batch, seq, hidden]
		seqLen := int(outputShape[1])
		hiddenSize := int(outputShape[2])

		lastHiddenState := make([][][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			lastHiddenState[i] = make([][]float32, seqLen)
			for j := 0; j < seqLen; j++ {
				lastHiddenState[i][j] = make([]float32, hiddenSize)
				baseIdx := (i*seqLen + j) * hiddenSize
				copy(lastHiddenState[i][j], outputData[baseIdx:baseIdx+hiddenSize])
			}
		}

		// For audio models, use mean pooling over time
		embeddings := make([][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			for j := 0; j < seqLen; j++ {
				for h := 0; h < hiddenSize; h++ {
					embeddings[i][h] += lastHiddenState[i][j][h]
				}
			}
			for h := 0; h < hiddenSize; h++ {
				embeddings[i][h] /= float32(seqLen)
			}
		}

		return &ModelOutput{
			LastHiddenState: lastHiddenState,
			Embeddings:      embeddings,
		}, nil

	case 2:
		// Pooled output: [batch, hidden]
		hiddenSize := int(outputShape[1])

		embeddings := make([][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			baseIdx := i * hiddenSize
			copy(embeddings[i], outputData[baseIdx:baseIdx+hiddenSize])
		}

		return &ModelOutput{
			Embeddings: embeddings,
		}, nil

	default:
		return nil, fmt.Errorf("unexpected audio output shape: %v (expected 2D or 3D)", outputShape)
	}
}

// forwardText handles inference for text models (e.g., BERT, CLIP text encoder).
func (m *ortModel) forwardText(ctx context.Context, inputs *ModelInputs) (*ModelOutput, error) {
	batchSize := len(inputs.InputIDs)
	if batchSize == 0 {
		return &ModelOutput{}, nil
	}

	seqLen := len(inputs.InputIDs[0])

	// Flatten input tensors
	flatInputIDs := make([]int64, batchSize*seqLen)
	flatAttentionMask := make([]int64, batchSize*seqLen)
	for i := 0; i < batchSize; i++ {
		for j := 0; j < seqLen; j++ {
			flatInputIDs[i*seqLen+j] = int64(inputs.InputIDs[i][j])
			flatAttentionMask[i*seqLen+j] = int64(inputs.AttentionMask[i][j])
		}
	}

	// Create input tensors
	inputIDsTensor, err := ort.NewTensor(ort.NewShape(int64(batchSize), int64(seqLen)), flatInputIDs)
	if err != nil {
		return nil, fmt.Errorf("creating input_ids tensor: %w", err)
	}
	defer inputIDsTensor.Destroy()

	attentionMaskTensor, err := ort.NewTensor(ort.NewShape(int64(batchSize), int64(seqLen)), flatAttentionMask)
	if err != nil {
		return nil, fmt.Errorf("creating attention_mask tensor: %w", err)
	}
	defer attentionMaskTensor.Destroy()

	// Build input tensors based on what the model expects
	inputTensors := make([]ort.Value, 0, len(m.inputNames))
	var tokenTypeIdsTensor *ort.Tensor[int64]
	for _, name := range m.inputNames {
		switch name {
		case "input_ids":
			inputTensors = append(inputTensors, inputIDsTensor)
		case "attention_mask":
			inputTensors = append(inputTensors, attentionMaskTensor)
		case "token_type_ids":
			// Create zeros tensor for token_type_ids (used by some BERT models)
			flatTokenTypeIds := make([]int64, batchSize*seqLen) // zeros by default
			tokenTypeIdsTensor, err = ort.NewTensor(ort.NewShape(int64(batchSize), int64(seqLen)), flatTokenTypeIds)
			if err != nil {
				return nil, fmt.Errorf("creating token_type_ids tensor: %w", err)
			}
			inputTensors = append(inputTensors, tokenTypeIdsTensor)
		}
	}
	if tokenTypeIdsTensor != nil {
		defer tokenTypeIdsTensor.Destroy()
	}

	// Run inference - pass nil outputs to let session allocate them
	outputTensors := make([]ort.Value, len(m.outputNames))
	for i := range outputTensors {
		outputTensors[i] = nil
	}
	err = m.session.Run(inputTensors, outputTensors)
	if err != nil {
		return nil, fmt.Errorf("running ONNX inference: %w", err)
	}
	defer func() {
		for _, t := range outputTensors {
			if t != nil {
				t.Destroy()
			}
		}
	}()

	if len(outputTensors) == 0 || outputTensors[0] == nil {
		return nil, fmt.Errorf("no output tensors returned")
	}

	// Get the output tensor and extract data.
	// For models with multiple outputs (e.g., CLIP text encoder exports
	// ["last_hidden_state", "pooler_output"]), we prefer the first output
	// for text models since we apply our own pooling strategy. Text models
	// that use a projection layer (CLIP/CLAP) need pooler_output, but
	// the projection is applied separately via EmbeddingPipeline.Projector.
	outputTensor := outputTensors[0]
	outputShape := outputTensor.GetShape()

	// Type assert to get the data
	floatTensor, ok := outputTensor.(*ort.Tensor[float32])
	if !ok {
		return nil, fmt.Errorf("output tensor is not float32")
	}
	outputData := floatTensor.GetData()

	// Handle different output shapes:
	// - 3D [batch, seq, hidden]: hidden states (encoder models)
	// - 2D [batch, classes]: logits (classification models)
	switch len(outputShape) {
	case 3:
		// Standard encoder output: [batch, seq, hidden]
		hiddenSize := int(outputShape[2])

		lastHiddenState := make([][][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			lastHiddenState[i] = make([][]float32, seqLen)
			for j := 0; j < seqLen; j++ {
				lastHiddenState[i][j] = make([]float32, hiddenSize)
				baseIdx := (i*seqLen + j) * hiddenSize
				copy(lastHiddenState[i][j], outputData[baseIdx:baseIdx+hiddenSize])
			}
		}

		// Apply pooling to get embeddings
		embeddings := m.poolHiddenStates(lastHiddenState, inputs.AttentionMask)

		return &ModelOutput{
			LastHiddenState: lastHiddenState,
			Embeddings:      embeddings,
		}, nil

	case 2:
		// 2D output: [batch, dim] - could be logits (classification) or embeddings (CLIP/CLAP)
		// We set both so consumers can use whichever is appropriate
		dim := int(outputShape[1])

		output := make([][]float32, batchSize)
		for i := 0; i < batchSize; i++ {
			output[i] = make([]float32, dim)
			baseIdx := i * dim
			copy(output[i], outputData[baseIdx:baseIdx+dim])
		}

		return &ModelOutput{
			Logits:     output, // For classification models
			Embeddings: output, // For embedding models (CLIP/CLAP text encoders)
		}, nil

	default:
		return nil, fmt.Errorf("unexpected output shape: %v (expected 2D or 3D)", outputShape)
	}
}

// poolHiddenStates applies pooling to get [batch, hidden] embeddings.
func (m *ortModel) poolHiddenStates(hiddenStates [][][]float32, attentionMask [][]int32) [][]float32 {
	batchSize := len(hiddenStates)
	if batchSize == 0 {
		return nil
	}
	hiddenSize := len(hiddenStates[0][0])

	embeddings := make([][]float32, batchSize)

	switch m.config.Pooling {
	case "cls":
		// Use [CLS] token (first token)
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			copy(embeddings[i], hiddenStates[i][0])
		}
	case "max":
		// Max pooling over sequence
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			for h := 0; h < hiddenSize; h++ {
				maxVal := float32(-1e9)
				for j := 0; j < len(hiddenStates[i]); j++ {
					if attentionMask[i][j] > 0 && hiddenStates[i][j][h] > maxVal {
						maxVal = hiddenStates[i][j][h]
					}
				}
				embeddings[i][h] = maxVal
			}
		}
	case "mean", "":
		// Mean pooling (default)
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			count := float32(0)
			for j := 0; j < len(hiddenStates[i]); j++ {
				if attentionMask[i][j] > 0 {
					for h := 0; h < hiddenSize; h++ {
						embeddings[i][h] += hiddenStates[i][j][h]
					}
					count++
				}
			}
			if count > 0 {
				for h := 0; h < hiddenSize; h++ {
					embeddings[i][h] /= count
				}
			}
		}
	default:
		// No pooling - return first token
		for i := 0; i < batchSize; i++ {
			embeddings[i] = make([]float32, hiddenSize)
			copy(embeddings[i], hiddenStates[i][0])
		}
	}

	return embeddings
}

func (m *ortModel) Close() error {
	if m.session != nil {
		m.session.Destroy()
		m.session = nil
	}
	if m.sessionOpts != nil {
		m.sessionOpts.Destroy()
		m.sessionOpts = nil
	}
	return nil
}

func (m *ortModel) Name() string {
	return m.path
}

func (m *ortModel) Backend() BackendType {
	return BackendONNX
}

func (m *ortModel) InputInfo() []InputOutputInfo {
	return m.inputInfo
}

// onnxSessionFactory implements SessionFactory for ONNX Runtime.
type onnxSessionFactory struct {
	backend *onnxBackend
}

func (f *onnxSessionFactory) CreateSession(modelPath string, opts ...SessionOption) (Session, error) {
	// Initialize ONNX Runtime if needed
	if err := f.backend.initONNX(); err != nil {
		return nil, fmt.Errorf("initializing ONNX Runtime: %w", err)
	}

	cfg := ApplySessionOptions(opts...)

	// Get input/output info from the model
	inputs, outputs, err := ort.GetInputOutputInfo(modelPath)
	if err != nil {
		return nil, fmt.Errorf("getting model info: %w", err)
	}

	inputNames := make([]string, len(inputs))
	inputInfo := make([]TensorInfo, len(inputs))
	for i, info := range inputs {
		inputNames[i] = info.Name
		inputInfo[i] = TensorInfo{
			Name:     info.Name,
			Shape:    info.Dimensions,
			DataType: onnxDataType(info.DataType),
		}
	}

	outputNames := make([]string, len(outputs))
	outputInfo := make([]TensorInfo, len(outputs))
	for i, info := range outputs {
		outputNames[i] = info.Name
		outputInfo[i] = TensorInfo{
			Name:     info.Name,
			Shape:    info.Dimensions,
			DataType: onnxDataType(info.DataType),
		}
	}

	// Create session options
	sessionOpts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, fmt.Errorf("creating session options: %w", err)
	}

	// Use shared environment allocator if available
	if err := f.backend.enableSharedAllocator(sessionOpts); err != nil {
		sessionOpts.Destroy()
		return nil, fmt.Errorf("enabling shared allocator: %w", err)
	}

	// Configure number of threads
	if cfg.NumThreads > 0 {
		if err := sessionOpts.SetIntraOpNumThreads(cfg.NumThreads); err != nil {
			sessionOpts.Destroy()
			return nil, fmt.Errorf("setting thread count: %w", err)
		}
	}

	// Enable CUDA if requested and available
	gpuMode := cfg.GPUMode
	if gpuMode == "" {
		gpuMode = f.backend.GetGPUMode()
	}
	useCUDA := gpuMode == GPUModeCuda || (gpuMode == GPUModeAuto && f.backend.useCUDA())
	if useCUDA {
		cudaOpts, err := ort.NewCUDAProviderOptions()
		if err == nil {
			if err := sessionOpts.AppendExecutionProviderCUDA(cudaOpts); err != nil {
				cudaOpts.Destroy()
			} else {
				defer cudaOpts.Destroy()
			}
		}
	}

	// Create the session
	session, err := ort.NewDynamicAdvancedSession(modelPath, inputNames, outputNames, sessionOpts)
	if err != nil {
		sessionOpts.Destroy()
		return nil, fmt.Errorf("creating ONNX session: %w", err)
	}

	return &onnxSession{
		session:     session,
		sessionOpts: sessionOpts,
		inputInfo:   inputInfo,
		outputInfo:  outputInfo,
	}, nil
}

func (f *onnxSessionFactory) Backend() BackendType {
	return BackendONNX
}

// onnxDataType converts ONNX data type to our DataType.
func onnxDataType(dt ort.TensorElementDataType) DataType {
	switch dt {
	case ort.TensorElementDataTypeFloat:
		return DataTypeFloat32
	case ort.TensorElementDataTypeInt64:
		return DataTypeInt64
	case ort.TensorElementDataTypeInt32:
		return DataTypeInt32
	case ort.TensorElementDataTypeBool:
		return DataTypeBool
	default:
		return DataTypeFloat32
	}
}

// onnxSession implements Session for ONNX Runtime.
type onnxSession struct {
	session     *ort.DynamicAdvancedSession
	sessionOpts *ort.SessionOptions
	inputInfo   []TensorInfo
	outputInfo  []TensorInfo
}

func (s *onnxSession) Run(inputs []NamedTensor) ([]NamedTensor, error) {
	if s.session == nil {
		return nil, fmt.Errorf("session is closed")
	}

	// Build a map of input name -> tensor for fast lookup
	inputMap := make(map[string]NamedTensor, len(inputs))
	for _, input := range inputs {
		inputMap[input.Name] = input
	}

	// Convert inputs to ONNX tensors in the order expected by the session
	ortInputs := make([]ort.Value, len(s.inputInfo))
	for i, info := range s.inputInfo {
		input, ok := inputMap[info.Name]
		if !ok {
			// Clean up already created tensors
			for j := 0; j < i; j++ {
				if ortInputs[j] != nil {
					ortInputs[j].Destroy()
				}
			}
			return nil, fmt.Errorf("missing input tensor: %s", info.Name)
		}
		tensor, err := createOrtTensor(input)
		if err != nil {
			// Clean up already created tensors
			for j := 0; j < i; j++ {
				if ortInputs[j] != nil {
					ortInputs[j].Destroy()
				}
			}
			return nil, fmt.Errorf("creating input tensor %s: %w", input.Name, err)
		}
		ortInputs[i] = tensor
	}
	defer func() {
		for _, t := range ortInputs {
			if t != nil {
				t.Destroy()
			}
		}
	}()

	// Run inference
	ortOutputs := make([]ort.Value, len(s.outputInfo))
	for i := range ortOutputs {
		ortOutputs[i] = nil
	}

	if err := s.session.Run(ortInputs, ortOutputs); err != nil {
		return nil, fmt.Errorf("running ONNX session: %w", err)
	}
	defer func() {
		for _, t := range ortOutputs {
			if t != nil {
				t.Destroy()
			}
		}
	}()

	// Convert outputs to NamedTensors
	outputs := make([]NamedTensor, len(ortOutputs))
	for i, ortOutput := range ortOutputs {
		if ortOutput == nil {
			continue
		}
		output, err := extractOrtTensor(ortOutput, s.outputInfo[i].Name)
		if err != nil {
			return nil, fmt.Errorf("extracting output tensor %s: %w", s.outputInfo[i].Name, err)
		}
		outputs[i] = output
	}

	return outputs, nil
}

func (s *onnxSession) InputInfo() []TensorInfo {
	return s.inputInfo
}

func (s *onnxSession) OutputInfo() []TensorInfo {
	return s.outputInfo
}

func (s *onnxSession) Close() error {
	if s.session != nil {
		s.session.Destroy()
		s.session = nil
	}
	if s.sessionOpts != nil {
		s.sessionOpts.Destroy()
		s.sessionOpts = nil
	}
	return nil
}

// sentinelByte is a non-nil backing buffer for zero-element tensors.
// ORT kernels like GroupQueryAttention dereference the tensor data pointer
// without null-checking when past_sequence_length is 0, causing a segfault
// when onnxruntime_go passes NULL for empty slices. Using CustomDataTensor
// with a valid pointer avoids this.
var sentinelByte = []byte{0, 0, 0, 0}

// createOrtTensor creates an ORT tensor from a NamedTensor.
func createOrtTensor(input NamedTensor) (ort.Value, error) {
	shape := ort.NewShape(input.Shape...)

	// For zero-element tensors (e.g., empty KV cache with shape [1,1,0,256]),
	// use CustomDataTensor with a valid (non-NULL) backing pointer.
	// ort.NewTensor passes NULL to CreateTensorWithDataAsOrtValue for empty
	// slices, and some ORT kernels (GroupQueryAttention) dereference it
	// without a null check, causing a segfault.
	if shape.FlattenedSize() == 0 {
		var dataType ort.TensorElementDataType = ort.TensorElementDataTypeFloat
		switch input.Data.(type) {
		case []int64:
			dataType = ort.TensorElementDataTypeInt64
		case []int32:
			dataType = ort.TensorElementDataTypeInt32
		case []bool:
			dataType = ort.TensorElementDataTypeBool
		}
		return ort.NewCustomDataTensor(shape, sentinelByte, dataType)
	}

	switch data := input.Data.(type) {
	case []float32:
		return ort.NewTensor(shape, data)
	case []int64:
		return ort.NewTensor(shape, data)
	case []int32:
		// Convert to int64 for ONNX
		int64Data := make([]int64, len(data))
		for i, v := range data {
			int64Data[i] = int64(v)
		}
		return ort.NewTensor(shape, int64Data)
	case []bool:
		return ort.NewTensor(shape, data)
	default:
		return nil, fmt.Errorf("unsupported data type: %T", data)
	}
}

// extractOrtTensor extracts a NamedTensor from an ORT tensor.
func extractOrtTensor(ortTensor ort.Value, name string) (NamedTensor, error) {
	shape := ortTensor.GetShape()

	// Try float32 first (most common)
	if floatTensor, ok := ortTensor.(*ort.Tensor[float32]); ok {
		data := floatTensor.GetData()
		dataCopy := make([]float32, len(data))
		copy(dataCopy, data)
		return NamedTensor{
			Name:  name,
			Shape: shape,
			Data:  dataCopy,
		}, nil
	}

	// Try int64
	if int64Tensor, ok := ortTensor.(*ort.Tensor[int64]); ok {
		data := int64Tensor.GetData()
		dataCopy := make([]int64, len(data))
		copy(dataCopy, data)
		return NamedTensor{
			Name:  name,
			Shape: shape,
			Data:  dataCopy,
		}, nil
	}

	// Try int32
	if int32Tensor, ok := ortTensor.(*ort.Tensor[int32]); ok {
		data := int32Tensor.GetData()
		dataCopy := make([]int32, len(data))
		copy(dataCopy, data)
		return NamedTensor{
			Name:  name,
			Shape: shape,
			Data:  dataCopy,
		}, nil
	}

	// Try bool
	if boolTensor, ok := ortTensor.(*ort.Tensor[bool]); ok {
		data := boolTensor.GetData()
		dataCopy := make([]bool, len(data))
		copy(dataCopy, data)
		return NamedTensor{
			Name:  name,
			Shape: shape,
			Data:  dataCopy,
		}, nil
	}

	return NamedTensor{}, fmt.Errorf("unsupported tensor type")
}

// GenerativeSessionFactory returns a factory for creating generative (LLM) sessions.
// This enables ortgenai-based text generation through the unified backend interface.
func (b *onnxBackend) GenerativeSessionFactory() GenerativeSessionFactory {
	return &onnxGenerativeSessionFactory{}
}
