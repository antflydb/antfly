package embedder

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"sync"

	"github.com/gomlx/go-huggingface/tokenizers/hftokenizer"
	"github.com/gomlx/gomlx/backends"
	_ "github.com/gomlx/gomlx/backends/simplego"
	_ "github.com/gomlx/gomlx/backends/simplego/highway"
	"github.com/gomlx/gomlx/pkg/core/graph"
	"github.com/gomlx/gomlx/pkg/core/tensors"
	mlctx "github.com/gomlx/gomlx/pkg/ml/context"
	"github.com/gomlx/onnx-gomlx/onnx/parser"
)

//go:embed builtin_model/model_i8.onnx
var embeddedModelONNX []byte

//go:embed builtin_model/tokenizer.json
var embeddedTokenizerJSON []byte

// Dimension is the output embedding dimension of the built-in model (all-MiniLM-L6-v2).
const Dimension = 384

// MaxSequenceLength is the maximum number of tokens the model supports.
// all-MiniLM-L6-v2 uses BERT positional embeddings fixed at 512.
const MaxSequenceLength = 512

// ModelName is the canonical name for the built-in embedder.
const ModelName = "antfly-builtin-embedder"

// BuiltinEmbedder is the built-in embedder using an embedded all-MiniLM-L6-v2 model.
type BuiltinEmbedder struct {
	tok    *hftokenizer.Tokenizer
	exec   *mlctx.Exec
	engine backends.Backend
	mu     sync.Mutex
}

var (
	instance *BuiltinEmbedder
	once     sync.Once
	initErr  error
)

// Get returns the singleton BuiltinEmbedder instance.
func Get() (*BuiltinEmbedder, error) {
	once.Do(func() {
		tok, err := hftokenizer.NewFromContent(nil, embeddedTokenizerJSON)
		if err != nil {
			initErr = fmt.Errorf("loading embedded tokenizer: %w", err)
			return
		}

		om, err := parser.Parse(embeddedModelONNX)
		if err != nil {
			initErr = fmt.Errorf("parsing embedded ONNX model: %w", err)
			return
		}

		ctx := mlctx.New()
		if err := om.VariablesToContext(ctx); err != nil {
			initErr = fmt.Errorf("loading ONNX variables: %w", err)
			return
		}

		engine, err := backends.NewWithConfig("go")
		if err != nil {
			initErr = fmt.Errorf("creating GoMLX backend: %w", err)
			return
		}

		graphFn := func(mlCtx *mlctx.Context, inputs []*graph.Node) []*graph.Node {
			inputMap := map[string]*graph.Node{
				"input_ids":      inputs[0],
				"attention_mask": inputs[1],
				"token_type_ids": inputs[2],
			}
			return om.CallGraph(mlCtx.Reuse(), inputs[0].Graph(), inputMap)
		}

		exec, err := mlctx.NewExecAny(engine, ctx, graphFn)
		if err != nil {
			initErr = fmt.Errorf("compiling ONNX graph: %w", err)
			return
		}

		instance = &BuiltinEmbedder{
			tok:    tok,
			exec:   exec,
			engine: engine,
		}
	})
	return instance, initErr
}

// EmbedTexts generates embeddings for the given texts.
func (e *BuiltinEmbedder) EmbedTexts(_ context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	batchSize := len(texts)
	var maxLen int
	tokenized := make([][]int, batchSize)
	for i, text := range texts {
		tokenized[i] = e.tok.Encode(text)
		if len(tokenized[i]) > MaxSequenceLength {
			tokenized[i] = tokenized[i][:MaxSequenceLength]
		}
		if len(tokenized[i]) > maxLen {
			maxLen = len(tokenized[i])
		}
	}
	if maxLen == 0 {
		maxLen = 1
	}

	flatIDs := make([]int64, batchSize*maxLen)
	flatMask := make([]int64, batchSize*maxLen)
	for i, tokens := range tokenized {
		for j, tok := range tokens {
			flatIDs[i*maxLen+j] = int64(tok)
			flatMask[i*maxLen+j] = 1
		}
	}

	inputIDsTensor := tensors.FromFlatDataAndDimensions(flatIDs, batchSize, maxLen)
	attentionMaskTensor := tensors.FromFlatDataAndDimensions(flatMask, batchSize, maxLen)
	tokenTypeIDsTensor := tensors.FromFlatDataAndDimensions(make([]int64, batchSize*maxLen), batchSize, maxLen)

	e.mu.Lock()
	results, err := e.exec.Exec(inputIDsTensor, attentionMaskTensor, tokenTypeIDsTensor)
	e.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("ONNX inference failed: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no output from ONNX model")
	}

	output := results[0]
	shape := output.Shape()

	var embeddings [][]float32

	switch len(shape.Dimensions) {
	case 3:
		hiddenStates := output.Value().([][][]float32)
		embeddings = meanPool(hiddenStates, tokenized, batchSize)
	case 2:
		data := output.Value().([][]float32)
		embeddings = make([][]float32, batchSize)
		for i := range batchSize {
			embeddings[i] = make([]float32, len(data[i]))
			copy(embeddings[i], data[i])
		}
	default:
		return nil, fmt.Errorf("unexpected output shape: %v", shape.Dimensions)
	}

	for i := range embeddings {
		normalizeL2(embeddings[i])
	}

	return embeddings, nil
}

// Close is a no-op for the singleton embedder.
func (e *BuiltinEmbedder) Close() error {
	return nil
}

func meanPool(hiddenStates [][][]float32, tokenized [][]int, batchSize int) [][]float32 {
	hiddenSize := len(hiddenStates[0][0])
	embeddings := make([][]float32, batchSize)
	for i := range batchSize {
		embeddings[i] = make([]float32, hiddenSize)
		count := float32(len(tokenized[i]))
		if count == 0 {
			continue
		}
		for j := 0; j < len(tokenized[i]); j++ {
			for h := range hiddenSize {
				embeddings[i][h] += hiddenStates[i][j][h]
			}
		}
		for h := range hiddenSize {
			embeddings[i][h] /= count
		}
	}
	return embeddings
}

func normalizeL2(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	norm := float32(math.Sqrt(sum))
	for i := range v {
		v[i] /= norm
	}
}
