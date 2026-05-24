package reranker

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"sync"

	"github.com/gomlx/go-huggingface/tokenizers/api"
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
var rerankerModelONNX []byte

//go:embed builtin_model/tokenizer.json
var rerankerTokenizerJSON []byte

// MaxSequenceLength is the maximum number of tokens the model supports.
// cross-encoder/ms-marco-MiniLM-L-6-v2 uses BERT positional embeddings fixed at 512.
const MaxSequenceLength = 512

// ModelName is the canonical name for the built-in reranker.
const ModelName = "antfly-builtin-reranker"

// BuiltinReranker is the built-in reranker using an embedded cross-encoder/ms-marco-MiniLM-L-6-v2 model.
type BuiltinReranker struct {
	tok    *hftokenizer.Tokenizer
	exec   *mlctx.Exec
	engine backends.Backend
	mu     sync.Mutex
	clsID  int
	sepID  int
}

var (
	instance *BuiltinReranker
	once     sync.Once
	initErr  error
)

// Get returns the singleton BuiltinReranker instance.
func Get() (*BuiltinReranker, error) {
	once.Do(func() {
		tok, err := hftokenizer.NewFromContent(nil, rerankerTokenizerJSON)
		if err != nil {
			initErr = fmt.Errorf("loading reranker tokenizer: %w", err)
			return
		}

		clsID, err := tok.SpecialTokenID(api.TokClassification)
		if err != nil {
			initErr = fmt.Errorf("resolving [CLS] token: %w", err)
			return
		}
		sepID, err := tok.SpecialTokenID(api.TokEndOfSentence)
		if err != nil {
			initErr = fmt.Errorf("resolving [SEP] token: %w", err)
			return
		}

		om, err := parser.Parse(rerankerModelONNX)
		if err != nil {
			initErr = fmt.Errorf("parsing reranker ONNX model: %w", err)
			return
		}

		ctx := mlctx.New()
		if err := om.VariablesToContext(ctx); err != nil {
			initErr = fmt.Errorf("loading reranker ONNX variables: %w", err)
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
			initErr = fmt.Errorf("compiling reranker ONNX graph: %w", err)
			return
		}

		instance = &BuiltinReranker{
			tok:    tok,
			exec:   exec,
			engine: engine,
			clsID:  clsID,
			sepID:  sepID,
		}
	})
	return instance, initErr
}

// RerankTexts scores raw text prompts against a query.
func (r *BuiltinReranker) RerankTexts(_ context.Context, query string, prompts []string) ([]float32, error) {
	if len(prompts) == 0 {
		return []float32{}, nil
	}

	batchSize := len(prompts)

	// Encode without post-processing — we add [CLS]/[SEP] manually below.
	queryIDs := r.tok.EncodeWithOptions(query, false)

	type pairTokens struct {
		ids     []int
		typeIDs []int
	}

	var maxLen int
	pairs := make([]pairTokens, batchSize)

	// Budget for document tokens: total limit minus special tokens ([CLS], 2x[SEP]) and query.
	docBudget := max(MaxSequenceLength-3-len(queryIDs), 1)

	for i, doc := range prompts {
		docIDs := r.tok.EncodeWithOptions(doc, false)
		if len(docIDs) > docBudget {
			docIDs = docIDs[:docBudget]
		}

		ids := make([]int, 0, 3+len(queryIDs)+len(docIDs))
		ids = append(ids, r.clsID)
		ids = append(ids, queryIDs...)
		ids = append(ids, r.sepID)
		ids = append(ids, docIDs...)
		ids = append(ids, r.sepID)

		typeIDs := make([]int, len(ids))
		docStart := 1 + len(queryIDs) + 1
		for j := docStart; j < len(typeIDs); j++ {
			typeIDs[j] = 1
		}

		if len(ids) > maxLen {
			maxLen = len(ids)
		}
		pairs[i] = pairTokens{ids: ids, typeIDs: typeIDs}
	}

	if maxLen == 0 {
		maxLen = 1
	}

	flatIDs := make([]int64, batchSize*maxLen)
	flatMask := make([]int64, batchSize*maxLen)
	flatTypeIDs := make([]int64, batchSize*maxLen)

	for i, p := range pairs {
		for j, id := range p.ids {
			flatIDs[i*maxLen+j] = int64(id)
			flatMask[i*maxLen+j] = 1
			flatTypeIDs[i*maxLen+j] = int64(p.typeIDs[j])
		}
	}

	inputIDsTensor := tensors.FromFlatDataAndDimensions(flatIDs, batchSize, maxLen)
	attentionMaskTensor := tensors.FromFlatDataAndDimensions(flatMask, batchSize, maxLen)
	tokenTypeIDsTensor := tensors.FromFlatDataAndDimensions(flatTypeIDs, batchSize, maxLen)

	r.mu.Lock()
	results, err := r.exec.Exec(inputIDsTensor, attentionMaskTensor, tokenTypeIDsTensor)
	r.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("reranker ONNX inference failed: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no output from reranker ONNX model")
	}

	output := results[0]
	shape := output.Shape()

	scores := make([]float32, batchSize)

	switch len(shape.Dimensions) {
	case 2:
		data := output.Value().([][]float32)
		for i := range batchSize {
			scores[i] = sigmoid(data[i][0])
		}
	case 1:
		data := output.Value().([]float32)
		for i := range batchSize {
			scores[i] = sigmoid(data[i])
		}
	default:
		return nil, fmt.Errorf("unexpected reranker output shape: %v", shape.Dimensions)
	}

	return scores, nil
}

// Rerank implements reranking.Model by delegating to RerankTexts.
func (r *BuiltinReranker) Rerank(ctx context.Context, query string, prompts []string) ([]float32, error) {
	return r.RerankTexts(ctx, query, prompts)
}

// Close is a no-op for the singleton reranker.
func (r *BuiltinReranker) Close() error {
	return nil
}

func sigmoid(x float32) float32 {
	return float32(1.0 / (1.0 + math.Exp(-float64(x))))
}
