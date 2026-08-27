/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/go/pkg/sdk/modelcache"
)

const (
	ModelFormatGGUF = modelcache.ModelFormatGGUF

	DefaultTextEmbeddingModel  = "antflydb/clipclap:gguf:Q4_K"
	DefaultImageEmbeddingModel = "antflydb/clipclap:gguf:Q4_K"
	DefaultExtractorModel      = "antflydb/gliner2-base-v1:gguf:Q4_K"
	DefaultRerankerModel       = "antflydb/mxbai-rerank-base-v1"
)

type (
	ModelRef          = modelcache.ModelRef
	ModelPullOptions  = modelcache.ModelPullOptions
	ModelPullProgress = modelcache.ModelPullProgress
	ModelPullError    = modelcache.ModelPullError
)

// SupportedModel describes an Antfly-hosted model that can be pulled from
// HuggingFace into the local Antfly inference cache.
type SupportedModel struct {
	Name           string
	Task           string
	DefaultFormat  string
	DefaultVariant string
}

var supportedModels = []SupportedModel{
	{Name: DefaultTextEmbeddingModel, Task: "embedder", DefaultFormat: ModelFormatGGUF, DefaultVariant: "Q4_K"},
	{Name: DefaultExtractorModel, Task: "extractor", DefaultFormat: ModelFormatGGUF, DefaultVariant: "Q4_K"},
	{Name: DefaultRerankerModel, Task: "reranker", DefaultFormat: ModelFormatGGUF, DefaultVariant: "Q4_K"},
}

// SupportedModels returns the Antfly-hosted model set supported by the SDK.
func SupportedModels() []SupportedModel {
	out := make([]SupportedModel, len(supportedModels))
	copy(out, supportedModels)
	return out
}

// IsSupportedModel reports whether model is one of the Antfly-hosted models
// that the SDK knows how to pull and place in the v0.2 inference cache.
func IsSupportedModel(model string) bool {
	_, ok := supportedModel(model)
	return ok
}

func supportedModel(model string) (SupportedModel, bool) {
	ref, err := modelcache.ParseModelRef(model)
	if err != nil {
		return SupportedModel{}, false
	}
	for _, m := range supportedModels {
		metaRef, err := modelcache.ParseModelRef(m.Name)
		if err == nil && metaRef.BaseName() == ref.BaseName() {
			return m, true
		}
	}
	return SupportedModel{}, false
}

// DefaultModelsDir returns the Antfly v0.2 local inference model cache:
// ~/.antfly/inference/models.
func DefaultModelsDir() (string, error) {
	return modelcache.DefaultModelsDir()
}

// ModelDir returns the flat owner/repo directory for model under modelsDir.
func ModelDir(modelsDir, model string) (string, error) {
	return modelcache.ModelDir(modelsDir, model)
}

// PullHuggingFaceModel pulls a supported Antfly model from HuggingFace into
// ~/.antfly/inference/models/<owner>/<repo> and returns that model directory.
func PullHuggingFaceModel(ctx context.Context, model string, opts ModelPullOptions) (string, error) {
	meta, ok := supportedModel(model)
	if !ok {
		return "", fmt.Errorf("unsupported Antfly model %q", model)
	}
	return modelcache.PullHuggingFaceModel(ctx, model, modelcache.ModelSpec{
		Task:           meta.Task,
		DefaultFormat:  meta.DefaultFormat,
		DefaultVariant: meta.DefaultVariant,
	}, opts)
}

// ParseModelRef parses owner/repo, owner/repo:variant, and
// owner/repo:format:variant refs. The cache directory always uses BaseName.
func ParseModelRef(model string) (ModelRef, error) {
	return modelcache.ParseModelRef(model)
}

// BaseModelName returns owner/repo for model refs with optional tags.
func BaseModelName(model string) string {
	return modelcache.BaseModelName(model)
}
