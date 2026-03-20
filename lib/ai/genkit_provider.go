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

package ai

import (
	"context"
	"fmt"

	generatinggenkit "github.com/antflydb/antfly/pkg/generating/genkit"
)

func NewGenKitGenerator(ctx context.Context, config GeneratorConfig) (*GenKitModelImpl, error) {
	modelInfo, err := generatinggenkit.NewModel(ctx, config)
	if err != nil {
		return nil, err
	}

	var opts []GenKitOption
	if modelInfo.MaxOutputTokens > 0 {
		opts = append(opts, WithMaxOutputTokens(modelInfo.MaxOutputTokens))
	}

	if modelInfo.Genkit == nil || modelInfo.Model == nil {
		return nil, fmt.Errorf("generating/genkit returned an incomplete model")
	}

	return NewGenKitSummarizer(modelInfo.Genkit, modelInfo.Model, opts...), nil
}
