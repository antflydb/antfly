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

//go:build !onnx || !ORT

package generation

import (
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"go.uber.org/zap"
)

// LoadGenerator loads a text generation model using the available backends.
// Without ONNX support, this only tries the pipeline-based approach.
func LoadGenerator(
	modelPath string,
	poolSize int,
	logger *zap.Logger,
	sessionManager *backends.SessionManager,
	modelBackends []string,
) (Generator, backends.BackendType, error) {
	cfg := &PooledPipelineGeneratorConfig{
		ModelPath: modelPath,
		PoolSize:  poolSize,
		Logger:    logger,
	}
	return NewPooledPipelineGenerator(cfg, sessionManager, modelBackends)
}
