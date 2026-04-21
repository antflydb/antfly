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

package chunking

import "context"

type vadConfigKey struct{}

// WithVADConfig returns a context that carries VAD configuration.
func WithVADConfig(ctx context.Context, cfg VADConfig) context.Context {
	return context.WithValue(ctx, vadConfigKey{}, cfg)
}

// VADConfigFromContext extracts VAD configuration from the context, if present.
func VADConfigFromContext(ctx context.Context) (VADConfig, bool) {
	cfg, ok := ctx.Value(vadConfigKey{}).(VADConfig)
	return cfg, ok
}
