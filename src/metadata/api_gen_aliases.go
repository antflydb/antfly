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

package metadata

import "github.com/antflydb/antfly/lib/ai"

// Type aliases for types that are referenced locally in the generated code
// but are actually defined in lib/ai. This happens when redocly joins specs
// and inlines schemas that reference other schemas in the same external file.

type FilterSpec = ai.FilterSpec
type ChatMessage = ai.ChatMessage
