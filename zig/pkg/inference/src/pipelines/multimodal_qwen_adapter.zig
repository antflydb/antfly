// Copyright 2026 Antfly, Inc.
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

const qwen2vl = @import("qwen2vl_multimodal.zig");

// Preferred adapter-facing import for Qwen2-VL-style multimodal prompt/image prep.
// This keeps task-oriented modules from depending on the legacy file name directly.
pub const PreprocessorConfig = qwen2vl.PreprocessorConfig;
pub const PreparedImage = qwen2vl.PreparedImage;
pub const PromptConfig = qwen2vl.PromptConfig;
pub const PreparedTextInput = qwen2vl.PreparedTextInput;

pub const prepareQueryText = qwen2vl.prepareQueryText;
pub const prepareDocumentPrompt = qwen2vl.prepareDocumentPrompt;
pub const appendSliceInto = qwen2vl.appendSliceInto;
pub const loadPreprocessorConfig = qwen2vl.loadPreprocessorConfig;
pub const parsePreprocessorConfig = qwen2vl.parsePreprocessorConfig;
pub const prepareImage = qwen2vl.prepareImage;
pub const smartResize = qwen2vl.smartResize;

test {
    _ = qwen2vl;
}
