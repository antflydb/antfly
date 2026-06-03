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

const runtime = @import("document_preprocessing_runtime.zig");

pub const default_max_length = runtime.default_max_length;
pub const default_mean = runtime.default_mean;
pub const default_std = runtime.default_std;

pub const OcrToken = runtime.OcrToken;
pub const PreparedInputs = runtime.PreparedInputs;
pub const PreparationSummary = runtime.PreparationSummary;
pub const PreprocessorConfig = runtime.PreprocessorConfig;

pub const prepareFromFiles = runtime.prepareFromFiles;
pub const summarizePreparedInputs = runtime.summarizePreparedInputs;
pub const loadPreprocessorConfig = runtime.loadPreprocessorConfig;

test {
    _ = runtime;
}
