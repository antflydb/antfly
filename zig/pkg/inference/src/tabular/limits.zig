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

//! Shared size guards for tabular predictor ingestion.

/// The tabular JSON loader currently buffers the whole document before
/// projecting it into the typed IR, so every ingestion path uses the same
/// conservative cap. This is not a predictor engine limit.
pub const max_model_json_bytes: usize = 256 * 1024 * 1024;

/// Source artifacts that are converted into `tabular_model.json` are also
/// buffered today. Keep a separate cap so ONNX-ML/XGBoost/LightGBM pulls do not
/// inherit a misleading JSON-specific limit name.
pub const max_model_artifact_bytes: usize = 512 * 1024 * 1024;
