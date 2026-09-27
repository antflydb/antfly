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

//! The single source of truth for the name of the full-text index every
//! Antfly table is provisioned with on creation unless the caller supplies
//! an explicit index catalog. This file intentionally has zero dependencies
//! so it can be relative-imported from compilation roots that do not share a
//! module graph (the server's `api/` tree, the C ABI surface in `capi/`, the
//! native `embedded/` package, and Antfly Lite's connection layer) without
//! pulling in the heavier `api/full_text_indexes.zig` dependency chain.
pub const default_full_text_index_name = "full_text_index_v0";
