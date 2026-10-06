// Copyright 2026 Antfly, Inc.
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

//! The single source of truth for the name of the full-text index every
//! Antfly table is provisioned with on creation unless the caller supplies
//! an explicit index catalog. This file intentionally has zero dependencies
//! so it can be relative-imported from compilation roots that do not share a
//! module graph (the server's `api/` tree, the C ABI surface in `capi/`, the
//! native `embedded/` package, and Antfly Lite's connection layer) without
//! pulling in the heavier `api/full_text_indexes.zig` dependency chain.
pub const default_full_text_index_name = "full_text_index_v0";
