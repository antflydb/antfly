// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Wire payload and working memory are separate bounded resources: decoded
//! parameters, staged rows and storage encodings can coexist during execution.
//! Token, node and nesting limits independently bound AST structure.
pub const request_bytes: usize = 64 * 1024 * 1024;
pub const default_memory_bytes: usize = 256 * 1024 * 1024;
pub const preparation_bytes: usize = default_memory_bytes;
