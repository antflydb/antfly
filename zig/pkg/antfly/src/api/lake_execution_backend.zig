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

//! Root-issued backend keeps source file identity with its actual owner.
//! The build graph binds lake_execution_root to that same caller Module.
const root = @import("lake_execution_root");
pub const implementation = if (@hasDecl(root, "lake_execution_backend")) root.lake_execution_backend else root.antfly_sources.lake_execution_backend;
