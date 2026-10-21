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

//! Native JavaScript/TypeScript grammar and AST, pinned in this monorepo.
pub const ast = @import("vendor/yuku/parser/ast.zig");
pub const parse = @import("vendor/yuku/parser/parser.zig").parse;

/// Complete pinned Python/Go/Rust grammars executed by the native GLR runtime.
pub const grammar = @import("grammar.zig");
pub const python = &@import("grammars/python.zig").grammar;
pub const go = &@import("grammars/go.zig").grammar;
pub const rust = &@import("grammars/rust.zig").grammar;
