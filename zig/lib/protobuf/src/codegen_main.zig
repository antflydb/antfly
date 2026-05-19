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

//! Thin wrapper that raises the executable's module root from
//! src/codegen/main.zig up to src/, so that src/codegen/main.zig and its
//! siblings can `@import("../descriptor.zig")` to reach src/descriptor.zig
//! without tripping Zig's "import of file outside module path" check.

const main_impl = @import("codegen/main.zig");
pub const main = main_impl.main;
