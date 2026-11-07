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

//! Standard-library-only Tile descriptions and lowerings shared by inference and vector search.

pub const tile = @import("tile.zig");
pub const cpu = @import("cpu.zig");
pub const gpu = @import("gpu.zig");
pub const fexpr = @import("fexpr.zig");
pub const vmath = @import("vmath.zig");
pub const parallel = @import("parallel.zig");
pub const gemm = @import("gemm.zig");
pub const norm = @import("norm.zig");
pub const layout = @import("layout.zig");
pub const attention = @import("attention.zig");
pub const gpu_model = @import("gpu_model.zig");
pub const scoring = @import("scoring.zig");
pub const rows = @import("rows.zig");
pub const conformance = @import("conformance.zig");
pub const native_math = @import("native_math.zig");
pub const native_normalization = @import("native_normalization.zig");
