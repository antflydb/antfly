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

//! Tabular IR optimiser passes. Each pass mutates a TreeEnsemble in place
//! and may set annotations consumed by the engine at load time.

const std = @import("std");
const ir = @import("../ir.zig");
pub const dead_leaf = @import("dead_leaf.zig");
pub const quantize = @import("quantize.zig");

pub const Options = struct {
    enable_dead_leaf: bool = true,
    dead_leaf_threshold_fraction: f64 = 1e-3,
    enable_quantize: bool = true,
};

pub fn optimizeEnsemble(alloc: std.mem.Allocator, te: *ir.TreeEnsemble, opts: Options) !void {
    if (opts.enable_dead_leaf) _ = dead_leaf.run(te, opts.dead_leaf_threshold_fraction);
    if (opts.enable_quantize) try quantize.run(alloc, te);
}

test {
    _ = dead_leaf;
    _ = quantize;
}
