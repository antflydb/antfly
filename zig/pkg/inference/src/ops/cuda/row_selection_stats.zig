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

//! F32 row argmax/LSE selection statistics, fixed1024-element tiles and256
//! threads. Historical exported symbols/parameter ABI are compatibility only.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const ops = @import("../ops.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const Program = @import("embedded_module.zig").EmbeddedModule(@import("embedded_module.zig").profile(catalog.row_selection_stats_f32).image, catalog.row_selection_stats_f32_entries);
pub const Module = struct {
    program: Program = .{},
    completed: [2]?ops.WhisperLogitsStatsRaw = @splat(null),
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.program.deinitChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.* = .{};
    }
    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }
    pub fn launch(self: *Module, ctx: *context.CudaContext, logits: driver.CUdeviceptr, ids: driver.CUdeviceptr, partials: driver.CUdeviceptr, output: driver.CUdeviceptr, params: ops.WhisperLogitsParams, count: u32) !void {
        var values = logits;
        var suppress = ids;
        var scratch = partials;
        var out = output;
        var p = params;
        var grammar: driver.CUdeviceptr = 0;
        var blocks = count;
        var first = [_]?*anyopaque{ &values, &suppress, &scratch, &p, &grammar };
        try self.program.launch(ctx, .partials, count, 1, 256, &first);
        var finish = [_]?*anyopaque{ &scratch, &out, &blocks, &values, &p };
        try self.program.launch(ctx, .combine, 1, 1, 1, &finish);
    }
    pub fn publish(self: *Module, slot: usize, stats: ops.WhisperLogitsStatsRaw) void {
        std.debug.assert(slot < self.completed.len);
        self.completed[slot] = stats;
    }
    pub fn read(self: *Module, slot: usize, out: *ops.WhisperLogitsStatsRaw) bool {
        if (slot >= self.completed.len) return false;
        const value = self.completed[slot] orelse return false;
        self.completed[slot] = null;
        if (!@import("../row_selection_stats.zig").completedFiniteResult(&value)) return false;
        out.* = value;
        return true;
    }
};
