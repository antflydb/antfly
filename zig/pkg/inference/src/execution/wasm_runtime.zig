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

//! Interpreter-only execution on WASM, behind the same preparation contract.
const std = @import("std");
const Graph = @import("ml").graph.Graph;
const ops_mod = @import("../ops/ops.zig");
const graph_interpreter = @import("../graph/interpreter.zig");

pub const Strategy = @import("strategy.zig").Strategy;

pub fn strategyFromEnv() Strategy {
    return .interpreter;
}

pub const Runtime = struct {
    default_backend: *const ops_mod.ComputeBackend,

    pub fn init(
        _: std.mem.Allocator,
        _: *const Graph,
        compute_backend: *const ops_mod.ComputeBackend,
        requested_strategy: Strategy,
    ) !Runtime {
        if (requested_strategy != .interpreter) return error.UnsupportedCompiledGraphRuntime;
        return .{ .default_backend = compute_backend };
    }

    pub fn deinit(_: *Runtime) void {}

    pub fn execute(
        self: *Runtime,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        options: graph_interpreter.ExecuteOptions,
    ) !Result {
        const transfer_audit = @import("transfer_audit.zig");
        var audit = transfer_audit.Audit{ .forbid_device_host_access = options.activation_readbacks == .forbidden };
        const auditing = options.collect_partition_stats or audit.forbid_device_host_access;
        if (auditing) try transfer_audit.begin(&audit);
        defer if (auditing) transfer_audit.end();
        var result = try graph_interpreter.execute(allocator, graph, self.default_backend, options);
        errdefer result.deinit(self.default_backend);
        const outputs = result.outputs;
        result.outputs = &.{};
        var stats = @import("stats.zig").ExecutionStats{};
        audit.recordStats(&stats);
        return .{
            .outputs = outputs,
            .allocator = result.allocator,
            .stats = stats,
            .owner_backend = self.default_backend.*,
        };
    }
};

pub const Result = struct {
    stats: @import("stats.zig").ExecutionStats = .{},
    outputs: []ops_mod.CT,
    owner_backend: ?ops_mod.ComputeBackend = null,
    allocator: std.mem.Allocator,

    pub fn backendFor(self: *const Result, runtime: *const Runtime, _: usize) *const ops_mod.ComputeBackend {
        if (self.owner_backend) |*backend| return backend;
        return runtime.default_backend;
    }

    pub fn deinit(self: *Result, runtime: *const Runtime) void {
        for (self.outputs, 0..) |ct, idx| {
            if (containsCt(self.outputs[0..idx], ct)) continue;
            self.backendFor(runtime, idx).free(ct);
        }
        self.allocator.free(self.outputs);
        self.outputs = &.{};
    }
};

fn containsCt(values: []const ops_mod.CT, value: ops_mod.CT) bool {
    for (values) |candidate| {
        if (candidate == value) return true;
    }
    return false;
}
