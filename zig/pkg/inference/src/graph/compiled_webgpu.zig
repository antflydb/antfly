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

const std = @import("std");
const build_options = @import("build_options");
const ml = @import("ml");

const cache_mod = @import("cache.zig");
const compiled_backend = @import("compiled_backend.zig");
const multi_executor = @import("multi_executor.zig");
const partition_mod = @import("partition.zig");
const webgpu_capabilities = @import("webgpu_capabilities.zig");
const webgpu_partition_executor = @import("webgpu_partition_executor.zig");

const Graph = ml.graph.Graph;
const OpCode = ml.graph.OpCode;

fn supportsForMode(
    _: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) *const fn (op: OpCode) bool {
    return &webgpu_capabilities.supportsWebGpuGraph;
}

fn decideForMode(
    _: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) ?*const fn (query: partition_mod.CapabilityQuery) partition_mod.CapabilityDecision {
    return &webgpu_capabilities.decideWebGpuGraph;
}

fn shouldAttach(
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    if (comptime !(build_options.enable_wasm and build_options.enable_webgpu)) return false;
    if (context.requested_backend != .webgpu) return false;
    return context.cb.kind() == .wasm;
}

fn partitionHasCompute(graph: *const Graph, part: partition_mod.Partition) bool {
    for (part.node_ids) |node_id| {
        const op = graph.node(node_id).op;
        if (op != .parameter and op != .constant) return true;
    }
    return false;
}

fn isWebGpuPartitionEligible(graph: *const Graph, part: partition_mod.Partition) bool {
    return part.backend == .webgpu and partitionHasCompute(graph, part);
}

fn hasCompilablePartition(
    graph: *const Graph,
    dpp: *const multi_executor.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .webgpu, context.attachment_target)) return false;
    for (dpp.base.partitions) |part| {
        if (isWebGpuPartitionEligible(graph, part)) return true;
    }
    return false;
}

fn attachExecutors(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const Graph,
    dpp: *multi_executor.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) !void {
    if (comptime !(build_options.enable_wasm and build_options.enable_webgpu)) return;

    entry.selectCompiledPartitionsBackend(.webgpu, context.attachment_target);

    if (entry.compiled_partitions_status == .uninitialized) {
        if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .webgpu, context.attachment_target)) {
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
            return;
        }

        var compiled = std.ArrayListUnmanaged(cache_mod.CompiledPartition).empty;
        errdefer {
            for (compiled.items) |*cp| cp.executor.deinitExecutor();
            compiled.deinit(allocator);
        }

        for (dpp.base.partitions, 0..) |part, part_idx| {
            if (!isWebGpuPartitionEligible(graph, part)) continue;

            const exec = try webgpu_partition_executor.WebGpuPartitionExecutor.create(
                allocator,
                graph,
                context.cb,
            );
            var transferred = false;
            errdefer if (!transferred) exec.partitionExecutor().deinitExecutor();

            try compiled.append(allocator, .{
                .partition_idx = @intCast(part_idx),
                .executor = exec.partitionExecutor().*,
            });
            transferred = true;
        }

        if (compiled.items.len > 0) {
            entry.compiled_partitions = try compiled.toOwnedSlice(allocator);
            entry.compiled_partitions_status = .ready;
        } else {
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
        }
    }

    if (entry.compiled_partitions_status == .ready) {
        if (entry.compiled_partitions) |compiled| {
            for (compiled) |*cp| {
                dpp.base.partitions[cp.partition_idx].executor = &cp.executor;
            }
            dpp.base.owns_executors = false;
        }
    }
}

pub const backend = compiled_backend.Definition{
    .kind = .webgpu,
    .priority = 10,
    .model_runtime_strategy = .none,
    .supports_for_mode = &supportsForMode,
    .decide_for_mode = &decideForMode,
    .should_attach = &shouldAttach,
    .has_compilable_partition = &hasCompilablePartition,
    .attach_executors = &attachExecutors,
};
