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

//! Prepared text IDs/masks through normalized vector readback, shared with HTTP.
const std = @import("std");
const internal = @import("inference_internal");
const factory = internal.architectures.session_factory;
const pipeline = internal.pipelines.embedding_gemma2;
const Bounded = internal.runtime.bounded_allocator.BoundedAllocator;

const Case = struct { name: []const u8, token_ids: []const i64, attention_mask: ?[]const i64 = null, dimensions: usize = 768 };
const Suite = struct { cases: []const Case };
const Memory = if (@import("build_options").enable_metal) internal.metal_runtime.RawRuntimeMemoryStats else struct {};
const Measurement = struct { name: []const u8, tokens: usize, samples_seconds: []f64, vector: []f32, host_peak_bytes: usize, host_live_after_release: usize, gpu_memory: []Memory, vm_measured_before: [3]u64, vm_measured_after: [3]u64 };
fn now() !u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS) return error.ClockFailed;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}

extern fn termite_metal_host_vm_counters(out: *[3]u64) c_int;
fn vmCounters() ![3]u64 {
    if (comptime !@import("build_options").enable_metal) return @splat(0);
    var result: [3]u64 = undefined;
    if (termite_metal_host_vm_counters(&result) != 0) return error.VmCountersUnavailable;
    return result;
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    var model: ?[]const u8 = null;
    var suite_path: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var metal = true;
    var warmups: usize = 2;
    var iterations: usize = 20;
    var long_iterations: usize = 5;
    var selected = std.ArrayList([]const u8).empty;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const key = args[i];
        const value = args[i + 1];
        if (std.mem.eql(u8, key, "--model")) model = value else if (std.mem.eql(u8, key, "--suite")) suite_path = value else if (std.mem.eql(u8, key, "--output")) output = value else if (std.mem.eql(u8, key, "--case")) try selected.append(a, value) else if (std.mem.eql(u8, key, "--warmups")) warmups = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--iterations")) iterations = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--long-iterations")) long_iterations = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--backend")) {
            if (!std.mem.eql(u8, value, "metal") and !std.mem.eql(u8, value, "native")) return error.InvalidBackend;
            metal = std.mem.eql(u8, value, "metal");
        } else return error.UnknownArgument;
    }
    if (iterations == 0 or iterations > 100 or long_iterations == 0 or long_iterations > 100 or warmups > 100) return error.InvalidIterations;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, suite_path orelse return error.MissingSuite, a, .limited(16 * 1024 * 1024));
    const parsed = try std.json.parseFromSlice(Suite, a, bytes, .{ .ignore_unknown_fields = true });
    for (selected.items) |name| {
        var found = false;
        for (parsed.value.cases) |case| if (std.mem.eql(u8, name, case.name)) {
            found = true;
            break;
        };
        if (!found) return error.UnknownCase;
    }
    const session = if (metal) try factory.createMetalSession(a, model orelse return error.MissingModel) else try factory.createNativeSession(a, model orelse return error.MissingModel);
    defer session.close();
    if (!metal) factory.attachIo(session, init.io);
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.WrongArchitecture;
    var measurements = std.ArrayList(Measurement).empty;
    for (parsed.value.cases) |case| {
        if (selected.items.len != 0) {
            var matched = false;
            for (selected.items) |name| if (std.mem.eql(u8, name, case.name)) {
                matched = true;
                break;
            };
            if (!matched) continue;
        }
        // Media soft tokens must be constructed by the processor, not treated
        // as text IDs by this benchmark.
        if (std.mem.eql(u8, case.name, "image") or std.mem.eql(u8, case.name, "audio") or std.mem.eql(u8, case.name, "mixed")) return error.TextOnlyBenchmark;
        const mask = case.attention_mask orelse blk: {
            const values = try a.alloc(i64, case.token_ids.len);
            @memset(values, 1);
            break :blk values;
        };
        const n = if (case.token_ids.len > 512) long_iterations else iterations;
        const samples = try a.alloc(f64, n);
        const gpu_memory = try a.alloc(Memory, warmups + n);
        var vector: []f32 = &.{};
        var peak: usize = 0;
        var vm_before: [3]u64 = @splat(0);
        var vm_after: [3]u64 = @splat(0);
        for (0..warmups + n) |iteration| {
            var bounded = Bounded{ .backing = pipeline.workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
            {
                var permit = try session.admit(.{ .batch = 1, .sequence = case.token_ids.len, .workspace_bytes = if (metal) 768 * 1024 * 1024 else 512 * 1024 * 1024, .output_bytes = 768 * 4 });
                defer permit.deinit();
                // The Python driver owns and can terminate this dedicated
                // process. Offline backend access avoids requiring an HTTP
                // worker's hard-cancellation boundary in this child.
                var cb = try factory.getComputeBackend(session, bounded.allocator());
                defer cb.deinit();
                if (iteration == warmups) vm_before = try vmCounters();
                const started = try now();
                const current = try pipeline.preparedText(&cb, bounded.allocator(), cfg, case.token_ids, mask, case.dimensions);
                defer bounded.allocator().free(current);
                const elapsed = try now() - started;
                if (iteration + 1 == warmups + n) vm_after = try vmCounters();
                if (comptime @import("build_options").enable_metal) {
                    if (metal) {
                        const compute: *internal.native_compute.metal.MetalCompute = @ptrCast(@alignCast(cb.ptr));
                        gpu_memory[iteration] = internal.metal_runtime.runtimeMemorySnapshot(compute.provider_impl.raw_decode_runtime);
                    } else gpu_memory[iteration] = .{};
                } else gpu_memory[iteration] = .{};
                if (iteration >= warmups) samples[iteration - warmups] = @as(f64, @floatFromInt(elapsed)) / std.time.ns_per_s;
                if (iteration + 1 == warmups + n) vector = try a.dupe(f32, current);
            }
            if (bounded.live != 0) return error.WorkspaceLeak;
            peak = @max(peak, bounded.peak);
        }
        try measurements.append(a, .{ .name = case.name, .tokens = case.token_ids.len, .samples_seconds = samples, .vector = vector, .host_peak_bytes = peak, .host_live_after_release = 0, .gpu_memory = gpu_memory, .vm_measured_before = vm_before, .vm_measured_after = vm_after });
        std.debug.print("encoder {s}: {d} samples, host peak {d}\n", .{ case.name, n, peak });
    }
    if (measurements.items.len == 0) return error.NoCases;
    const report = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .timing_scope = "prepared IDs/mask: embedding lookup, encoder, pooling, normalization and synchronized vector readback", .backend = if (metal) "metal" else "native", .warmups = warmups, .vm_counters_available = @import("build_options").enable_metal, .cases = measurements.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = report });
}
