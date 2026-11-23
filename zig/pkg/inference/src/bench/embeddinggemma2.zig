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

//! Text and ordered media through normalized vector readback, shared with HTTP.
const std = @import("std");
const internal = @import("inference_internal");
const factory = internal.architectures.session_factory;
const pipeline = internal.pipelines.embedding_gemma2;
const Bounded = internal.runtime.bounded_allocator.BoundedAllocator;

const Backend = enum { native, metal, cuda, pjrt };
const Session = internal.backends.Session;
const Tensor = internal.backends.Tensor;

const Content = struct { text: ?[]const u8 = null, image: ?[]const u8 = null, audio: ?[]const u8 = null, video: ?[]const u8 = null, sha256: ?[]const u8 = null };
const Case = struct { name: []const u8, token_ids: []const i64 = &.{}, attention_mask: ?[]const i64 = null, dimensions: usize = 768, content: []const Content = &.{} };
const Suite = struct { cases: []const Case };
const Memory = if (@import("build_options").enable_metal) internal.metal_runtime.RawRuntimeMemoryStats else struct {};
const Measurement = struct { backend: []const u8, name: []const u8, tokens: usize, timing_scope: []const u8, samples_seconds: []f64, vector: []f32, host_peak_bytes: usize, host_live_after_release: usize, gpu_memory: []Memory, vm_measured_before: [3]u64, vm_measured_after: [3]u64 };
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

fn positivePolicyBytes(value: []const u8) !usize {
    const bytes = try std.fmt.parseInt(usize, value, 10);
    return if (bytes == 0) error.InvalidPjrtMemoryPolicy else bytes;
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    var model: ?[]const u8 = null;
    var suite_path: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var backend: Backend = .metal;
    var pjrt_precision: internal.architectures.session_factory.PjrtSessionOptions.Precision = .f32;
    var cuda_f32_reduction = false;
    var compare_pjrt = false;
    var pjrt_capacity: usize = 0;
    var pjrt_device_bytes: usize = 0;
    var warmups: usize = 2;
    var iterations: usize = 20;
    var long_iterations: usize = 5;
    var selected = std.ArrayList([]const u8).empty;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const key = args[i];
        const value = args[i + 1];
        if (std.mem.eql(u8, key, "--compare-pjrt")) {
            compare_pjrt = std.mem.eql(u8, value, "true");
            if (!compare_pjrt and !std.mem.eql(u8, value, "false")) return error.InvalidComparison;
        } else if (std.mem.eql(u8, key, "--cuda-f32-reduction")) {
            if (std.mem.eql(u8, value, "true")) cuda_f32_reduction = true else if (std.mem.eql(u8, value, "false")) cuda_f32_reduction = false else return error.InvalidCudaReduction;
        } else if (std.mem.eql(u8, key, "--pjrt-precision")) pjrt_precision = std.meta.stringToEnum(@TypeOf(pjrt_precision), value) orelse return error.InvalidPjrtPrecision else if (std.mem.eql(u8, key, "--pjrt-capacity-bytes")) pjrt_capacity = try positivePolicyBytes(value) else if (std.mem.eql(u8, key, "--pjrt-device-bytes")) pjrt_device_bytes = try positivePolicyBytes(value) else if (std.mem.eql(u8, key, "--model")) model = value else if (std.mem.eql(u8, key, "--suite")) suite_path = value else if (std.mem.eql(u8, key, "--output")) output = value else if (std.mem.eql(u8, key, "--case")) try selected.append(a, value) else if (std.mem.eql(u8, key, "--warmups")) warmups = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--iterations")) iterations = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--long-iterations")) long_iterations = try std.fmt.parseInt(usize, value, 10) else if (std.mem.eql(u8, key, "--backend")) {
            backend = std.meta.stringToEnum(Backend, value) orelse return error.InvalidBackend;
        } else return error.UnknownArgument;
    }
    if (cuda_f32_reduction and backend != .cuda) return error.InvalidCudaReduction;
    if (backend != .pjrt and !compare_pjrt and pjrt_precision != .f32) return error.InvalidPjrtPrecision;
    if ((pjrt_capacity == 0) != (pjrt_device_bytes == 0) or (backend != .pjrt and !compare_pjrt and pjrt_capacity != 0)) return error.InvalidPjrtMemoryPolicy;
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
    if (compare_pjrt and (backend == .pjrt or warmups == 0 or iterations == 0)) return error.InvalidComparison;
    const metal = backend == .metal;
    const session = switch (backend) {
        .metal => if (comptime @import("build_options").enable_metal) try factory.createMetalSession(a, model orelse return error.MissingModel) else return error.MetalNotEnabled,
        .native => try factory.createNativeSession(a, model orelse return error.MissingModel),
        .cuda => if (comptime @import("build_options").enable_cuda) try factory.createCudaSession(std.heap.c_allocator, model orelse return error.MissingModel) else return error.CudaNotEnabled,
        // Frames retain this allocator, so request temporaries must be reusable.
        .pjrt => try factory.createPjrtSessionWithOptions(std.heap.c_allocator, model orelse return error.MissingModel, .{ .precision = pjrt_precision, .memory_policy = if (pjrt_capacity != 0) .{ .cuda = .{ .capacity_bytes = pjrt_capacity, .device_bytes = pjrt_device_bytes } } else null }),
    };
    defer session.close();
    // Offline comparison: keep BF16 storage/vendor geometry, but use the same
    // F32 partial-reduction contract as PJRT BF16. Report the selected profile.
    if (cuda_f32_reduction) {
        if (comptime @import("build_options").enable_cuda) {
            var cb = try factory.getComputeBackend(session, a);
            defer cb.deinit();
            const compute: *internal.native_compute.cuda.CudaCompute = @ptrCast(@alignCast(cb.ptr));
            if (compute.bf16_blas == null or compute.embedding_gemma2_policy.matmul_arithmetic != .bf16_reduced) return error.CudaReductionUnavailable;
            compute.embedding_gemma2_policy.matmul_arithmetic = .bf16_f32_accumulate;
        } else return error.CudaNotEnabled;
    }
    if (!metal) factory.attachIo(session, init.io);
    const comparison = if (compare_pjrt) try factory.createPjrtSessionWithOptions(std.heap.c_allocator, model.?, .{ .precision = pjrt_precision, .memory_policy = if (pjrt_capacity != 0) .{ .cuda = .{ .capacity_bytes = pjrt_capacity, .device_bytes = pjrt_device_bytes } } else null }) else null;
    defer if (comparison) |value| value.close();
    if (comparison) |value| factory.attachIo(value, init.io);
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.WrongArchitecture;
    const has_content = for (parsed.value.cases) |case| {
        if (case.content.len != 0) break true;
    } else false;
    const tokenizer = if (has_content) try internal.server.model_manager.loadHuggingFaceTokenizerFromDir(a, model.?) else null;
    defer if (tokenizer) |value| value.deinitSelf();
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
        // Encoded fixture I/O is outside timing; decode, preprocessing, media
        // towers, composition and the embedding encoder use the public path.
        if (case.content.len != 0 and (case.token_ids.len != 0 or case.attention_mask != null)) return error.AmbiguousBenchmarkInput;
        if (case.content.len == 0 and case.token_ids.len == 0) return error.MissingBenchmarkInput;
        if (case.content.len == 0) for (case.token_ids) |id| {
            if (id == 258880 or id == 258881 or id == 258884) return error.MissingBenchmarkMedia;
        };
        const parts = try a.alloc(pipeline.Part, case.content.len);
        for (case.content, parts) |part, *dest| {
            if (@as(usize, @intFromBool(part.text != null)) + @intFromBool(part.image != null) + @intFromBool(part.audio != null) + @intFromBool(part.video != null) != 1) return error.InvalidBenchmarkContent;
            if (part.text) |value| {
                if (part.sha256 != null) return error.InvalidFixtureHash;
                dest.* = .{ .text = value };
            } else {
                const path = try std.fs.path.resolve(a, &.{ std.fs.path.dirname(suite_path.?) orelse ".", part.image orelse part.audio orelse part.video.? });
                const media_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024));
                if (part.sha256) |expected| {
                    var digest: [32]u8 = undefined;
                    if (expected.len != 64) return error.InvalidFixtureHash;
                    _ = std.fmt.hexToBytes(&digest, expected) catch return error.InvalidFixtureHash;
                    var actual: [32]u8 = undefined;
                    std.crypto.hash.sha2.Sha256.hash(media_bytes, &actual, .{});
                    if (!std.mem.eql(u8, &digest, &actual)) return error.FixtureChanged;
                }
                dest.* = if (part.image != null) .{ .image = media_bytes } else if (part.audio != null) .{ .audio = .{ .bytes = media_bytes } } else .{ .video = media_bytes };
            }
        }
        const mask = case.attention_mask orelse blk: {
            const values = try a.alloc(i64, case.token_ids.len);
            @memset(values, 1);
            break :blk values;
        };
        const n = if (case.token_ids.len > 512) long_iterations else iterations;
        const State = struct {
            session: Session,
            backend: Backend,
            samples: []f64,
            gpu_memory: []Memory,
            vector: []f32 = &.{},
            peak: usize = 0,
            vm_before: [3]u64 = @splat(0),
            vm_after: [3]u64 = @splat(0),
            input_tokens: usize,
        };
        var states: [2]State = undefined;
        const count: usize = if (comparison != null) 2 else 1;
        for (states[0..count], 0..) |*state, index| state.* = .{
            .session = if (index == 0) session else comparison.?,
            .backend = if (index == 0) backend else .pjrt,
            .samples = try a.alloc(f64, n),
            .gpu_memory = try a.alloc(Memory, warmups + n),
            .input_tokens = case.token_ids.len,
        };
        var profile: ?std.DynLib = null;
        defer stopProfile(&profile);
        // Both sessions finish compilation/warmup before measurements. Reverse
        // each pair to produce AB/BA controls inside the same process/window.
        for (0..warmups + n) |iteration| for (0..count) |slot| {
            if (backend == .cuda and iteration == warmups and slot == 0) profile = try startProfile();
            const index = if (iteration % 2 == 0) slot else count - 1 - slot;
            const state = &states[index];
            var bounded = Bounded{ .backing = pipeline.workspaceBackingAllocator(state.session.backend()), .limit = 512 * 1024 * 1024 };
            {
                if (iteration == warmups) state.vm_before = try vmCounters();
                const measured = if (parts.len == 0) try measureIteration(state.session, bounded.allocator(), cfg, case, mask, state.backend) else media: {
                    const started = try now();
                    const result = try pipeline.embed(bounded.allocator(), state.session, tokenizer.?.tokenizer(), .{ .content = parts }, .{ .dimensions = case.dimensions }, null, .{ .io = init.io, .native_call_policy = .in_process });
                    state.input_tokens = result.input_tokens;
                    break :media Iteration{ .vector = result.vector, .elapsed = try now() - started };
                };
                defer bounded.allocator().free(measured.vector);
                if (iteration + 1 == warmups + n) state.vm_after = try vmCounters();
                state.gpu_memory[iteration] = measured.memory;
                if (iteration >= warmups) state.samples[iteration - warmups] = @as(f64, @floatFromInt(measured.elapsed)) / std.time.ns_per_s;
                if (iteration + 1 == warmups + n) state.vector = try a.dupe(f32, measured.vector);
            }
            if (bounded.live != 0) return error.WorkspaceLeak;
            state.peak = @max(state.peak, bounded.peak);
        };
        stopProfile(&profile);
        for (states[0..count]) |state| {
            try measurements.append(a, .{ .backend = @tagName(state.backend), .name = case.name, .tokens = state.input_tokens, .timing_scope = if (parts.len == 0) "prepared text IDs through synchronized vector readback" else "encoded media bytes and text through decode, preprocessing, towers, composition, encoder and synchronized vector readback; fixture I/O excluded", .samples_seconds = state.samples, .vector = state.vector, .host_peak_bytes = state.peak, .host_live_after_release = 0, .gpu_memory = state.gpu_memory, .vm_measured_before = state.vm_before, .vm_measured_after = state.vm_after });
            std.debug.print("encoder {s}/{s}: {d} samples, host peak {d}\n", .{ @tagName(state.backend), case.name, n, state.peak });
        }
    }
    if (measurements.items.len == 0) return error.NoCases;
    const report = try std.json.Stringify.valueAlloc(a, .{ .version = 3, .comparison_order = if (compare_pjrt) "paired AB/BA after both sessions warm up" else "single backend", .timing_scope = "see each case; media and prepared-text scopes differ", .backend = @tagName(backend), .memory_scope = "benchmark request/output allocator; media pipeline internal workspace and provider heaps excluded", .call_includes_public_session = backend == .pjrt or backend == .cuda, .compiled_call_includes_public_session = backend == .pjrt, .cuda_policy = factory.getEmbeddingGemma2CudaPolicy(session), .pjrt_precision = @tagName(pjrt_precision), .pjrt_pool_cap_bytes = pjrt_capacity, .pjrt_device_bytes = pjrt_device_bytes, .warmups = warmups, .vm_counters_available = @import("build_options").enable_metal, .cases = measurements.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = report });
}

const Iteration = struct { vector: []f32, elapsed: u64, memory: Memory = .{} };
fn measureIteration(session: Session, allocator: std.mem.Allocator, cfg: anytype, case: Case, mask: []const i64, backend: Backend) !Iteration {
    if (backend == .pjrt or backend == .cuda) {
        const started = try now();
        const vector = try sessionVector(session, allocator, case, mask);
        return .{ .vector = vector, .elapsed = try now() - started };
    }
    var permit = try session.admit(.{ .batch = 1, .sequence = case.token_ids.len, .workspace_bytes = if (backend == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024, .output_bytes = 768 * 4 });
    defer permit.deinit();
    var cb = try factory.getComputeBackend(session, allocator);
    defer cb.deinit();
    const started = try now();
    const vector = try pipeline.preparedText(&cb, allocator, cfg, case.token_ids, mask, case.dimensions);
    errdefer allocator.free(vector);
    const elapsed = try now() - started;
    var memory: Memory = .{};
    if (comptime @import("build_options").enable_metal) if (backend == .metal) {
        const compute: *internal.native_compute.metal.MetalCompute = @ptrCast(@alignCast(cb.ptr));
        memory = internal.metal_runtime.runtimeMemorySnapshot(compute.provider_impl.raw_decode_runtime);
    };
    return .{ .vector = vector, .elapsed = elapsed, .memory = memory };
}
fn sessionVector(session: Session, allocator: std.mem.Allocator, case: Case, mask: []const i64) ![]f32 {
    const shape = [_]i64{ 1, @intCast(case.token_ids.len) };
    const dimensions = [_]i64{@intCast(case.dimensions)};
    const inputs = [_]Tensor{
        .{ .data = @constCast(std.mem.sliceAsBytes(case.token_ids)), .dtype = .i64, .shape = &shape, .name = "input_ids", .allocator = allocator, .owns_data = false, .owns_shape = false },
        .{ .data = @constCast(std.mem.sliceAsBytes(mask)), .dtype = .i64, .shape = &shape, .name = "attention_mask", .allocator = allocator, .owns_data = false, .owns_shape = false },
        .{ .data = @constCast(std.mem.sliceAsBytes(&dimensions)), .dtype = .i64, .shape = &.{1}, .name = "output_dimensions", .allocator = allocator, .owns_data = false, .owns_shape = false },
    };
    const outputs = try session.runWithControl(&inputs, allocator, .{ .native_call_policy = .in_process });
    defer allocator.free(outputs);
    defer for (outputs) |*output| output.deinit();
    if (outputs.len != 1 or outputs[0].dtype != .f32 or !outputs[0].owns_data or !std.mem.eql(i64, outputs[0].shape, &.{ 1, @intCast(case.dimensions) })) return error.InvalidSessionEmbeddingOutput;
    const vector = outputs[0].asFloat32Mut();
    outputs[0].owns_data = false;
    return vector;
}

// Benchmark-only capture: loading, autotuning and warmups stay outside traces.
extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
fn startProfile() !?std.DynLib {
    const enabled = getenv("ANTFLY_CUDA_PROFILE") orelse return null;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return null;
    var lib = try std.DynLib.open("libcuda.so.1");
    errdefer lib.close();
    const start = lib.lookup(*const fn () callconv(.c) c_int, "cuProfilerStart") orelse return error.ProfilerUnavailable;
    if (start() != 0) return error.ProfilerStartFailed;
    return lib;
}
fn stopProfile(profile: *?std.DynLib) void {
    if (profile.*) |*lib| {
        const stop = lib.lookup(*const fn () callconv(.c) c_int, "cuProfilerStop") orelse unreachable;
        _ = stop();
        lib.close();
        profile.* = null;
    }
}
