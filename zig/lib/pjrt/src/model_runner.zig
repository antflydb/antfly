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

//! Native, single-device CUDA runner for prepared BF16 HLO model artifacts.
//! Model preparation is offline. Timed forwards use only PJRT device buffers.
const std = @import("std");
const pjrt = @import("pjrt.zig");
const A = std.heap.c_allocator;
extern "c" fn read(fd: c_int, buffer: [*]u8, count: usize) isize;
const Tensor = struct { name: []const u8, shape: []const i64, dtype: []const u8 };
const Case = struct { id: []const u8, prompt_ids: []const i64, max_tokens: usize, prefill: []const u8, decode: ?[]const u8 = null, state_shapes: []const []const i64 = &.{}, oracle_tokens: []const i64 = &.{}, require_exact_tokens: bool = false, oracle_vector: []const f32 = &.{}, teacher_ids: []const i64 = &.{}, oracle_logits: ?[]const u8 = null };
const Manifest = struct { version: u32, family: []const u8, parameters: []const Tensor, cases: []const Case, repeats: usize = 4, warmups: usize = 1, compile_options: ?[]const u8 = null, capacity_bytes: u64 = 21_000 * 1024 * 1024, device_bytes: u64 = 23_034 * 1024 * 1024 };
const Row = struct { cell: []const u8, repeat: usize, warmup: bool, ms: f64, ttft_ms: f64, decode_tok_s: f64, tokens: []i64 = &.{}, vector: []f32 = &.{} };
fn now() u64 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}
fn elements(shape: []const i64) !usize {
    var n: usize = 1;
    for (shape) |d| n = try std.math.mul(usize, n, std.math.cast(usize, d) orelse return error.InvalidShape);
    return n;
}
fn readAll(data: []u8) !void {
    var n: usize = 0;
    while (n < data.len) {
        const count = read(0, data[n..].ptr, data.len - n);
        if (count <= 0) return error.TruncatedWeightStream;
        n += @intCast(count);
    }
}
fn upload(client: *const pjrt.Client, spec: Tensor) !pjrt.Buffer {
    const n = try elements(spec.shape);
    if (std.mem.eql(u8, spec.dtype, "bfloat16")) {
        const data = try A.alloc(u16, n);
        defer A.free(data);
        try readAll(std.mem.sliceAsBytes(data));
        return client.bufferFromHostBFloat16(data, spec.shape);
    }
    if (std.mem.eql(u8, spec.dtype, "float32")) {
        const data = try A.alloc(f32, n);
        defer A.free(data);
        try readAll(std.mem.sliceAsBytes(data));
        return client.bufferFromHostFloat32(data, spec.shape);
    }
    return error.UnsupportedWeightType;
}
fn closeAll(buffers: []pjrt.Buffer) void {
    for (buffers) |*b| b.deinit();
    A.free(buffers);
}
fn zeroState(client: *const pjrt.Client, shapes: []const []const i64) ![]pjrt.Buffer {
    const result = try A.alloc(pjrt.Buffer, shapes.len);
    var done: usize = 0;
    errdefer {
        for (result[0..done]) |*b| b.deinit();
        A.free(result);
    }
    for (shapes, 0..) |shape, i| {
        const data = try A.alloc(u16, try elements(shape));
        defer A.free(data);
        @memset(data, 0);
        result[i] = try client.bufferFromHostBFloat16(data, shape);
        done += 1;
    }
    return result;
}
fn compile(client: *const pjrt.Client, io: std.Io, base: []const u8, file: []const u8, outputs: usize, options: []const u8) !pjrt.LoadedExecutable {
    const path = try std.fs.path.join(A, &.{ base, file });
    defer A.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, A, .limited(64 * 1024 * 1024));
    defer A.free(bytes);
    return if (options.len == 0) client.compile(bytes, outputs) else client.compileWithSerializedOptions(bytes, outputs, options);
}
fn forward(exec: *const pjrt.LoadedExecutable, params: []const pjrt.Buffer, state: []const pjrt.Buffer, ids: pjrt.Buffer, position: pjrt.Buffer) ![]pjrt.Buffer {
    const inputs = try A.alloc(pjrt.Buffer, params.len + state.len + 2);
    defer A.free(inputs);
    @memcpy(inputs[0..params.len], params);
    @memcpy(inputs[params.len..][0..state.len], state);
    inputs[inputs.len - 2] = ids;
    inputs[inputs.len - 1] = position;
    return exec.execute(inputs, A);
}
fn sameTokens(expected: []const i64, actual: []const i64, require_exact: bool) !void {
    if (expected.len == 0) return;
    if (expected.len != actual.len) return error.TokenParityFailed;
    for (expected, actual, 0..) |x, y, i| if (x != y) {
        std.debug.print("reference token divergence index={d} expected={d} actual={d} first_actual={any}\n", .{ i, x, y, actual[0..@min(actual.len, 16)] });
        if (require_exact) return error.TokenParityFailed;
        return;
    };
}
const Validation = struct { step: usize, cosine: f64, max_abs: f64, expected_token: usize, actual_token: usize, relative_l2: f64, reference_gap: f64, policy_passed: bool };
fn validateCase(client: *const pjrt.Client, params: []const pjrt.Buffer, prefill: *const pjrt.LoadedExecutable, decode: *const pjrt.LoadedExecutable, case: Case, io: std.Io, base: []const u8, validations: *std.ArrayList(Validation)) !void {
    const path = try std.fs.path.join(A, &.{ base, case.oracle_logits orelse return error.MissingOracle });
    defer A.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, A, .limited(512 * 1024 * 1024));
    defer A.free(bytes);
    const width = 262144;
    if (bytes.len != (case.teacher_ids.len + 1) * width * 4) return error.InvalidOracle;
    const expected = @as([*]align(1) const f32, @ptrCast(bytes.ptr))[0 .. bytes.len / 4];
    const state = try zeroState(client, case.state_shapes);
    defer closeAll(state);
    var ids = try client.bufferFromHostInt64(case.prompt_ids, &.{ 1, @intCast(case.prompt_ids.len) });
    defer ids.deinit();
    var position = try client.bufferFromHostInt64(&.{0}, &.{});
    defer position.deinit();
    var out = try forward(prefill, params, state, ids, position);
    defer closeAll(out);
    var qualified = true;
    for (0..case.teacher_ids.len + 1) |i| {
        const actual = try out[0].toFloat32WithShape(A, &.{ 1, width });
        defer A.free(actual);
        const target = expected[i * width ..][0..width];
        var dot: f64 = 0;
        var na: f64 = 0;
        var nb: f64 = 0;
        var max_abs: f64 = 0;
        var squared_error: f64 = 0;
        var ai: usize = 0;
        var bi: usize = 0;
        for (actual, target, 0..) |x, y, j| {
            dot += @as(f64, x) * y;
            na += @as(f64, x) * x;
            nb += @as(f64, y) * y;
            max_abs = @max(max_abs, @abs(@as(f64, x) - y));
            squared_error += (@as(f64, x) - y) * (@as(f64, x) - y);
            if (x > actual[ai]) ai = j;
            if (y > target[bi]) bi = j;
        }
        const cosine = dot / @sqrt(na * nb);
        const relative_l2 = @sqrt(squared_error / nb);
        const reference_gap = @as(f64, target[bi]) - target[ai];
        const bf16_ulp = std.math.pow(f64, 2, @floor(@log2(@max(@abs(@as(f64, target[bi])), 1))) - 7);
        const passed = std.math.isFinite(cosine) and std.math.isFinite(relative_l2) and cosine >= 0.9999 and relative_l2 <= 0.02 and reference_gap <= 2 * bf16_ulp;
        try validations.append(A, .{ .step = i, .cosine = cosine, .max_abs = max_abs, .expected_token = bi, .actual_token = ai, .relative_l2 = relative_l2, .reference_gap = reference_gap, .policy_passed = passed });
        qualified = qualified and passed;
        if (i % 32 == 0 or ai != bi) std.debug.print("logit validation step={d} cosine={d} max_abs={d} expected={d} actual={d}\n", .{ i, cosine, max_abs, bi, ai });
        if (i < case.teacher_ids.len) {
            var token = try client.bufferFromHostInt64(case.teacher_ids[i..][0..1], &.{ 1, 1 });
            defer token.deinit();
            const next = try forward(decode, params, out[1 .. out.len - 1], token, out[out.len - 1]);
            closeAll(out);
            out = next;
        }
    }
    // Keep all numerical evidence, including failed checks, available to the
    // qualification driver rather than dropping the report on an early exit.
    std.debug.print("numerical_policy_passed={}\n", .{qualified});
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(A);
    defer A.free(args);
    if (args.len != 4) {
        std.debug.print("usage: pjrt-cuda-model <plugin.so> <model.json> <results.json> < weights.bf16-stream\n", .{});
        return error.InvalidArguments;
    }
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, args[2], A, .limited(8 * 1024 * 1024));
    defer A.free(bytes);
    const doc = try std.json.parseFromSlice(Manifest, A, bytes, .{ .ignore_unknown_fields = true });
    defer doc.deinit();
    const m = doc.value;
    if (m.version != 1 or m.repeats == 0 or m.repeats > 100 or m.warmups > 20 or m.cases.len == 0) return error.InvalidManifest;
    if (!std.mem.eql(u8, m.family, "gemma4") and !std.mem.eql(u8, m.family, "gemma4-validation") and !std.mem.eql(u8, m.family, "embeddinggemma2")) return error.UnsupportedFamily;
    const plugin = try A.dupeSentinel(u8, args[1], 0);
    defer A.free(plugin);
    const started = now();
    var client = try pjrt.Client.initWithMemoryPolicy(plugin, .{ .cuda = .{ .capacity_bytes = m.capacity_bytes, .device_bytes = m.device_bytes } });
    defer client.deinit();
    if (!std.ascii.eqlIgnoreCase(try client.platformName(), "cuda")) return error.ExpectedCudaDevice;
    std.debug.print("PJRT native CUDA client ready\n", .{});
    const params = try A.alloc(pjrt.Buffer, m.parameters.len);
    var uploaded: usize = 0;
    defer {
        for (params[0..uploaded]) |*b| b.deinit();
        A.free(params);
    }
    var weight_bytes: usize = 0;
    for (m.parameters, 0..) |spec, i| {
        params[i] = try upload(&client, spec);
        uploaded += 1;
        weight_bytes += try params[i].onDeviceSizeInBytes();
        if (i % 50 == 0) std.debug.print("resident parameter {d}/{d}: {s}\n", .{ i, params.len, spec.name });
    }
    var end: [1]u8 = undefined;
    if (read(0, &end, 1) != 0) return error.TrailingWeightStream;
    const load_ms = @as(f64, @floatFromInt(now() - started)) / 1e6;
    std.debug.print("resident weights ready bytes={d} load_ms={d}\n", .{ weight_bytes, load_ms });
    const base = std.fs.path.dirname(args[2]) orelse ".";
    var rows: std.ArrayList(Row) = .empty;
    defer {
        for (rows.items) |r| {
            A.free(r.tokens);
            A.free(r.vector);
        }
        rows.deinit(A);
    }
    var validations: std.ArrayList(Validation) = .empty;
    defer validations.deinit(A);
    const options_path = if (m.compile_options) |path| try std.fs.path.join(A, &.{ base, path }) else null;
    defer if (options_path) |path| A.free(path);
    const options = if (options_path) |path| try std.Io.Dir.cwd().readFileAlloc(init.io, path, A, .limited(1024 * 1024)) else try A.alloc(u8, 0);
    defer A.free(options);
    var compilation_ms: f64 = 0;
    for (m.cases) |case| {
        const validation_mode = std.mem.eql(u8, m.family, "gemma4-validation");
        const generative = std.mem.eql(u8, m.family, "gemma4") or validation_mode;
        if (case.prompt_ids.len == 0 or case.max_tokens == 0 or case.max_tokens > 8192) return error.InvalidCase;
        const outputs: usize = if (generative) case.state_shapes.len + 2 else 1;
        const cs = now();
        var prefill = try compile(&client, init.io, base, case.prefill, outputs, options);
        defer prefill.deinit();
        var decode: ?pjrt.LoadedExecutable = if (case.decode) |file| try compile(&client, init.io, base, file, outputs, options) else null;
        defer if (decode) |*exe| exe.deinit();
        if (generative and decode == null) return error.MissingDecode;
        compilation_ms += @as(f64, @floatFromInt(now() - cs)) / 1e6;
        std.debug.print("compiled {s}\n", .{case.id});
        var ids = try client.bufferFromHostInt64(case.prompt_ids, &.{ 1, @intCast(case.prompt_ids.len) });
        defer ids.deinit();
        if (validation_mode) {
            try validateCase(&client, params, &prefill, &decode.?, case, init.io, base, &validations);
            continue;
        }
        const mask = if (!generative) try A.alloc(i64, case.prompt_ids.len) else try A.alloc(i64, 0);
        defer A.free(mask);
        @memset(mask, 1);
        var position = if (generative) try client.bufferFromHostInt64(&.{0}, &.{}) else try client.bufferFromHostInt64(mask, &.{ 1, @intCast(mask.len) });
        defer position.deinit();
        var profile: ?std.DynLib = null;
        defer stopProfile(&profile);
        for (0..m.warmups + m.repeats) |rep| {
            if (rep == m.warmups) profile = try startProfile();
            const state = try zeroState(&client, case.state_shapes);
            defer closeAll(state);
            const t = now();
            var out = try forward(&prefill, params, state, ids, position);
            defer closeAll(out);
            const first = now();
            const token_ids = try A.alloc(i64, if (generative) case.max_tokens else 0);
            errdefer A.free(token_ids);
            var vector: []f32 = &.{};
            errdefer A.free(vector);
            if (generative) {
                const first_token = try out[0].toInt64WithShape(A, &.{ 1, 1 });
                token_ids[0] = first_token[0];
                A.free(first_token);
                for (1..case.max_tokens) |step| {
                    const next = try forward(&decode.?, params, out[1 .. out.len - 1], out[0], out[out.len - 1]);
                    closeAll(out);
                    out = next;
                    const token = try out[0].toInt64WithShape(A, &.{ 1, 1 });
                    token_ids[step] = token[0];
                    A.free(token);
                }
                try sameTokens(case.oracle_tokens, token_ids, case.require_exact_tokens);
            } else {
                vector = try out[0].toFloat32WithShape(A, &.{ 1, 768 });
                var norm: f64 = 0;
                for (vector) |x| {
                    if (!std.math.isFinite(x)) return error.NonFiniteVector;
                    norm += @as(f64, x) * x;
                }
                if (@abs(norm - 1) > 0.001) return error.UnnormalizedVector;
                if (case.oracle_vector.len != 0) {
                    if (case.oracle_vector.len != vector.len) return error.VectorShapeMismatch;
                    var dot: f64 = 0;
                    var expected_norm: f64 = 0;
                    for (vector, case.oracle_vector) |x, y| {
                        dot += @as(f64, x) * y;
                        expected_norm += @as(f64, y) * y;
                    }
                    if (!std.math.isFinite(dot) or !std.math.isFinite(expected_norm) or expected_norm <= 0 or dot / @sqrt(norm * expected_norm) < 0.9999) return error.VectorParityFailed;
                }
            }
            const stop = now();
            const ms = @as(f64, @floatFromInt(stop - t)) / 1e6;
            const ttft = @as(f64, @floatFromInt(first - t)) / 1e6;
            try rows.append(A, .{ .cell = case.id, .repeat = rep, .warmup = rep < m.warmups, .ms = ms, .ttft_ms = ttft, .decode_tok_s = if (generative and case.max_tokens > 1) @as(f64, @floatFromInt(case.max_tokens - 1)) / (@as(f64, @floatFromInt(stop - first)) / 1e9) else 0, .tokens = token_ids, .vector = vector });
            std.debug.print("native-pjrt {s} repeat={d} ms={d} ttft_ms={d}\n", .{ case.id, rep, ms, ttft });
        }
    }
    const numerical_policy_passed: ?bool = if (validations.items.len == 0) null else blk: {
        for (validations.items) |v| if (!v.policy_passed) break :blk false;
        break :blk true;
    };
    const report = try std.json.Stringify.valueAlloc(A, .{ .backend = "Antfly native PJRT CUDA", .precision = "bf16", .family = m.family, .weight_bytes = weight_bytes, .load_ms = load_ms, .compilation_ms = compilation_ms, .rows = rows.items, .validations = validations.items, .numerical_policy_passed = numerical_policy_passed }, .{ .whitespace = .indent_2 });
    defer A.free(report);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[3], .data = report });
    if (numerical_policy_passed == false) return error.LogitParityFailed;
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
