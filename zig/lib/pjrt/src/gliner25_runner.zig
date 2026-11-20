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

//! Native, single-device GLiNER2.5 prepared-tensor CUDA/PJRT benchmark worker.
//! Model preparation is offline. Timed forwards use only PJRT device buffers.
const std = @import("std");
const pjrt = @import("pjrt.zig");
const A = std.heap.c_allocator;
const Tensor = struct { name: []const u8, shape: []const i64, dtype: []const u8 };
const Input = struct { name: []const u8, shape: []const i64, dtype: []const u8, data: []const i64 };
const Case = struct { id: []const u8, program: []const u8, inputs: []const Input, outputs: []const Tensor };
const Manifest = struct { family: []const u8, parameters: []const Tensor, cases: []const Case };
extern "c" fn read(c_int, [*]u8, usize) isize;
extern "c" fn write(c_int, [*]const u8, usize) isize;
fn now() u64 {
    var t: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &t);
    return @intCast(@as(i128, t.sec) * std.time.ns_per_s + t.nsec);
}
fn readAll(data: []u8) !void {
    var n: usize = 0;
    while (n < data.len) {
        const got = read(0, data[n..].ptr, data.len - n);
        if (got <= 0) return error.IncompleteInput;
        n += @intCast(got);
    }
}
fn writeAll(data: []const u8) !void {
    var n: usize = 0;
    while (n < data.len) {
        const got = write(1, data[n..].ptr, data.len - n);
        if (got <= 0) return error.OutputClosed;
        n += @intCast(got);
    }
}
fn elements(shape: []const i64) !usize {
    var n: usize = 1;
    for (shape) |d| {
        if (d <= 0) return error.InvalidShape;
        n = try std.math.mul(usize, n, @intCast(d));
    }
    if (n > 1024 * 1024 * 1024) return error.InputTooLarge;
    return n;
}
fn upload(client: *pjrt.Client, spec: Tensor) !pjrt.Buffer {
    const n = try elements(spec.shape);
    if (std.mem.eql(u8, spec.dtype, "bfloat16")) {
        const x = try A.alloc(u16, n);
        defer A.free(x);
        try readAll(std.mem.sliceAsBytes(x));
        return client.bufferFromHostBFloat16(x, spec.shape);
    }
    if (std.mem.eql(u8, spec.dtype, "float32")) {
        const x = try A.alloc(f32, n);
        defer A.free(x);
        try readAll(std.mem.sliceAsBytes(x));
        return client.bufferFromHostFloat32(x, spec.shape);
    }
    if (std.mem.eql(u8, spec.dtype, "int64")) {
        const x = try A.alloc(i64, n);
        defer A.free(x);
        try readAll(std.mem.sliceAsBytes(x));
        return client.bufferFromHostInt64(x, spec.shape);
    }
    return error.UnsupportedDtype;
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(A);
    defer A.free(args);
    if (args.len != 3 and args.len != 4) return error.ExpectedPluginManifestOptionalCache;
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[2], A, .limited(16 * 1024 * 1024));
    defer A.free(raw);
    const parsed = try std.json.parseFromSlice(Manifest, A, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const m = parsed.value;
    if (!std.mem.eql(u8, m.family, "gliner25") or m.cases.len == 0 or m.cases.len > 128) return error.InvalidManifest;
    const plugin = try A.dupeSentinel(u8, args[1], 0);
    defer A.free(plugin);
    var client = try pjrt.Client.initWithMemoryPolicy(plugin, .{ .cuda = .{ .capacity_bytes = 21_000 * 1024 * 1024, .device_bytes = 23_034 * 1024 * 1024 } });
    defer client.deinit();
    if (!std.ascii.eqlIgnoreCase(try client.platformName(), "cuda")) return error.ExpectedCuda;
    const params = try A.alloc(pjrt.Buffer, m.parameters.len);
    defer A.free(params);
    var uploaded: usize = 0;
    defer for (params[0..uploaded]) |*b| b.deinit();
    for (m.parameters, 0..) |spec, j| {
        params[j] = try upload(&client, spec);
        uploaded += 1;
        if (j % 100 == 0) std.debug.print("gliner parameter {d}/{d}\n", .{ j, params.len });
    }
    const programs = try A.alloc(pjrt.LoadedExecutable, m.cases.len);
    defer A.free(programs);
    var compiled: usize = 0;
    defer for (programs[0..compiled]) |*e| e.deinit();
    for (m.cases, 0..) |case, j| {
        const path = try std.fs.path.join(A, &.{ std.fs.path.dirname(args[2]) orelse ".", case.program });
        defer A.free(path);
        const hlo = try std.Io.Dir.cwd().readFileAlloc(init.io, path, A, .limited(256 * 1024 * 1024));
        defer A.free(hlo);
        // This optional cache belongs to one qualification campaign, on one
        // device/plugin. It must not be reused across plugin or GPU versions.
        const cache_path = if (args.len == 4) blk: {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(hlo, &hash, .{});
            const filename = try std.fmt.allocPrint(A, "{x}.pjrt", .{hash});
            defer A.free(filename);
            break :blk try std.fs.path.join(A, &.{ args[3], filename });
        } else null;
        defer if (cache_path) |cache_file| A.free(cache_file);
        const cached = if (cache_path) |cache_file|
            std.Io.Dir.cwd().readFileAlloc(init.io, cache_file, A, .limited(256 * 1024 * 1024)) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            }
        else
            null;
        defer if (cached) |bytes| A.free(bytes);
        if (cached) |bytes| {
            programs[j] = try client.deserializeExecutable(bytes, case.outputs.len);
        } else {
            programs[j] = try client.compile(hlo, case.outputs.len);
        }
        compiled += 1;
        if (cache_path != null and cached == null) {
            const serialized = try programs[j].serialize(A);
            defer A.free(serialized);
            try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = cache_path.?, .data = serialized });
        }
        std.debug.print("{s} gliner {s}\n", .{ if (cached == null) "compiled" else "deserialized", case.id });
    }
    try writeAll("READY\n");
    while (true) {
        var bytes: [4]u8 = undefined;
        try readAll(&bytes);
        const index = std.mem.readInt(u32, &bytes, .little);
        if (index == std.math.maxInt(u32)) break;
        if (index >= m.cases.len) return error.InvalidCaseIndex;
        const case = m.cases[index];
        const inputs = try A.alloc(pjrt.Buffer, params.len + case.inputs.len);
        defer A.free(inputs);
        @memcpy(inputs[0..params.len], params);
        var supplied: usize = 0;
        defer for (inputs[params.len..][0..supplied]) |*b| b.deinit();
        const started = now();
        for (case.inputs, 0..) |spec, j| {
            if (!std.mem.eql(u8, spec.dtype, "int64") or spec.data.len != try elements(spec.shape)) return error.InvalidInput;
            inputs[params.len + j] = try client.bufferFromHostInt64(spec.data, spec.shape);
            supplied += 1;
        }
        const ready = now();
        const outputs = try programs[index].execute(inputs, A);
        defer {
            for (outputs) |*b| b.deinit();
            A.free(outputs);
        }
        if (case.outputs.len != 1 or !std.mem.eql(u8, case.outputs[0].dtype, "float32")) return error.UnsupportedOutputs;
        const vector = try outputs[0].toFloat32WithShape(A, case.outputs[0].shape);
        defer A.free(vector);
        const ended = now();
        var duration: [16]u8 = undefined;
        std.mem.writeInt(u64, duration[0..8], ready - started, .little);
        std.mem.writeInt(u64, duration[8..16], ended - ready, .little);
        try writeAll(&duration);
        try writeAll(std.mem.sliceAsBytes(vector));
    }
}
