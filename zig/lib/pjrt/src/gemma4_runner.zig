// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Prepared Gemma 4 CUDA/PJRT worker. Python exports graphs and preprocesses
//! media; all timed model operations execute through this native PJRT C API.
const std = @import("std");
const pjrt = @import("pjrt.zig");
const a = std.heap.c_allocator;
extern "c" fn read(c_int, [*]u8, usize) isize;
extern "c" fn write(c_int, [*]const u8, usize) isize;
const Tensor = struct { name: []const u8, shape: []const i64, dtype: []const u8 };
const Case = struct { id: []const u8, inputs: []const Tensor, prefill: []const u8, decode: []const u8, state_shapes: []const []const i64, max_tokens: usize, logits: ?[]const u8 = null };
const Manifest = struct { version: u32, family: []const u8, parameters: []const Tensor, cases: []const Case, capacity_bytes: u64 = 21_000 * 1024 * 1024, device_bytes: u64 = 23_034 * 1024 * 1024 };
fn now() u64 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}
fn elements(shape: []const i64) !usize {
    var n: usize = 1;
    for (shape) |d| {
        if (d <= 0) return error.InvalidShape;
        n = try std.math.mul(usize, n, @intCast(d));
    }
    return n;
}
fn readAll(bytes: []u8) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = read(0, bytes[offset..].ptr, bytes.len - offset);
        if (n <= 0) return error.TruncatedInput;
        offset += @intCast(n);
    }
}
fn writeAll(bytes: []const u8) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = write(1, bytes[offset..].ptr, bytes.len - offset);
        if (n <= 0) return error.ClosedOutput;
        offset += @intCast(n);
    }
}
fn upload(client: *const pjrt.Client, spec: Tensor) !pjrt.Buffer {
    const n = try elements(spec.shape);
    if (std.mem.eql(u8, spec.dtype, "bfloat16")) {
        const data = try a.alloc(u16, n);
        defer a.free(data);
        try readAll(std.mem.sliceAsBytes(data));
        return client.bufferFromHostBFloat16(data, spec.shape);
    }
    if (std.mem.eql(u8, spec.dtype, "float32")) {
        const data = try a.alloc(f32, n);
        defer a.free(data);
        try readAll(std.mem.sliceAsBytes(data));
        return client.bufferFromHostFloat32(data, spec.shape);
    }
    if (std.mem.eql(u8, spec.dtype, "int64")) {
        const data = try a.alloc(i64, n);
        defer a.free(data);
        try readAll(std.mem.sliceAsBytes(data));
        return client.bufferFromHostInt64(data, spec.shape);
    }
    return error.UnsupportedDtype;
}
fn closeAll(buffers: []pjrt.Buffer) void {
    for (buffers) |*b| b.deinit();
    a.free(buffers);
}
fn zeroState(client: *const pjrt.Client, shapes: []const []const i64) ![]pjrt.Buffer {
    const buffers = try a.alloc(pjrt.Buffer, shapes.len);
    var done: usize = 0;
    errdefer {
        for (buffers[0..done]) |*b| b.deinit();
        a.free(buffers);
    }
    for (shapes, 0..) |shape, i| {
        const zeros = try a.alloc(u16, try elements(shape));
        defer a.free(zeros);
        @memset(zeros, 0);
        buffers[i] = try client.bufferFromHostBFloat16(zeros, shape);
        done += 1;
    }
    return buffers;
}
fn execute(exe: *const pjrt.LoadedExecutable, params: []const pjrt.Buffer, state: []const pjrt.Buffer, inputs: []const pjrt.Buffer, position: pjrt.Buffer) ![]pjrt.Buffer {
    const args = try a.alloc(pjrt.Buffer, params.len + state.len + inputs.len + 1);
    defer a.free(args);
    @memcpy(args[0..params.len], params);
    @memcpy(args[params.len..][0..state.len], state);
    @memcpy(args[params.len + state.len ..][0..inputs.len], inputs);
    args[args.len - 1] = position;
    return exe.execute(args, a);
}
fn program(client: *const pjrt.Client, io: std.Io, base: []const u8, name: []const u8, cache_dir: []const u8, outputs: usize) !pjrt.LoadedExecutable {
    const file = try std.fs.path.join(a, &.{ base, name });
    defer a.free(file);
    const hlo = try std.Io.Dir.cwd().readFileAlloc(io, file, a, .limited(64 * 1024 * 1024));
    defer a.free(hlo);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(hlo, &hash, .{});
    const encoded = std.fmt.bytesToHex(hash, .lower);
    const cache_name = try std.fmt.allocPrint(a, "{s}.pjrt", .{encoded});
    defer a.free(cache_name);
    const cache_path = try std.fs.path.join(a, &.{ cache_dir, cache_name });
    defer a.free(cache_path);
    const cached = std.Io.Dir.cwd().readFileAlloc(io, cache_path, a, .limited(512 * 1024 * 1024)) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (cached) |bytes| a.free(bytes);
    if (cached) |bytes| return client.deserializeExecutable(bytes, outputs);
    var compiled = try client.compile(hlo, outputs);
    errdefer compiled.deinit();
    const bytes = try compiled.serialize(a);
    defer a.free(bytes);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = cache_path, .data = bytes });
    return compiled;
}
const Programs = struct {
    prefill: pjrt.LoadedExecutable,
    decode: pjrt.LoadedExecutable,
    logits: ?pjrt.LoadedExecutable = null,
    fn deinit(self: *@This()) void {
        self.prefill.deinit();
        self.decode.deinit();
        if (self.logits) |*exe| exe.deinit();
    }
};
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 4) return error.ExpectedPluginManifestBoundCacheDirectory;
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, args[2], a, .limited(16 * 1024 * 1024));
    defer a.free(raw);
    const doc = try std.json.parseFromSlice(Manifest, a, raw, .{ .ignore_unknown_fields = true });
    defer doc.deinit();
    const m = doc.value;
    if (m.version != 2 or !std.mem.eql(u8, m.family, "gemma4") or m.cases.len == 0 or m.cases.len > 128 or m.capacity_bytes > m.device_bytes) return error.InvalidManifest;
    for (m.cases) |case| {
        if (case.max_tokens == 0 or case.max_tokens > 8192 or case.inputs.len == 0 or case.inputs.len > 8 or case.state_shapes.len != 48) return error.InvalidCase;
        const ids = case.inputs[0];
        if (!std.mem.eql(u8, ids.name, "input_ids") or !std.mem.eql(u8, ids.dtype, "int64") or ids.shape.len != 2 or ids.shape[0] != 1 or ids.shape[1] <= 0 or ids.shape[1] > 8192 - @as(i64, @intCast(case.max_tokens))) return error.InvalidPrompt;
        for (case.state_shapes) |shape| if (shape.len != 4 or shape[0] != 1 or shape[1] != 2 or shape[2] <= 0 or shape[2] > 8192 or (shape[3] != 256 and shape[3] != 512)) return error.InvalidCache;
    }
    const plugin = try a.dupeSentinel(u8, args[1], 0);
    defer a.free(plugin);
    var client = try pjrt.Client.initWithMemoryPolicy(plugin, .{ .cuda = .{ .capacity_bytes = m.capacity_bytes, .device_bytes = m.device_bytes } });
    defer client.deinit();
    if (!std.ascii.eqlIgnoreCase(try client.platformName(), "cuda")) return error.ExpectedCuda;
    const params = try a.alloc(pjrt.Buffer, m.parameters.len);
    var resident: usize = 0;
    defer {
        for (params[0..resident]) |*b| b.deinit();
        a.free(params);
    }
    var weight_bytes: usize = 0;
    for (m.parameters, 0..) |spec, i| {
        params[i] = try upload(&client, spec);
        resident += 1;
        weight_bytes += try params[i].onDeviceSizeInBytes();
        if (i % 100 == 0) std.debug.print("parameter {}/{} {s}\n", .{ i, params.len, spec.name });
    }
    std.debug.print("resident parameter bytes={}\n", .{weight_bytes});
    const base = std.fs.path.dirname(args[2]) orelse ".";
    const programs = try a.alloc(Programs, m.cases.len);
    var prepared: usize = 0;
    defer {
        for (programs[0..prepared]) |*p| p.deinit();
        a.free(programs);
    }
    for (m.cases, 0..) |case, i| {
        if (case.max_tokens == 0 or case.max_tokens > 8192 or case.inputs.len == 0 or case.inputs.len > 16 or case.state_shapes.len != 48) return error.InvalidCase;
        for (case.state_shapes) |shape| _ = try elements(shape);
        const nout = case.state_shapes.len + 2;
        var prefill = try program(&client, init.io, base, case.prefill, args[3], nout);
        errdefer prefill.deinit();
        var decode = try program(&client, init.io, base, case.decode, args[3], nout);
        errdefer decode.deinit();
        const logits = if (case.logits) |name| try program(&client, init.io, base, name, args[3], nout) else null;
        programs[i] = .{ .prefill = prefill, .decode = decode, .logits = logits };
        prepared += 1;
        std.debug.print("ready program {s}\n", .{case.id});
    }
    try writeAll("READY\n");
    while (true) {
        var command: [4]u8 = undefined;
        try readAll(&command);
        const request = std.mem.readInt(u32, &command, .little);
        if (request == std.math.maxInt(u32)) break;
        const validate = request & 0x80000000 != 0;
        const index = request & 0x7fffffff;
        if (index >= m.cases.len) return error.InvalidCaseIndex;
        const case = m.cases[index];
        const p = &programs[index];
        if (validate and p.logits == null) return error.MissingLogitProgram;
        const started = now();
        const inputs = try a.alloc(pjrt.Buffer, case.inputs.len);
        var supplied: usize = 0;
        defer {
            for (inputs[0..supplied]) |*b| b.deinit();
            a.free(inputs);
        }
        for (case.inputs, 0..) |spec, i| {
            inputs[i] = try upload(&client, spec);
            supplied += 1;
        }
        const state = try zeroState(&client, case.state_shapes);
        defer closeAll(state);
        var position = try client.bufferFromHostInt64(&.{0}, &.{});
        defer position.deinit();
        const ready = now();
        var out = try execute(if (validate) &p.logits.? else &p.prefill, params, state, inputs, position);
        defer closeAll(out);
        if (validate) {
            const values = try out[0].toFloat32WithShape(a, &.{ 1, 262144 });
            defer a.free(values);
            for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteLogits;
            const ended = now();
            var header: [24]u8 = undefined;
            std.mem.writeInt(u64, header[0..8], ready - started, .little);
            std.mem.writeInt(u64, header[8..16], ended - ready, .little);
            std.mem.writeInt(u64, header[16..24], 0, .little);
            try writeAll(&header);
            try writeAll(std.mem.sliceAsBytes(values));
            continue;
        }
        const tokens = try a.alloc(i64, case.max_tokens);
        defer a.free(tokens);
        const first = try out[0].toInt64WithShape(a, &.{ 1, 1 });
        tokens[0] = first[0];
        a.free(first);
        const first_ready = now();
        for (1..case.max_tokens) |step| {
            const next = try execute(&p.decode, params, out[1 .. out.len - 1], out[0..1], out[out.len - 1]);
            closeAll(out);
            out = next;
            const token = try out[0].toInt64WithShape(a, &.{ 1, 1 });
            tokens[step] = token[0];
            a.free(token);
        }
        for (tokens) |token| if (token < 0 or token >= 262144) return error.InvalidToken;
        const ended = now();
        var header: [24]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], ready - started, .little);
        std.mem.writeInt(u64, header[8..16], first_ready - ready, .little);
        std.mem.writeInt(u64, header[16..24], ended - first_ready, .little);
        try writeAll(&header);
        try writeAll(std.mem.sliceAsBytes(tokens));
    }
}
