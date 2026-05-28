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

const compat = @import("io/compat.zig");
const c_file = @import("util/c_file.zig");
const manifest_mod = @import("models/manifest.zig");
const tensor_access_mod = @import("models/tensor_access.zig");
const tensor_mod = @import("backends/tensor.zig");

const print = std.debug.print;

const Options = struct {
    model_dir: []const u8,
    output_path: ?[]const u8 = null,
    dry_run: bool = false,
};

pub fn main(allocator: std.mem.Allocator, _: std.Io, args: []const []const u8) !void {
    const opts = parseArgs(args) catch |err| {
        printUsage();
        return err;
    };

    const output_path = if (opts.output_path) |path|
        path
    else
        try defaultOutputPath(allocator, opts.model_dir);
    defer if (opts.output_path == null) allocator.free(output_path);

    var manifest = try manifest_mod.loadFromDir(allocator, opts.model_dir);
    defer manifest.deinit();

    const access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();

    var plan = try buildPlan(allocator, access);
    defer plan.deinit(allocator);

    if (opts.dry_run) {
        print("export dry-run target=safetensors\n", .{});
        print("source: {s}\n", .{opts.model_dir});
        print("output: {s} (not written)\n", .{output_path});
        print("tensors: {d}\n", .{plan.tensors.len});
        print("data_bytes: {d}\n", .{plan.data_bytes});
        return;
    }

    try writeSafetensors(allocator, access, plan, output_path);
    print("exported safetensors to {s}\n", .{output_path});
}

const TensorPlan = struct {
    name: []const u8,
    shape: []i64,
    dtype: tensor_mod.DType,
    byte_len: usize,

    fn deinit(self: *TensorPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.shape);
    }
};

const Plan = struct {
    tensors: []TensorPlan,
    data_bytes: u64,

    fn deinit(self: *Plan, allocator: std.mem.Allocator) void {
        for (self.tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(self.tensors);
    }
};

fn buildPlan(allocator: std.mem.Allocator, access: tensor_access_mod.TensorAccess) !Plan {
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    var data_bytes: u64 = 0;
    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();
        const dtype = switch (record.descriptor.encoding) {
            .dense => |value| value,
            .gguf => return error.UnsupportedQuantizedSafetensorsExport,
        };
        const shape = try allocator.dupe(i64, record.descriptor.shape);
        errdefer allocator.free(shape);
        try tensors.append(allocator, .{
            .name = try allocator.dupe(u8, record.descriptor.name),
            .shape = shape,
            .dtype = dtype,
            .byte_len = record.descriptor.byte_len,
        });
        data_bytes += record.descriptor.byte_len;
    }

    return .{
        .tensors = try tensors.toOwnedSlice(allocator),
        .data_bytes = data_bytes,
    };
}

fn writeSafetensors(
    allocator: std.mem.Allocator,
    access: tensor_access_mod.TensorAccess,
    plan: Plan,
    output_path: []const u8,
) !void {
    var header: std.Io.Writer.Allocating = .init(allocator);
    defer header.deinit();
    const writer = &header.writer;

    try writer.writeAll("{\"__metadata__\":{\"format\":\"termite\"}");
    var offset: u64 = 0;
    for (plan.tensors) |tensor| {
        try writer.writeByte(',');
        try writeJsonString(writer, tensor.name);
        try writer.print(":{{\"dtype\":\"{s}\",\"shape\":[", .{safetensorsDType(tensor.dtype)});
        for (tensor.shape, 0..) |dim, i| {
            if (i > 0) try writer.writeByte(',');
            try writer.print("{d}", .{dim});
        }
        const end = offset + tensor.byte_len;
        try writer.print("],\"data_offsets\":[{d},{d}]}}", .{ offset, end });
        offset = end;
    }
    try writer.writeByte('}');
    while ((header.written().len % 8) != 0) try writer.writeByte(' ');

    if (std.fs.path.dirname(output_path)) |dir| {
        if (dir.len > 0) try compat.cwd().createDirPath(compat.io(), dir);
    }

    const io = compat.io();
    var file = try compat.cwd().createFile(io, output_path, .{ .truncate = true });
    defer file.close(io);

    var size_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &size_buf, header.written().len, .little);
    try file.writeStreamingAll(io, &size_buf);
    try file.writeStreamingAll(io, header.written());

    for (plan.tensors) |tensor| {
        var record = try access.getRecord(allocator, tensor.name);
        defer record.deinit();
        if (record.raw_bytes.len != tensor.byte_len) return error.InvalidTensorByteLength;
        try file.writeStreamingAll(io, record.raw_bytes);
    }
}

fn writeJsonString(writer: *std.Io.Writer, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeByte(c),
        }
    }
    try writer.writeByte('"');
}

fn safetensorsDType(dtype: tensor_mod.DType) []const u8 {
    return switch (dtype) {
        .f32 => "F32",
        .f16 => "F16",
        .bf16 => "BF16",
        .f64 => "F64",
        .i8 => "I8",
        .i16 => "I16",
        .i32 => "I32",
        .i64 => "I64",
        .u8, .bool_ => "U8",
    };
}

fn defaultOutputPath(allocator: std.mem.Allocator, model_dir: []const u8) ![]u8 {
    const trimmed = trimRightSlash(model_dir);
    const base = std.fs.path.basename(trimmed);
    const parent = std.fs.path.dirname(trimmed) orelse ".";
    const filename = try std.fmt.allocPrint(allocator, "{s}.safetensors", .{base});
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ parent, filename });
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len == 0) return error.MissingModelDir;
    var opts = Options{ .model_dir = args[0] };
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--output") and i + 1 < args.len) {
            opts.output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--dry-run")) {
            opts.dry_run = true;
        } else {
            return error.UnknownArgument;
        }
    }
    return opts;
}

fn printUsage() void {
    print(
        \\usage: antfly inference export <model-dir> --target safetensors [--output <path>] [--dry-run]
        \\
        \\Exports dense tensor sources from ONNX, GGUF, or safetensors-backed models
        \\to a safetensors file. Packed GGUF quantized tensors are not exported yet.
        \\
    , .{});
}

test "defaultOutputPath writes sibling safetensors file" {
    const out = try defaultOutputPath(std.testing.allocator, "/tmp/models/antflydb/clipclap");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("/tmp/models/antflydb/clipclap.safetensors", out);
}

test "writeSafetensors emits sorted header and tensor payloads" {
    const allocator = std.testing.allocator;

    var mock = MockAccess{};
    const access = mock.tensorAccess();
    var plan = try buildPlan(allocator, access);
    defer plan.deinit(allocator);

    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/termite-safetensors-export-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(out_path);
    try writeSafetensors(allocator, access, plan, out_path);

    const bytes = try c_file.readFileMax(allocator, out_path, 4096);
    defer allocator.free(bytes);
    try std.testing.expect(bytes.len > 8);
    const header_len = std.mem.readInt(u64, bytes[0..8], .little);
    try std.testing.expectEqual(@as(u64, 0), header_len % 8);
    const header = bytes[8 .. 8 + @as(usize, @intCast(header_len))];
    try std.testing.expect(std.mem.indexOf(u8, header, "\"a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, header, "\"z\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, header, "\"a\"").? < std.mem.indexOf(u8, header, "\"z\"").?);
    const payload = bytes[8 + @as(usize, @intCast(header_len)) ..];
    try std.testing.expectEqualSlices(u8, &MockAccess.a_bytes, payload[0..MockAccess.a_bytes.len]);
    try std.testing.expectEqualSlices(u8, &MockAccess.z_bytes, payload[MockAccess.a_bytes.len..]);
}

const MockAccess = struct {
    const a_shape = [_]i64{2};
    const z_shape = [_]i64{2};
    const a_bytes = [_]u8{ 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40 };
    const z_bytes = [_]u8{ 0x00, 0x00, 0x40, 0x40, 0x00, 0x00, 0x80, 0x40 };

    const vtable = tensor_access_mod.TensorAccess.VTable{
        .getRecord = @ptrCast(&getRecordImpl),
        .listNames = @ptrCast(&listNamesImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    fn tensorAccess(self: *MockAccess) tensor_access_mod.TensorAccess {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn getRecordImpl(_: *MockAccess, _: std.mem.Allocator, name: []const u8) !tensor_access_mod.Record {
        if (std.mem.eql(u8, name, "a")) {
            return record("a", &a_shape, &a_bytes);
        }
        if (std.mem.eql(u8, name, "z")) {
            return record("z", &z_shape, &z_bytes);
        }
        return error.TensorNotFound;
    }

    fn listNamesImpl(_: *MockAccess, allocator: std.mem.Allocator) ![][]const u8 {
        const names = try allocator.alloc([]const u8, 2);
        names[0] = "z";
        names[1] = "a";
        return names;
    }

    fn deinitSelf(_: *MockAccess) void {}

    fn record(name: []const u8, shape: []const i64, bytes: []const u8) tensor_access_mod.Record {
        return .{
            .descriptor = .{
                .name = name,
                .shape = shape,
                .encoding = .{ .dense = .f32 },
                .byte_len = bytes.len,
                .quantized = false,
            },
            .raw_bytes = bytes,
        };
    }
};
