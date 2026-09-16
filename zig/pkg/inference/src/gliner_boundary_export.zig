// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Atomic precision-only conversion of a complete boundary checkpoint. Output
//! is a new directory; publication never overwrites an existing destination.
const std = @import("std");
const bundle = @import("models/gliner_boundary_bundle.zig");
const model = @import("models/gliner_boundary.zig");
const policy = @import("models/gliner_boundary_artifact.zig");
const access_mod = @import("models/tensor_access.zig");
const c_file = @import("util/c_file.zig");
const gguf = @import("gguf/root.zig");
const Control = @import("execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Options = struct { precision: policy.Precision = .fp32, control: ?Control = null };
pub const Result = struct {
    allocator: Allocator,
    receipt_json: []u8,
    tensor_bytes: u64,
    pub fn deinit(self: *Result) void {
        self.allocator.free(self.receipt_json);
        self.* = undefined;
    }
};

/// Verify a published bundle without materializing/dequantizing its weights.
/// Manifest parsing verifies the exact config/tokenizer bytes it consumes;
/// tensor validation hashes the actual opened GGUF mapping.
pub fn verifyDirectory(allocator: Allocator, directory: []const u8, control: ?Control) !policy.Summary {
    try check(control);
    var manifest = try @import("models/manifest.zig").loadFromDir(allocator, directory);
    defer manifest.deinit();
    const receipt = manifest.gliner_boundary_bundle orelse return error.InvalidGlinerBoundaryBundle;
    const config = manifest.gliner_boundary_config orelse return error.InvalidGlinerBoundaryBundle;
    var store = try @import("models/tensor_store.zig").openFromManifest(allocator, manifest);
    defer store.deinit();
    return bundle.validateLoadedGguf(allocator, receipt.value, config, store.ggufFile() orelse return error.InvalidGlinerBoundaryBundle, store.ggufArtifactBytes() orelse return error.InvalidGlinerBoundaryBundle, control);
}

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

/// Remove only an unpublished path owned by the caller. Blocking cooperative
/// cancellation here allows cleanup to finish when a different error already
/// won the request; the caller's process watchdog remains armed through teardown.
/// Never pass the published destination to either cleanup helper.
pub fn cleanupPrivateTree(io: std.Io, path: []const u8) void {
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    std.Io.Dir.cwd().deleteTree(io, path) catch {};
}

pub fn cleanupPrivateFile(io: std.Io, path: []const u8) void {
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    std.Io.Dir.cwd().deleteFile(io, path) catch {};
}

pub fn syncDirectory(io: std.Io, path: []const u8) !void {
    switch (@import("builtin").os.tag) {
        .linux, .macos, .freebsd, .openbsd, .netbsd, .dragonfly => {
            // iterate avoids an O_PATH descriptor on Linux, which cannot fsync.
            var dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
            defer dir.close(io);
            const file = std.Io.File{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
            try file.sync(io);
        },
        else => return error.UnsupportedGlinerBoundaryPublicationPlatform,
    }
}

pub fn publishDirectory(allocator: Allocator, io: std.Io, source: []const u8, destination: []const u8) !void {
    if (comptime @import("builtin").os.tag == .macos) {
        // Zig 0.16's generic renamePreserve uses a hard-link fallback on
        // Darwin. Directories require the native atomic RENAME_EXCL call.
        const Darwin = struct {
            extern "c" fn renamex_np([*:0]const u8, [*:0]const u8, c_uint) c_int;
        };
        const source_z = try allocator.dupeZ(u8, source);
        defer allocator.free(source_z);
        const destination_z = try allocator.dupeZ(u8, destination);
        defer allocator.free(destination_z);
        while (true) switch (std.posix.errno(Darwin.renamex_np(source_z.ptr, destination_z.ptr, 0x00000004))) {
            .SUCCESS => return,
            .INTR => continue,
            .EXIST, .NOTEMPTY => return error.PathAlreadyExists,
            .NOENT => return error.FileNotFound,
            .XDEV => return error.RenameAcrossMountPoints,
            .ACCES, .PERM => return error.AccessDenied,
            .NOTDIR => return error.NotDir,
            .NOSPC => return error.NoSpaceLeft,
            else => return error.GlinerBoundaryPublicationFailed,
        };
    } else if (comptime @import("builtin").os.tag == .linux) {
        try std.Io.Dir.cwd().renamePreserve(source, std.Io.Dir.cwd(), destination, io);
    } else return error.UnsupportedGlinerBoundaryPublicationPlatform;
}
fn pin(allocator: Allocator, path: []const u8, bytes: []const u8, control: ?Control) !bundle.FilePin {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| (256 * 1024));
        hash.update(bytes[offset..end]);
        offset = end;
    }
    return .{ .path = path, .size_bytes = bytes.len, .sha256 = try allocator.dupe(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower)) };
}

const Sink = struct {
    file: std.Io.File,
    io: std.Io,
    control: ?Control,
    hash: std.crypto.hash.sha2.Sha256 = std.crypto.hash.sha2.Sha256.init(.{}),
    bytes: u64 = 0,
    fn write(self: *@This(), bytes: []const u8) !void {
        var offset: usize = 0;
        while (offset < bytes.len) {
            try check(self.control);
            const end = @min(bytes.len, offset +| (256 * 1024));
            try self.file.writeStreamingAll(self.io, bytes[offset..end]);
            self.hash.update(bytes[offset..end]);
            self.bytes = try std.math.add(u64, self.bytes, end - offset);
            offset = end;
        }
    }
    fn padTo(self: *@This(), target: u64) !void {
        if (target < self.bytes) return error.InvalidTensorOffset;
        const zeros = [_]u8{0} ** 256;
        while (self.bytes < target) try self.write(zeros[0..@intCast(@min(target - self.bytes, zeros.len))]);
    }
};

fn writeTensor(allocator: Allocator, sink: *Sink, bytes: []const u8, kind: gguf.tensor_types.KnownTensorType, row_width: usize) !void {
    if (bytes.len % 4 != 0 or row_width == 0 or bytes.len / 4 % row_width != 0) return error.InvalidGlinerBoundaryTensorShape;
    if (kind == .F32) {
        var offset: usize = 0;
        while (offset < bytes.len) : (offset += 4) {
            if (offset % (64 * 1024) == 0) try check(sink.control);
            const value: f32 = @bitCast(std.mem.readInt(u32, bytes[offset..][0..4], .little));
            if (!std.math.isFinite(value)) return error.NonFiniteGlinerBoundaryWeight;
        }
        return sink.write(bytes);
    }
    const row = try allocator.alloc(f32, row_width);
    defer allocator.free(row);
    const half_bytes: []u8 = if (kind == .F16) try allocator.alloc(u8, try std.math.mul(usize, row_width, 2)) else &.{};
    defer if (kind == .F16) allocator.free(half_bytes);
    const row_bytes = try std.math.mul(usize, row_width, 4);
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += row_bytes) {
        try check(sink.control);
        for (row, 0..) |*value, index| {
            value.* = @bitCast(std.mem.readInt(u32, bytes[offset + index * 4 ..][0..4], .little));
            if (!std.math.isFinite(value.*)) return error.NonFiniteGlinerBoundaryWeight;
            // Reduced profiles store fp16 scales. Fail before conversion can
            // emit an infinity; published checkpoints are well within this cap.
            if (@abs(value.*) > 65504) return error.GlinerBoundaryPrecisionRangeExceeded;
        }
        if (kind == .F16) {
            for (row, 0..) |value, index| std.mem.writeInt(u16, half_bytes[index * 2 ..][0..2], @bitCast(@as(f16, @floatCast(value))), .little);
            try sink.write(half_bytes);
        } else {
            const encoded = switch (kind) {
                .Q8_0 => try gguf.quant_codec.quantizeQ8_0FromF32(allocator, row),
                .Q4_K => try gguf.quant_codec.quantizeQ4_KFromF32(allocator, row),
                .Q4_0 => try gguf.quant_codec.quantizeQ4_0FromF32(allocator, row),
                else => return error.UnsupportedGlinerBoundaryPrecision,
            };
            defer allocator.free(encoded);
            try sink.write(encoded);
        }
    }
}

pub fn exportBundle(allocator: Allocator, io: std.Io, source_dir: []const u8, output_dir: []const u8, options: Options) !Result {
    try check(options.control);
    if (output_dir.len == 0 or std.mem.eql(u8, std.fs.path.basename(output_dir), ".") or std.mem.eql(u8, std.fs.path.basename(output_dir), "..")) return error.InvalidOutputPath;
    const cwd = std.Io.Dir.cwd();
    if (cwd.access(io, output_dir, .{})) |_| return error.PathAlreadyExists else |err| if (err != error.FileNotFound) return err;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sidecars: [4][]const u8 = undefined;
    var source_pins: [5]bundle.FilePin = undefined;
    for (bundle.sidecar_names, 0..) |name, index| {
        const path = try std.fs.path.join(a, &.{ source_dir, name });
        sidecars[index] = try c_file.readFileMax(a, path, @intCast(try bundle.fileLimit(name)));
        source_pins[index + 1] = try pin(a, name, sidecars[index], options.control);
    }
    const config = try model.parseConfig(a, sidecars[0], sidecars[1]);
    try policy.validatePrecision(config.backbone, options.precision);
    const source_path = try std.fs.path.join(a, &.{ source_dir, "model.safetensors" });
    const owner = try access_mod.SafetensorsAccess.initAbsolute(allocator, source_path);
    const access = owner.tensorAccess();
    defer access.deinit();
    const source_bytes = owner.source.reader.file_bytes;
    if (source_bytes.len > try bundle.fileLimit("model.safetensors")) return error.GlinerBoundaryBundleLimitExceeded;
    source_pins[0] = try pin(a, "model.safetensors", source_bytes, options.control);
    const names = try access.listNames(a);
    const descriptors = try a.alloc(access_mod.Descriptor, names.len);
    for (names, descriptors) |name, *descriptor| {
        var record = try access.getRecord(a, name);
        defer record.deinit();
        descriptor.* = record.descriptor;
    }
    _ = try policy.validate(a, config.backbone, .fp32, descriptors, options.control);
    const specs = policy.specs(config.backbone);
    const tensors = try a.alloc(gguf.writer.TensorSpec, specs.len);
    var tensor_bytes: u64 = 0;
    for (specs, tensors) |spec, *tensor| {
        const kind = try policy.targetType(config.backbone, options.precision, spec);
        const dims = try a.alloc(u64, spec.shape.len);
        for (spec.shape, 0..) |dim, index| dims[dims.len - 1 - index] = @intCast(dim);
        tensor.* = .{ .name = spec.name, .dimensions = dims, .tensor_type = .{ .known = kind } };
        tensor_bytes = try std.math.add(u64, tensor_bytes, try policy.tensorBytes(spec, kind));
    }
    const metadata = [_]gguf.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = bundle.architecture } },
        .{ .key = "general.alignment", .value = .{ .u32 = 32 } },
        .{ .key = "antfly.boundary.architecture_version", .value = .{ .u32 = model.architecture_version } },
        .{ .key = "antfly.boundary.config_version", .value = .{ .u32 = model.config_version } },
        .{ .key = "antfly.boundary.tensor_policy_version", .value = .{ .u32 = policy.policy_version } },
        .{ .key = "antfly.boundary.backbone", .value = .{ .string = @tagName(config.backbone) } },
        .{ .key = "antfly.boundary.precision", .value = .{ .string = @tagName(options.precision) } },
        .{ .key = "antfly.boundary.source_sha256", .value = .{ .string = source_pins[0].sha256 } },
    };
    var layout = try gguf.writer.buildLayout(allocator, &metadata, tensors);
    defer layout.deinit(allocator);
    const parent = std.fs.path.dirname(output_dir) orelse ".";
    // Require the publication parent to exist: syncing a newly created deep
    // directory chain would otherwise leave ancestor durability unspecified.
    try syncDirectory(io, parent);
    var random: [16]u8 = undefined;
    try std.Io.randomSecure(io, &random);
    const staging = try std.fs.path.join(a, &.{ parent, try std.fmt.allocPrint(a, ".gliner25-build-{s}", .{std.fmt.bytesToHex(random, .lower)}) });
    try cwd.createDir(io, staging, .default_dir);
    var published = false;
    defer if (!published) cleanupPrivateTree(io, staging);
    const weight_path = try std.fs.path.join(a, &.{ staging, bundle.model_name });
    var file = try cwd.createFile(io, weight_path, .{ .exclusive = true });
    var file_open = true;
    defer if (file_open) file.close(io);
    var sink = Sink{ .file = file, .io = io, .control = options.control };
    try sink.write(layout.header_bytes);
    const data_offset = std.mem.alignForward(u64, layout.header_bytes.len, layout.alignment);
    for (specs, tensors, layout.offsets) |spec, tensor, offset| {
        try sink.padTo(try std.math.add(u64, data_offset, offset));
        var record = try access.getRecord(allocator, spec.name);
        defer record.deinit();
        try writeTensor(allocator, &sink, record.raw_bytes, tensor.tensor_type.known, @intCast(spec.shape[spec.shape.len - 1]));
        const expected_end = try std.math.add(u64, try std.math.add(u64, data_offset, offset), try policy.tensorBytes(spec, tensor.tensor_type.known));
        if (sink.bytes != expected_end) return error.InvalidGlinerBoundaryTensorShape;
    }
    try file.sync(io);
    file.close(io);
    file_open = false;
    var output_pins = source_pins;
    output_pins[0] = .{ .path = bundle.model_name, .size_bytes = sink.bytes, .sha256 = try a.dupe(u8, &std.fmt.bytesToHex(sink.hash.finalResult(), .lower)) };
    for (bundle.sidecar_names, sidecars) |name, bytes| {
        const path = try std.fs.path.join(a, &.{ staging, name });
        if (std.fs.path.dirname(path)) |dir| try cwd.createDirPath(io, dir);
        var sidecar = try cwd.createFile(io, path, .{ .exclusive = true });
        defer sidecar.close(io);
        try sidecar.writeStreamingAll(io, bytes);
        try sidecar.sync(io);
    }
    // Detect source mutation during conversion before publishing an output.
    try bundle.verifyBytes(source_pins[0], source_bytes, options.control);
    const receipt = bundle.Receipt{ .family = bundle.family, .version = 1, .architecture_version = model.architecture_version, .config_version = model.config_version, .tensor_policy_version = policy.policy_version, .backbone = config.backbone, .precision = options.precision, .source_files = &source_pins, .files = &output_pins };
    try bundle.validate(receipt);
    {
        var mapped = try c_file.MmapRegion.init(allocator, weight_path);
        defer mapped.deinit();
        var parsed = try gguf.format.parse(allocator, mapped.data);
        defer parsed.deinit(allocator);
        try gguf.format.validateTensorDataRanges(&parsed, mapped.data.len);
        _ = try bundle.validateLoadedGguf(allocator, receipt, config, &parsed, mapped.data, options.control);
    }
    const receipt_json = try std.json.Stringify.valueAlloc(allocator, receipt, .{ .whitespace = .indent_2 });
    errdefer allocator.free(receipt_json);
    const marker_path = try std.fs.path.join(a, &.{ staging, bundle.marker_name });
    {
        var marker = try cwd.createFile(io, marker_path, .{ .exclusive = true });
        defer marker.close(io);
        try marker.writeStreamingAll(io, receipt_json);
        try marker.sync(io);
    }
    try syncDirectory(io, try std.fs.path.join(a, &.{ staging, "encoder_config" }));
    try syncDirectory(io, staging);
    try check(options.control);
    try publishDirectory(allocator, io, staging, output_dir);
    published = true;
    // The destination is complete even if the final durability barrier fails;
    // never delete a published model in the error path.
    try syncDirectory(io, parent);
    return .{ .allocator = allocator, .receipt_json = receipt_json, .tensor_bytes = tensor_bytes };
}

test "gliner boundary private publication cleanup preserves pending cancellation and published files" {
    const a = std.testing.allocator;
    const test_io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(test_io, ".", a);
    defer a.free(parent);
    const stage = try std.fs.path.join(a, &.{ parent, "private-stage" });
    defer a.free(stage);
    const receipt = try std.fs.path.join(a, &.{ parent, "private-receipt.tmp" });
    defer a.free(receipt);
    const payload = try std.fs.path.join(a, &.{ stage, "payload" });
    defer a.free(payload);
    const published = try std.fs.path.join(a, &.{ parent, "published" });
    defer a.free(published);
    try temporary.dir.createDirPath(test_io, "private-stage/payload");
    try temporary.dir.writeFile(test_io, .{ .sub_path = "private-stage/payload/model", .data = "complete" });
    try temporary.dir.writeFile(test_io, .{ .sub_path = "private-stage/partial", .data = "partial" });
    try temporary.dir.writeFile(test_io, .{ .sub_path = "private-receipt.tmp", .data = "partial receipt" });
    try publishDirectory(a, test_io, payload, published);

    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Context = struct {
        io: std.Io,
        ready: std.Io.Event = .unset,
        never: std.Io.Event = .unset,
        cancellation_preserved: bool = false,

        fn run(self: *@This(), owned_stage: []const u8, owned_receipt: []const u8) !void {
            defer {
                cleanupPrivateTree(self.io, owned_stage);
                cleanupPrivateFile(self.io, owned_receipt);
                self.cancellation_preserved = if (self.io.checkCancel()) |_| false else |err| err == error.Canceled;
            }
            self.ready.set(self.io);
            self.never.wait(self.io) catch |err| switch (err) {
                error.Canceled => {
                    // Restore a real pending Io cancellation before unwinding
                    // with the distinct execution-control cancellation error.
                    self.io.recancel();
                    return error.Cancelled;
                },
            };
            return error.TestUnexpectedResult;
        }
    };
    var context = Context{ .io = io };
    var future = try io.concurrent(Context.run, .{ &context, stage, receipt });
    var joined = false;
    defer if (!joined) {
        _ = future.cancel(io) catch {};
    };
    try context.ready.wait(io);
    const result = future.cancel(io);
    joined = true;
    try std.testing.expectError(error.Cancelled, result);
    try std.testing.expect(context.cancellation_preserved);
    try std.testing.expectError(error.FileNotFound, temporary.dir.access(test_io, "private-stage", .{}));
    try std.testing.expectError(error.FileNotFound, temporary.dir.access(test_io, "private-receipt.tmp", .{}));
    const file = try temporary.dir.openFile(test_io, "published/model", .{});
    defer file.close(test_io);
    var bytes: [8]u8 = undefined;
    try std.testing.expectEqual(bytes.len, try file.readPositionalAll(test_io, &bytes, 0));
    try std.testing.expectEqualStrings("complete", &bytes);
}

test "gliner boundary conversion rejects invalid destination and cancellation before files" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidOutputPath, exportBundle(a, std.testing.io, "/unused", "", .{}));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, exportBundle(a, std.testing.io, "/unused", "/unused", .{ .control = .{ .check_fn = Cancel.check } }));
}

test "gliner boundary conversion pinned small FP32 roundtrip and same size substitution" {
    const a = std.testing.allocator;
    const source = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const target = try std.fs.path.join(a, &.{ parent, "bundle" });
    defer a.free(target);
    var result = try exportBundle(a, std.testing.io, source, target, .{});
    defer result.deinit();
    const summary = try verifyDirectory(a, target, null);
    try std.testing.expectEqual(@as(usize, 334), summary.tensors);
    try std.testing.expectError(error.PathAlreadyExists, exportBundle(a, std.testing.io, source, target, .{}));
    // Change one payload byte without changing file length or receipt. The
    // real session loader must reject the opened artifact before weights load.
    const path = try std.fs.path.join(a, &.{ target, bundle.model_name });
    defer a.free(path);
    var weight = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
    defer weight.close(std.testing.io);
    const stat = try weight.stat(std.testing.io);
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try weight.readPositional(std.testing.io, &.{&byte}, stat.size - 1));
    byte[0] ^= 1;
    try weight.writePositionalAll(std.testing.io, &byte, stat.size - 1);
    try weight.sync(std.testing.io);
    try std.testing.expectError(error.GlinerBoundaryArtifactMismatch, @import("architectures/session_factory.zig").createNativeSession(a, target));
}
