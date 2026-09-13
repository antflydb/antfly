// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Versioned boundary bundle integrity and precision contract. A receipt is
//! an integrity record, not a quality qualification or a publisher signature.
//! The model loader verifies the bytes of its opened tensor store, so a
//! different same-size GGUF cannot be admitted using stale receipt metadata.
const std = @import("std");
const model = @import("gliner_boundary.zig");
const policy = @import("gliner_boundary_artifact.zig");
const access = @import("tensor_access.zig");
const gguf = @import("../gguf/root.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const family = "gliner_boundary_bundle/v1";
pub const architecture = "antfly-gliner-boundary";
pub const marker_name = "antfly_inference_bundle.json";
pub const model_name = "model.gguf";
pub const sidecar_names = [_][]const u8{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" };
pub const max_receipt_bytes = 64 * 1024;

pub const FilePin = struct { path: []const u8, size_bytes: u64, sha256: []const u8 };
pub const Receipt = struct {
    family: []const u8,
    version: u32,
    architecture_version: u32,
    config_version: u32,
    tensor_policy_version: u32,
    backbone: model.Backbone,
    precision: policy.Precision,
    /// Source files are recorded independently from converted output files.
    source_files: []const FilePin,
    files: []const FilePin,
};
pub const Parsed = std.json.Parsed(Receipt);

/// Digest of bytes already consumed by a metadata parser. Keeping this small
/// value lets bundle admission verify that exact snapshot without reopening it.
pub const Digest = struct {
    size_bytes: u64,
    sha256: [64]u8,
    pub fn of(bytes: []const u8) Digest {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return .{ .size_bytes = bytes.len, .sha256 = std.fmt.bytesToHex(digest, .lower) };
    }
    pub fn verify(self: Digest, expected: FilePin) !void {
        if (self.size_bytes != expected.size_bytes or !std.mem.eql(u8, &self.sha256, expected.sha256))
            return error.GlinerBoundaryArtifactMismatch;
    }
};

/// The snapshot consumed by a live session. This is independent of source
/// pathnames and remains useful when an immutable model directory is renamed.
pub const Identity = struct {
    backbone: model.Backbone,
    precision: policy.Precision,
    weight: Digest,
    sidecars: [4]Digest,

    pub fn fingerprint(self: Identity) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-gliner-boundary-inference-artifact/v1\x00");
        hash.update(@tagName(self.backbone));
        hash.update(@tagName(self.precision));
        hash.update(&self.weight.sha256);
        for (self.sidecars) |sidecar| hash.update(&sidecar.sha256);
        return hash.finalResult();
    }

    pub fn verifySidecars(self: Identity, sidecars: [4]Digest) !void {
        for (self.sidecars, sidecars) |expected, actual|
            if (expected.size_bytes != actual.size_bytes or !std.mem.eql(u8, &expected.sha256, &actual.sha256))
                return error.GlinerBoundaryArtifactMismatch;
    }
};

pub fn fileLimit(path: []const u8) !u64 {
    if (std.mem.eql(u8, path, "model.safetensors") or std.mem.eql(u8, path, model_name)) return 16 * 1024 * 1024 * 1024;
    if (std.mem.eql(u8, path, "tokenizer.json")) return 32 * 1024 * 1024;
    for (sidecar_names) |name| if (std.mem.eql(u8, path, name)) return 1024 * 1024;
    return error.InvalidGlinerBoundaryBundle;
}

fn validatePins(pins: []const FilePin, weight_name: []const u8) !void {
    if (pins.len != sidecar_names.len + 1) return error.InvalidGlinerBoundaryBundle;
    for (pins, 0..) |pin, index| {
        if (!std.mem.eql(u8, pin.path, weight_name)) {
            var found = false;
            for (sidecar_names) |name| if (std.mem.eql(u8, pin.path, name)) {
                found = true;
                break;
            };
            if (!found) return error.InvalidGlinerBoundaryBundle;
        }
        for (pins[0..index]) |prior| if (std.mem.eql(u8, prior.path, pin.path)) return error.InvalidGlinerBoundaryBundle;
        if (pin.size_bytes == 0 or pin.size_bytes > try fileLimit(pin.path) or pin.sha256.len != 64) return error.InvalidGlinerBoundaryBundle;
        for (pin.sha256) |c| if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return error.InvalidGlinerBoundaryBundle;
    }
}

pub fn validate(receipt: Receipt) !void {
    if (!std.mem.eql(u8, receipt.family, family) or receipt.version != 1 or
        receipt.architecture_version != model.architecture_version or receipt.config_version != model.config_version or
        receipt.tensor_policy_version != policy.policy_version) return error.UnsupportedGlinerBoundaryBundle;
    try policy.validatePrecision(receipt.backbone, receipt.precision);
    try validatePins(receipt.source_files, "model.safetensors");
    try validatePins(receipt.files, model_name);
    // Sidecars are copied verbatim. A converter cannot alter tokenization or
    // configuration while presenting itself as a precision-only conversion.
    for (sidecar_names) |name| {
        const source = try pinFor(receipt.source_files, name);
        const output = try pinFor(receipt.files, name);
        if (source.size_bytes != output.size_bytes or !std.mem.eql(u8, source.sha256, output.sha256)) return error.InvalidGlinerBoundaryBundle;
    }
}

pub fn parse(allocator: Allocator, bytes: []const u8) !Parsed {
    if (bytes.len > max_receipt_bytes) return error.GlinerBoundaryBundleLimitExceeded;
    var parsed = try std.json.parseFromSlice(Receipt, allocator, bytes, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

pub fn pinFor(pins: []const FilePin, name: []const u8) !FilePin {
    for (pins) |pin| if (std.mem.eql(u8, pin.path, name)) return pin;
    return error.InvalidGlinerBoundaryBundle;
}

/// Use with the exact mapped bytes retained by the runtime or the bytes that
/// were passed to the tokenizer/config parser, not a separately reopened path.
pub fn verifyBytes(pin: FilePin, bytes: []const u8, control: ?Control) !void {
    if (bytes.len != pin.size_bytes or bytes.len > try fileLimit(pin.path)) return error.GlinerBoundaryArtifactMismatch;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        if (control) |active| try active.check();
        const end = @min(bytes.len, offset +| (256 * 1024));
        hasher.update(bytes[offset..end]);
        offset = end;
    }
    if (control) |active| try active.check();
    const digest = std.fmt.bytesToHex(hasher.finalResult(), .lower);
    if (!std.mem.eql(u8, &digest, pin.sha256)) return error.GlinerBoundaryArtifactMismatch;
}

pub fn validateLoadedGguf(allocator: Allocator, receipt: Receipt, config: model.Config, file: *const gguf.format.File, bytes: []const u8, control: ?Control) !policy.Summary {
    try validate(receipt);
    if (config.backbone != receipt.backbone or config.version != receipt.config_version or config.architecture_version != receipt.architecture_version)
        return error.GlinerBoundaryArtifactMismatch;
    try verifyBytes(try pinFor(receipt.files, model_name), bytes, control);
    const meta = gguf.metadata.View.init(file);
    if (!std.mem.eql(u8, meta.getString("general.architecture") orelse "", architecture) or
        !std.mem.eql(u8, meta.getString("antfly.boundary.precision") orelse "", @tagName(receipt.precision)) or
        !std.mem.eql(u8, meta.getString("antfly.boundary.backbone") orelse "", @tagName(receipt.backbone)) or
        !std.mem.eql(u8, meta.getString("antfly.boundary.source_sha256") orelse "", (try pinFor(receipt.source_files, "model.safetensors")).sha256) or
        meta.getU64("antfly.boundary.architecture_version") != model.architecture_version or
        meta.getU64("antfly.boundary.config_version") != model.config_version or
        meta.getU64("antfly.boundary.tensor_policy_version") != policy.policy_version)
        return error.InvalidGlinerBoundaryBundle;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const descriptors = try a.alloc(access.Descriptor, file.tensors.len);
    for (file.tensors, descriptors) |tensor, *descriptor| {
        const shape = try a.alloc(i64, tensor.dimensions.len);
        for (tensor.dimensions, 0..) |dim, index| shape[shape.len - 1 - index] = std.math.cast(i64, dim) orelse return error.InvalidGlinerBoundaryTensorShape;
        descriptor.* = .{
            .name = tensor.name,
            .shape = shape,
            .encoding = .{ .gguf = tensor.tensor_type },
            .byte_len = std.math.cast(usize, gguf.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.InvalidGlinerBoundaryTensorShape) orelse return error.InvalidGlinerBoundaryTensorShape,
            .quantized = !std.meta.eql(tensor.tensor_type, gguf.tensor_types.TensorType{ .known = .F32 }) and !std.meta.eql(tensor.tensor_type, gguf.tensor_types.TensorType{ .known = .F16 }),
        };
    }
    return policy.validate(a, config.backbone, receipt.precision, descriptors, control);
}

test "gliner boundary bundle rejects missing duplicate unsafe and changed sidecars" {
    const a = std.testing.allocator;
    const zero = "0" ** 64;
    const source = [_]FilePin{
        .{ .path = "model.safetensors", .size_bytes = 32, .sha256 = zero },
        .{ .path = "config.json", .size_bytes = 1, .sha256 = zero },
        .{ .path = "encoder_config/config.json", .size_bytes = 1, .sha256 = zero },
        .{ .path = "tokenizer.json", .size_bytes = 1, .sha256 = zero },
        .{ .path = "tokenizer_config.json", .size_bytes = 1, .sha256 = zero },
    };
    var output = source;
    output[0].path = model_name;
    const receipt = Receipt{ .family = family, .version = 1, .architecture_version = 1, .config_version = 3, .tensor_policy_version = 1, .backbone = .small, .precision = .q4_0, .source_files = &source, .files = &output };
    try validate(receipt);
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(bytes);
    var parsed = try parse(a, bytes);
    defer parsed.deinit();
    try std.testing.expectEqual(policy.Precision.q4_0, parsed.value.precision);
    const invalid_config = std.mem.zeroes(model.Config);
    const empty_file = std.mem.zeroes(gguf.format.File);
    try std.testing.expectError(error.GlinerBoundaryArtifactMismatch, validateLoadedGguf(a, receipt, invalid_config, &empty_file, "abc", null));
    output[4].path = "../tokenizer_config.json";
    try std.testing.expectError(error.InvalidGlinerBoundaryBundle, validate(receipt));
    output[4] = output[3];
    try std.testing.expectError(error.InvalidGlinerBoundaryBundle, validate(receipt));
    output[4] = source[4];
    output[4].sha256 = "1" ** 64;
    try std.testing.expectError(error.InvalidGlinerBoundaryBundle, validate(receipt));
    output[4] = source[4];
    var incomplete = receipt;
    incomplete.files = output[0..4];
    try std.testing.expectError(error.InvalidGlinerBoundaryBundle, validate(incomplete));
}

test "gliner boundary bundle rehashes actual bytes and checks cancellation" {
    const pin = FilePin{ .path = model_name, .size_bytes = 3, .sha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" };
    try verifyBytes(pin, "abc", null);
    try std.testing.expectError(error.GlinerBoundaryArtifactMismatch, verifyBytes(pin, "abd", null));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, verifyBytes(pin, "abc", .{ .check_fn = Cancel.check }));
}

test "gliner boundary identity binds artifact precision and the consumed sidecar snapshots" {
    const a = std.testing.allocator;
    _ = a;
    const config = Digest.of("config");
    const identity = Identity{ .backbone = .small, .precision = .fp32, .weight = Digest.of("weight"), .sidecars = .{ config, Digest.of("encoder"), Digest.of("tokenizer"), Digest.of("tokenizer_config") } };
    try identity.verifySidecars(identity.sidecars);
    var changed = identity;
    changed.sidecars[2] = Digest.of("Tokenizer");
    try std.testing.expectError(error.GlinerBoundaryArtifactMismatch, identity.verifySidecars(changed.sidecars));
    try std.testing.expect(!std.mem.eql(u8, &identity.fingerprint(), &changed.fingerprint()));
    changed = identity;
    changed.precision = .q8_0;
    try std.testing.expect(!std.mem.eql(u8, &identity.fingerprint(), &changed.fingerprint()));
    changed = identity;
    changed.weight = Digest.of("Weight");
    try std.testing.expect(!std.mem.eql(u8, &identity.fingerprint(), &changed.fingerprint()));
}
