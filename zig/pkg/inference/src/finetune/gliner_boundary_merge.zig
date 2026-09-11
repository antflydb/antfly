// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Materialize an adapter against a verified, immutable original Source.
//! The caller holds both owners unchanged until return. No source pathname or
//! mutable mapping is reopened, and only one adapted matrix is live at once.
//! Every tensor and sidecar is independently verified before publication.
//! These integrity receipts do not qualify model quality or interoperability.
const std = @import("std");
const builtin = @import("builtin");
const source_mod = @import("gliner_boundary_training_source.zig");
const run = @import("gliner_boundary_run.zig");
const adapter = @import("gliner_boundary_adapter.zig");
const model = @import("../models/gliner_boundary.zig");
const policy = @import("../models/gliner_boundary_artifact.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const access_mod = @import("../models/tensor_access.zig");
const writer = @import("../native_export_safetensors.zig");
const publication = @import("../gliner_boundary_export.zig");
const files = @import("../runtime/file_snapshot.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const fixed_scratch_bytes = 512 * 1024;

pub const family = "gliner_boundary_merge/v1";
pub const receipt_name = "antfly_gliner25_merge.json";
pub const math_policy = "f32_lora_delta_f64_dora_row_norm_v1";
pub const Limits = struct {
    /// Reclaiming metadata, one merged matrix, and fixed streaming scratch.
    /// Source and imported adapter owners are separately admitted by the job.
    max_scratch_bytes: usize = 64 * mib,
    max_output_bytes: u64 = 2 * 1024 * mib,
    max_header_bytes: usize = 4 * mib,
    max_receipt_bytes: usize = 64 * 1024,
    adapter: adapter.Limits = .{},
};
pub const AdapterFiles = struct {
    config: bundle.Digest,
    weights: bundle.Digest,
    /// Null requires an absent receipt, never an unchecked receipt file.
    receipt: ?bundle.Digest,
};
pub const Provenance = struct {
    configuration: bundle.Digest,
    adapter_files: AdapterFiles,
    schema_sha256: [32]u8,
};
pub const Estimate = struct {
    output_bytes_upper_bound: u64,
    tensor_payload_bytes: u64,
    largest_adapted_tensor_bytes: usize,
    reserved_scratch_bytes: usize,
    tensor_count: usize,
    merged_tensor_count: usize,
};
pub const Receipt = struct {
    family: []const u8 = family,
    version: u32 = 1,
    architecture_version: u32 = model.architecture_version,
    config_version: u32 = model.config_version,
    tensor_policy_version: u32 = policy.policy_version,
    math_policy: []const u8 = math_policy,
    source: bundle.Identity,
    provenance: Provenance,
    target_sha256: [32]u8,
    parameter_sha256: [32]u8,
    merged: bundle.Identity,
    tensor_count: usize,
    merged_tensor_count: usize,
};
pub const Result = struct {
    identity: bundle.Identity,
    receipt: bundle.Digest,
    output_bytes: u64,
    peak_scratch_bytes: usize,
    tensor_count: usize,
    merged_tensor_count: usize,
};

const View = struct {
    identity: bundle.Identity,
    parameters: []const run.Parameter,
    sidecars: [4][]const u8,

    fn of(source: *const source_mod.Source, control: ?Control) !View {
        if (source.config.backbone != source.identity.backbone) return error.InvalidBoundaryMergeSource;
        var result = View{ .identity = source.identity, .parameters = source.parameters, .sidecars = undefined };
        for (&result.sidecars, 0..) |*bytes, index| bytes.* = try source.sidecar(index);
        try validateSource(result, control);
        return result;
    }
};
const Tensor = struct {
    descriptor: access_mod.Descriptor,
    base: []const f32,
    module: ?adapter.Module,
};
const Prepared = struct {
    tensors: []Tensor,
    estimate: Estimate,
    target_sha256: [32]u8,
    parameter_sha256: [32]u8,
};

// A terminal allocation observer distinguishes declared denials from backing
// OOM, including allocating JSON writers that report WriteFailed. Failed
// resize/remap attempts are not terminal allocations and cannot poison it.
pub const Scratch = struct {
    budget: Budget,
    last_failure: ?Budget.AllocationFailure = null,

    pub fn init(a: Allocator, limit: usize) Scratch {
        return .{ .budget = .{ .backing = a, .limit = limit } };
    }
    pub fn allocator(self: *Scratch) Allocator {
        self.budget.failure_context = self;
        self.budget.allocation_failed = failed;
        return self.budget.allocator();
    }
    pub fn resetFailure(self: *Scratch) void {
        self.last_failure = null;
    }
    fn failed(raw: ?*anyopaque, failure: Budget.AllocationFailure) void {
        const self: *Scratch = @ptrCast(@alignCast(raw.?));
        self.last_failure = failure;
    }
    pub fn mapError(self: *const Scratch, err: anyerror) anyerror {
        if (err != error.OutOfMemory and err != error.WriteFailed) return err;
        const failure = self.last_failure orelse return err;
        return if (failure.kind == .declared_limit) error.BoundaryMergeMemoryLimitExceeded else error.OutOfMemory;
    }
    pub fn deinit(self: *Scratch) void {
        std.debug.assert(self.budget.live == 0);
        self.* = undefined;
    }
};

/// Complete inventory and identity validation, without creating output files.
/// The returned reservation includes one matrix and all verification scratch.
pub fn estimate(a: Allocator, source: *const source_mod.Source, loaded: *const adapter.Loaded, provenance: Provenance, limits: Limits, control: ?Control) !Estimate {
    try validateLimits(limits);
    var scratch = Scratch.init(a, limits.max_scratch_bytes - fixed_scratch_bytes);
    defer scratch.deinit();
    var arena = std.heap.ArenaAllocator.init(scratch.allocator());
    defer arena.deinit();
    const prepared = prepare(arena.allocator(), try View.of(source, control), loaded, provenance, limits, control) catch |err| return scratch.mapError(err);
    return prepared.estimate;
}

/// Source and Loaded are borrowed immutable leases. A successful return owns
/// no allocations. Cancellation before publication removes private staging;
/// after publication the complete artifact is never deleted by cleanup.
pub fn materialize(a: Allocator, io: std.Io, source: *const source_mod.Source, loaded: *const adapter.Loaded, output: []const u8, provenance: Provenance, limits: Limits, control: ?Control) !Result {
    try validateLimits(limits);
    try check(control);
    try validateDestination(io, output);
    const view = try View.of(source, control);
    var scratch = Scratch.init(a, limits.max_scratch_bytes - fixed_scratch_bytes);
    defer scratch.deinit();
    var arena = std.heap.ArenaAllocator.init(scratch.allocator());
    defer arena.deinit();
    const prepared = prepare(arena.allocator(), view, loaded, provenance, limits, control) catch |err| return scratch.mapError(err);
    scratch.resetFailure();
    var result = execute(scratch.allocator(), io, view, prepared, output, provenance, limits, control) catch |err| return scratch.mapError(err);
    result.peak_scratch_bytes = fixed_scratch_bytes + scratch.budget.peak;
    return result;
}

fn validateSource(source: View, control: ?Control) !void {
    if (source.identity.precision != .fp32 or source.identity.weight.size_bytes == 0) return error.InvalidBoundaryMergeSource;
    const specs = policy.specs(source.identity.backbone);
    if (source.parameters.len != specs.len) return error.InvalidBoundaryMergeSource;
    for (source.parameters, specs) |parameter, spec| {
        try check(control);
        const runtime_name = if (std.mem.startsWith(u8, spec.name, "encoder.")) spec.name[8..] else spec.name;
        if (parameter.kind != .original or !std.mem.eql(u8, parameter.canonical_name, spec.name) or
            !std.mem.eql(u8, parameter.name, runtime_name) or parameter.dimensions.len != spec.shape.len)
            return error.InvalidBoundaryMergeSource;
        for (parameter.dimensions, spec.shape) |actual, expected| if (actual != expected) return error.InvalidBoundaryMergeSource;
        if (parameter.values.len != try elements(parameter.dimensions)) return error.InvalidBoundaryMergeSource;
    }
    for (source.sidecars, source.identity.sidecars) |bytes, pin| {
        if (bytes.len == 0) return error.InvalidBoundaryMergeSource;
        try verifyBytes(bytes, pin, control);
    }
}

fn prepare(a: Allocator, source: View, loaded: *const adapter.Loaded, provenance: Provenance, limits: Limits, control: ?Control) !Prepared {
    try check(control);
    if (provenance.configuration.size_bytes == 0 or !std.meta.eql(source.identity, loaded.receipt.source) or
        !std.meta.eql(provenance.schema_sha256, loaded.receipt.schema_sha256) or
        !std.meta.eql(provenance.adapter_files.config, loaded.receipt.config) or
        !std.meta.eql(provenance.adapter_files.weights, loaded.receipt.weights) or
        !std.meta.eql(provenance.adapter_files.receipt, loaded.receipt_digest)) return error.BoundaryMergeIdentityMismatch;
    // Reconstruct the actual module descriptors from owned config/tensor
    // bytes. A caller-mutated cached modules/config view cannot bypass checks.
    const validated = try adapter.validateLoaded(a, loaded, limits.adapter, control);
    const tensors = try a.alloc(Tensor, source.parameters.len);
    var merged_count: usize = 0;
    var largest: usize = 0;
    var payload: u64 = 0;
    for (source.parameters, tensors) |parameter, *tensor| {
        try check(control);
        const shape = try a.alloc(i64, parameter.dimensions.len);
        for (parameter.dimensions, shape) |dim, *value| value.* = dim;
        const byte_len = try mul(parameter.values.len, 4);
        var module: ?adapter.Module = null;
        for (validated.modules) |candidate| if (std.mem.eql(u8, candidate.target.base_weight, parameter.canonical_name)) {
            if (module != null or shape.len != 2 or shape[0] != candidate.target.out_dim or shape[1] != candidate.target.in_dim or
                !std.mem.endsWith(u8, parameter.canonical_name, ".weight")) return error.InvalidBoundaryMergeSource;
            if (byte_len > limits.adapter.max_merge_tensor_bytes) return error.BoundaryMergeMemoryLimitExceeded;
            module = candidate;
            merged_count += 1;
            largest = @max(largest, byte_len);
        };
        tensor.* = .{ .descriptor = .{ .name = parameter.canonical_name, .shape = shape, .encoding = .{ .dense = .f32 }, .byte_len = byte_len, .quantized = false }, .base = parameter.values, .module = module };
        payload = try add(payload, byte_len);
    }
    if (merged_count != validated.modules.len or merged_count == 0) return error.BoundaryMergeIdentityMismatch;
    if (try headerBound(tensors) > limits.max_header_bytes) return error.BoundaryMergeOutputLimitExceeded;
    std.mem.sort(Tensor, tensors, {}, struct {
        fn lessThan(_: void, left: Tensor, right: Tensor) bool {
            return std.mem.lessThan(u8, left.descriptor.name, right.descriptor.name);
        }
    }.lessThan);
    var output_bytes = try add(payload, try add(limits.max_header_bytes, limits.max_receipt_bytes));
    output_bytes = try add(output_bytes, 8 + 64 * 1024);
    for (source.identity.sidecars) |sidecar| output_bytes = try add(output_bytes, sidecar.size_bytes);
    if (output_bytes > limits.max_output_bytes) return error.BoundaryMergeOutputLimitExceeded;
    if (try add(largest, fixed_scratch_bytes) >= limits.max_scratch_bytes) return error.BoundaryMergeMemoryLimitExceeded;
    return .{ .tensors = tensors, .target_sha256 = loaded.receipt.target_sha256, .parameter_sha256 = loaded.receipt.parameter_sha256, .estimate = .{
        .output_bytes_upper_bound = output_bytes,
        .tensor_payload_bytes = payload,
        .largest_adapted_tensor_bytes = largest,
        .reserved_scratch_bytes = limits.max_scratch_bytes,
        .tensor_count = tensors.len,
        .merged_tensor_count = merged_count,
    } };
}

const Access = struct {
    tensors: []const Tensor,
    merged: bool,
    limits: adapter.Limits,
    control: ?Control,

    fn interface(self: *Access) access_mod.TensorAccess {
        return .{ .ptr = self, .vtable = &.{ .getRecord = getRecord, .listNames = listNames, .deinit = deinit } };
    }
    fn getRecord(raw: *anyopaque, a: Allocator, name: []const u8) !access_mod.Record {
        const self: *Access = @ptrCast(@alignCast(raw));
        try check(self.control);
        for (self.tensors) |tensor| if (std.mem.eql(u8, tensor.descriptor.name, name)) {
            if (self.merged) if (tensor.module) |module| {
                const bytes = try a.alloc(u8, tensor.descriptor.byte_len);
                errdefer a.free(bytes);
                // Record frees bytes with their original alignment. The
                // scalar merge accepts an explicitly unaligned destination.
                try adapter.mergeInto(module, tensor.base, std.mem.bytesAsSlice(f32, bytes), self.limits, self.control);
                return .{ .descriptor = tensor.descriptor, .raw_bytes = bytes, .allocator = a, .owns_bytes = true };
            };
            return .{ .descriptor = tensor.descriptor, .raw_bytes = std.mem.sliceAsBytes(tensor.base) };
        };
        return error.InvalidBoundaryMergeSource;
    }
    fn listNames(raw: *anyopaque, a: Allocator) ![][]const u8 {
        const self: *Access = @ptrCast(@alignCast(raw));
        const names = try a.alloc([]const u8, self.tensors.len);
        for (self.tensors, names) |tensor, *name| name.* = tensor.descriptor.name;
        return names;
    }
    fn deinit(_: *anyopaque) void {}
};

fn execute(a: Allocator, io: std.Io, source: View, prepared: Prepared, output: []const u8, provenance: Provenance, limits: Limits, control: ?Control) !Result {
    try check(control);
    try validateDestination(io, output);
    // This writer preserves original little-endian tensor bytes. A future
    // big-endian profile must implement explicit streaming endian conversion.
    if (builtin.target.cpu.arch.endian() != .little) return error.UnsupportedBoundaryMergeEndianness;
    if (try headerBound(prepared.tensors) > limits.max_header_bytes) return error.BoundaryMergeOutputLimitExceeded;
    for (prepared.tensors) |tensor| try finite(tensor.base, control);
    for (source.sidecars, source.identity.sidecars) |bytes, pin| try verifyBytes(bytes, pin, control);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const paths = arena.allocator();
    const parent = std.fs.path.dirname(output) orelse ".";
    try publication.syncDirectory(io, parent);
    var random: [16]u8 = undefined;
    try std.Io.randomSecure(io, &random);
    const stage = try std.fs.path.join(paths, &.{ parent, try std.fmt.allocPrint(paths, ".gliner25-merge-{s}", .{std.fmt.bytesToHex(random, .lower)}) });
    const cwd = std.Io.Dir.cwd();
    try cwd.createDir(io, stage, .fromMode(0o700));
    defer publication.cleanupPrivateTree(io, stage);
    const payload = try std.fs.path.join(paths, &.{ stage, "payload" });
    try cwd.createDir(io, payload, .fromMode(0o700));
    const weight_path = try std.fs.path.join(paths, &.{ payload, "model.safetensors" });
    var metadata = Access{ .tensors = prepared.tensors, .merged = false, .limits = limits.adapter, .control = control };
    var values = metadata;
    values.merged = true;
    try writer.exportAccessWithMetadata(a, metadata.interface(), values.interface(), weight_path, control);
    const weights = try verifyWeights(a, io, weight_path, &values, limits, control);
    var output_bytes = weights.size_bytes;
    for (bundle.sidecar_names, source.sidecars, source.identity.sidecars) |name, bytes, pin| {
        const digest = try writeBytes(a, io, payload, name, bytes, control);
        if (!std.meta.eql(digest, pin)) return error.BoundaryMergeIdentityMismatch;
        _ = try verifyFileBytes(a, io, payload, name, bytes, control);
        output_bytes = try add(output_bytes, digest.size_bytes);
    }
    try publication.syncDirectory(io, try std.fs.path.join(paths, &.{ payload, "encoder_config" }));
    const identity = bundle.Identity{ .backbone = source.identity.backbone, .precision = .fp32, .weight = weights, .sidecars = source.identity.sidecars };
    const receipt = Receipt{ .source = source.identity, .provenance = provenance, .target_sha256 = prepared.target_sha256, .parameter_sha256 = prepared.parameter_sha256, .merged = identity, .tensor_count = prepared.tensors.len, .merged_tensor_count = prepared.estimate.merged_tensor_count };
    const receipt_json = try std.json.Stringify.valueAlloc(a, receipt, .{ .whitespace = .indent_2 });
    defer a.free(receipt_json);
    if (receipt_json.len > limits.max_receipt_bytes) return error.BoundaryMergeOutputLimitExceeded;
    const receipt_digest = try writeBytes(a, io, payload, receipt_name, receipt_json, control);
    _ = try verifyFileBytes(a, io, payload, receipt_name, receipt_json, control);
    output_bytes = try add(output_bytes, receipt_digest.size_bytes);
    if (output_bytes > prepared.estimate.output_bytes_upper_bound or output_bytes > limits.max_output_bytes) return error.BoundaryMergeOutputLimitExceeded;
    try publication.syncDirectory(io, payload);
    try check(control);
    try publication.publishDirectory(a, io, payload, output);
    publication.syncDirectory(io, parent) catch return error.BoundaryMergePublishedDurabilityUnconfirmed;
    return .{ .identity = identity, .receipt = receipt_digest, .output_bytes = output_bytes, .peak_scratch_bytes = 0, .tensor_count = prepared.tensors.len, .merged_tensor_count = prepared.estimate.merged_tensor_count };
}

fn verifyWeights(a: Allocator, io: std.Io, path: []const u8, access: *Access, limits: Limits, control: ?Control) !bundle.Digest {
    const file = try files.openRegular(io, std.Io.Dir.cwd(), path, control);
    defer file.close(io);
    const initial = try file.stat(io);
    if (initial.size < 8 or initial.size > limits.max_output_bytes) return error.BoundaryMergeOutputLimitExceeded;
    var prefix: [8]u8 = undefined;
    try readAt(file, io, &prefix, 0, control);
    const header_size = std.mem.readInt(u64, &prefix, .little);
    if (header_size == 0 or header_size > limits.max_header_bytes or header_size > initial.size - 8) return error.BoundaryMergeOutputLimitExceeded;
    const header = try a.alloc(u8, @intCast(header_size));
    defer a.free(header);
    try readAt(file, io, header, 8, control);
    try jsonDepth(header, 8);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, header, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const value = parsed.value;
    if (value != .object or value.object.count() != access.tensors.len + 1) return error.BoundaryMergeOutputMismatch;
    const metadata = value.object.get("__metadata__") orelse return error.BoundaryMergeOutputMismatch;
    if (metadata != .object or metadata.object.count() != 1 or !jsonString(metadata.object.get("format"), "antfly-inference")) return error.BoundaryMergeOutputMismatch;
    var payload: u64 = 0;
    for (access.tensors) |tensor| {
        try check(control);
        const raw = value.object.get(tensor.descriptor.name) orelse return error.BoundaryMergeOutputMismatch;
        if (raw != .object or raw.object.count() != 3 or !jsonString(raw.object.get("dtype"), "F32")) return error.BoundaryMergeOutputMismatch;
        const shape = raw.object.get("shape") orelse return error.BoundaryMergeOutputMismatch;
        if (shape != .array or shape.array.items.len != tensor.descriptor.shape.len) return error.BoundaryMergeOutputMismatch;
        for (shape.array.items, tensor.descriptor.shape) |dim, expected| if (try jsonUnsigned(dim) != @as(u64, @intCast(expected))) return error.BoundaryMergeOutputMismatch;
        const offsets = raw.object.get("data_offsets") orelse return error.BoundaryMergeOutputMismatch;
        if (offsets != .array or offsets.array.items.len != 2 or try jsonUnsigned(offsets.array.items[0]) != payload) return error.BoundaryMergeOutputMismatch;
        payload = try add(payload, tensor.descriptor.byte_len);
        if (try jsonUnsigned(offsets.array.items[1]) != payload) return error.BoundaryMergeOutputMismatch;
    }
    if (try add(header_size + 8, payload) != initial.size) return error.BoundaryMergeOutputMismatch;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&prefix);
    hash.update(header);
    var buffer: [256 * 1024]u8 = undefined;
    var position = header_size + 8;
    for (access.tensors) |tensor| {
        // A second independent merge is live only for this tensor. Untouched
        // tensors and all biases are compared with immutable original bytes.
        var record = try access.interface().getRecord(a, tensor.descriptor.name);
        defer record.deinit();
        var offset: usize = 0;
        while (offset < record.raw_bytes.len) {
            const end = @min(record.raw_bytes.len, offset +| buffer.len);
            const bytes = buffer[0 .. end - offset];
            try readAt(file, io, bytes, position, control);
            if (!std.mem.eql(u8, bytes, record.raw_bytes[offset..end])) return error.BoundaryMergeOutputMismatch;
            hash.update(bytes);
            position += bytes.len;
            offset = end;
        }
    }
    try unchanged(file, io, initial);
    try check(control);
    return .{ .size_bytes = initial.size, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}

fn writeBytes(a: Allocator, io: std.Io, directory: []const u8, name: []const u8, bytes: []const u8, control: ?Control) !bundle.Digest {
    try check(control);
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io, parent);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
    defer file.close(io);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| 256 * 1024);
        try file.writeStreamingAll(io, bytes[offset..end]);
        hash.update(bytes[offset..end]);
        offset = end;
    }
    try file.sync(io);
    try check(control);
    return .{ .size_bytes = bytes.len, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}
fn verifyFileBytes(a: Allocator, io: std.Io, directory: []const u8, name: []const u8, expected: []const u8, control: ?Control) !bundle.Digest {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const file = try files.openRegular(io, std.Io.Dir.cwd(), path, control);
    defer file.close(io);
    const initial = try file.stat(io);
    if (initial.size != expected.len) return error.BoundaryMergeOutputMismatch;
    var buffer: [256 * 1024]u8 = undefined;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < expected.len) {
        const end = @min(expected.len, offset +| buffer.len);
        const bytes = buffer[0 .. end - offset];
        try readAt(file, io, bytes, offset, control);
        if (!std.mem.eql(u8, bytes, expected[offset..end])) return error.BoundaryMergeOutputMismatch;
        hash.update(bytes);
        offset = end;
    }
    try unchanged(file, io, initial);
    return .{ .size_bytes = initial.size, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}
fn unchanged(file: std.Io.File, io: std.Io, initial: std.Io.File.Stat) !void {
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, initial.size) != 0) return error.BoundaryMergeOutputMismatch;
    const final = try file.stat(io);
    if (final.kind != .file or final.size != initial.size or final.inode != initial.inode or !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime)) return error.BoundaryMergeOutputMismatch;
}
fn readAt(file: std.Io.File, io: std.Io, bytes: []u8, position: u64, control: ?Control) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| 256 * 1024);
        if (try file.readPositionalAll(io, bytes[offset..end], try add(position, offset)) != end - offset) return error.BoundaryMergeOutputMismatch;
        offset = end;
    }
}
pub fn validateLimits(limits: Limits) !void {
    if (limits.max_scratch_bytes <= fixed_scratch_bytes or limits.max_scratch_bytes > 2 * 1024 * mib or
        limits.max_output_bytes == 0 or limits.max_output_bytes > 16 * 1024 * mib or
        limits.max_header_bytes == 0 or limits.max_header_bytes > 64 * mib or limits.max_header_bytes > limits.max_scratch_bytes or
        limits.max_receipt_bytes == 0 or limits.max_receipt_bytes > mib or limits.max_receipt_bytes > limits.max_scratch_bytes or
        limits.adapter.max_config_bytes == 0 or limits.adapter.max_config_bytes > mib or limits.adapter.max_receipt_bytes == 0 or limits.adapter.max_receipt_bytes > mib or
        limits.adapter.max_tensor_bytes == 0 or limits.adapter.max_tensor_bytes > 2 * 1024 * mib or limits.adapter.max_tensor_header_bytes == 0 or limits.adapter.max_tensor_header_bytes > 64 * mib or
        limits.adapter.max_targets == 0 or limits.adapter.max_targets > 4096 or limits.adapter.max_rank == 0 or limits.adapter.max_rank > 1024 or
        limits.adapter.max_merge_tensor_bytes == 0 or limits.adapter.max_merge_tensor_bytes > 1024 * mib) return error.InvalidBoundaryMergeLimits;
}
pub fn validateDestination(io: std.Io, output: []const u8) !void {
    const base = std.fs.path.basename(output);
    if (output.len == 0 or output.len > 4096 or std.mem.indexOfScalar(u8, output, 0) != null or base.len == 0 or
        std.mem.eql(u8, base, ".") or std.mem.eql(u8, base, "..") or std.mem.eql(u8, output, "/")) return error.InvalidOutputPath;
    if (std.Io.Dir.cwd().access(io, output, .{})) |_| return error.PathAlreadyExists else |err| if (err != error.FileNotFound) return err;
}
pub fn verifyBytes(bytes: []const u8, expected: bundle.Digest, control: ?Control) !void {
    if (bytes.len != expected.size_bytes) return error.BoundaryMergeIdentityMismatch;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| 256 * 1024);
        hash.update(bytes[offset..end]);
        offset = end;
    }
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower), &expected.sha256)) return error.BoundaryMergeIdentityMismatch;
}
pub fn jsonDepth(bytes: []const u8, maximum: usize) !void {
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes) |byte| {
        if (quoted) {
            if (escaped) escaped = false else if (byte == '\\') escaped = true else if (byte == '"') quoted = false;
        } else switch (byte) {
            '"' => quoted = true,
            '{', '[' => {
                depth += 1;
                if (depth > maximum) return error.BoundaryMergeJsonLimitExceeded;
            },
            '}', ']' => {
                if (depth == 0) return error.BoundaryMergeOutputMismatch;
                depth -= 1;
            },
            else => {},
        }
    }
    if (depth != 0 or quoted) return error.BoundaryMergeOutputMismatch;
}
fn jsonString(value: ?std.json.Value, wanted: []const u8) bool {
    const actual = value orelse return false;
    return actual == .string and std.mem.eql(u8, actual.string, wanted);
}
fn jsonUnsigned(value: std.json.Value) !u64 {
    if (value != .integer or value.integer < 0) return error.BoundaryMergeOutputMismatch;
    return @intCast(value.integer);
}
fn finite(values: []const f32, control: ?Control) !void {
    for (values, 0..) |value, index| {
        if (index % 65536 == 0) try check(control);
        if (!std.math.isFinite(value)) return error.NonFiniteBoundaryAdapterTensor;
    }
}
fn elements(dimensions: []const i32) !usize {
    if (dimensions.len == 0 or dimensions.len > 4) return error.InvalidBoundaryMergeSource;
    var count: usize = 1;
    for (dimensions) |dim| {
        if (dim <= 0) return error.InvalidBoundaryMergeSource;
        count = try mul(count, @intCast(dim));
    }
    return count;
}
fn headerBound(tensors: []const Tensor) !u64 {
    // Worst-case JSON escaping, integer digits, punctuation and final8-byte
    // padding. Validate before opening staging, not only when reading output.
    var bytes: u64 = 80;
    for (tensors) |tensor| bytes = try add(bytes, try add(try mul(tensor.descriptor.name.len, 6), try add(128, try mul(tensor.descriptor.shape.len, 22))));
    return bytes;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn add(left: u64, right: u64) !u64 {
    return std.math.add(u64, left, right) catch error.BoundaryMergeOutputLimitExceeded;
}
fn mul(left: usize, right: usize) !usize {
    return std.math.mul(usize, left, right) catch error.BoundaryMergeMemoryLimitExceeded;
}

// Private synthetic views exercise publication without weakening the public
// Source-only complete334 admission or allocating a published checkpoint.
const TestFixture = struct {
    const bias = [_]f32{ -0.0, 0.75 };
    const base = [_]f32{ 0.2, -0.4, 0.7, 0.1, -0.3, 0.9 };
    const frozen = [_]f32{ 1, -0.0, -2, 4 };
    const left = [_]f32{ 0.1, 0.3, -0.2, -0.1, 0.4, 0.2 };
    const right = [_]f32{ 0.2, -0.4, 0.3, 0.1 };
    const magnitude = [_]f32{ 0.8, 1.2 };
    tensors: [3]Tensor,

    fn init(dora: bool) TestFixture {
        const target = adapter.Target{ .module = "boundary_head.count_head", .base_weight = "boundary_head.count_head.weight", .in_dim = 3, .out_dim = 2, .a_key = "tiny.A", .b_key = "tiny.B", .magnitude_key = if (dora) "tiny.m" else null };
        return .{ .tensors = .{
            .{ .descriptor = .{ .name = "boundary_head.count_head.bias", .shape = &.{2}, .encoding = .{ .dense = .f32 }, .byte_len = 8, .quantized = false }, .base = &bias, .module = null },
            .{ .descriptor = .{ .name = target.base_weight, .shape = &.{ 2, 3 }, .encoding = .{ .dense = .f32 }, .byte_len = 24, .quantized = false }, .base = &base, .module = .{ .target = target, .rank = 2, .scale = 1.5, .a = &left, .b = &right, .magnitude = if (dora) &magnitude else null } },
            .{ .descriptor = .{ .name = "encoder.encoder.weight", .shape = &.{ 2, 2 }, .encoding = .{ .dense = .f32 }, .byte_len = 16, .quantized = false }, .base = &frozen, .module = null },
        } };
    }
    fn view() View {
        const sidecars = [_][]const u8{ "{\"boundary\":1}", "{\"encoder\":1}", "{\"tokenizer\":1}", "{\"normalizer\":1}" };
        var pins: [4]bundle.Digest = undefined;
        for (sidecars, &pins) |bytes, *pin| pin.* = bundle.Digest.of(bytes);
        return .{ .identity = .{ .backbone = .small, .precision = .fp32, .weight = bundle.Digest.of("immutable original source"), .sidecars = pins }, .parameters = &.{}, .sidecars = sidecars };
    }
    fn provenance() Provenance {
        return .{ .configuration = bundle.Digest.of("exact merge job bytes"), .adapter_files = .{ .config = bundle.Digest.of("exact adapter config"), .weights = bundle.Digest.of("exact adapter weights"), .receipt = null }, .schema_sha256 = @splat(17) };
    }
    fn prepared(self: *TestFixture) Prepared {
        return .{ .tensors = &self.tensors, .target_sha256 = @splat(11), .parameter_sha256 = @splat(13), .estimate = .{ .output_bytes_upper_bound = 128 * 1024, .tensor_payload_bytes = 48, .largest_adapted_tensor_bytes = 24, .reserved_scratch_bytes = 2 * mib, .tensor_count = self.tensors.len, .merged_tensor_count = 1 } };
    }
};

fn testExecute(a: Allocator, output: []const u8, dora: bool, control: ?Control) !Result {
    var fixture = TestFixture.init(dora);
    var scratch = Scratch.init(a, 2 * mib);
    defer scratch.deinit();
    var result = execute(scratch.allocator(), std.testing.io, TestFixture.view(), fixture.prepared(), output, TestFixture.provenance(), .{}, control) catch |err| return scratch.mapError(err);
    result.peak_scratch_bytes = scratch.budget.peak;
    return result;
}

test "boundary immutable merge streams LoRA DoRA preserves frozen bytes and never overwrites" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const safetensors = @import("../models/safetensors.zig");
    for ([_]bool{ false, true }) |dora| {
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
        defer a.free(parent);
        const output = try std.fs.path.join(a, &.{ parent, "merged" });
        defer a.free(output);
        const result = try testExecute(a, output, dora, null);
        try std.testing.expectEqual(@as(usize, 3), result.tensor_count);
        try std.testing.expectEqual(@as(usize, 1), result.merged_tensor_count);
        try std.testing.expect(result.peak_scratch_bytes < 2 * mib);
        const weight_bytes = try files.read(a, io, temporary.dir, "merged/model.safetensors", 32 * 1024, null);
        defer a.free(weight_bytes);
        try std.testing.expectEqual(bundle.Digest.of(weight_bytes), result.identity.weight);
        var reader = try safetensors.MMapReader.fromBytes(a, weight_bytes);
        defer reader.header.deinit();
        try safetensors.validateReader(a, &reader);
        const fixture = TestFixture.init(dora);
        for (fixture.tensors) |tensor| {
            const meta = reader.header.tensors.get(tensor.descriptor.name).?;
            const bytes = weight_bytes[@intCast(reader.data_offset + meta.data_start)..@intCast(reader.data_offset + meta.data_end)];
            if (tensor.module == null) {
                try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(tensor.base), bytes);
            } else for (0..2) |row| {
                var independent: [3]f64 = undefined;
                var squared: f64 = 0;
                for (0..3) |column| {
                    independent[column] = @as(f64, TestFixture.base[row * 3 + column]) + 1.5 * (@as(f64, TestFixture.right[row * 2]) * TestFixture.left[column] + @as(f64, TestFixture.right[row * 2 + 1]) * TestFixture.left[3 + column]);
                    squared += independent[column] * independent[column];
                }
                for (0..3) |column| {
                    const expected = independent[column] * (if (dora) @as(f64, TestFixture.magnitude[row]) / @sqrt(squared) else 1);
                    const actual: f32 = @bitCast(std.mem.readInt(u32, bytes[(row * 3 + column) * 4 ..][0..4], .little));
                    try std.testing.expectApproxEqAbs(expected, @as(f64, actual), 2e-7);
                }
            }
        }
        for (bundle.sidecar_names, TestFixture.view().sidecars) |name, bytes| _ = try verifyFileBytes(a, io, output, name, bytes, null);
        const raw_receipt = try files.read(a, io, temporary.dir, "merged/" ++ receipt_name, 64 * 1024, null);
        defer a.free(raw_receipt);
        try std.testing.expectEqual(bundle.Digest.of(raw_receipt), result.receipt);
        const parsed = try std.json.parseFromSlice(Receipt, a, raw_receipt, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(TestFixture.provenance(), parsed.value.provenance);
        try std.testing.expectEqual(result.identity, parsed.value.merged);
        try std.testing.expectError(error.PathAlreadyExists, testExecute(a, output, dora, null));
        try std.testing.expectEqual(result.identity.weight, try verifyFileBytes(a, io, output, "model.safetensors", weight_bytes, null));
        // Synthetic inventory is intentionally rejected at the public Source
        // admission boundary; publication tests cannot admit an incomplete model.
        try std.testing.expectError(error.InvalidBoundaryMergeSource, validateSource(TestFixture.view(), null));
    }
}

fn testAllocationFailures(a: Allocator, output: []const u8) !void {
    // Output cleanup belongs to this test, after successful publication only.
    // Production error cleanup never traverses a published output directory.
    const result = try testExecute(a, output, true, null);
    _ = result;
    try std.Io.Dir.cwd().deleteTree(std.testing.io, output);
}

test "boundary immutable merge cleans every allocation failure and cancelled publication" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const output = try std.fs.path.join(a, &.{ parent, "merged" });
    defer a.free(output);
    try std.testing.checkAllAllocationFailures(a, testAllocationFailures, .{output});
    var iterator = temporary.dir.iterate();
    try std.testing.expect((try iterator.next(io)) == null);
    const Cancel = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn call(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.stop) return error.Cancelled;
        }
    };
    var count = Cancel{};
    _ = try testExecute(a, output, true, .{ .ptr = &count, .check_fn = Cancel.call });
    try std.Io.Dir.cwd().deleteTree(io, output);
    try std.testing.expect(count.calls > 30);
    for (1..count.calls + 1) |stop| {
        var cancel = Cancel{ .stop = stop };
        try std.testing.expectError(error.Cancelled, testExecute(a, output, true, .{ .ptr = &cancel, .check_fn = Cancel.call }));
        iterator = temporary.dir.iterate();
        try std.testing.expect((try iterator.next(io)) == null);
    }
}

test "boundary immutable merge rejects mutated tensors sidecars header and declared scratch" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const output = try std.fs.path.join(a, &.{ parent, "merged" });
    defer a.free(output);
    var fixture = TestFixture.init(true);
    var invalid = TestFixture.view();
    invalid.sidecars[0] = "same owner, changed config";
    try std.testing.expectError(error.BoundaryMergeIdentityMismatch, execute(a, io, invalid, fixture.prepared(), output, TestFixture.provenance(), .{}, null));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, output, .{}));
    _ = try testExecute(a, output, true, null);
    const path = try std.fs.path.join(a, &.{ output, "model.safetensors" });
    defer a.free(path);
    var access = Access{ .tensors = &fixture.tensors, .merged = true, .limits = .{}, .control = null };
    const original = try verifyWeights(a, io, path, &access, .{}, null);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    var byte: [1]u8 = undefined;
    try readAt(file, io, &byte, original.size_bytes - 1, null);
    byte[0] ^= 1;
    try file.writePositionalAll(io, &byte, original.size_bytes - 1);
    try std.testing.expectError(error.BoundaryMergeOutputMismatch, verifyWeights(a, io, path, &access, .{}, null));
    var scratch = Scratch.init(a, 1);
    defer scratch.deinit();
    try std.testing.expectError(error.OutOfMemory, scratch.allocator().alloc(u8, 2));
    try std.testing.expectEqual(error.BoundaryMergeMemoryLimitExceeded, scratch.mapError(error.OutOfMemory));
    try std.testing.expectEqual(error.Cancelled, scratch.mapError(error.Cancelled));
    var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var backing = Scratch.init(fail.allocator(), mib);
    defer backing.deinit();
    try std.testing.expectError(error.OutOfMemory, backing.allocator().alloc(u8, 2));
    try std.testing.expectEqual(error.OutOfMemory, backing.mapError(error.OutOfMemory));
}

test "boundary immutable merge protects private cleanup from pending Io cancellation" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const output = try std.fs.path.join(a, &.{ parent, "merged" });
    defer a.free(output);
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Context = struct {
        allocator: Allocator,
        io: std.Io,
        directory: std.Io.Dir,
        ready: std.Io.Event = .unset,
        never: std.Io.Event = .unset,
        injected: bool = false,

        fn checkPoint(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.injected) return;
            var iterator = self.directory.iterate();
            while (try iterator.next(self.io)) |entry| if (std.mem.startsWith(u8, entry.name, ".gliner25-merge-")) {
                self.injected = true;
                self.ready.set(self.io);
                self.never.wait(self.io) catch |err| switch (err) {
                    error.Canceled => {
                        // Put the real Io cancellation back before returning
                        // a distinct callback error. The defer must run with
                        // cancellation blocked rather than consume and skip it.
                        self.io.recancel();
                        return error.Cancelled;
                    },
                };
                return error.TestUnexpectedResult;
            };
        }
        fn run(self: *@This(), destination: []const u8) !void {
            // Even an early unrelated failure releases the test's waiter.
            defer self.ready.set(self.io);
            var fixture = TestFixture.init(true);
            var scratch = Scratch.init(self.allocator, 2 * mib);
            defer scratch.deinit();
            _ = execute(scratch.allocator(), self.io, TestFixture.view(), fixture.prepared(), destination, TestFixture.provenance(), .{}, .{ .ptr = self, .check_fn = checkPoint }) catch |err| return scratch.mapError(err);
        }
    };
    var context = Context{ .allocator = a, .io = io, .directory = temporary.dir };
    var future = try io.concurrent(Context.run, .{ &context, output });
    var joined = false;
    defer if (!joined) {
        _ = future.cancel(io) catch {};
    };
    try context.ready.wait(io);
    const result = future.cancel(io);
    joined = true;
    try std.testing.expectError(error.Cancelled, result);
    try std.testing.expect(context.injected);
    var iterator = temporary.dir.iterate();
    try std.testing.expect((try iterator.next(std.testing.io)) == null);
}
