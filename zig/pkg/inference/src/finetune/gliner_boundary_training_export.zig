// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Portable final snapshots from an immutable original source and one locked
//! optimizer epoch. The caller holds its training-owner busy lock until return.
//! Optimizer state remains a separate checkpoint; an unfinished accumulation
//! window cannot be exported as a final model. No source path is reopened.
//!
//! Full/head snapshots stream all original canonical tensors, with exactly the
//! selected optimizer slots overlaid. PEFT snapshots use the complete run-level
//! layout, including adapters unused by the last batch. All files and training
//! provenance are verified and synced before an atomic, no-overwrite rename.
//! Receipts record integrity and provenance, never model quality qualification.
const std = @import("std");
const builtin = @import("builtin");
const source_mod = @import("gliner_boundary_training_source.zig");
const run = @import("gliner_boundary_run.zig");
const optimizer = @import("seeded_gradient_trainer.zig");
const adapter = @import("gliner_boundary_adapter.zig");
const layouts = @import("gliner_boundary_adapter_layout.zig");
const checkpoint = @import("safetensors_checkpoint.zig");
const model = @import("../models/gliner_boundary.zig");
const policy = @import("../models/gliner_boundary_artifact.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const publication = @import("../gliner_boundary_export.zig");
const files = @import("../runtime/file_snapshot.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const fixed_scratch_bytes = 512 * 1024;

pub const receipt_name = "antfly_gliner25_training.json";
pub const family = "gliner_boundary_training_snapshot/v1";
pub const Limits = struct {
    /// Aggregate metadata, validation copies made by the PEFT importer, and
    /// fixed streaming scratch. The backing allocator must reclaim frees.
    max_scratch_bytes: usize = 64 * mib,
    max_output_bytes: u64 = 2 * 1024 * mib,
    max_header_bytes: usize = 4 * mib,
    max_config_bytes: usize = 128 * 1024,
    max_receipt_bytes: usize = 64 * 1024,
    max_adapter_bytes: usize = 512 * mib,
    max_slots: usize = 4096,
};
pub const Slot = struct { name: []const u8, dimensions: []const i32, values: []const f32 };
pub const Provenance = struct {
    run_fingerprint: [32]u8,
    dataset_sha256: [32]u8,
    schemas_sha256: [32]u8,
    optimizer_identity: optimizer.Identity,
    accumulated_microbatches: u32,
};
pub const Snapshot = struct {
    mode: run.Mode,
    slots: []const Slot,
    adapter_layout: ?*const layouts.Layout = null,
    provenance: Provenance,
    /// Optional locator of the full original extractor, not its DeBERTa
    /// encoder. Null is preserved in standard PEFT metadata when unavailable.
    base_model_name_or_path: ?[]const u8 = null,
};
pub const Estimate = struct {
    /// Includes headers, all output sidecars/receipts, and directory allowance.
    /// Staging is renamed in place; no second model payload is created.
    output_bytes_upper_bound: u64,
    reserved_scratch_bytes: usize,
    tensor_payload_bytes: u64,
    tensor_count: usize,
};
pub const Receipt = struct {
    family: []const u8 = family,
    version: u32 = 1,
    architecture_version: u32 = model.architecture_version,
    config_version: u32 = model.config_version,
    tensor_policy_version: u32 = policy.policy_version,
    mode: run.Mode,
    source: bundle.Identity,
    provenance: Provenance,
    adapter_layout_sha256: ?[32]u8,
    weights: bundle.Digest,
    /// Full/head outputs preserve these exact original sidecars. Adapter
    /// outputs use standard PEFT config and the separately bound source.
    sidecars: ?[4]bundle.Digest,
    adapter_config: ?bundle.Digest,
    adapter_receipt: ?bundle.Digest,
};
pub const Result = struct {
    mode: run.Mode,
    weights: bundle.Digest,
    provenance: bundle.Digest,
    output_bytes: u64,
    peak_scratch_bytes: usize,
};

const View = struct {
    identity: bundle.Identity,
    parameters: []const run.Parameter,
    sidecars: [4][]const u8,

    fn of(source: *const source_mod.Source) !View {
        if (source.config.backbone != source.identity.backbone) return error.InvalidBoundaryTrainingSource;
        var result = View{ .identity = source.identity, .parameters = source.parameters, .sidecars = undefined };
        for (&result.sidecars, 0..) |*bytes, index| bytes.* = try source.sidecar(index);
        try validateSourceMetadata(result, null);
        return result;
    }
};
const Prepared = struct {
    tensors: []checkpoint.NamedTensor,
    config_bytes: ?[]const u8,
    layout_fingerprint: ?[32]u8,
    estimate: Estimate,
};

// Allocating JSON writers can report WriteFailed after allocation denial.
// Record actual allocation failures so only that case maps back to OOM; disk
// write failures must retain their original I/O error.
const AllocationGate = struct {
    budget: *Budget,
    terminal: ?Budget.AllocationFailure = null,

    fn allocator(self: *@This()) Allocator {
        self.budget.failure_context = self;
        self.budget.allocation_failed = observe;
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn observe(raw: ?*anyopaque, failure: Budget.AllocationFailure) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.terminal = failure;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.budget.allocator().rawAlloc(len, alignment, ret);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.budget.allocator().rawResize(bytes, alignment, len, ret);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.budget.allocator().rawRemap(bytes, alignment, len, ret);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.budget.allocator().rawFree(bytes, alignment, ret);
    }
    fn mapError(self: *@This(), err: anyerror) anyerror {
        if (err != error.OutOfMemory and err != error.WriteFailed) return err;
        const terminal = self.terminal orelse return err;
        return if (terminal.kind == .declared_limit) error.BoundaryTrainingExportLimitExceeded else error.OutOfMemory;
    }
};

/// Bounded read-only metadata preflight. This validates complete slot names,
/// shapes and the run-level layout; export additionally scans every FP32 value.
pub fn estimateSnapshot(a: Allocator, source: *const source_mod.Source, snapshot: Snapshot, limits: Limits, control: ?Control) !Estimate {
    return estimateView(a, try View.of(source), snapshot, limits, control);
}

fn estimateView(a: Allocator, source: View, snapshot: Snapshot, limits: Limits, control: ?Control) !Estimate {
    try validateLimits(limits);
    var budget = Budget{ .backing = a, .limit = limits.max_scratch_bytes - fixed_scratch_bytes };
    defer std.debug.assert(budget.live == 0);
    var gate = AllocationGate{ .budget = &budget };
    var arena = std.heap.ArenaAllocator.init(gate.allocator());
    defer arena.deinit();
    const prepared = prepare(arena.allocator(), source, snapshot, limits, control) catch |err| return gate.mapError(err);
    return prepared.estimate;
}

pub fn exportSnapshot(a: Allocator, io: std.Io, source: *const source_mod.Source, output: []const u8, snapshot: Snapshot, limits: Limits, control: ?Control) !Result {
    return exportView(a, io, try View.of(source), output, snapshot, limits, control);
}

fn exportView(a: Allocator, io: std.Io, source: View, output: []const u8, snapshot: Snapshot, limits: Limits, control: ?Control) !Result {
    try validateLimits(limits);
    try check(control);
    try validateDestination(io, output);
    var budget = Budget{ .backing = a, .limit = limits.max_scratch_bytes - fixed_scratch_bytes };
    defer std.debug.assert(budget.live == 0);
    var gate = AllocationGate{ .budget = &budget };
    var result = execute(gate.allocator(), io, source, output, snapshot, limits, control) catch |err| return gate.mapError(err);
    result.peak_scratch_bytes = fixed_scratch_bytes + budget.peak;
    return result;
}

fn execute(a: Allocator, io: std.Io, source: View, output: []const u8, snapshot: Snapshot, limits: Limits, control: ?Control) !Result {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const prepared = try prepare(scratch, source, snapshot, limits, control);
    // All semantic failures are detected before creating any staging files.
    for (prepared.tensors) |tensor| try finite(tensor.data, control);
    for (source.sidecars, source.identity.sidecars) |bytes, pin| try verifyBytes(bytes, pin, control);
    const parent = std.fs.path.dirname(output) orelse ".";
    try publication.syncDirectory(io, parent);
    var random: [16]u8 = undefined;
    try std.Io.randomSecure(io, &random);
    const staging_name = try std.fmt.allocPrint(scratch, ".gliner25-training-{s}", .{std.fmt.bytesToHex(random, .lower)});
    const stage = try std.fs.path.join(scratch, &.{ parent, staging_name });
    const cwd = std.Io.Dir.cwd();
    try cwd.createDir(io, stage, .fromMode(0o700));
    // The outer directory always remains private. Only its completed payload
    // directory is published, and cleanup never traverses that destination.
    defer publication.cleanupPrivateTree(io, stage);
    const payload = try std.fs.path.join(scratch, &.{ stage, "payload" });
    var output_bytes: u64 = 0;
    var adapter_config: ?bundle.Digest = null;
    var adapter_receipt: ?bundle.Digest = null;
    const weight_name = if (prepared.config_bytes != null) adapter.tensor_name else "model.safetensors";
    const weight_path = try std.fs.path.join(scratch, &.{ payload, weight_name });
    if (prepared.config_bytes) |config_bytes| {
        var exported = try adapter.exportDirectory(a, io, payload, config_bytes, prepared.tensors, try adapterBinding(source.identity, snapshot.provenance.schemas_sha256), .{
            .max_config_bytes = limits.max_config_bytes,
            .max_receipt_bytes = limits.max_receipt_bytes,
            .max_tensor_bytes = @intCast(@min(limits.max_output_bytes, try add(prepared.estimate.tensor_payload_bytes, limits.max_header_bytes + 8))),
            .max_tensor_header_bytes = limits.max_header_bytes,
            .max_targets = limits.max_slots / 2,
        }, control);
        defer exported.deinit();
        adapter_config = try verifyFileBytes(a, io, payload, adapter.config_name, config_bytes, limits.max_config_bytes, control);
        adapter_receipt = try verifyFileBytes(a, io, payload, adapter.receipt_name, exported.receipt_json, limits.max_receipt_bytes, control);
        output_bytes = try add(adapter_config.?.size_bytes, adapter_receipt.?.size_bytes);
    } else {
        try cwd.createDir(io, payload, .fromMode(0o700));
        try checkpoint.saveControlled(a, weight_path, prepared.tensors, control, true);
        for (bundle.sidecar_names, source.sidecars, source.identity.sidecars) |name, bytes, pin| {
            const actual = try writeBytes(a, io, payload, name, bytes, control);
            if (!std.meta.eql(actual, pin)) return error.BoundaryTrainingSnapshotMismatch;
            const verified = try verifyFileBytes(a, io, payload, name, bytes, bytes.len, control);
            if (!std.meta.eql(verified, pin)) return error.BoundaryTrainingSnapshotMismatch;
            output_bytes = try add(output_bytes, actual.size_bytes);
        }
        try publication.syncDirectory(io, try std.fs.path.join(scratch, &.{ payload, "encoder_config" }));
    }
    // Stream a second pass through the exact written payload, comparing every
    // byte with this locked epoch. No whole-model read or mmap is required.
    const weights = try verifyWeights(a, io, weight_path, prepared.tensors, limits, control);
    output_bytes = try add(output_bytes, weights.size_bytes);
    const receipt = Receipt{
        .mode = snapshot.mode,
        .source = source.identity,
        .provenance = snapshot.provenance,
        .adapter_layout_sha256 = prepared.layout_fingerprint,
        .weights = weights,
        .sidecars = if (prepared.config_bytes == null) source.identity.sidecars else null,
        .adapter_config = adapter_config,
        .adapter_receipt = adapter_receipt,
    };
    const receipt_json = try std.json.Stringify.valueAlloc(a, receipt, .{ .whitespace = .indent_2 });
    defer a.free(receipt_json);
    if (receipt_json.len > limits.max_receipt_bytes) return error.BoundaryTrainingExportLimitExceeded;
    const provenance = try writeBytes(a, io, payload, receipt_name, receipt_json, control);
    _ = try verifyFileBytes(a, io, payload, receipt_name, receipt_json, limits.max_receipt_bytes, control);
    output_bytes = try add(output_bytes, provenance.size_bytes);
    if (output_bytes > prepared.estimate.output_bytes_upper_bound or output_bytes > limits.max_output_bytes) return error.BoundaryTrainingExportLimitExceeded;
    try publication.syncDirectory(io, payload);
    try check(control);
    try publication.publishDirectory(a, io, payload, output);
    // A failure after publication leaves a complete artifact at output. The
    // caller can report the explicit durability ambiguity without deleting it.
    publication.syncDirectory(io, parent) catch return error.BoundaryTrainingExportPublishedDurabilityUnconfirmed;
    return .{ .mode = snapshot.mode, .weights = weights, .provenance = provenance, .output_bytes = output_bytes, .peak_scratch_bytes = 0 };
}

fn prepare(a: Allocator, source: View, snapshot: Snapshot, limits: Limits, control: ?Control) !Prepared {
    try check(control);
    if (source.identity.precision != .fp32) return error.UnsupportedBoundaryTrainingExportPrecision;
    if (snapshot.provenance.accumulated_microbatches != 0) return error.UnflushedBoundaryTrainingSnapshot;
    if (snapshot.provenance.optimizer_identity.optimizer_step > snapshot.provenance.optimizer_identity.microbatch_step) return error.InvalidBoundaryTrainingProgress;
    if (snapshot.slots.len == 0 or snapshot.slots.len > limits.max_slots) return error.InvalidBoundaryTrainingSnapshot;
    if (snapshot.base_model_name_or_path) |name| if (name.len == 0 or name.len > 4096 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidBoundaryTrainingSnapshot;
    try validateSourceIdentity(source);
    if (snapshot.mode == .full or snapshot.mode == .heads) try validateSourceMetadata(source, control);
    for (snapshot.slots, 0..) |slot, index| {
        try check(control);
        if (slot.name.len == 0 or slot.name.len > 1024) return error.InvalidBoundaryTrainingSnapshot;
        _ = try elements(slot.dimensions);
        for (snapshot.slots[0..index]) |prior| if (std.mem.eql(u8, prior.name, slot.name)) return error.DuplicateBoundaryTrainingSnapshotSlot;
    }
    var tensors: []checkpoint.NamedTensor = undefined;
    var config_bytes: ?[]const u8 = null;
    var fingerprint: ?[32]u8 = null;
    const is_adapter = snapshot.mode == .lora or snapshot.mode == .dora;
    if (is_adapter) {
        const provided = snapshot.adapter_layout orelse return error.MissingBoundaryTrainingAdapterLayout;
        if (provided.backbone != source.identity.backbone or (provided.config.kind == .dora) != (snapshot.mode == .dora)) return error.InvalidBoundaryTrainingAdapterLayout;
        // Re-resolve the immutable global configuration against the published
        // inventory. Never export an active per-batch graph's target subset.
        var layout = try layouts.init(a, source.identity.backbone, provided.config, .{ .max_adapters = limits.max_slots / 2, .max_adapter_bytes = limits.max_adapter_bytes });
        defer layout.deinit();
        if (!std.mem.eql(u8, &layout.fingerprint, &provided.fingerprint) or layout.slot_count != provided.slot_count or layout.parameter_bytes != provided.parameter_bytes)
            return error.InvalidBoundaryTrainingAdapterLayout;
        if (snapshot.slots.len != layout.slot_count) return error.IncompleteBoundaryTrainingSnapshot;
        tensors = try a.alloc(checkpoint.NamedTensor, layout.slot_count);
        const modules = try a.alloc([]const u8, layout.modules.len);
        var index: usize = 0;
        for (layout.modules, modules) |module, *name| {
            try check(control);
            name.* = try a.dupe(u8, module.target.module);
            const a_shape = [_]i32{ @intCast(module.rank), @intCast(module.target.in_dim) };
            const b_shape = [_]i32{ @intCast(module.target.out_dim), @intCast(module.rank) };
            tensors[index] = try namedTensor(a, module.target.a_key, try slotFor(snapshot.slots, module.a_name, &a_shape));
            tensors[index + 1] = try namedTensor(a, module.target.b_key, try slotFor(snapshot.slots, module.b_name, &b_shape));
            index += 2;
            if (module.magnitude_name) |name_m| {
                tensors[index] = try namedTensor(a, module.target.magnitude_key.?, try slotFor(snapshot.slots, name_m, &.{@intCast(module.target.out_dim)}));
                index += 1;
            }
        }
        config_bytes = try std.json.Stringify.valueAlloc(a, .{
            .peft_type = "LORA",
            .task_type = @as(?[]const u8, null),
            .base_model_name_or_path = snapshot.base_model_name_or_path,
            .revision = @as(?[]const u8, null),
            .r = layout.config.rank,
            .lora_alpha = layout.config.alpha,
            .lora_dropout = layout.config.dropout,
            .target_modules = modules,
            .bias = "none",
            .use_dora = snapshot.mode == .dora,
            .fan_in_fan_out = false,
            .inference_mode = true,
        }, .{ .whitespace = .indent_2 });
        if (config_bytes.?.len > limits.max_config_bytes) return error.BoundaryTrainingExportLimitExceeded;
        fingerprint = layout.fingerprint;
    } else {
        if (snapshot.adapter_layout != null) return error.InvalidBoundaryTrainingAdapterLayout;
        tensors = try fullTensors(a, source.parameters, snapshot, control);
    }
    std.mem.sort(checkpoint.NamedTensor, tensors, {}, struct {
        fn less(_: void, left: checkpoint.NamedTensor, right: checkpoint.NamedTensor) bool {
            return std.mem.lessThan(u8, left.name, right.name);
        }
    }.less);
    var payload_bytes: u64 = 0;
    var header_bound: u64 = 64;
    for (tensors) |tensor| {
        payload_bytes = try add(payload_bytes, try mul(tensor.data.len, 4));
        header_bound = try add(header_bound, try add(256 + tensor.shape.len * 24, tensor.name.len * 2));
    }
    header_bound = try add(header_bound, 7);
    if (header_bound > limits.max_header_bytes or (is_adapter and payload_bytes > limits.max_adapter_bytes)) return error.BoundaryTrainingExportLimitExceeded;
    // The standard adapter importer independently materializes the small
    // adapter tensors during validation. Reject an impossible reservation in
    // the metadata preflight, before training or writing any artifact. The
    // aggregate allocator also accounts for all metadata and arena overhead.
    if (is_adapter and payload_bytes >= limits.max_scratch_bytes - fixed_scratch_bytes) return error.BoundaryTrainingExportLimitExceeded;
    var output_bound = try add(try add(payload_bytes, header_bound + 8), limits.max_receipt_bytes);
    if (config_bytes) |bytes| {
        output_bound = try add(output_bound, try add(bytes.len, limits.max_receipt_bytes));
    } else {
        for (source.identity.sidecars) |pin| output_bound = try add(output_bound, pin.size_bytes);
    }
    // Filesystem blocks and two small staging-directory entries. This is an
    // admission allowance, not a claim about a filesystem's allocation policy.
    output_bound = try add(output_bound, 64 * 1024);
    if (output_bound > limits.max_output_bytes) return error.BoundaryTrainingExportLimitExceeded;
    return .{ .tensors = tensors, .config_bytes = config_bytes, .layout_fingerprint = fingerprint, .estimate = .{
        .output_bytes_upper_bound = output_bound,
        .reserved_scratch_bytes = limits.max_scratch_bytes,
        .tensor_payload_bytes = payload_bytes,
        .tensor_count = tensors.len,
    } };
}

fn validateSourceMetadata(source: View, control: ?Control) !void {
    try validateSourceIdentity(source);
    const expected = policy.specs(source.identity.backbone);
    if (source.parameters.len != expected.len or source.identity.weight.size_bytes == 0) return error.InvalidBoundaryTrainingSource;
    for (source.parameters, expected) |parameter, spec| {
        try check(control);
        const native_name = if (std.mem.startsWith(u8, spec.name, "encoder.")) spec.name["encoder.".len..] else spec.name;
        if (parameter.kind != .original or !std.mem.eql(u8, parameter.canonical_name, spec.name) or !std.mem.eql(u8, parameter.name, native_name) or parameter.dimensions.len != spec.shape.len)
            return error.InvalidBoundaryTrainingSource;
        for (parameter.dimensions, spec.shape) |actual, dimension| if (actual != dimension) return error.InvalidBoundaryTrainingSource;
        if (parameter.values.len != try elements(parameter.dimensions)) return error.InvalidBoundaryTrainingSource;
    }
}

fn validateSourceIdentity(source: View) !void {
    if (source.identity.precision != .fp32 or source.identity.weight.size_bytes == 0) return error.InvalidBoundaryTrainingSource;
    for (source.sidecars, source.identity.sidecars) |bytes, pin| if (bytes.len == 0 or bytes.len != pin.size_bytes) return error.InvalidBoundaryTrainingSource;
}

fn fullTensors(a: Allocator, parameters: []const run.Parameter, snapshot: Snapshot, control: ?Control) ![]checkpoint.NamedTensor {
    if (snapshot.mode != .full and snapshot.mode != .heads) return error.InvalidBoundaryTrainingSnapshot;
    var selected: usize = 0;
    for (parameters) |parameter| if (snapshot.mode == .full or !std.mem.startsWith(u8, parameter.canonical_name, "encoder.")) {
        selected += 1;
    };
    if (snapshot.slots.len != selected) return error.IncompleteBoundaryTrainingSnapshot;
    const tensors = try a.alloc(checkpoint.NamedTensor, parameters.len);
    for (parameters, tensors) |parameter, *tensor| {
        try check(control);
        const slot = if (snapshot.mode == .full or !std.mem.startsWith(u8, parameter.canonical_name, "encoder."))
            try slotFor(snapshot.slots, parameter.name, parameter.dimensions)
        else
            Slot{ .name = parameter.name, .dimensions = parameter.dimensions, .values = parameter.values };
        tensor.* = try namedTensor(a, parameter.canonical_name, slot);
    }
    return tensors;
}

fn namedTensor(a: Allocator, canonical_name: []const u8, slot: Slot) !checkpoint.NamedTensor {
    const shape = try a.alloc(usize, slot.dimensions.len);
    for (slot.dimensions, shape) |dim, *value| value.* = @intCast(dim);
    return .{ .name = try a.dupe(u8, canonical_name), .shape = shape, .data = slot.values };
}
fn slotFor(slots: []const Slot, name: []const u8, dimensions: []const i32) !Slot {
    for (slots) |slot| if (std.mem.eql(u8, name, slot.name)) {
        if (!std.mem.eql(i32, slot.dimensions, dimensions) or slot.values.len != try elements(dimensions)) return error.InvalidBoundaryTrainingSnapshotShape;
        return slot;
    };
    return error.MissingBoundaryTrainingSnapshotSlot;
}
fn elements(dimensions: []const i32) !usize {
    if (dimensions.len == 0 or dimensions.len > 4) return error.InvalidBoundaryTrainingSnapshotShape;
    var count: usize = 1;
    for (dimensions) |dimension| {
        if (dimension <= 0) return error.InvalidBoundaryTrainingSnapshotShape;
        count = std.math.mul(usize, count, @intCast(dimension)) catch return error.BoundaryTrainingExportLimitExceeded;
    }
    return count;
}
fn adapterBinding(identity: bundle.Identity, schemas: [32]u8) !adapter.Binding {
    var weight: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&weight, &identity.weight.sha256) catch return error.InvalidBoundaryTrainingSource;
    return .{ .source = identity, .schema_sha256 = schemas, .frozen_weight_sha256 = weight };
}

fn validateLimits(limits: Limits) !void {
    if (limits.max_scratch_bytes <= fixed_scratch_bytes or limits.max_header_bytes == 0 or limits.max_header_bytes > limits.max_scratch_bytes or
        limits.max_receipt_bytes == 0 or limits.max_receipt_bytes > limits.max_scratch_bytes or limits.max_config_bytes == 0 or limits.max_config_bytes > limits.max_scratch_bytes or
        limits.max_output_bytes == 0 or limits.max_adapter_bytes == 0 or limits.max_slots == 0 or limits.max_slots > 4096 or
        @as(u64, limits.max_scratch_bytes) > 4 * 1024 * mib or limits.max_header_bytes > 64 * mib or limits.max_config_bytes > mib or limits.max_receipt_bytes > mib or
        @as(u64, limits.max_adapter_bytes) > 2 * 1024 * mib or limits.max_output_bytes > 16 * 1024 * mib)
        return error.InvalidBoundaryTrainingExportLimits;
}
fn validateDestination(io: std.Io, output: []const u8) !void {
    const base = std.fs.path.basename(output);
    if (output.len == 0 or output.len > 4096 or std.mem.indexOfScalar(u8, output, 0) != null or base.len == 0 or
        std.mem.eql(u8, base, ".") or std.mem.eql(u8, base, "..") or std.mem.eql(u8, output, "/")) return error.InvalidOutputPath;
    if (std.Io.Dir.cwd().access(io, output, .{})) |_| return error.PathAlreadyExists else |err| if (err != error.FileNotFound) return err;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn add(left: u64, right: u64) !u64 {
    return std.math.add(u64, left, right) catch error.BoundaryTrainingExportLimitExceeded;
}
fn mul(left: u64, right: u64) !u64 {
    return std.math.mul(u64, left, right) catch error.BoundaryTrainingExportLimitExceeded;
}
fn finite(values: []const f32, control: ?Control) !void {
    for (values, 0..) |value, index| {
        if (index % (64 * 1024) == 0) try check(control);
        if (!std.math.isFinite(value)) return error.NonFiniteBoundaryTrainingSnapshot;
    }
    try check(control);
}
fn verifyBytes(bytes: []const u8, expected: bundle.Digest, control: ?Control) !void {
    if (bytes.len != expected.size_bytes) return error.BoundaryTrainingSnapshotMismatch;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| (256 * 1024));
        hash.update(bytes[offset..end]);
        offset = end;
    }
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower), &expected.sha256)) return error.BoundaryTrainingSnapshotMismatch;
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
        const end = @min(bytes.len, offset +| (256 * 1024));
        try file.writeStreamingAll(io, bytes[offset..end]);
        hash.update(bytes[offset..end]);
        offset = end;
    }
    try file.sync(io);
    try check(control);
    return .{ .size_bytes = bytes.len, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}
fn verifyFileBytes(a: Allocator, io: std.Io, directory: []const u8, name: []const u8, expected: []const u8, limit: usize, control: ?Control) !bundle.Digest {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const file = try files.openRegular(io, std.Io.Dir.cwd(), path, control);
    defer file.close(io);
    const initial = try file.stat(io);
    if (initial.size != expected.len) return error.BoundaryTrainingSnapshotMismatch;
    if (initial.size > limit) return error.BoundaryTrainingExportLimitExceeded;
    var buffer: [256 * 1024]u8 = undefined;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < expected.len) {
        const end = @min(expected.len, offset +| buffer.len);
        const bytes = buffer[0 .. end - offset];
        try readAt(file, io, bytes, offset, control);
        if (!std.mem.eql(u8, bytes, expected[offset..end])) return error.BoundaryTrainingSnapshotMismatch;
        hash.update(bytes);
        offset = end;
    }
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, initial.size) != 0) return error.BoundaryTrainingSnapshotMismatch;
    const final = try file.stat(io);
    if (final.kind != .file or final.size != initial.size or final.inode != initial.inode or !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime)) return error.BoundaryTrainingSnapshotMismatch;
    try check(control);
    return .{ .size_bytes = initial.size, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}

fn verifyWeights(a: Allocator, io: std.Io, path: []const u8, expected: []const checkpoint.NamedTensor, limits: Limits, control: ?Control) !bundle.Digest {
    const file = try files.openRegular(io, std.Io.Dir.cwd(), path, control);
    defer file.close(io);
    const initial = try file.stat(io);
    if (initial.size < 8 or initial.size > limits.max_output_bytes) return error.BoundaryTrainingExportLimitExceeded;
    var prefix: [8]u8 = undefined;
    if (try file.readPositionalAll(io, &prefix, 0) != prefix.len) return error.BoundaryTrainingSnapshotMismatch;
    const header_size = std.mem.readInt(u64, &prefix, .little);
    if (header_size == 0 or header_size > limits.max_header_bytes or header_size > initial.size - 8) return error.BoundaryTrainingExportLimitExceeded;
    const header = try a.alloc(u8, @intCast(header_size));
    defer a.free(header);
    try readAt(file, io, header, 8, control);
    try headerDepth(header);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, header, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const value = parsed.value;
    if (value != .object or value.object.count() != expected.len + 1) return error.BoundaryTrainingSnapshotMismatch;
    const metadata = value.object.get("__metadata__") orelse return error.BoundaryTrainingSnapshotMismatch;
    if (metadata != .object or metadata.object.count() != 1) return error.BoundaryTrainingSnapshotMismatch;
    if (!jsonString(metadata.object.get("format"), "pt")) return error.BoundaryTrainingSnapshotMismatch;
    var payload_bytes: u64 = 0;
    for (expected) |tensor| {
        try check(control);
        const raw = value.object.get(tensor.name) orelse return error.BoundaryTrainingSnapshotMismatch;
        if (raw != .object or raw.object.count() != 3 or !jsonString(raw.object.get("dtype"), "F32")) return error.BoundaryTrainingSnapshotMismatch;
        const shape = raw.object.get("shape") orelse return error.BoundaryTrainingSnapshotMismatch;
        if (shape != .array or shape.array.items.len != tensor.shape.len) return error.BoundaryTrainingSnapshotMismatch;
        for (shape.array.items, tensor.shape) |dim, expected_dim| if (try jsonUnsigned(dim) != expected_dim) return error.BoundaryTrainingSnapshotMismatch;
        const offsets = raw.object.get("data_offsets") orelse return error.BoundaryTrainingSnapshotMismatch;
        if (offsets != .array or offsets.array.items.len != 2 or try jsonUnsigned(offsets.array.items[0]) != payload_bytes) return error.BoundaryTrainingSnapshotMismatch;
        payload_bytes = try add(payload_bytes, try mul(tensor.data.len, 4));
        if (try jsonUnsigned(offsets.array.items[1]) != payload_bytes) return error.BoundaryTrainingSnapshotMismatch;
    }
    if (try add(header_size + 8, payload_bytes) != initial.size) return error.BoundaryTrainingSnapshotMismatch;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&prefix);
    hash.update(header);
    var buffer: [256 * 1024]u8 = undefined;
    var position = header_size + 8;
    for (expected) |tensor| {
        var offset: usize = 0;
        while (offset < tensor.data.len) {
            try check(control);
            const end = @min(tensor.data.len, offset + buffer.len / 4);
            const bytes = buffer[0 .. (end - offset) * 4];
            try readAt(file, io, bytes, position, control);
            if (builtin.target.cpu.arch.endian() == .little) {
                if (!std.mem.eql(u8, bytes, std.mem.sliceAsBytes(tensor.data[offset..end]))) return error.BoundaryTrainingSnapshotMismatch;
            } else {
                for (tensor.data[offset..end], 0..) |number, index| {
                    if (@as(u32, @bitCast(number)) != std.mem.readInt(u32, bytes[index * 4 ..][0..4], .little)) return error.BoundaryTrainingSnapshotMismatch;
                }
            }
            hash.update(bytes);
            position += bytes.len;
            offset = end;
        }
    }
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, initial.size) != 0) return error.BoundaryTrainingSnapshotMismatch;
    const final = try file.stat(io);
    if (final.kind != .file or final.size != initial.size or final.inode != initial.inode or !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime)) return error.BoundaryTrainingSnapshotMismatch;
    try check(control);
    return .{ .size_bytes = initial.size, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}
fn readAt(file: std.Io.File, io: std.Io, bytes: []u8, position: u64, control: ?Control) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| (256 * 1024));
        if (try file.readPositionalAll(io, bytes[offset..end], position + offset) != end - offset) return error.BoundaryTrainingSnapshotMismatch;
        offset = end;
    }
}
fn jsonString(value: ?std.json.Value, wanted: []const u8) bool {
    const actual = value orelse return false;
    return actual == .string and std.mem.eql(u8, actual.string, wanted);
}
fn jsonUnsigned(value: std.json.Value) !u64 {
    if (value != .integer or value.integer < 0) return error.BoundaryTrainingSnapshotMismatch;
    return @intCast(value.integer);
}
fn headerDepth(bytes: []const u8) !void {
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
                if (depth > 8) return error.BoundaryTrainingSnapshotMismatch;
            },
            '}', ']' => {
                if (depth == 0) return error.BoundaryTrainingSnapshotMismatch;
                depth -= 1;
            },
            else => {},
        }
    }
    if (depth != 0 or quoted) return error.BoundaryTrainingSnapshotMismatch;
}

fn testProvenance() Provenance {
    return .{ .run_fingerprint = @splat(11), .dataset_sha256 = @splat(12), .schemas_sha256 = @splat(13), .optimizer_identity = .{ .optimizer_step = 2, .microbatch_step = 5 }, .accumulated_microbatches = 0 };
}

// The private View isolates publication tests from model downloads. Public
// entrypoints accept only Source and validate all334 original descriptors.
const TestAdapter = struct {
    arena: std.heap.ArenaAllocator,
    layout: layouts.Layout,
    mode: run.Mode,
    slots: []Slot,
    source: View,

    fn init(a: Allocator, mode: run.Mode) !TestAdapter {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        var layout = try layouts.init(a, .small, .{ .kind = if (mode == .dora) .dora else .lora, .rank = 2, .alpha = 3, .dropout = 0.125, .targets = &.{ "boundary_head.count_head", "classifier.3" } }, .{});
        errdefer layout.deinit();
        const slots = try scratch.alloc(Slot, layout.slot_count);
        var index: usize = 0;
        for (layout.modules) |module| {
            slots[index] = try makeSlot(scratch, module.a_name, &.{ @intCast(module.rank), @intCast(module.target.in_dim) }, 0.25);
            slots[index + 1] = try makeSlot(scratch, module.b_name, &.{ @intCast(module.target.out_dim), @intCast(module.rank) }, -0.125);
            index += 2;
            if (module.magnitude_name) |name| {
                slots[index] = try makeSlot(scratch, name, &.{@intCast(module.target.out_dim)}, 1.5);
                index += 1;
            }
        }
        return .{ .arena = arena, .layout = layout, .mode = mode, .slots = slots, .source = .{
            .identity = .{ .backbone = .small, .precision = .fp32, .weight = bundle.Digest.of("synthetic immutable original FP32"), .sidecars = .{bundle.Digest.of("{}")} ** 4 },
            .parameters = &.{},
            .sidecars = .{"{}"} ** 4,
        } };
    }
    fn makeSlot(a: Allocator, name: []const u8, dimensions: []const i32, fill: f32) !Slot {
        const data = try a.alloc(f32, try elements(dimensions));
        @memset(data, fill);
        return .{ .name = name, .dimensions = try a.dupe(i32, dimensions), .values = data };
    }
    fn snapshot(self: *const TestAdapter) Snapshot {
        return .{ .mode = self.mode, .slots = self.slots, .adapter_layout = &self.layout, .provenance = testProvenance(), .base_model_name_or_path = "fastino/gliner2.5-small-v1" };
    }
    fn deinit(self: *TestAdapter) void {
        self.layout.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

test "boundary training export full and head snapshots preserve canonical inventory and frozen encoder" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const parameters = [_]run.Parameter{
        .{ .name = "encoder.weight", .canonical_name = "encoder.encoder.weight", .dimensions = &.{1}, .values = &.{1}, .kind = .original },
        .{ .name = "boundary_head.boundary_encoder.weight", .canonical_name = "boundary_head.boundary_encoder.weight", .dimensions = &.{1}, .values = &.{2}, .kind = .original },
        .{ .name = "classifier.bias", .canonical_name = "classifier.bias", .dimensions = &.{1}, .values = &.{3}, .kind = .original },
    };
    const slots = [_]Slot{
        .{ .name = "classifier.bias", .dimensions = &.{1}, .values = &.{33} },
        .{ .name = "boundary_head.boundary_encoder.weight", .dimensions = &.{1}, .values = &.{22} },
        .{ .name = "encoder.weight", .dimensions = &.{1}, .values = &.{11} },
    };
    for ([_]run.Mode{ .full, .heads }) |mode| {
        const snapshot = Snapshot{ .mode = mode, .slots = slots[0..if (mode == .full) 3 else 2], .provenance = testProvenance() };
        const tensors = try fullTensors(arena.allocator(), &parameters, snapshot, null);
        for (tensors, parameters) |tensor, parameter| try std.testing.expectEqualStrings(parameter.canonical_name, tensor.name);
        try std.testing.expectEqual(@as(f32, if (mode == .full) 11 else 1), tensors[0].data[0]);
        try std.testing.expectEqual(@as(f32, 22), tensors[1].data[0]);
        try std.testing.expectEqual(@as(f32, 33), tensors[2].data[0]);
        if (mode == .heads) try std.testing.expectEqual(parameters[0].values.ptr, tensors[0].data.ptr);
        var partial = snapshot;
        partial.slots = snapshot.slots[1..];
        try std.testing.expectError(error.IncompleteBoundaryTrainingSnapshot, fullTensors(arena.allocator(), &parameters, partial, null));
    }
}

test "boundary training export global LoRA and DoRA files preserve provenance and no overwrite" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]run.Mode{ .lora, .dora }) |mode| {
        var test_adapter = try TestAdapter.init(a, mode);
        defer test_adapter.deinit();
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
        defer a.free(parent);
        const output = try std.fs.path.join(a, &.{ parent, "adapter" });
        defer a.free(output);
        const snapshot = test_adapter.snapshot();
        const estimate = try estimateView(a, test_adapter.source, snapshot, .{}, null);
        const result = try exportView(a, io, test_adapter.source, output, snapshot, .{}, null);
        try std.testing.expectEqual(mode, result.mode);
        try std.testing.expect(result.output_bytes < estimate.output_bytes_upper_bound and result.peak_scratch_bytes <= estimate.reserved_scratch_bytes);
        try std.testing.expectEqual(test_adapter.layout.slot_count, estimate.tensor_count);
        const raw = try files.read(a, io, temporary.dir, "adapter/antfly_gliner25_training.json", 64 * 1024, null);
        defer a.free(raw);
        try std.testing.expectEqual(bundle.Digest.of(raw), result.provenance);
        const receipt = try std.json.parseFromSlice(Receipt, a, raw, .{});
        defer receipt.deinit();
        try std.testing.expectEqual(test_adapter.source.identity, receipt.value.source);
        try std.testing.expectEqual(snapshot.provenance, receipt.value.provenance);
        try std.testing.expectEqual(test_adapter.layout.fingerprint, receipt.value.adapter_layout_sha256.?);
        try std.testing.expectEqual(@as(?[4]bundle.Digest, null), receipt.value.sidecars);
        const config = try files.read(a, io, temporary.dir, "adapter/adapter_config.json", 128 * 1024, null);
        defer a.free(config);
        const parsed_config = try std.json.parseFromSlice(std.json.Value, a, config, .{});
        defer parsed_config.deinit();
        try std.testing.expect(jsonString(parsed_config.value.object.get("base_model_name_or_path"), snapshot.base_model_name_or_path.?));
        var imported = try adapter.importDirectory(a, output, try adapterBinding(test_adapter.source.identity, snapshot.provenance.schemas_sha256), .{}, null);
        defer imported.deinit();
        try std.testing.expectEqual(test_adapter.layout.modules.len, imported.modules.len);
        try std.testing.expectEqual(test_adapter.slots.len, imported.tensors.len);
        for (imported.tensors) |tensor| try std.testing.expect(std.mem.indexOf(u8, tensor.name, ".default") == null);
        try std.testing.expectError(error.PathAlreadyExists, exportView(a, io, test_adapter.source, output, snapshot, .{}, null));
        try std.testing.expectEqual(result.provenance, try verifyFileBytes(a, io, output, receipt_name, raw, 64 * 1024, null));
    }
}

test "boundary training export rejects partial extra invalid nonfinite and unflushed snapshots before publication" {
    const a = std.testing.allocator;
    var test_adapter = try TestAdapter.init(a, .lora);
    defer test_adapter.deinit();
    const original = test_adapter.snapshot();
    var snapshot = original;
    snapshot.slots = original.slots[1..];
    try std.testing.expectError(error.IncompleteBoundaryTrainingSnapshot, estimateView(a, test_adapter.source, snapshot, .{}, null));
    snapshot = original;
    snapshot.provenance.accumulated_microbatches = 1;
    try std.testing.expectError(error.UnflushedBoundaryTrainingSnapshot, estimateView(a, test_adapter.source, snapshot, .{}, null));
    snapshot = original;
    snapshot.mode = .dora;
    try std.testing.expectError(error.InvalidBoundaryTrainingAdapterLayout, estimateView(a, test_adapter.source, snapshot, .{}, null));
    snapshot = original;
    snapshot.adapter_layout = null;
    try std.testing.expectError(error.MissingBoundaryTrainingAdapterLayout, estimateView(a, test_adapter.source, snapshot, .{}, null));
    var quantized = test_adapter.source;
    quantized.identity.precision = .q8_0;
    try std.testing.expectError(error.UnsupportedBoundaryTrainingExportPrecision, estimateView(a, quantized, original, .{}, null));
    const slot = test_adapter.slots[0];
    test_adapter.slots[0] = test_adapter.slots[1];
    try std.testing.expectError(error.DuplicateBoundaryTrainingSnapshotSlot, estimateView(a, test_adapter.source, original, .{}, null));
    test_adapter.slots[0] = slot;
    test_adapter.slots[0].name = "unexpected.weight";
    try std.testing.expectError(error.MissingBoundaryTrainingSnapshotSlot, estimateView(a, test_adapter.source, original, .{}, null));
    test_adapter.slots[0] = slot;
    test_adapter.slots[0].dimensions = &.{1};
    try std.testing.expectError(error.InvalidBoundaryTrainingSnapshotShape, estimateView(a, test_adapter.source, original, .{}, null));
    test_adapter.slots[0] = slot;
    test_adapter.layout.fingerprint[0] ^= 1;
    try std.testing.expectError(error.InvalidBoundaryTrainingAdapterLayout, estimateView(a, test_adapter.source, original, .{}, null));
    test_adapter.layout.fingerprint[0] ^= 1;
    try std.testing.expectError(error.BoundaryTrainingExportLimitExceeded, estimateView(a, test_adapter.source, original, .{ .max_output_bytes = 1 }, null));
    try std.testing.expectError(error.BoundaryTrainingExportLimitExceeded, estimateView(a, test_adapter.source, original, .{ .max_header_bytes = 4 }, null));
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const output = try std.fs.path.join(a, &.{ parent, "bad" });
    defer a.free(output);
    @constCast(test_adapter.slots[0].values)[0] = std.math.nan(f32);
    _ = try estimateView(a, test_adapter.source, original, .{}, null); // metadata only
    try std.testing.expectError(error.NonFiniteBoundaryTrainingSnapshot, exportView(a, std.testing.io, test_adapter.source, output, original, .{}, null));
    var iterator = temporary.dir.iterate();
    try std.testing.expect((try iterator.next(std.testing.io)) == null);
}

test "boundary training export written tensors reject same-size mutation shape aliases and trailing bytes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const output = try std.fs.path.join(a, &.{ parent, "weights.safetensors" });
    defer a.free(output);
    const values = [_]f32{ 0.25, -0.0, 1.5, -2 };
    const tensors = [_]checkpoint.NamedTensor{.{ .name = "weight", .shape = &.{ 2, 2 }, .data = &values }};
    try checkpoint.saveControlled(a, output, &tensors, null, true);
    const original = try verifyWeights(a, io, output, &tensors, .{}, null);
    const same_values_different_shape = [_]checkpoint.NamedTensor{.{ .name = "weight", .shape = &.{4}, .data = &values }};
    try std.testing.expectError(error.BoundaryTrainingSnapshotMismatch, verifyWeights(a, io, output, &same_values_different_shape, .{}, null));
    const file = try std.Io.Dir.cwd().openFile(io, output, .{ .mode = .read_write });
    defer file.close(io);
    var last: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try file.readPositionalAll(io, &last, original.size_bytes - 1));
    last[0] ^= 1;
    try file.writePositionalAll(io, &last, original.size_bytes - 1);
    try file.sync(io);
    try std.testing.expectError(error.BoundaryTrainingSnapshotMismatch, verifyWeights(a, io, output, &tensors, .{}, null));
    last[0] ^= 1;
    try file.writePositionalAll(io, &last, original.size_bytes - 1);
    try file.writePositionalAll(io, &.{0}, original.size_bytes);
    try file.sync(io);
    try std.testing.expectError(error.BoundaryTrainingSnapshotMismatch, verifyWeights(a, io, output, &tensors, .{}, null));
}

fn exportAllocationFailures(a: Allocator, source: View, snapshot: Snapshot, output: []const u8) !void {
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, output) catch {};
    _ = try exportView(a, std.testing.io, source, output, snapshot, .{}, null);
}
fn estimateAllocationFailures(a: Allocator, source: View, snapshot: Snapshot) !void {
    _ = try estimateView(a, source, snapshot, .{}, null);
}

test "boundary training export bounds cancellation and all allocation failures reclaim private staging" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var test_adapter = try TestAdapter.init(a, .dora);
    defer test_adapter.deinit();
    const snapshot = test_adapter.snapshot();
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const output = try std.fs.path.join(a, &.{ parent, "output" });
    defer a.free(output);
    const Cancel = struct {
        fn call(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, exportView(a, io, test_adapter.source, output, snapshot, .{}, .{ .check_fn = Cancel.call }));
    try std.testing.expectError(error.BoundaryTrainingExportLimitExceeded, exportView(a, io, test_adapter.source, output, snapshot, .{ .max_scratch_bytes = fixed_scratch_bytes + 1, .max_header_bytes = 1, .max_config_bytes = 1, .max_receipt_bytes = 1 }, null));
    // Cancel only after the private directory exists: cleanup covers the
    // middle of publication, not just the initial admission check.
    const CancelStaged = struct {
        directory: std.Io.Dir,
        saw_stage: bool = false,
        fn call(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            var iterator = self.directory.iterate();
            if (try iterator.next(std.testing.io)) |_| {
                self.saw_stage = true;
                return error.Cancelled;
            }
        }
    };
    var cancel = CancelStaged{ .directory = temporary.dir };
    try std.testing.expectError(error.Cancelled, exportView(a, io, test_adapter.source, output, snapshot, .{}, .{ .ptr = &cancel, .check_fn = CancelStaged.call }));
    try std.testing.expect(cancel.saw_stage);
    try std.testing.checkAllAllocationFailures(a, estimateAllocationFailures, .{ test_adapter.source, snapshot });
    try std.testing.checkAllAllocationFailures(a, exportAllocationFailures, .{ test_adapter.source, snapshot, output });
    var iterator = temporary.dir.iterate();
    try std.testing.expect((try iterator.next(io)) == null);
}

test "boundary training export published small full and head complete FP32 snapshots" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_TRAINING_EXPORT_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    const source = try source_mod.Source.open(a, io, directory, .{}, null);
    defer source.deinit();
    if (source.config.backbone != .small) return error.InvalidBoundaryTrainingSource;
    for ([_]run.Mode{ .full, .heads }) |mode| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var slots = std.ArrayListUnmanaged(Slot).empty;
        for (source.parameters) |parameter| {
            if (mode == .heads and std.mem.startsWith(u8, parameter.canonical_name, "encoder.")) continue;
            var values = parameter.values;
            if (std.mem.eql(u8, parameter.canonical_name, "classifier.3.bias")) {
                const changed = try scratch.dupe(f32, values);
                changed[0] += 0.03125;
                values = changed;
            }
            try slots.append(scratch, .{ .name = parameter.name, .dimensions = parameter.dimensions, .values = values });
        }
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const parent = try temporary.dir.realPathFileAlloc(io, ".", scratch);
        const output = try std.fs.path.join(scratch, &.{ parent, "model" });
        const snapshot = Snapshot{ .mode = mode, .slots = slots.items, .provenance = testProvenance() };
        const estimate = try estimateSnapshot(a, source, snapshot, .{}, null);
        const result = try exportSnapshot(a, io, source, output, snapshot, .{}, null);
        try std.testing.expectEqual(@as(usize, 334), estimate.tensor_count);
        try std.testing.expect(result.output_bytes <= estimate.output_bytes_upper_bound and result.peak_scratch_bytes <= estimate.reserved_scratch_bytes);
        try std.testing.expect(!std.meta.eql(result.weights, source.identity.weight));
        const reopened = try source_mod.Source.open(a, io, output, .{}, null);
        defer reopened.deinit();
        try std.testing.expectEqual(result.weights, reopened.identity.weight);
        try std.testing.expectEqual(source.identity.sidecars, reopened.identity.sidecars);
        try std.testing.expectEqual(@as(usize, 334), reopened.parameters.len);
        for (source.parameters, reopened.parameters) |before, after| {
            try std.testing.expectEqualStrings(before.canonical_name, after.canonical_name);
            if (std.mem.eql(u8, before.canonical_name, "classifier.3.bias")) {
                try std.testing.expectEqual(before.values[0] + @as(f32, 0.03125), after.values[0]);
            } else try std.testing.expectEqualSlices(f32, before.values, after.values);
        }
        std.debug.print("boundary training export {s}: output={d}, scratch_peak={d}, reserved={d} bytes\n", .{ @tagName(mode), result.output_bytes, result.peak_scratch_bytes, estimate.reserved_scratch_bytes });
    }
}

test "boundary training export terminal allocation ignores prior resize denial and preserves actual IO errors" {
    const a = std.testing.allocator;
    var budget = Budget{ .backing = a, .limit = 64 };
    defer std.debug.assert(budget.live == 0);
    var gate = AllocationGate{ .budget = &budget };
    const tracked = gate.allocator();
    const bytes = try tracked.alloc(u8, 8);
    defer tracked.free(bytes);
    try std.testing.expect(!tracked.resize(bytes, 65));
    try std.testing.expect(tracked.remap(bytes, 65) == null);
    try std.testing.expect(budget.denied and gate.terminal == null);
    try std.testing.expectEqual(error.WriteFailed, gate.mapError(error.WriteFailed));
    var backing_failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    budget.backing = backing_failure.allocator();
    const failed = tracked.alloc(u8, 4);
    budget.backing = a;
    try std.testing.expectError(error.OutOfMemory, failed);
    try std.testing.expectEqual(.backing_allocator, gate.terminal.?.kind);
    try std.testing.expectEqual(error.OutOfMemory, gate.mapError(error.OutOfMemory));
    // An allocating JSON writer wraps this same final failure in WriteFailed.
    try std.testing.expectEqual(error.OutOfMemory, gate.mapError(error.WriteFailed));
    budget.limit = bytes.len;
    try std.testing.expectError(error.OutOfMemory, tracked.alloc(u8, 1));
    try std.testing.expectEqual(.declared_limit, gate.terminal.?.kind);
    try std.testing.expectEqual(error.BoundaryTrainingExportLimitExceeded, gate.mapError(error.OutOfMemory));
    try std.testing.expectEqual(error.BoundaryTrainingExportLimitExceeded, gate.mapError(error.WriteFailed));
    try std.testing.expectEqual(error.NoSpaceLeft, gate.mapError(error.NoSpaceLeft));
}
