// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Leased reader weights with explicit owner lifetime. No synthetic modules,
//! CPU fallback or implicit enlargement of operator budgets.
const std = @import("std");
const build_options = @import("build_options");
const module = @import("memorilla.zig");
const ops = @import("../ops/ops.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const cuda = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const A = std.mem.Allocator;
pub const parameter_bytes: usize = 776_495_104;

pub fn weightPlan() memory.AdmissionAmounts {
    return .{
        .host_weight_bytes = 512 * 1024 * 1024 + parameter_bytes + 16 * 1024 * 1024,
        .backend_weight_bytes = parameter_bytes,
        .backend_scratch_bytes = 128 * 1024 * 1024,
    };
}

pub fn requireLimits(limits: memory.Limits) !void {
    const plan = weightPlan();
    if (limits.host_limit_bytes == 0 or limits.backend_limit_bytes == 0 or limits.combined_limit_bytes == 0)
        return error.MemorillaFiniteBudgetRequired;
    if (plan.hostTotalBytes() > limits.host_limit_bytes or plan.backendTotalBytes() > limits.backend_limit_bytes or
        (try std.math.add(usize, plan.hostTotalBytes(), plan.backendTotalBytes())) > limits.combined_limit_bytes) return error.ResourceLimitExceeded;
}

pub fn requireAdmission(admission: *const memory.AdmissionLease, limits: memory.Limits) !void {
    try requireLimits(limits);
    const plan = weightPlan();
    if (admission.controller == null or admission.retain_backend_class != .gpu or
        admission.amounts.host_weight_bytes < plan.host_weight_bytes or
        admission.amounts.backend_weight_bytes < plan.backend_weight_bytes or
        admission.amounts.backend_scratch_bytes < plan.backend_scratch_bytes)
        return error.MemorillaReaderAdmissionRequired;
}

// Exported SafeTensors digests of the five independently source-verified and
// numerically qualified releases. A manifest's source SHA claim alone cannot
// authenticate arbitrary replacement tensors with a recomputed local digest.
const releases = [_]struct { checkpoint: []const u8, weights: []const u8 }{
    .{ .checkpoint = "stage1", .weights = "32497c063bc1a6667925f5ed7b7acdb08d0b1b32e96241c3f650a5a6ccd163d8" },
    .{ .checkpoint = "stage2", .weights = "a3f3e1d42f53ccc108005587bfd39718b913b2c727128e02e254b63665c32dbd" },
    .{ .checkpoint = "stage3", .weights = "3a7ec610dc29818c2aa29c3867d3be3e69908a191a94d0d6f0f416d564d3470e" },
    .{ .checkpoint = "pmv2", .weights = "c5a6a671d8223189e1015889715e310cf912254ebfbfd698e3d14064000ce301" },
    .{ .checkpoint = "pv4", .weights = "1d054e866b01fd4b3345ba90787f7bfafa915dcdff4065bd9341cd44f0c03445" },
};

fn requireRelease(bundle: *const module.Bundle) !void {
    if (!bundle.release) return error.SyntheticMemorillaBundleNotAllowed;
    const source = bundle.manifest.value.object.get("source") orelse return error.InvalidMemorillaManifest;
    if (source != .object) return error.InvalidMemorillaManifest;
    const checkpoint = source.object.get("checkpoint") orelse return error.InvalidMemorillaManifest;
    if (checkpoint != .string) return error.InvalidMemorillaManifest;
    for (releases) |release| {
        if (std.mem.eql(u8, checkpoint.string, release.checkpoint) and std.mem.eql(u8, &bundle.weights_sha256, release.weights)) return;
    }
    return error.UnsupportedMemorillaRelease;
}

pub const Owner = struct {
    allocator: A,
    bundle: module.Bundle,
    compute: if (build_options.enable_cuda) *cuda.CudaCompute else void,
    execution_mutex: std.atomic.Mutex = .unlocked,

    /// Caller owns the GPU admission lease and must release it AFTER deinit.
    pub fn open(a: A, io: std.Io, directory: []const u8, admission: *const memory.AdmissionLease, limits: memory.Limits, control: Control) !Owner {
        try control.check();
        try requireAdmission(admission, limits);
        try @import("memorilla_qualification.zig").requireCudaEnvironment();
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        const backing = @import("antfly_platform").allocator.reclaimingScratch(a);
        var guard = try control.enterUninterruptible(.uninterruptible);
        defer guard.deinit();
        var bundle = try module.Bundle.open(backing, directory, false);
        errdefer bundle.deinit();
        if (bundle.reader.mmap_region) |*mapping| mapping.discard_on_deinit = false;
        try requireRelease(&bundle);
        try control.check();
        var store = try bundle.loadFloat32Store();
        defer store.deinitOwned();
        try control.check();
        const compute = try backing.create(cuda.CudaCompute);
        errdefer backing.destroy(compute);
        compute.* = try cuda.CudaCompute.init(backing);
        errdefer compute.deinit();
        if (compute.ctx.info.compute_major != 12 or compute.ctx.info.compute_minor != 1) return error.UnsupportedMemorillaDevice;
        compute.strict_f32_weights = true;
        compute.capture_gates = .{};
        try compute.resident_weights.ensureUnusedCapacity(backing, store.resident_weights.count());
        var iter = store.resident_weights.iterator();
        while (iter.next()) |entry| {
            try control.check();
            const key = try backing.dupe(u8, entry.key_ptr.*);
            errdefer backing.free(key);
            try compute.insertWeightFromLoaded(key, entry.value_ptr);
        }
        _ = compute.releaseMemorillaRequestResources() catch @panic("Memorilla reader load cleanup failed; worker restart required");
        if (compute.snapshotStats().resident_weight_bytes != parameter_bytes) return error.UnsupportedMemorillaPrecision;
        _ = io;
        try control.check();
        return .{ .allocator = backing, .bundle = bundle, .compute = compute };
    }

    /// Borrowed backend. Caller serializes it with execution_mutex, propagates
    /// control, and frees every request CT before releaseRequestResources.
    pub fn computeBackend(self: *Owner) !ops.ComputeBackend {
        if (comptime build_options.enable_cuda) return self.compute.computeBackend();
        return error.CudaNotEnabled;
    }

    pub fn computeBackendWithBudget(self: *Owner, budget: *memory.RunBudget) !ops.ComputeBackend {
        if (comptime build_options.enable_cuda) return self.compute.computeBackendWithStepRunBudget(budget, @intFromPtr(self));
        return error.CudaNotEnabled;
    }

    pub fn releaseRequestResources(self: *Owner) !void {
        if (comptime build_options.enable_cuda) {
            _ = try self.compute.releaseMemorillaRequestResources();
        } else return error.CudaNotEnabled;
    }

    pub fn deinit(self: *Owner) void {
        if (comptime build_options.enable_cuda) {
            self.compute.deinit();
            self.allocator.destroy(self.compute);
        }
        self.bundle.deinit();
        self.* = undefined;
    }
};

test "Memorilla reader requires finite complete admission before artifact IO" {
    const released = memory.AdmissionLease{ .controller = null, .amounts = .{}, .amounts_by_backend = @splat(.{}), .retain_backend_class = null, .live_reserved_bytes = 0 };
    try std.testing.expectError(error.MemorillaFiniteBudgetRequired, requireAdmission(&released, .{}));
    const limits = memory.Limits{ .host_limit_bytes = 4 * 1024 * 1024 * 1024, .backend_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 6 * 1024 * 1024 * 1024 };
    try std.testing.expectError(error.MemorillaReaderAdmissionRequired, Owner.open(std.testing.allocator, std.testing.io, "/not-opened", &released, limits, .{}));
}
