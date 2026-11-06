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

const std = @import("std");
const platform = @import("antfly_platform");
const compiled_artifact = @import("../compiled_artifact.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const runtime = @import("../runtime/root.zig");

pub const Context = struct {
    allocator: std.mem.Allocator,
    manager: *model_manager_mod.ModelManager,
    policy: ?model_manager_mod.ModelLoadPolicy = null,
    decode_steps_limit: ?usize = null,
};

fn artifactShapeBytes(shape: []const i64, element_bytes: usize) !usize {
    var bytes = element_bytes;
    for (shape) |dim| {
        const size = std.math.cast(usize, dim) orelse return error.InvalidArtifactShape;
        bytes = std.math.mul(usize, bytes, size) catch return error.ResourceLimitExceeded;
    }
    return bytes;
}

/// Compiler-derived residency and workspace are admitted before a provider
/// can create a client or deserialize native code. No post-load budget growth.
pub const ArtifactDeviceAdmission = struct {
    manager: *model_manager_mod.ModelManager,
    limits: runtime.tier.memory.Limits,
    cpu: bool,
    planned: runtime.tier.memory.AdmissionAmounts,
    initial: runtime.tier.memory.AdmissionLease,
    device_bytes: usize = 0,
    pool_bytes: usize = 0,

    pub const Capacity = struct { total_bytes: usize, unified: bool };

    pub fn plan(artifact: compiled_artifact.Manifest) !compiled_artifact.PjrtMemoryPlan {
        if (!std.mem.eql(u8, artifact.kind, "pjrt_executable") and
            !std.mem.eql(u8, artifact.kind, "pjrt_partition_executable")) return error.UnsupportedPjrtFeature;
        const profile = artifact.pjrt_memory_plan orelse return error.UnsupportedPjrtFeature;
        if (profile.version != 1) return error.UnsupportedPjrtFeature;
        return profile;
    }

    pub fn limitsFor(context: *const Context, cpu: bool) runtime.tier.memory.Limits {
        const memory = runtime.tier.memory;
        var limits = memory.defaultLimitsForBackend(if (cpu) .cpu else .gpu);
        if (context.policy) |policy| limits = memory.applyLimitOverrides(limits, policy.limits);
        return limits;
    }

    pub fn plannedAmounts(artifact: compiled_artifact.Manifest) !runtime.tier.memory.AdmissionAmounts {
        const profile = try plan(artifact);
        var amounts = try executionAmounts(artifact, profile.memory, profile.platform == .cpu);
        if (profile.platform == .cuda) try addCudaHeadroom(&amounts);
        return amounts;
    }

    pub fn addCudaHeadroom(amounts: *runtime.tier.memory.AdmissionAmounts) !void {
        const quantum: usize = 2 * 1024 * 1024;
        const bytes = amounts.backendTotalBytes();
        const rounded = (std.math.add(usize, try std.math.add(usize, bytes, 2 * quantum), quantum - 1) catch return error.ResourceLimitExceeded) & ~(quantum - 1);
        amounts.backend_weight_bytes = try std.math.add(usize, amounts.backend_weight_bytes, rounded - bytes);
    }

    pub fn init(context: *const Context, artifact: compiled_artifact.Manifest) !@This() {
        const profile = try plan(artifact);
        return initAmounts(context, profile.platform == .cpu, try plannedAmounts(artifact), if (profile.platform == .cpu) 0 else profile.memory.code);
    }

    pub fn initAmounts(context: *const Context, cpu: bool, amounts: runtime.tier.memory.AdmissionAmounts, external_code: usize) !@This() {
        // Impossible operator caps must reject before even a CUDA capacity
        // probe, and long before plugin initialization or Client_Create.
        if (!try amounts.fitsLimits(limitsFor(context, cpu))) return error.ResourceLimitExceeded;
        var capacity: ?Capacity = null;
        if (!cpu) {
            try @import("pjrt").pjrt.validateBoundedCudaEnvironment();
            var driver = try @import("../ops/cuda/driver.zig").CudaDriver.open();
            defer driver.deinit();
            const device = try driver.defaultDeviceMemory(); // no CUDA context
            capacity = .{ .total_bytes = device.total_bytes, .unified = device.unified };
        }
        return initAmountsForCapacity(context, cpu, amounts, capacity, external_code);
    }

    pub fn initForCapacity(context: *const Context, artifact: compiled_artifact.Manifest, capacity: ?Capacity) !@This() {
        const profile = try plan(artifact);
        return initAmountsForCapacity(context, profile.platform == .cpu, try plannedAmounts(artifact), capacity, if (profile.platform == .cpu) 0 else profile.memory.code);
    }

    fn initAmountsForCapacity(context: *const Context, cpu: bool, amounts: runtime.tier.memory.AdmissionAmounts, capacity: ?Capacity, external_code: usize) !@This() {
        const memory = runtime.tier.memory;
        var limits = limitsFor(context, cpu);
        if (!cpu) {
            const physical = capacity orelse return error.UnsupportedPjrtFeature;
            limits = memory.clampDiscreteDeviceLimits(limits, physical.total_bytes, 0);
            if (physical.unified) {
                limits = memory.clampUnifiedDeviceLimits(limits, physical.total_bytes);
                try context.manager.tightenUnifiedDeviceCapacity(physical.total_bytes);
            }
        }
        return .{
            .manager = context.manager,
            .limits = limits,
            .cpu = cpu,
            .planned = amounts,
            .initial = try context.manager.acquireRunResourceAmounts(if (cpu) .cpu else .gpu, limits, amounts),
            .device_bytes = if (capacity) |physical| physical.total_bytes else 0,
            .pool_bytes = amounts.backendTotalBytes() - external_code,
        };
    }

    pub fn bufferAmounts(artifact: compiled_artifact.Manifest) !runtime.tier.memory.AdmissionAmounts {
        if (artifact.pjrt_input_shapes.len != artifact.pjrt_input_bindings.len or
            artifact.pjrt_output_shapes.len != artifact.pjrt_output_node_ids.len)
            return error.MissingArtifactMetadata;
        var amounts = runtime.tier.memory.AdmissionAmounts{};
        for (artifact.pjrt_input_shapes, artifact.pjrt_input_bindings) |shape, binding| {
            const bytes = try artifactShapeBytes(shape, if (compiled_artifact.pjrtBindingIsInputIds(binding)) 8 else 4);
            const field = if (compiled_artifact.pjrtBindingIsKvCache(binding)) &amounts.backend_kv_bytes else &amounts.backend_weight_bytes;
            field.* = try std.math.add(usize, field.*, bytes);
        }
        for (artifact.pjrt_output_shapes, artifact.pjrt_output_node_ids) |shape, node_id| {
            var kv = false;
            for (artifact.pjrt_output_bindings) |binding| {
                if (binding.node_id == node_id and compiled_artifact.pjrtBindingIsKvCache(binding)) kv = true;
            }
            const field = if (kv) &amounts.backend_kv_bytes else &amounts.backend_scratch_bytes;
            field.* = try std.math.add(usize, field.*, try artifactShapeBytes(shape, 4));
        }
        return amounts;
    }

    pub fn executionAmounts(artifact: compiled_artifact.Manifest, stats: compiled_artifact.PjrtMemoryStats, cpu: bool) !runtime.tier.memory.AdmissionAmounts {
        var required = try bufferAmounts(artifact);
        const shape_arguments = blk: {
            var bytes: usize = 0;
            for (artifact.pjrt_input_shapes, artifact.pjrt_input_bindings) |shape, binding| {
                bytes = try std.math.add(usize, bytes, try artifactShapeBytes(shape, if (compiled_artifact.pjrtBindingIsInputIds(binding)) 8 else 4));
            }
            break :blk bytes;
        };
        // Do not credit input/output aliasing: inputs remain borrowed until
        // execution completes and outputs are retained by the runtime.
        required.backend_weight_bytes = try std.math.add(usize, required.backend_weight_bytes, stats.code);
        required.backend_weight_bytes = try std.math.add(usize, required.backend_weight_bytes, stats.arguments -| shape_arguments);
        const shape_outputs = blk: {
            var bytes: usize = 0;
            for (artifact.pjrt_output_shapes) |shape| bytes = try std.math.add(usize, bytes, try artifactShapeBytes(shape, 4));
            break :blk bytes;
        };
        required.backend_scratch_bytes = try std.math.add(usize, required.backend_scratch_bytes, stats.outputs -| shape_outputs);
        required.backend_scratch_bytes = try std.math.add(usize, required.backend_scratch_bytes, stats.temporary);
        const accounted = try std.math.add(usize, try std.math.add(usize, required.backend_weight_bytes, required.backend_kv_bytes), required.backend_scratch_bytes);
        // Unclassified total residency survives between calls. Keep it with
        // executable residency, rather than allowing variants to share it as
        // though it were a mutually exclusive temporary workspace.
        required.backend_weight_bytes = try std.math.add(usize, required.backend_weight_bytes, stats.total -| accounted);
        required.backend_scratch_bytes = try std.math.add(usize, required.backend_scratch_bytes, stats.peak -| @max(accounted, stats.total));
        required.host_weight_bytes = try std.math.add(usize, stats.host_code, stats.host_arguments);
        required.host_scratch_bytes = try std.math.add(usize, stats.host_outputs, stats.host_temporary);
        if (cpu) required = try mergeHostAmounts(required);
        var largest_input: usize = 0;
        for (artifact.pjrt_input_shapes, artifact.pjrt_input_bindings) |shape, binding| {
            // Token IDs are already owned by the caller. Other bindings use
            // an owned F32 conversion before the provider copies/uploads them.
            if (!compiled_artifact.pjrtBindingIsInputIds(binding)) largest_input = @max(largest_input, try artifactShapeBytes(shape, 4));
        }
        var largest_output: usize = 0;
        var logical_outputs: usize = 0;
        const whole_model = std.mem.eql(u8, artifact.kind, "pjrt_executable");
        for (artifact.pjrt_output_shapes, artifact.pjrt_output_node_ids) |shape, node_id| {
            const bytes = try artifactShapeBytes(shape, 4);
            logical_outputs = try std.math.add(usize, logical_outputs, bytes);
            var retained_kv = false;
            for (artifact.pjrt_output_bindings) |binding| if (binding.node_id == node_id and compiled_artifact.pjrtBindingIsKvCache(binding)) {
                retained_kv = true;
            };
            // Whole-model execution retains semantic KV on device; it only
            // downloads logits. Partition comparison may download any output.
            if (!whole_model or !retained_kv) largest_output = @max(largest_output, bytes);
        }
        if (largest_output != 0) largest_output = try std.math.add(usize, largest_output, stats.outputs -| logical_outputs);
        const transfers = @max(largest_input, try std.math.mul(usize, largest_output, 2));
        required.host_scratch_bytes = try std.math.add(usize, required.host_scratch_bytes, transfers);
        return required;
    }

    fn compiledResidency(artifact: compiled_artifact.Manifest, stats: compiled_artifact.PjrtMemoryStats) !usize {
        var arguments: usize = 0;
        for (artifact.pjrt_input_shapes, artifact.pjrt_input_bindings) |shape, binding| arguments = try std.math.add(usize, arguments, try artifactShapeBytes(shape, if (compiled_artifact.pjrtBindingIsInputIds(binding)) 8 else 4));
        var outputs: usize = 0;
        for (artifact.pjrt_output_shapes) |shape| outputs = try std.math.add(usize, outputs, try artifactShapeBytes(shape, 4));
        const known = try std.math.add(usize, stats.code, try std.math.add(usize, @max(arguments, stats.arguments), try std.math.add(usize, @max(outputs, stats.outputs), stats.temporary)));
        return try std.math.add(usize, stats.code, stats.total -| known);
    }

    pub fn verifyLoaded(artifact: compiled_artifact.Manifest, stats: @import("pjrt").pjrt.CompiledMemoryStats) !void {
        const profile = try plan(artifact);
        const actual = compiled_artifact.PjrtMemoryStats.fromCompiler(stats);
        inline for (@typeInfo(compiled_artifact.PjrtMemoryStats).@"struct".field_names) |name| {
            if (@field(actual, name) > @field(profile.memory, name)) return error.InvalidArtifact;
        }
    }

    fn mergeHostAmounts(amounts: runtime.tier.memory.AdmissionAmounts) !runtime.tier.memory.AdmissionAmounts {
        return .{
            .host_weight_bytes = try std.math.add(usize, amounts.host_weight_bytes, amounts.backend_weight_bytes),
            .host_kv_bytes = try std.math.add(usize, amounts.host_kv_bytes, amounts.backend_kv_bytes),
            .host_scratch_bytes = try std.math.add(usize, amounts.host_scratch_bytes, amounts.backend_scratch_bytes),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.initial.release();
    }
};

/// Own the serialized buffer and all construction permits outside the client
/// lifetime. Read/hash once; the executor factory must borrow this same buffer.
pub const PreparedPjrtArtifact = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    encoded_bytes: usize,
    staging: runtime.tier.memory.AdmissionLease,
    device: ArtifactDeviceAdmission,

    pub fn init(context: *const Context, artifact: compiled_artifact.Manifest, plugin_path: [:0]const u8) !@This() {
        _ = try ArtifactDeviceAdmission.plan(artifact);
        var device = try ArtifactDeviceAdmission.init(context, artifact);
        errdefer device.deinit();
        const profile = artifact.pjrt_memory_plan.?;
        const c_file = @import("../util/c_file.zig");
        const allocator = platform.allocator.reclaimingScratch(context.allocator);
        const encoded = std.math.cast(usize, try c_file.fileSize(allocator, artifact.artifact_path)) orelse return error.ResourceLimitExceeded;
        if (encoded == 0) return error.InvalidArtifact;
        // One serialized input plus a constructor-owned copy, and a bounded
        // plugin hashing buffer. No mapping/reading can precede this permit.
        const peak = try std.math.add(usize, try std.math.mul(usize, encoded, 2), 64 * 1024);
        var staging = try context.manager.acquireRunResourceAmounts(.cpu, ArtifactDeviceAdmission.limitsFor(context, true), .{ .host_weight_bytes = peak });
        errdefer staging.release();
        const bytes = try c_file.readFileMax(allocator, artifact.artifact_path, encoded);
        errdefer allocator.free(bytes);
        if (bytes.len != encoded) return error.InvalidArtifact;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (!std.mem.eql(u8, &digest, &profile.artifact_sha256)) return error.InvalidArtifact;
        const plugin_digest = try c_file.sha256File(allocator, plugin_path);
        if (!std.mem.eql(u8, &plugin_digest, &profile.plugin_sha256)) return error.InvalidArtifact;
        try staging.retain(.{ .host_weight_bytes = try std.math.mul(usize, encoded, 2) });
        return .{
            .allocator = allocator,
            .bytes = bytes,
            .encoded_bytes = encoded,
            .staging = staging,
            .device = device,
        };
    }

    pub fn createClient(self: *const @This(), plugin_path: [:0]const u8) !@import("pjrt").pjrt.Client {
        const Client = @import("pjrt").pjrt.Client;
        var client = try Client.initWithMemoryPolicy(plugin_path, if (self.device.cpu) .cpu else .{ .cuda = .{
            .capacity_bytes = self.device.pool_bytes,
            .device_bytes = self.device.device_bytes,
        } });
        errdefer client.deinit();
        if (!client.api.hasCompiledMemoryStats()) return error.UnsupportedPjrtFeature;
        const name = try client.platformName();
        if (self.device.cpu) {
            if (!try client.isCpu()) return error.InvalidArtifact;
        } else if (!std.ascii.eqlIgnoreCase(name, "cuda")) return error.InvalidArtifact;
        return client;
    }

    pub fn loaded(self: *@This(), artifact: compiled_artifact.Manifest, stats: @import("pjrt").pjrt.CompiledMemoryStats) !void {
        try ArtifactDeviceAdmission.verifyLoaded(artifact, stats);
        self.allocator.free(self.bytes);
        self.bytes = &.{};
        // The provider may retain its own serialized representation. Keep that
        // bound until client destruction even after freeing our input copy.
        try self.staging.retain(.{ .host_weight_bytes = self.encoded_bytes });
    }

    pub fn deinit(self: *@This()) void {
        if (self.bytes.len != 0) self.allocator.free(self.bytes);
        self.staging.release();
        self.device.deinit();
    }
};

/// All loaded variants retain residency; mutually exclusive execution phases
/// share the largest KV/scratch envelope. The provider and every native handle
/// must close before this owner releases its permits.
pub const Package = struct {
    allocator: std.mem.Allocator,
    lifetime: model_manager_mod.ModelManager.ResourceLifetime,
    prefill_bytes: []u8,
    encoded_sizes: []usize,
    encoded_total: usize,
    metadata_bytes: usize,
    staging: runtime.tier.memory.AdmissionLease,
    device: ArtifactDeviceAdmission,
    client: @import("pjrt").pjrt.Client,

    fn readVerified(a: std.mem.Allocator, artifact: compiled_artifact.Manifest, size: usize) ![]u8 {
        const bytes = try @import("../util/c_file.zig").readFileMax(a, artifact.artifact_path, size);
        errdefer a.free(bytes);
        if (bytes.len == 0) return error.InvalidArtifact;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (!std.mem.eql(u8, &digest, &artifact.pjrt_memory_plan.?.artifact_sha256)) return error.InvalidArtifact;
        return bytes;
    }

    pub fn init(context: *const Context, artifacts: []const compiled_artifact.Manifest, plugin: [:0]const u8, metadata_bytes: usize) !*@This() {
        if (artifacts.len == 0) return error.MissingArtifactMetadata;
        const a = platform.allocator.reclaimingScratch(context.allocator);
        const lifetime = try context.manager.retainResourceLifetime();
        errdefer lifetime.deinit();
        const first = try ArtifactDeviceAdmission.plan(artifacts[0]);
        const sizes = try a.alloc(usize, artifacts.len);
        errdefer a.free(sizes);
        var amounts: runtime.tier.memory.AdmissionAmounts = .{};
        var host_code: usize = 0;
        var backend_code: usize = 0;
        var external_code: usize = 0;
        var encoded: usize = 0;
        var largest_encoded: usize = 0;
        const c_file = @import("../util/c_file.zig");
        for (artifacts, 0..) |artifact, i| {
            const profile = try ArtifactDeviceAdmission.plan(artifact);
            if (profile.platform != first.platform or !std.mem.eql(u8, &profile.plugin_sha256, &first.plugin_sha256)) return error.InvalidArtifact;
            const one = try ArtifactDeviceAdmission.executionAmounts(artifact, profile.memory, first.platform == .cpu);
            const residency = try ArtifactDeviceAdmission.compiledResidency(artifact, profile.memory);
            const resident_host = try std.math.add(usize, profile.memory.host_code, if (first.platform == .cpu) residency else 0);
            const resident_backend = if (first.platform == .cpu) 0 else residency;
            host_code = try std.math.add(usize, host_code, resident_host);
            backend_code = try std.math.add(usize, backend_code, resident_backend);
            if (first.platform == .cuda) external_code = try std.math.add(usize, external_code, profile.memory.code);
            amounts.host_weight_bytes = @max(amounts.host_weight_bytes, one.host_weight_bytes - resident_host);
            amounts.backend_weight_bytes = @max(amounts.backend_weight_bytes, one.backend_weight_bytes - resident_backend);
            amounts.host_kv_bytes = @max(amounts.host_kv_bytes, one.host_kv_bytes);
            amounts.backend_kv_bytes = @max(amounts.backend_kv_bytes, one.backend_kv_bytes);
            amounts.host_scratch_bytes = @max(amounts.host_scratch_bytes, one.host_scratch_bytes);
            amounts.backend_scratch_bytes = @max(amounts.backend_scratch_bytes, one.backend_scratch_bytes);
            sizes[i] = std.math.cast(usize, try c_file.fileSize(a, artifact.artifact_path)) orelse return error.ResourceLimitExceeded;
            encoded = try std.math.add(usize, encoded, sizes[i]);
            largest_encoded = @max(largest_encoded, sizes[i]);
        }
        amounts.host_weight_bytes = try std.math.add(usize, amounts.host_weight_bytes, try std.math.add(usize, host_code, metadata_bytes));
        amounts.backend_weight_bytes = try std.math.add(usize, amounts.backend_weight_bytes, backend_code);
        if (first.platform == .cuda) try ArtifactDeviceAdmission.addCudaHeadroom(&amounts);
        var device = try ArtifactDeviceAdmission.initAmounts(context, first.platform == .cpu, amounts, external_code);
        errdefer device.deinit();
        const peak = try std.math.add(usize, try std.math.add(usize, encoded, largest_encoded), 64 * 1024);
        var staging = try context.manager.acquireRunResourceAmounts(.cpu, ArtifactDeviceAdmission.limitsFor(context, true), .{ .host_weight_bytes = peak });
        errdefer staging.release();
        if (!std.mem.eql(u8, &try c_file.sha256File(a, plugin), &first.plugin_sha256)) return error.InvalidArtifact;
        const prefill = try readVerified(a, artifacts[0], sizes[0]);
        errdefer a.free(prefill);
        try staging.retain(.{ .host_weight_bytes = try std.math.add(usize, encoded, largest_encoded) });
        const owner = try a.create(@This());
        errdefer a.destroy(owner);
        owner.* = .{ .allocator = a, .lifetime = lifetime, .prefill_bytes = prefill, .encoded_sizes = sizes, .encoded_total = encoded, .metadata_bytes = metadata_bytes, .staging = staging, .device = device, .client = try @import("pjrt").pjrt.Client.initWithMemoryPolicy(plugin, if (device.cpu) .cpu else .{ .cuda = .{ .capacity_bytes = device.pool_bytes, .device_bytes = device.device_bytes } }) };
        errdefer owner.client.deinit();
        const cpu = try owner.client.isCpu();
        if (cpu != device.cpu or (!cpu and !std.ascii.eqlIgnoreCase(try owner.client.platformName(), "cuda"))) return error.InvalidArtifact;
        if (!owner.client.api.hasCompiledMemoryStats()) return error.UnsupportedPjrtFeature;
        return owner;
    }

    /// Stream one verified input at a time; provider-owned representations stay
    /// charged, but the loader never needs a second copy of the entire package.
    pub fn read(self: *@This(), index: usize, artifact: compiled_artifact.Manifest) ![]u8 {
        if (index == 0) {
            const bytes = self.prefill_bytes;
            self.prefill_bytes = &.{};
            return bytes;
        }
        return readVerified(self.allocator, artifact, self.encoded_sizes[index]);
    }

    pub fn loaded(self: *@This()) !void {
        self.allocator.free(self.encoded_sizes);
        self.encoded_sizes = &.{};
        try self.staging.retain(.{ .host_weight_bytes = self.encoded_total });
    }

    pub fn deinit(self: *@This()) void {
        self.client.deinit();
        self.allocator.free(self.prefill_bytes);
        self.allocator.free(self.encoded_sizes);
        self.staging.release();
        self.device.deinit();
        self.lifetime.deinit();
        self.allocator.destroy(self);
    }
};
