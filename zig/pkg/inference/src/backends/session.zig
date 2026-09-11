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
const ops = @import("../ops/ops.zig");
const Tensor = @import("tensor.zig").Tensor;
const TensorInfo = @import("tensor.zig").TensorInfo;
const BackendType = @import("backends.zig").BackendType;
const memory = @import("../runtime/tier/memory.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const Interruption = @import("../execution_control.zig").Interruption;

fn unsupportedControlledRun(
    _: *anyopaque,
    _: []const Tensor,
    _: std.mem.Allocator,
    _: InferenceExecutionControl,
) anyerror![]Tensor {
    return error.UncontrolledSession;
}

pub const ResidentInput = struct {
    value: ops.CT,
    backend: *const ops.ComputeBackend,
    estimated_bytes: usize = 0,
    shape: []const i64 = &.{},
};

pub const ResidentTextPooling = enum { mean, cls, max, last };

pub const ResidentTextEmbeddingRequest = struct {
    pooling: ResidentTextPooling,
    normalize: bool,
};

pub const ResidentOutputs = struct {
    outputs: []ops.CT,
    backend: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    backend_owner: ?*anyopaque = null,
    deinit_backend_owner: ?*const fn (owner: *anyopaque, allocator: std.mem.Allocator) void = null,
    resource_lease: ?memory.AdmissionLease = null,

    pub fn deinit(self: *ResidentOutputs) void {
        for (self.outputs, 0..) |output, idx| {
            var seen = false;
            for (self.outputs[0..idx]) |prev| {
                if (prev == output) {
                    seen = true;
                    break;
                }
            }
            if (!seen) self.backend.free(output);
        }
        self.allocator.free(self.outputs);
        if (self.resource_lease) |*lease| lease.release();
        self.outputs = &.{};
        if (self.backend_owner) |owner| {
            if (self.deinit_backend_owner) |deinit_owner| deinit_owner(owner, self.allocator);
        }
        self.backend_owner = null;
        self.deinit_backend_owner = null;
        self.resource_lease = null;
    }
};

/// Allocation-free request admission attached by ModelManager to serving
/// sessions. The static workspace estimate covers model intermediates that
/// cannot be derived from public output shapes; tensor-derived bytes scale it
/// for dynamic batches and sequence lengths.
pub const RunAdmission = struct {
    pub const ModelProfile = struct {
        hidden_size: usize = 0,
        intermediate_size: usize = 0,
        attention_heads: usize = 0,
        quadratic_attention: bool = false,
    };

    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    static_workspace_bytes: usize,
    backend_workspace_reserved: bool = false,
    model_profile: ModelProfile = .{},
    /// Serving sessions must retain the default live-memory pressure check.
    /// Deterministic unit tests may disable it when exercising admission
    /// accounting independently of the host's current memory pressure.
    check_live_memory: bool = true,

    fn acquire(
        self: RunAdmission,
        inputs: []const Tensor,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        const request = try RunRequest.fromTensors(inputs);
        return self.acquireAmounts(try self.estimateRequest(request, output_info));
    }

    fn acquireRequest(
        self: RunAdmission,
        request: RunRequest,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        return self.acquireAmounts(try self.estimateRequest(request, output_info));
    }

    fn acquireResidentInputs(
        self: RunAdmission,
        inputs: []const ResidentInput,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        return self.acquireAmounts(try self.estimateResidentAmounts(inputs, output_info));
    }

    fn acquireAmounts(
        self: RunAdmission,
        amounts: memory.AdmissionAmounts,
    ) !memory.AdmissionLease {
        if (self.controller.consumeForcedRunDenialForTesting()) {
            std.log.warn("test-only forced inference run admission denial", .{});
            return error.ResourceTemporarilyUnavailable;
        }
        return self.controller.tryAcquire(
            self.backend_class,
            self.limits,
            amounts,
            self.check_live_memory,
        );
    }

    fn estimateResidentAmounts(
        self: RunAdmission,
        inputs: []const ResidentInput,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        var input_bytes: usize = 0;
        for (inputs) |input| input_bytes = try addBytes(input_bytes, input.estimated_bytes);
        return self.estimateRequest(.{
            .batch = residentShapeDimension(inputs, 0),
            .sequence = residentShapeDimension(inputs, 1),
            .input_bytes = input_bytes,
        }, output_info);
    }

    fn estimateAmounts(
        self: RunAdmission,
        inputs: []const Tensor,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        return self.estimateRequest(try RunRequest.fromTensors(inputs), output_info);
    }

    pub fn estimateRequest(
        self: RunAdmission,
        request: RunRequest,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        const input_bytes = request.input_bytes;
        const reference_shape = [_]i64{
            std.math.cast(i64, request.batch) orelse return error.ResourceLimitExceeded,
            std.math.cast(i64, request.sequence) orelse return error.ResourceLimitExceeded,
        };
        const output_bytes = request.output_bytes orelse try estimatedOutputBytes(&reference_shape, output_info);
        if (request.output_kv_bytes > output_bytes) return error.ResourceLimitExceeded;
        const dynamic_base = try addBytes(input_bytes, output_bytes);
        const dynamic_workspace = try mulBytes(dynamic_base, 6);
        const workspace = @max(
            @max(self.static_workspace_bytes, request.workspace_bytes),
            @max(dynamic_workspace, try self.profiledWorkspace(request)),
        );
        const resident_input_bytes = try addBytes(request.host_preprocess_bytes, input_bytes);
        // A preceding host-only permit may remain live across execution. Its
        // retained bytes are an explicit credit, not permission to hide
        // outputs or backend workspace from this run reservation.
        if (request.pre_admitted_host_bytes > resident_input_bytes)
            return error.ResourceLimitExceeded;
        const host_output_peak = try mulBytes(output_bytes - request.output_kv_bytes, 2);
        const host_kv_peak = try mulBytes(request.output_kv_bytes, 2);
        const host_io_peak = try addBytes(
            resident_input_bytes - request.pre_admitted_host_bytes,
            host_output_peak,
        );

        return switch (self.backend_class) {
            .cpu => .{
                .host_scratch_bytes = try addBytes(host_io_peak, workspace),
                .host_kv_bytes = host_kv_peak,
            },
            .gpu => .{
                // Request inputs and materialized outputs occupy shared host
                // RAM. Device staging, activations, and outputs share the
                // backend workspace.
                .host_scratch_bytes = host_io_peak,
                .host_kv_bytes = host_kv_peak,
                .backend_scratch_bytes = if (self.backend_workspace_reserved)
                    0
                else
                    workspace,
            },
        };
    }

    fn profiledWorkspace(self: RunAdmission, request: RunRequest) !usize {
        const profile = self.model_profile;
        if (profile.hidden_size == 0 or request.batch == 0 or request.sequence == 0)
            return 0;

        const tokens = try mulBytes(request.batch, request.sequence);
        // Attention and FFN phases are sequential, so reserve the larger peak
        // rather than summing mutually exclusive intermediates.
        const attention_floats = try mulBytes(profile.hidden_size, 6);
        const ffn_floats = try addBytes(
            try mulBytes(profile.hidden_size, 2),
            try mulBytes(profile.intermediate_size, 3),
        );
        const floats_per_token = @max(attention_floats, ffn_floats);
        var peak = try mulBytes(
            try mulBytes(tokens, floats_per_token),
            @sizeOf(f32),
        );

        if (profile.quadratic_attention and profile.attention_heads > 0) {
            const score_elements = try mulBytes(
                try mulBytes(
                    try mulBytes(request.batch, profile.attention_heads),
                    request.sequence,
                ),
                request.sequence,
            );
            peak = @max(peak, try mulBytes(score_elements, @sizeOf(f32)));
        }
        return peak;
    }
};

pub const RunRequest = struct {
    batch: usize = 1,
    sequence: usize = 1,
    input_bytes: usize = 0,
    /// Resolved physical stage geometry; null retains graph metadata inference.
    output_bytes: ?usize = null,
    output_kv_bytes: usize = 0,
    workspace_bytes: usize = 0,
    host_preprocess_bytes: usize = 0,
    /// Host input bytes still covered by a separate live permit. This credit
    /// is limited to input + preprocessing residency and lets callers compose
    /// CPU preprocessing with backend execution without a release/reacquire
    /// gap or transient double charge.
    pre_admitted_host_bytes: usize = 0,

    pub fn fromTensors(tensors: []const Tensor) !RunRequest {
        var input_bytes: usize = 0;
        for (tensors) |tensor| input_bytes = try addBytes(input_bytes, tensor.data.len);
        const shape = if (tensors.len > 0) tensors[0].shape else &.{};
        return .{
            .batch = positiveDimension(shape, 0),
            .sequence = positiveDimension(shape, 1),
            .input_bytes = input_bytes,
        };
    }
};

pub const RunGeometry = struct {
    sequence: usize,
    output_bytes: usize,
    output_kv_bytes: usize = 0,
    workspace_bytes: usize = 0,
};

/// A pipeline-qualified projection, independent of the backend implementing it.
/// The named input determines sequence geometry, not its position in an array.
/// Used for imported seq2seq stages whose symbolic outputs omit the transform.
/// Allocation-free shape view shared by preparation and physical execution.
/// Geometry callbacks cannot inspect tensor payloads or require materialization.
pub const ShapeInputs = union(enum) {
    tensors: []const Tensor,
    shapes: []const TensorInfo,

    pub fn len(self: @This()) usize {
        return switch (self) {
            .tensors => |v| v.len,
            .shapes => |v| v.len,
        };
    }
    pub fn get(self: @This(), i: usize) TensorInfo {
        return switch (self) {
            .tensors => |v| .{ .name = v[i].name, .shape = v[i].shape, .dtype = v[i].dtype },
            .shapes => |v| v[i],
        };
    }
    pub fn named(self: @This(), name: []const u8) ?TensorInfo {
        for (0..self.len()) |i| {
            const input = self.get(i);
            if (std.mem.eql(u8, input.name, name)) return input;
        }
        return null;
    }
};

pub const SequenceOutputGeometry = struct {
    input_name: []const u8,
    sequence_axis: usize = 1,
    sequence_divisor: usize = 1,
    width: usize,

    fn resolve(self: @This(), inputs: ShapeInputs, batch: usize) !RunGeometry {
        if (self.width == 0 or self.sequence_divisor == 0) return error.InvalidInputShape;
        for (0..inputs.len()) |i| {
            const input = inputs.get(i);
            if (!std.mem.eql(u8, input.name, self.input_name)) continue;
            if (input.shape.len <= self.sequence_axis or input.shape[self.sequence_axis] <= 0) return error.InvalidInputShape;
            const sequence: usize = @intCast(input.shape[self.sequence_axis]);
            const output_sequence = (try addBytes(sequence, self.sequence_divisor - 1)) / self.sequence_divisor;
            return .{ .sequence = sequence, .output_bytes = try addBytes(try mulBytes(try mulBytes(try mulBytes(batch, output_sequence), self.width), @sizeOf(f32)), 3 * @sizeOf(i64)) };
        }
        return error.MissingInputs;
    }
};

/// Explicit merged seq2seq cache ABI. Cache axes are [batch, heads, time, dim];
/// self-attention time grows, while cross-attention time is encoder-context time.
pub const CachedDecoderGeometry = struct {
    vocab_size: usize,

    fn resolve(self: @This(), inputs: ShapeInputs, outputs: []const TensorInfo, batch: usize) !RunGeometry {
        const ids = inputs.named("input_ids") orelse return error.MissingInputs;
        const encoder = inputs.named("encoder_hidden_states") orelse return error.MissingInputs;
        if (ids.shape.len != 2 or encoder.shape.len != 3 or ids.shape[1] <= 0 or encoder.shape[1] <= 0) return error.InvalidInputShape;
        const tokens: usize = @intCast(ids.shape[1]);
        const context: usize = @intCast(encoder.shape[1]);
        var bytes: usize = 0;
        var cache_bytes: usize = 0;
        var sequence = @max(tokens, context);
        for (outputs) |output| {
            if (std.mem.eql(u8, output.name, "logits")) {
                bytes = try addBytes(bytes, try mulBytes(try mulBytes(try mulBytes(batch, tokens), self.vocab_size), output.dtype.byteSize()));
            } else {
                const prefix = "present.";
                if (!std.mem.startsWith(u8, output.name, prefix) or output.shape.len != 4) return error.UnresolvedOutputGeometry;
                const suffix = output.name[prefix.len..];
                var past: ?TensorInfo = null;
                for (0..inputs.len()) |i| {
                    const input = inputs.get(i);
                    if (std.mem.startsWith(u8, input.name, "past_key_values.") and std.mem.eql(u8, input.name["past_key_values.".len..], suffix)) {
                        past = input;
                        break;
                    }
                }
                const cached = past orelse return error.MissingInputs;
                if (cached.shape.len != 4 or cached.shape[1] <= 0 or cached.shape[2] < 0 or cached.shape[3] <= 0) return error.InvalidInputShape;
                const cross = std.mem.indexOf(u8, suffix, ".encoder.") != null;
                const time = if (cross) context else try addBytes(@intCast(cached.shape[2]), tokens);
                sequence = @max(sequence, time);
                const elements = try mulBytes(try mulBytes(try mulBytes(batch, @intCast(cached.shape[1])), time), @intCast(cached.shape[3]));
                const size = try mulBytes(elements, output.dtype.byteSize());
                bytes = try addBytes(bytes, size);
                cache_bytes = try addBytes(cache_bytes, try addBytes(size, try mulBytes(output.shape.len, @sizeOf(i64))));
            }
            bytes = try addBytes(bytes, try mulBytes(output.shape.len, @sizeOf(i64)));
        }
        return .{ .sequence = sequence, .output_bytes = bytes, .output_kv_bytes = cache_bytes };
    }
};

test "imported sequence geometry resolves transformed audio axes without native hooks" {
    const Probe = struct {
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{.{ .name = "hidden", .dtype = .f32, .shape = &.{ -1, -1, 384 } }};
        }
    };
    var marker: u8 = 0;
    var controller = memory.AdmissionController{};
    const session = Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = Probe.info, .backend = undefined, .close = undefined }, .output_geometry = .{ .input_name = "input_features", .sequence_axis = 2, .sequence_divisor = 2, .width = 384 }, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false } };
    const mel = Tensor{ .data = &.{}, .shape = &.{ 1, 80, 3000 }, .dtype = .f32, .name = "input_features", .allocator = std.testing.allocator, .owns_data = false, .owns_shape = false };
    const request = try session.planRun(&.{mel}, 8);
    try std.testing.expectEqual(@as(usize, 8 * 1500 * 384 * 4 + 24), request.output_bytes.?);
    const peak = try session.run_admission.?.estimateRequest(request, session.outputInfo());
    try std.testing.expect(peak.host_scratch_bytes >= request.output_bytes.?);
    const odd = Tensor{ .data = &.{}, .shape = &.{ 1, 80, 3001 }, .dtype = .f32, .name = "input_features", .allocator = std.testing.allocator, .owns_data = false, .owns_shape = false };
    try std.testing.expectEqual(@as(usize, 1501 * 384 * 4 + 24), (try session.planRun(&.{odd}, 1)).output_bytes.?);
    const shapes = try session.planShapes(&.{.{ .name = "input_features", .dtype = .f32, .shape = &.{ 8, 80, 3000 } }}, 8);
    try std.testing.expectEqual(request.output_bytes, shapes.output_bytes);
    try std.testing.expectEqual(@as(usize, 8 * 80 * 3000 * 4), shapes.input_bytes);
}

test "shape-only planning shares native geometry and workspace with execution" {
    const Probe = struct {
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1, -1 } }};
        }
        fn geometry(_: *anyopaque, inputs: ShapeInputs, batch: usize) !?RunGeometry {
            const ids = inputs.named("input_ids").?;
            const seq: usize = @intCast(ids.shape[1]);
            return .{ .sequence = seq, .output_bytes = batch * seq * 32768 * 4 + 24, .workspace_bytes = batch * seq * seq * 8 };
        }
    };
    var marker: u8 = 0;
    const session = Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = Probe.info, .backend = undefined, .close = undefined, .runGeometry = Probe.geometry } };
    var input = try Tensor.initInt64(std.testing.allocator, "input_ids", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    const physical = try session.planRun(&.{input}, 8);
    const planned = try session.planShapes(&.{.{ .name = input.name, .dtype = input.dtype, .shape = &.{ 8, 2 } }}, 8);
    try std.testing.expectEqualDeep(physical, planned);
    try std.testing.expectEqual(@as(usize, 8 * 2 * 32768 * 4 + 24), planned.output_bytes.?);
    try std.testing.expectEqual(@as(usize, 8 * 2 * 2 * 8), planned.workspace_bytes);
}

fn addBytes(lhs: usize, rhs: usize) !usize {
    return std.math.add(usize, lhs, rhs) catch error.ResourceLimitExceeded;
}

fn mulBytes(lhs: usize, rhs: usize) !usize {
    return std.math.mul(usize, lhs, rhs) catch error.ResourceLimitExceeded;
}

pub fn estimatedOutputBytes(
    reference_shape: []const i64,
    output_info: []const TensorInfo,
) !usize {
    var total: usize = 0;
    for (output_info) |info| {
        var elements: usize = 1;
        for (info.shape, 0..) |declared_dim, axis| {
            const resolved_dim: usize = if (declared_dim > 0)
                std.math.cast(usize, declared_dim) orelse
                    return error.ResourceLimitExceeded
            else if (axis < reference_shape.len and reference_shape[axis] > 0)
                std.math.cast(usize, reference_shape[axis]) orelse
                    return error.ResourceLimitExceeded
            else
                1;
            elements = try mulBytes(elements, resolved_dim);
        }
        const bytes = try mulBytes(elements, info.dtype.byteSize());
        total = try addBytes(total, bytes);
    }
    return total;
}

fn positiveDimension(shape: []const i64, axis: usize) usize {
    if (axis >= shape.len or shape[axis] <= 0) return 1;
    return std.math.cast(usize, shape[axis]) orelse 1;
}

fn residentShapeDimension(inputs: []const ResidentInput, axis: usize) usize {
    if (inputs.len == 0) return 1;
    return positiveDimension(inputs[0].shape, axis);
}

const OutputAdmission = struct {
    allocator: std.mem.Allocator,
    lease: memory.AdmissionLease,
    remaining: std.atomic.Value(usize),
    entries: []Entry,
    credits: OutputCredits,
    mutex: std.atomic.Mutex = .unlocked,
    const Entry = struct { owner: *OutputAdmission, index: usize };

    fn release(raw: *anyopaque) void {
        const entry: *Entry = @ptrCast(@alignCast(raw));
        const self = entry.owner;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.credits.release(entry.index, &self.lease);
        self.mutex.unlock();
        const previous = self.remaining.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        self.lease.release();
        self.credits.deinit(self.allocator);
        self.allocator.free(self.entries);
        self.allocator.destroy(self);
    }
};

pub fn retainedOutputAmounts(outputs: []const Tensor, cache_qualified: bool) !memory.AdmissionAmounts {
    var amounts = memory.AdmissionAmounts{};
    for (outputs) |output| {
        const bytes = try addBytes(output.data.len, try mulBytes(output.shape.len, @sizeOf(i64)));
        if (cache_qualified and std.mem.startsWith(u8, output.name, "present.")) {
            amounts.host_kv_bytes = try addBytes(amounts.host_kv_bytes, bytes);
        } else amounts.host_scratch_bytes = try addBytes(amounts.host_scratch_bytes, bytes);
    }
    return amounts;
}

/// Coalesce accounting for a decode step's related cache columns. Tensor
/// storage is freed immediately; credit stays conservative until the cohort's
/// final column dies. Non-cache outputs retain independent credit lifetimes.
/// The owner serializes release calls and releases the final lease itself.
pub const OutputCredits = struct {
    entries: []Entry,
    remaining_groups: usize,
    covered: bool,
    const Entry = struct { group: usize = 0, remaining: usize = 0, amounts: memory.AdmissionAmounts = .{} };

    fn cacheGroup(name: []const u8) ?bool {
        if (!std.mem.startsWith(u8, name, "present.")) return null;
        return std.mem.indexOf(u8, name, ".encoder.") != null;
    }

    pub fn init(allocator: std.mem.Allocator, outputs: []const Tensor, cache_qualified: bool, covered: bool) !OutputCredits {
        const entries = try allocator.alloc(Entry, outputs.len);
        errdefer allocator.free(entries);
        @memset(entries, .{});
        var groups: usize = 0;
        var cache_groups: [2]?usize = .{ null, null };
        for (outputs, 0..) |output, index| {
            var group = index;
            if (cacheGroup(output.name)) |kind| {
                const slot = &cache_groups[@intFromBool(kind)];
                group = slot.* orelse index;
                slot.* = group;
            }
            entries[index].group = group;
            if (group == index) groups += 1;
            entries[group].remaining += 1;
            const amounts = try retainedOutputAmounts(&.{output}, cache_qualified);
            entries[group].amounts.host_scratch_bytes = try addBytes(entries[group].amounts.host_scratch_bytes, amounts.host_scratch_bytes);
            entries[group].amounts.host_kv_bytes = try addBytes(entries[group].amounts.host_kv_bytes, amounts.host_kv_bytes);
        }
        return .{ .entries = entries, .remaining_groups = groups, .covered = covered };
    }

    pub fn release(self: *OutputCredits, index: usize, lease: *memory.AdmissionLease) void {
        const group = &self.entries[self.entries[index].group];
        std.debug.assert(group.remaining > 0);
        group.remaining -= 1;
        if (group.remaining != 0) return;
        self.remaining_groups -= 1;
        // The final group uses release(), never a redundant retain-then-release.
        if (self.remaining_groups == 0 or !self.covered) return;
        var retained = lease.amounts;
        retained.host_scratch_bytes -= group.amounts.host_scratch_bytes;
        retained.host_kv_bytes -= group.amounts.host_kv_bytes;
        if (!std.meta.eql(retained, lease.amounts)) lease.retain(retained) catch {};
    }

    pub fn deinit(self: *OutputCredits, allocator: std.mem.Allocator) void {
        allocator.free(self.entries);
    }
};

/// Session represents a loaded model that can run forward passes.
/// This is the core abstraction all backends implement.
pub const Session = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    run_admission: ?RunAdmission = null,
    output_geometry: ?SequenceOutputGeometry = null,
    cached_decoder_geometry: ?CachedDecoderGeometry = null,
    /// Explicit stage contract: these small control tensors are equal across
    /// a physical batch and passed once, never concatenated as independent rows.
    broadcast_inputs: []const []const u8 = &.{},
    /// Borrowed from the model/runtime owner; stable for every session copy.
    execution_gate: ?*std.atomic.Mutex = null,

    pub const VTable = struct {
        run: *const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor,
        inputInfo: *const fn (ptr: *anyopaque) []const TensorInfo,
        outputInfo: *const fn (ptr: *anyopaque) []const TensorInfo,
        /// Explicit stage-level row independence for multi-entry-point sessions
        /// whose static metadata cannot describe every invocation. A false
        /// result vetoes batching; null uses the pipeline opt-in plus metadata.
        independentBatchRows: ?*const fn (ptr: *anyopaque, inputs: []const Tensor) bool = null,
        /// Allocation-free stage planning. Inputs describe logical geometry;
        /// batch is the requested physical row count (possibly fused).
        runGeometry: ?*const fn (ptr: *anyopaque, inputs: ShapeInputs, batch: usize) anyerror!?RunGeometry = null,
        backend: *const fn (ptr: *anyopaque) BackendType,
        interruption: ?*const fn (ptr: *anyopaque) Interruption = null,
        close: *const fn (ptr: *anyopaque) void,
        runResident: ?*const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror!?ResidentOutputs = null,
        runResidentInputs: ?*const fn (ptr: *anyopaque, inputs: []const ResidentInput, allocator: std.mem.Allocator) anyerror!?ResidentOutputs = null,
        runResidentTextEmbedding: ?*const fn (ptr: *anyopaque, inputs: []const Tensor, request: ResidentTextEmbeddingRequest, allocator: std.mem.Allocator) anyerror!?ResidentOutputs = null,
        /// Every backend must provide the controlled entry point. Backends that
        /// cannot cooperatively interrupt their native call still check before
        /// and after it; the owner can then route those backends through the
        /// supervised process boundary without a silent uncontrolled fallback.
        runWithControl: *const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: InferenceExecutionControl) anyerror![]Tensor = &unsupportedControlledRun,
        runResidentWithControl: ?*const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: InferenceExecutionControl) anyerror!?ResidentOutputs = null,
        runResidentInputsWithControl: ?*const fn (ptr: *anyopaque, inputs: []const ResidentInput, allocator: std.mem.Allocator, control: InferenceExecutionControl) anyerror!?ResidentOutputs = null,
        runResidentTextEmbeddingWithControl: ?*const fn (ptr: *anyopaque, inputs: []const Tensor, request: ResidentTextEmbeddingRequest, allocator: std.mem.Allocator, control: InferenceExecutionControl) anyerror!?ResidentOutputs = null,
    };

    /// Run a forward pass with the given input tensors.
    pub fn run(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
        var resource_lease = if (self.run_admission) |admission|
            try admission.acquireRequest(try self.planRun(inputs, null), self.outputInfo())
        else
            null;
        errdefer if (resource_lease) |*lease| lease.release();
        const outputs = try self.vtable.run(self.ptr, inputs, allocator);
        errdefer deinitTensorSlice(outputs, allocator);
        return attachOutputAdmission(outputs, allocator, &resource_lease);
    }

    pub fn runWithControl(
        self: Session,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) ![]Tensor {
        const active = control orelse return self.run(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.interruption());
        defer hard_cancellation.deinit();
        var resource_lease = if (self.run_admission) |admission|
            try admission.acquireRequest(try self.planRun(inputs, null), self.outputInfo())
        else
            null;
        errdefer if (resource_lease) |*lease| lease.release();
        active.check() catch |err| {
            if (resource_lease) |*lease| lease.release();
            resource_lease = null;
            return err;
        };
        const outputs = try self.vtable.runWithControl(self.ptr, inputs, allocator, active);
        errdefer deinitTensorSlice(outputs, allocator);
        try active.check();
        return attachOutputAdmission(outputs, allocator, &resource_lease);
    }

    /// Borrows outputs and the lease until success. On error the caller still
    /// owns both; on success the tensors own the retained admission lease.
    fn attachOutputAdmission(
        outputs: []Tensor,
        allocator: std.mem.Allocator,
        resource_lease: *?memory.AdmissionLease,
    ) ![]Tensor {
        if (resource_lease.* == null) return outputs;
        if (outputs.len == 0) {
            resource_lease.*.?.release();
            return outputs;
        }
        var retained_output_bytes: usize = 0;
        for (outputs) |output| {
            retained_output_bytes = addBytes(
                retained_output_bytes,
                output.data.len,
            ) catch std.math.maxInt(usize);
            retained_output_bytes = addBytes(
                retained_output_bytes,
                mulBytes(output.shape.len, @sizeOf(i64)) catch
                    std.math.maxInt(usize),
            ) catch std.math.maxInt(usize);
        }
        const retained = try retainedOutputAmounts(outputs, resource_lease.*.?.amounts.host_kv_bytes > 0);
        if (resource_lease.*.?.amounts.host_kv_bytes > 0) {
            try resource_lease.*.?.retain(retained);
        } else resource_lease.*.?.retain(retained) catch {};

        const output_admission = try allocator.create(OutputAdmission);
        errdefer allocator.destroy(output_admission);
        const entries = try allocator.alloc(OutputAdmission.Entry, outputs.len);
        errdefer allocator.free(entries);
        var credits = try OutputCredits.init(allocator, outputs, resource_lease.*.?.amounts.host_kv_bytes > 0, retained.host_scratch_bytes <= resource_lease.*.?.amounts.host_scratch_bytes and retained.host_kv_bytes <= resource_lease.*.?.amounts.host_kv_bytes);
        errdefer credits.deinit(allocator);
        for (entries, 0..) |*entry, index| entry.* = .{
            .owner = output_admission,
            .index = index,
        };
        output_admission.* = .{
            .allocator = allocator,
            .lease = resource_lease.*.?,
            .remaining = std.atomic.Value(usize).init(outputs.len),
            .entries = entries,
            .credits = credits,
        };
        resource_lease.* = null;
        for (outputs, entries) |*output, *entry| {
            std.debug.assert(output.lifetime == null);
            output.lifetime = .{
                .context = entry,
                .release = OutputAdmission.release,
            };
            if (output_admission.lease.amounts.hostTotalBytes() >= retained_output_bytes) {
                output.shared_storage = output.data;
                output.admitted_storage_domain = output_admission.lease.controller;
            }
        }
        return outputs;
    }

    /// Reserve the full request peak before preprocessing allocates input
    /// buffers. The permit owns that lease and executes without double-counting.
    pub fn admit(self: Session, request: RunRequest) !RunPermit {
        return .{
            .session = self,
            .request = request,
            .lease = if (self.run_admission) |admission|
                try admission.acquireRequest(request, self.outputInfo())
            else
                null,
        };
    }

    pub fn planRun(self: Session, inputs: []const Tensor, batch_override: ?usize) !RunRequest {
        var request = try RunRequest.fromTensors(inputs);
        if (batch_override) |batch| {
            request.input_bytes = 0;
            for (inputs) |input| {
                const bytes = if (self.isBroadcastInput(input.name)) input.data.len else blk: {
                    if (input.data.len % request.batch != 0) return error.InvalidInputShape;
                    break :blk try mulBytes(input.data.len / request.batch, batch);
                };
                request.input_bytes = try addBytes(request.input_bytes, bytes);
            }
            request.batch = batch;
        }
        try self.resolveRunGeometry(.{ .tensors = inputs }, &request);
        if (batch_override == null) request.pre_admitted_host_bytes = try self.inputResidencyCredit(inputs);
        return request;
    }

    pub fn isBroadcastInput(self: Session, name: []const u8) bool {
        for (self.broadcast_inputs) |input| if (std.mem.eql(u8, input, name)) return true;
        return false;
    }

    pub fn planShapes(self: Session, inputs: []const TensorInfo, batch: usize) !RunRequest {
        if (inputs.len == 0 or batch == 0) return error.InvalidInputShape;
        var request = RunRequest{ .batch = batch, .sequence = positiveDimension(inputs[0].shape, 1) };
        for (inputs) |input| {
            var elements: usize = 1;
            for (input.shape) |dim| {
                if (dim < 0) return error.UnresolvedOutputGeometry;
                elements = try mulBytes(elements, @intCast(dim));
            }
            request.input_bytes = try addBytes(request.input_bytes, try mulBytes(elements, input.dtype.byteSize()));
        }
        try self.resolveRunGeometry(.{ .shapes = inputs }, &request);
        return request;
    }

    fn resolveRunGeometry(self: Session, inputs: ShapeInputs, request: *RunRequest) !void {
        const resolved_geometry = if (self.vtable.runGeometry) |geometry| try geometry(self.ptr, inputs, request.batch) else null;
        const concrete = resolved_geometry orelse if (self.cached_decoder_geometry) |geometry| try geometry.resolve(inputs, self.outputInfo(), request.batch) else if (self.output_geometry) |geometry| try geometry.resolve(inputs, request.batch) else null;
        if (concrete) |resolved| {
            request.sequence = resolved.sequence;
            request.output_bytes = resolved.output_bytes;
            request.output_kv_bytes = resolved.output_kv_bytes;
            request.workspace_bytes = resolved.workspace_bytes;
        }
    }

    pub fn inputResidencyCredit(self: Session, inputs: []const Tensor) !usize {
        const admission = self.run_admission orelse return 0;
        var bytes: usize = 0;
        for (inputs) |input| {
            if (input.admitted_storage_domain != @as(*anyopaque, admission.controller)) continue;
            const storage = input.shared_storage orelse continue;
            const base = @intFromPtr(storage.ptr);
            const address = @intFromPtr(input.data.ptr);
            if (address < base or address - base > storage.len or input.data.len > storage.len - (address - base)) continue;
            bytes = try addBytes(bytes, input.data.len);
        }
        return bytes;
    }

    /// Detach a lone surviving row from larger immutable host-cache columns.
    /// All candidates must be exclusive; reserve old/new overlap before copying
    /// and commit only after every copy and ownership descriptor succeeds.
    pub fn compactExclusiveRows(self: Session, allocator: std.mem.Allocator, tensors: []Tensor, minimum_saving: usize, control: ?InferenceExecutionControl) !bool {
        var count: usize = 0;
        var bytes: usize = 0;
        var saving: usize = 0;
        for (tensors) |tensor| {
            const storage = tensor.shared_storage orelse continue;
            if (tensor.data.len == 0 or tensor.data.len > storage.len / 2) continue;
            const lifetime = tensor.lifetime orelse return false;
            const exclusive = lifetime.is_exclusive orelse return false;
            if (!exclusive(lifetime.context) or tensor.dtype != .f32 or !std.mem.startsWith(u8, tensor.name, "present.")) return false;
            count += 1;
            bytes = try addBytes(bytes, try addBytes(tensor.data.len, try mulBytes(tensor.shape.len, @sizeOf(i64))));
            saving = try addBytes(saving, storage.len - tensor.data.len);
        }
        if (count == 0 or saving < minimum_saving) return false;
        if (control) |active| try active.check();
        var lease: ?memory.AdmissionLease = if (self.run_admission) |admission| try admission.acquireAmounts(.{
            .host_kv_bytes = bytes,
            .host_scratch_bytes = try addBytes(@sizeOf(OutputAdmission), try mulBytes(count, @sizeOf(Tensor) + @sizeOf(usize) + @sizeOf(OutputAdmission.Entry) + @sizeOf(OutputCredits.Entry))),
        }) else null;
        defer if (lease) |*active| active.release();
        const copies = try allocator.alloc(Tensor, count);
        defer allocator.free(copies);
        const indices = try allocator.alloc(usize, count);
        defer allocator.free(indices);
        var initialized: usize = 0;
        errdefer for (copies[0..initialized]) |*copy| copy.deinit();
        for (tensors, 0..) |tensor, index| {
            const storage = tensor.shared_storage orelse continue;
            if (tensor.data.len == 0 or tensor.data.len > storage.len / 2) continue;
            if (control) |active| try active.check();
            copies[initialized] = try Tensor.initFloat32(allocator, tensor.name, tensor.shape, tensor.asFloat32());
            indices[initialized] = index;
            initialized += 1;
        }
        if (control) |active| try active.check();
        _ = try attachOutputAdmission(copies, allocator, &lease);
        for (copies, indices) |copy, index| {
            tensors[index].deinit();
            tensors[index] = copy;
        }
        return true;
    }

    /// Allocation-free planning against permanent session limits. This is not
    /// a reservation: live pressure is still checked by admit at execution.
    /// Callers must include all retained input storage, without lease credits.
    pub fn fitsRun(self: Session, request: RunRequest) !bool {
        const admission = self.run_admission orelse return true;
        var uncredited = request;
        uncredited.pre_admitted_host_bytes = 0;
        const amounts = try admission.estimateRequest(uncredited, self.outputInfo());
        return amounts.fitsLimits(admission.limits);
    }

    /// Reserve tokenizer/preprocessing memory independently from the backend
    /// execution workspace. Dynamic text encoders can hold this lease while
    /// discovering the actual padded sequence length, then admit the backend
    /// run using that length instead of the model's maximum context.
    pub fn admitHostPreprocess(self: Session, host_scratch_bytes: usize) !RunPermit {
        return .{
            .session = self,
            .lease = if (self.run_admission) |admission|
                try admission.acquireAmounts(.{ .host_scratch_bytes = host_scratch_bytes })
            else
                null,
        };
    }

    pub fn admitResidentInputs(self: Session, inputs: []const ResidentInput) !RunPermit {
        return .{
            .session = self,
            .lease = if (self.run_admission) |admission|
                try admission.acquireResidentInputs(inputs, self.outputInfo())
            else
                null,
        };
    }

    pub fn inputInfo(self: Session) []const TensorInfo {
        return self.vtable.inputInfo(self.ptr);
    }

    pub fn outputInfo(self: Session) []const TensorInfo {
        return self.vtable.outputInfo(self.ptr);
    }

    pub fn backend(self: Session) BackendType {
        return self.vtable.backend(self.ptr);
    }

    pub fn interruption(self: Session) Interruption {
        if (self.vtable.interruption) |classify| return classify(self.ptr);
        return self.backend().executionInterruption();
    }

    pub fn close(self: Session) void {
        self.vtable.close(self.ptr);
    }

    pub fn runResident(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.vtable.runResident) |run_resident| {
            var resource_lease = if (self.run_admission) |admission|
                try admission.acquire(inputs, self.outputInfo())
            else
                null;
            errdefer if (resource_lease) |*lease| lease.release();
            var outputs = (try run_resident(self.ptr, inputs, allocator)) orelse {
                if (resource_lease) |*lease| lease.release();
                return null;
            };
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = resource_lease;
            return outputs;
        }
        return null;
    }

    pub fn runResidentInputs(self: Session, inputs: []const ResidentInput, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.vtable.runResidentInputs) |run_resident_inputs| {
            var resource_lease = if (self.run_admission) |admission|
                try admission.acquireResidentInputs(inputs, self.outputInfo())
            else
                null;
            errdefer if (resource_lease) |*lease| lease.release();
            var outputs = (try run_resident_inputs(self.ptr, inputs, allocator)) orelse {
                if (resource_lease) |*lease| lease.release();
                return null;
            };
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = resource_lease;
            return outputs;
        }
        return null;
    }

    pub fn runResidentTextEmbedding(self: Session, inputs: []const Tensor, request: ResidentTextEmbeddingRequest, allocator: std.mem.Allocator) !?ResidentOutputs {
        const run_resident = self.vtable.runResidentTextEmbedding orelse return null;
        return run_resident(self.ptr, inputs, request, allocator);
    }
};

pub const RunPermit = struct {
    session: Session,
    lease: ?memory.AdmissionLease,
    request: ?RunRequest = null,
    execution_yielded: bool = false,

    fn resolveGeometry(self: *RunPermit, inputs: []const Tensor) !void {
        const request = self.request orelse return;
        if (self.session.vtable.runGeometry == null and self.session.output_geometry == null and self.session.cached_decoder_geometry == null) return;
        const plan = try self.session.planRun(inputs, null);
        if (plan.output_bytes == null) return;
        // A preprocessing permit can span differently sized execution windows.
        // Recheck each invocation; a first small window must not freeze the
        // output reservation for a later larger one. An execution permit whose
        // geometry already covers this invocation must not yield recursively.
        if (request.output_bytes) |bytes| {
            if (bytes >= plan.output_bytes.? and request.output_kv_bytes >= plan.output_kv_bytes and request.workspace_bytes >= plan.workspace_bytes and request.sequence >= plan.sequence) return;
        }
        if (!self.execution_yielded) _ = try self.yieldExecution();
        self.request.?.output_bytes = @max(request.output_bytes orelse 0, plan.output_bytes.?);
        self.request.?.output_kv_bytes = @max(request.output_kv_bytes, plan.output_kv_bytes);
        self.request.?.workspace_bytes = @max(request.workspace_bytes, plan.workspace_bytes);
        self.request.?.sequence = @max(request.sequence, plan.sequence);
    }

    /// Queueing retains inputs/preprocessing, not idle compute workspace.
    /// Every subsequent run reacquires execution while these bytes stay owned.
    pub fn yieldExecution(self: *RunPermit) !bool {
        if (self.execution_yielded) return true;
        const request = self.request orelse return false;
        const resident = (try addBytes(request.input_bytes, request.host_preprocess_bytes)) - request.pre_admitted_host_bytes;
        try self.retainHostBytes(resident);
        self.execution_yielded = true;
        return true;
    }

    fn acquireExecution(self: *RunPermit) !RunPermit {
        var request = self.request.?;
        request.pre_admitted_host_bytes = try addBytes(request.input_bytes, request.host_preprocess_bytes);
        return self.session.admit(request);
    }

    /// Drop transient forward scratch once only materialized outputs remain.
    pub fn retainOutputs(self: *RunPermit, outputs: []const Tensor) !void {
        if (self.lease) |*lease| try lease.retain(try retainedOutputAmounts(outputs, lease.amounts.host_kv_bytes > 0));
    }

    pub fn run(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) ![]Tensor {
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) return self.runWithControl(inputs, allocator, null);
        return self.session.vtable.run(self.session.ptr, inputs, allocator);
    }

    pub fn runWithControl(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) ![]Tensor {
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) {
            var execution = try self.acquireExecution();
            defer execution.deinit();
            const outputs = try execution.runWithControl(inputs, allocator, control);
            errdefer deinitTensorSlice(outputs, allocator);
            return Session.attachOutputAdmission(outputs, allocator, &execution.lease);
        }
        const active = control orelse return self.run(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        const outputs = try self.session.vtable.runWithControl(
            self.session.ptr,
            inputs,
            allocator,
            active,
        );
        errdefer deinitTensorSlice(outputs, allocator);
        try active.check();
        return outputs;
    }

    pub fn runResident(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        if (self.execution_yielded) return self.runResidentWithControl(inputs, allocator, null);
        const run_resident = self.session.vtable.runResident orelse return null;
        return run_resident(self.session.ptr, inputs, allocator);
    }

    pub fn runResidentWithControl(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) !?ResidentOutputs {
        if (self.execution_yielded) {
            var execution = try self.acquireExecution();
            defer execution.deinit();
            var outputs = (try execution.runResidentWithControl(inputs, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            return outputs;
        }
        const active = control orelse return self.runResident(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        var outputs = if (self.session.vtable.runResidentWithControl) |run_controlled|
            (try run_controlled(self.session.ptr, inputs, allocator, active)) orelse return null
        else
            return null;
        errdefer outputs.deinit();
        try active.check();
        return outputs;
    }

    pub fn runResidentInputs(
        self: *RunPermit,
        inputs: []const ResidentInput,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        if (self.execution_yielded) return self.runResidentInputsWithControl(inputs, allocator, null);
        const run_resident_inputs = self.session.vtable.runResidentInputs orelse
            return null;
        return run_resident_inputs(self.session.ptr, inputs, allocator);
    }

    pub fn runResidentInputsWithControl(
        self: *RunPermit,
        inputs: []const ResidentInput,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) !?ResidentOutputs {
        if (self.execution_yielded) {
            var execution = try self.acquireExecution();
            defer execution.deinit();
            var outputs = (try execution.runResidentInputsWithControl(inputs, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            return outputs;
        }
        const active = control orelse return self.runResidentInputs(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        const run_controlled = self.session.vtable.runResidentInputsWithControl orelse return null;
        var outputs = (try run_controlled(self.session.ptr, inputs, allocator, active)) orelse return null;
        errdefer outputs.deinit();
        try active.check();
        return outputs;
    }

    pub fn runResidentTextEmbedding(
        self: *RunPermit,
        inputs: []const Tensor,
        request: ResidentTextEmbeddingRequest,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        if (self.execution_yielded) return self.runResidentTextEmbeddingWithControl(inputs, request, allocator, null);
        const run_resident = self.session.vtable.runResidentTextEmbedding orelse return null;
        return run_resident(self.session.ptr, inputs, request, allocator);
    }

    /// Reduce a host-only preprocessing lease to the bytes that remain live
    /// as model inputs. The retained permit can then be composed with a run
    /// request carrying the same `pre_admitted_host_bytes` credit.
    pub fn retainHostBytes(self: *RunPermit, bytes: usize) !void {
        if (self.lease) |*lease| try lease.retain(.{ .host_scratch_bytes = bytes });
    }

    pub fn runResidentTextEmbeddingWithControl(
        self: *RunPermit,
        inputs: []const Tensor,
        request: ResidentTextEmbeddingRequest,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) !?ResidentOutputs {
        if (self.execution_yielded) {
            var execution = try self.acquireExecution();
            defer execution.deinit();
            var outputs = (try execution.runResidentTextEmbeddingWithControl(inputs, request, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            return outputs;
        }
        const active = control orelse return self.runResidentTextEmbedding(inputs, request, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        var outputs = if (self.session.vtable.runResidentTextEmbeddingWithControl) |run_controlled|
            (try run_controlled(self.session.ptr, inputs, request, allocator, active)) orelse return null
        else
            return null;
        errdefer outputs.deinit();
        try active.check();
        return outputs;
    }

    pub fn deinit(self: *RunPermit) void {
        if (self.lease) |*lease| lease.release();
        self.lease = null;
    }
};

fn deinitTensorSlice(tensors: []Tensor, allocator: std.mem.Allocator) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

test "session vtable layout" {
    // Ensure the vtable has all required function pointers.
    const info = @typeInfo(Session.VTable);
    try std.testing.expectEqual(@as(usize, 15), info.@"struct".fields.len);
    try std.testing.expect(@hasField(Session.VTable, "independentBatchRows"));
}

const AdmissionProbeSession = struct {
    controller: *memory.AdmissionController,
    observed_active_lease: bool = false,

    fn run(
        ptr: *anyopaque,
        _: []const Tensor,
        allocator: std.mem.Allocator,
    ) ![]Tensor {
        const self: *AdmissionProbeSession = @ptrCast(@alignCast(ptr));
        self.observed_active_lease =
            self.controller.snapshot().host_scratch_bytes > 0;
        const outputs = try allocator.alloc(Tensor, 1);
        errdefer allocator.free(outputs);
        outputs[0] = try Tensor.initFloat32(
            allocator,
            "output",
            &.{1},
            &.{1.0},
        );
        return outputs;
    }

    fn runWithControl(
        ptr: *anyopaque,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: InferenceExecutionControl,
    ) ![]Tensor {
        try control.check();
        const outputs = try run(ptr, inputs, allocator);
        errdefer deinitTensorSlice(outputs, allocator);
        try control.check();
        return outputs;
    }

    fn inputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }

    fn outputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }

    fn backend(_: *anyopaque) BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}

    const vtable = Session.VTable{
        .run = run,
        .runWithControl = runWithControl,
        .inputInfo = inputInfo,
        .outputInfo = outputInfo,
        .backend = backend,
        .close = close,
    };
};

const DeadlineProbeSession = struct {
    fn run(
        _: *anyopaque,
        _: []const Tensor,
        _: std.mem.Allocator,
    ) ![]Tensor {
        return error.UncontrolledRunUsed;
    }

    fn runWithControl(
        _: *anyopaque,
        _: []const Tensor,
        _: std.mem.Allocator,
        control: InferenceExecutionControl,
    ) ![]Tensor {
        while (true) {
            try control.check();
            std.atomic.spinLoopHint();
        }
    }

    fn inputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }

    fn outputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }

    fn backend(_: *anyopaque) BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}

    const vtable = Session.VTable{
        .run = run,
        .inputInfo = inputInfo,
        .outputInfo = outputInfo,
        .backend = backend,
        .close = close,
        .runWithControl = runWithControl,
    };
};

test "controlled blocked backend expires and admission unwinds" {
    var controller = memory.AdmissionController{};
    var probe: u8 = 0;
    const session = Session{
        .ptr = &probe,
        .vtable = &DeadlineProbeSession.vtable,
        .run_admission = .{
            .controller = &controller,
            .backend_class = .cpu,
            .limits = .{},
            .static_workspace_bytes = 1,
        },
    };
    var permit = try session.admit(.{ .batch = 1, .sequence = 1 });
    defer permit.deinit();
    try std.testing.expect(controller.snapshot().host_scratch_bytes > 0);
    const control = InferenceExecutionControl{
        .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_ms,
    };
    try std.testing.expectError(error.Timeout, permit.runWithControl(&.{}, std.testing.allocator, control));
    permit.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "run admission scales dynamic outputs and honors reserved backend workspace" {
    var controller = memory.AdmissionController{};
    var input_bytes = [_]u8{0} ** 64;
    const input = Tensor{
        .data = &input_bytes,
        .dtype = .i64,
        .shape = &.{ 2, 4 },
        .name = "input_ids",
        .allocator = std.testing.allocator,
        .owns_data = false,
        .owns_shape = false,
    };
    const outputs = [_]TensorInfo{.{
        .name = "last_hidden_state",
        .dtype = .f32,
        .shape = &.{ -1, -1, 8 },
    }};

    const cpu = RunAdmission{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
        .check_live_memory = false,
    };
    const cpu_amounts = try cpu.estimateAmounts(&.{input}, &outputs);
    try std.testing.expectEqual(@as(usize, 4672), cpu_amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), cpu_amounts.backend_scratch_bytes);

    const gpu = RunAdmission{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
        .backend_workspace_reserved = true,
    };
    const gpu_amounts = try gpu.estimateAmounts(&.{input}, &outputs);
    try std.testing.expectEqual(@as(usize, 576), gpu_amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), gpu_amounts.backend_scratch_bytes);
    const resident_shape = [_]i64{ 8, 4 };
    const resident_amounts = try gpu.estimateResidentAmounts(&.{.{
        .value = undefined,
        .backend = undefined,
        .estimated_bytes = 8 * 4 * 8,
        .shape = &resident_shape,
    }}, &outputs);
    try std.testing.expectEqual(@as(usize, 2304), resident_amounts.host_scratch_bytes);

    var probe = AdmissionProbeSession{ .controller = &controller };
    const admitted_session = Session{
        .ptr = &probe,
        .vtable = &AdmissionProbeSession.vtable,
        .run_admission = cpu,
    };
    const result = try admitted_session.run(&.{input}, std.testing.allocator);
    try std.testing.expect(probe.observed_active_lease);
    try std.testing.expect(
        controller.snapshot().host_scratch_bytes >= @sizeOf(f32),
    );
    for (result) |*output| output.deinit();
    std.testing.allocator.free(result);
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );

    var permit = try admitted_session.admit(.{
        .batch = 2,
        .sequence = 4,
        .input_bytes = 64,
        .host_preprocess_bytes = 128,
    });
    try std.testing.expect(controller.snapshot().host_scratch_bytes > 0);
    const permitted_result = try permit.run(&.{input}, std.testing.allocator);
    for (permitted_result) |*output| output.deinit();
    std.testing.allocator.free(permitted_result);
    try std.testing.expect(controller.snapshot().host_scratch_bytes > 0);
    permit.deinit();
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );

    var preprocess_permit = try admitted_session.admitHostPreprocess(128);
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    var execution_permit = try admitted_session.admit(.{
        .batch = 2,
        .sequence = 4,
        .input_bytes = 64,
    });
    try std.testing.expect(controller.snapshot().host_scratch_bytes > 128);
    execution_permit.deinit();
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    preprocess_permit.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());

    const composed_request = RunRequest{
        .batch = 2,
        .sequence = 4,
        .input_bytes = 64,
        .host_preprocess_bytes = 128,
    };
    const full_composed_amounts = try cpu.estimateRequest(
        composed_request,
        admitted_session.outputInfo(),
    );
    var retained_preprocess = try admitted_session.admitHostPreprocess(512);
    try retained_preprocess.retainHostBytes(192);
    try std.testing.expectEqual(@as(usize, 192), controller.snapshot().host_scratch_bytes);
    var credited_request = composed_request;
    credited_request.pre_admitted_host_bytes = 192;
    const credited_amounts = try cpu.estimateRequest(
        credited_request,
        admitted_session.outputInfo(),
    );
    try std.testing.expectEqual(
        full_composed_amounts.host_scratch_bytes - 192,
        credited_amounts.host_scratch_bytes,
    );
    var composed_run = try admitted_session.admit(credited_request);
    try std.testing.expectEqual(
        credited_amounts.host_scratch_bytes,
        composed_run.lease.?.amounts.host_scratch_bytes,
    );
    try std.testing.expectEqual(
        full_composed_amounts.host_scratch_bytes,
        controller.snapshot().host_scratch_bytes,
    );
    composed_run.deinit();
    try std.testing.expectEqual(@as(usize, 192), controller.snapshot().host_scratch_bytes);
    retained_preprocess.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    credited_request.pre_admitted_host_bytes = 193;
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        admitted_session.admit(credited_request),
    );

    const profiled = RunAdmission{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
        .model_profile = .{
            .hidden_size = 768,
            .intermediate_size = 3072,
            .attention_heads = 12,
            .quadratic_attention = true,
        },
    };
    const profiled_amounts = try profiled.estimateRequest(.{
        .batch = 16,
        .sequence = 512,
        .input_bytes = 16 * 512 * 16,
        .host_preprocess_bytes = 16 * 512 * 32,
    }, &outputs);
    try std.testing.expect(profiled_amounts.host_scratch_bytes > 256 * 1024 * 1024);
}

test "forced run admission denials are counted and recover" {
    var controller = memory.AdmissionController{};
    controller.configureForcedRunDenialsForTesting(2);

    var probe = AdmissionProbeSession{ .controller = &controller };
    const session = Session{
        .ptr = &probe,
        .vtable = &AdmissionProbeSession.vtable,
        .run_admission = .{
            .controller = &controller,
            .backend_class = .cpu,
            .limits = .{},
            .static_workspace_bytes = 1,
        },
    };

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        session.run(&.{}, std.testing.allocator),
    );
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        session.run(&.{}, std.testing.allocator),
    );
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expect(!probe.observed_active_lease);

    const outputs = try session.run(&.{}, std.testing.allocator);
    try std.testing.expect(probe.observed_active_lease);
    for (outputs) |*output| output.deinit();
    std.testing.allocator.free(outputs);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "session output admission row compaction rolls back allocation and capacity failures" {
    const Source = struct {
        tensor: Tensor,
        permit: RunPermit,
        released: bool = false,
        fn exclusive(_: *anyopaque) bool {
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.released);
            self.tensor.deinit();
            self.permit.deinit();
            self.released = true;
        }
        fn check(allocator: std.mem.Allocator, deny: bool, cancel: bool) !void {
            const Source = @This();
            const CancelAfterCopy = struct {
                checks: usize = 0,
                fn check(raw: ?*anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    self.checks += 1;
                    if (self.checks == 3) return error.InferenceCancelled;
                }
            };
            var cancellation = CancelAfterCopy{};
            var controller = memory.AdmissionController{};
            defer std.debug.assert(controller.snapshot().hostTotalBytes() == 0);
            const session = Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{
                .controller = &controller,
                .backend_class = .cpu,
                .limits = .{ .host_limit_bytes = if (deny) 64 else 0 },
                .static_workspace_bytes = 1,
                .check_live_memory = false,
            } };
            var source = Source{
                .tensor = try Tensor.initFloat32(std.testing.allocator, "present.0.decoder.key", &.{ 4, 2 }, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }),
                .permit = try session.admitHostPreprocess(48),
            };
            var row = source.tensor.borrowedView(source.tensor.name);
            row.data = source.tensor.data[0..8];
            row.shape = &.{ 1, 2 };
            row.shared_storage = source.tensor.data;
            row.lifetime = .{ .context = &source, .release = Source.release, .is_exclusive = Source.exclusive };
            defer row.deinit();
            const slice = @as([*]Tensor, @ptrCast(&row))[0..1];
            const compacted = session.compactExclusiveRows(allocator, slice, 0, if (cancel) .{ .ptr = &cancellation, .check_fn = CancelAfterCopy.check } else null) catch |err| {
                try std.testing.expect(!source.released);
                try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, row.asFloat32());
                if (deny) {
                    try std.testing.expect(err == error.ResourceLimitExceeded or err == error.ResourceTemporarilyUnavailable);
                    return;
                }
                if (cancel) {
                    try std.testing.expectEqual(error.InferenceCancelled, err);
                    return;
                }
                return err;
            };
            try std.testing.expect(!deny and !cancel and compacted and source.released);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, row.asFloat32());
            try std.testing.expectEqual(@as(usize, 24), controller.snapshot().hostTotalBytes());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Source.check, .{ false, false });
    try Source.check(std.testing.allocator, true, false);
    try Source.check(std.testing.allocator, false, true);
}

test "session output admission coalesces cache column callbacks" {
    const Probe = struct {
        retains: usize = 0,
        releases: usize = 0,
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.retains += 1;
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var probe = Probe{};
    var controller = memory.AdmissionController{};
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer controller.configureResourceBudget(null) catch unreachable;
    const session = Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var value = [_]f32{1};
    var outputs: [129]Tensor = undefined;
    for (&outputs, 0..) |*output, i| output.* = .{
        .name = if (i < 64) "present.0.decoder.key" else if (i < 128) "present.0.encoder.key" else "logits",
        .data = std.mem.sliceAsBytes(&value),
        .shape = &.{1},
        .dtype = .f32,
        .allocator = std.testing.allocator,
        .owns_data = false,
        .owns_shape = false,
    };
    const amounts = try retainedOutputAmounts(&outputs, false);
    var permit = try session.admitHostPreprocess(amounts.host_scratch_bytes);
    defer permit.deinit();
    var credits = try OutputCredits.init(std.testing.allocator, &outputs, false, true);
    defer credits.deinit(std.testing.allocator);
    for (0..63) |i| credits.release(i, &permit.lease.?);
    try std.testing.expectEqual(@as(usize, 0), probe.retains);
    credits.release(63, &permit.lease.?);
    try std.testing.expectEqual(@as(usize, 1), probe.retains);
    try std.testing.expectEqual(@as(usize, 65 * 12), controller.snapshot().host_scratch_bytes);
    for (64..129) |i| credits.release(i, &permit.lease.?);
    try std.testing.expectEqual(@as(usize, 2), probe.retains);
    permit.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "session output admission releases obsolete outputs independently" {
    const alloc = std.testing.allocator;
    var controller = memory.AdmissionController{};
    const session = Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var outputs = [_]Tensor{
        try Tensor.initFloat32(alloc, "logits", &.{ 1, 2 }, &.{ 1, 2 }),
        try Tensor.initFloat32(alloc, "cross", &.{ 1, 2 }, &.{ 3, 4 }),
    };
    const retained = try retainedOutputAmounts(&outputs, false);
    var permit = try session.admitHostPreprocess(retained.host_scratch_bytes);
    defer permit.deinit();
    _ = try Session.attachOutputAdmission(&outputs, alloc, &permit.lease);
    outputs[0].deinit();
    try std.testing.expectEqual(retained.host_scratch_bytes / 2, controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, outputs[1].asFloat32());
    outputs[1].deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "session output admission has one owner on every allocation failure" {
    const Probe = struct {
        fn run(allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !void {
            var controller = memory.AdmissionController{};
            defer std.debug.assert(std.meta.eql(memory.AdmissionAmounts{}, controller.snapshot()));
            var probe = AdmissionProbeSession{ .controller = &controller };
            const session = Session{
                .ptr = &probe,
                .vtable = &AdmissionProbeSession.vtable,
                .run_admission = .{
                    .controller = &controller,
                    .backend_class = .cpu,
                    .limits = .{},
                    .static_workspace_bytes = 1,
                },
            };
            const outputs = try session.runWithControl(&.{}, allocator, control);
            defer deinitTensorSlice(outputs, allocator);
            try std.testing.expect(probe.observed_active_lease);
            try std.testing.expect(controller.snapshot().host_scratch_bytes > 0);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(?InferenceExecutionControl, null)});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(?InferenceExecutionControl, .{})});
}
