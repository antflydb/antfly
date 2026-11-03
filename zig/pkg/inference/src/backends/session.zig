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
const ops = @import("../ops/ops.zig");
const Tensor = @import("tensor.zig").Tensor;
const TensorInfo = @import("tensor.zig").TensorInfo;
const BackendType = @import("backends.zig").BackendType;
const memory = @import("../runtime/tier/memory.zig");
const invocation = @import("../execution/prepared_invocation.zig");
const storage_admission = @import("../execution/storage_admission.zig");
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

/// Borrowed stage inputs ordered by the destination session signature.
/// Host values are explicit uploads; resident values retain producer ownership.
pub const StageInput = union(enum) { host: Tensor, resident: ResidentInput };

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
    backend_owner_cleanup_services: ?*const BackendOwnerCleanupServices = null,

    resource_lease: ?memory.AdmissionLease = null,
    model_retention: ?*Session.ModelLifetime = null,

    // Ordinary outputs allocate no shared holder; view producers promote once.
    resource_hold: ?Session.ModelLifetime.ResourcePin = null,
    retirement_shell: ?@import("../execution/resource_retirement.zig").PreparedCharge = null,

    pub const BackendOwnerCleanupServices = struct {
        accepts: *const fn (*const ResidentOutputs) bool,
        deinit: *const fn (*anyopaque, std.mem.Allocator, @import("../execution/cleanup_context.zig").Context) void,
    };

    const SharedResources = struct {
        refs: std.atomic.Value(usize) = .init(1),
        allocator: std.mem.Allocator,
        owned: ResidentOutputs,
        // Only cold promoted holders can carve a pending payload charge.
        // Ordinary ResidentOutputs keeps its existing value layout unchanged.
        deferred_payload_charge: ?*@import("../execution/payload_charge.zig").Deferred = null,
        payload_claimed: std.atomic.Value(bool) = .init(false),
        payload_transferred: bool = false,

        fn payloadCharge(raw: *anyopaque) @import("../execution/payload_charge.zig").Capability {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .context = raw, .prepare_fn = preparePayload, .commit_fn = commitPayload, .abort_fn = abortPayload, .take_fn = takePayload, .bound_value = if (self.owned.outputs.len == 1) self.owned.outputs[0] else null };
        }
        fn preparePayload(raw: *anyopaque, bytes: usize, deadline_ns: ?u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.payload_claimed.cmpxchgStrong(false, true, .acquire, .monotonic) != null) return false;
            var published = false;
            defer if (!published) self.payload_claimed.store(false, .release);
            if (self.payload_transferred) return false;
            if (self.deferred_payload_charge) |pending| {
                if (pending.prepared != null) return error.PayloadPreparationAlreadyActive;
                if (pending.charge.?.lease.amounts.backend_scratch_bytes != bytes or pending.deadline_ns != deadline_ns) return false;
                published = true;
                return true;
            }
            const lease = if (self.owned.resource_lease) |*value| value else return false;
            const controller = lease.controller orelse return false;
            const model = self.owned.model_retention orelse return false;
            const pin = model.retainAdmissionDomain(controller) orelse return false;
            var pin_owned = true;
            defer if (pin_owned) pin.release();
            const pending = (try @import("../execution/payload_charge.zig").Deferred.create(deadline_ns)) orelse return false;
            errdefer pending.destroyUnused();
            pin_owned = false;
            if (!try pending.prepare(lease, pin, bytes)) {
                pending.destroyUnused();
                return false;
            }
            self.deferred_payload_charge = pending;
            published = true;
            return true;
        }
        fn commitPayload(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.payload_claimed.load(.acquire));
            self.deferred_payload_charge.?.commit();
        }
        fn abortPayload(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.payload_claimed.load(.acquire)) return;
            const pending = self.deferred_payload_charge.?;
            if (pending.abortPreparation()) {
                self.deferred_payload_charge = null;
                pending.destroyUnused();
            }
            self.payload_claimed.store(false, .release);
        }
        fn takePayload(raw: *anyopaque) @import("../execution/payload_charge.zig").Owned {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.payload_claimed.load(.acquire));
            const pending = self.deferred_payload_charge.?;
            self.deferred_payload_charge = null;
            const owned = pending.take();
            self.payload_transferred = true;
            self.payload_claimed.store(false, .release);
            return owned;
        }
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const old = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(old > 0 and old < std.math.maxInt(usize));
        }
        fn release(raw: *anyopaque) void {
            releaseWithCleanup(raw, .{ .outside_execution = true });
        }
        fn releaseWithBoundary(raw: *anyopaque, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
            releaseWithCleanup(raw, .{ .boundary = boundary });
        }
        fn releaseWithCleanup(raw: *anyopaque, current: @import("../execution/cleanup_context.zig").Context) void {
            const boundary = current.boundary;
            const self: *@This() = @ptrCast(@alignCast(raw));
            const old = self.refs.fetchSub(1, .acq_rel);
            std.debug.assert(old > 0);
            if (old != 1) return;
            const allocator = self.allocator;
            // One canonical destructor for ordinary and shared lifetimes. The
            // embedded owner has no pin back to this holder, so there is no cycle.
            const pending = self.deferred_payload_charge;
            if (self.payload_claimed.load(.acquire) or (pending != null and pending.?.prepared != null)) @import("antfly_platform").inference_process_supervisor.restartWorker();
            self.owned.deinitWithCleanup(current);
            allocator.destroy(self);
            if (pending) |value| value.retire(boundary);
        }
    };

    /// Explicit cold preparation for manually constructed resident owners.
    /// Serving callers prepare the same shell through RunPermit before lanes.
    pub fn prepareRetirement(self: *ResidentOutputs) !void {
        if (self.resource_lease == null or self.retirement_shell != null) return;
        self.retirement_shell = try @import("../execution/resource_retirement.zig").PreparedCharge.prepareForBackend(if (self.backend.kind() == .metal) .metal else null, self.outputs.len, try @import("../execution/resource_retirement.zig").outputOriginCapacity(if (self.backend.kind() == .metal) .metal else null, self.outputs.len));
    }

    /// Failure-atomic promotion of the complete tensor set, compute owner, model
    /// and admission lease. outputs remains a borrowed alias while a pin exists.
    pub fn retainResources(self: *ResidentOutputs, allocator: std.mem.Allocator) !Session.ModelLifetime.ResourcePin {
        if (self.resource_hold) |pin| return pin.retain().?;
        const shared = try allocator.create(SharedResources);
        shared.* = .{ .allocator = allocator, .owned = self.* };
        std.debug.assert(shared.owned.resource_hold == null);
        const pin = Session.ModelLifetime.ResourcePin{ .ptr = shared, .retain_fn = SharedResources.retain, .release_fn = SharedResources.release, .release_with_boundary_fn = SharedResources.releaseWithBoundary, .payload_charge_fn = SharedResources.payloadCharge };
        self.resource_hold = pin;
        self.retirement_shell = null;
        self.backend_owner = null;
        self.deinit_backend_owner = null;
        self.resource_lease = null;
        self.model_retention = null;
        return pin.retain().?;
    }

    /// Takes ownership of the supplied independent views/vector only on success.
    /// Child tensors retire before their source's shared physical lifetime. No
    /// separate pin-wrapper allocation or pointer/vtable backend wrapper is needed.
    pub fn ownViewsOf(self: *ResidentOutputs, allocator: std.mem.Allocator, views: []ops.CT) !ResidentOutputs {
        const parent = try self.retainResources(allocator);
        std.debug.assert(parent.release_fn == SharedResources.release);
        const shared: *SharedResources = @ptrCast(@alignCast(parent.ptr));
        // The child frees real view metadata before releasing its parent. Give
        // that cleanup its own fresh stable model scope, even for a nonfinal pin.
        const model = if (shared.owned.model_retention) |owner| owner.retain() else null;
        return .{
            .outputs = views,
            .backend = self.backend,
            .allocator = allocator,
            .backend_owner = parent.ptr,
            .deinit_backend_owner = releaseSharedBackend,
            .model_retention = model,
        };
    }

    fn releaseSharedBackend(owner: *anyopaque, _: std.mem.Allocator) void {
        SharedResources.release(owner);
    }

    pub fn deinit(self: *ResidentOutputs) void {
        self.deinitWithCleanup(.{ .outside_execution = true });
    }
    pub fn deinitWithBoundary(self: *ResidentOutputs, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(.{ .boundary = boundary });
    }
    fn deinitWithCleanup(self: *ResidentOutputs, current: @import("../execution/cleanup_context.zig").Context) void {
        const boundary = current.boundary;
        // A missing current B is not the explicit raw outside API contract.
        // Validate before clearing a published pin, decrementing any owner or
        // invoking a backend callback on an admitted/retained concrete owner.
        if (!current.outside_execution and boundary == null and
            (self.resource_lease != null or self.resource_hold != null or self.backend_owner_cleanup_services != null))
            @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (self.resource_hold) |pin| {
            self.outputs = &.{};
            self.resource_hold = null;
            if (current.outside_execution) pin.release() else pin.releaseWithBoundary(boundary);
            return;
        }
        if (boundary != null and self.resource_lease != null and self.retirement_shell == null)
            @import("antfly_platform").inference_process_supervisor.restartWorker();
        var retirement_shell = self.retirement_shell;
        self.retirement_shell = null;
        defer if (retirement_shell) |*prepared| prepared.destroyUnused();
        const accepted_cleanup = if (self.backend_owner_cleanup_services) |services| services.accepts(self) else false;
        const retained_finish = if (accepted_cleanup or self.deinit_backend_owner == releaseSharedBackend) self.backend.retainCleanupFinish() else null;
        // The exact provider-death fence and this local close fence are
        // independent. No boundary from a nonfinal view is saved as authority.
        defer if (retained_finish) |cell| {
            if (current.outside_execution) cell.releaseLocalOutsideExecution() catch @import("antfly_platform").inference_process_supervisor.restartWorker() else cell.releaseLocalWithBoundary(boundary) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        };
        var close_scope = if (self.model_retention) |model| model.beginClose() else Session.CloseScope{};
        defer close_scope.deinit();
        const retirement = @import("../execution/resource_retirement.zig");
        var retirement_scopes = if (self.resource_lease != null and self.backend.vtable.resource_retirement_services != null)
            (if (retirement_shell) |*prepared| prepared.begin(self.backend.*, self.outputs) else retirement.begin(self.backend.*, self.outputs)) catch @import("antfly_platform").inference_process_supervisor.restartWorker()
        else
            null;
        var retired_charge: ?*retirement.Charge = null;
        if (retirement_scopes) |*scopes| {
            // Zero-local-batch origins may still have their buffer retained
            // by a foreign provider. Eligible backing scopes need witnesses.
            if (scopes.count != 0) {
                var domain = if (self.model_retention) |model|
                    if (self.resource_lease.?.controller) |controller| model.retainAdmissionDomain(controller) else null
                else
                    null;
                retired_charge = (if (retirement_shell) |*prepared| prepared.arm(&self.resource_lease, &domain) else retirement.Charge.create(&self.resource_lease, &domain)) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                if (boundary) |b| retired_charge.?.holdUntilBoundary(b.boundary() catch @import("antfly_platform").inference_process_supervisor.restartWorker()) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                for (scopes.scopes()) |*scope|
                    retired_charge.?.attach(scope) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            }
        }
        // Payload completion and outer gate retirement are independent. Even
        // synchronous/non-Metal outputs cannot refund the request under B.
        if (retired_charge == null and self.resource_lease != null and boundary != null) {
            var domain = if (self.model_retention) |model|
                if (self.resource_lease.?.controller) |controller| model.retainAdmissionDomain(controller) else null
            else
                null;
            retired_charge = (if (retirement_shell) |*prepared| prepared.arm(&self.resource_lease, &domain) else retirement.Charge.create(&self.resource_lease, &domain)) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            retired_charge.?.holdUntilBoundary(boundary.?.boundary() catch @import("antfly_platform").inference_process_supervisor.restartWorker()) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        for (self.outputs, 0..) |output, idx| {
            var seen = false;
            for (self.outputs[0..idx]) |prev| {
                if (prev == output) {
                    seen = true;
                    break;
                }
            }
            if (!seen) self.backend.freeWithBoundary(output, boundary);
        }
        self.allocator.free(self.outputs);
        self.outputs = &.{};
        // Reverse-unlock all actual origin publication domains BEFORE backend
        // teardown, which may checked-submit/cancel/wait on those providers.
        if (retirement_scopes) |*scopes| scopes.end();
        if (self.backend_owner) |owner| {
            if (self.deinit_backend_owner) |deinit_owner| {
                if (deinit_owner == releaseSharedBackend) {
                    // Exact internal view adapter: atomic final shared release
                    // forwards CURRENT B into the actual unique source owner.
                    SharedResources.releaseWithCleanup(owner, current);
                } else if (self.backend_owner_cleanup_services) |services| {
                    if (services.accepts(self)) services.deinit(owner, self.allocator, current) else deinit_owner(owner, self.allocator);
                } else deinit_owner(owner, self.allocator);
            }
        }
        self.backend_owner = null;
        self.deinit_backend_owner = null;
        // Physical cleanup is done. End its local gate before external quota
        // callbacks; a deferred Charge still owns its independent pure pin.
        close_scope.deinit();
        if (self.resource_lease) |*lease| lease.release();
        self.resource_lease = null;
        if (self.model_retention) |lifetime| lifetime.releaseWithCleanup(current);
        self.model_retention = null;
        // Keep the root through final ModelLifetime/backend destruction too.
        if (retired_charge) |charge| {
            if (charge.domain == null) _ = charge.pollRaw();
            charge.release();
        }
        if (retirement_scopes != null) _ = retirement.poll();
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
        embedding_gemma2_cuda: bool = false,
        qwen_dense_workspace: bool = false,
        query_width: usize = 0,
        kv_width: usize = 0,
    };

    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    /// Preserve explicit caller envelopes unless the producer identifies a
    /// replaceable estimate (for example the model loader's weight-size guess).
    static_workspace_bytes: usize,
    static_workspace_kind: enum { mandatory, estimate } = .mandatory,
    /// Independent extra scratch outside both owned payload and legacy allowance.
    opaque_workspace_bytes: usize = 0,
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

    pub const RequestPlan = struct {
        /// Host preprocessing/input staging and materialized output/KV peaks.
        protected: memory.AdmissionAmounts,
        /// Conservative execution scratch; physical coverage must prove any replacement.
        compatibility: memory.AdmissionAmounts,
        /// Explicit/profile envelopes remain charged even with complete owned payload.
        opaque_compatibility: memory.AdmissionAmounts,
        required: memory.AdmissionAmounts,
        retained_host_credit: usize,
        retained_backend_credit: usize,

        /// Resolve the hard-limit peak without separately retained permits.
        /// Defer checked addition until required, preserving the compatibility
        /// estimator's behavior when only the credited peak is representable.
        pub fn uncredited(self: RequestPlan) !memory.AdmissionAmounts {
            var amounts = self.required;
            amounts.host_scratch_bytes = try addBytes(amounts.host_scratch_bytes, self.retained_host_credit);
            amounts.backend_scratch_bytes = try addBytes(amounts.backend_scratch_bytes, self.retained_backend_credit);
            return amounts;
        }
    };

    pub fn estimateRequest(
        self: RunAdmission,
        request: RunRequest,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        return (try self.planRequest(request, output_info)).required;
    }

    pub fn planRequest(
        self: RunAdmission,
        request: RunRequest,
        output_info: []const TensorInfo,
    ) !RequestPlan {
        const input_bytes = request.input_bytes;
        const reference_shape = [_]i64{
            std.math.cast(i64, request.batch) orelse return error.ResourceLimitExceeded,
            std.math.cast(i64, request.sequence) orelse return error.ResourceLimitExceeded,
        };
        const output_bytes = request.output_bytes orelse try estimatedOutputBytes(&reference_shape, output_info);
        if (request.output_kv_bytes > output_bytes) return error.ResourceLimitExceeded;
        const dynamic_base = try addBytes(request.backend_input_bytes orelse input_bytes, output_bytes);
        const dynamic_workspace = try mulBytes(dynamic_base, 6);
        if (request.workspace_planned and request.workspace_bytes == 0) return error.ResourceLimitExceeded;
        const declared_workspace = @max(request.workspace_bytes, try self.profiledWorkspace(request));
        const mandatory_static = if (!request.workspace_planned and self.static_workspace_kind == .mandatory) self.static_workspace_bytes else 0;
        const opaque_workspace = try addBytes(@max(declared_workspace, mandatory_static), self.opaque_workspace_bytes);
        const legacy_workspace = @max(@max(dynamic_workspace, declared_workspace), if (request.workspace_planned) @as(usize, 0) else self.static_workspace_bytes);
        const workspace = try addBytes(legacy_workspace, self.opaque_workspace_bytes);
        const resident_input_bytes = try addBytes(
            try addBytes(request.host_preprocess_bytes, input_bytes),
            try self.profiledHostScratch(request),
        );
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

        const protected = memory.AdmissionAmounts{
            .host_scratch_bytes = host_io_peak,
            .host_kv_bytes = host_kv_peak,
        };
        const retained_backend_credit = if (self.backend_workspace_reserved)
            0
        else
            @min(workspace, request.retained_backend_workspace_bytes);
        const compatibility: memory.AdmissionAmounts = switch (self.backend_class) {
            .cpu => .{ .host_scratch_bytes = workspace },
            .gpu => .{
                // Request IO remains protected host storage. Device staging,
                // activations and outputs share this compatibility workspace.
                .backend_scratch_bytes = if (self.backend_workspace_reserved)
                    0
                else
                    workspace - retained_backend_credit,
            },
        };
        const required = try protected.merge(compatibility);
        return .{
            .protected = protected,
            .compatibility = compatibility,
            .opaque_compatibility = if (self.backend_class == .cpu) .{ .host_scratch_bytes = opaque_workspace } else compatibility,
            .required = required,
            .retained_host_credit = request.pre_admitted_host_bytes,
            .retained_backend_credit = if (self.backend_class == .gpu) retained_backend_credit else 0,
        };
    }

    fn profiledWorkspace(self: RunAdmission, request: RunRequest) !usize {
        const profile = self.model_profile;
        if (profile.hidden_size == 0 or request.batch == 0 or request.sequence == 0)
            return 0;

        const workspace_sequence = if (profile.embedding_gemma2_cuda)
            embeddingGemma2AdmissionSequence(request.sequence)
        else
            request.sequence;
        const tokens = try mulBytes(request.batch, workspace_sequence);
        if (profile.embedding_gemma2_cuda) {
            // The resident encoder reuses four BF16 [B*S, 2048] arena
            // buffers across all 24 layers. Account the two persistent
            // [B*S, 512] activations, request staging, bounded attention and
            // cuBLASLt workspaces, plus the largest qualified media tower
            // scratch. No [B,H,S,S] score tensor is materialized.
            const arena = try mulBytes(try mulBytes(tokens, 4 * 2048), @sizeOf(u16));
            const states = try mulBytes(try mulBytes(tokens, 2 * 512), @sizeOf(u16));
            const staging = try addBytes(request.backend_input_bytes orelse request.input_bytes, try mulBytes(tokens, 2 * @sizeOf(i64)));
            const attention = try mulBytes(workspace_sequence, 4 * 256 * (@sizeOf(f32) + @sizeOf(u16)));
            // Media runs before the text encoder but its BF16 tower keeps a
            // ~60 MiB reusable layer arena live alongside the bounded
            // vision-attention arena and F32 bridge tensors. Reserve their
            // measured peak plus headroom independently of the Lt workspace.
            const fixed = 80 * 1024 * 1024 + (if (request.preprocessing_complete) @as(usize, 0) else 384 * 1024 * 1024);
            return try addBytes(try addBytes(try addBytes(arena, states), staging), try addBytes(attention, fixed));
        }
        // Attention and FFN phases are sequential, so reserve the larger peak
        // rather than summing mutually exclusive intermediates.
        const attention_floats = try mulBytes(profile.hidden_size, 6);
        const ffn_floats = try addBytes(
            try mulBytes(profile.hidden_size, 2),
            try mulBytes(profile.intermediate_size, 3),
        );
        const floats_per_token = @max(attention_floats, ffn_floats);
        // Qwen's prepared frame retains graph-plan hot slots, head-norm/RoPE
        // staging, and fp16 K/V. Its Q width may exceed the hidden width.
        const frame_floats = if (profile.qwen_dense_workspace)
            // 21 retained slots: 3Q + 4KV + 8max(Q,H) + 5I. Reserve
            // replacement overlap as well as head staging and fp16 KV.
            try mulBytes(try addBytes(
                try addBytes(try mulBytes(profile.query_width, 3), try mulBytes(profile.kv_width, 4)),
                try addBytes(try mulBytes(@max(profile.query_width, profile.hidden_size), 8), try mulBytes(profile.intermediate_size, 5)),
            ), 2)
        else
            floats_per_token;
        var peak = try mulBytes(
            try mulBytes(tokens, frame_floats),
            @sizeOf(f32),
        );
        if (profile.qwen_dense_workspace) peak = try addBytes(peak, try self.qwenTransientWorkspaceBytes(tokens));

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

    fn profiledHostScratch(self: RunAdmission, request: RunRequest) !usize {
        if (!self.model_profile.embedding_gemma2_cuda) return 0;
        const padded_sequence = embeddingGemma2AdmissionSequence(request.sequence);
        if (padded_sequence == request.sequence) return 0;
        // The input-embedding route retains the caller's original F32 tensor
        // while materializing a complete zero-padded `[B,roundedS,512]` copy.
        // Token-id requests over-reserve this amount, which is preferable to
        // admitting an embedding request that cannot allocate its staging.
        return try mulBytes(
            try mulBytes(try mulBytes(request.batch, padded_sequence), 512),
            @sizeOf(f32),
        );
    }

    /// Retained slots cannot fund fresh upload, head staging, pooling, and
    /// shape-dependent temporary allocations on a smaller subsequent frame.
    pub fn qwenTransientWorkspaceBytes(self: RunAdmission, tokens: usize) !usize {
        const profile = self.model_profile;
        const floats = try addBytes(try addBytes(try mulBytes(profile.hidden_size, 12), profile.kv_width), try mulBytes(profile.intermediate_size, 4));
        return addBytes(16 * 1024 * 1024, try mulBytes(try mulBytes(tokens, floats), @sizeOf(f32)));
    }
};

/// The qualified SM89 resident encoder rounds medium and long sequences to a
/// complete 64-query tile. Admission does not depend on a live device probe,
/// so conservatively reserve that larger shape for every EG2 CUDA session.
/// Accepted model inputs never exceed the official 8192-token topology.
fn embeddingGemma2AdmissionSequence(sequence: usize) usize {
    if (sequence < 128 or sequence % 64 == 0 or sequence >= 8192) return sequence;
    return ((sequence + 63) / 64) * 64;
}

pub const RunRequest = struct {
    batch: usize = 1,
    sequence: usize = 1,
    input_bytes: usize = 0,
    /// Physical backend inputs, independent of retained host source bytes.
    /// Null preserves conservative legacy admission for other callers.
    backend_input_bytes: ?usize = null,
    /// Resolved physical stage geometry; null retains graph metadata inference.
    output_bytes: ?usize = null,
    output_kv_bytes: usize = 0,
    workspace_bytes: usize = 0,
    /// A qualified phase provides its own positive workspace envelope. Physical
    /// staging and model-profile floors still apply; only the generic allowance
    /// derived from total model weight size is replaced.
    workspace_planned: bool = false,
    host_preprocess_bytes: usize = 0,
    /// Media towers have completed; their transient peak is no longer live.
    /// Retained model caches carry independent leases.
    preprocessing_complete: bool = false,
    /// Host input bytes still covered by a separate live permit. This credit
    /// is limited to input + preprocessing residency and lets callers compose
    /// CPU preprocessing with backend execution without a release/reacquire
    /// gap or transient double charge.
    pre_admitted_host_bytes: usize = 0,
    /// Only an exclusive backend owner may provide this credit. Its retained
    /// leases remain charged, and fresh allocations are bounded separately.
    retained_backend_workspace_bytes: usize = 0,

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
    /// Geometry only. This variant makes no storage or import promise.
    shapes: []const TensorInfo,
    /// A caller that will construct ordinary dense host Tensor inputs may
    /// declare that entry before allocating/tokenizing their payloads.
    host_shapes: []const TensorInfo,
    resident_inputs: struct { info: []const TensorInfo, values: []const ResidentInput },
    stage_inputs: struct { info: []const TensorInfo, values: []const StageInput },

    pub const Source = union(enum) { unknown, host_dense, resident: ResidentInput };

    pub fn validate(self: @This()) !void {
        switch (self) {
            .resident_inputs => |v| if (v.info.len != v.values.len) return error.InputArityMismatch,
            .stage_inputs => |v| if (v.info.len != v.values.len) return error.InputArityMismatch,
            else => {},
        }
    }
    pub fn len(self: @This()) usize {
        return switch (self) {
            .tensors => |v| v.len,
            .shapes, .host_shapes => |v| v.len,
            .resident_inputs => |v| v.info.len,
            .stage_inputs => |v| v.info.len,
        };
    }
    pub fn get(self: @This(), i: usize) TensorInfo {
        return switch (self) {
            .tensors => |v| .{ .name = v[i].name, .shape = v[i].shape, .dtype = v[i].dtype },
            .shapes, .host_shapes => |v| v[i],
            .resident_inputs => |v| v.info[i],
            .stage_inputs => |v| v.info[i],
        };
    }
    pub fn source(self: @This(), i: usize) Source {
        return switch (self) {
            .tensors, .host_shapes => .host_dense,
            .shapes => .unknown,
            .resident_inputs => |v| .{ .resident = v.values[i] },
            .stage_inputs => |v| switch (v.values[i]) {
                .host => .host_dense,
                .resident => |value| .{ .resident = value },
            },
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
    resource_pin: ?Session.ModelLifetime.ResourcePin = null,
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
        const resource_pin = self.resource_pin;
        self.allocator.destroy(self);
        if (resource_pin) |pin| pin.release();
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

/// Resolved semantic/execution metadata, copied with session handles. Optional
/// wrappers preserve this value rather than rediscovering concrete vtables.
pub const ModelContract = struct {
    pub const Task = enum { generic, classifier, extractor };
    pub const Embedding = struct {
        family: enum { encoder, last_token_decoder, gemma2 } = .encoder,
        max_length: usize,
        reject_truncation: bool,
        normalize: bool,
        pooling: ResidentTextPooling,
        /// Borrowed from the model owner, like public tensor-info names.
        document_prefix: []const u8,
        image: bool,
        audio: bool,
        video: bool,
    };
    task: ?Task = null,
    /// Borrowed architecture semantics, preserved by session wrappers.
    decoder_config: ?*const @import("../models/gpt.zig").Config = null,
    embedding: ?Embedding = null,
    resolved: bool = false,
    padding: enum { declared, batch_max } = .declared,
    resident_text_embeddings: bool = false,
    /// Geometry for declared additive attention masks, resolved by the model owner.
    text_attention_heads: u32 = 0,
};

pub const ComputeBackendOptions = struct {
    run_budget: ?*memory.RunBudget = null,
    borrow_shared_provider: bool = false,
};

/// Typed acquisition operation published by the loaded model owner. Wrappers
/// retain this binding even when their public implementation pointer changes.
pub const ComputeBackendProvider = struct {
    ptr: *anyopaque,
    services: *const VTable,
    pub const VTable = struct {
        acquire: *const fn (ctx: *anyopaque, session: Session, allocator: std.mem.Allocator, options: ComputeBackendOptions) anyerror!ops.ComputeBackend,
        /// Immutable endpoint identity only, under the caller's owned model pin.
        cold_target_services: ?*const ColdTargetServices = null,
    };
    /// Static full-table acceptance precedes any implementation-context cast.
    pub const ColdTargetServices = struct {
        accepts: *const fn (*const VTable) bool,
        snapshot: *const fn (*anyopaque, Session, ?InferenceExecutionControl) anyerror!?@import("cold_compute_target.zig").Target,
    };
};

/// Session-owned immutable preparation entry point. Context is retained by the
/// loaded model and copied with its handle, including through session wrappers.
/// Preparation is synchronous and runs before serving publication, while the
/// caller still holds the construction peak and model serialization lease.
pub const ResidentPreparation = struct {
    ptr: *anyopaque,
    prepare: *const fn (*anyopaque, Session, ?InferenceExecutionControl) anyerror!void,
};

/// Loaded model execution handle. Copies preserve the resolved model contract.
pub const Session = struct {
    pub const ModelLifetime = struct {
        pub const ResourcePin = struct {
            ptr: *anyopaque,
            release_fn: *const fn (*anyopaque) void,
            release_with_boundary_fn: ?*const fn (*anyopaque, ?*const @import("../execution/retirement_sink.zig").Epoch) void = null,
            retain_fn: ?*const fn (*anyopaque) void = null,
            payload_charge_fn: ?*const fn (*anyopaque) @import("../execution/payload_charge.zig").Capability = null,
            pub fn payloadCharge(self: ResourcePin) ?@import("../execution/payload_charge.zig").Capability {
                const callback = self.payload_charge_fn orelse return null;
                return callback(self.ptr);
            }
            pub fn retain(self: ResourcePin) ?ResourcePin {
                const retain_fn = self.retain_fn orelse return null;
                retain_fn(self.ptr);
                return self;
            }
            pub fn release(self: ResourcePin) void {
                self.release_fn(self.ptr);
            }
            pub fn releaseWithBoundary(self: ResourcePin, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
                if (self.release_with_boundary_fn) |callback| callback(self.ptr, boundary) else self.release();
            }
        };
        references: std.atomic.Value(usize) = .init(1),
        // Numerical lanes outlive a serving aggregate when retained decoder
        // state still owns this physical model. Cold publication sets pointers
        // only after the lifetime has reached its final address.
        execution_gate: std.atomic.Mutex = .unlocked,
        retirement_sink_mutex: std.atomic.Mutex = .unlocked,
        retirement_sink: ?@import("../execution/retirement_sink.zig").Ref = null,
        owner: *anyopaque,
        destroy_fn: *const fn (*anyopaque) void,
        destroy_with_boundary_fn: ?*const fn (*anyopaque, ?*const @import("../execution/retirement_sink.zig").Epoch) void = null,
        quiesce_fn: ?*const fn (*anyopaque) void = null,
        allocator: ?std.mem.Allocator = null,
        model_lease: ?memory.AdmissionLease = null,
        resource_pin: ?ResourcePin = null,
        /// Controller metadata is independent of manager/model/IO ownership.
        admission_domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null,
        close_protection: ?CloseProtection = null,
        close_protection_owned: bool = false,
        // Cold, process-owned and inert until the actual final drop. No model,
        // provider, IO, request allocator or construction Epoch is captured.
        prepared_final_resources: ?*FinalResources = null,

        /// Pure process-owned cleanup metadata. The caller keeps this model
        /// alive during acquisition, then owns only the returned sink pin.
        /// Reserve an exact epoch before acquiring the actual execution gate.
        pub fn retainRetirementSink(self: *ModelLifetime) !@import("../execution/retirement_sink.zig").Ref {
            std.debug.assert(self.references.load(.acquire) != 0);
            @import("antfly_platform").sync.lockYielding(&self.retirement_sink_mutex);
            defer self.retirement_sink_mutex.unlock();
            if (self.retirement_sink == null) self.retirement_sink = try @import("../execution/retirement_sink.zig").Ref.create();
            return self.retirement_sink.?.retain();
        }

        pub fn retainAdmissionDomain(self: *const ModelLifetime, controller: *memory.AdmissionController) ?@import("../runtime/tier/admission_domain.zig").Pin {
            const pin = self.admission_domain orelse return null;
            if (pin.controller() != controller) return null;
            return pin.retain();
        }

        /// Construction/publication only, before any concurrent owner or gate.
        /// Ordinary nonfinal release never prepares or inspects this capsule.
        pub fn prepareFinalResources(self: *ModelLifetime) !void {
            const a = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());
            return self.prepareFinalResourcesWithAllocator(a);
        }
        fn prepareFinalResourcesWithAllocator(self: *ModelLifetime, a: std.mem.Allocator) !void {
            if (self.prepared_final_resources != null) return;
            if (self.references.load(.acquire) != 1) return error.ModelLifetimeAlreadyPublished;
            const node = try a.create(FinalResources);
            node.* = .{ .metadata_allocator = a };
            self.prepared_final_resources = node;
        }

        /// Constructor abort only, before publication or resource attachment.
        /// This drops inert process metadata, never quota/SDK/model ownership.
        pub fn discardUnpublishedFinalResources(self: *ModelLifetime) void {
            const node = self.prepared_final_resources orelse return;
            std.debug.assert(self.references.load(.acquire) == 1 and self.model_lease == null and self.resource_pin == null and self.admission_domain == null and !self.close_protection_owned);
            std.debug.assert(node.epoch == null and node.model_lease == null and node.pin == null and node.domain == null and node.protection == null);
            self.prepared_final_resources = null;
            node.metadata_allocator.destroy(node);
        }

        pub fn installBorrowedCloseProtection(self: *ModelLifetime, protection: ?CloseProtection) void {
            std.debug.assert(!self.close_protection_owned);
            if (self.close_protection) |old| {
                const replacement = protection orelse unreachable;
                std.debug.assert(sameCloseProtection(old, replacement));
            } else self.close_protection = protection;
        }

        fn sameCloseProtection(left: CloseProtection, right: CloseProtection) bool {
            return left.ptr == right.ptr and left.begin_fn == right.begin_fn and left.release_fn == right.release_fn;
        }

        pub fn beginClose(self: *const ModelLifetime) CloseScope {
            return if (self.close_protection) |protection| protection.begin() else .{};
        }

        pub fn retain(self: *ModelLifetime) *ModelLifetime {
            const old = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(old > 0 and old < std.math.maxInt(usize));
            return self;
        }
        pub fn release(self: *ModelLifetime) void {
            self.releaseWithCleanup(.{ .outside_execution = true });
        }

        pub fn releaseWithBoundary(self: *ModelLifetime, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
            self.releaseWithCleanup(.{ .boundary = boundary });
        }
        pub fn releaseWithCleanup(self: *ModelLifetime, current: @import("../execution/cleanup_context.zig").Context) void {
            const old = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(old > 0);
            if (old != 1) return;
            self.releaseFinalWithCleanup(current);
        }

        /// Explicit outer cleanup only: the caller owns no provider/model lane.
        /// Nested callers must forward their current B to releaseWithBoundary.
        pub fn releaseOutsideExecution(self: *ModelLifetime) void {
            const fatal = @import("antfly_platform").inference_process_supervisor.restartWorker;
            var sink = self.retainRetirementSink() catch fatal();
            defer sink.release();
            var guard = sink.enter() catch fatal();
            defer guard.finish();
            var epoch = guard.retainEpoch() catch fatal();
            defer epoch.release();
            self.releaseWithBoundary(&epoch);
        }

        const FinalResources = struct {
            node: @import("../execution/resource_retirement.zig").Node = undefined,
            epoch: ?@import("../execution/retirement_sink.zig").Epoch = null,
            metadata_allocator: std.mem.Allocator = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback()),
            model_lease: ?memory.AdmissionLease = null,
            pin: ?ResourcePin = null,
            domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null,
            protection: ?CloseProtection = null,
            protection_owned: bool = false,
            outside_execution: bool = false,

            fn drop(self: *@This(), boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
                // External quota callbacks see protection and resource pins alive.
                if (self.model_lease) |*lease| lease.release();
                if (self.protection_owned) if (self.protection) |p| p.release();
                if (self.pin) |p| if (self.outside_execution) p.release() else p.releaseWithBoundary(boundary);
                if (self.domain) |p| p.release();
            }
            fn retire(raw: *anyopaque) callconv(.c) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var epoch = self.epoch;
                self.drop(if (epoch) |*e| e else null);
                const a = self.metadata_allocator;
                a.destroy(self);
                if (epoch) |*e| e.release();
            }
        };

        noinline fn releaseFinalWithCleanup(self: *ModelLifetime, supplied: @import("../execution/cleanup_context.zig").Context) void {
            const platform = @import("antfly_platform");
            const fatal = platform.inference_process_supervisor.restartWorker;
            // Embedded lifetimes may disappear inside physical destruction.
            const owner = self.owner;
            const destroy = self.destroy_fn;
            const destroy_with_boundary = self.destroy_with_boundary_fn;
            const allocator = self.allocator;
            const prepared_final = self.prepared_final_resources;
            self.prepared_final_resources = null;
            const final_allocator = if (prepared_final) |node| node.metadata_allocator else platform.allocator.processAllocator(platform.allocator.concurrentFallback());
            var sink = self.retirement_sink;
            // Only the actual final raw release enters an already-cold model
            // Sink. Ordinary nonfinal references keep their atomic fast path.
            var raw_guard: ?@import("../execution/retirement_sink.zig").Guard = null;
            var raw_epoch: ?@import("../execution/retirement_sink.zig").Epoch = null;
            defer if (raw_epoch) |*epoch| epoch.release();
            defer if (raw_guard) |*guard| guard.finish();
            var current_context = supplied;
            if (supplied.outside_execution) if (sink) |stable| {
                raw_guard = stable.enter() catch fatal();
                raw_epoch = raw_guard.?.retainEpoch() catch fatal();
                current_context = .{ .boundary = &raw_epoch.? };
            };
            const boundary = current_context.boundary;
            if (boundary == null and !current_context.outside_execution and
                (self.model_lease != null or self.resource_pin != null or self.admission_domain != null or sink != null)) fatal();
            var resources = FinalResources{
                .metadata_allocator = final_allocator,
                .model_lease = self.model_lease,
                .pin = self.resource_pin,
                .domain = self.admission_domain,
                .protection = self.close_protection,
                .protection_owned = self.close_protection_owned,
                .outside_execution = current_context.outside_execution,
            };
            const current = boundary;
            var deferred: ?*FinalResources = null;
            if (boundary) |b| {
                // Managed current-B destruction consumes already-owned metadata.
                // Only the historical explicit outside API may allocate here.
                const node = prepared_final orelse if (supplied.outside_execution)
                    final_allocator.create(FinalResources) catch fatal()
                else
                    fatal();
                node.* = resources;
                node.epoch = b.retain() catch fatal();
                node.node = .{ .context = node, .release_fn = FinalResources.retire };
                deferred = node;
            }
            var scope = if (resources.protection) |p| p.begin() else CloseScope{};
            if (current_context.outside_execution) destroy(owner) else if (destroy_with_boundary) |destroy_b| destroy_b(owner, current) else destroy(owner);
            scope.deinit();
            if (sink) |*s| s.close() catch fatal();
            if (allocator) |a| a.destroy(self);
            if (deferred) |node| {
                const b = boundary.?.boundary() catch fatal();
                b.register(b.context, b.nonce, &node.node) catch fatal();
                // Registration may synchronously consume node; never read it.
            } else {
                if (prepared_final) |node| final_allocator.destroy(node);
                resources.drop(current);
            }
            if (sink) |*s| s.release();
        }
        fn close(self: *ModelLifetime, protection: ?CloseProtection, lease: *?memory.AdmissionLease) void {
            self.closeWithCleanup(protection, lease, .{ .outside_execution = true });
        }
        fn closeWithBoundary(self: *ModelLifetime, protection: ?CloseProtection, lease: *?memory.AdmissionLease, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
            self.closeWithCleanup(protection, lease, .{ .boundary = boundary });
        }
        fn closeWithCleanup(self: *ModelLifetime, protection: ?CloseProtection, lease: *?memory.AdmissionLease, current: @import("../execution/cleanup_context.zig").Context) void {
            if (!current.outside_execution and current.boundary == null and
                (lease.* != null or self.model_lease != null or self.resource_pin != null or self.admission_domain != null or self.retirement_sink != null))
                @import("antfly_platform").inference_process_supervisor.restartWorker();
            std.debug.assert(!self.close_protection_owned);
            if (lease.* != null) {
                std.debug.assert(self.model_lease == null);
                self.model_lease = lease.*;
                lease.* = null;
            }
            if (self.close_protection) |old| {
                const incoming = protection orelse unreachable;
                std.debug.assert(sameCloseProtection(old, incoming));
            } else self.close_protection = protection;
            self.close_protection_owned = protection != null;
            var scope = if (protection) |p| p.begin() else CloseScope{};
            defer scope.deinit();
            if (self.quiesce_fn) |quiesce| quiesce(self.owner);
            scope.deinit();
            self.releaseWithCleanup(current);
        }
    };

    /// Borrowed model-owned observation capability, independent of format.
    invocation_diagnostics: ?@import("../execution/invocation_observation.zig").Binding = null,
    invocation_services: ?*const invocation.Services = null,
    model_lifetime: ?*ModelLifetime = null,
    model_contract: ModelContract = .{},
    compute_backend_provider: ?ComputeBackendProvider = null,
    resident_preparation: ?ResidentPreparation = null,
    /// Borrowed common preparation owner. Staged models retain it when loaded;
    /// resident outputs retain it independently of the publishing session.
    shared_compute_context: ?*@import("compute_context.zig").SharedBackendContext = null,
    ptr: *anyopaque,
    vtable: *const VTable,
    close_protection: ?CloseProtection = null,
    run_admission: ?RunAdmission = null,
    /// Borrowed from the session owner, backed by its retained model lease.
    /// Copies share one ledger so concurrent requests cannot reuse the credit.
    generation_workspace: ?*memory.ReservedGenerationWorkspace = null,
    output_geometry: ?SequenceOutputGeometry = null,
    cached_decoder_geometry: ?CachedDecoderGeometry = null,
    /// Explicit stage contract: these small control tensors are equal across
    /// a physical batch and passed once, never concatenated as independent rows.
    broadcast_inputs: []const []const u8 = &.{},
    /// Borrowed from the model/runtime owner; stable for every session copy.
    execution_gate: ?*std.atomic.Mutex = null,
    cohort_scheduler: ?*@import("../runtime/embedding_scheduler.zig").Scheduler = null,

    /// Queue only ready work; callers own admitted preparation outside this
    /// scope. Keep the original raw gate for native/backend interoperability.
    /// Select one model-owned ready turn while keeping the actual numerical
    /// gate free for resource suppliers. A raw external gate remains its
    /// caller's scheduling contract; published owners install the shared FIFO.
    pub fn selectReadyCohort(self: Session, gate: ?*std.atomic.Mutex, control: InferenceExecutionControl, waiter: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) !void {
        const lock = gate orelse return;
        if (self.cohort_scheduler) |scheduler| {
            if (self.execution_gate != lock) return error.ExecutionCohortGateChanged;
            return scheduler.selectReady(waiter, lock, control);
        }
        if (self.execution_gate != null) return error.MissingExecutionCohortScheduler;
        try control.check();
        waiter.* = .{ .gate = lock, .ready = true };
    }
    pub fn acquireSelectedCohort(_: Session, control: InferenceExecutionControl, waiter: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) !void {
        if (waiter.owner != null) return @import("../runtime/embedding_scheduler.zig").Scheduler.acquireSelected(waiter, control);
        const gate = waiter.gate orelse return;
        if (!waiter.ready or waiter.acquired) return error.UnselectedExecutionCohort;
        try control.lock(gate);
        waiter.acquired = true;
    }
    pub fn leaveSelectedCohort(_: Session, waiter: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) void {
        if (waiter.owner != null) return @import("../runtime/embedding_scheduler.zig").Scheduler.leave(waiter);
        if (waiter.acquired) waiter.gate.?.unlock();
        waiter.* = .{};
    }
    pub fn releaseSelectedCohortGate(_: Session, waiter: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) void {
        if (waiter.owner != null) return @import("../runtime/embedding_scheduler.zig").Scheduler.releaseGate(waiter);
        if (waiter.acquired) waiter.gate.?.unlock();
        waiter.acquired = false;
    }
    pub fn enterExecutionCohort(self: Session, gate: ?*std.atomic.Mutex, control: InferenceExecutionControl, waiter: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) !void {
        const lock = gate orelse return;
        if (self.cohort_scheduler) |scheduler| if (self.execution_gate == lock) return scheduler.enter(waiter, lock, control);
        try control.lock(lock);
    }
    pub fn leaveExecutionCohort(_: Session, gate: ?*std.atomic.Mutex, waiter: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) void {
        if (waiter.owner != null) @import("../runtime/embedding_scheduler.zig").Scheduler.leave(waiter) else if (gate) |lock| lock.unlock();
    }

    /// Stable planning capacity, shared by all copies of this session. Live
    /// contention is resolved by workspace acquisition after chunk selection.
    pub fn generationWorkspaceCapacity(self: Session) ?memory.AdmissionAmounts {
        return if (self.generation_workspace) |workspace| workspace.capacity else null;
    }

    pub fn prepareResident(self: Session, control: ?InferenceExecutionControl) !void {
        const preparation = self.resident_preparation orelse return;
        try preparation.prepare(preparation.ptr, self, control);
    }

    /// Cold owner configuration. A published execution gate coordinates this
    /// with numerical work. Ungated/raw sessions require the caller to quiesce
    /// backend users, as with other mutable backend configuration/teardown.
    pub fn configureExecutionDiagnostics(self: Session, enabled: bool) bool {
        if (self.invocation_diagnostics) |binding| {
            if (enabled) {
                if (self.execution_gate) |gate| {
                    @import("antfly_platform").sync.lockYielding(gate);
                    defer gate.unlock();
                    return binding.configure(true);
                } else return binding.configure(true);
            } else return binding.configure(false);
        }
        const configure = self.vtable.configureExecutionDiagnostics orelse return false;
        configure(self.ptr, enabled);
        return true;
    }
    /// Raw cold configuration of a separately retained numerical backend.
    /// PRECONDITION: the caller guarantees this exact backend is exclusively
    /// owned/quiescent through this call, including all aliases and submitters.
    /// The model gate below protects model diagnostics; it does not establish
    /// quiescence of an arbitrary foreign backend. No backend pointer is stored.
    /// Returns false when the model publishes no diagnostics capability.
    pub fn configureQuiescentExecutionBackendDiagnostics(self: Session, execution_backend: *const ops.ComputeBackend, enabled: bool) bool {
        const binding = self.invocation_diagnostics orelse return false;
        var selected = binding.forBackend(execution_backend.*);
        selected.context_sampling = .caller_exclusive;
        if (enabled) {
            if (self.execution_gate) |gate| {
                @import("antfly_platform").sync.lockYielding(gate);
                defer gate.unlock();
                return selected.configure(true);
            } else return selected.configure(true);
        } else return selected.configure(false);
    }

    pub fn executionDiagnostics(self: Session) ?@import("../execution/diagnostics.zig").Snapshot {
        if (self.invocation_diagnostics) |binding| return binding.diagnostics.snapshot();
        const snapshot = self.vtable.executionDiagnostics orelse return null;
        return snapshot(self.ptr);
    }

    pub const CloseBoundaryServices = struct {
        accepts: *const fn (*const VTable) bool,
        close: *const fn (*anyopaque, ?*const @import("../execution/retirement_sink.zig").Epoch) void,
        /// Owned pure controller metadata, acquired before physical ctx close.
        retain_admission_domain: ?*const fn (*anyopaque, *memory.AdmissionController) ?@import("../runtime/tier/admission_domain.zig").Pin = null,
    };

    pub const VTable = struct {
        quiesce: ?*const fn (*anyopaque) void = null,
        configureExecutionDiagnostics: ?*const fn (ptr: *anyopaque, enabled: bool) void = null,
        executionDiagnostics: ?*const fn (ptr: *anyopaque) ?@import("../execution/diagnostics.zig").Snapshot = null,
        hasLayaDecisions: ?*const fn (ptr: *anyopaque) bool = null,
        runLayaDecisions: ?*const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) anyerror!?[]Tensor = null,
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
        close_boundary_services: ?*const CloseBoundaryServices = null,
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

    pub fn coldComputeTarget(self: Session, control: ?InferenceExecutionControl) !?@import("cold_compute_target.zig").Target {
        if (control) |active| try active.check();
        const provider = self.compute_backend_provider orelse return null;
        const services = provider.services.cold_target_services orelse return null;
        if (!services.accepts(provider.services)) return null;
        return services.snapshot(provider.ptr, self, control);
    }

    pub fn acquireComputeBackend(self: Session, allocator: std.mem.Allocator, options: ComputeBackendOptions) !ops.ComputeBackend {
        const provider = self.compute_backend_provider orelse return error.UnsupportedSessionComputeBackend;
        return provider.services.acquire(provider.ptr, self, allocator, options);
    }

    /// Run a forward pass with the given input tensors.
    pub fn run(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
        if (self.preparesInvocation()) {
            var permit: RunPermit = undefined;
            try self.admitInputsInto(inputs, allocator, null, &permit);
            defer permit.deinit();
            const outputs = try permit.run(inputs, allocator);
            errdefer deinitTensorSlice(outputs, allocator);
            return attachOutputAdmission(outputs, allocator, &permit.lease, self.resourcePin());
        }
        var resource_lease = if (self.run_admission) |admission|
            try admission.acquireRequest(try self.planRun(inputs, null), self.outputInfo())
        else
            null;
        errdefer if (resource_lease) |*lease| lease.release();
        const outputs = try @import("../execution/invocation_observation.zig").call(self.invocation_diagnostics, self.vtable.run, .{
            self.ptr,
            inputs,
            allocator,
        });
        errdefer deinitTensorSlice(outputs, allocator);
        return attachOutputAdmission(outputs, allocator, &resource_lease, self.resourcePin());
    }

    pub fn runWithControl(
        self: Session,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) ![]Tensor {
        if (self.preparesInvocation()) {
            var permit: RunPermit = undefined;
            try self.admitInputsInto(inputs, allocator, control, &permit);
            defer permit.deinit();
            const outputs = try permit.runWithControl(inputs, allocator, control);
            errdefer deinitTensorSlice(outputs, allocator);
            return attachOutputAdmission(outputs, allocator, &permit.lease, self.resourcePin());
        }
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
        const outputs = try @import("../execution/invocation_observation.zig").call(self.invocation_diagnostics, self.vtable.runWithControl, .{
            self.ptr,
            inputs,
            allocator,
            active,
        });
        errdefer deinitTensorSlice(outputs, allocator);
        try active.check();
        return attachOutputAdmission(outputs, allocator, &resource_lease, self.resourcePin());
    }

    /// Optional typed-decision route. Only an unsupported/disabled capability
    /// returns null; execution and admission failures propagate without fallback.
    pub fn runLayaDecisionsWithControl(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?[]Tensor {
        const run_decisions = self.vtable.runLayaDecisions orelse return null;
        if (self.vtable.hasLayaDecisions) |supported| if (!supported(self.ptr)) return null;
        const active = control orelse InferenceExecutionControl{};
        try active.check();
        var protection = if (control) |c| try c.enterUninterruptible(self.interruption()) else null;
        defer if (protection) |*p| p.deinit();
        var lease = if (self.run_admission) |admission|
            try admission.acquireRequest(try self.planRun(inputs, null), self.outputInfo())
        else
            null;
        errdefer if (lease) |*l| l.release();
        const outputs = (try @import("../execution/invocation_observation.zig").call(self.invocation_diagnostics, run_decisions, .{
            self.ptr,
            inputs,
            allocator,
            control,
        })) orelse {
            if (lease) |*l| l.release();
            return null;
        };
        errdefer deinitTensorSlice(outputs, allocator);
        try active.check();
        return try attachOutputAdmission(outputs, allocator, &lease, self.resourcePin());
    }

    /// Borrows outputs and the lease until success. On error the caller still
    /// owns both; on success the tensors own the retained admission lease.
    fn attachOutputAdmission(
        outputs: []Tensor,
        allocator: std.mem.Allocator,
        resource_lease: *?memory.AdmissionLease,
        resource_pin: ?ModelLifetime.ResourcePin,
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
            .resource_pin = if (resource_pin) |pin| pin.retain() else null,
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

    /// Select exact code and warm storage before acquiring its request peak.
    pub fn admitInputs(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        var permit: RunPermit = undefined;
        try self.admitInputsInto(inputs, allocator, control, &permit);
        return permit;
    }

    fn admitInputsInto(self: *const Session, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, permit: *RunPermit) !void {
        try self.admitShapeRequestInto(.{ .tensors = inputs }, try self.planRunBorrowed(inputs, null), allocator, control, permit);
    }

    pub fn admitShapes(self: Session, inputs: []const TensorInfo, batch: usize, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        return self.admitShapeRequest(.{ .shapes = inputs }, try self.planShapes(inputs, batch), allocator, control);
    }

    /// Explicit dense-host entry; plain admitShapes remains geometry-only.
    pub fn admitHostShapes(self: Session, inputs: []const TensorInfo, batch: usize, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        return self.admitShapeRequest(.{ .host_shapes = inputs }, try self.planShapes(inputs, batch), allocator, control);
    }

    /// Caller-supplied request preserves preprocessing/retained-input policy.
    /// Shapes are consumed synchronously and never borrowed by the permit.
    pub fn admitShapeRequest(self: Session, shapes: ShapeInputs, request: RunRequest, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        var permit: RunPermit = undefined;
        try self.admitShapeRequestInto(shapes, request, allocator, control, &permit);
        return permit;
    }

    /// This synchronous helper owns complete Session/policy snapshots in the
    /// permit and preparation. Borrowing only avoids intermediate aggregates.
    fn admitShapeRequestInto(self: *const Session, shapes: ShapeInputs, request: RunRequest, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, permit: *RunPermit) !void {
        return self.admitShapeRequestIntoWithCleanup(shapes, request, allocator, control, permit, .{}, null);
    }

    fn admitShapeRequestIntoWithCleanup(self: *const Session, shapes: ShapeInputs, request: RunRequest, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, permit: *RunPermit, cleanup: @import("../execution/cleanup_context.zig").Context, ready_guard: ?*@import("../execution/retirement_sink.zig").Guard) !void {
        try shapes.validate();
        var boundary = if (control) |active| try active.enterUninterruptible(self.interruption()) else null;
        defer if (boundary) |*scope| scope.deinit();
        permit.* = .{ .session = self.*, .lease = null };
        try invocation.prepareIntoWithCleanup(self, shapes, allocator, control, &permit.prepared, cleanup);
        // The explicit caller has no outer numerical/provider/model gate.
        // Retired old-bank quota must return before the next supplier request.
        if (ready_guard) |guard| guard.finish();
        errdefer {
            if (permit.prepared) |*prepared| {
                if (ready_guard != null) retireRejectedBeforeExecution(prepared) else prepared.deinitWithCleanup(cleanup);
            }
            permit.prepared = null;
        }
        var physical_credit = memory.AdmissionAmounts{};
        const lease = if (self.run_admission) |admission| blk: {
            if (admission.controller.consumeForcedRunDenialForTesting()) return error.ResourceTemporarilyUnavailable;
            const plan = try admission.planRequest(request, self.outputInfo());
            var required = plan.required;
            if (permit.prepared) |*prepared| {
                prepared.requirement.protected = plan.protected;
                const replace_guess = admission.backend_class == .cpu and request.workspace_bytes == 0 and !request.workspace_planned and prepared.hasCompleteOwnedPayload();
                const compatibility = if (replace_guess) plan.opaque_compatibility else plan.compatibility;
                prepared.requirement.compatibility = compatibility;
                const base = try plan.protected.merge(compatibility);
                var full = try base.merge(prepared.requirement.physical);
                full.host_scratch_bytes = try addBytes(full.host_scratch_bytes, plan.retained_host_credit);
                full.backend_scratch_bytes = try addBytes(full.backend_scratch_bytes, plan.retained_backend_credit);
                if (!try full.fitsLimits(admission.limits)) return error.ResourceLimitExceeded;
                const warm = prepared.requirement.warm_credit;
                const physical = prepared.requirement.physical;
                if (warm.host_weight_bytes != 0 or warm.backend_weight_bytes != 0 or warm.host_kv_bytes != 0 or warm.backend_kv_bytes != 0 or warm.host_scratch_bytes > physical.host_scratch_bytes or warm.backend_scratch_bytes > physical.backend_scratch_bytes) return error.InvalidPhysicalStorageCredit;
                var fresh = physical;
                fresh.host_scratch_bytes -= warm.host_scratch_bytes;
                fresh.backend_scratch_bytes -= warm.backend_scratch_bytes;
                physical_credit = .{ .host_scratch_bytes = fresh.host_scratch_bytes, .backend_scratch_bytes = fresh.backend_scratch_bytes };
                required = try base.merge(fresh);
            }
            break :blk admission.controller.tryAcquire(admission.backend_class, admission.limits, required, admission.check_live_memory) catch |err| retry: {
                if (err != error.ResourceTemporarilyUnavailable) return err;
                const prepared = if (permit.prepared) |*value| value else return err;
                const reclaimed = if (ready_guard != null)
                    try reclaimBeforeExecution(prepared, control)
                else if (prepared.reclaim_idle_with_cleanup) |reclaim|
                    try reclaim(prepared, control, cleanup)
                else if (prepared.reclaim_idle) |reclaim|
                    try reclaim(prepared, control)
                else
                    return err;
                if (!reclaimed) return err;
                if (control) |active| try active.check();
                break :retry try admission.controller.tryAcquire(admission.backend_class, admission.limits, required, admission.check_live_memory);
            };
        } else null;
        permit.request = request;
        permit.lease = lease;
        permit.physical_credit = physical_credit;
        if (lease != null) if (self.model_lifetime) |lifetime| if (lifetime.resource_pin) |pin| {
            permit.resource_pin = pin.retain();
        };
    }

    noinline fn retireRejectedBeforeExecution(prepared: *invocation.PreparedInvocation) void {
        const fatal = @import("antfly_platform").inference_process_supervisor.restartWorker;
        const lifetime = prepared.published.model_lifetime orelse {
            prepared.deinit();
            return;
        };
        var sink = lifetime.retainRetirementSink() catch fatal();
        defer sink.release();
        var guard = sink.enter() catch fatal();
        defer guard.finish();
        var epoch = guard.retainEpoch() catch fatal();
        defer epoch.release();
        prepared.deinitWithCleanup(.{ .boundary = &epoch });
    }
    noinline fn reclaimBeforeExecution(prepared: *invocation.PreparedInvocation, control: ?InferenceExecutionControl) !bool {
        const lifetime = prepared.published.model_lifetime orelse return false;
        const reclaim = prepared.reclaim_idle_with_cleanup orelse return false;
        var sink = try lifetime.retainRetirementSink();
        defer sink.release();
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        var scope = prepared.published.beginClose();
        defer scope.deinit();
        return reclaim(prepared, control, .{ .boundary = &epoch });
    }

    /// Explicit caller contract: the selected FIFO ready owner holds no outer
    /// numerical/provider/model gate. Cleanup of evicted programs is completed
    /// before any supplier acquisition, and its epoch is never saved in a plan.
    pub fn admitShapeRequestBeforeExecution(self: Session, shapes: ShapeInputs, request: RunRequest, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        if (self.model_lifetime) |lifetime| {
            var sink = try lifetime.retainRetirementSink();
            defer sink.release();
            var guard = try sink.enter();
            defer guard.finish();
            var epoch = try guard.retainEpoch();
            defer epoch.release();
            var permit: RunPermit = undefined;
            try self.admitShapeRequestIntoWithCleanup(shapes, request, allocator, control, &permit, .{ .boundary = &epoch }, &guard);
            return permit;
        }
        return self.admitShapeRequest(shapes, request, allocator, control);
    }
    pub fn admitInputsBeforeExecution(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        return self.admitShapeRequestBeforeExecution(.{ .tensors = inputs }, try self.planRun(inputs, null), allocator, control);
    }

    pub fn preparesInvocation(self: Session) bool {
        const services = self.invocation_services orelse return false;
        return services.accepts(self);
    }

    /// Reserve the full request peak before preprocessing allocates input
    /// buffers. The permit owns that lease and executes without double-counting.
    pub fn admit(self: Session, request: RunRequest) !RunPermit {
        return self.retainPermit(.{
            .session = self,
            .request = request,
            .lease = if (self.run_admission) |admission|
                try admission.acquireRequest(request, self.outputInfo())
            else
                null,
        });
    }

    /// Acquire only a later phase's additional peak while the original lease
    /// remains live. Counters, live-memory checks and external ownership all
    /// use ordinary admission; a rejected supplement leaves the base untouched.
    /// Both owners must outlive every allocation covered by their union.
    pub fn admitSupplement(self: Session, base: *const RunPermit, target: memory.AdmissionAmounts) !RunPermit {
        return self.admitSupplementPair(base, null, target);
    }

    /// Credit two independent live owners without merging external lease
    /// tokens. Only acquire the component-wise deficit beyond both owners.
    pub fn admitSupplementPair(self: Session, base: *const RunPermit, other: ?*const RunPermit, target: memory.AdmissionAmounts) !RunPermit {
        var pressure: ?memory.AdmissionPressure = null;
        return self.admitSupplementPairWithPressure(base, other, target, &pressure);
    }

    /// Supplemental admission reports the same denial constraint as initial
    /// admission, while preserving credit from independently owned live leases.
    pub fn admitSupplementPairWithPressure(self: Session, base: *const RunPermit, other: ?*const RunPermit, target: memory.AdmissionAmounts, pressure: *?memory.AdmissionPressure) !RunPermit {
        pressure.* = null;
        if (base.session.ptr != self.ptr or base.session.vtable != self.vtable or base.execution_yielded) return error.InvalidAdmissionLeaseReduction;
        const admission = self.run_admission orelse return self.retainPermit(.{ .session = self, .lease = null });
        const lease = base.lease orelse return error.InvalidAdmissionLeaseReduction;
        if (lease.controller != admission.controller or lease.retain_backend_class != admission.backend_class) return error.InvalidAdmissionLeaseReduction;
        var owned = lease.amounts;
        if (other) |peer| {
            if (peer == base or peer.session.ptr != self.ptr or peer.session.vtable != self.vtable or peer.execution_yielded) return error.InvalidAdmissionLeaseReduction;
            if (peer.lease) |credit| {
                if (credit.controller != admission.controller or credit.retain_backend_class != admission.backend_class or (credit.external_lease != null and credit.external_lease == lease.external_lease)) return error.InvalidAdmissionLeaseReduction;
                owned = try owned.merge(credit.amounts);
            }
        }
        var extra = memory.AdmissionAmounts{};
        inline for (@typeInfo(memory.AdmissionAmounts).@"struct".field_names) |name| @field(extra, name) = @field(target, name) -| @field(owned, name);
        if (std.meta.eql(extra, memory.AdmissionAmounts{})) return self.retainPermit(.{ .session = self, .lease = null });
        if (admission.controller.consumeForcedRunDenialForTesting()) return error.ResourceTemporarilyUnavailable;
        return self.retainPermit(.{ .session = self, .lease = try admission.controller.tryAcquireWithPressure(admission.backend_class, admission.limits, extra, admission.check_live_memory, pressure) });
    }

    /// Preserve the exact denial constraint for bounded batch planning. A
    /// failed reservation publishes no lease; a fit probe is never a permit.
    pub fn admitWithPressure(self: Session, request: RunRequest, pressure: *?memory.AdmissionPressure) !RunPermit {
        pressure.* = null;
        return self.retainPermit(.{
            .session = self,
            .request = request,
            .lease = if (self.run_admission) |admission| blk: {
                if (admission.controller.consumeForcedRunDenialForTesting()) return error.ResourceTemporarilyUnavailable;
                break :blk try admission.controller.tryAcquireWithPressure(admission.backend_class, admission.limits, try admission.estimateRequest(request, self.outputInfo()), admission.check_live_memory, pressure);
            } else null,
        });
    }

    pub fn planRun(self: Session, inputs: []const Tensor, batch_override: ?usize) !RunRequest {
        return self.planRunBorrowed(inputs, batch_override);
    }

    fn planRunBorrowed(self: *const Session, inputs: []const Tensor, batch_override: ?usize) !RunRequest {
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
        if (batch_override == null) request.pre_admitted_host_bytes = try self.inputResidencyCreditBorrowed(inputs);
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

    fn resolveRunGeometry(self: *const Session, inputs: ShapeInputs, request: *RunRequest) !void {
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
        return self.inputResidencyCreditBorrowed(inputs);
    }

    fn inputResidencyCreditBorrowed(self: *const Session, inputs: []const Tensor) !usize {
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
        _ = try attachOutputAdmission(copies, allocator, &lease, self.resourcePin());
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
        var pressure: ?memory.AdmissionPressure = null;
        return self.admitHostPreprocessWithPressure(host_scratch_bytes, &pressure);
    }

    /// Host-only preflight reports its denial constraint without estimating a
    /// model run, enabling callers to reclaim relevant optional caches safely.
    pub fn admitHostPreprocessWithPressure(self: Session, host_scratch_bytes: usize, pressure: *?memory.AdmissionPressure) !RunPermit {
        pressure.* = null;
        return self.retainPermit(.{
            .session = self,
            .lease = if (self.run_admission) |admission| blk: {
                if (admission.controller.consumeForcedRunDenialForTesting()) return error.ResourceTemporarilyUnavailable;
                break :blk try admission.controller.tryAcquireWithPressure(admission.backend_class, admission.limits, .{ .host_scratch_bytes = host_scratch_bytes }, admission.check_live_memory, pressure);
            } else null,
        });
    }

    /// Owned media cache allocations can outlive a single execution permit.
    /// Charge them independently until eviction, including full device buffers
    /// retained by views rather than only their logical tensor payload.
    pub fn admitRetainedMedia(self: Session, host_bytes: usize, device_bytes: usize) !RunPermit {
        return self.retainPermit(.{ .session = self, .lease = if (self.run_admission) |admission|
            try admission.acquireAmounts(.{ .host_scratch_bytes = host_bytes, .backend_scratch_bytes = device_bytes })
        else
            null });
    }

    pub fn prepareResidentRetirement(self: Session) !?@import("../execution/resource_retirement.zig").PreparedCharge {
        if (self.run_admission == null) return null;
        return try @import("../execution/resource_retirement.zig").PreparedCharge.prepareForBackend(if (self.backend() == .metal) .metal else null, self.outputInfo().len, try @import("../execution/resource_retirement.zig").outputOriginCapacity(if (self.backend() == .metal) .metal else null, self.outputInfo().len));
    }
    pub fn admitResidentInputs(self: Session, inputs: []const ResidentInput) !RunPermit {
        var prepared = try self.prepareResidentRetirement();
        defer if (prepared) |*shell| shell.destroyUnused();
        return self.admitResidentInputsWithRetirement(inputs, &prepared);
    }
    /// The owning caller prepares before source/destination provider lanes.
    pub fn admitResidentInputsWithRetirement(self: Session, inputs: []const ResidentInput, prepared: *?@import("../execution/resource_retirement.zig").PreparedCharge) !RunPermit {
        if (self.run_admission != null and prepared.* == null) return error.UnpreparedOutputRetirement;
        var permit = self.retainPermit(.{ .session = self, .lease = if (self.run_admission) |admission| try admission.acquireResidentInputs(inputs, self.outputInfo()) else null });
        errdefer permit.deinit();
        try permit.adoptResidentOutputRetirement(prepared);
        return permit;
    }

    /// Capability preflight is metadata-only and precedes admission or uploads.
    /// A source from another device context is never normalized through the host.
    pub fn supportsStageInputs(self: Session, inputs: []const StageInput) bool {
        const shared = self.shared_compute_context orelse return false;
        if (self.vtable.runResidentInputs == null or inputs.len != self.inputInfo().len) return false;
        const stage_backend = shared.stableBackend();
        for (inputs, self.inputInfo()) |input, info| switch (input) {
            .host => |host| if (!std.mem.eql(u8, host.name, info.name) or host.dtype != info.dtype) return false,
            .resident => |value| {
                if (value.backend.ptr == stage_backend.ptr and value.backend.vtable == stage_backend.vtable) continue;
                if (value.backend.kind() == .native and stage_backend.kind() == .native) continue;
                return false;
            },
        };
        return true;
    }

    /// A mixed stage has one admission boundary. Resident activations stay with
    /// their producer; only host controls are imported into the selected context.
    pub fn runStageInputsWithControl(self: Session, inputs: []const StageInput, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?ResidentOutputs {
        if (!self.supportsStageInputs(inputs)) return null;
        if (control != null and self.vtable.runResidentInputsWithControl == null) return null;
        if (control) |active| try active.check();
        var protection = if (control) |active| try active.enterUninterruptible(self.interruption()) else null;
        defer if (protection) |*scope| scope.deinit();
        var metadata = std.heap.ArenaAllocator.init(allocator);
        defer metadata.deinit();
        const info = try metadata.allocator().alloc(TensorInfo, inputs.len);
        for (inputs, self.inputInfo(), info) |input, declared, *actual| {
            actual.* = switch (input) {
                .host => |host| .{ .name = host.name, .dtype = host.dtype, .shape = host.shape },
                .resident => |value| .{
                    .name = declared.name,
                    .dtype = declared.dtype,
                    .shape = try value.backend.tensorShape(value.value, metadata.allocator()),
                },
            };
        }
        const batch = if (info.len > 0) positiveDimension(info[0].shape, 0) else 1;
        var permit = try self.admitShapeRequest(.{ .stage_inputs = .{ .info = info, .values = inputs } }, try self.planShapes(info, batch), allocator, control);
        defer permit.deinit();
        try permit.prepareResidentOutputRetirement();
        var outputs = (try @import("../execution/invocation_observation.zig").callStage(self.invocation_diagnostics, inputs, RunPermit.runStageInputsWithinBoundary, .{ &permit, inputs, allocator, control })) orelse return null;
        std.debug.assert(outputs.resource_lease == null);
        outputs.resource_lease = permit.lease;
        permit.lease = null;
        self.pinResident(&outputs);
        return outputs;
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

    /// A manager may attach an independently owned protection ticket without
    /// changing backend identity. Session copies borrow the same ticket, just
    /// as they borrow ptr; exactly one owner must eventually call close.
    pub const CloseProtection = struct {
        ptr: *anyopaque,
        begin_fn: *const fn (*anyopaque) CloseScope,
        release_fn: *const fn (*anyopaque) void,

        pub fn begin(self: CloseProtection) CloseScope {
            return self.begin_fn(self.ptr);
        }

        pub fn release(self: CloseProtection) void {
            self.release_fn(self.ptr);
        }
    };

    /// Keeps protection alive while an aggregate owner destroys dependent
    /// caches as well as its session. Starting close is irreversible: callers
    /// must finish destroying the owner, rather than resume inference.
    pub const CloseScope = struct {
        ptr: ?*anyopaque = null,
        release_fn: ?*const fn (*anyopaque) void = null,

        pub fn deinit(self: *CloseScope) void {
            const ptr = self.ptr orelse return;
            self.ptr = null;
            self.release_fn.?(ptr);
        }
    };

    /// Install only before concurrent publication. Session retains the owned
    /// ticket until close transfers it; this borrowed capability starts no scope.
    pub fn installBorrowedCloseProtection(self: *Session, protection: ?CloseProtection) void {
        if (self.close_protection) |old| {
            const incoming = protection orelse unreachable;
            std.debug.assert(ModelLifetime.sameCloseProtection(old, incoming));
        } else self.close_protection = protection;
        if (self.model_lifetime) |lifetime| lifetime.installBorrowedCloseProtection(protection);
    }

    pub fn beginClose(self: Session) CloseScope {
        return if (self.close_protection) |protection| protection.begin() else .{};
    }

    fn closeBoundaryCallback(self: Session) ?*const fn (*anyopaque, ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        const services = self.vtable.close_boundary_services orelse return null;
        if (!services.accepts(self.vtable)) return null;
        return services.close;
    }

    fn retainCloseAdmissionDomain(self: Session, controller: *memory.AdmissionController) ?@import("../runtime/tier/admission_domain.zig").Pin {
        const services = self.vtable.close_boundary_services orelse return null;
        if (!services.accepts(self.vtable)) return null;
        const retain_domain = services.retain_admission_domain orelse return null;
        const pin = retain_domain(self.ptr, controller) orelse return null;
        if (pin.controller() != controller) @import("antfly_platform").inference_process_supervisor.restartWorker();
        return pin;
    }

    fn ensureModelLifetime(self: *Session, allocator: std.mem.Allocator) !*ModelLifetime {
        const metadata = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());
        return self.ensureModelLifetimeWithAllocators(allocator, metadata);
    }
    fn ensureModelLifetimeWithAllocators(self: *Session, allocator: std.mem.Allocator, metadata: std.mem.Allocator) !*ModelLifetime {
        if (self.model_lifetime == null) {
            const lifetime = try allocator.create(ModelLifetime);
            errdefer allocator.destroy(lifetime);
            lifetime.* = .{ .owner = self.ptr, .destroy_fn = self.vtable.close, .destroy_with_boundary_fn = self.closeBoundaryCallback(), .quiesce_fn = self.vtable.quiesce, .allocator = allocator };
            try lifetime.prepareFinalResourcesWithAllocator(metadata);
            lifetime.installBorrowedCloseProtection(self.close_protection);
            self.model_lifetime = lifetime;
        } else try self.model_lifetime.?.prepareFinalResourcesWithAllocator(metadata);
        return self.model_lifetime.?;
    }

    /// Cold loading/publication only. Every numerical caller uses this same
    /// lane; retained model state keeps the mutex alive through final unlock.
    pub fn installOwnedExecutionGate(self: *Session, allocator: std.mem.Allocator) !void {
        const lifetime = try self.ensureModelLifetime(allocator);
        self.execution_gate = &lifetime.execution_gate;
    }

    pub fn attachResourcePin(self: *Session, allocator: std.mem.Allocator, pin: ModelLifetime.ResourcePin) !void {
        const lifetime = try self.ensureModelLifetime(allocator);
        if (lifetime.resource_pin != null) return error.ResourcePinAlreadyAttached;
        lifetime.resource_pin = pin;
    }

    fn retainPermit(self: Session, permit: RunPermit) RunPermit {
        var owned = permit;
        if (owned.lease != null) if (self.resourcePin()) |pin| {
            owned.resource_pin = pin.retain();
        };
        return owned;
    }

    fn resourcePin(self: Session) ?ModelLifetime.ResourcePin {
        return if (self.model_lifetime) |lifetime| lifetime.resource_pin else null;
    }

    pub fn pinResident(self: Session, outputs: *ResidentOutputs) void {
        if (outputs.model_retention == null) {
            if (self.model_lifetime) |lifetime| outputs.model_retention = lifetime.retain();
        }
    }

    pub fn retainRetirementSink(self: Session) !?@import("../execution/retirement_sink.zig").Ref {
        const lifetime = self.model_lifetime orelse return null;
        return try lifetime.retainRetirementSink();
    }

    /// Explicit cold conversion for a raw Session which will close under B.
    /// One existing ModelLifetime owns both the close authority and its capsule;
    /// no second raw holder or request-time preparation path is introduced.
    pub fn prepareCloseRetainingResources(self: *Session, allocator: std.mem.Allocator, lease: *const ?memory.AdmissionLease) !void {
        const metadata = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());
        return self.prepareCloseRetainingResourcesWithAllocators(allocator, metadata, lease);
    }
    fn prepareCloseRetainingResourcesWithAllocators(self: *Session, allocator: std.mem.Allocator, metadata: std.mem.Allocator, lease: *const ?memory.AdmissionLease) !void {
        var held_domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
        defer if (held_domain) |pin| pin.release();
        if (lease.*) |owned| if (owned.controller) |controller| {
            if (self.model_lifetime) |model| if (model.admission_domain) |pin| {
                if (pin.controller() != controller) return error.ForeignAdmissionDomain;
            };
            const existing = if (self.model_lifetime) |model| model.retainAdmissionDomain(controller) else null;
            held_domain = existing orelse self.retainCloseAdmissionDomain(controller) orelse return error.MissingPureCloseDomain;
        };
        const lifetime = try self.ensureModelLifetimeWithAllocators(allocator, metadata);
        if (lifetime.admission_domain == null and held_domain != null) {
            lifetime.admission_domain = held_domain;
            held_domain = null;
        }
    }
    fn validatePreparedClose(self: Session, lease: *const ?memory.AdmissionLease, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (current.outside_execution) return;
        if (self.model_lifetime) |model| {
            if (current.boundary != null and model.prepared_final_resources == null) return error.UnpreparedModelClose;
            return;
        }
        if (lease.* != null or self.close_protection != null) {
            if (current.boundary == null) return error.MissingCurrentCleanupEpoch;
            return error.UnpreparedModelClose;
        }
    }

    pub fn closeRetainingResources(self: Session, lease: *?memory.AdmissionLease) void {
        self.closeRetainingResourcesWithCleanup(lease, .{ .outside_execution = true });
    }

    pub fn closeRetainingResourcesWithBoundary(self: Session, lease: *?memory.AdmissionLease, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.closeRetainingResourcesWithCleanup(lease, .{ .boundary = boundary });
    }
    fn closeRetainingResourcesWithCleanup(self: Session, lease: *?memory.AdmissionLease, current: @import("../execution/cleanup_context.zig").Context) void {
        self.validatePreparedClose(lease, current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        const boundary = current.boundary;
        if (self.model_lifetime) |lifetime| {
            lifetime.closeWithCleanup(self.close_protection, lease, current);
            return;
        }
        const fatal = @import("antfly_platform").inference_process_supervisor.restartWorker;
        var deferred: ?*ModelLifetime.FinalResources = null;
        if (boundary != null and (lease.* != null or self.close_protection != null)) {
            const b = boundary.?;
            const domain = if (lease.*) |owned|
                if (owned.controller) |controller| self.retainCloseAdmissionDomain(controller) else null
            else
                null;
            // Caller-owned raw controller/budget metadata cannot escape its
            // owner's close into an unsealed epoch without a certified pin.
            if (lease.* != null and domain == null and !(b.isSealed() catch fatal())) fatal();
            const a = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());
            const node = a.create(ModelLifetime.FinalResources) catch fatal();
            node.* = .{
                .epoch = b.retain() catch fatal(),
                .model_lease = lease.*,
                .pin = null,
                .domain = domain,
                .protection = self.close_protection,
                .protection_owned = self.close_protection != null,
            };
            node.node = .{ .context = node, .release_fn = ModelLifetime.FinalResources.retire };
            deferred = node;
            lease.* = null;
        }
        var scope = self.beginClose();
        defer scope.deinit();
        defer if (deferred == null) {
            if (self.close_protection) |protection| protection.release();
        };
        if (current.outside_execution) self.vtable.close(self.ptr) else if (self.closeBoundaryCallback()) |close_b| close_b(self.ptr, boundary) else self.vtable.close(self.ptr);
        scope.deinit();
        if (deferred) |node| {
            const b = boundary.?.boundary() catch fatal();
            b.register(b.context, b.nonce, &node.node) catch fatal();
            // An already-sealed B may consume node synchronously; no access.
        } else {
            if (lease.*) |*owned| owned.release();
            lease.* = null;
        }
    }

    pub fn close(self: Session) void {
        var lease: ?memory.AdmissionLease = null;
        self.closeRetainingResources(&lease);
    }

    pub fn closeWithBoundary(self: Session, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        var lease: ?memory.AdmissionLease = null;
        self.closeRetainingResourcesWithBoundary(&lease, boundary);
    }

    /// Transfer the separately owned model reservation before an aggregate owner
    /// closes its raw sessions. Legacy sessions leave the lease with their caller.
    pub fn retainResources(self: Session, lease: *?memory.AdmissionLease) void {
        const lifetime = self.model_lifetime orelse return;
        if (lease.* == null) return;
        std.debug.assert(lifetime.model_lease == null);
        lifetime.model_lease = lease.*;
        lease.* = null;
    }

    pub fn runResident(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.preparesInvocation()) {
            var permit: RunPermit = undefined;
            try self.admitInputsInto(inputs, allocator, null, &permit);
            defer permit.deinit();
            try permit.prepareResidentOutputRetirement();
            var outputs = (try permit.runResident(inputs, allocator)) orelse return null;
            outputs.resource_lease = permit.lease;
            permit.lease = null;
            return outputs;
        }
        if (self.vtable.runResident) |run_resident| {
            var resource_lease = if (self.run_admission) |admission|
                try admission.acquire(inputs, self.outputInfo())
            else
                null;
            errdefer if (resource_lease) |*lease| lease.release();
            var prepared_shell: ?@import("../execution/resource_retirement.zig").PreparedCharge = if (resource_lease != null) try @import("../execution/resource_retirement.zig").PreparedCharge.prepareForBackend(if (self.backend() == .metal) .metal else null, self.outputInfo().len, try @import("../execution/resource_retirement.zig").outputOriginCapacity(if (self.backend() == .metal) .metal else null, self.outputInfo().len)) else null;
            defer if (prepared_shell) |*prepared| prepared.destroyUnused();
            var outputs = (try @import("../execution/invocation_observation.zig").call(self.invocation_diagnostics, run_resident, .{
                self.ptr,
                inputs,
                allocator,
            })) orelse {
                if (resource_lease) |*lease| lease.release();
                return null;
            };
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = resource_lease;
            outputs.retirement_shell = prepared_shell;
            prepared_shell = null;
            self.pinResident(&outputs);
            return outputs;
        }
        return null;
    }

    pub fn runResidentInputs(self: Session, inputs: []const ResidentInput, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.preparesInvocation()) {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const info = try residentShapeInfo(self, inputs, arena.allocator());
            var request = try self.planShapes(info, if (info.len > 0) positiveDimension(info[0].shape, 0) else 1);
            request.input_bytes = 0;
            for (inputs) |input| request.input_bytes = try addBytes(request.input_bytes, input.estimated_bytes);
            var permit = try self.admitShapeRequest(.{ .resident_inputs = .{ .info = info, .values = inputs } }, request, allocator, null);
            defer permit.deinit();
            try permit.prepareResidentOutputRetirement();
            var outputs = (try permit.runResidentInputs(inputs, allocator)) orelse return null;
            outputs.resource_lease = permit.lease;
            permit.lease = null;
            return outputs;
        }
        if (self.vtable.runResidentInputs) |run_resident_inputs| {
            var resource_lease = if (self.run_admission) |admission|
                try admission.acquireResidentInputs(inputs, self.outputInfo())
            else
                null;
            errdefer if (resource_lease) |*lease| lease.release();
            var prepared_shell: ?@import("../execution/resource_retirement.zig").PreparedCharge = if (resource_lease != null) try @import("../execution/resource_retirement.zig").PreparedCharge.prepareForBackend(if (self.backend() == .metal) .metal else null, self.outputInfo().len, try @import("../execution/resource_retirement.zig").outputOriginCapacity(if (self.backend() == .metal) .metal else null, self.outputInfo().len)) else null;
            defer if (prepared_shell) |*prepared| prepared.destroyUnused();
            var outputs = (try @import("../execution/invocation_observation.zig").callResident(self.invocation_diagnostics, inputs, run_resident_inputs, .{
                self.ptr,
                inputs,
                allocator,
            })) orelse {
                if (resource_lease) |*lease| lease.release();
                return null;
            };
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = resource_lease;
            outputs.retirement_shell = prepared_shell;
            prepared_shell = null;
            self.pinResident(&outputs);
            return outputs;
        }
        return null;
    }

    pub fn runResidentTextEmbedding(self: Session, inputs: []const Tensor, request: ResidentTextEmbeddingRequest, allocator: std.mem.Allocator) !?ResidentOutputs {
        const run_resident = self.vtable.runResidentTextEmbedding orelse return null;
        var outputs = (try @import("../execution/invocation_observation.zig").call(self.invocation_diagnostics, run_resident, .{
            self.ptr,
            inputs,
            request,
            allocator,
        })) orelse return null;
        self.pinResident(&outputs);
        return outputs;
    }
};

pub const RunPermit = struct {
    session: Session,
    resource_pin: ?Session.ModelLifetime.ResourcePin = null,
    prepared: ?invocation.PreparedInvocation = null,
    physical_credit: memory.AdmissionAmounts = .{},
    physical_storage: ?*@import("../execution/prepared_storage_admission.zig").Prepared = null,
    physical_prepared: bool = false,
    // Synchronous dispatch pin, even when the cold Set is empty/warm. Never
    // transferred to a returned output or used for later physical destruction.
    dispatch_cleanup_epoch: ?@import("../execution/retirement_sink.zig").Epoch = null,
    output_retirement_shell: ?@import("../execution/resource_retirement.zig").PreparedCharge = null,
    invocation_started: bool = false,
    lease: ?memory.AdmissionLease,
    request: ?RunRequest = null,
    execution_yielded: bool = false,

    fn retirePrepared(self: *RunPermit) void {
        self.retirePreparedWithCleanup(.{});
    }
    /// Retire consumed execution owners after numerical unlock, before moving
    /// a remaining output lease into a longer-lived output holder. No current
    /// operation/epoch is saved in that holder for its later destruction.
    pub fn retireExecutionWithCleanup(self: *RunPermit, cleanup: @import("../execution/cleanup_context.zig").Context) void {
        self.retirePreparedWithCleanup(cleanup);
    }
    fn retirePreparedWithCleanup(self: *RunPermit, cleanup: @import("../execution/cleanup_context.zig").Context) void {
        var dispatch_epoch = self.dispatch_cleanup_epoch;
        self.dispatch_cleanup_epoch = null;
        defer if (dispatch_epoch) |*held| held.release();
        if (dispatch_epoch != null and cleanup.boundary == null)
            @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.retirePhysicalStorageWithBoundary(cleanup.boundary);
        if (self.prepared) |*selected| selected.deinitWithCleanup(cleanup);
        self.prepared = null;
        self.physical_credit = .{};
    }

    /// Called with actual input facts before selecting the actual numerical
    /// gate. Every external quota/lifetime/partition callback completes here.
    pub fn preparePhysicalStorage(self: *RunPermit, allocator: std.mem.Allocator) !void {
        if (self.physical_prepared) return;
        if (self.lease == null) return; // Raw execution owns no supplier.
        const selected = if (self.prepared) |*value| value else {
            if (self.session.preparesInvocation()) return error.MissingPreparedInvocation;
            return; // Custom callbacks preserve their own allocation policy.
        };
        if (selected.executed) return error.PreparedInvocationAlreadyConsumed;
        const pool = selected.physical_pool orelse {
            self.physical_prepared = true;
            return;
        };
        const policy = self.session.run_admission orelse return error.MissingRunAdmission;
        const identity = storage_admission.Identity{ .controller = policy.controller, .backend_class = policy.backend_class };
        var requirements = @import("../execution/prepared_storage_admission.zig").Builder{ .allocator = allocator };
        defer requirements.deinit();
        try pool.collectAdmissionRequirements(&requirements, if (selected.frame_claim) |*claim| claim else null, identity);
        var total = memory.AdmissionAmounts{};
        for (requirements.items()) |item| {
            if (item.backend == .native or item.class == .host_workspace)
                total.host_scratch_bytes = try addBytes(total.host_scratch_bytes, item.bytes)
            else
                total.backend_scratch_bytes = try addBytes(total.backend_scratch_bytes, item.bytes);
        }
        // A mutable/mismatched declaration cannot borrow protected IO quota or
        // silently fall back after exact physical credit has been granted.
        if (!std.meta.eql(total, self.physical_credit)) return error.PreparedPhysicalRequirementsChanged;
        const source = storage_admission.Capability{ .identity = identity, .context = self, .services = &.{ .reserve = reservePhysical } };
        self.physical_storage = try @import("../execution/prepared_storage_admission.zig").Prepared.prepare(source, requirements.items());
        self.physical_prepared = true;
    }

    /// The actual owner binds current B before acquiring model/provider gates.
    /// Raw resident dispatch also needs B even when no physical Set was admitted.
    pub fn bindPhysicalCleanupEpoch(self: *RunPermit, epoch: *const @import("../execution/retirement_sink.zig").Epoch) !void {
        if (self.lease != null and self.session.preparesInvocation() and !self.physical_prepared)
            return error.UnpreparedPhysicalStorageReservation;
        return self.bindCurrentCleanupEpoch(epoch);
    }
    /// Explicit synchronous dispatch authority, independent of storage readiness.
    /// This is never inherited from an allocation or recovered from ambient state.
    pub fn bindCurrentCleanupEpoch(self: *RunPermit, epoch: *const @import("../execution/retirement_sink.zig").Epoch) !void {
        if (self.dispatch_cleanup_epoch) |held| {
            if (held.record != epoch.record or held.nonce != epoch.nonce) return error.BackendOperationCleanupChanged;
            return;
        }
        var held = try epoch.retain();
        errdefer held.release();
        if (try held.isSealed()) return error.RetirementEpochSealed;
        if (self.physical_storage) |prepared| try prepared.bindConstructionEpoch(epoch);
        self.dispatch_cleanup_epoch = held;
    }

    fn retirePhysicalStorageWithBoundary(self: *RunPermit, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        const storage = self.physical_storage;
        self.physical_storage = null;
        self.physical_prepared = false;
        if (storage) |prepared| prepared.finishInvocationWithBoundary(boundary);
    }
    fn reservePhysical(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: @import("../execution/storage_accounting.zig").Class, bytes: usize) !storage_admission.Reservation {
        const self: *RunPermit = @ptrCast(@alignCast(raw));
        const policy = self.session.run_admission orelse return error.UnsupportedPhysicalStorageAdmission;
        const source = if (self.lease) |*lease| lease else return error.MissingRunAdmission;
        // Only the controller must survive late backing death. Keep actual
        // numerical/allocator owners separately through the existing program
        // and output lifetime; quota callbacks must not retain manager IO.
        const domain = if (self.session.model_lifetime) |owner| owner.retainAdmissionDomain(policy.controller) else null;
        defer if (domain) |pin| pin.release();
        const lifetime: ?storage_admission.Lifetime = if (domain) |pin|
            storage_admission.Lifetime.forAdmissionDomain(pin)
        else if (self.resource_pin) |pin|
            if (pin.retain_fn) |retain| .{ .context = pin.ptr, .retain_infallible = retain, .release = pin.release_fn } else null
        else
            null;
        return storage_admission.fromRunLease(allocator, source, .{
            .controller = policy.controller,
            .backend_class = policy.backend_class,
            .limits = policy.limits,
            .check_live_memory = policy.check_live_memory,
            .credit = &self.physical_credit,
            .lifetime = lifetime,
        }, kind, class, bytes);
    }

    fn storageCapability(self: *RunPermit) ?storage_admission.Capability {
        const policy = self.session.run_admission orelse return null;
        if (self.lease == null) return null;
        if (self.physical_prepared) return .{ .identity = .{ .controller = policy.controller, .backend_class = policy.backend_class }, .context = self, .services = &prepared_dispatch_services };
        return .{ .identity = .{ .controller = policy.controller, .backend_class = policy.backend_class }, .context = self, .services = &.{ .reserve = reservePhysical } };
    }

    // Static delegation preserves Capability width and makes the actual current
    // dispatch epoch available independently of cold allocation requirements.
    const prepared_dispatch_services = storage_admission.Capability.VTable{ .reserve = denyPreparedReservation, .takeBacking = takePreparedBacking, .constructionEpoch = currentDispatchEpoch, .currentCleanupEpoch = currentDispatchEpoch };
    fn denyPreparedReservation(_: *anyopaque, _: std.mem.Allocator, _: @import("../graph/backend_contracts.zig").BackendKind, _: @import("../execution/storage_accounting.zig").Class, _: usize) !storage_admission.Reservation {
        return error.UnpreparedPhysicalStorageReservation;
    }
    fn takePreparedBacking(raw: *anyopaque, allocator: std.mem.Allocator, backend: @import("../graph/backend_contracts.zig").BackendKind, class: @import("../execution/storage_accounting.zig").Class, bytes: usize) !storage_admission.BackingReservation {
        const self: *RunPermit = @ptrCast(@alignCast(raw));
        const policy = self.session.run_admission orelse return error.MissingRunAdmission;
        const delegate = @import("../execution/prepared_storage_admission.zig").Prepared.forDispatch(self.physical_storage, .{ .controller = policy.controller, .backend_class = policy.backend_class });
        return (try delegate.reserveBacking(allocator, backend, class, bytes)) orelse error.UnpreparedPhysicalStorageReservation;
    }
    fn currentDispatchEpoch(raw: *anyopaque) ?*const @import("../execution/retirement_sink.zig").Epoch {
        const self: *RunPermit = @ptrCast(@alignCast(raw));
        return if (self.dispatch_cleanup_epoch) |*held| held else null;
    }
    fn discardFailedResident(self: *RunPermit, outputs: *ResidentOutputs) void {
        if (self.dispatch_cleanup_epoch) |*current| outputs.deinitWithBoundary(current) else outputs.deinit();
    }
    fn validateResidentDispatch(self: *const RunPermit) !void {
        // Exact execution/admission must be obtained before the caller binds B
        // and enters its gate. Do not acquire a replacement permit under it.
        if (self.dispatch_cleanup_epoch != null and self.execution_yielded) return error.PreparedAdmissionRequiresReadyTurn;
    }

    pub fn prepareResidentOutputRetirement(self: *RunPermit) !void {
        if (self.lease == null or self.output_retirement_shell != null) return;
        // Metal owns at most three nonrecursive Ref values per output, plus
        // the destination runtime. Actual handles/origins are checked on attach.
        const capacity = try @import("../execution/resource_retirement.zig").outputOriginCapacity(if (self.session.backend() == .metal) .metal else null, self.session.outputInfo().len);
        self.output_retirement_shell = try @import("../execution/resource_retirement.zig").PreparedCharge.prepareForBackend(if (self.session.backend() == .metal) .metal else null, self.session.outputInfo().len, capacity);
    }
    pub fn adoptResidentOutputRetirement(self: *RunPermit, prepared: *?@import("../execution/resource_retirement.zig").PreparedCharge) !void {
        if (self.lease == null) return;
        if (self.output_retirement_shell != null) return error.OutputRetirementAlreadyPrepared;
        self.output_retirement_shell = prepared.* orelse return error.UnpreparedOutputRetirement;
        prepared.* = null;
    }
    pub fn validateResidentOutputRetirement(self: *const RunPermit) !void {
        if (self.lease != null and self.output_retirement_shell == null) return error.UnpreparedOutputRetirement;
    }
    fn pinResident(self: *RunPermit, outputs: *ResidentOutputs) void {
        self.session.pinResident(outputs);
        if (outputs.retirement_shell == null) {
            outputs.retirement_shell = self.output_retirement_shell;
            self.output_retirement_shell = null;
        }
    }

    fn prepareForInputs(self: *RunPermit, inputs: invocation.Inputs, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !void {
        if (self.prepared != null or !self.session.preparesInvocation()) return;
        switch (inputs) {
            .host => |host| try invocation.prepareInto(&self.session, .{ .tensors = host }, allocator, control, &self.prepared),
            .resident => |resident| {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();
                const info = try residentShapeInfo(self.session, resident, arena.allocator());
                try invocation.prepareInto(&self.session, .{ .resident_inputs = .{ .info = info, .values = resident } }, allocator, control, &self.prepared);
            },
        }
        // Compatibility permits were already acquired without shape selection.
        // New physical allocations receive separately admitted deficits; none
        // of their protected legacy quota is claimed as physical credit.
    }

    fn executePrepared(self: *RunPermit, inputs: invocation.Inputs, placement: invocation.Placement, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?invocation.Result {
        if (placement == .resident and self.lease != null and self.output_retirement_shell == null) return error.UnpreparedOutputRetirement;
        if (self.prepared == null and !self.session.preparesInvocation()) return null;
        if (self.prepared) |*selected| if (selected.executed) {
            if (self.dispatch_cleanup_epoch != null) return error.PreparedAdmissionRequiresReadyTurn;
            self.retirePrepared();
        };
        var boundary = if (control) |active| try active.enterUninterruptible(self.session.interruption()) else null;
        defer if (boundary) |*scope| scope.deinit();
        try self.prepareForInputs(inputs, allocator, control);
        const selected = if (self.prepared) |*prepared| prepared else return null;
        var result = try selected.run(inputs, placement, allocator, control, self.storageCapability());
        if (result == .resident) self.pinResident(&result.resident);
        if (control) |active| active.check() catch |err| {
            switch (result) {
                .host => |outputs| deinitTensorSlice(outputs, allocator),
                .resident => |outputs| {
                    var owned = outputs;
                    self.discardFailedResident(&owned);
                },
            }
            return err;
        };
        self.invocation_started = true;
        return result;
    }

    fn executePreparedResident(self: *RunPermit, inputs: invocation.Inputs, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?ResidentOutputs {
        if (try self.executePrepared(inputs, .resident, allocator, control)) |result| {
            var outputs = result.resident;
            self.pinResident(&outputs);
            return outputs;
        }
        return null;
    }

    pub fn acquireExecutionInputs(self: *RunPermit, inputs: invocation.Inputs, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        var request = self.request orelse return error.MissingRunRequest;
        request.pre_admitted_host_bytes = @min(if (self.lease) |lease| lease.amounts.host_scratch_bytes else 0, try addBytes(request.input_bytes, request.host_preprocess_bytes));
        return switch (inputs) {
            .host => |host| self.session.admitShapeRequest(.{ .tensors = host }, request, allocator, control),
            .resident => |resident| blk: {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();
                const info = try residentShapeInfo(self.session, resident, arena.allocator());
                break :blk self.session.admitShapeRequest(.{ .resident_inputs = .{ .info = info, .values = resident } }, request, allocator, control);
            },
        };
    }

    pub fn acquireExecutionInputsBeforeExecution(self: *RunPermit, inputs: invocation.Inputs, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !RunPermit {
        var request = self.request orelse return error.MissingRunRequest;
        request.pre_admitted_host_bytes = @min(if (self.lease) |lease| lease.amounts.host_scratch_bytes else 0, try addBytes(request.input_bytes, request.host_preprocess_bytes));
        return switch (inputs) {
            .host => |host| self.session.admitShapeRequestBeforeExecution(.{ .tensors = host }, request, allocator, control),
            .resident => |resident| blk: {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();
                const info = try residentShapeInfo(self.session, resident, arena.allocator());
                break :blk self.session.admitShapeRequestBeforeExecution(.{ .resident_inputs = .{ .info = info, .values = resident } }, request, allocator, control);
            },
        };
    }

    fn resolveGeometry(self: *RunPermit, inputs: []const Tensor) !void {
        if (self.invocation_started and !self.execution_yielded and self.session.preparesInvocation()) {
            const actual = try self.session.planRun(inputs, null);
            const previous = self.request orelse return error.MissingRunRequest;
            if (actual.batch != previous.batch or actual.sequence != previous.sequence or actual.input_bytes != previous.input_bytes) {
                if (self.dispatch_cleanup_epoch != null) return error.PreparedAdmissionRequiresReadyTurn;
                _ = try self.yieldExecution();
                var next = previous;
                next.batch = actual.batch;
                next.sequence = actual.sequence;
                next.input_bytes = actual.input_bytes;
                next.output_bytes = actual.output_bytes;
                next.output_kv_bytes = actual.output_kv_bytes;
                next.workspace_bytes = actual.workspace_bytes;
                self.request = next;
            }
        }

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
        self.retirePrepared();
        try self.retainHostBytes(resident);
        self.execution_yielded = true;
        return true;
    }

    /// Drop transient forward scratch once only materialized outputs remain.
    pub fn retainOutputs(self: *RunPermit, outputs: []const Tensor) !void {
        if (self.lease) |*lease| try lease.retain(try retainedOutputAmounts(outputs, lease.amounts.host_kv_bytes > 0));
    }

    /// Move the exact host-output reservation after provider/model unlock.
    /// On failure outputs and this permit remain independently owned.
    pub fn attachHostOutputs(self: *RunPermit, outputs: []Tensor, allocator: std.mem.Allocator) ![]Tensor {
        return Session.attachOutputAdmission(outputs, allocator, &self.lease, self.session.resourcePin());
    }

    pub fn run(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) ![]Tensor {
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) return self.runWithControl(inputs, allocator, null);
        if (try self.executePrepared(.{ .host = inputs }, .host, allocator, null)) |result| return result.host;
        return @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, self.session.vtable.run, .{
            self.session.ptr,
            inputs,
            allocator,
        });
    }

    pub fn runWithControl(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) ![]Tensor {
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) {
            var execution = try self.acquireExecutionInputs(.{ .host = inputs }, allocator, control);
            defer execution.deinit();
            const outputs = try execution.runWithControl(inputs, allocator, control);
            errdefer deinitTensorSlice(outputs, allocator);
            return Session.attachOutputAdmission(outputs, allocator, &execution.lease, self.session.resourcePin());
        }
        if (try self.executePrepared(.{ .host = inputs }, .host, allocator, control)) |result| return result.host;
        const active = control orelse return self.run(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        const outputs = try @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, self.session.vtable.runWithControl, .{
            self.session.ptr,
            inputs,
            allocator,
            active,
        });
        errdefer deinitTensorSlice(outputs, allocator);
        try active.check();
        return outputs;
    }

    /// Use the chunk's existing admission for resident typed decisions.
    pub fn runLayaDecisionsWithControl(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) !?[]Tensor {
        const run_decisions = self.session.vtable.runLayaDecisions orelse return null;
        if (self.session.vtable.hasLayaDecisions) |supported| if (!supported(self.session.ptr)) return null;
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) {
            var execution = try self.acquireExecutionInputs(.{ .host = inputs }, allocator, control);
            defer execution.deinit();
            const outputs = (try execution.runLayaDecisionsWithControl(inputs, allocator, control)) orelse return null;
            errdefer deinitTensorSlice(outputs, allocator);
            return try Session.attachOutputAdmission(outputs, allocator, &execution.lease, self.session.resourcePin());
        }
        const active = control orelse InferenceExecutionControl{};
        try active.check();
        var protection = if (control) |c| try c.enterUninterruptible(self.session.interruption()) else null;
        defer if (protection) |*p| p.deinit();
        const outputs = (try @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, run_decisions, .{
            self.session.ptr,
            inputs,
            allocator,
            control,
        })) orelse return null;
        errdefer deinitTensorSlice(outputs, allocator);
        try active.check();
        return outputs;
    }

    pub fn runResident(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) return self.runResidentWithControl(inputs, allocator, null);
        if (try self.executePreparedResident(.{ .host = inputs }, allocator, null)) |outputs| return outputs;
        const run_resident = self.session.vtable.runResident orelse return null;
        var outputs = (try @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, run_resident, .{
            self.session.ptr,
            inputs,
            allocator,
        })) orelse return null;
        self.pinResident(&outputs);
        return outputs;
    }

    pub fn runResidentWithControl(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) !?ResidentOutputs {
        try self.validateResidentDispatch();
        try self.resolveGeometry(inputs);
        if (self.execution_yielded) {
            var execution = try self.acquireExecutionInputs(.{ .host = inputs }, allocator, control);
            defer execution.deinit();
            if (execution.lease != null) {
                try self.validateResidentOutputRetirement();
                try execution.adoptResidentOutputRetirement(&self.output_retirement_shell);
            }
            var outputs = (try execution.runResidentWithControl(inputs, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            self.pinResident(&outputs);
            return outputs;
        }
        if (try self.executePreparedResident(.{ .host = inputs }, allocator, control)) |outputs| return outputs;
        const active = control orelse return self.runResident(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        var outputs = if (self.session.vtable.runResidentWithControl) |run_controlled|
            (try @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, run_controlled, .{
                self.session.ptr,
                inputs,
                allocator,
                active,
            })) orelse return null
        else
            return null;
        self.pinResident(&outputs);
        errdefer self.discardFailedResident(&outputs);
        try active.check();
        return outputs;
    }

    /// The permit and hard-cancellation scope cover imports as well as execution.
    /// All input owners remain borrowed until the backend has consumed them.
    pub fn runStageInputsWithControl(self: *RunPermit, inputs: []const StageInput, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?ResidentOutputs {
        try self.validateResidentDispatch();
        if (!self.session.supportsStageInputs(inputs)) return null;
        if (control != null and self.session.vtable.runResidentInputsWithControl == null) return null;
        if (self.execution_yielded) {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const info = try stageShapeInfo(self.session, inputs, arena.allocator());
            var request = self.request orelse return error.MissingRunRequest;
            request.pre_admitted_host_bytes = @min(if (self.lease) |lease| lease.amounts.host_scratch_bytes else 0, try addBytes(request.input_bytes, request.host_preprocess_bytes));
            var execution = try self.session.admitShapeRequest(.{ .stage_inputs = .{ .info = info, .values = inputs } }, request, allocator, control);
            defer execution.deinit();
            if (execution.lease != null) {
                try self.validateResidentOutputRetirement();
                try execution.adoptResidentOutputRetirement(&self.output_retirement_shell);
            }
            var outputs = (try execution.runStageInputsWithControl(inputs, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            self.pinResident(&outputs);
            return outputs;
        }
        if (control) |active| try active.check();
        var protection = if (control) |active| try active.enterUninterruptible(self.session.interruption()) else null;
        defer if (protection) |*scope| scope.deinit();
        return @import("../execution/invocation_observation.zig").callStage(self.session.invocation_diagnostics, inputs, RunPermit.runStageInputsWithinBoundary, .{ self, inputs, allocator, control });
    }

    fn runStageInputsWithinBoundary(self: *RunPermit, inputs: []const StageInput, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?ResidentOutputs {
        const shared = self.session.shared_compute_context.?;
        const backend = shared.stableBackend();
        const resident = try allocator.alloc(ResidentInput, inputs.len);
        defer allocator.free(resident);
        var initialized: usize = 0;
        defer for (inputs[0..initialized], resident[0..initialized]) |input, value| {
            if (input == .host) backend.free(value.value);
        };
        for (inputs, resident) |input, *value| {
            if (control) |active| try active.check();
            value.* = switch (input) {
                .host => |host| .{
                    .value = try shared.importHostTensor(allocator, &host),
                    .backend = backend,
                    .shape = host.shape,
                    .estimated_bytes = host.data.len,
                },
                .resident => |existing| existing,
            };
            initialized += 1;
        }
        if (try self.executePreparedResident(.{ .resident = resident }, allocator, control)) |outputs| return outputs;
        var outputs = (if (control) |active|
            try @import("../execution/invocation_observation.zig").callResident(self.session.invocation_diagnostics, resident, self.session.vtable.runResidentInputsWithControl.?, .{ self.session.ptr, resident, allocator, active })
        else
            try @import("../execution/invocation_observation.zig").callResident(self.session.invocation_diagnostics, resident, self.session.vtable.runResidentInputs.?, .{ self.session.ptr, resident, allocator })) orelse return null;
        self.pinResident(&outputs);
        errdefer self.discardFailedResident(&outputs);
        if (control) |active| try active.check();
        return outputs;
    }

    pub fn runResidentInputs(
        self: *RunPermit,
        inputs: []const ResidentInput,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        if (self.execution_yielded) return self.runResidentInputsWithControl(inputs, allocator, null);
        if (try self.executePreparedResident(.{ .resident = inputs }, allocator, null)) |outputs| return outputs;
        const run_resident_inputs = self.session.vtable.runResidentInputs orelse
            return null;
        var outputs = (try @import("../execution/invocation_observation.zig").callResident(self.session.invocation_diagnostics, inputs, run_resident_inputs, .{
            self.session.ptr,
            inputs,
            allocator,
        })) orelse return null;
        self.pinResident(&outputs);
        return outputs;
    }

    pub fn runResidentInputsWithControl(
        self: *RunPermit,
        inputs: []const ResidentInput,
        allocator: std.mem.Allocator,
        control: ?InferenceExecutionControl,
    ) !?ResidentOutputs {
        try self.validateResidentDispatch();
        if (self.execution_yielded) {
            var execution = try self.acquireExecutionInputs(.{ .resident = inputs }, allocator, control);
            defer execution.deinit();
            if (execution.lease != null) {
                try self.validateResidentOutputRetirement();
                try execution.adoptResidentOutputRetirement(&self.output_retirement_shell);
            }
            var outputs = (try execution.runResidentInputsWithControl(inputs, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            self.pinResident(&outputs);
            return outputs;
        }
        if (try self.executePreparedResident(.{ .resident = inputs }, allocator, control)) |outputs| return outputs;
        const active = control orelse return self.runResidentInputs(inputs, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        const run_controlled = self.session.vtable.runResidentInputsWithControl orelse return null;
        var outputs = (try @import("../execution/invocation_observation.zig").callResident(self.session.invocation_diagnostics, inputs, run_controlled, .{
            self.session.ptr,
            inputs,
            allocator,
            active,
        })) orelse return null;
        self.pinResident(&outputs);
        errdefer self.discardFailedResident(&outputs);
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
        var outputs = (try @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, run_resident, .{
            self.session.ptr,
            inputs,
            request,
            allocator,
        })) orelse return null;
        self.pinResident(&outputs);
        return outputs;
    }

    /// Reduce an execution peak after preprocessing/vision has completed.
    /// Re-estimate with the session profile and retained inputs; a phase may
    /// never borrow capacity from its predecessor or another concurrent run.
    /// Reduce two independently owned leases without merging their external
    /// process-owner tokens. All required capacity must already be admitted.
    pub fn retainForRunWithSupplement(self: *RunPermit, extra: *RunPermit, request: RunRequest) !void {
        self.retirePrepared();
        if (extra.session.ptr != self.session.ptr or extra.session.vtable != self.session.vtable or self.execution_yielded or extra.execution_yielded) return error.InvalidAdmissionLeaseReduction;
        const admission = self.session.run_admission orelse {
            self.request = request;
            return;
        };
        const base = if (self.lease) |*lease| lease else return error.InvalidAdmissionLeaseReduction;
        const needed = try admission.estimateRequest(request, self.session.outputInfo());
        const available = try base.amounts.merge(if (extra.lease) |lease| lease.amounts else .{});
        var base_keep = needed;
        var extra_keep = needed;
        inline for (@typeInfo(memory.AdmissionAmounts).@"struct".field_names) |name| {
            if (@field(needed, name) > @field(available, name)) return error.InvalidAdmissionLeaseReduction;
            @field(base_keep, name) = @min(@field(needed, name), @field(base.amounts, name));
            @field(extra_keep, name) = @field(needed, name) - @field(base_keep, name);
        }
        try base.retain(base_keep);
        if (extra.lease) |*lease| try lease.retain(extra_keep);
        self.request = request;
    }

    pub fn retainForRun(self: *RunPermit, request: RunRequest) !void {
        self.retirePrepared();
        if (self.execution_yielded) return error.InvalidAdmissionLeaseReduction;
        if (self.lease) |*lease| {
            const admission = self.session.run_admission orelse return error.InvalidAdmissionLeaseReduction;
            try lease.retain(try admission.estimateRequest(request, self.session.outputInfo()));
        }
        self.request = request;
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
        try self.validateResidentDispatch();
        if (self.execution_yielded) {
            var execution = try self.acquireExecutionInputs(.{ .host = inputs }, allocator, control);
            defer execution.deinit();
            if (execution.lease != null) {
                try self.validateResidentOutputRetirement();
                try execution.adoptResidentOutputRetirement(&self.output_retirement_shell);
            }
            var outputs = (try execution.runResidentTextEmbeddingWithControl(inputs, request, allocator, control)) orelse return null;
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = execution.lease;
            execution.lease = null;
            self.pinResident(&outputs);
            return outputs;
        }
        const active = control orelse return self.runResidentTextEmbedding(inputs, request, allocator);
        try active.check();
        var hard_cancellation = try active.enterUninterruptible(self.session.interruption());
        defer hard_cancellation.deinit();
        var outputs = if (self.session.vtable.runResidentTextEmbeddingWithControl) |run_controlled|
            (try @import("../execution/invocation_observation.zig").call(self.session.invocation_diagnostics, run_controlled, .{
                self.session.ptr,
                inputs,
                request,
                allocator,
                active,
            })) orelse return null
        else
            return null;
        self.pinResident(&outputs);
        errdefer self.discardFailedResident(&outputs);
        try active.check();
        return outputs;
    }

    pub fn deinit(self: *RunPermit) void {
        self.deinitWithCleanup(.{});
    }
    pub fn deinitWithCleanup(self: *RunPermit, cleanup: @import("../execution/cleanup_context.zig").Context) void {
        self.retirePreparedWithCleanup(cleanup);
        if (self.output_retirement_shell) |*prepared| prepared.destroyUnused();
        self.output_retirement_shell = null;
        if (self.lease) |*lease| lease.release();
        self.lease = null;
        if (self.resource_pin) |pin| pin.releaseWithBoundary(cleanup.boundary);
        self.resource_pin = null;
    }
};

fn residentShapeInfo(session: Session, inputs: []const ResidentInput, allocator: std.mem.Allocator) ![]TensorInfo {
    if (inputs.len != session.inputInfo().len) return error.InputArityMismatch;
    const info = try allocator.alloc(TensorInfo, inputs.len);
    for (inputs, session.inputInfo(), info) |input, declared, *actual| actual.* = .{
        .name = declared.name,
        .dtype = declared.dtype,
        .shape = if (input.shape.len != 0) input.shape else try input.backend.tensorShape(input.value, allocator),
    };
    return info;
}

fn stageShapeInfo(session: Session, inputs: []const StageInput, allocator: std.mem.Allocator) ![]TensorInfo {
    if (inputs.len != session.inputInfo().len) return error.InputArityMismatch;
    const info = try allocator.alloc(TensorInfo, inputs.len);
    for (inputs, session.inputInfo(), info) |input, declared, *actual| actual.* = switch (input) {
        .host => |host| .{ .name = host.name, .dtype = host.dtype, .shape = host.shape },
        .resident => |value| .{ .name = declared.name, .dtype = declared.dtype, .shape = if (value.shape.len != 0) value.shape else try value.backend.tensorShape(value.value, allocator) },
    };
    return info;
}

fn deinitTensorSlice(tensors: []Tensor, allocator: std.mem.Allocator) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

test "session vtable exposes required entry points" {
    // Optional capabilities can be added without changing this contract.
    try std.testing.expect(@hasField(Session.VTable, "run"));
    try std.testing.expect(@hasField(Session.VTable, "inputInfo"));
    try std.testing.expect(@hasField(Session.VTable, "outputInfo"));
    try std.testing.expect(@hasField(Session.VTable, "backend"));
    try std.testing.expect(@hasField(Session.VTable, "close"));
    try std.testing.expect(@hasField(Session.VTable, "independentBatchRows"));
    try std.testing.expect(@hasField(Session.VTable, "hasLayaDecisions"));
    try std.testing.expect(@hasField(Session.VTable, "runLayaDecisions"));
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

test "laya decision permit reuses admission and releases yielded output leases" {
    const Probe = struct {
        controller: *memory.AdmissionController,
        expected: memory.AdmissionAmounts = .{},
        fail: bool = false,

        fn runDecisions(ptr: *anyopaque, _: []const Tensor, allocator: std.mem.Allocator, _: ?InferenceExecutionControl) !?[]Tensor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.expected, self.controller.snapshot());
            if (self.fail) return error.InvalidLayaOutput;
            const outputs = try allocator.alloc(Tensor, 1);
            errdefer allocator.free(outputs);
            outputs[0] = try Tensor.initFloat32(allocator, "decisions", &.{1}, &.{1});
            return outputs;
        }
    };
    var controller = memory.AdmissionController{};
    var probe = Probe{ .controller = &controller };
    var vtable = AdmissionProbeSession.vtable;
    vtable.runLayaDecisions = Probe.runDecisions;
    const session = Session{
        .ptr = &probe,
        .vtable = &vtable,
        .run_admission = .{
            .controller = &controller,
            .backend_class = .cpu,
            .limits = .{},
            .static_workspace_bytes = 4096,
        },
    };
    for ([_]bool{ false, true }) |yielded| {
        var permit = try session.admit(.{ .batch = 1, .sequence = 1, .output_bytes = 12 });
        defer permit.deinit();
        probe.expected = controller.snapshot();
        if (yielded) try std.testing.expect(try permit.yieldExecution());
        const outputs = (try permit.runLayaDecisionsWithControl(&.{}, std.testing.allocator, .{})).?;
        deinitTensorSlice(outputs, std.testing.allocator);
        probe.fail = true;
        try std.testing.expectError(error.InvalidLayaOutput, permit.runLayaDecisionsWithControl(&.{}, std.testing.allocator, null));
        probe.fail = false;
        permit.deinit();
        try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    }
}

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

test "run admission credits retained Qwen workspace once and keeps hard limits" {
    var controller = memory.AdmissionController{};
    const admission = RunAdmission{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{ .scratch_limit_bytes = 384 * memory.bytes_per_mib },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
        .model_profile = .{
            .hidden_size = 1024,
            .intermediate_size = 3072,
            .attention_heads = 16,
            .qwen_dense_workspace = true,
            .query_width = 2048,
            .kv_width = 1024,
        },
    };
    const request = RunRequest{ .batch = 2, .sequence = 256, .input_bytes = 512 * 12, .output_bytes = 2 * 1024 * 4 };
    const peak = try admission.estimateRequest(request, &.{});
    var narrow = admission;
    narrow.model_profile.query_width = 1024;
    try std.testing.expect(peak.backend_scratch_bytes > (try narrow.estimateRequest(request, &.{})).backend_scratch_bytes);
    var retained = try admission.acquireRequest(request, &.{});
    defer retained.release();
    const retained_bytes = 160 * memory.bytes_per_mib;
    try retained.retain(.{ .backend_scratch_bytes = retained_bytes });
    var credited = request;
    credited.retained_backend_workspace_bytes = retained_bytes;
    var next = try admission.acquireRequest(credited, &.{});
    defer next.release();
    try std.testing.expectEqual(peak, controller.snapshot());
    var larger = credited;
    larger.batch = 8;
    larger.input_bytes *= 4;
    larger.output_bytes = request.output_bytes.? * 4;
    try std.testing.expectError(error.ResourceLimitExceeded, admission.acquireRequest(larger, &.{}));
    try std.testing.expectEqual(peak, controller.snapshot());
    next.release();
    try std.testing.expectEqual(retained_bytes, controller.snapshot().backend_scratch_bytes);
    var smaller = RunRequest{ .batch = 7, .sequence = 20, .input_bytes = 140 * 12, .output_bytes = 7 * 1024 * 4 };
    smaller.retained_backend_workspace_bytes = retained_bytes;
    const fresh = try admission.qwenTransientWorkspaceBytes(140);
    smaller.workspace_bytes = retained_bytes + fresh;
    var small_permit = try admission.acquireRequest(smaller, &.{});
    defer small_permit.release();
    try std.testing.expectEqual(fresh, small_permit.amounts.backend_scratch_bytes);
    try std.testing.expectEqual(retained_bytes + fresh, controller.snapshot().backend_scratch_bytes);
    small_permit.release();
    retained.release();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "run admission scales dynamic outputs and honors reserved backend workspace" {
    var controller = memory.AdmissionController{};
    var input_bytes = @as([64]u8, @splat(0));
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

    var phased = try admitted_session.admit(.{ .batch = 2, .sequence = 32, .input_bytes = 64, .host_preprocess_bytes = 512, .workspace_bytes = 4096, .output_bytes = 256 });
    const peak = controller.snapshot().host_scratch_bytes;
    try phased.retainForRun(.{ .batch = 2, .sequence = 4, .input_bytes = 64, .host_preprocess_bytes = 128, .workspace_bytes = 512, .output_bytes = 64 });
    const retained = controller.snapshot().host_scratch_bytes;
    try std.testing.expect(retained < peak);
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, phased.retainForRun(.{ .workspace_bytes = 16384 }));
    try std.testing.expectEqual(retained, controller.snapshot().host_scratch_bytes);
    phased.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());

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

test "EmbeddingGemma2 CUDA admission follows the bounded resident arena" {
    var controller = memory.AdmissionController{};
    const admission = RunAdmission{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{},
        .static_workspace_bytes = 1024 * 1024 * 1024,
        .model_profile = .{
            .hidden_size = 512,
            .intermediate_size = 2048,
            .attention_heads = 4,
            .embedding_gemma2_cuda = true,
        },
    };
    const b8 = try admission.profiledWorkspace(.{ .batch = 8, .sequence = 8192, .input_bytes = 8 * 8192 * 24 });
    const b32 = try admission.profiledWorkspace(.{ .batch = 32, .sequence = 8192, .input_bytes = 32 * 8192 * 24 });
    try std.testing.expect(b8 < 2 * 1024 * 1024 * 1024);
    try std.testing.expect(b32 < 6 * 1024 * 1024 * 1024);
    try std.testing.expect(b32 > b8);
    const tail = try admission.profiledWorkspace(.{ .batch = 32, .sequence = 270, .input_bytes = 32 * 270 * 24 });
    const complete_tile = try admission.profiledWorkspace(.{ .batch = 32, .sequence = 320, .input_bytes = 32 * 270 * 24 });
    try std.testing.expectEqual(complete_tile, tail);
    const tail_amounts = try admission.estimateRequest(.{ .batch = 32, .sequence = 8191, .input_bytes = 32 * 8191 * 512 * @sizeOf(f32) }, &.{});
    try std.testing.expect(tail_amounts.host_scratch_bytes >= 32 * 8192 * 512 * @sizeOf(f32));
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        admission.profiledHostScratch(.{ .batch = std.math.maxInt(usize), .sequence = 8191 }),
    );
    try std.testing.expectEqual(@as(usize, 127), embeddingGemma2AdmissionSequence(127));
    try std.testing.expectEqual(@as(usize, 8192), embeddingGemma2AdmissionSequence(8191));
    try std.testing.expectEqual(std.math.maxInt(usize), embeddingGemma2AdmissionSequence(std.math.maxInt(usize)));
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        admission.profiledWorkspace(.{ .batch = std.math.maxInt(usize), .sequence = 8192 }),
    );
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
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Source.check, .{ false, false });
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
    _ = try Session.attachOutputAdmission(&outputs, alloc, &permit.lease, null);
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
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(?InferenceExecutionControl, null)});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(?InferenceExecutionControl, .{})});
}

test "embeddinggemma2 stage supplements preserve base admission and reject overcommit" {
    var controller = memory.AdmissionController{};
    var probe = AdmissionProbeSession{ .controller = &controller };
    const session = Session{ .ptr = &probe, .vtable = &AdmissionProbeSession.vtable, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .scratch_limit_bytes = 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var base = try session.admitHostPreprocess(256);
    var extra = try session.admitSupplement(&base, .{ .host_scratch_bytes = 768 });
    try std.testing.expectEqual(@as(usize, 768), controller.snapshot().host_scratch_bytes);
    extra.deinit();
    try std.testing.expectEqual(@as(usize, 256), controller.snapshot().host_scratch_bytes);
    var peer = try session.admitHostPreprocess(256);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, session.admitSupplement(&base, .{ .host_scratch_bytes = 900 }));
    try std.testing.expectEqual(@as(usize, 512), controller.snapshot().host_scratch_bytes);
    peer.deinit();
    var smaller = try session.admitSupplement(&base, .{ .host_scratch_bytes = 128 });
    smaller.deinit();
    try std.testing.expectEqual(@as(usize, 256), controller.snapshot().host_scratch_bytes);
    var retry = try session.admitSupplement(&base, .{ .host_scratch_bytes = 900 });
    try std.testing.expectEqual(@as(usize, 900), controller.snapshot().host_scratch_bytes);
    retry.deinit();
    base.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "embeddinggemma2 CUDA phased admission retires media peak without borrowing" {
    var controller = memory.AdmissionController{};
    const admission = RunAdmission{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false, .model_profile = .{ .hidden_size = 512, .intermediate_size = 2048, .attention_heads = 4, .embedding_gemma2_cuda = true } };
    const before = try admission.estimateRequest(.{ .sequence = 1024, .host_preprocess_bytes = 128 * 1024 * 1024 }, &.{});
    const after = try admission.estimateRequest(.{ .sequence = 64, .host_preprocess_bytes = 1024, .preprocessing_complete = true }, &.{});
    try std.testing.expect(before.backend_scratch_bytes > after.backend_scratch_bytes + 384 * 1024 * 1024);
    var lease = try controller.tryAcquire(.gpu, .{}, before, false);
    defer lease.release();
    try lease.retain(after);
    try std.testing.expectEqual(after, controller.snapshot());
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, lease.retain(before));
    try std.testing.expectEqual(after, controller.snapshot());
}

test "embeddinggemma2 physical backend inputs do not charge retained host rasters as device staging" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const admission = RunAdmission{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const before = try admission.estimateRequest(.{ .input_bytes = 128 * 1024 * 1024, .pre_admitted_host_bytes = 128 * 1024 * 1024, .workspace_bytes = 64 * 1024 * 1024, .output_bytes = 768 * 4 }, &.{});
    const after = try admission.estimateRequest(.{ .input_bytes = 128 * 1024 * 1024, .backend_input_bytes = 512 * 260 * 4, .pre_admitted_host_bytes = 128 * 1024 * 1024, .workspace_bytes = 64 * 1024 * 1024, .output_bytes = 768 * 4 }, &.{});
    try std.testing.expectEqual(before.host_scratch_bytes, after.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), after.backend_scratch_bytes);
    try std.testing.expect(before.backend_scratch_bytes > after.backend_scratch_bytes + 600 * 1024 * 1024);
    try std.testing.expectError(error.ResourceLimitExceeded, admission.estimateRequest(.{ .input_bytes = 128, .backend_input_bytes = 0, .pre_admitted_host_bytes = 129 }, &.{}));
}

test "embeddinggemma2 stage admission reduces independent supplements without borrowing or merging tokens" {
    const Mock = struct {
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{};
        }
    };
    var marker: u8 = 0;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const session = Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = undefined, .close = undefined }, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
    const mb = 1024 * 1024;
    var base = try session.admit(.{ .workspace_bytes = 16 * mb, .output_bytes = 0 });
    defer base.deinit();
    var extra = try session.admitSupplement(&base, try session.run_admission.?.estimateRequest(.{ .workspace_bytes = 64 * mb, .output_bytes = 0 }, &.{}));
    defer extra.deinit();
    var covered = try session.admitSupplementPair(&base, &extra, try session.run_admission.?.estimateRequest(.{ .workspace_bytes = 64 * mb, .output_bytes = 0 }, &.{}));
    defer covered.deinit();
    try std.testing.expect(covered.lease == null);
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, session.admitSupplementPair(&base, &base, .{}));
    try base.retainForRunWithSupplement(&extra, .{ .workspace_bytes = 40 * mb, .output_bytes = 0 });
    try std.testing.expectEqual(@as(usize, 40 * mb), controller.snapshot().backend_scratch_bytes);
    const before = controller.snapshot();
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, base.retainForRunWithSupplement(&extra, .{ .workspace_bytes = 41 * mb, .output_bytes = 0 }));
    try std.testing.expectEqual(before, controller.snapshot());
    extra.deinit();
    base.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().scratchTotalBytes());
}

test "embeddinggemma2 explicit phase workspace replaces only the generic model allowance" {
    var controller = memory.AdmissionController{};
    const admission = RunAdmission{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 4096, .check_live_memory = false, .model_profile = .{ .hidden_size = 16, .intermediate_size = 32 } };
    var request = RunRequest{ .workspace_bytes = 128, .backend_input_bytes = 0, .output_bytes = 0 };
    try std.testing.expectEqual(@as(usize, 4096), (try admission.estimateRequest(request, &.{})).backend_scratch_bytes);
    request.workspace_planned = true;
    // The model profile remains a hard floor above the explicit 128 bytes.
    try std.testing.expectEqual(@as(usize, 512), (try admission.estimateRequest(request, &.{})).backend_scratch_bytes);
    request.backend_input_bytes = 1024;
    try std.testing.expectEqual(@as(usize, 6144), (try admission.estimateRequest(request, &.{})).backend_scratch_bytes);
    request.workspace_bytes = 0;
    try std.testing.expectError(error.ResourceLimitExceeded, admission.estimateRequest(request, &.{}));
}

test "resident preparation retains model owner and published policy through wrappers" {
    const Probe = struct {
        calls: usize = 0,
        published_ptr: *anyopaque,
        fail: bool = false,
        fn prepare(raw: *anyopaque, published: Session, control: ?InferenceExecutionControl) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(published.ptr == self.published_ptr);
            try std.testing.expect(published.model_contract.resident_text_embeddings);
            try std.testing.expect(published.generation_workspace != null);
            try (control orelse return error.MissingControl).check();
            self.calls += 1;
            if (self.fail) return error.PreparationFailed;
        }
        fn cancelled(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var wrapper: u8 = 0;
    var workspace: memory.ReservedGenerationWorkspace = undefined;
    var probe = Probe{ .published_ptr = &wrapper };
    const wrapped = Session{
        .ptr = &wrapper,
        .vtable = undefined,
        .model_contract = .{ .resident_text_embeddings = true },
        .generation_workspace = &workspace,
        .resident_preparation = .{ .ptr = &probe, .prepare = Probe.prepare },
    };
    // Deliberately no usable vtable: preparation addresses its retained model
    // owner and receives the wrapper's policy without rediscovering its type.
    try wrapped.prepareResident(.{});
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectError(error.Cancelled, wrapped.prepareResident(.{ .check_fn = Probe.cancelled }));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    probe.fail = true;
    try std.testing.expectError(error.PreparationFailed, wrapped.prepareResident(.{}));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    var unsupported = wrapped;
    unsupported.resident_preparation = null;
    try unsupported.prepareResident(.{});
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}

test "resident model retention quiesces immediately and destroys compute before weights and resource domain" {
    const Probe = struct {
        phase: usize = 0,
        invalid_order: bool = false,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn step(self: *@This(), expected: usize) void {
            if (self.phase != expected) self.invalid_order = true;
            self.phase += 1;
        }
        fn quiesce(raw: *anyopaque) void {
            from(raw).step(0);
        }
        fn computeClose(raw: *anyopaque, _: std.mem.Allocator) void {
            from(raw).step(1);
        }
        fn close(raw: *anyopaque) void {
            from(raw).step(2);
        }
        fn pinRelease(raw: *anyopaque) void {
            from(raw).step(3);
        }
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.NotUsed;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) BackendType {
            return .native;
        }
        fn resident(raw: *anyopaque, _: []const Tensor, a: std.mem.Allocator) !?ResidentOutputs {
            return .{ .outputs = try a.alloc(ops.CT, 0), .backend = undefined, .allocator = a, .backend_owner = raw, .deinit_backend_owner = computeClose };
        }
        const vtable = Session.VTable{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close, .runResident = resident, .quiesce = quiesce };
    };
    var probe = Probe{};
    var lifetime = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.close, .quiesce_fn = Probe.quiesce };
    var session = Session{ .ptr = &probe, .vtable = &Probe.vtable, .model_lifetime = &lifetime };
    const pin = Session.ModelLifetime.ResourcePin{ .ptr = &probe, .release_fn = Probe.pinRelease };
    try session.attachResourcePin(std.testing.allocator, pin);
    try std.testing.expectError(error.ResourcePinAlreadyAttached, session.attachResourcePin(std.testing.allocator, pin));
    var output = (try session.runResident(&.{}, std.testing.allocator)).?;
    try std.testing.expectEqual(@as(usize, 2), lifetime.references.load(.monotonic));
    session.close();
    try std.testing.expectEqual(@as(usize, 1), probe.phase);
    try std.testing.expectEqual(@as(usize, 1), lifetime.references.load(.monotonic));
    output.deinit();
    try std.testing.expectEqual(@as(usize, 4), probe.phase);
    try std.testing.expect(!probe.invalid_order);
}

test "cold model retention adoption is failure atomic" {
    const Case = struct {
        closed: bool = false,
        pinned: bool = false,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn close(raw: *anyopaque) void {
            from(raw).closed = true;
        }
        fn release(raw: *anyopaque) void {
            from(raw).pinned = false;
        }
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.NotUsed;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) BackendType {
            return .native;
        }
        const vtable = Session.VTable{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
        fn check(a: std.mem.Allocator) !void {
            var probe = @This(){};
            var session = Session{ .ptr = &probe, .vtable = &vtable };
            var opened = true;
            defer if (opened) session.close();
            const pin = Session.ModelLifetime.ResourcePin{ .ptr = &probe, .release_fn = release };
            try session.attachResourcePin(a, pin);
            probe.pinned = true;
            const retained = session.model_lifetime.?.retain();
            session.close();
            opened = false;
            try std.testing.expect(!probe.closed and probe.pinned);
            retained.release();
            try std.testing.expect(probe.closed and !probe.pinned);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.check, .{});
}

test "run request plan preserves exact protected compatibility and uncredited peaks" {
    const request = RunRequest{
        .batch = 2,
        .sequence = 8,
        .input_bytes = 64,
        .host_preprocess_bytes = 32,
        .pre_admitted_host_bytes = 16,
        .output_bytes = 128,
        .output_kv_bytes = 48,
        .retained_backend_workspace_bytes = 512,
    };
    for ([_]memory.BackendClass{ .cpu, .gpu }) |kind| {
        for ([_]bool{ false, true }) |reserved| {
            var controller = memory.AdmissionController{};
            const admission = RunAdmission{ .controller = &controller, .backend_class = kind, .limits = .{}, .static_workspace_bytes = 2048, .backend_workspace_reserved = reserved, .check_live_memory = false };
            const plan = try admission.planRequest(request, &.{});
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 240, .host_kv_bytes = 96 }, plan.protected);
            const compatibility: memory.AdmissionAmounts = if (kind == .cpu)
                .{ .host_scratch_bytes = 2048 }
            else
                .{ .backend_scratch_bytes = if (reserved) 0 else 1536 };
            try std.testing.expectEqualDeep(compatibility, plan.compatibility);
            try std.testing.expectEqualDeep(try plan.protected.merge(compatibility), plan.required);
            try std.testing.expectEqualDeep(plan.required, try admission.estimateRequest(request, &.{}));
            var uncredited = request;
            uncredited.pre_admitted_host_bytes = 0;
            uncredited.retained_backend_workspace_bytes = 0;
            try std.testing.expectEqualDeep(try admission.estimateRequest(uncredited, &.{}), try plan.uncredited());
            try std.testing.expectEqual(@as(usize, if (kind == .cpu) 2288 else 240), plan.required.host_scratch_bytes);
            try std.testing.expectEqual(@as(usize, if (kind == .cpu) 2304 else 256), (try plan.uncredited()).host_scratch_bytes);
        }
    }
}

test "credited request estimator defers unrepresentable uncredited peak" {
    var controller = memory.AdmissionController{};
    const admission = RunAdmission{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const request = RunRequest{
        .batch = 1,
        .sequence = 1,
        .host_preprocess_bytes = std.math.maxInt(usize),
        .pre_admitted_host_bytes = std.math.maxInt(usize),
        .output_bytes = 1,
    };
    const plan = try admission.planRequest(request, &.{});
    try std.testing.expectEqual(@as(usize, 8), plan.required.host_scratch_bytes);
    try std.testing.expectEqualDeep(plan.required, try admission.estimateRequest(request, &.{}));
    try std.testing.expectError(error.ResourceLimitExceeded, plan.uncredited());
}

test "resident output ordinary backend destruction retains actual request reservation through Native cleanup" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var controller = memory.AdmissionController{};
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    const Probe = struct {
        compute: native.NativeCompute,
        controller: *memory.AdmissionController,
        observed_charged: bool = false,
        closed: bool = false,
        fn cleanup(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observed_charged = self.controller.snapshot().host_scratch_bytes == 64;
            self.compute.deinit();
            self.closed = true;
        }
    };
    var probe = Probe{ .compute = native.NativeCompute.init(a, &weights, null), .controller = &controller };
    const backend = probe.compute.computeBackend();
    var outputs = ResidentOutputs{
        .outputs = try a.alloc(ops.CT, 0),
        .backend = &backend,
        .allocator = a,
        .backend_owner = &probe,
        .deinit_backend_owner = Probe.cleanup,
        .resource_lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = 1024 }, .{ .host_scratch_bytes = 64 }, false),
    };
    try outputs.prepareRetirement();
    outputs.deinit();
    try std.testing.expect(probe.closed);
    try std.testing.expect(probe.observed_charged);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "resident output resource promotion is failure atomic and retains zero-output ownership through final borrower" {
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    const Probe = struct {
        controller: *memory.AdmissionController,
        backend_closed: bool = false,
        model_closed: bool = false,
        fn kind(_: *anyopaque) ops.BackendKind {
            return .native;
        }
        fn backendClose(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.backend_closed and !self.model_closed);
            std.debug.assert(self.controller.snapshot().host_scratch_bytes == 64);
            self.backend_closed = true;
        }
        fn modelClose(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.backend_closed and !self.model_closed);
            std.debug.assert(self.controller.snapshot().host_scratch_bytes == 0);
            self.model_closed = true;
        }
    };
    var probe = Probe{ .controller = &controller };
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.modelClose };
    // Empty tensor vectors still use backend kind/retirement metadata. This
    // fixture's only physical close is its explicit backendClose callback.
    var table: ops.ComputeBackend.VTable = undefined;
    table.backendKind = Probe.kind;
    table.cold_cleanup_services = null;
    table.resource_retirement_services = null;
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    var outputs = ResidentOutputs{
        .outputs = try a.alloc(ops.CT, 0),
        .backend = &backend,
        .allocator = a,
        .backend_owner = &probe,
        .deinit_backend_owner = Probe.backendClose,
        .model_retention = &model,
        .resource_lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = 1024 }, .{ .host_scratch_bytes = 64 }, false),
    };
    try outputs.prepareRetirement();
    var original_open = true;
    defer if (original_open) outputs.deinit();
    try std.testing.expectError(error.OutOfMemory, outputs.retainResources(std.testing.failing_allocator));
    try std.testing.expect(outputs.backend_owner == @as(*anyopaque, @ptrCast(&probe)));
    try std.testing.expect(outputs.resource_lease != null and outputs.model_retention == &model and outputs.resource_hold == null);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_scratch_bytes);
    const first = try outputs.retainResources(a);
    var first_open = true;
    defer if (first_open) first.release();
    // Repeated sharing borrows the same cold owner without another allocation.
    const second = try outputs.retainResources(std.testing.failing_allocator);
    var second_open = true;
    defer if (second_open) second.release();
    try std.testing.expect(first.ptr == second.ptr);
    try std.testing.expect(outputs.backend_owner == null and outputs.resource_lease == null and outputs.model_retention == null);
    outputs.deinit();
    original_open = false;
    try std.testing.expect(!probe.backend_closed and !probe.model_closed);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_scratch_bytes);
    first.release();
    first_open = false;
    try std.testing.expect(!probe.backend_closed and !probe.model_closed);
    second.release();
    second_open = false;
    try std.testing.expect(probe.backend_closed and probe.model_closed);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

fn checkRetainedResidentTensors(cb: ops.ComputeBackend, _: ops.ComputeBackend) !void {
    const a = std.testing.allocator;
    const tensor = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 });
    const values = try a.alloc(ops.CT, 2);
    values[0] = tensor;
    values[1] = tensor;
    var outputs = ResidentOutputs{ .outputs = values, .backend = &cb, .allocator = a };
    const pin = try outputs.retainResources(a);
    defer pin.release();
    outputs.deinit();
    const retained = try cb.toFloat32(tensor, a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, retained);
    // Final pin release deduplicates the actual Native handle and frees the
    // descriptor vector exactly once; the testing allocator verifies both.
}

test "resident output shared resources preserve actual Native tensors after original output retirement" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(@import("../ops/media_residency_test.zig").noWeights, checkRetainedResidentTensors);
}

fn checkActualSharedResidentViews(cb: ops.ComputeBackend, _: ops.ComputeBackend) !void {
    const a = std.testing.allocator;
    const tensor = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 4 });
    const original_values = try a.alloc(ops.CT, 1);
    original_values[0] = tensor;
    var original = ResidentOutputs{ .outputs = original_values, .backend = &cb, .allocator = a };
    var original_open = true;
    defer if (original_open) original.deinit();
    const first_views = try a.alloc(ops.CT, 1);
    first_views[0] = try cb.sliceRows2D(a, tensor, 0, 1, 4);
    var views_owned = true;
    defer if (views_owned) {
        cb.free(first_views[0]);
        a.free(first_views);
    };
    try std.testing.expectError(error.OutOfMemory, original.ownViewsOf(std.testing.failing_allocator, first_views));
    try std.testing.expect(original.resource_hold == null);
    const still_owned = try cb.toFloat32(first_views[0], a);
    defer a.free(still_owned);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, still_owned);
    var first = try original.ownViewsOf(a, first_views);
    views_owned = false;
    var first_open = true;
    defer if (first_open) first.deinit();
    const second_views = try a.alloc(ops.CT, 1);
    second_views[0] = try cb.sliceRows2D(a, tensor, 1, 1, 4);
    var second = try original.ownViewsOf(a, second_views);
    defer second.deinit();
    try std.testing.expect(first.backend == original.backend and second.backend == original.backend);
    try std.testing.expect(first.backend_owner == second.backend_owner);
    try std.testing.expect(first.resource_hold == null and second.resource_hold == null);
    original.deinit();
    original_open = false;
    first.deinit();
    first_open = false;
    const retained = try second.backend.toFloat32(second.outputs[0], a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8 }, retained);
}

test "resident child views retain actual source storage across original and sibling retirement" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(@import("../ops/media_residency_test.zig").noWeights, checkActualSharedResidentViews);
}

test "resident final cleanup borrows stable model close protection without arming idle retention" {
    const a = std.testing.allocator;
    const Probe = struct {
        scopes: usize = 0,
        begins: usize = 0,
        ends: usize = 0,
        owner_releases: usize = 0,
        backend_closed: bool = false,
        model_closed: bool = false,
        fn begin(raw: *anyopaque) Session.CloseScope {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.scopes += 1;
            self.begins += 1;
            return .{ .ptr = raw, .release_fn = end };
        }
        fn end(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes > 0);
            self.scopes -= 1;
            self.ends += 1;
        }
        fn releaseProtection(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owner_releases += 1;
        }
        fn backendClose(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes > 0 and !self.backend_closed and !self.model_closed);
            self.backend_closed = true;
        }
        fn modelClose(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes > 0 and self.backend_closed and !self.model_closed);
            self.model_closed = true;
        }
    };
    var probe = Probe{};
    const protection = Session.CloseProtection{ .ptr = &probe, .begin_fn = Probe.begin, .release_fn = Probe.releaseProtection };
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.modelClose };
    model.installBorrowedCloseProtection(protection);
    model.installBorrowedCloseProtection(protection);
    try std.testing.expectEqual(@as(usize, 0), probe.begins);
    var unused_backend: ops.ComputeBackend = undefined;
    var outputs = ResidentOutputs{ .outputs = try a.alloc(ops.CT, 0), .backend = &unused_backend, .allocator = a, .backend_owner = &probe, .deinit_backend_owner = Probe.backendClose, .model_retention = model.retain() };
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    var lease: ?memory.AdmissionLease = null;
    model.close(protection, &lease);
    try std.testing.expectEqual(@as(usize, 0), probe.scopes);
    try std.testing.expectEqual(@as(usize, 0), probe.owner_releases);
    try std.testing.expect(!probe.backend_closed and !probe.model_closed);
    outputs.deinit();
    outputs_open = false;
    try std.testing.expect(probe.backend_closed and probe.model_closed);
    try std.testing.expectEqual(@as(usize, 0), probe.scopes);
    try std.testing.expectEqual(probe.begins, probe.ends);
    try std.testing.expectEqual(@as(usize, 3), probe.begins);
    try std.testing.expectEqual(@as(usize, 1), probe.owner_releases);
}

test "session diagnostics cold installation holds published gate before enabled publication" {
    const Probe = struct {
        diagnostics: *@import("../execution/diagnostics.zig").Diagnostics,
        gate: *std.atomic.Mutex,
        calls: usize = 0,
        saw_gate_locked: bool = false,
        saw_disabled: bool = false,
        fn prepare(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.gate.tryLock()) {
                self.gate.unlock();
                self.saw_gate_locked = false;
            } else self.saw_gate_locked = true;
            self.saw_disabled = !self.diagnostics.enabled.load(.acquire);
        }
    };
    var diagnostics = @import("../execution/diagnostics.zig").Diagnostics{};
    var gate: std.atomic.Mutex = .unlocked;
    var probe = Probe{ .diagnostics = &diagnostics, .gate = &gate };
    var table: ops.ComputeBackend.VTable = undefined;
    table.enableRuntimeObservation = Probe.prepare;
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    const table_session: Session.VTable = undefined;
    const session = Session{ .ptr = &probe, .vtable = &table_session, .execution_gate = &gate, .invocation_diagnostics = .{ .diagnostics = &diagnostics, .stable_backend = backend } };
    try std.testing.expect(session.configureExecutionDiagnostics(true));
    try std.testing.expect(probe.calls == 1 and probe.saw_gate_locked and probe.saw_disabled);
    try std.testing.expect(diagnostics.enabled.load(.acquire));
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(session.configureExecutionDiagnostics(false));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(!diagnostics.enabled.load(.acquire));
}

fn ownedPermitPreparationHandoff(a: std.mem.Allocator) !void {
    if (comptime !@import("build_options").enable_native or @import("build_options").enable_wasm) return error.SkipZigTest;
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    try graph.markOutput(try builder.neg(x));
    const bytes = try @import("onnx_graph").exportGraph(a, &graph, .{});
    defer a.free(bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "owned-permit.onnx", .data = bytes });
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "owned-permit.onnx" });
    defer a.free(path);
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var session = try @import("imported_onnx_session.zig").createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .partitioned });
    var open = true;
    defer if (open) session.close();
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .static_workspace_bytes = 1024, .static_workspace_kind = .estimate, .check_live_memory = false };
    var input = try Tensor.initFloat32(a, "x", &.{ 1, 4 }, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    const original = session.invocation_services.?;
    const Probe = struct {
        services: invocation.Services,
        original: *const invocation.Services,
        expected: ?*RunPermit = null,
        into_calls: usize = 0,
        value_calls: usize = 0,
        fn owner(session_value: *const Session) *@This() {
            return @constCast(@as(*const @This(), @fieldParentPtr("services", session_value.invocation_services.?)));
        }
        fn prepareInto(session_value: *const Session, shapes: ShapeInputs, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, out: *invocation.PreparedInvocation) !void {
            const self = owner(session_value);
            if (self.expected) |permit| {
                // The optional discriminator is defined before calling the
                // provider; no payload field has been initialized/read yet.
                if (permit.prepared == null or out != &permit.prepared.?) return error.CopiedPreparedPermitPayload;
            }
            self.into_calls += 1;
            try self.original.prepare_into.?(session_value, shapes, allocator, control, out);
        }
        fn prepareValue(session_value: Session, shapes: ShapeInputs, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !invocation.PreparedInvocation {
            const self = owner(&session_value);
            self.value_calls += 1;
            return self.original.prepare(session_value, shapes, allocator, control);
        }
        fn reject(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.WrapperPolicyRejected;
        }
    };
    var probe = Probe{ .services = original.*, .original = original };
    // This fixture exercises legacy into/value overloads. Opt out of the
    // newer copied cleanup-aware overload which otherwise takes precedence.
    probe.services.prepare_into_with_cleanup = null;
    probe.services.prepare_into = Probe.prepareInto;
    probe.services.prepare = Probe.prepareValue;
    session.invocation_services = &probe.services;
    const before = session.model_lifetime.?.references.load(.acquire);
    {
        var permit: RunPermit = undefined;
        probe.expected = &permit;
        try session.admitInputsInto(&.{input}, a, null, &permit);
        defer permit.deinit();
        try std.testing.expectEqual(@as(usize, 1), probe.into_calls);
        try std.testing.expectEqual(@as(usize, 0), probe.value_calls);
        try std.testing.expectEqual(before + 1, session.model_lifetime.?.references.load(.acquire));
        try std.testing.expectEqual(@as(usize, 64), permit.session.run_admission.?.limits.host_limit_bytes);
        try std.testing.expectEqual(@as(usize, 64), permit.prepared.?.published.run_admission.?.limits.host_limit_bytes);
        // Both returned policy snapshots are values, not borrowed pointers to
        // the caller's subsequently changed policy/Session stack.
        session.run_admission.?.limits = .{ .host_limit_bytes = 63, .combined_limit_bytes = 63 };
        const outputs = try permit.run(&.{input}, a);
        defer {
            for (outputs) |*output| output.deinit();
            a.free(outputs);
        }
        try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, outputs[0].asFloat32());
        try std.testing.expectEqual(@as(usize, 64), permit.session.run_admission.?.limits.host_limit_bytes);
    }
    probe.expected = null;
    session.run_admission.?.limits = .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 };
    try std.testing.expectEqual(before, session.model_lifetime.?.references.load(.acquire));
    // Existing providers without the optional callback keep their exact value
    // constructor and all lifetime/admission semantics.
    probe.services.prepare_into = null;
    {
        var permit: RunPermit = undefined;
        try session.admitInputsInto(&.{input}, a, null, &permit);
        defer permit.deinit();
        try std.testing.expectEqual(@as(usize, 1), probe.value_calls);
        const outputs = try permit.run(&.{input}, a);
        defer {
            for (outputs) |*output| output.deinit();
            a.free(outputs);
        }
        try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, outputs[0].asFloat32());
    }
    probe.services.prepare_into = Probe.prepareInto;
    var table = session.vtable.*;
    table.run = Probe.reject;
    var wrapped = session;
    wrapped.vtable = &table;
    wrapped.run_admission = null;
    const calls = probe.into_calls;
    var denied: ?invocation.PreparedInvocation = null;
    try invocation.prepareInto(&wrapped, .{ .tensors = &.{input} }, a, null, &denied);
    try std.testing.expect(denied == null);
    try std.testing.expectEqual(calls, probe.into_calls);
    if (wrapped.run(&.{input}, a)) |outputs| {
        for (outputs) |*output| output.deinit();
        a.free(outputs);
        return error.ExpectedWrapperPolicyRejection;
    } else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.WrapperPolicyRejected, err);
    }
    var held = (try session.runResident(&.{input}, a)) orelse return error.MissingResidentOutput;
    defer held.deinit();
    session.close();
    open = false;
    const values = try held.backend.toFloat32(held.outputs[0], a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ -1, -2, -3, -4 }, values);
}

test "owned permit handoff constructs actual prepared payload in place and preserves snapshots fallback policy held outputs and OOM" {
    try ownedPermitPreparationHandoff(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, ownedPermitPreparationHandoff, .{});
}

test "actual external budget callbacks reenter after physical close with watchdog owners pinned" {
    const domain_mod = @import("../runtime/tier/admission_domain.zig");
    const Probe = struct {
        controller: *memory.AdmissionController,
        scopes: usize = 0,
        protection_owners: usize = 1,
        resource_owners: usize = 1,
        model_destroyed: bool = false,
        backend_destroyed: bool = false,
        require_backend: bool = false,
        invalid_owner_callback: bool = false,
        callback_error: ?anyerror = null,
        reservations: usize = 0,
        releases: usize = 0,
        reentries: usize = 0,
        fn protection(self: *@This()) Session.CloseProtection {
            return .{ .ptr = self, .begin_fn = begin, .release_fn = releaseProtection };
        }
        fn begin(raw: *anyopaque) Session.CloseScope {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.scopes += 1;
            return .{ .ptr = self, .release_fn = end };
        }
        fn end(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes != 0);
            self.scopes -= 1;
        }
        fn releaseProtection(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.protection_owners == 1);
            self.protection_owners = 0;
        }
        fn releaseResource(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.resource_owners == 1);
            self.resource_owners = 0;
        }
        fn quiesce(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes != 0 and !self.model_destroyed);
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes != 0 and !self.model_destroyed);
            self.model_destroyed = true;
        }
        fn destroyBackend(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scopes != 0 and !self.backend_destroyed);
            self.backend_destroyed = true;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reservations += 1;
            return self.reservations;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
            if (token != 1) return; // Nested admission uses its own exactly-once token.
            self.invalid_owner_callback = self.protection_owners != 1 or self.resource_owners != 1 or (if (self.require_backend) !self.backend_destroyed else !self.model_destroyed);
            // Exercise real external-budget -> admission-controller reentry and
            // an independently nested public CloseProtection/CloseScope pair.
            // An outer watchdog deadline scope is legitimate and does not
            // imply that a provider/publication mutex is held.
            var nested = self.controller.tryAcquire(.cpu, .{}, .{}, false) catch |err| {
                self.callback_error = err;
                return;
            };
            var scope = self.protection().begin();
            scope.deinit();
            self.reentries += 1;
            nested.release();
        }
    };
    for (0..3) |mode| {
        const domain = try domain_mod.Domain.create();
        defer domain.release();
        var probe = Probe{ .controller = &domain.controller, .require_backend = mode == 2 };
        try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
        errdefer if (lease) |*owned| owned.release();
        var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroy, .quiesce_fn = Probe.quiesce, .resource_pin = .{ .ptr = &probe, .release_fn = Probe.releaseResource }, .admission_domain = domain.pin() };
        const protection = probe.protection();
        model.installBorrowedCloseProtection(protection);
        if (mode == 0) {
            model.model_lease = lease;
            lease = null;
            model.close_protection_owned = true;
            model.release();
        } else if (mode == 1) {
            model.close(protection, &lease);
        } else {
            const native = @import("../ops/native_compute.zig");
            var weights = native.WeightStore{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty };
            defer weights.deinitOwned();
            var compute = native.NativeCompute.init(std.testing.allocator, &weights, null);
            defer compute.deinit();
            const backend = compute.computeBackend();
            var outputs = ResidentOutputs{ .outputs = try std.testing.allocator.alloc(ops.CT, 0), .backend = &backend, .allocator = std.testing.allocator, .backend_owner = &probe, .deinit_backend_owner = Probe.destroyBackend, .resource_lease = lease, .model_retention = model.retain() };
            try outputs.prepareRetirement();
            lease = null;
            var no_model_lease: ?memory.AdmissionLease = null;
            model.close(protection, &no_model_lease);
            outputs.deinit();
        }
        if (probe.callback_error) |err| return err;
        try std.testing.expect(!probe.invalid_owner_callback);
        try std.testing.expectEqual(@as(usize, 1), probe.reentries);
        try std.testing.expectEqual(@as(usize, 2), probe.reservations);
        try std.testing.expectEqual(@as(usize, 2), probe.releases);
        try std.testing.expectEqual(@as(usize, 0), probe.scopes);
        try std.testing.expectEqual(@as(usize, 0), probe.protection_owners);
        try std.testing.expectEqual(@as(usize, 0), probe.resource_owners);
        try std.testing.expect(probe.model_destroyed);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    }
}

test "planned bank controller quota does not retain model or serving IO ownership" {
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const Probe = struct {
        resources: usize = 1,
        model_destroyed: bool = false,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resources += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resources -= 1;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.model_destroyed = true;
        }
    };
    const domain = try Domain.create();
    var domain_live = true;
    defer if (domain_live) domain.release();
    var probe = Probe{};
    const resource = Session.ModelLifetime.ResourcePin{ .ptr = &probe, .retain_fn = Probe.retain, .release_fn = Probe.release };
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroy, .resource_pin = resource, .admission_domain = domain.pin() };
    var model_live = true;
    defer if (model_live) model.release();
    var permit = RunPermit{ .session = .{ .ptr = &probe, .vtable = undefined, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64 }, .static_workspace_bytes = 0, .check_live_memory = false } }, .lease = try domain.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_scratch_bytes = 64 }, false), .physical_credit = .{ .host_scratch_bytes = 16 }, .resource_pin = resource.retain() };
    defer permit.deinit();
    var bank = try RunPermit.reservePhysical(&permit, std.testing.allocator, .native, .activation, 16);
    defer bank.release();
    try std.testing.expectEqual(@as(usize, 2), probe.resources); // model + permit, no IO pin in bank
    permit.deinit();
    model.release();
    model_live = false;
    domain.release();
    domain_live = false;
    try std.testing.expect(probe.model_destroyed);
    try std.testing.expectEqual(@as(usize, 0), probe.resources);
    const held = (try bank.retainAdmissionDomain()) orelse return error.MissingPureBankDomain;
    defer held.release();
    try std.testing.expectEqual(@as(usize, 16), held.controller().snapshot().host_scratch_bytes);
    bank.release();
    try std.testing.expectEqual(@as(usize, 0), held.controller().snapshot().host_scratch_bytes);
}

test "resident shared final cleanup forwards only the final sibling epoch" {
    const Sink = @import("../execution/retirement_sink.zig");
    const Probe = struct {
        frees: usize = 0,
        nonce: u64 = 0,
        fn ordinary(raw: *anyopaque, _: ops.CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
        }
        fn bounded(raw: *anyopaque, tensor: ops.CT, cleanup_context: @import("../execution/cleanup_context.zig").Context) void {
            const epoch = cleanup_context.boundary;
            ordinary(raw, tensor);
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (epoch) |actual| {
                std.debug.assert(!(actual.isSealed() catch unreachable));
                self.nonce = (actual.boundary() catch unreachable).nonce;
            }
        }
        fn accepts(table: *const ops.ComputeBackend.VTable) bool {
            return table == &table_impl;
        }
        const cleanup = ops.ColdCleanupServices{ .accepts = accepts, .free = bounded };
        const table_impl: ops.ComputeBackend.VTable = blk: {
            var table: ops.ComputeBackend.VTable = undefined;
            table.freeTensor = ordinary;
            table.cold_cleanup_services = &cleanup;
            table.resource_retirement_services = null;
            break :blk table;
        };
        fn run(allocator: std.mem.Allocator) !void {
            var sink = try Sink.Ref.create();
            defer sink.release();
            var first = try sink.enter();
            defer first.finish();
            var second = try sink.enter();
            defer second.finish();
            var first_epoch = try first.retainEpoch();
            defer first_epoch.release();
            var final_epoch = try second.retainEpoch();
            defer final_epoch.release();
            var probe = @This(){};
            // Only these two cold callbacks are used by this fixture. There is
            // no payload/SDK activity and resource_lease remains absent.
            const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table_impl };
            const outputs = try allocator.alloc(ops.CT, 1);
            outputs[0] = @ptrCast(&probe);
            var owner = ResidentOutputs{ .outputs = outputs, .backend = &backend, .allocator = allocator };
            var owner_live = true;
            defer if (owner_live) owner.deinit();
            const sibling = try owner.retainResources(allocator);
            owner_live = false;
            owner.deinitWithBoundary(&first_epoch);
            try std.testing.expectEqual(@as(usize, 0), probe.frees);
            first.finish();
            sibling.releaseWithBoundary(&final_epoch);
            try std.testing.expectEqual(@as(usize, 1), probe.frees);
            try std.testing.expectEqual((try final_epoch.boundary()).nonce, probe.nonce);
            try std.testing.expect(probe.nonce != (try first_epoch.boundary()).nonce);
        }
    };
    try Probe.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "failed synchronous receiver preparation retains source charge beyond model caller Io and arena cleanup" {
    if (comptime @import("build_options").enable_wasm or (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos)) return error.SkipZigTest;
    const completion = @import("../execution/completion_retirement.zig");
    const Sink = @import("../execution/retirement_sink.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const platform = @import("antfly_platform");
    const Probe = struct {
        gate: std.atomic.Mutex = .unlocked,
        model_destroyed: bool = false,
        io_destroyed: bool = false,
        arena_destroyed: bool = false,
        callbacks: std.atomic.Value(usize) = .init(0),
        bad: std.atomic.Value(bool) = .init(false),
        frees: usize = 0,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.model_destroyed = true;
        }
        fn kind(_: *anyopaque) ops.BackendKind {
            return .native;
        }
        fn free(raw: *anyopaque, _: ops.CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
        }
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 7;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (token != 7 or !self.model_destroyed or !self.io_destroyed or !self.arena_destroyed) self.bad.store(true, .release);
            if (self.gate.tryLock()) self.gate.unlock() else self.bad.store(true, .release);
            _ = self.callbacks.fetchAdd(1, .acq_rel);
        }
        fn retainContext(_: *anyopaque) bool {
            return true;
        }
        fn releaseContext(_: *anyopaque) void {}
    };
    var probe = Probe{};
    const domain = try Domain.create();
    defer domain.release();
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release, .retain_context = Probe.retainContext, .release_context = Probe.releaseContext });
    var sink = try Sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    // This independent fence outlives every metadata owner below and bounds all
    // stack-budget witnesses through actual callback return, not merely ledger0.
    defer {
        guard.finish();
        const end = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
        while (completion.testingPending() != 0) {
            if (platform.time.monotonicNs() >= end) @panic("deferred failed preparation did not retire");
            std.testing.io.sleep(.fromMilliseconds(1), .awake) catch unreachable;
        }
    }
    var caller_io = std.Io.Threaded.init(std.testing.allocator, .{});
    var io_open = true;
    defer if (io_open) {
        caller_io.deinit();
        probe.io_destroyed = true;
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    var arena_open = true;
    defer if (arena_open) {
        arena.deinit();
        probe.arena_destroyed = true;
    };
    const allocator = arena.allocator();
    const model = try allocator.create(Session.ModelLifetime);
    model.* = .{ .owner = &probe, .destroy_fn = Probe.destroy, .allocator = allocator, .admission_domain = domain.pin() };
    var model_owned = true;
    defer if (model_owned) model.release();
    try model.prepareFinalResources();
    var table: ops.ComputeBackend.VTable = undefined;
    table.backendKind = Probe.kind; // Synthetic payload has no SDK origin; metadata is still concrete.
    table.freeTensor = Probe.free;
    table.cold_cleanup_services = null;
    table.resource_retirement_services = null;
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    const vector = try allocator.alloc(ops.CT, 1);
    vector[0] = @ptrCast(&probe);
    const lease = try domain.controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 8 }, .{ .backend_scratch_bytes = 8 }, false);
    var outputs = ResidentOutputs{ .outputs = vector, .backend = &backend, .allocator = allocator, .resource_lease = lease, .model_retention = model };
    try outputs.prepareRetirement();
    model_owned = false;
    var outputs_open = true;
    defer if (outputs_open) outputs.deinitWithBoundary(&epoch);
    const resources = try outputs.retainResources(allocator);
    var resources_open = true;
    defer if (resources_open) resources.releaseWithBoundary(&epoch);
    const capability = resources.payloadCharge().?;
    const deadline = platform.time.monotonicNs() + 30 * std.time.ns_per_s;
    // Reserve all fallible/token metadata before the real source lane. A
    // synchronous failure after commit keeps charge with the original holder.
    try std.testing.expect(try capability.prepare_fn(capability.context, 8, deadline));
    try std.testing.expect(probe.gate.tryLock());
    capability.commit_fn(capability.context);
    probe.gate.unlock();
    capability.abort_fn(capability.context);
    try std.testing.expectEqual(@as(usize, 8), domain.controller.snapshot().backend_scratch_bytes);
    // A later synchronous attempt reuses the original committed charge; it
    // cannot carve or renew the whole-task quota/deadline a second time.
    try std.testing.expect(try capability.prepare_fn(capability.context, 8, deadline));
    try std.testing.expect(probe.gate.tryLock());
    capability.commit_fn(capability.context);
    probe.gate.unlock();
    capability.abort_fn(capability.context);
    try std.testing.expectEqual(@as(usize, 8), domain.controller.snapshot().backend_scratch_bytes);
    outputs.deinitWithBoundary(&epoch);
    outputs_open = false;
    resources.releaseWithBoundary(&epoch);
    resources_open = false;
    try std.testing.expect(probe.model_destroyed);
    try std.testing.expectEqual(@as(usize, 1), probe.frees);
    try std.testing.expectEqual(@as(usize, 0), probe.callbacks.load(.acquire));
    caller_io.deinit();
    io_open = false;
    probe.io_destroyed = true;
    arena.deinit();
    arena_open = false;
    probe.arena_destroyed = true;
    try std.testing.expectEqual(@as(usize, 8), domain.controller.snapshot().backend_scratch_bytes);
    guard.finish();
    const end = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (completion.testingPending() != 0) {
        if (platform.time.monotonicNs() >= end) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1), probe.callbacks.load(.acquire));
    try std.testing.expect(!probe.bad.load(.acquire));
}

test "model retirement sink is shared cold metadata and survives actual model lifetime destruction" {
    const Sink = @import("../execution/retirement_sink.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const Probe = struct {
        destroyed: bool = false,
        calls: usize = 0,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed = true;
        }
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.destroyed);
            self.calls += 1;
        }
    };
    const a = std.testing.allocator;
    var probe = Probe{};
    const model = try a.create(Session.ModelLifetime);
    model.* = .{ .owner = &probe, .destroy_fn = Probe.destroy, .allocator = a };
    var model_live = true;
    defer if (model_live) model.release();
    var first = try model.retainRetirementSink();
    defer first.release();
    var second = try model.retainRetirementSink();
    defer second.release();
    try std.testing.expectEqual(first.identity, second.identity);
    var guard = try first.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var node = retirement.Node{ .context = &probe, .release_fn = Probe.release };
    const boundary = try epoch.boundary();
    try boundary.register(boundary.context, boundary.nonce, &node);
    model.release();
    model_live = false;
    try std.testing.expect(probe.destroyed);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    try std.testing.expectError(error.RetirementSinkClosed, second.enter());
    try std.testing.expect(!try epoch.isSealed());
    guard.finish();
    try std.testing.expect(try epoch.isSealed());
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    _ = Sink;
}

test "final model cleanup uses current epoch and keeps quota protection through real outer unlock" {
    const sink_mod = @import("../execution/retirement_sink.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const Probe = struct {
        outer: std.atomic.Mutex = .unlocked,
        destroyed: bool = false,
        saw_boundary: bool = false,
        pins: usize = 1,
        protections: usize = 1,
        callbacks: usize = 0,
        invalid_callback: bool = false,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed = true;
        }
        fn destroyWithBoundary(raw: *anyopaque, epoch: ?*const sink_mod.Epoch) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.saw_boundary = epoch != null;
            if (epoch) |e| std.debug.assert(!(e.isSealed() catch unreachable));
            destroy(raw);
        }
        fn pinRelease(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pins -= 1;
        }
        fn beginProtection(_: *anyopaque) Session.CloseScope {
            return .{};
        }
        fn protectionRelease(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.protections -= 1;
        }
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn quotaRelease(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const unlocked = self.outer.tryLock();
            if (unlocked) self.outer.unlock();
            self.invalid_callback = !unlocked or !self.destroyed or self.pins != 1 or self.protections != 1;
            self.callbacks += 1;
        }
    };
    for (0..2) |mode| {
        var probe = Probe{};
        const domain = try Domain.create();
        defer domain.release();
        try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.quotaRelease });
        var model = Session.ModelLifetime{
            .owner = &probe,
            .destroy_fn = Probe.destroy,
            .destroy_with_boundary_fn = Probe.destroyWithBoundary,
            .model_lease = try domain.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 32 }, false),
            .resource_pin = .{ .ptr = &probe, .release_fn = Probe.pinRelease },
            .admission_domain = domain.pin(),
            .close_protection = .{ .ptr = &probe, .begin_fn = Probe.beginProtection, .release_fn = Probe.protectionRelease },
            .close_protection_owned = true,
        };
        model.prepareFinalResources() catch |err| {
            model.release();
            return err;
        };
        if (mode == 0) {
            var sink = try sink_mod.Ref.create();
            defer sink.release();
            var old_guard = try sink.enter();
            var old_epoch = try old_guard.retainEpoch();
            defer old_epoch.release();
            _ = model.retain();
            model.releaseWithBoundary(&old_epoch); // A is nonfinal, never stored.
            old_guard.finish();
            try std.testing.expect(!probe.destroyed);
            var current_guard = try sink.enter();
            var current_epoch = try current_guard.retainEpoch();
            defer current_epoch.release();
            @import("antfly_platform").sync.lockYielding(&probe.outer);
            model.releaseWithBoundary(&current_epoch);
            try std.testing.expect(probe.destroyed and probe.saw_boundary);
            try std.testing.expectEqual(@as(usize, 0), probe.callbacks);
            try std.testing.expect(domain.controller.snapshot().host_scratch_bytes != 0);
            probe.outer.unlock();
            current_guard.finish();
        } else model.releaseOutsideExecution(); // Explicit outer owner creates current B.
        try std.testing.expect(probe.destroyed and probe.saw_boundary);
        try std.testing.expectEqual(@as(usize, 1), probe.callbacks);
        try std.testing.expect(!probe.invalid_callback);
        try std.testing.expectEqual(@as(usize, 0), probe.pins);
        try std.testing.expectEqual(@as(usize, 0), probe.protections);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    }
}

test "Session close boundary accepts full canonical table and preserves copied custom close" {
    const tensor = @import("tensor.zig");
    const Probe = struct {
        ordinary: usize = 0,
        custom: usize = 0,
        boundary: usize = 0,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ordinary += 1;
        }
        fn customClose(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.custom += 1;
        }
        fn closeBoundary(raw: *anyopaque, _: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.boundary += 1;
        }
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.UnexpectedRun;
        }
        fn backendKind(_: *anyopaque) @import("backends.zig").BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn accepts(actual: *const Session.VTable) bool {
            return actual == &table;
        }
        const services = Session.CloseBoundaryServices{ .accepts = accepts, .close = closeBoundary };
        const table = Session.VTable{ .backend = backendKind, .run = run, .inputInfo = info, .outputInfo = info, .close = close, .close_boundary_services = &services };
    };
    for (0..2) |managed| {
        var probe = Probe{};
        var canonical = Session{ .ptr = &probe, .vtable = &Probe.table };
        if (managed != 0) try canonical.installOwnedExecutionGate(std.testing.allocator);
        canonical.closeWithBoundary(null);
        try std.testing.expectEqual(@as(usize, 1), probe.boundary);
        try std.testing.expectEqual(@as(usize, 0), probe.ordinary);
        var copied_table = Probe.table;
        copied_table.close = Probe.customClose;
        var copied = Session{ .ptr = &probe, .vtable = &copied_table };
        if (managed != 0) {
            try copied.installOwnedExecutionGate(std.testing.allocator);
            try std.testing.expect(copied.model_lifetime.?.destroy_with_boundary_fn == null);
        }
        copied.closeWithBoundary(null);
        try std.testing.expectEqual(@as(usize, 1), probe.custom);
        try std.testing.expectEqual(@as(usize, 1), probe.boundary);
    }
}

test "raw Session close retains lease and protection until current outer boundary ends" {
    const Sink = @import("../execution/retirement_sink.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const Probe = struct {
        domain_owner: *Domain,
        original_domain_live: bool = true,
        gate: std.atomic.Mutex = .unlocked,
        scope_active: bool = false,
        owners: usize = 1,
        destroyed: bool = false,
        releases: usize = 0,
        invalid: bool = false,
        fn begin(raw: *anyopaque) Session.CloseScope {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.scope_active and self.owners == 1);
            self.scope_active = true;
            return .{ .ptr = self, .release_fn = end };
        }
        fn end(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scope_active and self.owners == 1);
            self.scope_active = false;
        }
        fn dropProtection(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.scope_active and self.owners == 1);
            self.owners = 0;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.scope_active);
            self.destroyed = true;
            self.original_domain_live = false;
            self.domain_owner.release();
        }
        fn closeBoundary(raw: *anyopaque, _: ?*const Sink.Epoch) void {
            close(raw);
        }
        fn retainDomain(raw: *anyopaque, controller: *memory.AdmissionController) ?@import("../runtime/tier/admission_domain.zig").Pin {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (&self.domain_owner.controller != controller) return null;
            return self.domain_owner.pin();
        }
        fn accepts(actual: *const Session.VTable) bool {
            return actual == &table;
        }
        const services = Session.CloseBoundaryServices{ .accepts = accepts, .close = closeBoundary, .retain_admission_domain = retainDomain };
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn dropQuota(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const unlocked = self.gate.tryLock();
            if (unlocked) self.gate.unlock();
            self.invalid = !unlocked or self.scope_active or !self.destroyed or self.owners != 1;
            self.releases += 1;
        }
        fn backend(_: *anyopaque) BackendType {
            return .native;
        }
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.UnexpectedRun;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{};
        }
        const table = Session.VTable{ .backend = backend, .run = run, .inputInfo = info, .outputInfo = info, .close = close, .close_boundary_services = &services };
    };
    for (0..2) |late| {
        const domain = try Domain.create();
        var probe = Probe{ .domain_owner = domain };
        defer if (probe.original_domain_live) domain.release();
        try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.dropQuota });
        var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 8 }, false);
        var sink = try Sink.Ref.create();
        defer sink.release();
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        if (late != 0) guard.finish();
        var session = Session{ .ptr = &probe, .vtable = &Probe.table, .close_protection = .{ .ptr = &probe, .begin_fn = Probe.begin, .release_fn = Probe.dropProtection } };
        try session.prepareCloseRetainingResources(std.testing.allocator, &lease);
        if (late == 0) @import("antfly_platform").sync.lockYielding(&probe.gate);
        var locked = late == 0;
        defer if (locked) probe.gate.unlock();
        session.closeRetainingResourcesWithBoundary(&lease, &epoch);
        try std.testing.expect(lease == null and probe.destroyed and !probe.scope_active);
        if (late == 0) {
            try std.testing.expectEqual(@as(usize, 0), probe.releases);
            try std.testing.expectEqual(@as(usize, 1), probe.owners);
            probe.gate.unlock();
            locked = false;
            guard.finish();
        }
        try std.testing.expectEqual(@as(usize, 1), probe.releases);
        try std.testing.expectEqual(@as(usize, 0), probe.owners);
        try std.testing.expect(!probe.invalid);
        // Last pure pin is retired with the node; controller may be destroyed.
        try std.testing.expect(!probe.original_domain_live);
    }
}

test "cold compute target inherited services reject copied acquire tables before invalid context access" {
    const Probe = struct {
        calls: usize = 0,
        fn acquire(_: *anyopaque, _: Session, _: std.mem.Allocator, _: ComputeBackendOptions) !ops.ComputeBackend {
            return error.UnexpectedColdAcquire;
        }
        fn alteredAcquire(_: *anyopaque, _: Session, _: std.mem.Allocator, _: ComputeBackendOptions) !ops.ComputeBackend {
            return error.CustomAcquire;
        }
        fn snapshot(raw: *anyopaque, _: Session, _: ?InferenceExecutionControl) !?@import("cold_compute_target.zig").Target {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return null;
        }
        fn accepts(actual: *const ComputeBackendProvider.VTable) bool {
            return actual == &table;
        }
        const table = ComputeBackendProvider.VTable{ .acquire = acquire, .cold_target_services = &.{ .accepts = accepts, .snapshot = snapshot } };
    };
    var probe = Probe{};
    var session = Session{ .ptr = &probe, .vtable = undefined, .compute_backend_provider = .{ .ptr = &probe, .services = &Probe.table } };
    try std.testing.expect((try session.coldComputeTarget(null)) == null);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var copied = Probe.table;
    copied.acquire = Probe.alteredAcquire;
    session.compute_backend_provider = .{ .ptr = @ptrFromInt(16), .services = &copied };
    try std.testing.expect((try session.coldComputeTarget(null)) == null);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectError(error.CustomAcquire, session.acquireComputeBackend(std.testing.allocator, .{}));
}

test "cold compute provider services preserve original value and optional value widths" {
    const Original = struct {
        ptr: *anyopaque,
        acquire: *const fn (*anyopaque, Session, std.mem.Allocator, ComputeBackendOptions) anyerror!ops.ComputeBackend,
    };
    try std.testing.expectEqual(@sizeOf(Original), @sizeOf(ComputeBackendProvider));
    try std.testing.expectEqual(@sizeOf(?Original), @sizeOf(?ComputeBackendProvider));
    try std.testing.expectEqual(@alignOf(Original), @alignOf(ComputeBackendProvider));
}

test "resident owner Context descriptor veto preserves custom callback before backend access" {
    const Probe = struct {
        canonical_calls: usize = 0,
        replacement_calls: usize = 0,
        cleanup_calls: usize = 0,
        fn canonical(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.canonical_calls += 1;
        }
        fn replacement(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.replacement_calls += 1;
        }
        fn accepts(outputs: *const ResidentOutputs) bool {
            return outputs.deinit_backend_owner == canonical;
        }
        fn cleanup(raw: *anyopaque, _: std.mem.Allocator, _: @import("../execution/cleanup_context.zig").Context) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cleanup_calls += 1;
        }
        const services = ResidentOutputs.BackendOwnerCleanupServices{ .accepts = accepts, .deinit = cleanup };
    };
    var probe = Probe{};
    var outputs = ResidentOutputs{
        .outputs = try std.testing.allocator.alloc(ops.CT, 0),
        // A vetoed custom owner must never dispatch into this inherited backend.
        .backend = undefined,
        .allocator = std.testing.allocator,
        .backend_owner = &probe,
        .deinit_backend_owner = Probe.replacement,
        .backend_owner_cleanup_services = &Probe.services,
    };
    outputs.deinit();
    try std.testing.expectEqual(@as(usize, 0), probe.canonical_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.replacement_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.cleanup_calls);
}

test "raw final model pin release keeps nonfinal fast path and explicit outside callback" {
    const Probe = struct {
        destroyed: usize = 0,
        raw_calls: usize = 0,
        boundary_calls: usize = 0,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.destroyed == 1);
            self.raw_calls += 1;
        }
        fn releaseWithBoundary(raw: *anyopaque, _: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.boundary_calls += 1;
        }
    };
    var probe = Probe{};
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroy, .resource_pin = .{ .ptr = &probe, .release_fn = Probe.release, .release_with_boundary_fn = Probe.releaseWithBoundary } };
    _ = model.retain();
    model.release();
    try std.testing.expect(model.retirement_sink == null);
    try std.testing.expectEqual(@as(usize, 0), probe.destroyed);
    try std.testing.expectEqual(@as(usize, 0), probe.raw_calls);
    model.release();
    try std.testing.expectEqual(@as(usize, 1), probe.destroyed);
    try std.testing.expectEqual(@as(usize, 1), probe.raw_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.boundary_calls);
}

test "ResidentOutputs raw final model drop preserves explicit outside context" {
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const a = std.testing.allocator;
    const domain = try Domain.create();
    defer domain.release();
    const Probe = struct {
        destroyed: bool = false,
        callbacks: usize = 0,
        invalid: bool = false,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed = true;
        }
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.invalid = !self.destroyed;
            self.callbacks += 1;
        }
    };
    var probe = Probe{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroy, .admission_domain = domain.pin(), .model_lease = try domain.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false) };
    var model_open = true;
    defer if (model_open) model.release();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var outputs = ResidentOutputs{ .outputs = try a.alloc(ops.CT, 0), .backend = &backend, .allocator = a, .model_retention = &model };
    model_open = false;
    outputs.deinit();
    try std.testing.expect(probe.destroyed and !probe.invalid);
    try std.testing.expectEqual(@as(usize, 1), probe.callbacks);
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().host_scratch_bytes);
}

const FinalCapsuleProbe = struct {
    domain: *@import("../runtime/tier/admission_domain.zig").Domain,
    closes: usize = 0,
    releases: usize = 0,
    context_refs: usize = 0,
    fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
        return error.UnexpectedModelRun;
    }
    fn info(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) BackendType {
        return .native;
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.closes += 1;
    }
    fn closeBoundary(raw: *anyopaque, _: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        close(raw);
    }
    fn accepts(actual: *const Session.VTable) bool {
        return actual == &table;
    }
    fn retainDomain(raw: *anyopaque, controller: *memory.AdmissionController) ?@import("../runtime/tier/admission_domain.zig").Pin {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (&self.domain.controller != controller) return null;
        return self.domain.pin();
    }
    fn retainContext(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.context_refs += 1;
        return true;
    }
    fn releaseContext(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.context_refs -= 1;
    }
    fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
        return 1;
    }
    fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
    fn release(raw: *anyopaque, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.releases += 1;
    }
    const services = Session.CloseBoundaryServices{ .accepts = accepts, .close = closeBoundary, .retain_admission_domain = retainDomain };
    const table = Session.VTable{ .run = run, .backend = backend, .inputInfo = info, .outputInfo = info, .close = close, .close_boundary_services = &services };
};

fn finalCapsuleColdOomCase(allocator: std.mem.Allocator) !void {
    const Sink = @import("../execution/retirement_sink.zig");
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    var probe = FinalCapsuleProbe{ .domain = domain };
    defer std.debug.assert(probe.context_refs == 0);
    defer domain.release();
    try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = FinalCapsuleProbe.retainContext, .release_context = FinalCapsuleProbe.releaseContext, .try_reserve = FinalCapsuleProbe.reserve, .retain = FinalCapsuleProbe.retain, .release = FinalCapsuleProbe.release });
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
    defer if (lease) |*owned| owned.release();
    var session = Session{ .ptr = &probe, .vtable = &FinalCapsuleProbe.table };
    var open = true;
    defer if (open) session.close();
    var owner = try Sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try std.testing.expectError(error.UnpreparedModelClose, session.validatePreparedClose(&lease, .{ .boundary = &epoch }));
    try std.testing.expect(session.model_lifetime == null and lease != null and probe.closes == 0 and probe.releases == 0);
    session.prepareCloseRetainingResourcesWithAllocators(allocator, allocator, &lease) catch |err| {
        try std.testing.expect(session.model_lifetime == null and lease != null and probe.closes == 0 and probe.releases == 0);
        try std.testing.expectEqual(@as(usize, 16), domain.controller.snapshot().host_scratch_bytes);
        return err;
    };
    try std.testing.expect(lease != null and session.model_lifetime.?.prepared_final_resources != null);
    try std.testing.expect(session.model_lifetime.?.admission_domain.?.controller() == &domain.controller);
    session.closeRetainingResourcesWithBoundary(&lease, &epoch);
    open = false;
    try std.testing.expect(lease == null and probe.closes == 1 and probe.releases == 0);
    try std.testing.expectEqual(@as(usize, 16), domain.controller.snapshot().host_scratch_bytes);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().host_scratch_bytes);
}

test "cold final model capsule OOM preserves raw owner lease and pure domain before publication" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, finalCapsuleColdOomCase, .{});
}

test "final model capsule has no late allocation and current B permits real provider and model reentry" {
    const platform = @import("antfly_platform");
    const operation = @import("../execution/backend_operation.zig");
    const Shared = @import("../backends/compute_context.zig").SharedBackendContext;
    const Sink = @import("../execution/retirement_sink.zig");
    const a = std.testing.allocator;
    const source = try Shared.init(a, .native, null);
    defer source.release();
    const destination = try Shared.init(a, .native, null);
    defer destination.release();
    var marker: u8 = 0;
    const Destroy = struct {
        fn noop(_: *anyopaque) void {}
    };
    var outer_model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Destroy.noop };
    defer outer_model.release();
    const Probe = struct {
        source_owner: *Shared,
        model: *Session.ModelLifetime,
        backends: [2]ops.ComputeBackend,
        destroyed: bool = false,
        callbacks: usize = 0,
        failed: bool = false,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed = true;
            self.source_owner.release();
        }
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            if (!self.destroyed or !self.model.execution_gate.tryLock()) {
                self.failed = true;
                return;
            }
            self.model.execution_gate.unlock();
            for (&self.backends) |*backend| {
                var scope = operation.enter(backend.*, null, null, .managed) catch {
                    self.failed = true;
                    return;
                };
                defer scope.deinitChecked() catch @panic("quota callback provider cleanup failed");
                const input = backend.fromFloat32(&.{ 1, 3 }) catch {
                    self.failed = true;
                    return;
                };
                defer backend.free(input);
                const result = backend.add(input, input) catch {
                    self.failed = true;
                    return;
                };
                defer backend.free(result);
                const values = backend.toFloat32(result, std.heap.c_allocator) catch {
                    self.failed = true;
                    return;
                };
                defer std.heap.c_allocator.free(values);
                if (!std.mem.eql(f32, values, &.{ 2, 6 })) self.failed = true;
                scope.finish() catch {
                    self.failed = true;
                    return;
                };
            }
        }
    };
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var probe = Probe{ .source_owner = source.retain(), .model = &outer_model, .backends = .{ source.computeBackend(), destination.computeBackend() } };
    var physical_owner = true;
    defer if (physical_owner) probe.source_owner.release();
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroy, .model_lease = try domain.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false), .admission_domain = domain.pin() };
    var alive = true;
    defer if (alive) {
        model.release();
        physical_owner = false;
    };
    var allocation = std.testing.FailingAllocator.init(a, .{});
    try model.prepareFinalResourcesWithAllocator(allocation.allocator());
    const capsule = model.prepared_final_resources;
    const cold_allocations = allocation.alloc_index;
    allocation.fail_index = cold_allocations; // Every later capsule allocator request fails.
    var owner = try Sink.Ref.create();
    defer owner.release();
    var old_guard = try owner.enter();
    defer old_guard.finish();
    var old_epoch = try old_guard.retainEpoch();
    defer old_epoch.release();
    _ = model.retain();
    model.releaseWithBoundary(&old_epoch); // Nonfinal remains atomic and stores no A.
    old_guard.finish();
    try std.testing.expect(model.prepared_final_resources == capsule and !probe.destroyed);
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    platform.sync.lockYielding(&outer_model.execution_gate);
    var locked = true;
    defer if (locked) outer_model.execution_gate.unlock();
    var scope = try operation.enter(probe.backends[1], null, null, .managed);
    defer scope.deinitChecked() catch @panic("final capsule outer provider cleanup failed");
    try std.testing.expect(try scope.bindCleanupEpoch(&epoch));
    model.releaseWithBoundary(&epoch);
    alive = false;
    physical_owner = false;
    try std.testing.expect(probe.destroyed and model.prepared_final_resources == null and probe.callbacks == 0);
    try std.testing.expectEqual(@as(usize, 16), domain.controller.snapshot().host_scratch_bytes);
    try scope.finish();
    outer_model.execution_gate.unlock();
    locked = false;
    guard.finish();
    try std.testing.expectEqual(@as(usize, 1), probe.callbacks);
    try std.testing.expect(!probe.failed);
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(cold_allocations, allocation.alloc_index);
    try std.testing.expect(!allocation.has_induced_failure);
}

test "cold final model capsule rejects foreign and unsupported domains before ownership moves" {
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const a = std.testing.allocator;
    const first = try Domain.create();
    defer first.release();
    const second = try Domain.create();
    defer second.release();
    var probe = FinalCapsuleProbe{ .domain = second };
    var lease: ?memory.AdmissionLease = try first.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
    defer if (lease) |*owned| owned.release();
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = FinalCapsuleProbe.close, .admission_domain = second.pin() };
    defer model.release();
    var session = Session{ .ptr = &probe, .vtable = &FinalCapsuleProbe.table, .model_lifetime = &model };
    try std.testing.expectError(error.ForeignAdmissionDomain, session.prepareCloseRetainingResources(a, &lease));
    try std.testing.expect(session.model_lifetime == &model and model.prepared_final_resources == null and lease != null and probe.closes == 0);
    var copied = FinalCapsuleProbe.table;
    var raw = Session{ .ptr = &probe, .vtable = &copied };
    try std.testing.expectError(error.MissingPureCloseDomain, raw.prepareCloseRetainingResources(a, &lease));
    try std.testing.expect(raw.model_lifetime == null and lease != null and probe.closes == 0);
    try std.testing.expectEqual(@as(usize, 16), first.controller.snapshot().host_scratch_bytes);
}

test "unpublished final model capsule abort releases only inert metadata" {
    const Probe = struct {
        fn close(raw: *anyopaque) void {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += 1;
        }
    };
    var closes: usize = 0;
    var model = Session.ModelLifetime{ .owner = &closes, .destroy_fn = Probe.close };
    var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    try model.prepareFinalResourcesWithAllocator(allocator.allocator());
    try std.testing.expect(model.prepared_final_resources != null);
    model.discardUnpublishedFinalResources();
    try std.testing.expect(model.prepared_final_resources == null and closes == 0 and model.references.load(.acquire) == 1);
    model.discardUnpublishedFinalResources(); // Empty abort is idempotent.
    model.release();
    try std.testing.expectEqual(@as(usize, 1), closes);
}

test "bound raw resident cancellation after actual Native forward keeps quota until current B unlock and two provider reentry" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Shared = @import("compute_context.zig").SharedBackendContext;
    const operation = @import("../execution/backend_operation.zig");
    const Sink = @import("../execution/retirement_sink.zig");
    const first = try Shared.init(a, .native, null);
    defer first.release();
    const second = try Shared.init(a, .native, null);
    defer second.release();
    const Probe = struct {
        backends: [2]ops.ComputeBackend,
        gate: *std.atomic.Mutex,
        lease: ?memory.AdmissionLease = null,
        canceled: bool = false,
        callbacks: usize = 0,
        failure: ?anyerror = null,
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.UnexpectedHostForward;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{.{ .name = "x", .dtype = .f32, .shape = &.{ 1, 2 } }};
        }
        fn backend(_: *anyopaque) BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.Canceled;
        }
        fn forward(raw: *anyopaque, _: []const Tensor, allocator: std.mem.Allocator, _: InferenceExecutionControl) !?ResidentOutputs {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const backend_value = &self.backends[0];
            var scope = try operation.enter(backend_value.*, null, null, .managed);
            defer scope.deinitChecked() catch @panic("cancel fixture provider close failed");
            const values = try allocator.alloc(ops.CT, 1);
            errdefer allocator.free(values);
            values[0] = try backend_value.fromFloat32Shape(&.{ 1, 3 }, &.{ 1, 2 });
            errdefer backend_value.free(values[0]);
            try scope.finish();
            const lease = self.lease;
            self.lease = null;
            self.canceled = true;
            return .{ .outputs = values, .backend = backend_value, .allocator = allocator, .resource_lease = lease };
        }
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            self.reenter() catch |err| {
                self.failure = err;
            };
        }
        fn reenter(self: *@This()) !void {
            if (!self.gate.tryLock()) return error.QuotaReleasedUnderModelGate;
            self.gate.unlock();
            for (&self.backends) |*backend_value| {
                var scope = try operation.enter(backend_value.*, null, null, .managed);
                defer scope.deinitChecked() catch @panic("cancel fixture reentry close failed");
                const input = try backend_value.fromFloat32(&.{ 2, 5 });
                defer backend_value.free(input);
                const doubled = try backend_value.add(input, input);
                defer backend_value.free(doubled);
                const values = try backend_value.toFloat32(doubled, std.testing.allocator);
                defer std.testing.allocator.free(values);
                try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, values);
                try scope.finish();
            }
        }
        const table = Session.VTable{ .run = run, .runResidentWithControl = forward, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    var gate: std.atomic.Mutex = .unlocked;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var probe = Probe{ .gate = &gate, .backends = .{ first.computeBackend(), second.computeBackend() } };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    probe.lease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
    defer if (probe.lease) |*lease| lease.release();
    var owner = try Sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var permit = RunPermit{ .session = .{ .ptr = &probe, .vtable = &Probe.table }, .lease = null };
    defer permit.deinitWithCleanup(.{ .boundary = &epoch });
    permit.output_retirement_shell = try @import("../execution/resource_retirement.zig").PreparedCharge.prepareForBackend(null, 1, 1);
    try permit.bindPhysicalCleanupEpoch(&epoch); // Raw compatibility still binds its actual current B.
    try std.testing.expect(permit.dispatch_cleanup_epoch.?.record == epoch.record);
    @import("antfly_platform").sync.lockYielding(&gate);
    var locked = true;
    defer if (locked) gate.unlock();
    try std.testing.expectError(error.Canceled, permit.runResidentWithControl(&.{}, a, .{ .ptr = &probe, .check_fn = Probe.check, .native_call_policy = .in_process }));
    try std.testing.expect(probe.lease == null and probe.callbacks == 0);
    try std.testing.expectEqual(@as(usize, 16), controller.snapshot().host_scratch_bytes);
    gate.unlock();
    locked = false;
    guard.finish();
    try std.testing.expectEqual(@as(usize, 1), probe.callbacks);
    if (probe.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}
