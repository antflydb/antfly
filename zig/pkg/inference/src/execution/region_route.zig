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

//! A frozen numerical region can own several graph nodes and output slots.
//! Its schedule and resources are selected before quota is credited. Metadata
//! about a possible fusion is not a resource declaration for that fusion.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const kernel = @import("kernel_route.zig");
const descriptor = @import("../ops/kernel_descriptor.zig");
const NodeId = ml.graph.NodeId;

pub const Opaque = packed struct(u8) {
    outputs: bool = false,
    dispatch_workspace: bool = false,
    preparation_workspace: bool = false,
    constant_uploads: bool = false,
    host_metadata: bool = false,
    dynamic_alternative: bool = false,
    policy: bool = false,
    reserved: bool = false,

    pub fn complete(self: Opaque) bool {
        return @as(u8, @bitCast(self)) == 0;
    }
};

pub const Output = struct {
    node: NodeId,
    logical: ml.graph.Shape,
    /// Aliases are published logical views of another output. They do not
    /// grant another writable range or another allocation's capacity.
    alias_of: ?NodeId = null,
    bytes: usize,
    alignment: u32 = @alignOf(f32),
};

pub const Requirements = struct {
    outputs: []const Output,
    payload_claim_required: bool = false,
    /// These phases are disjoint: a planner may share their backing only
    /// after proving the cold phase finishes before numerical dispatch.
    dispatch_workspace: descriptor.WorkspaceRequirement = .{},
    preparation_workspace: descriptor.WorkspaceRequirement = .{},
    host_alignment: u32 = 1,
    preparation_host_alignment: u32 = 1,
    /// Immutable uploads must be bound by the variant's literal owner. A
    /// logical byte sum cannot prove a cold constant is already resident.
    literals: []const NodeId = &.{},
    uncovered: Opaque,

    /// Each phase must declare valid alignment independently. Taking a
    /// maximum can hide an invalid boundary behind its valid peer.
    pub fn validWorkspace(self: Requirements) bool {
        for ([_]descriptor.WorkspaceRequirement{ self.dispatch_workspace, self.preparation_workspace }) |workspace| {
            if (workspace.alignment == 0 or !std.math.isPowerOfTwo(workspace.alignment)) return false;
        }
        return @max(self.dispatch_workspace.byte_size, self.preparation_workspace.byte_size) % @sizeOf(f32) == 0 and self.host_alignment != 0 and self.preparation_host_alignment != 0 and std.math.isPowerOfTwo(self.host_alignment) and std.math.isPowerOfTwo(self.preparation_host_alignment);
    }

    pub fn replacementEligible(self: Requirements) bool {
        if (!self.uncovered.complete() or self.outputs.len == 0 or !self.validWorkspace()) return false;
        for (self.outputs, 0..) |output, index| {
            const elements = output.logical.numElements() orelse return false;
            if (elements <= 0) return false;
            const logical_bytes = std.math.mul(usize, @intCast(elements), output.logical.dtype.byteSize()) catch return false;
            if (output.bytes < logical_bytes or output.alignment == 0 or !std.math.isPowerOfTwo(output.alignment)) return false;
            if (output.alias_of) |parent| {
                var found = false;
                for (self.outputs[0..index]) |prior| if (prior.node == parent and prior.alias_of == null and output.bytes <= prior.bytes) {
                    found = true;
                    break;
                };
                if (!found) return false;
            }
        }
        return true;
    }
};

pub const Validate = struct {
    implementation: kernel.Implementation,
    /// Full backend schedule/precision/environment epoch, not a geometry hash.
    policy_key: u64,
    inputs: []const kernel.Binding,
    outputs: []const kernel.OutputSlot,
    /// Present only after pre-admission stabilization. Cache availability may
    /// change while this invocation's exact pinned payload remains usable.
    payload_claim: ?*const PayloadClaim = null,
};
pub const Assigned = struct {
    tensor: ops.CT,
    authority: ?kernel.AssignedOutput,
};
pub const Invocation = struct {
    validation: Validate,
    inputs: []const ops.CT,
    outputs: []const Assigned,
    workspace: ?ops.CT = null,
    host_workspace: []u8 = &.{},
    prepared_resources: ?*const AssignedResources = null,
};

pub const AssignedResources = struct {
    context: *anyopaque,
    key: u64,
    validate: *const fn (*anyopaque, Invocation) anyerror!void,
    deinit: *const fn (*anyopaque) void,
};
pub const Outcome = enum { declined_before_submission, completed };

pub const Owned = struct {
    context: *anyopaque,
    implementation: kernel.Implementation,
    key: u64,
    policy_key: u64,
    /// Exact consumed nodes and external output publications. Counts cannot
    /// substitute for this ownership boundary in a replacement proof.
    covered_nodes: []const NodeId,
    input_nodes: []const NodeId,
    entry_node: NodeId,
    requirements: Requirements,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Metadata-only. Includes input layout/storage/version and all static
        /// backend profile facts that selected the original tuned schedule.
        matches: *const fn (*anyopaque, Validate) bool,
        validate: *const fn (*anyopaque, Validate) anyerror!void,
        /// Called only after admission and assigned Lease-backed ranges exist.
        /// Cold setup may use only the declared preparation/literal resources.
        prepare_assigned: ?*const fn (*anyopaque, Invocation) anyerror!?AssignedResources = null,
        /// Metadata/refcount work only. No ensure/upload/transform/probe before
        /// admission. A null required pin requires fresh preparation/fallback
        /// before any quota credit, not hidden work during dispatch.
        claim_before_admission: ?*const fn (*anyopaque, ClaimRequest) anyerror!?PayloadClaim = null,
        /// Runs the same tuned implementation selected by this owner. An error
        /// is committed/uncertain and always propagates; it never means decline.
        dispatch: *const fn (*anyopaque, Invocation) anyerror!Outcome,
        deinit: *const fn (*anyopaque) void,
    };

    pub fn matches(self: *const Owned, request: Validate) bool {
        return self.implementation.context == request.implementation.context and self.implementation.table == request.implementation.table and self.policy_key == request.policy_key and self.vtable.matches(self.context, request);
    }
    pub fn validate(self: *const Owned, request: Validate) !void {
        if (!self.matches(request) or request.inputs.len != self.input_nodes.len) return error.PreparedRegionBindingChanged;
        if (request.outputs.len != self.requirements.outputs.len) return error.PreparedRegionOutputMismatch;
        for (request.outputs, self.requirements.outputs) |slot, output| {
            if (output.alignment == 0 or !std.math.isPowerOfTwo(output.alignment)) return error.PreparedRegionOutputMismatch;
            const elements = output.logical.numElements() orelse return error.PreparedRegionOutputMismatch;
            if (elements <= 0 or output.bytes < try std.math.mul(usize, @intCast(elements), output.logical.dtype.byteSize())) return error.PreparedRegionOutputMismatch;
            if (slot.node != output.node or slot.logical.dtype != output.logical.dtype or slot.logical.rank() != output.logical.rank() or !std.mem.eql(i64, slot.logical.dims[0..slot.logical.rank()], output.logical.dims[0..output.logical.rank()])) return error.PreparedRegionOutputMismatch;
            if (output.alias_of != null) {
                if (slot.writable_bytes != 0) return error.PreparedRegionOutputMismatch;
            } else if (slot.writable_bytes < output.bytes) return error.PreparedRegionOutputMismatch;
        }
        if (request.payload_claim) |claim| {
            if (claim.key != self.key) return error.PreparedRegionOwnerChanged;
            try claim.validate(request);
        }
        try self.vtable.validate(self.context, request);
    }
    pub fn claimBeforeAdmission(self: *const Owned, request: ClaimRequest) !?PayloadClaim {
        // Losing readiness before quota is a normal request to prepare the
        // explicit compatibility variant. Once pinned, validation errors are
        // hard errors and may never authorize numerical fallback.
        self.validate(request.validation) catch |err| switch (err) {
            error.PreparedRegionBindingChanged => return null,
            else => return err,
        };
        const acquire = self.vtable.claim_before_admission orelse {
            if (self.requirements.payload_claim_required) return error.PreparedRegionClaimUnavailable;
            return null;
        };
        var claim = (try acquire(self.context, request)) orelse return null;
        errdefer claim.release();
        var validation = request.validation;
        validation.payload_claim = &claim;
        try self.validate(validation);
        return claim;
    }
    pub fn prepareAssigned(self: *const Owned, request: Invocation) !?AssignedResources {
        try self.validate(request.validation);
        try self.validateAssigned(request);
        const resources = if (self.vtable.prepare_assigned) |prepare| try prepare(self.context, request) else return null;
        if (resources) |owner| if (owner.key != self.key) {
            owner.deinit(owner.context);
            return error.PreparedRegionOwnerChanged;
        };
        return resources;
    }
    pub fn dispatch(self: *const Owned, request: Invocation) !Outcome {
        try self.validate(request.validation);
        try self.validateAssigned(request);
        if (request.prepared_resources) |resources| try resources.validate(resources.context, request);
        const outcome = try self.vtable.dispatch(self.context, request);
        if (outcome == .declined_before_submission and self.requirements.replacementEligible()) return error.PreparedRegionDeclined;
        return outcome;
    }
    fn validateAssigned(self: *const Owned, request: Invocation) !void {
        if (self.requirements.payload_claim_required and request.validation.payload_claim == null) return error.PreparedRegionClaimRequired;
        if (!self.requirements.validWorkspace()) return error.PreparedRegionWorkspaceMismatch;
        const host_bytes = @max(self.requirements.dispatch_workspace.host_byte_size, self.requirements.preparation_workspace.host_byte_size);
        const host_alignment = @max(self.requirements.host_alignment, self.requirements.preparation_host_alignment);
        if (host_alignment == 0 or !std.math.isPowerOfTwo(host_alignment) or request.host_workspace.len < host_bytes or (host_bytes != 0 and @intFromPtr(request.host_workspace.ptr) % host_alignment != 0)) return error.PreparedRegionWorkspaceMismatch;
        if (@max(self.requirements.dispatch_workspace.byte_size, self.requirements.preparation_workspace.byte_size) != 0 and request.workspace == null) return error.PreparedRegionWorkspaceMismatch;
        if (request.inputs.len != self.input_nodes.len or request.outputs.len != self.requirements.outputs.len) return error.PreparedRegionOutputMismatch;
        try validateDistinctOutputs(request.outputs);
        for (request.outputs, request.validation.outputs, self.requirements.outputs) |assigned, slot, output| {
            if (output.alias_of != null) {
                if (assigned.authority != null) return error.PreparedRegionOutputMismatch;
                continue;
            }
            const authority = assigned.authority orelse return error.PreparedRegionOutputMismatch;
            if (authority.slot.node != slot.node or authority.slot.allocation != slot.allocation or authority.slot.epoch != slot.epoch or authority.slot.writable_bytes != slot.writable_bytes or authority.region.epoch != slot.epoch or authority.region.byte_size != slot.writable_bytes or authority.region.byte_offset % output.alignment != 0) return error.PreparedRegionOutputMismatch;
        }
    }
    pub fn deinit(self: *Owned) void {
        self.vtable.deinit(self.context);
        self.* = undefined;
    }
};

/// Cold target-owned selection over the exact optimized partition schedule.
/// All views are borrowed for this call. Owners copy the semantic/profile facts
/// and retain immutable source capabilities they need beyond preparation.
pub const Prepare = struct {
    allocator: std.mem.Allocator,
    implementation: kernel.Implementation,
    policy_key: u64,
    execution_profile: ExecutionProfile,
    graph: *const ml.graph.Graph,
    reachable: []const bool,
    /// Actual whole-program submission order, not graph ID ordering.
    node_order: []const NodeId,
    last_use: []const u32,
    partition_index: u32,
    partition_nodes: []const NodeId,
    /// Indexed by graph NodeId. Unknown/unbacked planned representations never
    /// establish a cached immutable source or cold-upload credit.
    node_facts: []const kernel.Binding,
};

/// One allocator-owned list with exactly one retirement path. Each context
/// owns copied matching metadata and any retained backend/source resources.
pub const OwnedList = struct {
    allocator: std.mem.Allocator,
    items: []Owned,
    /// Required for a dynamically cold/declined zero-item result. A null list
    /// means static unsupported, not a reusable proof of current readiness.
    dependency: ?SelectionDependency = null,

    pub fn deinit(self: *OwnedList) void {
        for (self.items) |*owner| owner.deinit();
        self.allocator.free(self.items);
        if (self.dependency) |dependency| dependency.vtable.deinit(dependency.context);
        self.* = undefined;
    }
};

fn validateDistinctOutputs(outputs: []const Assigned) !void {
    for (outputs, 0..) |output, index| {
        const current = output.authority orelse continue;
        const end = try std.math.add(usize, current.region.byte_offset, current.region.byte_size);
        for (outputs[0..index]) |previous| {
            const prior = previous.authority orelse continue;
            if (!std.meta.eql(current.region.backing, prior.region.backing)) continue;
            const prior_end = try std.math.add(usize, prior.region.byte_offset, prior.region.byte_size);
            if (current.region.byte_offset < prior_end and prior.region.byte_offset < end) return error.OverlappingRegionOutputs;
        }
    }
}

/// A metadata-only readiness dependency, including the zero-region cold case.
/// The target copies source/profile identities and watches only relevant slot
/// generations. It never stabilizes a payload or authorizes quota by itself.
pub const SelectionDependency = struct {
    context: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        matches: *const fn (*anyopaque) bool,
        validate: *const fn (*anyopaque) anyerror!void,
        deinit: *const fn (*anyopaque) void,
    };
};

/// Independently retained release capability. Completion tickets and captures
/// retain their own reference before a submitting Frame drops its claim.
pub const ReleaseCapability = struct {
    context: *anyopaque,
    retain_fn: *const fn (*anyopaque) void,
    release_fn: *const fn (*anyopaque) void,
    pub fn retain(self: ReleaseCapability) ReleaseCapability {
        self.retain_fn(self.context);
        return self;
    }
    pub fn release(self: ReleaseCapability) void {
        self.release_fn(self.context);
    }
};
pub const Submission = struct {
    implementation: kernel.Implementation,
    outcome: enum { completed_or_enqueued, declined_before_submission, failed_uncertain },
    /// Only a target-owned capture implementation can consume this capability.
    /// The initial Metal bridge declares capture unsupported.
    capture_owner: ?*anyopaque = null,
    release: ReleaseCapability,
};
pub const ClaimRequest = struct { allocator: std.mem.Allocator, validation: Validate };

/// Move-only pre-admission pin on actual existing payload. It grants no memory
/// credit by itself. Metadata selection/cache readiness and payload lifetime
/// are separate: queued eviction may invalidate future preparation while this
/// admitted claim's exact profile remains valid until its completion owner.
pub const PayloadClaim = struct {
    resource: ReleaseCapability,
    key: u64,
    held: bool = true,
    vtable: *const VTable,
    pub const VTable = struct {
        validate: *const fn (*anyopaque, Validate) anyerror!void,
        /// Success independently retains the release capability in a real
        /// command completion/capture owner. Failure leaves this claim intact.
        transfer_to_submission: *const fn (*anyopaque, Submission) anyerror!void,
        /// Explicit terminal completion/drain proof for uncertain submissions.
        /// A fence merely recorded or a GPU dependency enqueued is not proof.
        drain: *const fn (*anyopaque) anyerror!void,
    };
    pub fn validate(self: *const PayloadClaim, request: Validate) !void {
        if (!self.held) return error.PreparedRegionClaimConsumed;
        try self.vtable.validate(self.resource.context, request);
    }
    pub fn release(self: *PayloadClaim) void {
        if (!self.held) return;
        self.held = false;
        self.resource.release();
    }
    pub fn transferToSubmission(self: *PayloadClaim, implementation: kernel.Implementation, outcome: @FieldType(Submission, "outcome"), capture_owner: ?*anyopaque) !void {
        if (!self.held) return error.PreparedRegionClaimConsumed;
        try self.vtable.transfer_to_submission(self.resource.context, .{ .implementation = implementation, .outcome = outcome, .capture_owner = capture_owner, .release = self.resource });
        self.release();
    }
    pub fn drain(self: *const PayloadClaim) !void {
        if (self.held) try self.vtable.drain(self.resource.context);
    }
};

/// Admission-safe recovery from a disappeared warm payload. The compatibility
/// variant is prepared before quota, never selected during numerical dispatch.
pub const SelectionPolicy = enum { select_regions, compatibility_regions };

/// Supplied by the invocation owner, never inferred from a previous workload's
/// mutable provider state. Unknown keeps ordinary compatibility execution.
pub const ExecutionProfile = enum {
    unknown,
    ordinary_frame,
    /// Explicit backend operation with terminal synchronous native commands.
    /// A live Claim still proves exact graph/provider/nonce authority; this
    /// value alone never permits borrowing or disabling an ambient frame.
    synchronous,
    capture,
};

test "region phase workspace contracts cannot hide malformed requirements behind a shared maximum" {
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const outputs = [_]Output{.{ .node = 0, .logical = shape, .bytes = 16 }};
    const original = Requirements{ .outputs = &outputs, .dispatch_workspace = .{ .byte_size = 64, .alignment = 16 }, .preparation_workspace = .{ .byte_size = 32, .alignment = 8 }, .host_alignment = 16, .preparation_host_alignment = 8, .uncovered = .{} };
    try std.testing.expect(original.replacementEligible());
    inline for (.{ "dispatch_workspace", "preparation_workspace" }) |phase| {
        var malformed = original;
        // The valid peer's larger alignment used to mask this fault.
        @field(malformed, phase).alignment = 3;
        try std.testing.expect(!malformed.replacementEligible());
    }
    inline for (.{ "host_alignment", "preparation_host_alignment" }) |phase| {
        var malformed = original;
        @field(malformed, phase) = 3;
        try std.testing.expect(!malformed.replacementEligible());
    }
}

test "region malformed phase workspace refuses actual cold preparation and dispatch before provider callbacks" {
    const Probe = struct {
        preparations: usize = 0,
        dispatches: usize = 0,
        fn matches(_: *anyopaque, _: Validate) bool {
            return true;
        }
        fn validate(_: *anyopaque, _: Validate) !void {}
        fn prepare(raw: *anyopaque, _: Invocation) !?AssignedResources {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.preparations += 1;
            return null;
        }
        fn dispatch(raw: *anyopaque, _: Invocation) !Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.dispatches += 1;
            return .completed;
        }
        fn deinit(_: *anyopaque) void {}
        const table = Owned.VTable{ .matches = matches, .validate = validate, .prepare_assigned = prepare, .dispatch = dispatch, .deinit = deinit };
    };
    var probe = Probe{};
    // No tensor access is necessary to exercise the actual resource boundary.
    // The valid zero-scratch control must reach both real provider callbacks.
    const implementation = kernel.Implementation{ .context = &probe, .table = &Probe.table };
    var owner = Owned{ .context = &probe, .implementation = implementation, .key = 7, .policy_key = 11, .covered_nodes = &.{}, .input_nodes = &.{}, .entry_node = 0, .requirements = .{ .outputs = &.{}, .dispatch_workspace = .{ .alignment = 16 }, .preparation_workspace = .{ .alignment = 8 }, .host_alignment = 16, .preparation_host_alignment = 8, .uncovered = .{} }, .vtable = &Probe.table };
    const original = owner.requirements;
    const request = Invocation{ .validation = .{ .implementation = implementation, .policy_key = 11, .inputs = &.{}, .outputs = &.{} }, .inputs = &.{}, .outputs = &.{} };
    try std.testing.expect((try owner.prepareAssigned(request)) == null);
    try std.testing.expectEqual(Outcome.completed, try owner.dispatch(request));
    inline for (.{ "dispatch_workspace", "preparation_workspace" }) |phase| {
        owner.requirements = original;
        @field(owner.requirements, phase).alignment = 3;
        try std.testing.expectError(error.PreparedRegionWorkspaceMismatch, owner.prepareAssigned(request));
        try std.testing.expectError(error.PreparedRegionWorkspaceMismatch, owner.dispatch(request));
    }
    inline for (.{ "host_alignment", "preparation_host_alignment" }) |phase| {
        owner.requirements = original;
        @field(owner.requirements, phase) = 3;
        try std.testing.expectError(error.PreparedRegionWorkspaceMismatch, owner.prepareAssigned(request));
        try std.testing.expectError(error.PreparedRegionWorkspaceMismatch, owner.dispatch(request));
    }
    try std.testing.expectEqual(@as(usize, 1), probe.preparations);
    try std.testing.expectEqual(@as(usize, 1), probe.dispatches);
    owner.requirements = original;
    try std.testing.expect((try owner.prepareAssigned(request)) == null);
    try std.testing.expectEqual(Outcome.completed, try owner.dispatch(request));
    try std.testing.expectEqual(@as(usize, 2), probe.preparations);
    try std.testing.expectEqual(@as(usize, 2), probe.dispatches);
}
