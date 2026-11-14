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

//! Owned input-aware kernel schedules and their declared physical resources.
//! Metadata selection precedes allocation; address-dependent vendor preparation
//! uses assigned unpublished slots. Dispatch validates the selected program and
//! never discovers a different schedule after quota has been credited.
const std = @import("std");
const ml = @import("ml");
const contracts = @import("../graph/backend_contracts.zig");
const descriptor = @import("../ops/kernel_descriptor.zig");
const storage = @import("storage_ownership.zig");

pub const Implementation = struct {
    context: *anyopaque,
    table: *const anyopaque,
    /// Generic callers know provider identity but cannot mint backend policy
    /// epochs. A selected route owns/validates its actual concrete profile.
    policy_epoch: ?u64 = null,
};
pub const Binding = union(enum) {
    /// Borrowed immutable model tensor. The prepared invocation pins its
    /// Session/backend owner; the route may additionally own cached plane pins.
    tensor: struct { value: contracts.CT, implementation: Implementation },
    /// A predecessor with an exact selected physical representation. No future
    /// upload or inferred reduced mirror is invented from logical shape.
    planned: struct { logical: ml.graph.Shape, planes: []const storage.Plane },
    /// Explicit host admission entry, including metadata-only text signatures.
    /// Resident input shape metadata must never construct this variant.
    imported_host: struct {
        logical: ml.graph.Shape,
        target: contracts.BackendKind,
        /// Explicit complete host-ingress selection. Ordinary/mixed imports and
        /// already-resident stage controls must not reserve unused mutable slots.
        owned_ingress: bool = false,
        /// Actual synchronous caller storage, not a metadata-only signature.
        /// The Native variant may borrow only after proving all consumers and
        /// publication independent of this private, non-donatable input header.
        borrowable_host: bool = false,
    },
    /// Actual resident metadata obtained without allocation, transfer or GPU
    /// submission. Plane facts include canonical storage identity/version.
    resident: struct { logical: ml.graph.Shape, planes: []const storage.Plane, implementation: Implementation },
    unknown,
};

pub const InputBinding = struct { node_id: ml.graph.NodeId, value: Binding };

pub const OutputSlot = struct {
    /// IDs belong to the pinned prepared executable. The actual view retains
    /// its slot authority through its physical Lease after plan eviction.
    allocation: u32,
    node: ml.graph.NodeId,
    epoch: u64,
    logical: ml.graph.Shape,
    /// Slot-local writable capacity, never the underlying bank's spare tail.
    /// Zero during initial metadata selection grants no write permission; the
    /// planner fills the declared requirement before validation/assignment.
    writable_bytes: usize,
};

/// Borrowed dispatch capability. Assigned resources must copy these values
/// or retain their Lease-backed authority; the Invocation pointer never escapes.
pub const AssignedOutput = struct {
    slot: OutputSlot,
    region: storage.WritableRegion,
    input_generation: u64 = 0,
};

pub const AssignedInput = struct { node: ml.graph.NodeId, lease_identity: usize, authority: *const storage.AllocationSlotAuthority, generation: u64 };

pub const Requirements = struct {
    /// Exact primary allocation-slot extent including an approved padded row
    /// bucket. Logical count, shape, tensor length and readback stay unchanged.
    primary_bytes: usize,
    primary_alignment: u32 = @alignOf(f32),
    /// A backend-owned static physical precision/layout/schedule profile.
    profile: u32,
    workspace: descriptor.WorkspaceRequirement = .{},
    host_alignment: u32 = 1,
    /// Cold algorithm probes run before capture with owned assigned slots.
    /// Reuse normal scratch only when the lifetimes are provably disjoint;
    /// the plan reserves the maximum of these declared requirements.
    probe_workspace: descriptor.WorkspaceRequirement = .{},
    probe_host_alignment: u32 = 1,
    probe_output: enum { none, assigned_unpublished } = .none,
    auxiliaries: descriptor.AuxiliaryRequirements = .{},
    capture: enum { unsupported, supported } = .unsupported,
};

pub const Prepare = struct {
    /// Synchronous cold authority for optional SDK descriptor/algorithm
    /// qualification. Never retained in a route/profile/frame or inferred
    /// from an ambient operation; unknown/raw callers remain conservative.
    metadata_authority: ?@import("backend_operation.zig").Borrowed = null,
    host_readonly_inputs: [3]bool = @splat(false),
    allocator: std.mem.Allocator,
    /// Exact target callback-table identity; a generic caller cannot substitute
    /// the backend default table for a derived/wrapped implementation.
    implementation: ?Implementation = null,
    descriptor: *const descriptor.Descriptor,
    inputs: []const Binding,
    /// Cold authority supplied by Selection, not inferred from a planned tag.
    /// A witnessed predecessor is rooted in validated public facts or an
    /// immutable literal bank; mutable model-Tensor aliases remain unwitnessed.
    inputs_witnessed: [3]bool = @splat(false),
    output: OutputSlot,
};
pub const Validate = struct {
    inputs: []const Binding,
    output: OutputSlot,
};
pub const PrepareAssigned = struct {
    allocator: std.mem.Allocator,
    invocation: *const descriptor.Invocation,
    output: OutputSlot,
};
/// Per-frame resources may depend on actual assigned addresses/alignment.
/// They pin the selected vendor algorithm/handle across cache eviction and
/// captures, and remain separate from the immutable program/profile owner.
pub const Assigned = struct {
    context: *anyopaque,
    key: u64,
    validate: *const fn (*anyopaque, PrepareAssigned) anyerror!void,
    deinit: *const fn (*anyopaque) void,
};

pub const ValidationScope = enum {
    invocation,
    /// Pure prepared descriptor program. Operand/layout facts are fixed by
    /// the immutable plan and its separately validated public input bindings.
    /// No mutable backend policy, model Tensor header, or vendor state may
    /// participate in this promise. Default extensions remain invocation-scoped.
    prepared_program,
};

/// Layout of a materialized assigned output, independent of input values and
/// live numerical profile. This never certifies a model Tensor's own planes or
/// permits skipping the producer's invocation validation. Default propagation
/// retains input-dependent taint. Explicit providers guarantee dense primary
/// plus EVERY declared auxiliary, with no hidden output mirrors/aliases.
pub const OutputPlaneMetadata = enum { input_dependent, assigned_dense };

pub const Owned = struct {
    context: *anyopaque,
    implementation: Implementation,
    /// Complete static program/schedule key, including reduction order,
    /// physical plane/profile, slot region and implementation/storage epochs.
    key: u64,
    requirements: Requirements,
    validation_scope: ValidationScope = .invocation,
    output_plane_metadata: OutputPlaneMetadata = .input_dependent,
    vtable: *const VTable,

    pub const VTable = struct {
        host_input_constraint: ?*const fn (*anyopaque, usize) ?@import("host_ingress.zig").IndexBounds = null,
        /// Metadata only; run before admission when credit is requested, and
        /// again before any frame allocation/submission at dispatch. A mismatch
        /// requires preparation/admission; it must not silently reselect.
        validate: *const fn (*anyopaque, Validate) anyerror!void,
        /// Full metadata equivalence for bounded variant lookup. Key hashes
        /// only index candidates; they never establish identity or credit.
        /// No allocation, submission or vendor probing on this cache path.
        matches: *const fn (*anyopaque, Validate) bool,
        /// After admission and owned assigned slots exist, before capture or
        /// normal dispatch. Receives real views/addresses/alignment. Vendor
        /// numerical probes may use only declared probe scratch and permitted
        /// unpublished output ranges. No hidden staging, input mutation or
        /// program/profile reselect is allowed. Failure aborts this run.
        prepare_assigned: ?*const fn (*anyopaque, PrepareAssigned) anyerror!?Assigned = null,
        deinit: *const fn (*anyopaque) void,
    };
    pub fn validate(self: *const Owned, request: Validate) !void {
        return self.vtable.validate(self.context, request);
    }
    pub fn prepareAssigned(self: *const Owned, request: PrepareAssigned) !?Assigned {
        const prepare = self.vtable.prepare_assigned orelse return null;
        return try prepare(self.context, request);
    }
    pub fn deinit(self: *Owned) void {
        self.vtable.deinit(self.context);
        self.* = undefined;
    }
};
