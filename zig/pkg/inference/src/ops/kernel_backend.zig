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

//! Target-owned descriptor execution and storage hooks. A backend installs
//! its own table as a unit; deriving other compute operations from a native
//! table never inherits native tensor casts into a device implementation.
const CT = @import("../graph/backend_contracts.zig").CT;
const kernel_descriptor = @import("kernel_descriptor.zig");
const physical_storage = @import("physical_storage.zig");
/// Optional complete project-payload proof, with a full-table veto before any
/// concrete context/tensor callback. Does not bound driver-private memory.
pub const OwnedPayloadServices = struct {
    accepts: *const fn (*const anyopaque) bool,
    available: *const fn (*anyopaque) bool,
    input: *const fn (*anyopaque, @import("../execution/kernel_route.zig").Binding, @import("ml").graph.Shape) anyerror!bool,
    route: *const fn (*anyopaque, *const @import("../execution/kernel_route.zig").Owned) bool,
};
pub const VTable = struct {
    /// Canonical full compute-table identity. Copying ordinary math callbacks
    /// does not silently inherit an equivalent native physical program.
    pub fn accepts(self: *const VTable, table: *const anyopaque) bool {
        if (self.implementation_table == table) return true;
        if (self.acceptsImplementation) |accept| return accept(table);
        return self.implementation_table == null;
    }
    implementation_table: ?*const anyopaque = null,
    ownedPayloadServices: ?*const OwnedPayloadServices = null,
    /// Known canonical ownership facades may change only lifetime callbacks.
    /// This allowlist never accepts arbitrary copies of a math table.
    acceptsImplementation: ?*const fn (*const anyopaque) bool = null,

    /// Dense F32 reshapes create only retained metadata views on this target.
    physicalReplayReshapeViews: bool = false,
    /// Synchronous, metadata-only F32 aliases. No payload conversion or allocation.
    createMetadataAlias: ?*const fn (*anyopaque, CT, *const @import("../execution/metadata_alias.zig").Request) anyerror!?CT = null,
    /// Cold metadata-only target selection. Dynamic cold/readiness cases return
    /// an owned zero-item dependency; null is statically unsupported.
    regionPolicyKey: ?*const fn (*anyopaque) anyerror!?u64 = null,
    /// Backend-owned, metadata-only operation proof. Unsupported/foreign or
    /// capture scopes return null; a frontend must never mint ordinary mode.
    claimRegionExecutionProfile: ?*const fn (*anyopaque, @import("../execution/region_execution_profile.zig").Request) anyerror!?@import("../execution/region_execution_profile.zig").Claim = null,
    prepareRegionRoutes: ?*const fn (*anyopaque, @import("../execution/region_route.zig").Prepare) anyerror!?@import("../execution/region_route.zig").OwnedList = null,
    /// Optional metadata-only registration of retained input storage owners.
    /// The caller holds this exact tensor/backend borrow for the whole scope.
    registerTensorOwnershipObservation: ?*const fn (*anyopaque, CT) void = null,
    /// Borrow the current frame's stable storage table. Entries may be filled
    /// during warmup; capture retains their leases before recording addresses.
    physicalExecutionScope: ?*const fn (ctx: *anyopaque, storages: ?[]const ?*physical_storage.Lease) anyerror!void = null,
    allocatePhysicalStorage: ?*const fn (ctx: *anyopaque, byte_size: usize) anyerror!?CT = null,
    /// Canonical admitted bank allocation: ownership metadata and conditional
    /// nonrecycling authority exist before any SDK buffer escapes.
    allocateAdmittedPhysicalStorage: ?*const fn (ctx: *anyopaque, byte_size: usize) anyerror!?CT = null,
    /// Metadata-only canonical-bank adoption. Conditional nonrecycling must be
    /// established before a token moves or any fallible SDK witness install.
    markPhysicalStorageAdmitted: ?*const fn (*anyopaque, CT) anyerror!void = null,
    /// Backing buffer span, including allocator reuse/rounding. Null is unknown.
    /// Metadata-only preflight upper bound for the owned SDK allocation span.
    physicalStorageReservationBytes: ?*const fn (ctx: *anyopaque, byte_size: usize) anyerror!usize = null,
    /// Authoritative cold backing/plane ownership. Visitors allocate nothing
    /// in the provider; aliases resolve full canonical owners and exact quota.
    storageOwnership: ?*const fn (ctx: *anyopaque, tensor: CT, visitor: @import("../execution/storage_ownership.zig").Visitor) anyerror!@import("../execution/storage_ownership.zig").Coverage = null,
    /// Compatibility for external/custom implementations. Normal backends use
    /// storageOwnership; this field does not authorize quota or alias credit.
    physicalStorageBytes: ?*const fn (ctx: *anyopaque, tensor: CT) ?usize = null,
    physicalTensorView: ?*const fn (ctx: *anyopaque, request: physical_storage.View) anyerror!?CT = null,
    /// Optional private-frame metadata reuse. The view must remain semantically
    /// immutable and uniquely referenced; actual external references still block
    /// warm credit even when they share this same CT header/Lease pin.
    hostInputViewExclusive: ?*const fn (*anyopaque, CT) bool = null,
    hostInputRequirements: ?*const fn (*anyopaque, @import("ml").graph.Shape) anyerror!?@import("../execution/host_ingress.zig").Requirements = null,
    createHostInput: ?*const fn (*anyopaque, @import("../execution/host_ingress.zig").Create) anyerror!?@import("../execution/host_ingress.zig").Upload = null,
    copyPhysicalInput: ?*const fn (ctx: *anyopaque, destination: CT, source: CT) anyerror!void = null,
    createPhysicalReplay: ?*const fn (ctx: *anyopaque) anyerror!?physical_storage.Replay = null,
    physicalReplayTensorSupported: ?*const fn (ctx: *anyopaque, tensor: CT) bool = null,
    physicalReplayKernelSupported: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) bool = null,
    /// Static backend legality. A true result still requires tensor/layout
    /// binding validation before dispatch; it is not a replacement proof.
    assignedKernelSupported: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) bool = null,
    /// Cold output-layout promise for an otherwise inferred metadata view.
    /// True means this exact operation materializes a dense result on this
    /// implementation. It does not establish input readiness or alias legality.
    materializesOutput: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) bool = null,
    assignedKernelAuxiliaries: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) anyerror!kernel_descriptor.AuxiliaryRequirements = null,
    assignedKernelWorkspace: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) anyerror!kernel_descriptor.WorkspaceRequirement = null,
    prepareKernelRoute: ?*const fn (*anyopaque, @import("../execution/kernel_route.zig").Prepare) anyerror!?@import("../execution/kernel_route.zig").Owned = null,
    runAssignedKernel: ?*const fn (ctx: *anyopaque, request: kernel_descriptor.Invocation) anyerror!bool = null,
    createPhysicalCompletion: ?*const fn (ctx: *anyopaque) anyerror!physical_storage.Completion = null,
    readFloat32Into: ?*const fn (ctx: *anyopaque, tensor: CT, output: []f32) anyerror!bool = null,
    /// Metadata-only gate for borrowing shared literals in this invocation.
    /// Metal declines foreign active frames whose quota lifetime is unowned.
    physicalLiteralBindingSupported: ?*const fn (ctx: *anyopaque) bool = null,
    writePhysicalFloat32: ?*const fn (ctx: *anyopaque, output: CT, input: []const f32) anyerror!void = null,
    retainImmutableTensor: ?*const fn (ctx: *anyopaque, tensor: CT) anyerror!?CT = null,
};
