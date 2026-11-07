// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pure metadata pins for exact, already-witnessed workspace charges. The
//! caller prepares these BEFORE acquiring any numerical owner. A pin cannot
//! certify an SDK root: only the accepted concrete root enumerator may bind it.
//! No model/provider/IO/arena or creation epoch is retained here.
const std = @import("std");
const retirement = @import("../execution/resource_retirement.zig");
const sink = @import("../execution/retirement_sink.zig");
const platform = @import("antfly_platform");
const metadata_allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());

/// One actual SDK root, not a model or fungible cache-byte estimate. The
/// creator/cache pin and every independently owning DeviceBufferRef share
/// this exact root. A snapshot retains only metadata, never these physical
/// references, so it cannot replace the final caller's current cleanup B.
pub const Root = struct {
    backing: @import("../execution/backing_retirement.zig").BackingOwner,
    identity: u64,
    admission_identity: @import("../execution/storage_admission.zig").Identity,
    logical_references: std.atomic.Value(usize) = .init(1),
    creator_live: std.atomic.Value(bool) = .init(true),
    next_cached: ?*Root = null,
    listed: bool = false,
    creator_identity: u64 = 0,
    endpoint: ?*@import("metal_runtime_lifetime.zig").Lifetime = null,
    close_fence: ?*@import("metal_workspace_fence.zig").Fence = null,

    pub fn init(self: *Root, charge: *retirement.Charge, metadata: @import("../execution/backing_retirement.zig").BackingOwner.Metadata) void {
        self.backing.init(charge, metadata);
        self.identity = @import("../execution/source_identity.zig").next();
        self.admission_identity = (charge.storageReservation() orelse @panic("workspace root requires physical reservation")).identity;
        self.logical_references = .init(1);
        self.creator_live = .init(true);
        self.next_cached = null;
        self.listed = false;
        self.creator_identity = 0;
        self.endpoint = null;
        self.close_fence = null;
    }
    /// Pins only endpoint metadata; no SDK/provider/Model lifetime is captured.
    pub fn bindEndpoint(self: *Root, endpoint: *@import("metal_runtime_lifetime.zig").Lifetime) void {
        std.debug.assert(self.endpoint == null);
        self.endpoint = endpoint.retain();
    }
    /// Called while a REAL owning SDK handle or creator/cache pin keeps the
    /// associated canonical Root alive. Closed roots cannot resurrect.
    pub fn retainPhysical(self: *Root) bool {
        var before = self.logical_references.load(.acquire);
        while (before != 0 and before != std.math.maxInt(usize)) {
            before = self.logical_references.cmpxchgWeak(before, before + 1, .acq_rel, .acquire) orelse return true;
        }
        return false;
    }
    /// Returning an independently held old root to the CURRENT typed reuse
    /// pool acquires a real cache pin before transferring its SDK retain.
    pub fn ensureCreator(self: *Root) !bool {
        if (self.creator_live.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return false;
        if (!self.retainPhysical()) {
            self.creator_live.store(false, .release);
            return error.ClosedWorkspaceRoot;
        }
        return true;
    }
    pub fn releaseCreator(self: *Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.creator_live.cmpxchgStrong(true, false, .acq_rel, .acquire) != null) return error.WorkspaceCreatorAlreadyReleased;
        self.releasePhysical(current) catch |err| {
            self.creator_live.store(true, .release);
            return err;
        };
    }
    pub fn validatePhysicalRelease(self: *const Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        const count = self.logical_references.load(.acquire);
        if (count == 0) return error.ClosedWorkspaceRoot;
        if (count != 1) return;
        if (current.boundary) |epoch| {
            if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        } else if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
    }
    /// A physical snapshot pins metadata/quota only. It never lends one old
    /// physical owner's credit to another newly allocated SDK root.
    pub fn retainSnapshot(self: *Root) @import("../execution/backing_retirement.zig").BackingOwner.Metadata {
        return self.backing.retainMetadataPin();
    }
    /// Consume the canonical C census's already-retained pure pin. This
    /// constructs only a release value; it neither casts opaque SDK state nor
    /// acquires a second pin. Descriptor acceptance precedes this call.
    pub fn ownedSnapshotPin(self: *Root) @import("../execution/backing_retirement.zig").BackingOwner.Metadata {
        return .{ .context = self, .release = releaseOwnedSnapshotPin };
    }
    fn releaseOwnedSnapshotPin(raw: *anyopaque) void {
        const self: *Root = @ptrCast(@alignCast(raw));
        self.backing.releaseMetadataPinOutsideExecution();
    }
    /// Pre-SDK validation and decrement. Only the ACTUAL last physical owner
    /// registers its current B. The associated SDK witness separately waits
    /// for driver/command-buffer retains and never invokes quota callbacks.
    pub fn releasePhysical(self: *Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        var before = self.logical_references.load(.acquire);
        while (true) {
            if (before == 0) return error.ClosedWorkspaceRoot;
            if (before == 1) {
                if (current.boundary) |epoch| {
                    if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
                } else if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
            }
            before = self.logical_references.cmpxchgWeak(before, before - 1, .acq_rel, .acquire) orelse break;
        }
        if (before != 1) return;
        if (current.boundary) |epoch| self.backing.release(epoch) else self.backing.releaseOutsideExecution();
        // Registration can synchronously retire metadata. No self access.
    }
};

/// Intrusive creator custody for ONE actual runtime, serialized by its real
/// provider owner. Existing Root metadata supplies the links; no map, hot
/// allocation, extra quota, SDK retention or remembered cleanup epoch.
/// The caller includes exact roots parked in that runtime's explicit custody
/// banks in `parked`; empty runtime fields alone NEVER retire those creators.
pub const Creators = struct {
    identity: u64 = 0,
    head: ?*Root = null,
    endpoint: ?*@import("metal_runtime_lifetime.zig").Lifetime = null,

    pub fn init(endpoint: *@import("metal_runtime_lifetime.zig").Lifetime) Creators {
        return .{ .identity = @import("../execution/source_identity.zig").next(), .endpoint = endpoint };
    }
    fn validateOrigin(self: *const Creators, root: *const Root) !void {
        const endpoint = self.endpoint orelse return error.MissingWorkspaceEndpoint;
        if (root.endpoint != endpoint) return error.ForeignWorkspaceEndpoint;
        if (self.identity == 0 or (root.creator_identity != 0 and root.creator_identity != self.identity))
            return error.ForeignWorkspaceCreator;
        if (root.logical_references.load(.acquire) == 0) return error.ClosedWorkspaceRoot;
    }
    /// The caller holds the actual SDK buffer/physical Root while adopting.
    /// A different runtime's endpoint, copied header or reuse mode is no proof.
    pub fn adopt(self: *Creators, root: *Root) !void {
        try self.validateOrigin(root);
        if (root.creator_identity == self.identity) return;
        _ = try root.ensureCreator();
        root.next_cached = self.head;
        root.creator_identity = self.identity;
        root.listed = true;
        self.head = root;
    }
    /// The unique provider close path MUST detach all creators with NOW's
    /// explicit cleanup before freeing provider storage/closing its endpoint.
    /// Root keeps only this immutable numeric identity, NEVER a provider/list
    /// pointer; late SDK/use Nodes own pure metadata after this transaction.
    /// Caller invokes this BEFORE irreversible endpoint close, and detach
    /// repeats it under the SAME serialized creator owner/current B.
    pub fn preflightDetachForProviderClose(self: *const Creators, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (current.boundary) |epoch| {
            if (current.outside_execution or try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        } else if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
        var cursor = self.head;
        while (cursor) |root| : (cursor = root.next_cached) {
            try self.validateOrigin(root);
            if (!root.listed or root.creator_identity != self.identity or !root.creator_live.load(.acquire)) return error.ForeignWorkspaceCreator;
            if (current.boundary != null) {
                const close = root.close_fence orelse return error.MissingWorkspaceCloseFence;
                var prior = self.head;
                while (prior) |other| : (prior = other.next_cached) {
                    if (other == root) break;
                    if (other.close_fence == close) return error.DuplicateWorkspaceCloseFence;
                }
                try close.preflightClosingRetirement(&root.backing, current);
            } else if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
        }
    }
    pub fn detachForProviderClose(self: *Creators, current: @import("../execution/cleanup_context.zig").Context) !void {
        // Caller already proved SDK quiescence, all prepared-slot retirements,
        // and exclusive endpoint closure. No SDK failure may be replayed after
        // actual close cells are armed. Outside-only compatibility has no B.
        try self.preflightDetachForProviderClose(current);
        if (current.boundary != null) {
            var cursor = self.head;
            while (cursor) |root| : (cursor = root.next_cached) {
                const close = root.close_fence.?;
                // All ordinary failures were preflighted above. Actual owner
                // serialization keeps the private cells/creators unchanged;
                // no recoverable partial close or SDK phase follows arming.
                close.bindClosingBeforeRetirement(&root.backing) catch platform.inference_process_supervisor.restartWorker();
                close.retire(current) catch platform.inference_process_supervisor.restartWorker();
            }
        }
        try self.reconcile(&.{}, &.{}, current);
        self.endpoint = null;
    }
    pub fn releaseOne(self: *Creators, root: *Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        try self.validateOrigin(root);
        if (root.creator_identity != self.identity or !root.listed) return error.ForeignWorkspaceCreator;
        if (current.boundary) |epoch| {
            if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        } else if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
        var link = &self.head;
        while (link.*) |entry| {
            if (entry != root) {
                link = &entry.next_cached;
                continue;
            }
            link.* = root.next_cached;
            root.next_cached = null;
            root.creator_identity = 0;
            root.listed = false;
            try root.releaseCreator(current);
            return; // may destroy Root: never read after release
        }
        return error.ForeignWorkspaceCreator;
    }
    fn contains(roots: []const *Root, target: *const Root) bool {
        for (roots) |root| if (root == target) return true;
        return false;
    }
    /// Actual idle provider census, after checked submission completion and
    /// BEFORE SDK fields are destroyed/replaced. All incoming pointers were
    /// certified by the canonical RootOperations descriptor before casting.
    /// Current B is supplied by this transaction, never saved on the list.
    pub fn reconcile(self: *Creators, live: []const *Root, parked: []const *Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        // Validate the COMPLETE transaction before changing creator custody.
        for (live) |root| try self.validateOrigin(root);
        for (parked) |root| try self.validateOrigin(root);
        if (current.boundary) |epoch| {
            if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        } else if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
        for (live) |root| try self.adopt(root);
        for (parked) |root| try self.adopt(root);
        var link = &self.head;
        while (link.*) |root| {
            if (contains(live, root) or contains(parked, root)) {
                link = &root.next_cached;
                continue;
            }
            // Unlink BEFORE a last-reference release can destroy Root metadata.
            link.* = root.next_cached;
            root.next_cached = null;
            root.creator_identity = 0;
            root.listed = false;
            try root.releaseCreator(current);
            // Root may now be destroyed: do not read it after release.
        }
    }
};

test "workspace root snapshot cannot substitute for actual final view cleanup boundary" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    const Case = struct {
        destroyed: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
    };
    var metadata = Case{};
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var reservation: ?storage.Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ try controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 64 }, .{ .backend_scratch_bytes = 64 }, false), null },
        .domain = .backend,
    };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var root: Root = undefined;
    root.init(charge, .{ .context = &metadata, .release = Case.release });
    // Exact metadata custodians for the actual SDK witness and outside snapshot.
    const sdk = root.backing.retainMetadataPin();
    const snapshot = root.retainSnapshot();
    try std.testing.expect(root.retainPhysical()); // actual independently owning view
    var reference = try sink.Ref.create();
    defer reference.release();
    var old = try reference.enter();
    var old_epoch = try old.retainEpoch();
    defer old_epoch.release();
    try root.releasePhysical(.{ .boundary = &old_epoch }); // old cache pin only
    old.finish();
    try std.testing.expectEqual(@as(usize, 1), root.logical_references.load(.acquire));
    try std.testing.expectError(error.MissingWorkspaceCleanupEpoch, root.releasePhysical(.{}));
    try std.testing.expectError(error.SealedWorkspaceCleanupEpoch, root.releasePhysical(.{ .boundary = &old_epoch }));
    try std.testing.expectEqual(@as(usize, 1), root.logical_references.load(.acquire));
    var current = try reference.enter();
    defer current.finish();
    var epoch = try current.retainEpoch();
    defer epoch.release();
    try root.releasePhysical(.{ .boundary = &epoch });
    try std.testing.expect(!root.retainPhysical()); // no closed-root resurrection
    snapshot.release(snapshot.context); // outside snapshot drops no SDK ownership
    sdk.release(sdk.context); // SDK witness death alone cannot substitute for B
    try std.testing.expectEqual(@as(usize, 0), metadata.destroyed);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    current.finish();
    try std.testing.expectEqual(@as(usize, 1), metadata.destroyed);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

pub const RootOperations = extern struct {
    retain: *const fn (*Root) callconv(.c) c_int,
    validate_release: *const fn (*Root, *const @import("../execution/cleanup_context.zig").Context) callconv(.c) c_int,
    release: *const fn (*Root, *const @import("../execution/cleanup_context.zig").Context) callconv(.c) c_int,
    begin_runtime_use: *const fn (*Root, *anyopaque) callconv(.c) ?*@import("metal_runtime_lifetime.zig").Lifetime,
    retain_metadata: *const fn (*Root) callconv(.c) void,
    pub const canonical: RootOperations = .{ .retain = retainRoot, .validate_release = validate, .release = releaseRoot, .begin_runtime_use = beginRuntimeUse, .retain_metadata = retainMetadata };
    fn retainMetadata(root: *Root) callconv(.c) void {
        // Transfer this exact pure pin to the census entry while its SDK
        // handle still keeps Root metadata alive under the source lock.
        _ = root.retainSnapshot();
    }
    fn beginRuntimeUse(root: *Root, runtime: *anyopaque) callconv(.c) ?*@import("metal_runtime_lifetime.zig").Lifetime {
        const endpoint = root.endpoint orelse return null;
        return if (endpoint.beginUseFor(@intFromPtr(runtime))) endpoint else null;
    }
    fn retainRoot(root: *Root) callconv(.c) c_int {
        return if (root.retainPhysical()) 0 else -1;
    }
    fn validate(root: *Root, current: *const @import("../execution/cleanup_context.zig").Context) callconv(.c) c_int {
        // Every charged SDK-reference release requires explicit authority, even
        // when another physical owner currently exists: that owner may retire
        // concurrently before the SDK release completes.
        if (current.boundary) |epoch| {
            if (epoch.isSealed() catch return -1) return -1;
            if (!root.backing.hasUseEpoch(epoch)) return -1;
        } else if (!current.outside_execution) return -1;
        root.validatePhysicalRelease(current.*) catch return -1;
        return 0;
    }
    fn releaseRoot(root: *Root, current: *const @import("../execution/cleanup_context.zig").Context) callconv(.c) c_int {
        if (validate(root, current) != 0) return -1;
        root.releasePhysical(current.*) catch return -1;
        return 0;
    }
};

/// One independent actual invocation/owner use bank. Capacity comes from the
/// complete concrete root census, NOT a byte envelope or32-only graph bound.
/// Cells are reusable after their own B drains; distinct concurrent foreign
/// scopes require distinct banks. No late callback captures this bank.
/// This is a private leaf-use capability, never generic free authority.
pub const UseBank = struct {
    const Fence = @import("metal_workspace_fence.zig").Fence;
    const Entry = struct { cell: *Fence, root: ?*Root = null, identity: u64 = 0 };
    allocator: std.mem.Allocator,
    entries: []Entry,
    count: usize = 0,
    epoch: ?sink.Epoch = null,

    pub fn requestedMetadataBytes(actual_capacity: usize) !usize {
        return std.math.mul(usize, actual_capacity, @sizeOf(Entry) + @sizeOf(Fence));
    }
    pub fn prepareExact(actual_capacity: usize) !UseBank {
        const entries = try metadata_allocator.alloc(Entry, actual_capacity);
        errdefer metadata_allocator.free(entries);
        var made: usize = 0;
        errdefer for (entries[0..made]) |entry| entry.cell.deinitOutsideExecution();
        for (entries) |*entry| {
            entry.* = .{ .cell = try Fence.prepare() };
            made += 1;
        }
        return .{ .allocator = metadata_allocator, .entries = entries };
    }
    /// All roots already have independent Snapshot metadata pins. This method
    /// runs OUTSIDE every numerical/provider/model lane, BEFORE reserve/math.
    /// A partial failure leaves exact owned cells for outside destruction;
    /// registered nodes remain independently alive until their actual B seals.
    pub fn bindOutsideExecution(self: *UseBank, roots: []const *Root, actual: *const sink.Epoch) !void {
        if (self.epoch != null or self.count != 0) return error.WorkspaceUseBankBusy;
        if (try actual.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        if (roots.len > self.entries.len) return error.UnpreparedWorkspaceUses;
        for (roots, 0..) |root, i| {
            for (roots[0..i]) |previous| if (root == previous) return error.DuplicateWorkspaceUse;
        }
        self.epoch = try actual.retain();
        for (roots, self.entries[0..roots.len]) |root, *entry| {
            try entry.cell.bindBackingOutsideExecution(&root.backing);
            // Own a bound cancellation cell even if registration fails.
            entry.root = root;
            entry.identity = root.identity;
            self.count += 1;
            try entry.cell.retire(.{ .boundary = actual });
        }
    }
    /// Explicit receiver-header leaf capability only. The receiving wrapper
    /// first validates its actual operation and SAME NOWB at bind. A generic
    /// free, an empty Context or a nested foreign B receives no fallback here.
    pub fn validateRoot(self: *const UseBank, root: *const Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        const actual = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        const held = self.epoch orelse return error.UnpreparedWorkspaceUses;
        if (current.outside_execution or actual.record != held.record or actual.nonce != held.nonce)
            return error.ForeignWorkspaceUse;
        if (try actual.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        for (self.entries[0..self.count]) |entry| if (entry.root == root) {
            if (entry.identity != root.identity or !entry.cell.validatesUse(&root.backing, actual))
                return error.ForeignWorkspaceUse;
            return;
        };
        return error.UnpreparedWorkspaceUses;
    }
    /// Reuse the SAME cold cells after every registered node finished all
    /// callbacks. Busy/draining cells refuse before mutation or allocation.
    pub fn resetOutsideExecution(self: *UseBank) !void {
        if (self.epoch) |held| {
            if (!try held.isSealed()) return error.WorkspaceUseBankBusy;
        }
        for (self.entries) |entry| if (!entry.cell.isIdle()) return error.WorkspaceUseBankBusy;
        if (self.epoch) |*held| held.release();
        self.epoch = null;
        self.count = 0;
        for (self.entries) |*entry| {
            entry.root = null;
            entry.identity = 0;
        }
    }
    pub fn deinitOutsideExecution(self: *UseBank) void {
        // Queued nodes own independent cells; they may outlive this bank and
        // every original caller/provider/model pointer. No Root deref here.
        for (self.entries) |entry| entry.cell.deinitOutsideExecution();
        self.allocator.free(self.entries);
        if (self.epoch) |*epoch| epoch.release();
        self.* = undefined;
    }
};

/// Borrowed ONLY by the actual ephemeral receiving MetalCompute wrapper.
/// One shared InvocationContext type carries this loan beside NOWB. Neither
/// this object nor its Phase/Bank pointers can escape into SDK/cache/Model
/// metadata. Generic free never consults it; private allocating-leaf failure
/// cleanup must validate exact root membership and explicit actual receiver.
pub const ReceiverUses = struct {
    existing: *const UseBank,
    allocations: ?*Phase = null,
    bound_operation: ?@import("../execution/backend_operation.zig").Borrowed = null,

    pub fn bindReceiver(self: *ReceiverUses, cb: *const @import("../ops/ops.zig").ComputeBackend, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.bound_operation != null) return error.WorkspaceReceiverAlreadyBound;
        const actual = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        const held = self.existing.epoch orelse return error.UnpreparedWorkspaceUses;
        if (current.outside_execution or actual.record != held.record or actual.nonce != held.nonce)
            return error.ForeignWorkspaceUse;
        if (try actual.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        self.bound_operation = try current.operationFor(cb);
    }
    pub fn unbindReceiver(self: *ReceiverUses) void {
        self.bound_operation = null;
    }
    pub fn validateRoot(self: *const ReceiverUses, root: *const Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        const expected = self.bound_operation orelse return error.WorkspaceReceiverNotBound;
        const actual = current.operation orelse return error.MissingBackendOperation;
        if (actual.context != expected.context or actual.nonce != expected.nonce or actual.mode != expected.mode or actual.vtable != expected.vtable)
            return error.ForeignWorkspaceUse;
        self.existing.validateRoot(root, current) catch |err| {
            if (err != error.UnpreparedWorkspaceUses) return err;
            const allocations = self.allocations orelse return err;
            return allocations.validateFreshRootUse(root, current);
        };
    }
};

/// One cold-prepared metadata slot for one actual physical allocation. Slots
/// survive their synchronous Phase through only pure BackingOwner/Charge pins.
/// No Phase, provider, SDK object, model or caller allocator is captured late.
const Slot = struct {
    allocator: std.mem.Allocator,
    shell: retirement.PreparedCharge,
    root: Root = undefined,
    initial_use: *@import("metal_workspace_fence.zig").Fence,
    close_fence: *@import("metal_workspace_fence.zig").Fence,
    metadata_charge: ?*retirement.Charge = null,
    initial_use_owned: bool = true,
    phase_metadata: ?@import("../execution/backing_retirement.zig").BackingOwner.Metadata = null,
    armed: bool = false,
    published: bool = false,

    fn prepare(a: std.mem.Allocator) !*Slot {
        const self = try a.create(Slot);
        errdefer a.destroy(self);
        var shell = try retirement.PreparedCharge.prepareForBackend(.metal, 1, 1);
        errdefer shell.destroyUnused();
        const initial_use = try @import("metal_workspace_fence.zig").Fence.prepare();
        errdefer initial_use.deinitOutsideExecution();
        const close_fence = try @import("metal_workspace_fence.zig").Fence.prepare();
        self.* = .{ .allocator = a, .shell = shell, .initial_use = initial_use, .close_fence = close_fence };
        return self;
    }
    fn destroyUnused(self: *Slot) void {
        std.debug.assert(!self.armed);
        self.shell.destroyUnused();
        self.initial_use.deinitOutsideExecution();
        self.close_fence.deinitOutsideExecution();
        const metadata_charge = self.metadata_charge;
        self.allocator.destroy(self);
        if (metadata_charge) |charge| charge.release();
    }
    fn releaseMetadata(raw: *anyopaque) void {
        const self: *Slot = @ptrCast(@alignCast(raw));
        std.debug.assert(self.armed and self.shell.owner == null);
        std.debug.assert(self.phase_metadata == null);
        const endpoint = self.root.endpoint;
        const initial_use = if (self.initial_use_owned) self.initial_use else null;
        const close_fence = self.close_fence;
        const metadata_charge = self.metadata_charge;
        self.allocator.destroy(self);
        if (initial_use) |cell| cell.deinitOutsideExecution();
        close_fence.deinitOutsideExecution();
        if (endpoint) |pin| pin.release();
        if (metadata_charge) |charge| charge.release();
    }
    const Attach = struct {
        slot: *Slot,
        buffer: *anyopaque,
        const table: retirement.Scope.VTable = .{
            .begin = begin,
            .attach = unsupported,
            .attach_prepared = attach,
            .end = end,
            .notify = retirement.pollRetirementChargesCPU,
        };
        fn begin(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT) !retirement.Scope.Begun {
            const self: *Attach = @ptrCast(@alignCast(raw));
            return .{ .origin = .{ .identity = self.slot.root.identity }, .pending_batches = 0 };
        }
        fn unsupported(_: *anyopaque, _: []const @import("../ops/ops.zig").CT, _: []retirement.Node, _: retirement.Payload) !void {
            return error.UnpreparedWorkspaceRoot;
        }
        fn attach(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload, prepared: *retirement.BackendPreparation) !void {
            if (nodes.len != 0) return error.InvalidWorkspaceRoot;
            const self: *Attach = @ptrCast(@alignCast(raw));
            const metal = @import("metal_runtime.zig");
            const witnesses = try prepared.armMetal(null, &.{self.buffer}, payload);
            if (antfly_metal_workspace_witness_tag(witnesses, &self.slot.root, &RootOperations.canonical) != 0) return error.InvalidWorkspaceRoot;
            if (metal.antfly_metal_buffer_retirement_attach(witnesses) != 0) return error.InvalidWorkspaceRoot;
        }
        fn end(_: *anyopaque) void {}
    };
    extern fn antfly_metal_workspace_witness_tag(witnesses: *anyopaque, root: *Root, operations: *const RootOperations) c_int;

    fn publish(self: *Slot, buffer: *anyopaque, actual: usize, current: *const sink.Epoch) !void {
        if (!self.armed or self.published) return error.InvalidWorkspaceRoot;
        if (actual == 0 or actual > self.root.backing.admittedBytes()) return error.WorkspaceBackingExceedsAdmission;
        var attach = Attach{ .slot = self, .buffer = buffer };
        var scope: retirement.Scope = .{ .context = &attach, .origin = .{ .identity = self.root.identity }, .outputs = &.{}, .vtable = &Attach.table, .active = true };
        defer scope.end();
        var ticket = try self.root.backing.charge.prepareAttachment();
        defer ticket.deinit();
        const metadata = self.root.backing.retainMetadataPin();
        ticket.attachWithMetadata(&scope, metadata) catch |err| {
            // Ticket consumption transfers the pure metadata pin even if an
            // SDK witness publication subsequently fails. Pre-consumption
            // validation failure leaves it owned here.
            if (ticket.group != null) metadata.release(metadata.context);
            return err;
        };
        self.published = true;
        self.root.backing.measure(current, actual);
    }
};

/// Synchronous actual allocator service. Its final-address Phase is borrowed
/// ONLY while the caller owns the real numerical/provider operation. Every
/// Slot, group, SDK witness and final-boundary node is prepared cold.
pub const AllocationService = extern struct {
    context: *anyopaque,
    runtime_address: usize,
    root_operations: *const RootOperations,
    take: *const fn (*anyopaque, usize) callconv(.c) ?*anyopaque,
    publish: *const fn (*anyopaque, *anyopaque, *anyopaque, usize) callconv(.c) c_int,
    cancel: *const fn (*anyopaque, *anyopaque) callconv(.c) void,
    retire_transient: *const fn (*anyopaque, *anyopaque) callconv(.c) c_int,
    retain_cached: *const fn (*anyopaque, *Root, *const @import("../execution/cleanup_context.zig").Context) callconv(.c) c_int,
};
pub const Phase = struct {
    /// Set only by the actual, cold-revalidated reference program receiver.
    host_reference_prepared: bool = false,
    reference_implementation: ?@import("../execution/kernel_route.zig").Implementation = null,
    /// Borrowed only through this synchronous receiving scope. Slots/late
    /// Nodes never copy this original permit pointer or its host authority.
    reference_host_lease: ?*const @import("../runtime/tier/memory.zig").AdmissionLease = null,
    reference_host_bytes: usize = 0,
    reference_host_floor: usize = 0,
    slots: []*Slot,
    cursor: usize = 0,
    model_sink: ?sink.Ref = null,
    target_runtime_address: usize = 0,
    envelope: ?@import("../execution/storage_admission.zig").Reservation = null,
    metadata_charge: ?*retirement.Charge = null,
    receiving_capacity: usize = 0,
    domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null,
    current: ?*const sink.Epoch = null,
    endpoint: ?*@import("metal_runtime_lifetime.zig").Lifetime = null,
    creators: ?*Creators = null,
    operation: ?@import("../execution/backend_operation.zig").Borrowed = null,
    failure: ?anyerror = null,
    allocator: std.mem.Allocator = metadata_allocator,

    /// Exact fresh allocation count comes from the selected physical role
    /// plan. Never size metadata from a byte envelope/minimum allocation.
    pub fn prepareExact(count: usize) !Phase {
        const slots = try metadata_allocator.alloc(*Slot, count);
        errdefer metadata_allocator.free(slots);
        var made: usize = 0;
        errdefer for (slots[0..made]) |slot| slot.destroyUnused();
        for (slots) |*slot| {
            slot.* = try Slot.prepare(metadata_allocator);
            made += 1;
        }
        return .{ .slots = slots };
    }
    extern fn antfly_metal_retirement_arena_requested_bytes(output_capacity: usize, origin_capacity: usize) usize;
    /// Two actual fences per finite Slot (initial use and permanent close),
    /// pure Charge/Group metadata plus the exact C/ObjC prepared witness bank.
    /// No count derives from a byte envelope or minimum buffer size.
    pub fn requestedPersistentMetadataBytes(count: usize, receiving_count: usize) !usize {
        if (count == 0 and receiving_count == 0) return 0;
        if (comptime !@import("build_options").enable_metal) return error.UnsupportedBackend;
        const sdk = antfly_metal_retirement_arena_requested_bytes(3, 1);
        if (sdk == 0) return error.InvalidWorkspaceMetadataSize;
        const fences = 2 * @sizeOf(@import("metal_workspace_fence.zig").Fence);
        const per_slot = try std.math.add(usize, @sizeOf(Slot) + @sizeOf(*Slot) + fences, try std.math.add(usize, try retirement.PreparedCharge.requestedMetadataBytes(1), sdk));
        // One shared existing pure Charge keeps this whole finite metadata
        // allowance until the LAST actual Slot dies, including escaped Roots.
        const receiving = try std.math.mul(usize, receiving_count, @sizeOf(@import("metal_workspace_fence.zig").Fence));
        return std.math.add(usize, try std.math.add(usize, try std.math.mul(usize, count, per_slot), receiving), try retirement.PreparedCharge.requestedMetadataBytes(1));
    }
    /// Cold move from the SAME actual request's backend-scratch token. The
    /// selected role/producer declaration supplies a physical SDK upper bound
    /// and exact fresh count, ADDED by admitDeclaredStorageWithPressure to the
    /// complete original peak; host IO, opaque floors and all remaining request
    /// amounts stay in the original permit. No retain/shrink/refund callback is
    /// run while any numerical owner is held.
    ///
    /// Every fallible model/shell/domain/partition preparation precedes commit.
    /// No extra controller reservation, reconstructed external token, guessed
    /// per-byte shell count or borrowed Model/Arena lifetime enters the roots.
    pub fn prepareFromPermitOutsideExecution(session: @import("session.zig").Session, permit: *@import("session.zig").RunPermit, retained: *const @import("cold_compute_target.zig").Retained, count: usize, physical_bytes: usize, receiving_count: usize, control: ?@import("../execution_control.zig").InferenceExecutionControl) !Phase {
        return prepareFromPermitOutsideExecutionWithHostCpu(session, permit, retained, count, physical_bytes, receiving_count, 0, control);
    }

    pub fn prepareFromPermitOutsideExecutionWithHostCpu(session: @import("session.zig").Session, permit: *@import("session.zig").RunPermit, retained: *const @import("cold_compute_target.zig").Retained, count: usize, physical_bytes: usize, receiving_count: usize, reference_host_bytes: usize, control: ?@import("../execution_control.zig").InferenceExecutionControl) !Phase {
        if (permit.session.ptr != session.ptr or permit.session.vtable != session.vtable)
            return error.ForeignWorkspacePermit;
        const policy = session.run_admission orelse return error.MissingRunAdmission;
        if (policy.backend_class != .gpu) return error.UnsupportedWorkspaceAdmission;
        const model = session.model_lifetime orelse return error.MissingWorkspaceModelLifetime;
        const source = if (permit.lease) |*lease| lease else return error.MissingRunAdmission;
        if (source.controller != policy.controller or source.retain_backend_class != .gpu)
            return error.ForeignWorkspaceAdmission;
        if ((count == 0) != (physical_bytes == 0)) return error.InvalidWorkspaceDeclaration;
        // Only an actual additive declaration may lend these bytes. Ordinary
        // scratch contains private/static/opaque floors in the SAME class;
        // backend classification alone is never evidence of a disjoint loan.
        const floor = permit.declared_storage_floor orelse return error.MissingDeclaredWorkspaceAllowance;
        if (permit.prepared != null or permit.physical_storage != null or permit.physical_prepared)
            return error.ForeignPreparedWorkspaceAllowance;
        const persistent_metadata = try requestedPersistentMetadataBytes(count, receiving_count);
        if (physical_bytes > permit.physical_credit.backend_scratch_bytes or try std.math.add(usize, persistent_metadata, reference_host_bytes) > permit.physical_credit.host_scratch_bytes)
            return error.InsufficientWorkspaceAdmission;
        const retained_backend = try std.math.add(usize, floor.backend_scratch_bytes, permit.physical_credit.backend_scratch_bytes);
        const retained_host = try std.math.add(usize, floor.host_scratch_bytes, permit.physical_credit.host_scratch_bytes);
        if (source.amounts.backend_scratch_bytes < retained_backend or source.amounts.host_scratch_bytes < retained_host or
            source.amounts.backend_weight_bytes < floor.backend_weight_bytes or source.amounts.host_weight_bytes < floor.host_weight_bytes or
            source.amounts.backend_kv_bytes < floor.backend_kv_bytes or source.amounts.host_kv_bytes < floor.host_kv_bytes)
            return error.InsufficientWorkspaceAdmission;
        if (control) |active| try active.check();
        var result = try prepareExact(count);
        errdefer result.finishOutsideExecution();
        try result.certifyModelTarget(session, retained, control);
        const domain = model.retainAdmissionDomain(policy.controller) orelse return error.MissingWorkspaceAdmissionDomain;
        var domain_owned = true;
        defer if (domain_owned) domain.release();
        var metadata_shell: ?retirement.PreparedCharge = if (persistent_metadata != 0) try retirement.PreparedCharge.prepare(1) else null;
        defer if (metadata_shell) |*shell| shell.destroyUnused();
        var metadata_domain: ?@import("../runtime/tier/admission_domain.zig").Pin = if (persistent_metadata != 0) domain.retain() else null;
        defer if (metadata_domain) |pin| pin.release();
        // ONE parent partition prepares the complete disjoint storage amount.
        // Its exclusive future child is then prepared for metadata separately.
        // Do not prepare two independent splits of the SAME source lease.
        var combined: ?@import("../runtime/tier/memory.zig").AdmissionLease.PreparedPartition = null;
        defer if (combined) |*split| split.abort();
        var host_split: ?@import("../runtime/tier/memory.zig").AdmissionLease.PreparedPartition = null;
        defer if (host_split) |*split| split.abort();
        if (physical_bytes != 0 or persistent_metadata != 0) {
            combined = try source.preparePartition(metadata_allocator, .{ .backend_scratch_bytes = physical_bytes, .host_scratch_bytes = persistent_metadata });
            if (persistent_metadata != 0) host_split = try combined.?.child.preparePartition(metadata_allocator, .{ .host_scratch_bytes = persistent_metadata });
        }
        if (control) |active| try active.check();
        var reservation: @import("../execution/storage_admission.zig").Reservation = .{
            .identity = .{ .controller = policy.controller, .backend_class = .gpu },
            .domain = .backend,
        };
        if (combined) |*split| {
            // Discard only this by-value COPY: split.child is the actual moved
            // combined owner, and remains the host_split.source until commit.
            _ = split.commit();
            if (host_split) |*metadata_split| {
                var metadata_reservation: ?@import("../execution/storage_admission.zig").Reservation = .{
                    .identity = reservation.identity,
                    .domain = .host,
                    .leases = .{ metadata_split.commit(), null },
                };
                const charge = metadata_shell.?.armReservation(&metadata_reservation, &metadata_domain) catch
                    platform.inference_process_supervisor.restartWorker();
                result.metadata_charge = charge;
                for (result.slots) |slot| {
                    slot.metadata_charge = charge.retainOwner();
                    slot.initial_use.bindMetadataChargeOutsideExecution(charge) catch platform.inference_process_supervisor.restartWorker();
                    slot.close_fence.bindMetadataChargeOutsideExecution(charge) catch platform.inference_process_supervisor.restartWorker();
                }
            }
            reservation.leases[0] = split.child;
        }
        permit.physical_credit.backend_scratch_bytes -= physical_bytes;
        permit.physical_credit.host_scratch_bytes -= persistent_metadata;
        // Consume only the explicit additive CPU declaration. Its SAME source
        // lease remains in the original permit until all host temporaries die
        // and gates unlock; no old fungible floor is lent a second time.
        if (reference_host_bytes != 0) {
            permit.physical_credit.host_scratch_bytes -= reference_host_bytes;
            result.reference_host_lease = source;
            result.reference_host_bytes = reference_host_bytes;
            result.reference_host_floor = floor.host_scratch_bytes;
        }
        // No fallible action follows this exact-once source move. Each Slot
        // carries a pure shared metadata charge, never the source/model arena.
        result.envelope = reservation;
        result.receiving_capacity = receiving_count;
        result.domain = domain;
        domain_owned = false;
        return result;
    }
    pub fn requireReferenceHostAllowance(self: *const Phase, bytes: usize) !void {
        if (bytes == 0) return error.MissingReferenceHostAllowance;
        const lease = self.reference_host_lease orelse return error.MissingReferenceHostAllowance;
        const domain = self.domain orelse return error.MissingWorkspaceAdmissionDomain;
        if (lease.controller != domain.controller() or lease.retain_backend_class != .gpu) return error.ForeignWorkspaceAdmission;
        if (self.reference_host_bytes != bytes or lease.amounts.host_scratch_bytes < try std.math.add(usize, self.reference_host_floor, bytes))
            return error.InsufficientWorkspaceAdmission;
    }

    /// Borrowed only by this active numerical scope. This proves the existing
    /// Phase's exact source host allowance, receiver/runtime and NOWB again at
    /// the original CPU reference producer; a target boolean grants no credit.
    pub fn validateReferenceHostUse(self: *const Phase, runtime: *anyopaque, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (!self.host_reference_prepared) return error.MissingReferenceHostAllowance;
        try self.requireReferenceHostAllowance(self.reference_host_bytes);
        const expected_epoch = self.current orelse return error.UnpreparedWorkspaceUses;
        const actual_epoch = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        const expected = self.operation orelse return error.UnpreparedWorkspaceUses;
        const actual = current.operation orelse return error.MissingBackendOperation;
        const implementation = self.reference_implementation orelse return error.UnpreparedWorkspaceUses;
        const endpoint = self.endpoint orelse return error.UnpreparedWorkspaceUses;
        if (self.target_runtime_address != @intFromPtr(runtime) or endpoint.endpoint_address != @intFromPtr(runtime) or
            self.creators == null or self.creators.?.endpoint != endpoint) return error.ForeignWorkspaceEndpoint;
        if (current.outside_execution or actual_epoch.record != expected_epoch.record or actual_epoch.nonce != expected_epoch.nonce or
            actual.context != expected.context or actual.nonce != expected.nonce or actual.mode != expected.mode or actual.vtable != expected.vtable)
            return error.ForeignWorkspaceUse;
        if (try actual_epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        try actual.validate(.{ .implementation = implementation, .program = null, .mode = .legacy_unknown_frame });
        var bound = (try actual.retainCleanupEpoch()) orelse return error.MissingWorkspaceCleanupEpoch;
        defer bound.release();
        if (bound.record != actual_epoch.record or bound.nonce != actual_epoch.nonce) return error.ForeignWorkspaceUse;
    }

    pub fn fundReceivingBankOutsideExecution(self: *const Phase, bank: *UseBank) !void {
        if (bank.entries.len != self.receiving_capacity or bank.epoch != null or bank.count != 0) return error.InvalidWorkspaceMetadataBank;
        if (bank.entries.len == 0) return;
        const charge = self.metadata_charge orelse return error.MissingWorkspaceMetadataAdmission;
        for (bank.entries) |entry| if (!entry.cell.isIdle() or entry.cell.metadata_charge != null) return error.WorkspaceFenceBusy;
        for (bank.entries) |entry| entry.cell.bindMetadataChargeOutsideExecution(charge) catch unreachable;
    }
    /// Cold-only certification for the ACTUAL model target. Installing its
    /// pure Sink before charge creation guarantees final raw ModelLifetime
    /// release has its own new post-close epoch. Owner.beginResident's fallback
    /// Sink alone is insufficient: a manual Session without ModelLifetime may
    /// keep legacy behavior, but can NEVER mint a modern workspace charge.
    /// The caller retains the target/model through this synchronous Phase.
    /// NOWB can legitimately be FOREIGN; it need not belong to model_sink.
    pub fn certifyModelTarget(self: *Phase, session: @import("session.zig").Session, retained: *const @import("cold_compute_target.zig").Retained, control: ?@import("../execution_control.zig").InferenceExecutionControl) !void {
        if (self.model_sink != null or self.envelope != null or self.cursor != 0) return error.InvalidWorkspacePhase;
        const model = session.model_lifetime orelse return error.MissingWorkspaceModelLifetime;
        if (retained.model != model) return error.ForeignWorkspaceModelLifetime;
        // Final-resource storage is a constructor invariant, never lazily
        // allocated on an ordinary published admission/model gate.
        if (model.prepared_final_resources == null) return error.UnpreparedWorkspaceModelLifetime;
        if (session.ptr != model.owner or model.destroy_with_boundary_fn == null)
            return error.UnsupportedWorkspaceModelClose;
        const target = switch (retained.target) {
            .metal => |value| value,
            else => return error.UnsupportedWorkspaceTarget,
        };
        const fresh = (try session.coldComputeTarget(control)) orelse return error.WorkspaceTargetChanged;
        const current = switch (fresh) {
            .metal => |value| value,
            else => return error.WorkspaceTargetChanged,
        };
        if (current.runtime != target.runtime or current.exclusion != target.exclusion) return error.WorkspaceTargetChanged;
        const stable_sink = try model.retainRetirementSink(); // actual model field, not fallback
        self.model_sink = stable_sink;
        self.target_runtime_address = @intFromPtr(target.runtime);
    }
    /// Move the independently partitioned physical envelope outside all
    /// execution lanes. It must already have a shared external-token owner,
    /// making every later partition metadata-only/no supplier/no allocation.
    pub fn adopt(self: *Phase, envelope: *?@import("../execution/storage_admission.zig").Reservation, domain: *?@import("../runtime/tier/admission_domain.zig").Pin) !void {
        if (self.envelope != null or self.current != null) return error.InvalidWorkspacePhase;
        const value = envelope.* orelse return error.MissingWorkspaceAdmission;
        const pin = domain.* orelse return error.MissingWorkspaceAdmissionDomain;
        if (value.identity.controller != pin.controller() or value.identity.backend_class != .gpu) return error.ForeignAdmissionDomain;
        for (value.leases) |maybe| if (maybe) |lease| {
            if (lease.controller != value.identity.controller) return error.ForeignAdmissionDomain;
            if (lease.external_lease != null and lease.shared_external == null) return error.UnpreparedWorkspacePartition;
        };
        // Actual cold ownership boundary: a matching separate Domain cannot
        // launder arbitrary Reservation lifetime callbacks into gate-time
        // partitioning or late pure Charge destruction.
        if (value.lifetime) |lifetime| {
            if (lifetime.retain != null) return error.UnsupportedWorkspaceLifetime;
            const actual = (try value.retainAdmissionDomain()) orelse return error.UnsupportedWorkspaceLifetime;
            defer actual.release(); // cold; canonical independent metadata only
            if (actual.owner != pin.owner) return error.ForeignAdmissionDomain;
        }
        if (self.model_sink == null or self.target_runtime_address == 0) return error.MissingWorkspaceModelLifetime;
        self.envelope = value;
        self.domain = pin;
        envelope.* = null;
        domain.* = null;
    }
    pub fn bind(self: *Phase, current: @import("../execution/cleanup_context.zig").Context, cb: *const @import("../ops/ops.zig").ComputeBackend, endpoint: *@import("metal_runtime_lifetime.zig").Lifetime, creators: *Creators) !AllocationService {
        if (self.current != null or self.envelope == null or self.domain == null) return error.InvalidWorkspacePhase;
        _ = try current.operationFor(cb);
        const boundary = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        if (try boundary.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        if (self.model_sink == null or self.target_runtime_address == 0) return error.MissingWorkspaceModelLifetime;
        if (endpoint.endpoint_address != self.target_runtime_address or creators.endpoint != endpoint) return error.ForeignWorkspaceEndpoint;
        self.current = boundary; // borrowed only until unbind, never retained in Slot
        self.endpoint = endpoint;
        self.creators = creators;
        self.operation = current.operation;
        self.reference_implementation = .{ .context = cb.ptr, .table = cb.vtable };
        return .{ .context = self, .runtime_address = self.target_runtime_address, .root_operations = &RootOperations.canonical, .take = take, .publish = publish, .cancel = cancel, .retire_transient = retireTransient, .retain_cached = retainCached };
    }
    pub fn unbind(self: *Phase) void {
        self.host_reference_prepared = false;
        self.reference_host_lease = null;
        self.reference_implementation = null;
        self.current = null;
        self.endpoint = null;
        self.creators = null;
        self.operation = null;
    }
    /// Private newly allocating leaf only: exact actual receiver/current B
    /// must match the synchronous Phase and its already-registered initial
    /// cell. This does NOT authorize public generic free or escaped outputs.
    pub fn validateFreshRootUse(self: *const Phase, root: *const Root, current: @import("../execution/cleanup_context.zig").Context) !void {
        const expected_epoch = self.current orelse return error.UnpreparedWorkspaceUses;
        const actual_epoch = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        const expected = self.operation orelse return error.UnpreparedWorkspaceUses;
        const actual = current.operation orelse return error.MissingBackendOperation;
        if (current.outside_execution or actual_epoch.record != expected_epoch.record or actual_epoch.nonce != expected_epoch.nonce or
            actual.context != expected.context or actual.nonce != expected.nonce or actual.mode != expected.mode or actual.vtable != expected.vtable)
            return error.ForeignWorkspaceUse;
        if (try actual_epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        for (self.slots[0..self.cursor]) |slot| if (&slot.root == root) {
            if (!slot.published or !slot.initial_use.validatesUse(&root.backing, actual_epoch)) return error.UnpreparedWorkspaceUses;
            return;
        };
        return error.UnpreparedWorkspaceUses;
    }
    fn retainCached(raw: *anyopaque, root: *Root, current: *const @import("../execution/cleanup_context.zig").Context) callconv(.c) c_int {
        const self: *Phase = @ptrCast(@alignCast(raw));
        const expected = self.operation orelse return -1;
        const actual = current.operation orelse return -1;
        const epoch = self.current orelse return -1;
        const supplied = current.boundary orelse return -1;
        if (current.outside_execution or actual.context != expected.context or actual.nonce != expected.nonce or actual.mode != expected.mode or actual.vtable != expected.vtable or supplied.record != epoch.record or supplied.nonce != epoch.nonce) return -1;
        const origin = root.endpoint orelse return -1;
        if (self.endpoint != origin) return -1;
        // bind validated the actual operation; this synchronous service ends
        // BEFORE that owner/lease may leave. Under SDK metadata locks compare
        // exact immutable capabilities only, never reenter Engine/Sink locks.
        (self.creators orelse return -1).adopt(root) catch return -1;
        return 0;
    }
    fn take(raw: *anyopaque, bound: usize) callconv(.c) ?*anyopaque {
        const self: *Phase = @ptrCast(@alignCast(raw));
        if (self.current == null or bound < 16384 or self.cursor == self.slots.len) {
            self.failure = error.UnpreparedWorkspaceAllocation;
            return null;
        }
        var reservation: ?@import("../execution/storage_admission.zig").Reservation = self.envelope.?.partition(self.allocator, bound) catch |err| {
            self.failure = err;
            return null;
        };
        var pin: ?@import("../runtime/tier/admission_domain.zig").Pin = self.domain.?.retain();
        const slot = self.slots[self.cursor];
        const charge = slot.shell.armReservation(&reservation, &pin) catch platform.inference_process_supervisor.restartWorker();
        slot.root.init(charge, .{ .context = slot, .release = Slot.releaseMetadata });
        slot.root.close_fence = slot.close_fence;
        slot.root.bindEndpoint(self.endpoint orelse unreachable);
        slot.armed = true;
        self.cursor += 1;
        // Pure Phase custody keeps the Slot valid until its outside reconcile
        // even if the actual guard seals after an aborted SDK allocation.
        slot.phase_metadata = slot.root.retainSnapshot();
        slot.initial_use.bindInitialBackingBeforePublication(&slot.root.backing) catch |err| {
            self.failure = err;
            return null;
        };
        slot.initial_use.retire(.{ .boundary = self.current }) catch |err| {
            self.failure = err;
            return null;
        };
        return slot;
    }
    fn publish(raw: *anyopaque, token: *anyopaque, buffer: *anyopaque, actual: usize) callconv(.c) c_int {
        const self: *Phase = @ptrCast(@alignCast(raw));
        const slot: *Slot = @ptrCast(@alignCast(token));
        const creators = self.creators orelse return -1;
        // Validate actual creator provenance BEFORE SDK witness publication.
        // The provider owner serializes these fields, and a fresh Slot's real
        // creator pin cannot disappear during synchronous publication.
        creators.validateOrigin(&slot.root) catch |err| {
            self.failure = err;
            return -1;
        };
        if (slot.root.listed or slot.root.creator_identity != 0 or !slot.root.creator_live.load(.acquire)) {
            self.failure = error.InvalidWorkspaceRoot;
            return -1;
        }
        slot.publish(buffer, actual, self.current orelse return -1) catch |err| {
            self.failure = err;
            return -1;
        };
        // No recoverable failure after successful witness publication: C's
        // cancellation contract applies only to an UNPUBLISHED fresh Slot.
        creators.adopt(&slot.root) catch platform.inference_process_supervisor.restartWorker();
        return 0;
    }
    fn retireTransient(raw: *anyopaque, token: *anyopaque) callconv(.c) c_int {
        const self: *Phase = @ptrCast(@alignCast(raw));
        const slot: *Slot = @ptrCast(@alignCast(token));
        var accepted = false;
        for (self.slots[0..self.cursor]) |actual| if (actual == slot) {
            accepted = true;
            break;
        };
        if (!accepted or !slot.published) return -1;
        const epoch = self.current orelse return -1;
        (self.creators orelse return -1).releaseOne(&slot.root, .{ .boundary = epoch, .operation = self.operation }) catch return -1;
        return 0;
    }
    fn cancel(_: *anyopaque, token: *anyopaque) callconv(.c) void {
        const slot: *Slot = @ptrCast(@alignCast(token));
        // Keep the taken conservative quota in its slot until after numerical
        // unlock. A failed SDK allocation does not run supplier callbacks here.
        std.debug.assert(slot.armed and !slot.published);
    }
    /// Caller first reconciles every created Slot with the actual C field
    /// census: cached roots transfer their physical creator pin to provider;
    /// absent roots release it with the actual current cleanup context. It
    /// then clears the allocator service and leaves ALL real execution lanes.
    pub fn finishOutsideExecution(self: *Phase) void {
        std.debug.assert(self.current == null);
        for (self.slots[0..self.cursor]) |slot| {
            if (!slot.published) {
                if (slot.initial_use_owned) {
                    slot.initial_use_owned = false;
                    slot.initial_use.deinitOutsideExecution();
                }
                if (slot.root.creator_live.load(.acquire))
                    slot.root.releaseCreator(.{ .outside_execution = true }) catch platform.inference_process_supervisor.restartWorker();
            } else std.debug.assert(!slot.root.creator_live.load(.acquire) or slot.root.listed);
            const pin = slot.phase_metadata.?;
            slot.phase_metadata = null;
            pin.release(pin.context); // can destroy Slot; no later access
        }
        for (self.slots[self.cursor..]) |slot| slot.destroyUnused();
        self.allocator.free(self.slots);
        if (self.envelope) |*owned| owned.release();
        if (self.metadata_charge) |charge| charge.release();
        if (self.domain) |pin| pin.release();
        if (self.model_sink) |*stable| stable.release();
        self.* = undefined;
    }
};

test "workspace cold adoption rejects custom lifetime before any callback or ownership move" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const storage = @import("../execution/storage_admission.zig");
    const admission = @import("../runtime/tier/admission_domain.zig");
    const Spy = struct {
        retains: usize = 0,
        releases: usize = 0,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.retains += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    const owner = try admission.Domain.create();
    defer owner.release();
    var pin: ?admission.Pin = owner.pin();
    defer if (pin) |held| held.release();
    var spy: Spy = .{};
    var reservation: ?storage.Reservation = .{
        .identity = .{ .controller = &owner.controller, .backend_class = .gpu },
        .leases = .{ try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null },
        .lifetime = .{ .context = &spy, .retain = Spy.retain, .release = Spy.release },
        .domain = .backend,
    };
    defer if (reservation) |*held| held.release();
    var phase = try Phase.prepareExact(0);
    defer phase.finishOutsideExecution();
    try std.testing.expectError(error.UnsupportedWorkspaceLifetime, phase.adopt(&reservation, &pin));
    try std.testing.expect(reservation != null and pin != null and phase.envelope == null and phase.domain == null);
    try std.testing.expectEqual(@as(usize, 0), spy.retains);
    try std.testing.expectEqual(@as(usize, 0), spy.releases);
    try std.testing.expectEqual(@as(usize, 64), owner.controller.snapshot().backend_scratch_bytes);
}

test "workspace creator census preserves parked custody and refuses another actual runtime" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    const Lifetime = @import("metal_runtime_lifetime.zig").Lifetime;
    const Case = struct {
        root: Root = undefined,
        destroyed: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
            self.root.endpoint.?.release();
        }
    };
    const endpoint = try Lifetime.prepare();
    defer {
        endpoint.closeAndDrain();
        endpoint.release();
    }
    endpoint.bindEndpointAddress(17);
    const foreign = try Lifetime.prepare();
    defer {
        foreign.closeAndDrain();
        foreign.release();
    }
    foreign.bindEndpointAddress(18);
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var reservation: ?storage.Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null },
        .domain = .backend,
    };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var fixture: Case = .{};
    fixture.root.init(charge, .{ .context = &fixture, .release = Case.release });
    fixture.root.bindEndpoint(endpoint);
    var creators = Creators.init(endpoint);
    var other = Creators.init(foreign);
    try creators.adopt(&fixture.root);
    try std.testing.expectError(error.ForeignWorkspaceEndpoint, other.adopt(&fixture.root));
    try std.testing.expect(creators.head == &fixture.root and other.head == null);
    // A bank owns an actual independent logical backing reference, not just
    // a Model/metadata pin. Runtime fields can be empty while this bank lives.
    try std.testing.expect(fixture.root.retainPhysical());
    var reference = try sink.Ref.create();
    defer reference.release();
    var cache_scope = try reference.enter();
    var cache_epoch = try cache_scope.retainEpoch();
    defer cache_epoch.release();
    try creators.reconcile(&.{}, &.{&fixture.root}, .{ .boundary = &cache_epoch });
    try std.testing.expect(creators.head == &fixture.root and fixture.root.creator_live.load(.acquire));
    try std.testing.expectError(error.MissingWorkspaceCleanupEpoch, creators.reconcile(&.{}, &.{}, .{}));
    try std.testing.expect(creators.head == &fixture.root);
    // The actual runtime creator is retired explicitly; bank custody remains.
    try creators.reconcile(&.{}, &.{}, .{ .boundary = &cache_epoch });
    try std.testing.expect(creators.head == null and !fixture.root.listed and fixture.root.creator_identity == 0);
    cache_scope.finish();
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    var bank_scope = try reference.enter();
    var bank_epoch = try bank_scope.retainEpoch();
    defer bank_epoch.release();
    try fixture.root.releasePhysical(.{ .boundary = &bank_epoch });
    try std.testing.expectEqual(@as(usize, 0), fixture.destroyed);
    try std.testing.expectError(error.ClosedWorkspaceRoot, creators.adopt(&fixture.root));
    bank_scope.finish();
    try std.testing.expectEqual(@as(usize, 1), fixture.destroyed);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "workspace receiver use bank rejects outer A as missing or foreign current cleanup and reuses cold cells" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const Metadata = struct {
        deaths: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.deaths += 1;
        }
    };
    var metadata: Metadata = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false);
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    var physical: ?@import("../execution/storage_admission.zig").Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ lease, null },
        .domain = .backend,
    };
    lease = null;
    const charge = try retirement.Charge.createReservation(&physical, &domain);
    var root: Root = undefined;
    root.init(charge, .{ .context = &metadata, .release = Metadata.release });
    const pin = root.retainSnapshot();
    try std.testing.expect(root.retainPhysical()); // independent logical view owner
    var bank = try UseBank.prepareExact(1);
    defer bank.deinitOutsideExecution();
    var reference = try sink.Ref.create();
    defer reference.release();
    var outer = try reference.enter();
    var a = try outer.retainEpoch();
    defer a.release();
    try bank.bindOutsideExecution(&.{&root}, &a);
    try std.testing.expectError(error.WorkspaceUseBankBusy, bank.resetOutsideExecution());
    var foreign = try reference.enter();
    var b = try foreign.retainEpoch();
    defer b.release();
    const absent: @import("../execution/cleanup_context.zig").Context = .{};
    // An active A does not make a raw/empty generic release authoritative,
    // even while another exact physical owner keeps Root non-final.
    try std.testing.expectEqual(@as(c_int, -1), RootOperations.canonical.validate_release(&root, &absent));
    const references_before_foreign = root.logical_references.load(.acquire);
    try std.testing.expectEqual(@as(c_int, -1), RootOperations.canonical.validate_release(&root, &.{ .boundary = &b }));
    try std.testing.expectEqual(references_before_foreign, root.logical_references.load(.acquire));
    try std.testing.expectEqual(@as(c_int, 0), RootOperations.canonical.validate_release(&root, &.{ .boundary = &a }));
    try std.testing.expectError(error.MissingWorkspaceCleanupEpoch, bank.validateRoot(&root, absent));
    try std.testing.expectError(error.ForeignWorkspaceUse, bank.validateRoot(&root, .{ .boundary = &b }));
    try bank.validateRoot(&root, .{ .boundary = &a });
    foreign.finish();
    outer.finish();
    try std.testing.expectError(error.SealedWorkspaceCleanupEpoch, bank.validateRoot(&root, .{ .boundary = &a }));
    try bank.resetOutsideExecution();
    // SAME exact cells and root; this round performs no prepare/allocation.
    var again = try reference.enter();
    var now = try again.retainEpoch();
    defer now.release();
    try bank.bindOutsideExecution(&.{&root}, &now);
    try bank.validateRoot(&root, .{ .boundary = &now });
    try root.releaseCreator(.{ .boundary = &now });
    try root.releasePhysical(.{ .boundary = &now });
    pin.release(pin.context);
    try std.testing.expectEqual(@as(usize, 0), metadata.deaths);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    again.finish();
    try std.testing.expectEqual(@as(usize, 1), metadata.deaths);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    try bank.resetOutsideExecution(); // never dereferences the now-dead root
}

test "workspace modern charge refuses manual Session fallback sink without actual ModelLifetime" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const session_mod = @import("session.zig");
    const storage = @import("../execution/storage_admission.zig");
    const admission = @import("../runtime/tier/admission_domain.zig");
    const Raw = struct {
        fn backend(_: *anyopaque) @import("backends.zig").BackendType {
            return .metal;
        }
        fn destroy(_: *anyopaque) void {}
        const table: session_mod.Session.VTable = .{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .close = undefined, .backend = backend };
    };
    const session: session_mod.Session = .{ .ptr = @ptrFromInt(16), .vtable = &Raw.table };
    var outer = try @import("../execution/cleanup_owner.zig").Owner.beginResident(session);
    defer outer.finishAfterUnlock();
    try std.testing.expect(outer.epoch() != null); // real independent fallback B
    var phase = try Phase.prepareExact(0);
    defer phase.finishOutsideExecution();
    const unusable: @import("cold_compute_target.zig").Retained = undefined;
    try std.testing.expectError(error.MissingWorkspaceModelLifetime, phase.certifyModelTarget(session, &unusable, null));
    var manual_model: session_mod.Session.ModelLifetime = .{ .owner = @ptrFromInt(16), .destroy_fn = Raw.destroy };
    var manual_session = session;
    manual_session.model_lifetime = &manual_model;
    const unprepared: @import("cold_compute_target.zig").Retained = .{ .target = undefined, .model = &manual_model };
    try std.testing.expectError(error.UnpreparedWorkspaceModelLifetime, phase.certifyModelTarget(manual_session, &unprepared, null));
    try std.testing.expect(manual_model.retirement_sink == null and manual_model.prepared_final_resources == null);
    const owner = try admission.Domain.create();
    defer owner.release();
    var pin: ?admission.Pin = owner.pin();
    defer if (pin) |held| held.release();
    var reservation: ?storage.Reservation = .{
        .identity = .{ .controller = &owner.controller, .backend_class = .gpu },
        .leases = .{ try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null },
        .domain = .backend,
    };
    defer if (reservation) |*held| held.release();
    try std.testing.expectError(error.MissingWorkspaceModelLifetime, phase.adopt(&reservation, &pin));
    try std.testing.expect(phase.model_sink == null and phase.envelope == null and phase.domain == null);
    try std.testing.expect(reservation != null and pin != null);
    try std.testing.expectEqual(@as(usize, 64), owner.controller.snapshot().backend_scratch_bytes);
}

fn testPermanentCloseSlot(a: std.mem.Allocator, controller: *@import("../runtime/tier/memory.zig").AdmissionController, bytes: usize, endpoint: *@import("metal_runtime_lifetime.zig").Lifetime) !*Slot {
    const storage = @import("../execution/storage_admission.zig");
    const slot = try Slot.prepare(a);
    errdefer slot.destroyUnused();
    var reservation: ?storage.Reservation = .{ .identity = .{ .controller = controller, .backend_class = .gpu }, .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = bytes }, false), null }, .domain = .backend };
    defer if (reservation) |*owned| owned.release();
    var pin: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try slot.shell.armReservation(&reservation, &pin);
    slot.root.init(charge, .{ .context = slot, .release = Slot.releaseMetadata });
    slot.root.close_fence = slot.close_fence;
    slot.root.bindEndpoint(endpoint);
    slot.armed = true;
    return slot;
}

test "workspace permanent provider close preflights every cold cell before any creator or list mutation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const Lifetime = @import("metal_runtime_lifetime.zig").Lifetime;
    const endpoint = try Lifetime.prepare();
    defer {
        endpoint.closeAndDrain();
        endpoint.release();
    }
    endpoint.bindEndpointAddress(17); // pure metadata-only fixture, no SDK dereference
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    const first = try testPermanentCloseSlot(std.testing.allocator, &controller, 64, endpoint);
    var first_pin: ?@import("../execution/backing_retirement.zig").BackingOwner.Metadata = first.root.retainSnapshot();
    defer if (first_pin) |pin| {
        if (first.root.creator_live.load(.acquire)) first.root.releaseCreator(.{ .outside_execution = true }) catch unreachable;
        pin.release(pin.context);
    };
    const second = try testPermanentCloseSlot(std.testing.allocator, &controller, 64, endpoint);
    var second_pin: ?@import("../execution/backing_retirement.zig").BackingOwner.Metadata = second.root.retainSnapshot();
    defer if (second_pin) |pin| {
        if (second.root.creator_live.load(.acquire)) second.root.releaseCreator(.{ .outside_execution = true }) catch unreachable;
        pin.release(pin.context);
    };
    var creators = Creators.init(endpoint);
    try creators.adopt(&second.root);
    try creators.adopt(&first.root);
    var source = try sink.Ref.create();
    defer source.release();
    var current = try source.enter();
    defer current.finish();
    var epoch = try current.retainEpoch();
    defer epoch.release();
    var busy = try source.enter();
    defer busy.finish();
    var busy_epoch = try busy.retainEpoch();
    defer busy_epoch.release();
    try second.close_fence.bindClosingBeforeRetirement(&second.root.backing);
    try second.close_fence.retire(.{ .boundary = &busy_epoch });
    try std.testing.expect(!second.root.backing.hasUseEpoch(&busy_epoch));
    try std.testing.expectEqual(@as(c_int, -1), RootOperations.canonical.validate_release(&second.root, &.{ .boundary = &busy_epoch }));
    try std.testing.expectError(error.WorkspaceFenceBusy, creators.preflightDetachForProviderClose(.{ .boundary = &epoch }));
    try std.testing.expectError(error.WorkspaceFenceBusy, creators.detachForProviderClose(.{ .boundary = &epoch }));
    try std.testing.expect(creators.head == &first.root and first.root.next_cached == &second.root);
    try std.testing.expect(first.root.creator_live.load(.acquire) and second.root.creator_live.load(.acquire) and first.close_fence.isIdle());
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().backend_scratch_bytes);
    busy.finish();
    {
        first.root.close_fence = null;
        defer first.root.close_fence = first.close_fence;
        try std.testing.expectError(error.MissingWorkspaceCloseFence, creators.preflightDetachForProviderClose(.{ .boundary = &epoch }));
        try std.testing.expect(first.close_fence.isIdle() and second.close_fence.isIdle());
    }
    {
        first.root.close_fence = second.close_fence;
        defer first.root.close_fence = first.close_fence;
        try std.testing.expectError(error.DuplicateWorkspaceCloseFence, creators.preflightDetachForProviderClose(.{ .boundary = &epoch }));
        try std.testing.expect(first.close_fence.isIdle() and second.close_fence.isIdle());
    }
    try std.testing.expectError(error.MissingWorkspaceCleanupEpoch, creators.preflightDetachForProviderClose(.{}));
    try creators.preflightDetachForProviderClose(.{ .boundary = &epoch });
    endpoint.closeAndDrain(); // irreversible endpoint step comes AFTER all preflights
    try creators.detachForProviderClose(.{ .boundary = &epoch });
    try std.testing.expect(creators.head == null and creators.endpoint == null);
    try std.testing.expect(!first.root.creator_live.load(.acquire) and !second.root.creator_live.load(.acquire));
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().backend_scratch_bytes);
    current.finish();
    const first_owned = first_pin.?;
    first_pin = null;
    first_owned.release(first_owned.context);
    const second_owned = second_pin.?;
    second_pin = null;
    second_owned.release(second_owned.context);
    _ = retirement.pollRawController(&controller);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
}

fn testPermanentCloseSdkCase(close_first: bool) !void {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const metal = @import("metal_runtime.zig");
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const Tensor = @import("metal_tensor.zig").MetalTensor;
    const Probe = struct {
        gate: ?*std.atomic.Mutex = null,
        unsafe: bool = false,
        releases: usize = 0,
        fn check(self: *@This()) void {
            if (self.gate) |gate| {
                if (gate.tryLock()) gate.unlock() else self.unsafe = true;
            }
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return 7;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            self.releases += 1;
        }
        extern fn antfly_metal_workspace_allocation_bound(?*metal.RawMetalDecodeRuntime, usize, c_int) usize;
    };
    var probe: Probe = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var provider = try @import("metal_native_provider.zig").MetalNativeProvider.create();
    var provider_live = true;
    defer if (provider_live) provider.deinitOwned();
    probe.gate = &provider.linear_slot_index_mutex;
    const runtime = provider.raw_decode_runtime orelse return error.ExpectedActualMetalRuntime;
    const endpoint = provider.workspace_endpoint orelse return error.ExpectedActualWorkspaceEndpoint;
    const bound = Probe.antfly_metal_workspace_allocation_bound(runtime, 512, 0);
    try std.testing.expect(bound >= 512);
    var source = try sink.Ref.create();
    defer source.release();
    var origin = try source.enter();
    defer origin.finish();
    var closing = try source.enter();
    defer closing.finish();
    var a = try origin.retainEpoch();
    defer a.release();
    var b = try closing.retainEpoch();
    defer b.release();
    const slot = try testPermanentCloseSlot(std.testing.allocator, &controller, bound, endpoint);
    var metadata: ?@import("../execution/backing_retirement.zig").BackingOwner.Metadata = slot.root.retainSnapshot();
    // SDK values below unwind first. Keep Slot/creator metadata stable even
    // if a failed assertion exposes an incomplete close transaction.
    defer {
        if (provider_live) {
            provider.deinitOwned();
            provider_live = false;
        }
        if (metadata) |pin| {
            if (slot.root.creator_live.load(.acquire)) slot.root.releaseCreator(.{ .outside_execution = true }) catch unreachable;
            pin.release(pin.context);
        }
        _ = retirement.pollRawController(&controller);
    }
    try slot.initial_use.bindInitialBackingBeforePublication(&slot.root.backing);
    try slot.initial_use.retire(.{ .boundary = &a });
    const handle = metal.termite_metal_buffer_alloc_fresh(runtime, 512, 1) orelse return error.OutOfMemory;
    var handle_live = true;
    defer if (handle_live) metal.termite_metal_buffer_release(handle);
    try slot.publish(handle, metal.termite_metal_buffer_allocated_size(handle), &a);
    try provider.workspace_creators.adopt(&slot.root);
    var tensor = Tensor.deviceOwned(@ptrCast(runtime), handle, 0, 512, &.{128});
    handle_live = false;
    var tensor_live = true;
    defer if (tensor_live) tensor.deinitWithCleanup(.{ .outside_execution = true });
    const external = metal.termite_metal_buffer_retain_external(runtime, handle, 512) orelse return error.ExpectedActualSdkRetain;
    var external_live = true;
    defer if (external_live) metal.termite_metal_buffer_release(external);
    try std.testing.expect(external == handle);
    try std.testing.expect(slot.root.logical_references.load(.acquire) >= 2);
    const before_close_releases = probe.releases;
    // The actual provider owns its own SDK quiesce/endpoint/slot preflights;
    // the permanent close cell captures B before a NONFINAL creator drop.
    provider.deinitOwnedWithCleanup(.{ .boundary = &b });
    provider_live = false;
    try std.testing.expect(!slot.root.creator_live.load(.acquire));
    try std.testing.expect(!slot.root.backing.hasUseEpoch(&b)); // close cell is NOT an SDK loan
    try std.testing.expectEqual(@as(usize, 1), slot.root.backing.closing_uses);
    try std.testing.expectEqual(before_close_releases, probe.releases);
    const pure_pin = metadata.?;
    metadata = null;
    pure_pin.release(pure_pin.context); // now ONLY real SDK and exact A/B pins remain
    if (close_first) closing.finish() else origin.finish();
    try std.testing.expectEqual(bound, controller.snapshot().backend_scratch_bytes);
    // Actual owning Ref is released after its original provider closed. The
    // independent raw SDK retain keeps the physical backing alive even then.
    tensor.deinitWithCleanup(.{ .outside_execution = true });
    tensor_live = false;
    _ = retirement.pollRawController(&controller);
    try std.testing.expectEqual(bound, controller.snapshot().backend_scratch_bytes);
    if (!close_first) {
        try std.testing.expect(provider.linear_slot_index_mutex.tryLock());
    }
    var held_gate = !close_first;
    defer if (held_gate) provider.linear_slot_index_mutex.unlock();
    metal.termite_metal_buffer_release(external);
    external_live = false;
    _ = retirement.pollRawController(&controller);
    try std.testing.expectEqual(bound, controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(before_close_releases, probe.releases);
    if (held_gate) {
        provider.linear_slot_index_mutex.unlock();
        held_gate = false;
    }
    if (close_first) origin.finish() else closing.finish();
    _ = retirement.pollRawController(&controller);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(probe.releases > before_close_releases and !probe.unsafe);
}

test "workspace permanent provider close fences exact B and actual retained SDK death in both seal orders" {
    try testPermanentCloseSdkCase(false);
    try testPermanentCloseSdkCase(true);
}

test "reference host allowance preserves original floor refuses cap minus one foreign and consumed source before bind" {
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const domain = try Domain.create();
    defer domain.release();
    const foreign = try Domain.create();
    defer foreign.release();
    const floor: usize = 64;
    const reference: usize = 256;
    try std.testing.expectError(error.ResourceLimitExceeded, domain.controller.tryAcquire(.gpu, .{ .host_limit_bytes = floor + reference - 1 }, .{ .host_scratch_bytes = floor + reference }, false));
    var lease = try domain.controller.tryAcquire(.gpu, .{ .host_limit_bytes = floor + reference }, .{ .host_scratch_bytes = floor + reference }, false);
    defer lease.release();
    var phase: Phase = .{ .slots = &.{}, .domain = domain.pin(), .reference_host_bytes = reference, .reference_host_floor = floor };
    defer phase.domain.?.release();
    try std.testing.expectError(error.MissingReferenceHostAllowance, phase.requireReferenceHostAllowance(reference));
    phase.reference_host_lease = &lease;
    try phase.requireReferenceHostAllowance(reference);
    try std.testing.expectEqual(@as(usize, floor + reference), domain.controller.snapshot().host_scratch_bytes);
    var peer = try foreign.controller.tryAcquire(.gpu, .{}, .{ .host_scratch_bytes = floor + reference }, false);
    defer peer.release();
    phase.reference_host_lease = &peer;
    try std.testing.expectError(error.ForeignWorkspaceAdmission, phase.requireReferenceHostAllowance(reference));
    phase.reference_host_lease = &lease;
    var split = try lease.preparePartition(std.testing.allocator, .{ .host_scratch_bytes = 1 });
    var child = split.commit();
    defer child.release();
    try std.testing.expectError(error.InsufficientWorkspaceAdmission, phase.requireReferenceHostAllowance(reference));
    // The real source, child and all six original resource classes stay owned.
    try std.testing.expectEqual(@as(usize, floor + reference), domain.controller.snapshot().host_scratch_bytes);
    phase.unbind();
    try std.testing.expect(phase.reference_host_lease == null and !phase.host_reference_prepared);
}
