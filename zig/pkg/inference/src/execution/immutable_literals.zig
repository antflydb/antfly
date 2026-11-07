// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable literal banks shared by physical route variants of one program.
//! Mutable frames borrow these owners; published values use independent slots.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const buffers = @import("../graph/buffer_plan.zig");
const admission = @import("storage_admission.zig");
const accounting = @import("storage_accounting.zig");
const CT = ops.CT;

pub const Literal = struct {
    node: ml.graph.NodeId,
    shape: ml.graph.Shape,
    data: []const f32,
    allocation: buffers.AllocationId,
    backend_index: usize,
    cold_bound: usize,
    staging_bound: usize = 0,
    consumers: []const ml.graph.NodeId = &.{},
};
pub const Publication = struct { node: ml.graph.NodeId, literal: usize, allocation: buffers.AllocationId, shape: ml.graph.Shape };
const Owner = struct { lease: ?*ops.physical_storage.Lease = null, value: ?CT = null, failure: ?anyerror = null, published_epoch: u64 = 0 };

pub const SharedLiteralStore = struct {
    prepared_backings: std.atomic.Value(bool) = .init(false),
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    /// SDK initialization/admission remains serialized, but owns no mutex
    /// across callbacks. Competing metadata selection declines before reserve.
    initializing: bool = false,
    epoch: u64 = 0,
    cached_claim: ?struct { identity: admission.Identity, epoch: u64, totals: ClaimTotals } = null,
    literals: []Literal,
    node_indices: []u32,
    publications: []?Publication,
    consumers: []ml.graph.NodeId,
    owners: []Owner,
    backends: []ops.ComputeBackend,
    completions: []?ops.physical_storage.Completion,

    /// Borrow immutable graph bytes: the semantic program must outlive every
    /// executable/route invocation. Materialized device owners retain no IR.
    pub fn create(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, plan: *buffers.BufferPlan, backends: []const ops.ComputeBackend) !?*SharedLiteralStore {
        var count: usize = 0;
        for (graph.nodes.items, 0..) |_, node| if (eligible(graph, plan, backends, @intCast(node)) != null) {
            count += 1;
        };
        if (count == 0) return null;
        const literals = try allocator.alloc(Literal, count);
        errdefer allocator.free(literals);
        const indices = try allocator.alloc(u32, graph.nodeCount());
        errdefer allocator.free(indices);
        @memset(indices, ml.graph.null_node);
        const owners = try allocator.alloc(Owner, count);
        errdefer allocator.free(owners);
        @memset(owners, .{});
        const publications = try allocator.alloc(?Publication, graph.nodeCount());
        errdefer allocator.free(publications);
        @memset(publications, null);
        const owned_backends = try allocator.dupe(ops.ComputeBackend, backends);
        for (owned_backends) |*backend| backend.* = backend.withoutSynchronousBorrows();
        errdefer allocator.free(owned_backends);
        const completions = try allocator.alloc(?ops.physical_storage.Completion, backends.len);
        errdefer allocator.free(completions);
        @memset(completions, null);
        var index: usize = 0;
        for (graph.nodes.items, 0..) |node, node_id| {
            const backend = eligible(graph, plan, backends, @intCast(node_id)) orelse continue;
            const attrs = node.op.constant;
            const bytes = try std.math.mul(usize, attrs.data_len, @sizeOf(f32));
            const bound = try ops.physical_storage.reservationBytes(backends[backend], .{ .byte_size = bytes });
            if (bound < bytes) return error.InvalidPhysicalStorageReservation;
            var staging_bound: usize = 0;
            if (backends[backend].kind() == .metal) {
                const functions = backends[backend].vtable.kernels.?;
                if (functions.physicalLiteralInitializer) |query| if (try query(backends[backend].ptr, bytes)) |initializer| {
                    if (initializer.staging.byte_size != bytes or initializer.staging.usage != .host_input or initializer.staging.admitted)
                        return error.InvalidPhysicalLiteralInitialization;
                    staging_bound = try ops.physical_storage.reservationBytes(backends[backend], initializer.staging);
                };
            }
            literals[index] = .{ .node = @intCast(node_id), .shape = node.output_shape, .data = graph.constantData(attrs.data_offset, attrs.data_len), .allocation = try plan.addConstantBinding(backends[backend].kind(), bytes), .backend_index = backend, .cold_bound = bound, .staging_bound = staging_bound };
            indices[node_id] = @intCast(index);
            index += 1;
        }
        const roots = try allocator.alloc(u32, plan.slots.len);
        defer allocator.free(roots);
        @memset(roots, ml.graph.null_node);
        for (plan.slots, 0..) |slot, slot_index| {
            if (slot.node_id < indices.len and indices[slot.node_id] != ml.graph.null_node) roots[slot_index] = indices[slot.node_id] else if (slot.source_slot) |source| {
                if (source >= slot_index) return error.InvalidAliasSource;
                roots[slot_index] = roots[source];
            }
        }
        const Consumer = struct { literal: usize, node: ml.graph.NodeId };
        var uses: std.ArrayListUnmanaged(Consumer) = .empty;
        defer uses.deinit(allocator);
        const use_counts = try allocator.alloc(usize, count);
        defer allocator.free(use_counts);
        @memset(use_counts, 0);
        for (graph.nodes.items, 0..) |node, node_id| {
            const output_slot = plan.slotForNode(@intCast(node_id));
            const output_literal = if (output_slot) |slot| roots[slot.id] else ml.graph.null_node;
            var seen: [4]u32 = @splat(ml.graph.null_node);
            var seen_count: usize = 0;
            for (node.getInputs()) |input| {
                const source_slot = plan.slotForNode(input) orelse continue;
                const literal = roots[source_slot.id];
                if (literal == ml.graph.null_node or literal == output_literal) continue;
                var duplicate = false;
                for (seen[0..seen_count]) |prior| if (prior == literal) {
                    duplicate = true;
                    break;
                };
                if (duplicate) continue;
                seen[seen_count] = literal;
                seen_count += 1;
                try uses.append(allocator, .{ .literal = literal, .node = @intCast(node_id) });
                use_counts[literal] += 1;
            }
        }
        const consumers = try allocator.alloc(ml.graph.NodeId, uses.items.len);
        errdefer allocator.free(consumers);
        var first: usize = 0;
        for (literals, use_counts) |*literal, *cursor| {
            const next = first + cursor.*;
            literal.consumers = consumers[first..next];
            cursor.* = first;
            first = next;
        }
        for (uses.items) |use| {
            consumers[use_counts[use.literal]] = use.node;
            use_counts[use.literal] += 1;
        }
        // Resolve aliases using the authoritative buffer-plan source links.
        // Publication copies raw physical layout and preserves view metadata;
        // partial/broadcast views cannot expose the shared literal owner.
        for (graph.outputs.items) |output| {
            var slot = plan.slotForNode(output) orelse continue;
            var remaining = plan.slots.len;
            while (slot.source_slot) |source| {
                if (remaining == 0 or source >= plan.slots.len) return error.InvalidAliasSource;
                remaining -= 1;
                slot = &plan.slots[source];
            }
            const literal = indices[slot.node_id];
            if (literal == ml.graph.null_node) continue;
            const item = literals[literal];
            const bytes = try std.math.mul(usize, item.data.len, @sizeOf(f32));
            const id = try plan.addWorkspace(backends[item.backend_index].kind(), bytes, @alignOf(f32));
            plan.allocations[id].kind = .tensor;
            plan.allocations[id].reusable = false;
            publications[output] = .{ .node = output, .literal = literal, .allocation = id, .shape = graph.node(output).output_shape };
        }
        const self = try allocator.create(SharedLiteralStore);
        self.* = .{ .allocator = allocator, .literals = literals, .node_indices = indices, .publications = publications, .consumers = consumers, .owners = owners, .backends = owned_backends, .completions = completions };
        return self;
    }

    fn eligible(graph: *const ml.graph.Graph, plan: *const buffers.BufferPlan, backends: []const ops.ComputeBackend, node_id: ml.graph.NodeId) ?usize {
        const node = graph.node(node_id);
        if (node.op != .constant or node.output_shape.dtype != .f32) return null;
        const elements = node.output_shape.numElements() orelse return null;
        if (elements <= 0 or elements > std.math.maxInt(i32) or elements != node.op.constant.data_len) return null;
        const slot = plan.slotForNode(node_id) orelse return null;
        for (backends, 0..) |backend, index| {
            if (backend.kind() != slot.backend) continue;
            const functions = backend.vtable.kernels orelse continue;
            if (!functions.accepts(backend.vtable)) continue;
            if (functions.physicalTensorView != null and functions.writePhysicalFloat32 != null and functions.copyPhysicalInput != null) return index;
        }
        return null;
    }
    pub fn retain(self: *SharedLiteralStore) void {
        _ = self.references.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *SharedLiteralStore) void {
        self.releaseWithCleanup(.{});
    }
    pub fn releaseWithCleanup(self: *SharedLiteralStore, cleanup: @import("cleanup_context.zig").Context) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        for (self.completions) |maybe| if (maybe) |completion| {
            completion.wait() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            completion.deinit();
        };
        for (self.owners, self.literals) |owner, literal| {
            if (owner.value) |tensor| self.backends[literal.backend_index].freeWithCleanup(tensor, cleanup);
            if (owner.lease) |lease| lease.releaseWithCleanup(cleanup);
        }
        const allocator = self.allocator;
        allocator.free(self.completions);
        allocator.free(self.backends);
        allocator.free(self.owners);
        allocator.free(self.publications);
        allocator.free(self.consumers);
        allocator.free(self.node_indices);
        allocator.free(self.literals);
        allocator.destroy(self);
    }

    pub const AdmissionSpan = struct { backend: @import("../graph/backend_contracts.zig").BackendKind, bytes: usize, staging_bytes: usize = 0 };
    /// Short metadata snapshot only. Caller holds the selected store's strong
    /// immutable claim; no SDK, allocator or admission callback runs locked.
    pub fn unadmittedSpan(self: *SharedLiteralStore, index: usize, identity: admission.Identity) !?AdmissionSpan {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.initializing) return error.QueueFull;
        if (index >= self.literals.len or index >= self.owners.len) return error.InvalidImmutableStorageClaim;
        const literal = self.literals[index];
        const owner = self.owners[index];
        if (owner.failure) |failure| return failure;
        if (owner.lease) |lease| {
            if (lease.admission) |reserved| {
                if (!reserved.identity.eql(identity)) return error.PhysicalStorageAdmissionMismatch;
                return null;
            }
            const bytes = lease.backing_byte_size orelse lease.byte_size;
            if (bytes > literal.cold_bound) return error.InvalidPhysicalStorageReservation;
            return .{ .backend = self.backends[literal.backend_index].kind(), .bytes = bytes };
        }
        return .{ .backend = self.backends[literal.backend_index].kind(), .bytes = literal.cold_bound, .staging_bytes = literal.staging_bound };
    }

    pub const ClaimTotals = struct {
        credit: @import("../runtime/tier/memory.zig").AdmissionAmounts = .{},
        capacity_reduction: @import("../runtime/tier/memory.zig").AdmissionAmounts = .{},
    };
    /// A cold, immutable mask owner shared by the route variant and its
    /// allocation-free claims. Its own store pin survives variant eviction.
    pub const LiteralSelection = struct {
        allocator: std.mem.Allocator,
        references: std.atomic.Value(usize) = .init(1),
        store: *SharedLiteralStore,
        enabled: []const bool,
        cached_claim: ?struct { identity: admission.Identity, epoch: u64, totals: ClaimTotals } = null,
        pub fn create(allocator: std.mem.Allocator, store: *SharedLiteralStore, enabled: []const bool) !*LiteralSelection {
            if (enabled.len != store.literals.len) return error.InvalidLiteralSelection;
            const owned = try allocator.dupe(bool, enabled);
            errdefer allocator.free(owned);
            const self = try allocator.create(LiteralSelection);
            store.retain();
            self.* = .{ .allocator = allocator, .store = store, .enabled = owned };
            return self;
        }
        pub fn retain(self: *LiteralSelection) void {
            _ = self.references.fetchAdd(1, .monotonic);
        }
        pub fn release(self: *LiteralSelection) void {
            self.releaseWithCleanup(.{});
        }
        pub fn releaseWithCleanup(self: *LiteralSelection, cleanup: @import("cleanup_context.zig").Context) void {
            if (self.references.fetchSub(1, .acq_rel) != 1) return;
            const allocator = self.allocator;
            self.store.releaseWithCleanup(cleanup);
            allocator.free(self.enabled);
            allocator.destroy(self);
        }
    };
    /// An independent store pin lends immutable owner reservations even when
    /// every mutable frame is busy. The invocation also pins backend lifetime.
    pub const ImmutableClaim = struct {
        store: ?*SharedLiteralStore,
        identity: admission.Identity,
        epoch: u64,
        totals: ClaimTotals,
        selection: ?*LiteralSelection = null,
        pub fn release(self: *ImmutableClaim) void {
            self.releaseWithCleanup(.{});
        }
        pub fn releaseWithCleanup(self: *ImmutableClaim, cleanup: @import("cleanup_context.zig").Context) void {
            const store = self.store orelse return;
            self.store = null;
            if (self.selection) |selection| selection.releaseWithCleanup(cleanup);
            self.selection = null;
            store.releaseWithCleanup(cleanup);
        }
        pub fn validate(self: *const ImmutableClaim, store: *SharedLiteralStore, capability: ?admission.Capability) !void {
            if (self.store != store) return error.InvalidImmutableStorageClaim;
            const cap = capability orelse return error.PhysicalStorageAdmissionMismatch;
            if (!cap.identity.eql(self.identity)) return error.PhysicalStorageAdmissionMismatch;
            @import("antfly_platform").sync.lockYielding(&store.mutex);
            defer store.mutex.unlock();
            if (self.selection) |selection| if (selection.store != store) return error.InvalidLiteralSelection;
            const totals = try store.cachedTotals(self.identity, self.selection);
            if (totals.credit.host_scratch_bytes < self.totals.credit.host_scratch_bytes or totals.credit.backend_scratch_bytes < self.totals.credit.backend_scratch_bytes or totals.capacity_reduction.host_scratch_bytes < self.totals.capacity_reduction.host_scratch_bytes or totals.capacity_reduction.backend_scratch_bytes < self.totals.capacity_reduction.backend_scratch_bytes) return error.InvalidImmutableStorageClaim;
            // Owners never replace published allocations or shrink their live
            // charge. Newly initialized banks can only reduce cold demand.
        }
    };
    pub fn claimImmutable(self: *SharedLiteralStore, identity: admission.Identity) !ImmutableClaim {
        return self.claimImmutableFor(identity, null);
    }
    pub fn claimImmutableFor(self: *SharedLiteralStore, identity: admission.Identity, selection: ?*LiteralSelection) !ImmutableClaim {
        if (selection) |selected| if (selected.store != self) return error.InvalidLiteralSelection;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const totals = try self.cachedTotals(identity, selection);
        self.retain();
        if (selection) |selected| selected.retain();
        return .{ .store = self, .identity = identity, .epoch = self.epoch, .totals = totals, .selection = selection };
    }
    fn cachedTotals(self: *SharedLiteralStore, identity: admission.Identity, selection: ?*LiteralSelection) !ClaimTotals {
        if (self.initializing) return error.QueueFull;
        // Selection objects own immutable mask contents, not borrowed slice
        // addresses. Separate selections can never reuse each other's cache.
        if (selection) |selected| {
            if (selected.cached_claim) |cached| if (cached.epoch == self.epoch and cached.identity.eql(identity)) return cached.totals;
            const totals = try self.claimTotals(identity, selected.enabled);
            selected.cached_claim = .{ .identity = identity, .epoch = self.epoch, .totals = totals };
            return totals;
        }
        if (self.cached_claim) |cached| if (cached.epoch == self.epoch and cached.identity.eql(identity)) return cached.totals;
        const totals = try self.claimTotals(identity, null);
        self.cached_claim = .{ .identity = identity, .epoch = self.epoch, .totals = totals };
        return totals;
    }
    fn claimTotals(self: *SharedLiteralStore, identity: admission.Identity, enabled: ?[]const bool) !ClaimTotals {
        var result: ClaimTotals = .{};
        for (self.owners, self.literals, 0..) |owner, literal, index| {
            if (enabled) |mask| if (!mask[index]) continue;
            if (owner.failure) |failure| return failure;
            const lease = owner.lease orelse continue;
            const bytes = lease.backing_byte_size orelse lease.byte_size;
            if (bytes > literal.cold_bound) return error.InvalidPhysicalStorageReservation;
            const reduction = try std.math.add(usize, literal.cold_bound - bytes, literal.staging_bound);
            const host = self.backends[literal.backend_index].kind() == .native;
            if (host) result.capacity_reduction.host_scratch_bytes = try std.math.add(usize, result.capacity_reduction.host_scratch_bytes, reduction) else result.capacity_reduction.backend_scratch_bytes = try std.math.add(usize, result.capacity_reduction.backend_scratch_bytes, reduction);
            if (lease.admission) |reserved| {
                if (!reserved.identity.eql(identity)) return error.PhysicalStorageAdmissionMismatch;
                const charge = @min(bytes, lease.admittedBytes());
                if (host) result.credit.host_scratch_bytes = try std.math.add(usize, result.credit.host_scratch_bytes, charge) else result.credit.backend_scratch_bytes = try std.math.add(usize, result.credit.backend_scratch_bytes, charge);
            }
        }
        return result;
    }

    pub const Value = struct { value: CT, lease: *ops.physical_storage.Lease, fresh: bool };
    pub fn value(self: *SharedLiteralStore, index: usize, capability: ?admission.Capability, ledger: ?*accounting.Ledger) !Value {
        const construction_cleanup: @import("cleanup_context.zig").Context = .{ .boundary = if (capability) |cap| cap.constructionEpoch() else null };
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        if (self.initializing) {
            self.mutex.unlock();
            return error.QueueFull;
        }
        const literal = self.literals[index];
        const cb = self.backends[literal.backend_index];
        const functions = cb.vtable.kernels.?;
        const owner = &self.owners[index];
        if (owner.failure) |failure| {
            self.mutex.unlock();
            return failure;
        }
        if (owner.lease) |lease| {
            if (lease.isQuarantined()) {
                self.mutex.unlock();
                return error.PhysicalStorageAdmissionQuarantined;
            }
            // Existing admitted or raw immutable values need no callback.
            if (capability == null or lease.admission != null) {
                if (capability) |cap| if (!lease.admission.?.identity.eql(cap.identity)) {
                    self.mutex.unlock();
                    return error.PhysicalStorageAdmissionMismatch;
                };
                const result = Value{ .value = owner.value.?, .lease = lease, .fresh = false };
                self.mutex.unlock();
                return result;
            }
        }
        if (capability) |cap| if (cap.services.takeBacking != null and construction_cleanup.boundary == null) {
            self.mutex.unlock();
            return error.MissingCurrentCleanupEpoch;
        };
        const next_epoch = std.math.add(u64, self.epoch, 1) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.initializing = true;
        self.retain();
        self.mutex.unlock();
        defer {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            self.initializing = false;
            self.mutex.unlock();
            self.releaseWithCleanup(construction_cleanup);
        }
        if (owner.lease) |lease| {
            // This mutation is protected by the initialization claim; every
            // metadata snapshot declines until publication completes.
            lease.ensureAdmission(capability.?) catch |err| {
                // Cleanup-only quarantine needs actual teardown B, but earns
                // no ready admission, immutable credit or published epoch.
                if (lease.retirement_owner != null) self.prepared_backings.store(true, .release);
                return err;
            };
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            if (lease.retirement_owner != null) self.prepared_backings.store(true, .release);
            self.epoch = next_epoch;
            self.cached_claim = null;
            const result = Value{ .value = owner.value.?, .lease = lease, .fresh = false };
            self.mutex.unlock();
            return result;
        }
        // Refresh construction capability before any SDK allocation. An active
        // frame or changed staging policy cannot silently use legacy staging.
        const literal_bytes = try std.math.mul(usize, literal.data.len, @sizeOf(f32));
        const initializer: ?ops.physical_storage.LiteralInitializer = if (literal.staging_bound != 0) blk: {
            const binding_supported = functions.physicalLiteralBindingSupported orelse return error.UnsupportedPhysicalLiteralInitialization;
            if (!binding_supported(cb.ptr)) return error.UnsupportedPhysicalLiteralInitialization;
            const query = functions.physicalLiteralInitializer orelse return error.UnsupportedPhysicalLiteralInitialization;
            const current = (try query(cb.ptr, literal_bytes)) orelse return error.UnsupportedPhysicalLiteralInitialization;
            if (current.staging.byte_size != literal_bytes or current.staging.usage != .host_input or current.staging.admitted or
                try ops.physical_storage.reservationBytes(cb, current.staging) != literal.staging_bound)
                return error.InvalidPhysicalLiteralInitialization;
            break :blk current;
        } else null;
        if (self.completions[literal.backend_index] == null) {
            if (functions.createPhysicalCompletion) |make| self.completions[literal.backend_index] = try make(cb.ptr);
        }
        if (self.completions[literal.backend_index]) |completion| try completion.beforeReuse();
        const lease = (try ops.physical_storage.Lease.createAdmitted(self.allocator, cb, try std.math.mul(usize, literal.data.len, 4), ledger, .constant, capability)) orelse return error.PhysicalStorageAllocationDeclined;
        var published = false;
        errdefer if (!published) lease.releaseWithCleanup(construction_cleanup);
        var dimensions: [8]i32 = undefined;
        for (literal.shape.dims[0..literal.shape.rank()], 0..) |dimension, axis| dimensions[axis] = @intCast(dimension);
        const tensor = (try functions.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = dimensions[0..literal.shape.rank()] })) orelse return error.UnsupportedPhysicalTensorView;
        errdefer if (!published) cb.freeWithCleanup(tensor, construction_cleanup);
        var failure: ?anyerror = null;
        if (initializer) |initialize| {
            const stage = (try ops.physical_storage.Lease.createAdmittedWithUsage(self.allocator, cb, initialize.staging.byte_size, ledger, .activation, capability, .host_input)) orelse return error.PhysicalStorageAllocationDeclined;
            defer stage.releaseWithCleanup(construction_cleanup);
            initialize.initialize(cb.ptr, lease, stage, literal.data) catch |err| {
                failure = err;
            };
        } else functions.writePhysicalFloat32.?(cb.ptr, tensor, literal.data) catch |err| {
            failure = err;
        };
        // Even a failed upload can have queued work. Record its attempted
        // stream; a failed record retains the backend's checked drain proof.
        if (self.completions[literal.backend_index]) |completion| completion.record() catch |err| {
            if (failure == null) failure = err;
        };
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.epoch = next_epoch;
        owner.* = .{ .lease = lease, .value = tensor, .failure = failure, .published_epoch = self.epoch };
        if (lease.retirement_owner != null) self.prepared_backings.store(true, .release);
        self.cached_claim = null;
        published = true;
        self.mutex.unlock();
        // A failed upload/fence quarantines the exact backing and quota until
        // checked completion during owner destruction; it is never retried.
        if (failure) |err| return err;
        return .{ .value = tensor, .lease = lease, .fresh = true };
    }
};

const LiteralCallbackProbe = struct {
    const native = @import("../ops/native_compute.zig");
    const memory = @import("../runtime/tier/memory.zig");
    compute: native.NativeCompute,
    store: *SharedLiteralStore = undefined,
    controller: *memory.AdmissionController,
    source: *memory.AdmissionLease,
    credit: memory.AdmissionAmounts = .{ .host_scratch_bytes = 16 },
    fail_upload: bool,
    fail_view_metadata: bool = false,
    view_failures: usize = 0,
    callbacks: usize = 0,
    writes: usize = 0,
    records: usize = 0,
    fn fromCompute(raw: *anyopaque) *@This() {
        const compute: *native.NativeCompute = @ptrCast(@alignCast(raw));
        return @fieldParentPtr("compute", compute);
    }
    fn outsideMutex(self: *@This()) !void {
        if (!self.store.mutex.tryLock()) return error.LiteralCallbackUnderMutex;
        self.store.mutex.unlock();
        self.callbacks += 1;
        // Reentry cannot observe a half-admitted/uploaded literal or issue a
        // second upload; it declines synchronously without waiting on itself.
        try std.testing.expectError(error.QueueFull, self.store.value(0, null, null));
        try std.testing.expectError(error.QueueFull, self.store.claimImmutable(.{ .controller = self.controller, .backend_class = .cpu }));
    }
    fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.outsideMutex();
        return admission.fromRunLease(allocator, self.source, .{ .controller = self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 16 }, .check_live_memory = false, .credit = &self.credit }, kind, class, bytes);
    }
    fn completion(raw: *anyopaque) !ops.physical_storage.Completion {
        const self = fromCompute(raw);
        try self.outsideMutex();
        return .{ .ptr = self, .vtable = &completion_table };
    }
    fn beforeReuse(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.outsideMutex();
    }
    fn record(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.outsideMutex();
        self.records += 1;
    }
    fn deinitCompletion(_: *anyopaque) void {}
    fn wait(_: *anyopaque) !void {}
    const completion_table = ops.physical_storage.Completion.VTable{ .record = record, .before_reuse = beforeReuse, .wait = wait, .deinit = deinitCompletion };
    fn write(raw: *anyopaque, value: CT, input: []const f32) !void {
        const self = fromCompute(raw);
        try self.outsideMutex();
        self.writes += 1;
        try native.vtable_impl.kernels.?.writePhysicalFloat32.?(raw, value, input);
        if (self.fail_upload) return error.InjectedLiteralUploadFailure;
    }
    fn allocate(raw: *anyopaque, request: ops.physical_storage.Request) !?CT {
        try fromCompute(raw).outsideMutex();
        return native.vtable_impl.kernels.?.physical_storage_policy.?.allocate(raw, request);
    }
    fn view(raw: *anyopaque, request: ops.physical_storage.View) !?CT {
        const self = fromCompute(raw);
        try self.outsideMutex();
        if (self.fail_view_metadata) {
            // The actual prepared bank already exists. Fail the real Native
            // view-header allocation, not a synthetic producer result.
            var failing = std.testing.FailingAllocator.init(self.compute.allocator, .{ .fail_index = 0 });
            const original = self.compute.allocator;
            self.compute.allocator = failing.allocator();
            defer self.compute.allocator = original;
            self.view_failures += 1;
            return native.vtable_impl.kernels.?.physicalTensorView.?(raw, request);
        }
        return native.vtable_impl.kernels.?.physicalTensorView.?(raw, request);
    }
    const physical_policy: ops.physical_storage.Policy = .{ .allocate = allocate };
    const kernels: ops.KernelBackend = blk: {
        var result = native.vtable_impl.kernels.?.*;
        result.implementation_table = null;
        result.acceptsImplementation = null;
        result.createPhysicalCompletion = completion;
        result.physical_storage_policy = &physical_policy;
        result.physicalTensorView = view;
        result.writePhysicalFloat32 = write;
        break :blk result;
    };
    fn acceptsCleanup(value: *const ops.ComputeBackend.VTable) bool {
        return value == &table;
    }
    fn freeWithCleanup(raw: *anyopaque, value: CT, current: @import("cleanup_context.zig").Context) void {
        native.vtable_impl.cold_cleanup_services.?.free(raw, value, current);
    }
    const cleanup_services = ops.ColdCleanupServices{ .accepts = acceptsCleanup, .free = freeWithCleanup };
    const table: ops.ComputeBackend.VTable = blk: {
        var result = native.vtable_impl;
        result.kernels = &kernels;
        result.cold_cleanup_services = &cleanup_services;
        break :blk result;
    };
};

fn literalCallbackCase(allocator: std.mem.Allocator, comptime fail_upload: bool) !void {
    const native = LiteralCallbackProbe.native;
    const memory = LiteralCallbackProbe.memory;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var source = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
    defer source.release();
    {
        var weights = native.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
        defer weights.resident_weights.deinit(allocator);
        defer weights.lazy_weights.deinit(allocator);
        var probe = LiteralCallbackProbe{ .compute = native.NativeCompute.init(allocator, &weights, null), .controller = &controller, .source = &source, .fail_upload = fail_upload };
        defer probe.compute.deinit();
        const backend = ops.ComputeBackend{ .ptr = &probe.compute, .vtable = &LiteralCallbackProbe.table };
        var owners = [_]Owner{.{}};
        const data = [_]f32{ 0.5, 1, -3, 7 };
        var literals = [_]Literal{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}), .data = &data, .allocation = 0, .backend_index = 0, .cold_bound = 16 }};
        var backends = [_]ops.ComputeBackend{backend};
        var completions = [_]?ops.physical_storage.Completion{null};
        // Stack fixture owns its arrays; value's temporary store pin must
        // return to exactly one without running heap-owner destruction.
        var store = SharedLiteralStore{ .allocator = allocator, .literals = &literals, .node_indices = &.{}, .publications = &.{}, .consumers = &.{}, .owners = &owners, .backends = &backends, .completions = &completions };
        probe.store = &store;
        defer {
            for (completions) |maybe| if (maybe) |item| {
                item.wait() catch @panic("literal test completion failed");
                item.deinit();
            };
            if (owners[0].value) |value| backend.free(value);
            if (owners[0].lease) |lease| lease.release();
        }
        const capability = admission.Capability{ .identity = .{ .controller = &controller, .backend_class = .cpu }, .context = &probe, .services = &.{ .reserve = LiteralCallbackProbe.reserve } };
        if (fail_upload) {
            try std.testing.expectError(error.InjectedLiteralUploadFailure, store.value(0, capability, null));
            const callbacks = probe.callbacks;
            try std.testing.expectError(error.InjectedLiteralUploadFailure, store.value(0, capability, null));
            try std.testing.expectEqual(callbacks, probe.callbacks);
            try std.testing.expect(owners[0].lease != null and owners[0].value != null);
        } else {
            const first = try store.value(0, capability, null);
            try std.testing.expect(first.fresh);
            const values = try backend.toFloat32(first.value, allocator);
            defer allocator.free(values);
            try std.testing.expectEqualSlices(f32, &data, values);
            const callbacks = probe.callbacks;
            const second = try store.value(0, capability, null);
            try std.testing.expect(!second.fresh and second.lease == first.lease and second.value == first.value);
            try std.testing.expectEqual(callbacks, probe.callbacks);
            var claim = try store.claimImmutable(capability.identity);
            defer claim.release();
            try std.testing.expectEqual(@as(usize, 16), claim.totals.credit.host_scratch_bytes);
        }
        try std.testing.expect(!store.initializing);
        try std.testing.expectEqual(@as(usize, 1), store.references.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), probe.writes);
        // A failed upload still records its attempted work before quarantine.
        try std.testing.expectEqual(@as(usize, 1), probe.records);
    }
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
fn literalCallbackOomCase(allocator: std.mem.Allocator) !void {
    try literalCallbackCase(allocator, false);
}

test "immutable literal SDK and admission callbacks reenter unlocked without half publication or duplicate upload" {
    try literalCallbackCase(std.testing.allocator, false);
    try literalCallbackCase(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, literalCallbackOomCase, .{});
}

fn preparedLiteralConstructionRollbackCase(comptime quarantine_upload: bool) !void {
    const a = std.testing.allocator;
    const Memory = @import("../runtime/tier/memory.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig");
    const Sink = @import("retirement_sink.zig");
    const Prepared = @import("prepared_storage_admission.zig").Prepared;
    const Native = @import("../ops/native_compute.zig");
    const domain = try Domain.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    const Source = struct {
        pin: *const Domain.Pin,
        run: Memory.AdmissionLease = undefined,
        credit: Memory.AdmissionAmounts = .{ .host_scratch_bytes = 16 },
        gate: std.atomic.Mutex = .unlocked,
        violation: bool = false,
        next: usize = 0,
        fn observe(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
        }
        fn externalReserve(raw: *anyopaque, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.next += 1;
            return self.next;
        }
        fn externalRetain(raw: *anyopaque, _: usize, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn externalRelease(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            return admission.fromRunLease(allocator, &self.run, .{ .controller = self.pin.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64 }, .check_live_memory = false, .credit = &self.credit, .lifetime = admission.Lifetime.forAdmissionDomain(self.pin.*) }, kind, class, bytes);
        }
    };
    var source = Source{ .pin = &pin };
    try pin.controller().configureResourceBudget(.{ .context = &source, .try_reserve = Source.externalReserve, .retain = Source.externalRetain, .release = Source.externalRelease });
    defer pin.controller().configureResourceBudget(null) catch @panic("prepared literal quota leak");
    var weights = Native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.resident_weights.deinit(a);
    defer weights.lazy_weights.deinit(a);
    var probe = LiteralCallbackProbe{ .compute = Native.NativeCompute.init(a, &weights, null), .controller = pin.controller(), .source = &source.run, .fail_upload = quarantine_upload };
    defer probe.compute.deinit();
    const backend = ops.ComputeBackend{ .ptr = &probe.compute, .vtable = &LiteralCallbackProbe.table };
    var owners = [_]Owner{.{}};
    const values = [_]f32{ 0.5, 1, -3, 7 };
    var literals = [_]Literal{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}), .data = &values, .allocation = 0, .backend_index = 0, .cold_bound = 16 }};
    var backends = [_]ops.ComputeBackend{backend};
    var completions = [_]?ops.physical_storage.Completion{null};
    var store = SharedLiteralStore{ .allocator = a, .literals = &literals, .node_indices = &.{}, .publications = &.{}, .consumers = &.{}, .owners = &owners, .backends = &backends, .completions = &completions };
    probe.store = &store;
    var sink = try Sink.Ref.create();
    defer sink.release();
    defer {
        // Actual test-owned cleanup gate/B, including early assertion errors.
        var guard = sink.enter() catch @panic("literal cleanup guard");
        var epoch = guard.retainEpoch() catch @panic("literal cleanup epoch");
        @import("antfly_platform").sync.lockYielding(&source.gate);
        if (owners[0].value) |value| backend.freeWithBoundary(value, &epoch);
        if (owners[0].lease) |lease| lease.releaseWithBoundary(&epoch);
        source.gate.unlock();
        guard.finish();
        epoch.release();
        for (completions) |maybe| if (maybe) |completion| {
            completion.wait() catch @panic("literal completion");
            completion.deinit();
        };
    }
    const ordinary = admission.Capability{ .identity = .{ .controller = pin.controller(), .backend_class = .cpu }, .context = &source, .services = &.{ .reserve = Source.reserve } };
    const Missing = struct {
        fn reserve(_: *anyopaque, _: std.mem.Allocator, _: @import("../graph/backend_contracts.zig").BackendKind, _: accounting.Class, _: usize) !admission.Reservation {
            return error.UnexpectedUnpreparedProducer;
        }
        fn take(_: *anyopaque, _: std.mem.Allocator, _: @import("../graph/backend_contracts.zig").BackendKind, _: accounting.Class, _: usize) !admission.BackingReservation {
            return error.UnexpectedUnpreparedProducer;
        }
    };
    const missing = admission.Capability{ .identity = ordinary.identity, .context = &source, .services = &.{ .reserve = Missing.reserve, .takeBacking = Missing.take } };
    try std.testing.expectError(error.MissingCurrentCleanupEpoch, store.value(0, missing, null));
    try std.testing.expect(!store.initializing and !store.prepared_backings.load(.acquire));
    try std.testing.expect(owners[0].lease == null and owners[0].value == null and completions[0] == null);
    try std.testing.expectEqual(@as(usize, 0), probe.callbacks);
    try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
    for (0..2) |attempt| {
        source.credit = .{ .host_scratch_bytes = 16 };
        source.run = try pin.controller().tryAcquire(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_scratch_bytes = 16 }, false);
        defer source.run.release();
        const prepared = (try Prepared.prepare(ordinary, &.{.{ .backend = .native, .class = .constant, .bytes = 16 }})).?;
        var prepared_live = true;
        var bound = false;
        defer if (prepared_live and !bound) prepared.abortBeforeExecution() catch @panic("literal unused preparation");
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try prepared.bindConstructionEpoch(&epoch);
        bound = true;
        defer if (prepared_live) prepared.releaseAfterExecution(&epoch);
        const capability = Prepared.forDispatch(prepared, ordinary.identity);
        probe.fail_view_metadata = attempt == 0;
        @import("antfly_platform").sync.lockYielding(&source.gate);
        var gate_held = true;
        defer if (gate_held) source.gate.unlock();
        if (attempt == 0) {
            try std.testing.expectError(error.OutOfMemory, store.value(0, capability, null));
            try std.testing.expect(owners[0].lease == null and owners[0].value == null);
            try std.testing.expectEqual(@as(usize, 1), probe.view_failures);
        } else if (quarantine_upload) {
            try std.testing.expectError(error.InjectedLiteralUploadFailure, store.value(0, capability, null));
            try std.testing.expect(owners[0].lease != null and owners[0].value != null);
            const callbacks = probe.callbacks;
            try std.testing.expectError(error.InjectedLiteralUploadFailure, store.value(0, capability, null));
            try std.testing.expectEqual(callbacks, probe.callbacks);
        } else {
            const value = try store.value(0, capability, null);
            try std.testing.expect(value.fresh and value.lease.retirement_owner != null);
            const warm = try store.value(0, capability, null);
            try std.testing.expect(!warm.fresh and warm.lease == value.lease);
        }
        if (attempt == 1) {
            if (owners[0].value) |value| backend.freeWithBoundary(value, &epoch);
            owners[0].value = null;
            if (owners[0].lease) |lease| lease.releaseWithBoundary(&epoch);
            owners[0].lease = null;
        }
        prepared.releaseAfterExecution(&epoch);
        prepared_live = false;
        try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().host_scratch_bytes);
        try std.testing.expect(!store.initializing);
        try std.testing.expectEqual(@as(usize, 1), store.references.load(.acquire));
        source.gate.unlock();
        gate_held = false;
        guard.finish();
        try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
        try std.testing.expect(!source.violation);
    }
}

test "prepared immutable literal actual view metadata OOM rolls real backing under construction B then fresh retry or upload quarantine" {
    try preparedLiteralConstructionRollbackCase(false);
    try preparedLiteralConstructionRollbackCase(true);
}

fn preparedLiteralMetalAttachmentRollbackCase(comptime partial_payload: bool, comptime inject_failure: bool) !void {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Memory = @import("../runtime/tier/memory.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig");
    const Sink = @import("retirement_sink.zig");
    const Prepared = @import("prepared_storage_admission.zig").Prepared;
    const Retirement = @import("resource_retirement.zig");
    const Cleanup = @import("cleanup_context.zig").Context;
    const Metal = @import("../ops/metal_compute.zig").MetalCompute;
    const domain = try Domain.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const canonical = context.computeBackend();
    // This test wrapper preserves the actual Metal receiver and SDK callbacks.
    // Only the attachment return is faulted, once before or after real payload
    // publication. The latter is legal only with zero pending batch nodes.
    const Fault = struct {
        var original: *const ops.ComputeBackend.VTable = undefined;
        var table: *const ops.ComputeBackend.VTable = undefined;
        var origin: *const Retirement.Scope.VTable = undefined;
        var fault: bool = true;
        var calls: usize = 0;
        fn accepts(value: *const ops.ComputeBackend.VTable) bool {
            return value == table;
        }
        fn free(raw: *anyopaque, value: CT, current: Cleanup) void {
            original.cold_cleanup_services.?.free(raw, value, current);
        }
        fn begin(raw: *anyopaque, outputs: []const CT) !Retirement.ScopeSet {
            var result = try original.resource_retirement_services.?.begin(raw, outputs);
            errdefer result.end();
            if (result.count != 1) return error.UnexpectedPhysicalOriginCount;
            origin = result.scopes()[0].vtable;
            // No asynchronous batch node may be published by a failing attach.
            if (result.scopes()[0].pending_batches != 0) return error.UnexpectedPendingPhysicalBatch;
            result.scopes()[0].vtable = &scope_table;
            return result;
        }
        fn beginOrigin(raw: *anyopaque, outputs: []const CT) !Retirement.Scope.Begun {
            return origin.begin(raw, outputs);
        }
        fn attach(raw: *anyopaque, outputs: []const CT, nodes: []Retirement.Node, payload: Retirement.Payload) !void {
            return origin.attach(raw, outputs, nodes, payload);
        }
        fn attachPrepared(raw: *anyopaque, outputs: []const CT, nodes: []Retirement.Node, payload: Retirement.Payload, metadata: *Retirement.BackendPreparation) !void {
            calls += 1;
            try std.testing.expectEqual(@as(usize, 0), nodes.len);
            if (fault and !partial_payload) return error.InjectedPhysicalAttachmentFailure;
            try origin.attach_prepared.?(raw, outputs, nodes, payload, metadata);
            if (fault) return error.InjectedPhysicalAttachmentFailure;
        }
        fn end(raw: *anyopaque) void {
            origin.end(raw);
        }
        fn notify() callconv(.c) void {
            if (origin.notify) |callback| callback();
        }
        const scope_table = Retirement.Scope.VTable{ .begin = beginOrigin, .attach = attach, .attach_prepared = attachPrepared, .end = end, .notify = notify };
        const services = Retirement.Services{ .accepts = accepts, .begin = begin };
        const cleanup_services = ops.ColdCleanupServices{ .accepts = accepts, .free = free };
    };
    Fault.original = canonical.vtable;
    Fault.fault = inject_failure;
    Fault.calls = 0;
    var kernels = canonical.vtable.kernels.?.*;
    kernels.implementation_table = null;
    kernels.acceptsImplementation = null;
    var table = canonical.vtable.*;
    table.kernels = &kernels;
    table.resource_retirement_services = &Fault.services;
    table.cold_cleanup_services = &Fault.cleanup_services;
    Fault.table = &table;
    const backend = ops.ComputeBackend{ .ptr = canonical.ptr, .vtable = &table };
    var owners = [_]Owner{.{}};
    const data = [_]f32{ 0.5, 1, -3, 7 };
    const upper = try ops.physical_storage.reservationBytes(backend, .{ .byte_size = 16 });
    var literals = [_]Literal{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}), .data = &data, .allocation = 0, .backend_index = 0, .cold_bound = upper }};
    var backends = [_]ops.ComputeBackend{backend};
    var completions = [_]?ops.physical_storage.Completion{null};
    var store = SharedLiteralStore{ .allocator = a, .literals = &literals, .node_indices = &.{}, .publications = &.{}, .consumers = &.{}, .owners = &owners, .backends = &backends, .completions = &completions };
    var sink = try Sink.Ref.create();
    defer sink.release();
    // Registered before raw creation so every early failure gets real B cleanup.
    defer {
        var guard = sink.enter() catch @panic("Metal literal cleanup guard");
        var epoch = guard.retainEpoch() catch @panic("Metal literal cleanup epoch");
        if (owners[0].value) |value| backend.freeWithBoundary(value, &epoch);
        if (owners[0].lease) |lease| lease.releaseWithBoundary(&epoch);
        guard.finish();
        epoch.release();
        for (completions) |maybe| if (maybe) |completion| {
            completion.wait() catch @panic("Metal literal completion");
            completion.deinit();
        };
        Retirement.pollRetirementChargesCPU();
    }
    const raw = try store.value(0, null, null);
    try Metal.syncOutputTensor(&canonical, raw.value);
    // A real independent SDK alias, not another physical Lease retain.
    var alias = (try Metal.retainDenseDeviceTensor(raw.lease.tensor)).?;
    var alias_live = true;
    defer if (alias_live) alias.deinit();
    const bytes = raw.lease.backing_byte_size orelse raw.lease.byte_size;
    const Source = struct {
        pin: *const Domain.Pin,
        run: Memory.AdmissionLease = undefined,
        credit: Memory.AdmissionAmounts = .{},
        fn reserve(raw_source: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, count: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw_source));
            return admission.fromRunLease(allocator, &self.run, .{ .controller = self.pin.controller(), .backend_class = .gpu, .limits = .{}, .check_live_memory = false, .credit = &self.credit, .lifetime = admission.Lifetime.forAdmissionDomain(self.pin.*) }, kind, class, count);
        }
    };
    var source = Source{ .pin = &pin };
    const ordinary = admission.Capability{ .identity = .{ .controller = pin.controller(), .backend_class = .gpu }, .context = &source, .services = &.{ .reserve = Source.reserve } };
    for (0..2) |attempt| {
        source.credit = .{ .backend_scratch_bytes = bytes };
        source.run = try pin.controller().tryAcquire(.gpu, .{}, source.credit, false);
        defer source.run.release();
        const prepared = (try Prepared.prepare(ordinary, &.{.{ .backend = .metal, .class = .constant, .bytes = bytes }})).?;
        var prepared_live = true;
        var bound = false;
        defer if (prepared_live and !bound) prepared.abortBeforeExecution() catch @panic("Metal unused preparation");
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try prepared.bindConstructionEpoch(&epoch);
        bound = true;
        defer if (prepared_live) prepared.releaseAfterExecution(&epoch);
        const capability = Prepared.forDispatch(prepared, ordinary.identity);
        if (attempt == 0 and inject_failure) {
            try std.testing.expectError(error.InjectedPhysicalAttachmentFailure, store.value(0, capability, null));
            try std.testing.expect(raw.lease.isQuarantined());
            try std.testing.expect(!store.initializing and store.prepared_backings.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), raw.lease.admittedBytes());
            try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, raw.lease.admittedAmounts());
        } else if (inject_failure) {
            // A fresh capability cannot reuse a partially witnessed SDK bank.
            try std.testing.expectError(error.PhysicalStorageAdmissionQuarantined, store.value(0, capability, null));
        } else {
            const value = try store.value(0, capability, null);
            try std.testing.expect(value.lease == raw.lease and value.lease.admission != null);
            const warm = try store.value(0, Prepared.forDispatch(null, ordinary.identity), null);
            try std.testing.expect(warm.lease == value.lease and !warm.fresh);
        }
        if (inject_failure) {
            try std.testing.expectError(error.PhysicalStorageAdmissionQuarantined, store.value(0, null, null));
            try std.testing.expectError(error.PhysicalStorageAdmissionQuarantined, raw.lease.ensureAdmission(capability));
        }
        try std.testing.expectEqual(@as(usize, 1), Fault.calls);
        var claim = try store.claimImmutable(ordinary.identity);
        defer claim.release();
        try std.testing.expectEqual(if (inject_failure) @as(usize, 0) else bytes, claim.totals.credit.backend_scratch_bytes);
        prepared.releaseAfterExecution(&epoch);
        prepared_live = false;
        guard.finish();
        const expected_bytes = bytes;
        var settlement_polls: usize = 0;
        while (pin.controller().snapshot().backend_scratch_bytes != expected_bytes and settlement_polls < 2000) : (settlement_polls += 1) {
            Retirement.pollRetirementChargesCPU();
            try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(expected_bytes, pin.controller().snapshot().backend_scratch_bytes);
    }
    var final_guard = try sink.enter();
    defer final_guard.finish();
    var final_epoch = try final_guard.retainEpoch();
    defer final_epoch.release();
    backend.freeWithBoundary(owners[0].value.?, &final_epoch);
    owners[0].value = null;
    owners[0].lease.?.releaseWithBoundary(&final_epoch);
    owners[0].lease = null;
    Retirement.pollRetirementChargesCPU();
    // Construction A is sealed. Both cleanup-only quarantine and successful
    // admission now follow this actual final B, independently of the SDK pin.
    try std.testing.expectEqual(bytes, pin.controller().snapshot().backend_scratch_bytes);
    alias.deinitWithBoundary(&final_epoch);
    alias_live = false;
    Retirement.pollRetirementChargesCPU();
    // Both full and quarantined ownership follow the actual unsealed final B.
    // A raw SDK alias outliving that bank B is an outside-only legacy API;
    // this test drops it BEFORE B seals and grants no third-boundary authority.
    try std.testing.expectEqual(bytes, pin.controller().snapshot().backend_scratch_bytes);
    final_guard.finish();
    var polls: usize = 0;
    while (pin.controller().snapshot().backend_scratch_bytes != 0 and polls < 2000) : (polls += 1) {
        Retirement.pollRetirementChargesCPU();
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
}

test "Metal immutable literal failed prepared attachment quarantines reuse and actual SDK alias plus later B fence full and partial witnesses" {
    try preparedLiteralMetalAttachmentRollbackCase(false, true);
    try preparedLiteralMetalAttachmentRollbackCase(true, true);
    try preparedLiteralMetalAttachmentRollbackCase(false, false);
}

test "Metal literal Shared staging keeps Private bank immutable and warm credit excludes dead stage under actual epochs" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Memory = @import("../runtime/tier/memory.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig");
    const Sink = @import("retirement_sink.zig");
    const Retirement = @import("resource_retirement.zig");
    const Prepared = @import("prepared_storage_admission.zig").Prepared;
    const domain = try Domain.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    const Source = struct {
        pin: *const Domain.Pin,
        run: Memory.AdmissionLease = undefined,
        credit: Memory.AdmissionAmounts,
        limit: usize,
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.run, .{ .controller = self.pin.controller(), .backend_class = .gpu, .limits = .{ .backend_limit_bytes = self.limit }, .check_live_memory = false, .credit = &self.credit, .lifetime = admission.Lifetime.forAdmissionDomain(self.pin.*) }, kind, class, bytes);
        }
    };
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const functions = cb.vtable.kernels.?;
    const query = functions.physicalLiteralInitializer orelse return error.MissingLiteralInitializer;
    const initializer = (try query(cb.ptr, 16)) orelse return error.SkipZigTest;
    const bound = try ops.physical_storage.reservationBytes(cb, .{ .byte_size = 16 });
    const stage_bound = try ops.physical_storage.reservationBytes(cb, initializer.staging);
    const total = try std.math.add(usize, bound, stage_bound);
    try std.testing.expectError(error.ResourceLimitExceeded, pin.controller().tryAcquire(.gpu, .{ .backend_limit_bytes = total - 1 }, .{ .backend_scratch_bytes = total }, false));
    var source = Source{ .pin = &pin, .credit = .{ .backend_scratch_bytes = total }, .limit = total };
    source.run = try pin.controller().tryAcquire(.gpu, .{ .backend_limit_bytes = total }, .{ .backend_scratch_bytes = total }, false);
    defer source.run.release();
    const ordinary = admission.Capability{ .identity = .{ .controller = pin.controller(), .backend_class = .gpu }, .context = &source, .services = &.{ .reserve = Source.reserve } };
    const prepared = (try Prepared.prepare(ordinary, &.{ .{ .backend = .metal, .class = .constant, .bytes = bound }, .{ .backend = .metal, .class = .activation, .bytes = stage_bound } })).?;
    var prepared_live = true;
    var prepared_bound = false;
    defer if (prepared_live and !prepared_bound) prepared.abortBeforeExecution() catch @panic("literal preparation rollback");
    var owners = [_]Owner{.{}};
    const data = [_]f32{ -1, 2, 3.5, -4 };
    var literals = [_]Literal{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{ 2, 2 }), .data = &data, .allocation = 0, .backend_index = 0, .cold_bound = bound, .staging_bound = stage_bound }};
    var backends = [_]ops.ComputeBackend{cb};
    var completions = [_]?ops.physical_storage.Completion{null};
    var store = SharedLiteralStore{ .allocator = a, .literals = &literals, .node_indices = &.{}, .publications = &.{}, .consumers = &.{}, .owners = &owners, .backends = &backends, .completions = &completions };
    var sink = try Sink.Ref.create();
    defer sink.release();
    var held: ?CT = null;
    defer {
        var guard = sink.enter() catch @panic("literal error cleanup");
        var epoch = guard.retainEpoch() catch @panic("literal error epoch");
        for (completions) |maybe| if (maybe) |completion| {
            completion.wait() catch @panic("literal completion");
            completion.deinit();
        };
        if (held) |value| cb.freeWithBoundary(value, &epoch);
        if (owners[0].value) |value| cb.freeWithBoundary(value, &epoch);
        if (owners[0].lease) |lease| lease.releaseWithBoundary(&epoch);
        if (prepared_live and prepared_bound) prepared.releaseAfterExecution(&epoch);
        guard.finish();
        epoch.release();
    }
    var guard_a = try sink.enter();
    defer guard_a.finish();
    var epoch_a = try guard_a.retainEpoch();
    defer epoch_a.release();
    try prepared.bindConstructionEpoch(&epoch_a);
    prepared_bound = true;
    const capability = Prepared.forDispatch(prepared, ordinary.identity);
    const first = try store.value(0, capability, null);
    try std.testing.expect(first.fresh and first.lease.usage == .ordinary and first.lease.retirement_owner != null);
    held = (try functions.physicalTensorView.?(cb.ptr, .{ .storage = first.lease, .shape = &.{4} })).?;
    const actual = try cb.toFloat32(first.value, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &data, actual);
    const counters = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    const warm = try store.value(0, capability, null);
    try std.testing.expect(!warm.fresh and warm.lease == first.lease);
    try std.testing.expectEqual(counters.device_owned_buffers_created, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_buffers_created);
    try std.testing.expectError(error.ReadOnlyPhysicalStorage, functions.copyPhysicalInput.?(cb.ptr, held.?, first.value));
    var claim = try store.claimImmutable(capability.identity);
    defer claim.release();
    const totals = claim.totals;
    try std.testing.expect(totals.capacity_reduction.backend_scratch_bytes >= stage_bound);
    const physical = first.lease.backing_byte_size.?;
    prepared.releaseAfterExecution(&epoch_a);
    prepared_live = false;
    guard_a.finish();
    // SDK death and B-end only make the Charge eligible. Refund runs on an
    // outside CPU poll; the background poller may already own that callback.
    var stage_polls: usize = 0;
    while (pin.controller().snapshot().backend_scratch_bytes != physical and stage_polls < 2000) : (stage_polls += 1) {
        Retirement.pollRetirementChargesCPU();
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(physical, pin.controller().snapshot().backend_scratch_bytes);
    // A foreign live frame must not convert construction to asynchronous copy.
    const refused_destination = (try ops.physical_storage.Lease.createTracked(a, cb, 16, null, .constant)).?;
    defer refused_destination.release();
    const refused_stage = (try ops.physical_storage.Lease.createAdmittedWithUsage(a, cb, 16, null, .activation, null, .host_input)).?;
    defer refused_stage.release();
    const before_foreign = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    try std.testing.expectError(error.InvalidPhysicalLiteralInitialization, initializer.initialize(cb.ptr, first.lease, refused_stage, &data));
    try std.testing.expectEqual(before_foreign.device_owned_buffers_created, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_buffers_created);
    try std.testing.expect(try cb.decoderRuntimeBeginFrame());
    var frame_live = true;
    defer if (frame_live) cb.decoderRuntimeCancelFrame() catch @panic("literal frame cancel");
    try std.testing.expect((try query(cb.ptr, 16)) != null);
    try std.testing.expect(!functions.physicalLiteralBindingSupported.?(cb.ptr));
    try std.testing.expectError(error.UnsupportedPhysicalLiteralInitialization, initializer.initialize(cb.ptr, refused_destination, refused_stage, &data));
    try std.testing.expect(cb.decoderRuntimeHasActiveFrame());
    try cb.decoderRuntimeCancelFrame();
    frame_live = false;
    // A real submitted blit retains runtime bookkeeping after active ownership
    // ends. Do not birth another transient Shared stage under that record.
    const queued_destination = (try ops.physical_storage.Lease.createTracked(a, cb, 16, null, .activation)).?;
    defer queued_destination.release();
    const queued_output = (try functions.physicalTensorView.?(cb.ptr, .{ .storage = queued_destination, .shape = &.{4} })).?;
    defer cb.free(queued_output);
    const queued_source = (try functions.physicalTensorView.?(cb.ptr, .{ .storage = refused_stage, .shape = &.{4} })).?;
    defer cb.free(queued_source);
    try functions.writePhysicalFloat32.?(cb.ptr, queued_source, &data);
    try std.testing.expect(try cb.decoderRuntimeBeginFrame());
    frame_live = true;
    try functions.copyPhysicalInput.?(cb.ptr, queued_output, queued_source);
    try cb.decoderRuntimeSubmitFrame();
    frame_live = false;
    var submitted_live = true;
    defer if (submitted_live) cb.decoderRuntimeWaitSubmittedFrame() catch @panic("literal submitted wait");
    try std.testing.expect(cb.decoderRuntimeHasSubmittedFrame() and !cb.decoderRuntimeHasActiveFrame());
    try std.testing.expect((try query(cb.ptr, 16)) != null);
    try std.testing.expect(!functions.physicalLiteralBindingSupported.?(cb.ptr));
    try std.testing.expectError(error.UnsupportedPhysicalLiteralInitialization, initializer.initialize(cb.ptr, refused_destination, refused_stage, &data));
    try std.testing.expect(cb.decoderRuntimeHasSubmittedFrame());
    try cb.decoderRuntimeWaitSubmittedFrame();
    submitted_live = false;
    var guard_b = try sink.enter();
    defer guard_b.finish();
    var epoch_b = try guard_b.retainEpoch();
    defer epoch_b.release();
    cb.freeWithBoundary(owners[0].value.?, &epoch_b);
    owners[0].value = null;
    owners[0].lease.?.releaseWithBoundary(&epoch_b);
    owners[0].lease = null;
    guard_b.finish();
    Retirement.pollRetirementChargesCPU();
    // Polling cannot refund the Private bank while its actual held view lives.
    try std.testing.expectEqual(physical, pin.controller().snapshot().backend_scratch_bytes);
    var guard_c = try sink.enter();
    defer guard_c.finish();
    var epoch_c = try guard_c.retainEpoch();
    defer epoch_c.release();
    cb.freeWithBoundary(held.?, &epoch_c);
    held = null;
    guard_c.finish();
    var final_polls: usize = 0;
    while (pin.controller().snapshot().backend_scratch_bytes != 0 and final_polls < 2000) : (final_polls += 1) {
        Retirement.pollRetirementChargesCPU();
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
}

test "Metal literal completed copy error releases transient stage and quarantines Private publication without retry" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const canonical = context.computeBackend();
    const Fault = struct {
        var original: *const ops.ComputeBackend.VTable = undefined;
        var calls: usize = 0;
        fn query(raw: *anyopaque, bytes: usize) !?ops.physical_storage.LiteralInitializer {
            var value = (try original.kernels.?.physicalLiteralInitializer.?(raw, bytes)) orelse return null;
            value.initialize = initialize;
            return value;
        }
        fn initialize(raw: *anyopaque, destination: *ops.physical_storage.Lease, stage: *ops.physical_storage.Lease, values: []const f32) !void {
            calls += 1;
            const actual = (try original.kernels.?.physicalLiteralInitializer.?(raw, destination.byte_size)).?;
            try actual.initialize(raw, destination, stage, values);
            // Fault AFTER the original owned blit has returned from its wait.
            // This exercises failure custody; it does not simulate a GPU crash.
            return error.InjectedCompletedLiteralCopyFailure;
        }
    };
    Fault.original = canonical.vtable;
    Fault.calls = 0;
    var kernels = canonical.vtable.kernels.?.*;
    kernels.physicalLiteralInitializer = Fault.query;
    kernels.implementation_table = null;
    kernels.acceptsImplementation = null;
    var table = canonical.vtable.*;
    table.kernels = &kernels;
    const backend = ops.ComputeBackend{ .ptr = canonical.ptr, .vtable = &table };
    const descriptor = (try Fault.query(backend.ptr, 16)) orelse return error.SkipZigTest;
    var owners = [_]Owner{.{}};
    const data = [_]f32{ 1, 3, 5, 7 };
    var literals = [_]Literal{.{ .node = 0, .shape = ml.graph.Shape.init(.f32, &.{4}), .data = &data, .allocation = 0, .backend_index = 0, .cold_bound = try ops.physical_storage.reservationBytes(backend, .{ .byte_size = 16 }), .staging_bound = try ops.physical_storage.reservationBytes(backend, descriptor.staging) }};
    var backends = [_]ops.ComputeBackend{backend};
    var completions = [_]?ops.physical_storage.Completion{null};
    var store = SharedLiteralStore{ .allocator = a, .literals = &literals, .node_indices = &.{}, .publications = &.{}, .consumers = &.{}, .owners = &owners, .backends = &backends, .completions = &completions };
    defer {
        for (completions) |maybe| if (maybe) |completion| {
            completion.wait() catch @panic("failed literal completion");
            completion.deinit();
        };
        if (owners[0].value) |value| backend.freeWithCleanup(value, .{ .outside_execution = true });
        if (owners[0].lease) |lease| lease.releaseWithCleanup(.{ .outside_execution = true });
    }
    const before = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    try std.testing.expectError(error.InjectedCompletedLiteralCopyFailure, store.value(0, null, null));
    try std.testing.expect(owners[0].lease != null and owners[0].value != null);
    try std.testing.expectEqual(@as(usize, 1), Fault.calls);
    const failed = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_buffers_created + 2, failed.device_owned_buffers_created);
    try std.testing.expectEqual(before.device_owned_buffers_released + 1, failed.device_owned_buffers_released);
    try std.testing.expectError(error.InjectedCompletedLiteralCopyFailure, store.value(0, null, null));
    try std.testing.expectEqual(@as(usize, 1), Fault.calls);
    try std.testing.expectEqual(failed.device_owned_buffers_created, @import("../backends/metal_tensor.zig").memoryStatsSnapshot().device_owned_buffers_created);
}
