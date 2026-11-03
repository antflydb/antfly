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
        errdefer allocator.free(owned_backends);
        const completions = try allocator.alloc(?ops.physical_storage.Completion, backends.len);
        errdefer allocator.free(completions);
        @memset(completions, null);
        var index: usize = 0;
        for (graph.nodes.items, 0..) |node, node_id| {
            const backend = eligible(graph, plan, backends, @intCast(node_id)) orelse continue;
            const attrs = node.op.constant;
            const bytes = try std.math.mul(usize, attrs.data_len, @sizeOf(f32));
            const functions = backends[backend].vtable.kernels.?;
            const bound = if (functions.physicalStorageReservationBytes) |query| try query(backends[backend].ptr, bytes) else bytes;
            if (bound < bytes) return error.InvalidPhysicalStorageReservation;
            literals[index] = .{ .node = @intCast(node_id), .shape = node.output_shape, .data = graph.constantData(attrs.data_offset, attrs.data_len), .allocation = try plan.addConstantBinding(backends[backend].kind(), bytes), .backend_index = backend, .cold_bound = bound };
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

    pub const AdmissionSpan = struct { backend: @import("../graph/backend_contracts.zig").BackendKind, bytes: usize };
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
        return .{ .backend = self.backends[literal.backend_index].kind(), .bytes = literal.cold_bound };
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
            const reduction = literal.cold_bound - bytes;
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
        if (capability) |cap| if (cap.services.takeBacking != null) self.prepared_backings.store(true, .release);
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
            self.releaseWithCleanup(.{ .boundary = if (capability) |cap| cap.constructionEpoch() else null });
        }
        if (owner.lease) |lease| {
            // This mutation is protected by the initialization claim; every
            // metadata snapshot declines until publication completes.
            try lease.ensureAdmission(capability.?);
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            self.epoch = next_epoch;
            self.cached_claim = null;
            const result = Value{ .value = owner.value.?, .lease = lease, .fresh = false };
            self.mutex.unlock();
            return result;
        }
        if (self.completions[literal.backend_index] == null) {
            if (functions.createPhysicalCompletion) |make| self.completions[literal.backend_index] = try make(cb.ptr);
        }
        if (self.completions[literal.backend_index]) |completion| try completion.beforeReuse();
        const lease = (try ops.physical_storage.Lease.createAdmitted(self.allocator, cb, try std.math.mul(usize, literal.data.len, 4), ledger, .constant, capability)) orelse return error.PhysicalStorageAllocationDeclined;
        var published = false;
        errdefer if (!published) lease.release();
        var dimensions: [8]i32 = undefined;
        for (literal.shape.dims[0..literal.shape.rank()], 0..) |dimension, axis| dimensions[axis] = @intCast(dimension);
        const tensor = (try functions.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = dimensions[0..literal.shape.rank()] })) orelse return error.UnsupportedPhysicalTensorView;
        errdefer if (!published) cb.free(tensor);
        var failure: ?anyerror = null;
        functions.writePhysicalFloat32.?(cb.ptr, tensor, literal.data) catch |err| {
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
    fn allocate(raw: *anyopaque, bytes: usize) !?CT {
        try fromCompute(raw).outsideMutex();
        return native.vtable_impl.kernels.?.allocatePhysicalStorage.?(raw, bytes);
    }
    fn view(raw: *anyopaque, request: ops.physical_storage.View) !?CT {
        try fromCompute(raw).outsideMutex();
        return native.vtable_impl.kernels.?.physicalTensorView.?(raw, request);
    }
    const kernels: ops.KernelBackend = blk: {
        var result = native.vtable_impl.kernels.?.*;
        result.implementation_table = null;
        result.acceptsImplementation = null;
        result.createPhysicalCompletion = completion;
        result.allocatePhysicalStorage = allocate;
        result.physicalTensorView = view;
        result.writePhysicalFloat32 = write;
        break :blk result;
    };
    const table: ops.ComputeBackend.VTable = blk: {
        var result = native.vtable_impl;
        result.kernels = &kernels;
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
