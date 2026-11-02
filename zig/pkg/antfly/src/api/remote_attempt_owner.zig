// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! All protocol state lives in the API kernel. Bootstrap copies descriptors;
//! callers never transport native ledgers, mutexes or executor pointers.
const std = @import("std");
const attempts = @import("../common/remote_attempts.zig");
const worker = @import("../common/remote_worker.zig");
const replies = @import("../common/remote_attempt_reply.zig");
const membership = @import("../common/remote_membership.zig");
const durable = @import("../common/remote_fence_store.zig");
const client_mod = @import("remote_attempt_client.zig");
const executor_mod = @import("../common/http/std_http_executor.zig");
const auth = @import("internal_service_auth.zig");
const clock = @import("antfly_platform").time;
pub const node_coordinator_capacity = 256;

pub const Target = struct {
    local: membership.Local,
    base_uri: []const u8,
    max_attempts: usize = 8,
    max_bytes: usize = 4 << 20,
    available: bool = true,
};
pub const Bootstrap = struct {
    root_path: []const u8,
    local: membership.Local,
    targets: []const Target,
    worker_limits: worker.Limits = .{ .coordinators = node_coordinator_capacity, .attempts = 64, .per_coordinator_attempts = 8, .duplicate_window = 32, .bytes = 16 << 20, .per_coordinator_bytes = 4 << 20, .resource_references = 32 },
    max_outbound_attempts: usize = 64,
    max_outbound_bytes: usize = 16 << 20,
    reconciliation: @import("remote_reconciliation.zig").Policy = .{},

    pub fn validate(self: Bootstrap) !void {
        try self.reconciliation.validate();
        try self.worker_limits.validate(65535);
        _ = try membership.coordinatorIdentity(self.local.metadata_incarnation, self.local.node_id);
        if (self.root_path.len == 0 or self.root_path.len > 4096 or self.targets.len == 0 or
            self.targets.len > 256 or self.targets.len > self.worker_limits.coordinators or
            self.max_outbound_attempts == 0 or self.max_outbound_attempts > 65535 or self.max_outbound_bytes == 0) return error.InvalidArgument;
        for (self.targets, 0..) |target, i| {
            if (!std.mem.eql(u8, &target.local.metadata_incarnation, &self.local.metadata_incarnation) or
                target.base_uri.len == 0 or target.base_uri.len > 2048 or target.max_attempts == 0 or
                target.max_attempts > self.max_outbound_attempts or target.max_bytes == 0 or target.max_bytes > self.max_outbound_bytes) return error.InvalidArgument;
            const uri = std.Uri.parse(target.base_uri) catch return error.InvalidArgument;
            if ((!std.mem.eql(u8, uri.scheme, "http") and !std.mem.eql(u8, uri.scheme, "https")) or uri.host == null or
                uri.user != null or uri.password != null or uri.query != null or uri.fragment != null) return error.InvalidArgument;
            var path: [2]u8 = undefined;
            const base_path = uri.path.toRaw(&path) catch return error.InvalidArgument;
            if (base_path.len != 0 and !std.mem.eql(u8, base_path, "/")) return error.InvalidArgument;
            for (self.targets[0..i]) |previous| if (previous.local.root_incarnation == target.local.root_incarnation) return error.InvalidArgument;
        }
    }
};
pub const Stats = struct {
    initialized: bool = false,
    recovery_complete: bool = false,
    generation: u64 = 0,
    startup_fences_remaining: usize = 0,
    outbound_attempts: usize = 0,
    outbound_bytes: usize = 0,
    worker_attempts: usize = 0,
    worker_bytes: usize = 0,
};

pub const Initialization = struct {
    authority: durable.InitializationAuthority,
    activate: *const fn (*anyopaque, durable.InitializationId) anyerror!void,
    scheduler: *@import("antfly_local_sources").common_request_admission.runnable_scheduler.Scheduler,
};

pub const State = struct {
    allocator: std.mem.Allocator,
    baseline: @import("antfly_local_sources").common_request_admission.runnable_scheduler.Scheduler.ControlBaseline,
    io: std.Io,
    dir: std.Io.Dir,
    store: durable.Store,
    registry: worker.Registry,
    signer: replies.Signer,
    ledger: attempts.Coordinator,
    slots: []?attempts.Attempt,
    destinations: []attempts.Destination,
    targets: []client_mod.Target,
    endpoint_view: *@import("remote_endpoint_view.zig").View,
    endpoint_source: ?*@import("remote_endpoint_source.zig").Source = null,
    capabilities: ?*@import("remote_capability_publication.zig").Cache = null,
    read_executor: executor_mod.StdHttpExecutor,
    control_executor: executor_mod.StdHttpExecutor,
    control_admission: *@import("remote_control_admission.zig").Gate,
    control_rpc_timeout_ns: u64,

    /// Fixed node metadata only. Endpoint views and control gates reserve their
    /// own baselines; dynamic transport allocations remain in accounted pools.
    pub fn reservationBytes(config: Bootstrap) !u64 {
        try config.validate();
        var bytes: u64 = @sizeOf(State);
        bytes = try std.math.add(u64, bytes, try std.math.mul(u64, config.max_outbound_attempts, @sizeOf(?attempts.Attempt)));
        bytes = try std.math.add(u64, bytes, try std.math.mul(u64, config.targets.len, @sizeOf(attempts.Destination)));
        bytes = try std.math.add(u64, bytes, try worker.Registry.metadataBytes(config.worker_limits));
        return std.math.add(u64, bytes, try durable.Store.metadataBytes(config.worker_limits.coordinators));
    }

    pub fn reservedBytes(self: *const State) u64 {
        return self.baseline.bytes;
    }

    /// Authorities were resolved from ONE owned linearizable metadata cut.
    /// Signing must be loaded from the physical root, never supplied by a peer.
    pub fn create(allocator: std.mem.Allocator, read_allocator: std.mem.Allocator, io: std.Io, config: Bootstrap, signing: anytype, local_authority: replies.Authority, authorities: []const replies.Authority, initialization: Initialization) !*State {
        try config.validate();
        if (authorities.len != config.targets.len or !std.meta.eql(local_authority.local, config.local)) return error.InvalidRemoteMembership;
        var baseline = initialization.scheduler.reserveControlBaseline(try reservationBytes(config)) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        const signer = try replies.Signer.initEnrolled(local_authority, signing);
        const self = try allocator.create(State);
        errdefer allocator.destroy(self);
        const view = try @import("remote_endpoint_view.zig").View.create(allocator, initialization.scheduler, config.targets, authorities);
        errdefer view.release();
        const targets = view.targets;
        const slots = try allocator.alloc(?attempts.Attempt, config.max_outbound_attempts);
        errdefer allocator.free(slots);
        const destinations = try allocator.alloc(attempts.Destination, targets.len);
        errdefer allocator.free(destinations);
        for (destinations, config.targets) |*destination, target| destination.* = .{ .worker_incarnation = target.local.root_incarnation, .max_attempts = target.max_attempts, .max_bytes = target.max_bytes };
        const dir = if (std.fs.path.isAbsolute(config.root_path))
            try std.Io.Dir.openDirAbsolute(io, config.root_path, .{})
        else
            try std.Io.Dir.cwd().openDir(io, config.root_path, .{});
        errdefer dir.close(io);
        self.* = .{
            .allocator = allocator,
            .baseline = baseline,
            .io = io,
            .dir = dir,
            .store = try durable.Store.openAuthorized(allocator, io, dir, config.local.root_incarnation, config.worker_limits.coordinators, initialization.authority),
            .registry = undefined,
            .signer = signer,
            .ledger = undefined,
            .slots = slots,
            .destinations = destinations,
            .targets = targets,
            .endpoint_view = view,
            .read_executor = undefined,
            .control_executor = undefined,
            .control_admission = undefined,
            .control_rpc_timeout_ns = config.reconciliation.rpc_timeout_ns,
        };
        errdefer self.store.deinit();
        self.registry = try worker.Registry.init(allocator, &self.store, config.local, config.worker_limits, clock.monotonicNs);
        errdefer self.registry.deinit() catch unreachable;
        try self.read_executor.initInPlaceFallible(read_allocator, .{ .io_concurrent_limit = 8, .pool_max_connections = 4, .pool_max_per_host = 4, .pool_max_waiters = 0, .pool_max_wait_bytes = 0, .pool_max_wait_ms = 0 });
        errdefer self.read_executor.deinit();
        // One RPC still needs bounded nested request/connect/watchdog tasks.
        try self.control_executor.initInPlaceFallible(allocator, .{ .io_concurrent_limit = 8, .pool_max_connections = 1, .pool_max_per_host = 1, .pool_max_waiters = 0, .pool_max_wait_bytes = 0, .pool_max_wait_ms = 0 });
        errdefer self.control_executor.deinit();
        self.control_admission = try @import("remote_control_admission.zig").Gate.create(allocator, io, initialization.scheduler);
        errdefer self.control_admission.tryDestroy() catch unreachable;
        // Finish fallible heap/executor setup before consuming a clean receipt.
        // The exclusive lease keeps its checkpoint stable throughout setup.
        const clean_join = @import("../common/remote_quiescence.zig").matches(&self.store, local_authority);
        // Every prior destination still needs a fresh signed fence; the receipt
        // only proves this root's physically joined inbound ownership.
        self.ledger = try self.store.startCoordinator(try membership.coordinatorIdentity(config.local.metadata_incarnation, config.local.node_id), 0, slots, destinations, config.max_outbound_bytes);
        // Activation follows durable journal publication and precedes any
        // control ingress or periodic consumer acquiring this state.
        const journal = try self.store.initializationState();
        try initialization.activate(initialization.authority.context, journal.id);
        if (clean_join) {
            try self.registry.completeRecovery(.{ .context = &clean_join, .quiescent = cleanJoin });
        } else if (!journal.publication_started) try self.registry.completeRecovery(.{ .context = &self.store, .quiescent = unusedJournal });
        return self;
    }
    fn cleanJoin(raw: *const anyopaque) bool {
        return @as(*const bool, @ptrCast(@alignCast(raw))).*;
    }
    fn unusedJournal(raw: *const anyopaque) bool {
        const store: *durable.Store = @ptrCast(@alignCast(@constCast(raw)));
        const journal = store.initializationState() catch return false;
        return !std.mem.allEqual(u8, &journal.id, 0) and !journal.publication_started;
    }
    pub fn client(self: *State, read_allocator: std.mem.Allocator, signing: auth.Config) client_mod.Client {
        return .{ .reads_ready = self.registry.recoveryComplete(), .capabilities = self.capabilities, .ledger = &self.ledger, .store = &self.store, .targets = self.targets, .endpoint_view = self.endpoint_view, .endpoint_source = self.endpoint_source, .signing = signing, .read_allocator = read_allocator, .control_allocator = self.allocator, .read_executor = self.read_executor.executor(), .control_executor = self.control_executor.executor(), .control_admission = self.control_admission, .control_rpc_timeout_ns = self.control_rpc_timeout_ns };
    }
    pub fn stats(self: *State) Stats {
        const outbound = self.ledger.snapshot();
        const inbound = self.registry.snapshot();
        return .{ .initialized = true, .recovery_complete = self.registry.recoveryComplete(), .generation = self.ledger.generationState().generation, .startup_fences_remaining = outbound.startup_fences_remaining, .outbound_attempts = outbound.attempts, .outbound_bytes = outbound.bytes, .worker_attempts = inbound.attempts, .worker_bytes = inbound.bytes };
    }
    /// The supervisor has joined the periodic consumer and every physical read
    /// owner before destruction. Uncertainty survives through durable history.
    pub fn destroy(self: *State) void {
        if (self.ledger.reconciler_bound) @panic("remote reconciliation still owns the ledger during API destruction");
        if (self.endpoint_source) |source| {
            source.close();
            source.deinit() catch @panic("endpoint root or control lease still live during API destruction");
        }
        if (self.endpoint_view.referenceCount() != 1) @panic("remote endpoint borrowers still live during API destruction");
        if (self.ledger.hasRetainedParents()) @panic("remote descendants still retain physical parents during API destruction");
        // Recovery-blocked processes may not manufacture evidence from their
        // newly empty registry. Unknown outbound work also prevents a receipt.
        const outbound = self.ledger.snapshot();
        const clean_join = self.registry.recoveryComplete() and outbound.attempts == 0 and
            outbound.startup_fences_remaining == 0 and outbound.queued == 0;
        self.registry.deinit() catch @panic("remote worker resources still live during API destruction");
        self.control_admission.tryDestroy() catch @panic("control RPC permit still live during API destruction");
        self.read_executor.deinit();
        self.control_executor.deinit();
        // Both transports and all query/worker parents are now physically joined.
        // Missing/incomplete receipts give no authority. If a complete signed
        // receipt survives an uncertain sync, its already-joined checkpoint is
        // still safe; the next epoch prevents reuse by a later unclean owner.
        if (clean_join) @import("../common/remote_quiescence.zig").record(&self.store, &self.signer) catch {};
        self.store.deinit();
        self.dir.close(self.io);
        self.endpoint_view.release();
        self.allocator.free(self.destinations);
        self.allocator.free(self.slots);
        const allocator = self.allocator;
        var baseline = self.baseline;
        allocator.destroy(self);
        baseline.release();
    }
};
