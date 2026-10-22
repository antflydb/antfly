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

//! Scoped execution capabilities between independently compiled native owners.
//! Offers are layout/toolchain checked before reading their native method table.
//! Scopes share the producer's real counters, executors and durable registry.
//! A client closes after its consumers stop; close drains its durable owners and
//! waits for resource borrows. Io Receivers remain caller-owned and stable until
//! their tasks finish, and must retire before releasing the associated resource.
const std = @import("std");
const background = @import("background_runtime.zig");
const runtime_abi = @import("antfly_runtime_abi");
const native = runtime_abi.native_abi;
const io_abi = runtime_abi.io_abi;
const callbacks = @import("named_callback.zig");
const Runtime = background.BackendRuntime;
const Allocator = std.mem.Allocator;
const pdf = @import("antfly_pdf");
const Scheduler = background.MaintenanceScheduler;
const RunnableCallbacks = struct { step: *const fn (*anyopaque, runtime_abi.runnable_abi.Borrowed) anyerror!?u64 };
const RunnableBoundary = callbacks.Boundary(RunnableCallbacks);
pub const Maintenance = Scheduler.RegistrationSpec;
pub const Info = struct {
    backend: background.Backend,
    limits: @import("antfly_runtime_fs").threaded_io_limits.BackendRuntimeLaneLimits,
    maintenance_registration_limit: usize,
    uses_borrowed_io: bool,
    runnable_owner: ?runtime_abi.runnable_abi.Owner,
};

pub const Purpose = enum { general, filesystem, raft_inbound, raft_outbound, api, inference, control, request_forward, api_network, api_filesystem };
pub const OwnerAction = enum { drain, close, pause, resume_owner, reopen };
pub const Resource = struct {
    handle: *anyopaque,
    io: io_abi.Borrow,
    capacity: usize,
    client_state: *anyopaque,
    state_contract: native.TypeContract,
};
const Methods = struct {
    open: *const fn (*anyopaque) anyerror!*anyopaque,
    close: *const fn (*anyopaque) anyerror!void,
    workers: *const fn (*anyopaque, Runtime.WorkerOptions) anyerror!Resource,
    purpose: *const fn (*anyopaque, Purpose) anyerror!Resource,
    sql: *const fn (*anyopaque, Runtime.SharedSqlRole, usize) anyerror!Resource,
    release: *const fn (*anyopaque, *anyopaque, bool) anyerror!void,
    info: *const fn (*anyopaque) anyerror!Info,
    io_view: *const fn (*anyopaque, Purpose) anyerror!io_abi.Borrow,
    pdf: *const fn (*anyopaque) anyerror!Resource,
    pdf_batch: *const fn (*anyopaque, *anyopaque, []const *anyopaque, *const fn (*anyopaque, Allocator) void, usize) anyerror!pdf.PageRenderExecutor.BatchStats,
    maintenance: *const fn (*anyopaque, Maintenance) anyerror!Resource,
    wake_maintenance: *const fn (*anyopaque, *anyopaque) anyerror!void,
    maintenance_stats: *const fn (*anyopaque) anyerror!?Scheduler.Stats,
    ensure_maintenance: *const fn (*anyopaque) anyerror!Scheduler.Stats,
    stop_maintenance: *const fn (*anyopaque) anyerror!void,
    stop_registration: *const fn (*anyopaque, *anyopaque) anyerror!void,
    registration_ready: *const fn (*anyopaque, *anyopaque) anyerror!bool,
    owner: *const fn (*anyopaque) anyerror!u64,
    jobs: *const fn (*anyopaque) anyerror!background.DurableJobLane,
    arm_probe: *const fn (*anyopaque, u64, background.OwnerMaintenanceProbe) anyerror!void,
    disarm_probe: *const fn (*anyopaque, u64) anyerror!void,
    stats: *const fn (*anyopaque) anyerror!Runtime.LaneStats,
    memory_stats: *const fn (*anyopaque) anyerror!?Runtime.RuntimeMemoryStats,
    quiesce: *const fn (*anyopaque) anyerror!void,
    sql_stats: *const fn (*anyopaque, Runtime.SharedSqlRole) anyerror!Runtime.SharedSqlStats,
    sql_memory: *const fn (*anyopaque, Runtime.SharedSqlRole, bool) anyerror!?background.SqlWorkerMemory.Bank.Stats,
    install_sql: *const fn (*anyopaque, runtime_abi.memory_abi.Parent, background.SqlWorkerMemory.Limits) anyerror!void,
    pdf_stats: *const fn (*anyopaque) anyerror!?@import("../common/bounded_worker_lane.zig").Stats,
};
const Boundary = callbacks.Boundary(Methods);
const methods_contract = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(Methods);
};

pub const Offer = extern struct {
    // V3 adds the finite-step recovery Job class and reserves its workers in
    // the aggregate lane budget. Older producers cannot provide this contract.
    // V2 introduced whole-graph Raft outbound admission.
    pub const abi_version = 3;
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(Offer),
    context: ?*anyopaque,
    contract: native.TypeContract = methods_contract,
    purpose_contract: native.TypeContract = native.TypeContract.of(Purpose),
    methods: ?*const Methods = &producer_methods,
    dispatch: ?callbacks.Dispatch = Boundary.local_dispatch,

    pub fn init(runtime: *Runtime) Offer {
        return .{ .context = runtime };
    }
    fn read(source: *const Offer) !Offer {
        const Header = extern struct { version: u32, struct_size: u32 };
        const header: *const Header = @ptrCast(source);
        if (header.version != abi_version or header.struct_size != @sizeOf(Offer)) return error.UnsupportedVersion;
        if (!source.contract.matches(methods_contract)) return error.UnsupportedVersion;
        if (!source.purpose_contract.matches(native.TypeContract.of(Purpose))) return error.UnsupportedVersion;
        if (source.context == null or source.methods == null or source.dispatch == null) return error.InvalidArgument;
        if (@intFromPtr(source.methods.?) % @alignOf(Methods) != 0) return error.InvalidArgument;
        return source.*;
    }
    fn call(self: Offer, comptime method: []const u8, args: anytype) !@typeInfo(@typeInfo(@typeInfo(@FieldType(Methods, method)).pointer.child).@"fn".return_type.?).error_union.payload {
        @setEvalBranchQuota(100_000);
        return Boundary.call(method, self.dispatch.?, @field(self.methods.?, method), args);
    }
};

/// Move-only client and resource values. Durable lanes borrow the client scope;
/// they must not be used after close. Resources retain their producer scope and
/// can release while a different thread is waiting in Client.close.
pub const Client = struct {
    offer: Offer,
    scope: ?*anyopaque,
    pub fn open(source: *const Offer) !Client {
        const offer = try Offer.read(source);
        return .{ .offer = offer, .scope = try offer.call("open", .{offer.context.?}) };
    }
    fn context(self: *const Client) !*anyopaque {
        return self.scope orelse error.BackendRuntimeShuttingDown;
    }
    pub fn close(self: *Client) !void {
        const scope = self.scope orelse return;
        try self.offer.call("close", .{scope});
        self.scope = null;
    }
    pub fn quiesce(self: *const Client) !void {
        return self.offer.call("quiesce", .{try self.context()});
    }
    pub fn sqlStats(self: *const Client, role: Runtime.SharedSqlRole) !Runtime.SharedSqlStats {
        return self.offer.call("sql_stats", .{ try self.context(), role });
    }
    pub fn sqlMemory(self: *const Client, role: Runtime.SharedSqlRole, stacks: bool) !?background.SqlWorkerMemory.Bank.Stats {
        return self.offer.call("sql_memory", .{ try self.context(), role, stacks });
    }
    pub fn installSql(self: *const Client, parent: runtime_abi.memory_abi.Parent, limits: background.SqlWorkerMemory.Limits) !void {
        return self.offer.call("install_sql", .{ try self.context(), parent, limits });
    }
    pub fn pdfStats(self: *const Client) !?@import("../common/bounded_worker_lane.zig").Stats {
        return self.offer.call("pdf_stats", .{try self.context()});
    }
    pub fn acquireWorkers(self: *const Client, options: Runtime.WorkerOptions) !Lease {
        return Lease.init(self.offer, try self.context(), try self.offer.call("workers", .{ try self.context(), options }));
    }
    pub fn acquireIo(self: *const Client, purpose: Purpose) !Lease {
        return Lease.init(self.offer, try self.context(), try self.offer.call("purpose", .{ try self.context(), purpose }));
    }
    pub fn acquireSql(self: *const Client, role: Runtime.SharedSqlRole, minimum: usize) !Lease {
        return Lease.init(self.offer, try self.context(), try self.offer.call("sql", .{ try self.context(), role, minimum }));
    }
    pub fn info(self: *const Client) !Info {
        return self.offer.call("info", .{try self.context()});
    }
    pub fn ioView(self: *const Client, purpose: Purpose) !io_abi.Receiver {
        const borrow = try self.offer.call("io_view", .{ try self.context(), purpose });
        return borrow.receive();
    }
    pub fn acquirePdf(self: *const Client) !Lease {
        return Lease.init(self.offer, try self.context(), try self.offer.call("pdf", .{try self.context()}));
    }
    pub fn registerMaintenance(self: *const Client, spec: Maintenance) !Lease {
        return Lease.init(self.offer, try self.context(), try self.offer.call("maintenance", .{ try self.context(), spec }));
    }
    pub fn wakeMaintenance(self: *const Client, raw: *anyopaque) !void {
        return self.offer.call("wake_maintenance", .{ try self.context(), raw });
    }
    pub fn maintenanceStats(self: *const Client) !?Scheduler.Stats {
        return self.offer.call("maintenance_stats", .{try self.context()});
    }
    pub fn ensureMaintenance(self: *const Client) !Scheduler.Stats {
        return self.offer.call("ensure_maintenance", .{try self.context()});
    }
    pub fn stopMaintenanceAdmission(self: *const Client) !void {
        return self.offer.call("stop_maintenance", .{try self.context()});
    }
    pub fn maintenancePort(self: *Client) !Scheduler.RemotePort {
        _ = try self.ensureMaintenance();
        return .{ .context = self, .register_fn = remoteRegister, .wake_fn = remoteWake, .close_admission_fn = remoteCloseAdmission, .snapshot_fn = remoteSnapshot };
    }
    fn remoteRegister(raw: *anyopaque, spec: Maintenance) !Scheduler.RemoteHandle {
        const self: *Client = @ptrCast(@alignCast(raw));
        var lease = try self.registerMaintenance(spec);
        return lease.intoMaintenanceHandle();
    }
    fn remoteWake(raw: *anyopaque, context_value: *anyopaque) void {
        const self: *Client = @ptrCast(@alignCast(raw));
        self.wakeMaintenance(context_value) catch |err| switch (err) {
            error.BackendRuntimeShuttingDown => {},
            else => @panic("execution services wake contract"),
        };
    }
    fn remoteCloseAdmission(raw: *anyopaque) void {
        const self: *Client = @ptrCast(@alignCast(raw));
        self.stopMaintenanceAdmission() catch @panic("execution services close contract");
    }
    fn remoteSnapshot(raw: *anyopaque) Scheduler.Stats {
        const self: *Client = @ptrCast(@alignCast(raw));
        return self.ensureMaintenance() catch @panic("execution services snapshot contract");
    }
    pub fn allocOwnerId(self: *const Client) !u64 {
        return self.offer.call("owner", .{try self.context()});
    }
    pub fn durableJobs(self: *const Client) !background.DurableJobLane {
        return self.offer.call("jobs", .{try self.context()});
    }
    pub fn armProbe(self: *const Client, owner: u64, probe: background.OwnerMaintenanceProbe) !void {
        return self.offer.call("arm_probe", .{ try self.context(), owner, probe });
    }
    pub fn disarmProbe(self: *const Client, owner: u64) !void {
        return self.offer.call("disarm_probe", .{ try self.context(), owner });
    }
    pub fn stats(self: *const Client) !Runtime.LaneStats {
        return self.offer.call("stats", .{try self.context()});
    }
    pub fn memoryStats(self: *const Client) !?Runtime.RuntimeMemoryStats {
        return self.offer.call("memory_stats", .{try self.context()});
    }
};
// Producer-funded opaque receiver storage keeps a movable lease's std.Io stable.
// Only the receiving compilation unit initializes or interprets these bytes.
const ClientState = struct {
    offer: Offer,
    scope: *anyopaque,
    handle: *anyopaque,
    receiver: io_abi.Receiver,
    fn finishRegistration(raw: *anyopaque, cancel: bool) void {
        const self: *ClientState = @ptrCast(@alignCast(raw));
        self.offer.call("release", .{ self.scope, self.handle, cancel }) catch @panic("execution services registration release contract");
    }
    fn readyRegistration(raw: *anyopaque, out: *bool) callconv(.c) runtime_abi.error_abi.Status {
        const self: *ClientState = @ptrCast(@alignCast(raw));
        out.* = self.offer.call("registration_ready", .{ self.scope, self.handle }) catch |cause| return runtime_abi.error_abi.statusFromError(cause);
        return .ok;
    }
    fn stopRegistration(raw: *anyopaque) void {
        const self: *ClientState = @ptrCast(@alignCast(raw));
        self.offer.call("stop_registration", .{ self.scope, self.handle }) catch @panic("execution services registration stop contract");
    }
    fn runPdf(raw: *anyopaque, contexts: []const *anyopaque, run: *const fn (*anyopaque, Allocator) void, scratch: usize) !pdf.PageRenderExecutor.BatchStats {
        const self: *ClientState = @ptrCast(@alignCast(raw));
        return self.offer.call("pdf_batch", .{ self.scope, self.handle, contexts, run, scratch });
    }
};
const client_state_contract = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(ClientState);
};

pub const Lease = struct {
    offer: Offer,
    scope: *anyopaque,
    resource: ?Resource,
    fn init(offer: Offer, scope: *anyopaque, resource: Resource) !Lease {
        errdefer offer.call("release", .{ scope, resource.handle, true }) catch @panic("failed execution service rollback");
        if (!resource.state_contract.matches(client_state_contract) or @intFromPtr(resource.client_state) % @alignOf(ClientState) != 0)
            return error.UnsupportedVersion;
        const state: *ClientState = @ptrCast(@alignCast(resource.client_state));
        state.* = .{ .offer = offer, .scope = scope, .handle = resource.handle, .receiver = try resource.io.receive() };
        return .{ .offer = offer, .scope = scope, .resource = resource };
    }
    pub fn io(self: *const Lease) std.Io {
        const state: *ClientState = @ptrCast(@alignCast(self.resource.?.client_state));
        return state.receiver.io();
    }
    pub fn pageExecutor(self: *const Lease) pdf.PageRenderExecutor {
        return .{ .ptr = self.resource.?.client_state, .concurrent_capacity = self.capacity(), .run_batch_fn = ClientState.runPdf };
    }
    pub fn receiveIo(self: *const Lease) !io_abi.Receiver {
        const resource = self.resource orelse return error.BackendRuntimeShuttingDown;
        return resource.io.receive();
    }
    pub fn intoMaintenanceHandle(self: *Lease) Scheduler.RemoteHandle {
        const state = self.resource.?.client_state;
        self.resource = null;
        return .{ .context = state, .finish_fn = ClientState.finishRegistration, .stop_fn = ClientState.stopRegistration, .ready_fn = ClientState.readyRegistration };
    }
    pub fn capacity(self: *const Lease) usize {
        return self.resource.?.capacity;
    }
    pub fn release(self: *Lease) !void {
        const resource = self.resource orelse return;
        try self.offer.call("release", .{ self.scope, resource.handle, true });
        self.resource = null;
    }
    pub fn awaitRegistration(self: *Lease) !void {
        const resource = self.resource orelse return;
        try self.offer.call("release", .{ self.scope, resource.handle, false });
        self.resource = null;
    }
};

const Scope = struct {
    runtime: *Runtime,
    retention: Runtime.ExecutionServicesRetention,
    io: std.Io,
    allocator: Allocator,
    mutex: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    closing: bool = false,
    maintenance_closing: bool = false,
    calls: usize = 0,
    resources: usize = 0,
    resource_head: ?*Token = null,
    io_slots: [@typeInfo(Purpose).@"enum".field_names.len]?std.Io = @splat(null),
    owners: std.ArrayListUnmanaged(u64) = .empty,

    fn cast(raw: *anyopaque) *Scope {
        return @ptrCast(@alignCast(raw));
    }
    fn begin(self: *Scope, cleanup: bool) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closing and !cleanup) return error.BackendRuntimeShuttingDown;
        self.calls += 1;
    }
    fn end(self: *Scope) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        std.debug.assert(self.calls > 0);
        self.calls -= 1;
        self.changed.broadcast(self.io);
    }
    fn ownsLocked(self: *const Scope, owner: u64) bool {
        return std.mem.indexOfScalar(u64, self.owners.items, owner) != null;
    }
    fn checkOwner(self: *Scope, owner: u64) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (!self.ownsLocked(owner)) return error.BackgroundOwnerClosed;
    }
    fn attach(self: *Scope, token: *Token) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        token.next = self.resource_head;
        self.resource_head = token;
        self.resources += 1;
    }
    fn describe(_: *Scope, token: *Token) Resource {
        return .{ .handle = token, .io = io_abi.Borrow.init(&token.io), .capacity = token.capacity, .client_state = &token.client_state, .state_contract = client_state_contract };
    }
    fn publish(self: *Scope, token: *Token) Resource {
        self.attach(token);
        return self.describe(token);
    }
    fn detach(self: *Scope, raw: *anyopaque) !*Token {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var link = &self.resource_head;
        while (link.*) |token| {
            if (@as(*anyopaque, token) == raw) {
                link.* = token.next;
                return token;
            }
            link = &token.next;
        }
        return error.InvalidArgument;
    }
    fn checkedToken(self: *Scope, raw: *anyopaque) !*Token {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var it = self.resource_head;
        while (it) |token| : (it = token.next) if (@as(*anyopaque, token) == raw) return token;
        return error.InvalidArgument;
    }
    fn chooseIo(self: *Scope, purpose_value: Purpose) ?std.Io {
        return switch (purpose_value) {
            .general => self.runtime.io(),
            .filesystem => self.runtime.filesystemIo(),
            .raft_inbound => self.runtime.raftInboundIo(),
            .raft_outbound => self.runtime.raftOutboundIo(),
            .api => self.runtime.apiIo(),
            .api_network => self.runtime.apiNetworkIo(),
            .api_filesystem => self.runtime.apiFilesystemIo(),
            .inference => self.runtime.inferenceIo(),
            .control => self.runtime.controlIo(),
            .request_forward => null,
        };
    }
    fn info(raw: *anyopaque) !Info {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        return .{ .backend = self.runtime.backend, .limits = self.runtime.lane_limits, .maintenance_registration_limit = self.runtime.maintenance_registration_limit, .uses_borrowed_io = self.runtime.usesBorrowedIo(), .runnable_owner = self.runtime.runnableOwner() };
    }
    fn ioView(raw: *anyopaque, purpose_value: Purpose) !io_abi.Borrow {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const slot = &self.io_slots[@backingInt(purpose_value)];
        if (slot.* == null) slot.* = self.chooseIo(purpose_value) orelse return error.BackendRuntimeUnavailable;
        return io_abi.Borrow.init(&slot.*.?);
    }
    fn open(raw: *anyopaque) !*anyopaque {
        const runtime: *Runtime = @ptrCast(@alignCast(raw));
        var retention = try runtime.retainExecutionServices();
        errdefer retention.release();
        const io = runtime.io() orelse runtime.filesystemIo() orelse return error.BackendRuntimeUnavailable;
        const scope = try runtime.alloc.create(Scope);
        scope.* = .{ .runtime = runtime, .retention = retention, .io = io, .allocator = runtime.alloc };
        return scope;
    }
    fn quiesce(raw: *anyopaque) !void {
        const self = cast(raw);
        try stopMaintenance(raw);
        self.mutex.lockUncancelable(self.io);
        self.closing = true;
        while (self.calls != 0) self.changed.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
        // Registrations stop growing once closing is published. Never hold the
        // scope mutex across a join: a job may close a nested owner here.
        for (self.owners.items) |owner| self.runtime.durable_jobs.closeOwner(owner);
    }
    fn close(raw: *anyopaque) !void {
        const self = cast(raw);
        try quiesce(raw);
        self.mutex.lockUncancelable(self.io);
        while (self.calls != 0 or self.resources != 0) self.changed.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
        const allocator = self.allocator;
        var retention = self.retention;
        self.owners.deinit(allocator);
        allocator.destroy(self);
        // Physical scope destruction precedes the last runtime borrow release.
        retention.release();
    }
    fn workers(raw: *anyopaque, options: Runtime.WorkerOptions) !Resource {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        const allocator = options.allocator orelse self.allocator;
        const token = try allocator.create(Token);
        errdefer allocator.destroy(token);
        const lease = try self.runtime.acquireWorkers(options);
        token.* = .{ .scope = self, .allocator = allocator, .inner = .{ .workers = lease }, .io = lease.io(), .capacity = lease.capacity };
        return self.publish(token);
    }
    fn purpose(raw: *anyopaque, purpose_value: Purpose) !Resource {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        const allocator = if (self.runtime.runtime_memory) |memory| switch (purpose_value) {
            .control => memory.controlAllocator(),
            .raft_inbound => memory.raftAllocator(.inbound),
            .raft_outbound => memory.raftAllocator(.outbound),
            else => self.allocator,
        } else self.allocator;
        const token = try allocator.create(Token);
        errdefer allocator.destroy(token);
        token.* = .{ .scope = self, .allocator = allocator, .inner = undefined, .io = undefined, .capacity = 0 };
        switch (purpose_value) {
            .api, .api_network, .api_filesystem => {
                const lease = try self.runtime.acquireApiLane();
                token.inner = .{ .api = lease };
                token.io = switch (purpose_value) {
                    .api_network => self.runtime.apiNetworkIo().?,
                    .api_filesystem => self.runtime.apiFilesystemIo().?,
                    else => lease.io(),
                };
                token.capacity = lease.concurrentCapacity();
            },
            .inference => {
                const lease = try self.runtime.acquireInferenceLane();
                token.inner = .{ .inference = lease };
                token.io = lease.io();
                token.capacity = lease.concurrentCapacity();
            },
            .control => {
                const lease = try self.runtime.acquireControlLane();
                token.inner = .{ .control = lease };
                token.io = lease.io();
                token.capacity = lease.concurrentCapacity();
            },
            .raft_outbound => {
                const lease = try self.runtime.acquireRaftOutboundLane();
                token.inner = .{ .raft_outbound = lease };
                token.io = lease.io();
                token.capacity = @import("antfly_runtime_fs").threaded_io_limits.raft_outbound_workers_per_request;
            },
            .request_forward => {
                const lease = try self.runtime.acquireRequestForwardLane();
                token.inner = .{ .request_forward = lease };
                token.io = lease.io();
                token.capacity = self.runtime.lane_limits.request_forward;
            },
            else => {
                const io = switch (purpose_value) {
                    .general => self.runtime.io(),
                    .filesystem => self.runtime.filesystemIo(),
                    .raft_inbound => self.runtime.raftInboundIo(),
                    .raft_outbound => self.runtime.raftOutboundIo(),
                    else => unreachable,
                } orelse return error.BackendRuntimeUnavailable;
                token.inner = .borrowed;
                token.io = io;
                token.capacity = switch (purpose_value) {
                    .raft_inbound => self.runtime.lane_limits.raft_inbound,
                    .raft_outbound => self.runtime.lane_limits.raft_outbound,
                    else => self.runtime.lane_limits.durable_background,
                };
            },
        }
        return self.publish(token);
    }
    fn sql(raw: *anyopaque, role: Runtime.SharedSqlRole, minimum: usize) !Resource {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        const allocator = if (self.runtime.shared_sql_memory) |bank| bank.allocator(role) else self.allocator;
        const token = try allocator.create(Token);
        errdefer allocator.destroy(token);
        const lease = try self.runtime.acquireSharedSqlWorkers(role, minimum);
        token.* = .{ .scope = self, .allocator = allocator, .inner = .{ .sql = lease }, .io = lease.io(), .capacity = lease.capacity };
        return self.publish(token);
    }
    fn release(raw: *anyopaque, resource: *anyopaque, cancel: bool) !void {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        const token = try self.detach(resource);
        if (token.inner == .raft_outbound) {
            // This permit only borrows the runtime Io and its tasks have joined.
            // Metadata may use its memory bank, so retire the header before the
            // final native permit can unblock owner shutdown.
            var lease = token.inner.raft_outbound;
            const allocator = token.allocator;
            allocator.destroy(token);
            lease.release();
        } else {
            switch (token.inner) {
                inline .workers, .sql, .api, .inference, .control, .request_forward, .pdf => |*lease| lease.release(),
                .maintenance => |*registration| if (cancel) registration.cancel(self.io) else registration.await(self.io),
                .maintenance_pending, .raft_outbound => unreachable,
                .borrowed => {},
            }
            const allocator = token.allocator;
            allocator.destroy(token);
        }
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        std.debug.assert(self.resources > 0);
        self.resources -= 1;
        self.changed.broadcast(self.io);
    }
    fn pdfLease(raw: *anyopaque) !Resource {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        const token = try self.allocator.create(Token);
        errdefer self.allocator.destroy(token);
        const lease = try self.runtime.acquirePdfRenderLane();
        token.* = .{ .scope = self, .allocator = self.allocator, .inner = .{ .pdf = lease }, .io = self.io, .capacity = lease.concurrentCapacity() };
        return self.publish(token);
    }
    fn pdfBatch(raw: *anyopaque, resource: *anyopaque, contexts: []const *anyopaque, run: *const fn (*anyopaque, Allocator) void, scratch: usize) !pdf.PageRenderExecutor.BatchStats {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        const token = try self.checkedToken(resource);
        if (std.meta.activeTag(token.inner) != .pdf) return error.InvalidArgument;
        return token.inner.pdf.pageExecutor().runBatch(contexts, run, scratch);
    }
    fn maintenance(raw: *anyopaque, spec: Maintenance) !Resource {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        if ((spec.step == null) == (spec.runnable_step == null) or (spec.runnable_step != null) != (spec.pass != null)) return error.InvalidArgument;
        const scheduler = try self.runtime.maintenanceScheduler();
        const token = try self.allocator.create(Token);
        errdefer self.allocator.destroy(token);
        token.* = .{ .scope = self, .allocator = self.allocator, .inner = .maintenance_pending, .io = self.io, .capacity = 1, .maintenance_spec = spec };
        // Publish the wake identity before the scheduler can invoke a callback.
        // A callback may notify itself before register returns to the caller.
        self.mutex.lockUncancelable(self.io);
        if (self.maintenance_closing) {
            self.mutex.unlock(self.io);
            return error.BackendRuntimeShuttingDown;
        }
        token.next = self.resource_head;
        self.resource_head = token;
        self.resources += 1;
        self.mutex.unlock(self.io);
        errdefer {
            _ = self.detach(token) catch unreachable;
            self.mutex.lockUncancelable(self.io);
            self.resources -= 1;
            self.changed.broadcast(self.io);
            self.mutex.unlock(self.io);
        }
        const registration = if (spec.pass) |requested| blk: {
            var pass = requested;
            pass.retained_bytes = std.math.add(u64, pass.retained_bytes, @sizeOf(Token)) catch return error.InvalidArgument;
            break :blk try scheduler.registerRunnableClass(spec.class, pass, token, Token.runnableStep);
        } else try scheduler.registerClass(spec.class, token, Token.simpleStep);
        self.mutex.lockUncancelable(self.io);
        token.inner = .{ .maintenance = registration };
        if (token.stop_requested.load(.acquire)) token.inner.maintenance.requestStop();
        self.mutex.unlock(self.io);
        return self.describe(token);
    }
    fn wakeMaintenance(raw: *anyopaque, context_value: *anyopaque) !void {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var it = self.resource_head;
        while (it) |token| : (it = token.next) {
            if (token.maintenance_spec) |spec| if (spec.context == context_value) self.runtime.wakeMaintenance(token);
        }
    }
    fn ensureMaintenance(raw: *anyopaque) !Scheduler.Stats {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        return (try self.runtime.maintenanceScheduler()).snapshot();
    }
    fn stopMaintenance(raw: *anyopaque) !void {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.maintenance_closing = true;
        var it = self.resource_head;
        while (it) |token| : (it = token.next) if (token.maintenance_spec != null) {
            token.stop_requested.store(true, .release);
            if (std.meta.activeTag(token.inner) == .maintenance) token.inner.maintenance.requestStop();
        };
    }
    fn stopRegistration(raw: *anyopaque, handle: *anyopaque) !void {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var it = self.resource_head;
        while (it) |token| : (it = token.next) if (@as(*anyopaque, token) == handle) {
            if (token.maintenance_spec == null) return error.InvalidArgument;
            token.stop_requested.store(true, .release);
            if (std.meta.activeTag(token.inner) == .maintenance) token.inner.maintenance.requestStop();
            return;
        };
        return error.InvalidArgument;
    }
    fn registrationReady(raw: *anyopaque, handle: *anyopaque) !bool {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var it = self.resource_head;
        while (it) |token| : (it = token.next) if (@as(*anyopaque, token) == handle) {
            if (token.maintenance_spec == null) return error.InvalidArgument;
            return switch (token.inner) {
                .maintenance_pending => false,
                .maintenance => |*registration| registration.readyToFinish(),
                else => error.InvalidArgument,
            };
        };
        return error.InvalidArgument;
    }
    fn maintenanceStats(raw: *anyopaque) !?Scheduler.Stats {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        const scheduler = self.runtime.maintenance_scheduler.load(.acquire) orelse return null;
        return scheduler.snapshot();
    }
    fn allocateOwner(raw: *anyopaque) !u64 {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        try self.owners.ensureUnusedCapacity(self.allocator, 1);
        const id = try self.runtime.allocOwnerId();
        self.owners.appendAssumeCapacity(id);
        return id;
    }
    fn jobs(raw: *anyopaque) !background.DurableJobLane {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        return .{ .ptr = self, .vtable = if (self.runtime.durable_jobs.executesInline()) &inline_jobs else &threaded_jobs };
    }
    fn submit(raw: *anyopaque, job: background.Job) !void {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        try self.checkOwner(job.owner_id);
        try self.runtime.durable_jobs.submit(job);
    }
    fn ownerAction(raw: *anyopaque, id: u64, action: OwnerAction) !void {
        const self = cast(raw);
        try self.begin(action == .drain or action == .close);
        defer self.end();
        try self.checkOwner(id);
        switch (action) {
            .drain => self.runtime.durable_jobs.drainOwner(id),
            .close => self.runtime.durable_jobs.closeOwner(id),
            .pause => try self.runtime.durable_jobs.pauseOwner(id),
            .resume_owner => try self.runtime.durable_jobs.resumeOwner(id),
            .reopen => try self.runtime.durable_jobs.reopenOwner(id),
        }
    }
    fn waitOwner(raw: *anyopaque, id: u64) !bool {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        try self.checkOwner(id);
        return self.runtime.durable_jobs.waitOwner(id);
    }
    fn quiesceOwner(raw: *anyopaque, id: u64) !bool {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        try self.checkOwner(id);
        return self.runtime.durable_jobs.quiesceOwner(id);
    }
    fn drainOwner(raw: *anyopaque, id: u64) void {
        ownerAction(raw, id, .drain) catch {};
    }
    fn closeOwner(raw: *anyopaque, id: u64) void {
        ownerAction(raw, id, .close) catch {};
    }
    fn pauseOwner(raw: *anyopaque, id: u64) !void {
        return ownerAction(raw, id, .pause);
    }
    fn resumeOwner(raw: *anyopaque, id: u64) !void {
        return ownerAction(raw, id, .resume_owner);
    }
    fn reopenOwner(raw: *anyopaque, id: u64) !void {
        return ownerAction(raw, id, .reopen);
    }
    fn poll(raw: *anyopaque, maximum: usize) !usize {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        return self.runtime.durable_jobs.poll(maximum);
    }
    fn accepting(raw: *anyopaque) bool {
        const self = cast(raw);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return !self.closing and self.runtime.durable_jobs.isAccepting();
    }
    fn armProbe(raw: *anyopaque, id: u64, probe: background.OwnerMaintenanceProbe) !void {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        try self.checkOwner(id);
        try self.runtime.armOwnerMaintenanceProbe(id, probe);
    }
    fn disarmProbe(raw: *anyopaque, id: u64) !void {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        try self.checkOwner(id);
        self.runtime.disarmOwnerMaintenanceProbe(id);
    }
    fn stats(raw: *anyopaque) !Runtime.LaneStats {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        return self.runtime.laneStats();
    }
    fn sqlStats(raw: *anyopaque, role: Runtime.SharedSqlRole) !Runtime.SharedSqlStats {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        return self.runtime.sharedSqlStats(role);
    }
    fn sqlMemory(raw: *anyopaque, role: Runtime.SharedSqlRole, stacks: bool) !?background.SqlWorkerMemory.Bank.Stats {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        return if (stacks) self.runtime.sharedSqlStackStats(role) else self.runtime.sharedSqlMemoryStats(role);
    }
    fn installSql(raw: *anyopaque, parent: runtime_abi.memory_abi.Parent, limits: background.SqlWorkerMemory.Limits) !void {
        const self = cast(raw);
        try self.begin(false);
        defer self.end();
        return self.runtime.installSharedSqlMemory(parent, limits);
    }
    fn pdfStats(raw: *anyopaque) !?@import("../common/bounded_worker_lane.zig").Stats {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        return self.runtime.pdfRenderExecutorStats();
    }
    fn memoryStats(raw: *anyopaque) !?Runtime.RuntimeMemoryStats {
        const self = cast(raw);
        try self.begin(true);
        defer self.end();
        return self.runtime.runtimeMemoryStats();
    }
};
const Token = struct {
    next: ?*Token = null,
    client_state: ClientState = undefined,
    maintenance_spec: ?Maintenance = null,
    stop_requested: std.atomic.Value(bool) = .init(false),
    scope: *Scope,
    allocator: Allocator,
    io: std.Io,
    capacity: usize,
    inner: union(enum) {
        workers: Runtime.WorkerLease,
        sql: Runtime.SharedSqlLease,
        api: Runtime.ApiLaneLease,
        inference: Runtime.InferenceLaneLease,
        control: Runtime.ControlLaneLease,
        request_forward: Runtime.RequestForwardLaneLease,
        raft_outbound: Runtime.RaftOutboundLaneLease,
        pdf: Runtime.PdfRenderLaneLease,
        maintenance: Scheduler.Handle,
        maintenance_pending,
        borrowed,
    },
    fn simpleStep(self: *Token) ?u64 {
        if (self.stop_requested.load(.acquire)) return null;
        const spec = self.maintenance_spec.?;
        return spec.step.?(spec.context);
    }
    fn runnableStep(self: *Token, borrow: runtime_abi.runnable_abi.Borrowed) !?u64 {
        if (self.stop_requested.load(.acquire)) return null;
        const spec = self.maintenance_spec.?;
        return RunnableBoundary.call("step", spec.callback_dispatch, spec.runnable_step.?, .{ spec.context, borrow });
    }
};
const threaded_jobs: background.DurableJobLane.VTable = .{
    .submit = Scope.submit,
    .wait_owner = Scope.waitOwner,
    .quiesce_owner = Scope.quiesceOwner,
    .drain_owner = Scope.drainOwner,
    .close_owner = Scope.closeOwner,
    .pause_owner = Scope.pauseOwner,
    .resume_owner = Scope.resumeOwner,
    .reopen_owner = Scope.reopenOwner,
    .poll = Scope.poll,
    .is_accepting = Scope.accepting,
};
const inline_jobs: background.DurableJobLane.VTable = blk: {
    var value = threaded_jobs;
    value.executes_inline = true;
    break :blk value;
};
const producer_methods: Methods = .{
    .open = Scope.open,
    .close = Scope.close,
    .workers = Scope.workers,
    .purpose = Scope.purpose,
    .sql = Scope.sql,
    .release = Scope.release,
    .info = Scope.info,
    .io_view = Scope.ioView,
    .pdf = Scope.pdfLease,
    .pdf_batch = Scope.pdfBatch,
    .maintenance = Scope.maintenance,
    .wake_maintenance = Scope.wakeMaintenance,
    .maintenance_stats = Scope.maintenanceStats,
    .ensure_maintenance = Scope.ensureMaintenance,
    .stop_maintenance = Scope.stopMaintenance,
    .stop_registration = Scope.stopRegistration,
    .registration_ready = Scope.registrationReady,
    .owner = Scope.allocateOwner,
    .jobs = Scope.jobs,
    .arm_probe = Scope.armProbe,
    .disarm_probe = Scope.disarmProbe,
    .stats = Scope.stats,
    .memory_stats = Scope.memoryStats,
    .quiesce = Scope.quiesce,
    .sql_stats = Scope.sqlStats,
    .sql_memory = Scope.sqlMemory,
    .install_sql = Scope.installSql,
    .pdf_stats = Scope.pdfStats,
};
