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

const std = @import("std");
const rpc = @import("retained_read_rpc.zig");
const registry = @import("../storage/retained_read_registry.zig");
const reads = @import("antfly_local_sources").api_table_read_source;
const types = @import("antfly_local_sources").storage_db_types;
const metadata = @import("../metadata/api.zig");
const http = @import("antfly_local_sources").common_http_http_common;
const Admission = reads.RelationalReadView.Admission;
const time = @import("antfly_platform").time;
const retirement = @import("retained_read_retirement.zig");
const attempts = @import("../common/remote_attempts.zig");
const protocol_mod = @import("remote_attempt_client.zig");

/// Trusted node-owned protocol/transport descriptors. Binding.parent must pin
/// their entire allocator, ledger, store, target and executor lifetime chain.
/// Copies never save a foreground admission Port or query Dispatcher.
const dispatch = @import("retained_query_dispatch.zig");
pub const Signed = dispatch.SignedRead;

/// Owns endpoint and catalog capability bytes. Executor must be a stable
/// server-owned authenticated transport, never a stack-built hosted adapter.
pub const Client = struct {
    alloc: std.mem.Allocator,
    executor: http.RequestExecutor,
    uri: []u8,
    route: []u8,
    schema_version: u32,
    deadline_ns: u64,
    token: registry.Token,
    connection: u128,
    sequence: u64 = 0,
    poisoned: bool = false,
    managed: ?retirement.Registry.Ticket = null,
    binding: ?retirement.Binding = null,
    metadata_alloc: ?std.mem.Allocator = null,
    token_known: bool = false,
    accepts_busy: bool = false,
    creation_absent: bool = false,
    signed: ?Signed = null,
    creation_id: ?attempts.Id = null,
    references: std.atomic.Value(usize) = .init(1),
    closed: std.atomic.Value(bool) = .init(false),

    fn create(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64) !*Client {
        // Table names are escaped at the path boundary, not interpolated raw.
        const escaped = try @import("http_client.zig").percentEncodePathComponent(alloc, table);
        defer alloc.free(escaped);
        const self = try alloc.create(Client);
        errdefer alloc.destroy(self);
        const uri = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/{d}/tables/{s}/retained-read", .{ std.mem.trimEnd(u8, base_uri, "/"), route.route.group_id, escaped });
        errdefer alloc.free(uri);
        const encoded_route = try std.json.Stringify.valueAlloc(alloc, route, .{});
        errdefer alloc.free(encoded_route);
        const clock = executor.clock_io orelse return error.ReadUnavailable;
        var receiver = try clock.receive();
        var nonce: [16]u8 = undefined;
        try receiver.io().randomSecure(&nonce);
        const connection = std.mem.readInt(u128, &nonce, .little);
        if (connection == 0) return error.ReadUnavailable;
        self.* = .{ .alloc = alloc, .executor = executor, .uri = uri, .route = encoded_route, .schema_version = schema_version, .deadline_ns = deadline_ns orelse time.monotonicNs() +| 30 * std.time.ns_per_s, .token = undefined, .connection = connection };
        return self;
    }

    fn cloneManaged(self: *Client) !*Client {
        const binding = self.binding.?;
        const bytes = std.math.add(usize, @sizeOf(Client), try std.math.add(usize, self.uri.len, self.route.len)) catch return error.CleanupCapacityUnavailable;
        const ticket = try binding.registry.reserve(binding.parent, bytes);
        errdefer ticket.abort() catch unreachable;
        const alloc = ticket.allocator();
        const value = try alloc.create(Client);
        errdefer alloc.destroy(value);
        const uri = try alloc.dupe(u8, self.uri);
        errdefer alloc.free(uri);
        const route = try alloc.dupe(u8, self.route);
        errdefer alloc.free(route);
        value.* = .{ .alloc = self.alloc, .executor = self.executor, .uri = uri, .route = route, .schema_version = self.schema_version, .deadline_ns = self.deadline_ns, .token = undefined, .connection = self.connection, .managed = ticket, .binding = binding, .metadata_alloc = alloc, .signed = self.signed };
        try ticket.bind(.{ .ptr = value, .poll = pollManaged, .reconcile = pollManaged, .destroy = destroyManaged, .observe = observeManaged });
        return value;
    }
    fn observeManaged(raw: *anyopaque, bytes: []const u8) void {
        if (bytes.len > 4096) return;
        const self: *Client = @ptrCast(@alignCast(raw));
        var storage: [32 * 1024]u8 = undefined;
        var arena = std.heap.FixedBufferAllocator.init(&storage);
        const value = std.json.parseFromSliceLeaky(rpc.Response, arena.allocator(), bytes, .{}) catch return;
        if (value.cleanup != null or value.rows.len != 0 or value.writes.len != 0 or value.range_proofs.len != 0 or value.after != null or value.sequence != 0) return;
        if (value.busy) {
            if (self.accepts_busy and value.token == null) self.creation_absent = true;
        } else if (value.token) |token| {
            if (token.incarnation == 0 or token.sequence == 0 or token.slot >= 4096) return;
            self.token = token;
            self.token_known = true;
        }
    }
    fn pollManaged(raw: *anyopaque, poll: retirement.Poll) !retirement.Outcome {
        const self: *Client = @ptrCast(@alignCast(raw));
        if (self.signed != null) return self.pollSigned(poll);
        if (!self.token_known) return .needs_fence;
        const outcome = self.retireTokenScoped(self.token, poll.allocator, poll.control) catch |err| switch (err) {
            error.RetainedReadRestartRequired, error.UnsupportedVersion => return .needs_fence,
            else => return err,
        };
        return if (outcome == .retired) .retired else .pending;
    }
    fn destroyManaged(raw: *anyopaque) void {
        const self: *Client = @ptrCast(@alignCast(raw));
        self.closed.store(true, .release);
        self.destroy();
    }
    fn discardCreation(self: *Client) void {
        self.closed.store(true, .release);
        if (self.managed) |ticket| {
            if (self.creation_absent) {
                ticket.acknowledgeNotCreated() catch unreachable;
                return;
            }
            ticket.abandon() catch unreachable;
            return;
        }
        self.cancelUnknown();
        self.destroy();
    }

    fn destroy(self: *Client) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        const alloc = self.metadata_alloc orelse self.alloc;
        alloc.free(self.uri);
        alloc.free(self.route);
        alloc.destroy(self);
    }

    fn retainAttempt(self: *Client, remote_bytes: usize) !attempts.Retention {
        const parent = self.binding.?.parent;
        const metadata_bytes = try std.math.add(usize, @sizeOf(Client), try std.math.add(usize, self.uri.len, self.route.len));
        const bytes = try std.math.add(usize, remote_bytes, metadata_bytes);
        try parent.acquire(bytes);
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        return .{ .context = self, .index = bytes, .id = .{ .coordinator = 0, .generation = 0, .operation = 0, .sequence = 0 }, .release_fn = releaseAttempt, .cancellation = &self.closed, .deadline_ns = self.deadline_ns };
    }
    fn releaseAttempt(raw: *anyopaque, bytes: usize, _: attempts.Id) void {
        const self: *Client = @ptrCast(@alignCast(raw));
        const parent = self.binding.?.parent;
        // Metadata is destroyed before the final full-chain parent release.
        self.destroy();
        parent.retire(bytes);
    }
    fn signedPath(self: *Client, descriptor: Signed) ![]const u8 {
        if (descriptor.destination >= descriptor.client.targets.len) return error.InvalidRemoteAttemptTarget;
        const base = std.mem.trimEnd(u8, descriptor.client.targets[descriptor.destination].base_uri, "/");
        if (self.uri.len <= base.len or !std.mem.startsWith(u8, self.uri, base) or self.uri[base.len] != '/') return error.InvalidRemoteAttemptTarget;
        return self.uri[base.len..];
    }
    fn callSigned(self: *Client, alloc: std.mem.Allocator, request: http.HttpRequest, creation: ?*Client, cleanup: bool, deadline_ns: u64) !http.HttpResponse {
        const descriptor = self.signed.?;
        var protocol = descriptor.client;
        var outgoing = request;
        outgoing.uri = self.signedPath(descriptor) catch |err| {
            if (creation) |owner| owner.creation_absent = true;
            return err;
        };
        protocol.read_allocator = alloc;
        protocol.control_allocator = alloc;
        protocol.control_wait = request.execution_wait;
        if (cleanup) return protocol.cleanupRetainedRead(descriptor.destination, self.creation_id orelse return error.InvalidRemoteAttempt, deadline_ns, outgoing, self.route);
        const parent_owner = creation orelse self;
        var parent: ?attempts.Retention = parent_owner.retainAttempt(descriptor.charged_bytes) catch |err| {
            if (creation) |owner| owner.creation_absent = true;
            return err;
        };
        defer if (parent) |owned| owned.release();
        const delivery = protocol.dispatchRetainedRead(descriptor.destination, descriptor.operation, descriptor.charged_bytes, deadline_ns, outgoing, self.route, &parent) catch |err| {
            // Public dispatch errors precede executor entry. A Delivery alone
            // can represent published uncertainty, including local signing OOM.
            if (creation) |owner| owner.creation_absent = true;
            return err;
        };
        const id = switch (delivery) {
            .response => |value| value.id,
            .uncertain => |value| value.id,
        };
        if (creation) |owner| owner.creation_id = id;
        return switch (delivery) {
            .response => |value| value.value,
            .uncertain => |value| value.cause,
        };
    }
    fn pollSigned(self: *Client, poll: retirement.Poll) !retirement.Outcome {
        const descriptor = self.signed.?;
        const id = self.creation_id orelse return .needs_fence;
        var protocol = descriptor.client;
        protocol.control_allocator = poll.allocator;
        protocol.control_permit = poll.control_permit;
        protocol.control_wait = if (poll.control) |port| try port.executionWait() else null;
        const deadline_ns = if (poll.control) |port| try port.deadlineNs() orelse return error.InvalidArgument else time.monotonicNs() +| 100 * std.time.ns_per_ms;
        var cleanup_call: Call = .{ .control = poll.control };
        var cancellation: http.RequestCancellation = .{ .execution_deadline_ns = deadline_ns, .borrowed_context = &cleanup_call, .borrowed_is_cancelled = Call.canceled };
        // Cancellation seals unknown creation before connection revocation. A
        // still-active attempt keeps every charge until its resources die.
        const proof = try protocol.retireAttempt(descriptor.destination, id, deadline_ns, &cancellation);
        if (proof != .retained) return .retired;
        try cleanup_call.check();
        const input: rpc.Request = if (self.token_known)
            .{ .operation = .close, .schema_version = self.schema_version, .token = self.token }
        else
            .{ .operation = .cancel, .schema_version = self.schema_version, .connection = self.connection };
        const body = try std.json.Stringify.valueAlloc(poll.allocator, input, .{});
        defer poll.allocator.free(body);
        const path = try self.signedPath(descriptor);
        var response = protocol.cleanupRetainedRead(descriptor.destination, id, deadline_ns, .{ .method = .POST, .uri = path, .body = body, .cancellation = &cancellation, .execution_wait = protocol.control_wait }, self.route) catch return .pending;
        defer response.deinit(poll.allocator);
        // Even an exact cleanup ACK cannot substitute for signed parent proof.
        return if (response.status == 409) .needs_fence else .pending;
    }

    pub fn admit(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64) !void {
        return admitWithAdmission(alloc, executor, base_uri, table, route, schema_version, deadline_ns, null);
    }
    pub fn admitWithAdmission(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64, control: ?Admission) !void {
        if (control) |port| try port.checkpoint();
        const remote = if (control) |port| try port.remoteExecutor() else null;
        const scoped = remote orelse executor;
        if (remote != null and (try dispatch.Dispatcher.retainedReadDescriptor(scoped, base_uri)) == null) return error.UnsupportedRemoteAttemptProtocol;
        const self = try create(alloc, scoped, base_uri, table, route, schema_version, deadline_ns);
        defer self.destroy();
        var response = try self.callWithAdmission(.{ .operation = .admit, .schema_version = schema_version }, control);
        defer response.deinit(alloc);
    }

    pub fn capture(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64) !?reads.StatementReadFence {
        return captureWithAdmission(alloc, executor, base_uri, table, route, schema_version, deadline_ns, null);
    }
    pub fn captureWithAdmission(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64, control: ?Admission) !?reads.StatementReadFence {
        const remote = if (control) |port| try port.remoteExecutor() else null;
        const scoped = remote orelse executor;
        const binding = if (control) |port| try port.retirementBinding() else null;
        if (try dispatch.Dispatcher.retainedReadDescriptor(scoped, base_uri)) |signed|
            return captureSignedOwned(alloc, base_uri, table, route, schema_version, deadline_ns, control, binding orelse return error.CleanupCapacityUnavailable, signed);
        if (remote != null) return error.UnsupportedRemoteAttemptProtocol;
        return captureControlled(alloc, scoped, base_uri, table, route, schema_version, deadline_ns, control, binding, null);
    }
    pub fn captureOwned(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64, control: ?Admission, binding: retirement.Binding) !?reads.StatementReadFence {
        return captureControlled(alloc, executor, base_uri, table, route, schema_version, deadline_ns, control, binding, null);
    }
    pub fn captureSignedOwned(alloc: std.mem.Allocator, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64, control: ?Admission, binding: retirement.Binding, signed: Signed) !?reads.StatementReadFence {
        if (signed.operation == 0 or signed.charged_bytes == 0 or signed.destination >= signed.client.targets.len) return error.InvalidRemoteAttempt;
        var owned = signed;
        owned.client = try signed.client.pinReadCapabilities(signed.destination, true);
        return captureControlled(alloc, owned.client.read_executor, base_uri, table, route, schema_version, deadline_ns, control, binding, owned);
    }
    fn captureControlled(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64, control: ?Admission, binding: ?retirement.Binding, signed: ?Signed) !?reads.StatementReadFence {
        if (control) |port| try port.checkLifetime();
        const initial = try create(alloc, executor, base_uri, table, route, schema_version, deadline_ns);
        initial.signed = signed;
        if (initial.signed) |*descriptor| descriptor.client.control_wait = null;
        const self = if (binding) |owned| value: {
            defer initial.destroy();
            initial.binding = owned;
            break :value try initial.cloneManaged();
        } else initial;
        errdefer self.discardCreation();
        self.accepts_busy = true;
        var response = self.callTracked(.{ .operation = .capture, .schema_version = schema_version, .lease_ms = 5000, .connection = self.connection }, control, if (self.managed != null) self else null) catch |err| {
            // The signed dispatch contract reports errors only before executor
            // entry; uncertain delivery is a separate Delivery variant. Retry
            // capture only after the caller releases every partial fence.
            if (err == error.RemoteAttemptCapacity and self.creation_absent) return error.Backpressured;
            return err;
        };
        defer response.deinit(alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        if (parsed.value.busy and parsed.value.token == null) {
            if (self.managed) |ticket| try ticket.acknowledgeNotCreated() else self.destroy();
            return null;
        }
        self.token = parsed.value.token orelse return error.InvalidRetainedReadResponse;
        self.token_known = true;
        if (control) |port| try port.checkLifetime();
        return .{ .ptr = self, .vtable = &.{ .validate = validate, .validate_with_admission = validateWithAdmission, .open = open, .capture_snapshot = captureSnapshot, .capture_snapshot_with_admission = captureSnapshotWithAdmission, .open_with_admission = openWithAdmission, .release = close } };
    }

    fn call(self: *Client, request: rpc.Request) !http.HttpResponse {
        return self.callWithAdmission(request, null);
    }
    const Call = struct {
        control: ?Admission,
        failure: std.atomic.Value(u16) = .init(0),
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            const control = self.control orelse return false;
            control.checkLifetime() catch |cause| {
                const mutable: *@This() = @constCast(self);
                _ = mutable.failure.cmpxchgStrong(0, @intFromError(cause), .acq_rel, .acquire);
                return true;
            };
            return false;
        }
        fn check(self: *const @This()) !void {
            const code = self.failure.load(.acquire);
            if (code != 0) return @errorFromInt(code);
            if (self.control) |control| try control.checkLifetime();
        }
    };
    fn callWithAdmission(self: *Client, request: rpc.Request, control: ?Admission) !http.HttpResponse {
        return self.callScoped(request, control, self.alloc, null);
    }
    fn callTracked(self: *Client, request: rpc.Request, control: ?Admission, creation: ?*Client) !http.HttpResponse {
        return self.callScoped(request, control, self.alloc, creation);
    }
    fn callScoped(self: *Client, request: rpc.Request, control: ?Admission, alloc: std.mem.Allocator, creation: ?*Client) !http.HttpResponse {
        const publication = if (creation) |owner| owner.managed else null;
        const cleanup = request.operation == .close or request.operation == .cancel;
        const fenced = switch (request.operation) {
            .capture, .validate, .snapshot, .open => true,
            else => false,
        };
        if (self.poisoned and !cleanup) return error.RetainedReadRestartRequired;
        var attempt: Call = .{ .control = control };
        if (attempt.control) |port| {
            if (fenced) try port.checkLifetime() else try port.checkpoint();
            try attempt.check();
        }
        const effective_deadline = if (cleanup and control != null)
            try control.?.deadlineNs() orelse return error.InvalidArgument
        else if (cleanup and self.signed != null) time.monotonicNs() +| 100 * std.time.ns_per_ms else if (attempt.control) |port| @min(self.deadline_ns, try port.deadlineNs() orelse self.deadline_ns) else self.deadline_ns;
        const body = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(body);
        if (body.len > rpc.max_request_bytes) return error.InvalidRetainedReadQuery;
        const now = time.monotonicNs();
        if (now >= effective_deadline and (!cleanup or control != null)) return error.DeadlineExceeded;
        const timeout: u32 = if (cleanup and control == null) 100 else if (cleanup) @intCast(@max(1, @min(100, (effective_deadline -| now) / std.time.ns_per_ms))) else @intCast(@max(1, @min(5000, (effective_deadline -| now) / std.time.ns_per_ms)));
        var timeout_buffer: [16]u8 = undefined;
        const headers = [_]http.RequestHeader{
            .{ .name = "accept", .value = @import("retained_read_page.zig").compact_media_type },
            .{ .name = metadata.catalog_route_fence_header, .value = self.route },
            .{ .name = metadata.catalog_route_deadline_ms_header, .value = try std.fmt.bufPrint(&timeout_buffer, "{d}", .{timeout}) },
        };
        // NEVER retry: capture/open may have allocated a capability; next may
        // already have advanced. Unknown handles are bounded by owner expiry.
        var cancellation: http.RequestCancellation = .{ .execution_deadline_ns = if (cleanup and control == null) null else effective_deadline, .borrowed_context = &attempt, .borrowed_is_cancelled = Call.canceled };
        if (attempt.control) |port| if (fenced) try port.checkLifetime() else try port.checkpoint();
        const outgoing: http.HttpRequest = .{ .method = .POST, .uri = self.uri, .headers = &headers, .content_type = "application/json", .body = body, .timeout_ms = timeout, .max_response_bytes = if (cleanup or publication != null) 4096 else rpc.max_response_bytes, .cancellation = if (control == null) null else &cancellation, .execution_wait = if (attempt.control) |port| (if (fenced) try port.executionWaitAfterLifetime() else try port.executionWait()) else null };
        if (publication) |ticket| try ticket.publish();
        var response = (if (self.signed != null)
            self.callSigned(alloc, outgoing, creation, cleanup, effective_deadline)
        else
            self.executor.execute(alloc, outgoing)) catch |err| {
            self.poisoned = true;
            try attempt.check();
            return err;
        };
        errdefer response.deinit(alloc);
        // Preserve an acknowledged token for cleanup before returning a late
        // original scope error. This observes fixed metadata, never row data.
        if (publication) |ticket| if (response.status == 200 and std.mem.eql(u8, response.header(metadata.catalog_route_fence_ack_header) orelse "", metadata.catalog_route_fence_ack_value))
            try ticket.observe(response.body);
        try attempt.check();
        if (attempt.control) |port| if (fenced) try port.checkLifetime() else try port.checkpoint();
        // This negative capability response is safe to classify without a
        // success acknowledgement. It authorizes no cursor/page reuse: the
        // caller must release captures and explicitly activate via Raft.
        if (response.status == 409 and std.mem.eql(u8, response.body, "SqlRangeTrackingRequired")) {
            self.poisoned = true;
            return error.SqlRangeTrackingRequired;
        }
        if (response.status != 200 or !std.mem.eql(u8, response.header(metadata.catalog_route_fence_ack_header) orelse "", metadata.catalog_route_fence_ack_value)) {
            self.poisoned = true;
            return error.RetainedReadRestartRequired;
        }
        return response;
    }

    pub fn validate(ptr: *anyopaque) !void {
        return validateControlled(ptr, null);
    }
    fn validateWithAdmission(ptr: *anyopaque, control: Admission) !void {
        return validateControlled(ptr, control);
    }
    fn validateControlled(ptr: *anyopaque, control: ?Admission) !void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.callWithAdmission(.{ .operation = .validate, .schema_version = self.schema_version, .token = self.token }, control);
        defer response.deinit(self.alloc);
    }

    fn captureSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
        return captureSnapshotControlled(ptr, alloc, null);
    }
    fn captureSnapshotWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, control: Admission) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
        return captureSnapshotControlled(ptr, alloc, control);
    }
    fn captureSnapshotControlled(ptr: *anyopaque, alloc: std.mem.Allocator, control: ?Admission) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
        if (control) |port| try port.checkpoint();
        const self: *Client = @ptrCast(@alignCast(ptr));
        if (self.managed != null) return self.captureSnapshotOwned(alloc, control);
        var response = try self.callWithAdmission(.{ .operation = .snapshot, .schema_version = self.schema_version, .token = self.token, .connection = self.connection, .lease_ms = rpc.max_lease_ms }, control);
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        const token = parsed.value.token orelse return error.InvalidRetainedReadResponse;
        errdefer _ = self.retireToken(token) catch {};
        const snapshot = try alloc.create(Client);
        errdefer alloc.destroy(snapshot);
        const uri = try alloc.dupe(u8, self.uri);
        errdefer alloc.free(uri);
        const route = try alloc.dupe(u8, self.route);
        errdefer alloc.free(route);
        if (control) |port| try port.checkpoint();
        snapshot.* = .{ .alloc = alloc, .executor = self.executor, .uri = uri, .route = route, .schema_version = self.schema_version, .deadline_ns = self.deadline_ns, .token = token, .connection = self.connection };
        return .{ .ptr = snapshot, .vtable = &.{ .open = openSnapshot, .open_with_admission = openSnapshotWithAdmission, .release = close } };
    }

    fn open(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.openKind(alloc, from, to, opts, false, null);
    }

    fn openSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.openKind(alloc, from, to, opts, true, null);
    }

    fn openWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, control: Admission) !reads.RelationalReadView {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.openKind(alloc, from, to, opts, false, control);
    }
    fn openSnapshotWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, control: Admission) !reads.RelationalReadView {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.openKind(alloc, from, to, opts, true, control);
    }
    fn openKind(self: *Client, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, delayed: bool, control: ?Admission) !reads.RelationalReadView {
        if (control) |port| try port.checkpoint();
        var parsed_query: ?std.json.Parsed(types.RelationalRowQuery) = null;
        defer if (parsed_query) |*parsed| parsed.deinit();
        const query = opts.relational_query orelse blk: {
            parsed_query = try std.json.parseFromSlice(types.RelationalRowQuery, alloc, opts.relational_query_json, .{ .parse_numbers = false });
            break :blk parsed_query.?.value;
        };
        if (self.managed != null) return self.openOwned(alloc, from, to, opts, query, delayed, control);
        // Preallocate the local owner before creating the remote capability.
        const cursor = try alloc.create(Client);
        errdefer alloc.destroy(cursor);
        const uri = try alloc.dupe(u8, self.uri);
        errdefer alloc.free(uri);
        const route = try alloc.dupe(u8, self.route);
        errdefer alloc.free(route);
        errdefer self.cancelUnknown();
        var response = try self.callWithAdmission(.{ .operation = if (delayed) .open_snapshot else .open, .schema_version = self.schema_version, .token = self.token, .connection = self.connection, .lease_ms = rpc.max_lease_ms, .from = from, .to = to, .query = query, .filter_query_json = opts.filter_query_json, .inclusive_from = opts.inclusive_from, .exclusive_to = opts.exclusive_to, .sql_document_preimage = opts.sql_document_preimage, .include_content_hashes = opts.include_content_hashes, .include_range_proofs = opts.include_range_proofs, .row_policy_principal_proof = opts.row_policy_principal_proof, .row_policy_database = opts.row_policy_database }, control);
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        if (control) |port| try port.checkpoint();
        cursor.* = .{ .alloc = alloc, .executor = self.executor, .uri = uri, .route = route, .schema_version = self.schema_version, .deadline_ns = self.deadline_ns, .token = parsed.value.token orelse return error.InvalidRetainedReadResponse, .connection = self.connection };
        return .{ .ptr = cursor, .vtable = &.{ .next = next, .next_with_admission = nextWithAdmission, .close = close, .normalize = normalize, .normalize_with_admission = normalizeWithAdmission, .range_proofs = rangeProofs, .range_proofs_with_admission = rangeProofsWithAdmission } };
    }

    fn captureSnapshotOwned(self: *Client, alloc: std.mem.Allocator, control: ?Admission) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
        const child = try self.cloneManaged();
        errdefer child.discardCreation();
        var response = try self.callTracked(.{ .operation = .snapshot, .schema_version = self.schema_version, .token = self.token, .connection = self.connection, .lease_ms = rpc.max_lease_ms }, control, child);
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        child.token = parsed.value.token orelse return error.InvalidRetainedReadResponse;
        child.token_known = true;
        if (control) |port| try port.checkpoint();
        return .{ .ptr = child, .vtable = &.{ .open = openSnapshot, .open_with_admission = openSnapshotWithAdmission, .release = close } };
    }
    fn openOwned(self: *Client, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, query: types.RelationalRowQuery, delayed: bool, control: ?Admission) !reads.RelationalReadView {
        const child = try self.cloneManaged();
        errdefer child.discardCreation();
        var response = try self.callTracked(.{ .operation = if (delayed) .open_snapshot else .open, .schema_version = self.schema_version, .token = self.token, .connection = self.connection, .lease_ms = rpc.max_lease_ms, .from = from, .to = to, .query = query, .filter_query_json = opts.filter_query_json, .inclusive_from = opts.inclusive_from, .exclusive_to = opts.exclusive_to, .sql_document_preimage = opts.sql_document_preimage, .include_content_hashes = opts.include_content_hashes, .include_range_proofs = opts.include_range_proofs, .row_policy_principal_proof = opts.row_policy_principal_proof, .row_policy_database = opts.row_policy_database }, control, child);
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        child.token = parsed.value.token orelse return error.InvalidRetainedReadResponse;
        child.token_known = true;
        if (control) |port| try port.checkpoint();
        return .{ .ptr = child, .vtable = &.{ .next = next, .next_with_admission = nextWithAdmission, .close = close, .normalize = normalize, .normalize_with_admission = normalizeWithAdmission, .range_proofs = rangeProofs, .range_proofs_with_admission = rangeProofsWithAdmission } };
    }

    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !reads.RelationalReadView.Page {
        return nextControlled(ptr, alloc, limit, null);
    }
    fn nextWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32, control: Admission) !reads.RelationalReadView.Page {
        return nextControlled(ptr, alloc, limit, control);
    }
    fn nextControlled(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32, control: ?Admission) !reads.RelationalReadView.Page {
        if (control) |port| try port.checkpoint();
        const self: *Client = @ptrCast(@alignCast(ptr));
        errdefer self.poisoned = true;
        var response = try self.callWithAdmission(.{ .operation = .next, .schema_version = self.schema_version, .token = self.token, .sequence = self.sequence, .limit = limit }, control);
        defer response.deinit(self.alloc);
        var page = try rpc.decodePage(alloc, response.body, self.sequence, self.schema_version, limit);
        errdefer page.deinit();
        if (control) |port| try port.checkpoint();
        self.sequence += 1;
        return page;
    }

    /// A successful routing ACK alone is not a retirement proof. Callers that
    /// own scheduler/recovery charges must retain them on pending or error.
    /// Evidence covers only this token, not sibling/child capabilities or a
    /// signed parent/generation fence. The request is idempotent cleanup; it
    /// never replays a capture or page.
    pub fn closeAcknowledged(self: *Client) !registry.Retirement {
        return self.retireToken(self.token);
    }
    fn retireToken(self: *Client, token: registry.Token) !registry.Retirement {
        return self.retireTokenScoped(token, self.alloc, null);
    }
    fn retireTokenScoped(self: *Client, token: registry.Token, alloc: std.mem.Allocator, control: ?Admission) !registry.Retirement {
        var response = try self.callScoped(.{ .operation = .close, .schema_version = self.schema_version, .token = token }, control, alloc, null);
        defer response.deinit(alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        const value = parsed.value;
        const evidence = value.cleanup orelse return error.RetainedReadRestartRequired;
        if (evidence.version != 1) return error.UnsupportedVersion;
        if (evidence.incarnation != token.incarnation or evidence.connection != 0 or
            !std.meta.eql(evidence.token orelse return error.InvalidRetainedReadResponse, token) or
            evidence.state == .revoked or value.token != null or value.busy or value.sequence != 0 or
            value.rows.len != 0 or value.range_proofs.len != 0 or value.writes.len != 0 or value.after != null)
            return error.InvalidRetainedReadResponse;
        return if (evidence.state == .retired) .retired else .pending;
    }

    fn close(ptr: *anyopaque) void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        self.closed.store(true, .release);
        if (self.managed) |ticket| {
            ticket.transfer() catch unreachable;
            return;
        }
        // Legacy view destruction remains best effort. It cannot authorize a
        // signed owned-read root to retire remote charges; that adoption must
        // use closeAcknowledged() and retain/transfer unresolved ownership instead.
        _ = self.closeAcknowledged() catch {};
        self.destroy();
    }

    fn rangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]@import("antfly_local_sources").storage_range_protection.Proof {
        return rangeProofsControlled(ptr, alloc, null);
    }
    fn rangeProofsWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, control: Admission) ![]@import("antfly_local_sources").storage_range_protection.Proof {
        return rangeProofsControlled(ptr, alloc, control);
    }
    fn rangeProofsControlled(ptr: *anyopaque, alloc: std.mem.Allocator, control: ?Admission) ![]@import("antfly_local_sources").storage_range_protection.Proof {
        if (control) |port| try port.checkpoint();
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.callWithAdmission(.{ .operation = .range_proofs, .schema_version = self.schema_version, .token = self.token }, control);
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, self.alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        const proofs = parsed.value.range_proofs;
        if (proofs.len == 0 or proofs.len > @import("antfly_local_sources").api_range_read_guards.max_proofs) return error.InvalidRetainedReadResponse;
        for (proofs, 0..) |proof, i| {
            if (control) |port| try port.checkpoint();
            const tracking = @import("antfly_local_sources").storage_range_protection;
            tracking.validateProof(proof) catch return error.InvalidRetainedReadResponse;
            if (i != 0 and !tracking.proofLess(proofs[i - 1], proof)) return error.InvalidRetainedReadResponse;
        }
        const result = try alloc.dupe(@import("antfly_local_sources").storage_range_protection.Proof, proofs);
        errdefer alloc.free(result);
        if (control) |port| try port.checkpoint();
        return result;
    }

    fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        return normalizeControlled(ptr, alloc, writes, null);
    }
    fn normalizeWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: Admission) ![]types.BatchWrite {
        return normalizeControlled(ptr, alloc, writes, control);
    }
    fn normalizeControlled(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: ?Admission) ![]types.BatchWrite {
        if (control) |port| try port.checkpoint();
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.callWithAdmission(.{ .operation = .normalize, .schema_version = self.schema_version, .token = self.token, .writes = writes }, control);
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, self.alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.cleanup != null) return error.InvalidRetainedReadResponse;
        if (parsed.value.writes.len != writes.len) return error.InvalidRetainedReadResponse;
        const result = try alloc.alloc(types.BatchWrite, writes.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
                for (row.json_null_fields) |field| alloc.free(field);
                if (row.json_null_fields.len != 0) alloc.free(row.json_null_fields);
            }
            alloc.free(result);
        }
        for (writes, parsed.value.writes, result) |input, row, *out| {
            if (control) |port| try port.checkpoint();
            if (!std.mem.eql(u8, input.key, row.key)) return error.InvalidRetainedReadResponse;
            const key = try alloc.dupe(u8, row.key);
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, row.value);
            errdefer alloc.free(value);
            out.* = .{ .key = key, .value = value, .json_null_fields = try types.cloneJsonNullFields(alloc, row.json_null_fields) };
            initialized += 1;
        }
        if (control) |port| try port.checkpoint();
        return result;
    }

    fn cancelUnknown(self: *Client) void {
        if (self.call(.{ .operation = .cancel, .schema_version = self.schema_version, .connection = self.connection })) |value| {
            var response = value;
            response.deinit(self.alloc);
        } else |_| {}
    }
};

pub const consumer_tests = consumerTests();
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const root = @import("antfly_source_root");
    if (@hasDecl(root, "implementation_tests_only") and root.implementation_tests_only) return struct {};
    return struct {
        test "retained read client page borrows original lifetime during parked transport and never replays rejected output" {
            const a = std.testing.allocator;
            const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            const admission = @import("antfly_local_sources").common_read_admission;
            const Mode = enum { success, canceled, expired, rejected_resume, unsupported_lifetime };
            const Budget = struct {
                execution: *native.Execution,
                scheduler: *native.Scheduler,
                deadline: u64,
                mode: Mode,
                failure: ?anyerror = null,
                poisoned: bool = false,
                waits: usize = 0,
                fn portCheck(raw: *anyopaque) !void {
                    try @as(*@This(), @ptrCast(@alignCast(raw))).checkpoint();
                }
                pub fn checkpoint(self: *@This()) !void {
                    if (self.poisoned) return error.TestBorrowEscaped;
                    try self.execution.checkpoint();
                    try self.checkLifetime();
                }
                pub fn checkLifetime(self: *@This()) !void {
                    if (self.poisoned) return error.TestBorrowEscaped;
                    if (self.failure) |cause| return cause;
                    try self.execution.checkLifetime();
                }
                pub fn deadlineNs(self: *@This()) !?u64 {
                    return self.deadline;
                }
                pub fn waitForProgress(_: *@This()) !void {
                    return error.ExecutionWaitUnsupported;
                }
                pub fn executionBoundary(self: *@This()) ?@import("antfly_local_sources").common_execution_wait.Boundary {
                    return .{ .ptr = self, .begin = begin, .end = end };
                }
                fn begin(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.execution.park(.resource_blocked);
                    self.waits += 1;
                    try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
                    try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
                    var health: native.Execution = .{};
                    try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                    health.finish();
                }
                fn end(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.checkLifetime();
                    try self.execution.enter(.query);
                }
            };
            const Fixture = struct {
                budget: *Budget,
                calls: usize = 0,
                cleanup_calls: usize = 0,
                const route: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    var parsed = try std.json.parseFromSlice(rpc.Request, alloc, request.body, .{});
                    defer parsed.deinit();
                    if (parsed.value.operation == .close or parsed.value.operation == .cancel) {
                        self.cleanup_calls += 1;
                        try std.testing.expect(request.cancellation == null);
                        try std.testing.expect(request.execution_wait == null);
                        return .{ .status = 200 };
                    }
                    self.calls += 1;
                    try self.budget.checkpoint();
                    try std.testing.expect(request.timeout_ms.? <= 250);
                    try std.testing.expectEqual(self.budget.deadline, request.cancellation.?.execution_deadline_ns.?);
                    const timeout_header = try std.fmt.parseInt(u32, request.header(metadata.catalog_route_deadline_ms_header).?, 10);
                    try std.testing.expectEqual(request.timeout_ms.?, timeout_header);
                    var response: http.HttpResponse = .{ .status = 200 };
                    errdefer response.deinit(alloc);
                    response.body = try std.json.Stringify.valueAlloc(alloc, rpc.Response{ .sequence = parsed.value.sequence, .token = if (parsed.value.operation == .capture) .{ .incarnation = 1, .sequence = 1, .slot = 0 } else null }, .{});
                    response.headers = try alloc.alloc(http.Header, 1);
                    response.headers[0] = .{ .name = &.{}, .value = &.{} };
                    response.headers[0].name = try alloc.dupe(u8, metadata.catalog_route_fence_ack_header);
                    response.headers[0].value = try alloc.dupe(u8, metadata.catalog_route_fence_ack_value);
                    const wait = request.execution_wait orelse {
                        try std.testing.expect(parsed.value.operation == .validate);
                        try std.testing.expectEqual(@as(usize, 1), self.budget.scheduler.stats().runnable_slots);
                        try std.testing.expectEqual(@as(usize, 1), self.budget.scheduler.stats().operations);
                        return response;
                    };
                    try wait.beginWait();
                    // Polling cancellation while CPU is parked must use the
                    // lifetime path, never an ordinary runnable checkpoint.
                    try std.testing.expect(!request.cancellation.?.isCancelled());
                    switch (self.budget.mode) {
                        .success => {},
                        .unsupported_lifetime => return error.TestUnexpectedTransport,
                        .canceled => self.budget.failure = error.Canceled,
                        .expired => self.budget.failure = error.DeadlineExceeded,
                        .rejected_resume => self.budget.scheduler.close(),
                    }
                    if (self.budget.failure != null) try std.testing.expect(request.cancellation.?.isCancelled());
                    try wait.endWait();
                    return response;
                }
            };
            for ([_]Mode{ .success, .canceled, .expired, .rejected_resume, .unsupported_lifetime }) |mode| {
                var scheduler = try native.Scheduler.init(.{ .slots = 2 });
                var execution: native.Execution = .{};
                try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
                defer execution.finish();
                var budget: Budget = .{ .execution = &execution, .scheduler = &scheduler, .deadline = time.monotonicNs() + 250 * std.time.ns_per_ms, .mode = mode };
                var adapter = admission.Adapter(*Budget){ .budget = &budget };
                const payload = if (mode == .unsupported_lifetime) admission.Payload{ .ptr = &budget, .vtable = &.{ .checkpoint = Budget.portCheck }, .execution_wait = budget.executionBoundary() } else adapter.payload();
                var fixture: Fixture = .{ .budget = &budget };
                const client = try Client.create(a, .{ .ptr = &fixture, .clock_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } }, "http://peer", "rows", Fixture.route, 5, null);
                defer client.destroy();
                client.token = .{ .incarnation = 1, .sequence = 1, .slot = 0 };
                if (mode == .success) {
                    try Client.admitWithAdmission(a, client.executor, "http://peer", "rows", Fixture.route, 5, null, .init(&payload));
                    const captured = (try Client.captureWithAdmission(a, client.executor, "http://peer", "rows", Fixture.route, 5, null, .init(&payload))) orelse return error.TestMissingCapture;
                    try captured.validateWithAdmission(.init(&payload));
                    // Resource retirement uses independent cleanup authority.
                    captured.deinit();
                    try std.testing.expectEqual(@as(usize, 3), fixture.calls);
                    try std.testing.expectEqual(@as(usize, 1), fixture.cleanup_calls);
                    fixture.calls = 0;
                    budget.waits = 0;
                }
                const result = Client.nextWithAdmission(client, a, 1, admission.Port.init(&payload));
                if (mode == .success) {
                    var page = try result;
                    defer page.deinit();
                    execution.finish();
                    budget.poisoned = true;
                    var second_execution: native.Execution = .{};
                    try second_execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
                    defer second_execution.finish();
                    var second: Budget = .{ .execution = &second_execution, .scheduler = &scheduler, .deadline = time.monotonicNs() + 250 * std.time.ns_per_ms, .mode = .success };
                    var second_adapter = admission.Adapter(*Budget){ .budget = &second };
                    const second_payload = second_adapter.payload();
                    fixture.budget = &second;
                    var continuation = try Client.nextWithAdmission(client, a, 1, admission.Port.init(&second_payload));
                    defer continuation.deinit();
                    try std.testing.expectEqual(@as(u64, 2), client.sequence);
                    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
                } else {
                    try std.testing.expectError(switch (mode) {
                        .canceled => error.Canceled,
                        .expired => error.DeadlineExceeded,
                        .rejected_resume => error.SchedulerClosed,
                        .unsupported_lifetime => error.ExecutionWaitUnsupported,
                        .success => unreachable,
                    }, result);
                    try std.testing.expectError(error.RetainedReadRestartRequired, Client.next(client, a, 1));
                    try std.testing.expectEqual(@as(usize, if (mode == .unsupported_lifetime) 0 else 1), fixture.calls);
                }
                try std.testing.expectEqual(@as(usize, if (mode == .unsupported_lifetime) 0 else 1), budget.waits);
            }
        }
        test "retained read client never replays an ambiguous page request" {
            const Fixture = struct {
                calls: usize = 0,
                fn execute(raw: *anyopaque, _: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.calls += 1;
                    return error.ConnectionResetByPeer;
                }
            };
            var fixture: Fixture = .{};
            const alloc = std.testing.allocator;
            const client = try Client.create(alloc, .{ .ptr = &fixture, .clock_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } }, "http://peer", "a/b", .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } }, 5, null);
            defer client.destroy();
            client.token = .{ .incarnation = 1, .sequence = 1, .slot = 0 };
            try std.testing.expect(std.mem.endsWith(u8, client.uri, "/tables/a%2Fb/retained-read"));
            try std.testing.expectError(error.ConnectionResetByPeer, Client.next(client, alloc, 1));
            try std.testing.expectError(error.RetainedReadRestartRequired, Client.next(client, alloc, 1));
            try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        }

        test "retained read owner managed clients keep original charges through detached cleanup and lost replies" {
            const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            const Snapshot = @import("antfly_local_sources").storage_statement_read_fence.Snapshot;
            const Fixture = struct {
                execution: *native.Execution,
                scheduler: *native.Scheduler,
                receiver: *registry.Registry,
                allow_ack: std.atomic.Value(bool) = .init(false),
                detached: std.atomic.Value(bool) = .init(false),
                pins: std.atomic.Value(usize) = .init(0),
                charged: std.atomic.Value(usize) = .init(0),
                closes: std.atomic.Value(usize) = .init(0),
                rpc_calls: std.atomic.Value(usize) = .init(0),
                cancel_reply: ?*bool = null,
                drained: std.Io.Event = .unset,
                const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                fn retain(raw: *anyopaque, bytes: usize) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.execution.retainReadBytes(bytes);
                    _ = self.charged.fetchAdd(bytes, .acq_rel);
                    _ = self.pins.fetchAdd(1, .acq_rel);
                }
                fn release(raw: *anyopaque, _: usize) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (self.pins.fetchSub(1, .acq_rel) == 1 and self.detached.load(.acquire)) {
                        self.execution.finish();
                        self.drained.set(std.testing.io);
                    }
                }
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expect(self.pins.load(.acquire) != 0);
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = open, .capture_snapshot = snapshot, .release = close } };
                }
                fn validate(_: *anyopaque) !void {}
                fn snapshot(raw: *anyopaque, _: std.mem.Allocator) !Snapshot {
                    return .{ .ptr = raw, .vtable = &.{ .open = open, .release = close } };
                }
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !reads.RelationalReadView {
                    return .{ .ptr = raw, .vtable = &.{ .next = next, .close = close } };
                }
                fn next(_: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
                    return .{ .arena = .init(alloc), .rows = &.{}, .after = null };
                }
                fn close(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    _ = self.closes.fetchAdd(1, .acq_rel);
                }
                fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    _ = self.rpc_calls.fetchAdd(1, .acq_rel);
                    var input = try std.json.parseFromSlice(rpc.Request, alloc, request.body, .{});
                    defer input.deinit();
                    const cleanup = input.value.operation == .close;
                    if (cleanup) {
                        try std.testing.expect(request.cancellation != null);
                        try std.testing.expect(request.execution_wait != null);
                        try std.testing.expect(request.timeout_ms.? <= 100);
                        try request.execution_wait.?.beginWait();
                        try std.testing.expect(!request.cancellation.?.isCancelled());
                        var health: native.Execution = .{};
                        try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                        health.finish();
                    }
                    const result = rpc.execute(alloc, .{ .registry = self.receiver, .source = .{ .ptr = self, .route_fence = route, .remote_statement_fences_safe = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = capture } } }, scope, "rows", input.value);
                    if (cleanup) request.execution_wait.?.endWait() catch |err| {
                        if (result) |bytes| alloc.free(bytes) else |_| {}
                        return err;
                    };
                    const bytes = try result;
                    var response: http.HttpResponse = .{ .status = 200, .body = bytes };
                    errdefer response.deinit(alloc);
                    if (cleanup and !self.allow_ack.load(.acquire)) return error.ConnectionResetByPeer;
                    const headers = try alloc.alloc(http.Header, 1);
                    headers[0] = .{ .name = &.{}, .value = &.{} };
                    response.headers = headers;
                    // response owns bytes from this point, including failure.
                    headers[0].name = try alloc.dupe(u8, metadata.catalog_route_fence_ack_header);
                    headers[0].value = try alloc.dupe(u8, metadata.catalog_route_fence_ack_value);
                    if (input.value.operation == .capture) if (self.cancel_reply) |flag| {
                        flag.* = true;
                        self.cancel_reply = null;
                    };
                    return response;
                }
            };
            for ([_]bool{ false, true }) |late_cancel| {
                const a = std.testing.allocator;
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_bytes = 4 * 1024 * 1024 });
                var execution: native.Execution = .{};
                try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
                defer execution.finish();
                var receiver = try registry.Registry.init(a, std.testing.io, 7, 4, 4, 30 * std.time.ns_per_s);
                defer receiver.deinit();
                var fixture: Fixture = .{ .execution = &execution, .scheduler = &scheduler, .receiver = &receiver };
                const owner = try retirement.Runtime.create(a, std.testing.io, &scheduler, 4, 64 * 1024, 16 * 1024);
                defer {
                    fixture.allow_ack.store(true, .release);
                    const limit = time.monotonicNs() + 5 * std.time.ns_per_s;
                    while (owner.registry.snapshot().retained != 0 and time.monotonicNs() < limit) std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
                    owner.join() catch unreachable;
                    owner.destroyJoined() catch unreachable;
                }
                var foreground = std.heap.ArenaAllocator.init(a);
                var foreground_live = true;
                defer if (foreground_live) foreground.deinit();
                const executor: http.RequestExecutor = .{ .ptr = &fixture, .clock_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } };
                const binding: retirement.Binding = .{ .registry = &owner.registry, .parent = .{ .ptr = &fixture, .retain = Fixture.retain, .release = Fixture.release } };
                const ReadAdmission = @import("antfly_local_sources").common_read_admission;
                const Scope = struct {
                    binding: retirement.Binding,
                    canceled: bool = false,
                    poisoned: bool = false,
                    pub fn checkpoint(self: *@This()) !void {
                        try self.checkLifetime();
                    }
                    pub fn checkLifetime(self: *@This()) !void {
                        if (self.poisoned) return error.TestForegroundBorrowEscaped;
                        if (self.canceled) return error.Canceled;
                    }
                    pub fn waitForProgress(self: *@This()) !void {
                        try self.checkLifetime();
                        return error.Backpressured;
                    }
                    pub fn retirementBinding(self: *@This()) !?retirement.Binding {
                        try self.checkLifetime();
                        return self.binding;
                    }
                };
                var scope: Scope = .{ .binding = binding };
                var adapter: ReadAdmission.Adapter(*Scope) = .{ .budget = &scope };
                const payload = adapter.payload();
                var trace: ReadAdmission.RejectionTrace = .{};
                var restricted: ReadAdmission.RetainedDependency = .{};
                const control = restricted.bind(trace.bind(.init(&payload)));
                // A bad cross-module contract or expired caller never becomes
                // an absent capability/legacy fallback and publishes no RPC.
                var invalid = ReadAdmission.Port.init(&payload);
                invalid.contract.version +%= 1;
                try std.testing.expectError(error.UnsupportedVersion, Client.captureWithAdmission(foreground.allocator(), executor, "http://peer", "rows", Fixture.route, 5, null, invalid));
                scope.canceled = true;
                try std.testing.expectError(error.Canceled, Client.captureWithAdmission(foreground.allocator(), executor, "http://peer", "rows", Fixture.route, 5, null, .init(&payload)));
                scope.canceled = false;
                try std.testing.expectEqual(@as(usize, 0), fixture.rpc_calls.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), fixture.pins.load(.acquire));
                if (late_cancel) {
                    fixture.cancel_reply = &scope.canceled;
                    try std.testing.expectError(error.Canceled, Client.captureWithAdmission(foreground.allocator(), executor, "http://peer", "rows", Fixture.route, 5, null, control));
                    scope.poisoned = true;
                    try execution.rejectRetained(error.Canceled);
                } else {
                    const fence = (try Client.captureWithAdmission(foreground.allocator(), executor, "http://peer", "rows", Fixture.route, 5, null, control)).?;
                    var fence_live = true;
                    defer if (fence_live) fence.deinit();
                    const snapshot = try fence.captureSnapshot(a);
                    var snapshot_live = true;
                    defer if (snapshot_live) snapshot.deinit();
                    const view = try snapshot.open(a, "", "", .{ .relational_query = .{ .fields = &.{} } });
                    var view_live = true;
                    defer if (view_live) view.deinit();
                    const extra = (try Client.captureWithAdmission(foreground.allocator(), executor, "http://peer", "rows", Fixture.route, 5, null, control)).?;
                    const before = fixture.rpc_calls.load(.acquire);
                    try std.testing.expectError(error.CleanupCapacityUnavailable, Client.captureWithAdmission(foreground.allocator(), executor, "http://peer", "rows", Fixture.route, 5, null, control));
                    try std.testing.expectEqual(before, fixture.rpc_calls.load(.acquire));
                    extra.deinit();
                    try std.testing.expectEqual(@as(usize, 3), owner.registry.snapshot().foreground);
                    try std.testing.expectEqual(@as(usize, 4), fixture.pins.load(.acquire));
                    try execution.park(.output_blocked);
                    fence.deinit();
                    fence_live = false;
                    snapshot.deinit();
                    snapshot_live = false;
                    view.deinit();
                    view_live = false;
                }
                scope.poisoned = true;
                // Cleanup owns stable metadata, not this allocator or a saved Port.
                foreground.deinit();
                foreground_live = false;
                fixture.detached.store(true, .release);
                try std.testing.expectEqual(@as(usize, if (late_cancel) 1 else 4), fixture.pins.load(.acquire));
                try std.testing.expect(owner.registry.snapshot().bytes > 0);
                const held = scheduler.stats();
                try std.testing.expectEqual(@as(u64, 17) + fixture.charged.load(.acquire), held.retained_bytes - held.control_retained_bytes);
                // A node join/drain refusal does not stop the worker or sever
                // this real remote client's original root and parent chain.
                try std.testing.expectError(error.CleanupPending, owner.join());
                try std.testing.expect(!owner.joined and !owner.stopping.load(.acquire));
                const expired: std.Io.Clock.Timestamp = .{ .raw = .{ .nanoseconds = 0 }, .clock = .awake };
                try std.testing.expectError(error.CleanupPending, owner.waitDrainedUntil(std.testing.io, expired));
                try std.testing.expectEqual(@as(usize, if (late_cancel) 1 else 4), fixture.pins.load(.acquire));
                try std.testing.expect(scheduler.stats().retained_bytes > 17);
                fixture.allow_ack.store(true, .release);
                try fixture.drained.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
                try owner.join();
                try std.testing.expectEqual(@as(usize, if (late_cancel) 1 else 4), fixture.closes.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), fixture.pins.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), owner.registry.snapshot().retained);
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                try std.testing.expectEqual(owner.reservedBytes(), scheduler.stats().retained_bytes);
            }
        }

        test "retained read client requires exact physical cleanup evidence through response loss and restart" {
            const Mode = enum { valid, lost, missing_ack, missing_evidence, version, incarnation, token, connection, revoked, extra, malformed };
            const Fixture = struct {
                owner: *registry.Registry,
                mode: Mode = .valid,
                closes: usize = 0,
                calls: usize = 0,
                const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                fn close(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.closes += 1;
                }
                fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.calls += 1;
                    try std.testing.expect(rpc.isControlBody(request.body));
                    try std.testing.expect(request.execution_wait == null and request.cancellation == null);
                    try std.testing.expectEqual(@as(?u32, 100), request.timeout_ms);
                    var input = try std.json.parseFromSlice(rpc.Request, alloc, request.body, .{});
                    defer input.deinit();
                    try std.testing.expect(input.value.operation == .close);
                    const bytes = try rpc.execute(alloc, .{ .registry = self.owner, .source = .{ .ptr = self, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } } }, scope, "rows", input.value);
                    defer alloc.free(bytes);
                    if (self.mode == .lost) return error.ConnectionResetByPeer;
                    var parsed = try std.json.parseFromSlice(rpc.Response, alloc, bytes, .{});
                    defer parsed.deinit();
                    switch (self.mode) {
                        .valid, .missing_ack => {},
                        .lost => unreachable,
                        .missing_evidence => parsed.value.cleanup = null,
                        .version => parsed.value.cleanup.?.version += 1,
                        .incarnation => parsed.value.cleanup.?.incarnation += 1,
                        .token => parsed.value.cleanup.?.token.?.sequence += 1,
                        .connection => parsed.value.cleanup.?.connection = 99,
                        .revoked => parsed.value.cleanup.?.state = .revoked,
                        .extra => parsed.value.sequence = 1,
                        .malformed => {},
                    }
                    var response: http.HttpResponse = .{ .status = 200 };
                    errdefer response.deinit(alloc);
                    response.body = if (self.mode == .malformed) try alloc.dupe(u8, "{") else try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
                    if (self.mode != .missing_ack) {
                        response.headers = try alloc.alloc(http.Header, 1);
                        response.headers[0] = .{ .name = &.{}, .value = &.{} };
                        response.headers[0].name = try alloc.dupe(u8, metadata.catalog_route_fence_ack_header);
                        response.headers[0].value = try alloc.dupe(u8, metadata.catalog_route_fence_ack_value);
                    }
                    return response;
                }
            };
            const alloc = std.testing.allocator;
            var owner = try registry.Registry.init(alloc, std.testing.io, 7, 1, 1, 30 * std.time.ns_per_s);
            defer owner.deinit();
            var fixture: Fixture = .{ .owner = &owner };
            const resource: registry.Resource = .{ .ptr = &fixture, .close = Fixture.close, .kind = .cursor };
            const token = try owner.insert(Fixture.scope, 55, time.monotonicNs(), time.monotonicNs() + std.time.ns_per_s, resource);
            const client = try Client.create(alloc, .{ .ptr = &fixture, .clock_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } }, "http://peer", "rows", Fixture.route, 5, null);
            defer client.destroy();
            client.token = token;
            var borrow = try owner.borrow(token, Fixture.scope, .cursor, time.monotonicNs());
            defer borrow.deinit();
            try std.testing.expectEqual(registry.Retirement.pending, try client.closeAcknowledged());
            try std.testing.expectEqual(@as(usize, 0), fixture.closes);
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner.insert(Fixture.scope, 56, time.monotonicNs(), time.monotonicNs() + std.time.ns_per_s, resource));
            for ([_]Mode{ .missing_ack, .missing_evidence, .version, .incarnation, .token, .connection, .revoked, .extra, .malformed }) |mode| {
                fixture.mode = mode;
                const result = client.closeAcknowledged();
                if (result) |_| return error.TestAcceptedInvalidRetirementEvidence else |_| {}
                try std.testing.expectEqual(@as(usize, 0), fixture.closes);
                try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner.insert(Fixture.scope, 56, time.monotonicNs(), time.monotonicNs() + std.time.ns_per_s, resource));
            }
            borrow.deinit();
            fixture.mode = .valid;
            try std.testing.expectEqual(@as(usize, 1), fixture.closes);
            try std.testing.expectEqual(registry.Retirement.retired, try client.closeAcknowledged());
            // Loss after physical cleanup conveys no evidence. Repeating only
            // this exact idempotent close can later learn terminal retirement.
            client.token = try owner.insert(Fixture.scope, 56, time.monotonicNs(), time.monotonicNs() + std.time.ns_per_s, resource);
            fixture.mode = .lost;
            try std.testing.expectError(error.ConnectionResetByPeer, client.closeAcknowledged());
            try std.testing.expectEqual(@as(usize, 2), fixture.closes);
            fixture.mode = .valid;
            try std.testing.expectEqual(registry.Retirement.retired, try client.closeAcknowledged());
            try std.testing.expectEqual(@as(usize, 2), fixture.closes);
            var restarted = try registry.Registry.init(alloc, std.testing.io, 8, 1, 1, 30 * std.time.ns_per_s);
            defer restarted.deinit();
            fixture.owner = &restarted;
            try std.testing.expectError(error.RetainedReadRestartRequired, client.closeAcknowledged());
            // The node ledger retains the original stopped execution through
            // a changed process, and retires only after the original peer ACK.
            const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            const ParentContext = struct {
                execution: *native.Execution,
                pins: usize = 0,
                fn retain(raw: *anyopaque, bytes: usize) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.execution.growInputBytes(bytes);
                    self.pins += 1;
                }
                fn release(raw: *anyopaque, _: usize) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.pins -= 1;
                    if (self.pins == 0) self.execution.finish();
                }
            };
            var scheduler = try native.Scheduler.init(.{ .slots = 2 });
            var original: native.Execution = .{};
            try original.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
            defer original.finish();
            var parent: ParentContext = .{ .execution = &original };
            var ledger = try retirement.Registry.init(alloc, 1, 16 * 1024, 16 * 1024);
            defer {
                ledger.close();
                ledger.deinit() catch unreachable;
            }
            client.binding = .{ .registry = &ledger, .parent = .{ .ptr = &parent, .retain = ParentContext.retain, .release = ParentContext.release } };
            const managed = try client.cloneManaged();
            try managed.managed.?.publish();
            managed.token = try owner.insert(Fixture.scope, 57, time.monotonicNs(), time.monotonicNs() + std.time.ns_per_s, resource);
            managed.token_known = true;
            Client.close(managed);
            try original.rejectRetained(error.Canceled);
            try std.testing.expectEqual(retirement.Registry.Progress.needs_fence, ledger.pollOne(.{ .allocator = alloc }));
            try std.testing.expectEqual(@as(usize, 1), ledger.snapshot().needs_fence);
            try std.testing.expectEqual(@as(usize, 1), parent.pins);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            ledger.close();
            try std.testing.expectError(error.CleanupPending, ledger.deinit());
            fixture.owner = &owner;
            try std.testing.expectEqual(retirement.Registry.Progress.retired, ledger.reconcileOne(.{ .allocator = alloc }));
            try std.testing.expectEqual(@as(usize, 0), parent.pins);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }

        test "retained read client round trips owned snapshots and exact typed pages" {
            const Fixture = struct {
                registry_owner: *registry.Registry,
                capture_closed: usize = 0,
                cursor_closed: usize = 0,
                advances: usize = 0,
                fail_delivery: bool = false,
                lose_capture: bool = false,
                legacy_pages: bool = false,
                const scope = registry.Scope{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route = metadata.CatalogRouteFence{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = open, .release = release } };
                }
                pub fn validate(_: *anyopaque) !void {}
                fn release(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.capture_closed += 1;
                }
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
                    try std.testing.expectEqual(@as(?u32, 5), opts.relational_query.?.schema_version);
                    try std.testing.expect(opts.sql_document_preimage and opts.include_content_hashes);
                    try std.testing.expect(opts.include_range_proofs);
                    return .{ .ptr = raw, .vtable = &.{ .next = next, .close = close, .normalize = normalizeWrites, .range_proofs = proofRows } };
                }
                fn proofRows(_: *anyopaque, alloc: std.mem.Allocator) ![]@import("antfly_local_sources").storage_range_protection.Proof {
                    return alloc.dupe(@import("antfly_local_sources").storage_range_protection.Proof, &.{ .{ .bucket = 0, .generation = null }, .{ .bucket = 256, .generation = std.math.maxInt(u64) } });
                }
                fn normalizeWrites(_: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
                    const result = try alloc.alloc(types.BatchWrite, writes.len);
                    // Fixture uses the owner's temporary arena; production
                    // providers must likewise return owned normalized bytes.
                    for (writes, result) |row, *out| out.* = .{ .key = try alloc.dupe(u8, row.key), .value = try alloc.dupe(u8, row.value), .json_null_fields = try types.cloneJsonNullFields(alloc, row.json_null_fields) };
                    return result;
                }
                fn close(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.cursor_closed += 1;
                }
                fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.advances += 1;
                    var arena = std.heap.ArenaAllocator.init(alloc);
                    errdefer arena.deinit();
                    const rows = try arena.allocator().alloc(reads.RelationalReadView.Row, 1);
                    rows[0] = .{ .id = "a", .version = 7, .schema_version = 5, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"n\":9007199254740993,\"j\":null}", .{ .parse_numbers = false }), .sql_nulls = &.{ false, false }, .expected_content_digest = @splat(17), .document = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"n\":9007199254740993,\"extra\":{\"retained\":true}}", .{ .parse_numbers = false }) };
                    return .{ .arena = arena, .rows = rows, .after = null };
                }
                fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    var parsed = try std.json.parseFromSlice(rpc.Request, alloc, request.body, .{ .parse_numbers = false });
                    defer parsed.deinit();
                    var native: @import("retained_read_page.zig").Format = .json;
                    for (request.headers) |header| if (std.ascii.eqlIgnoreCase(header.name, "accept")) {
                        native = @import("retained_read_page.zig").formatForAccept(header.value);
                    };
                    if (self.legacy_pages) native = .json; // Older owners ignore the v2 Accept value.
                    const body = try rpc.executeWithCodec(alloc, .{ .registry = self.registry_owner, .source = .{ .ptr = self, .route_fence = route, .remote_statement_fences_safe = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = capture } } }, scope, "rows", parsed.value, native);
                    errdefer alloc.free(body);
                    if ((self.fail_delivery and parsed.value.operation == .next) or (self.lose_capture and parsed.value.operation == .capture)) return error.ConnectionResetByPeer;
                    const headers = try alloc.alloc(http.Header, 1);
                    errdefer alloc.free(headers);
                    const name = try alloc.dupe(u8, metadata.catalog_route_fence_ack_header);
                    errdefer alloc.free(name);
                    headers[0] = .{ .name = name, .value = try alloc.dupe(u8, metadata.catalog_route_fence_ack_value) };
                    return .{ .status = 200, .body = body, .headers = headers };
                }
            };
            const alloc = std.testing.allocator;
            var lifetime = try registry.Registry.init(alloc, std.testing.io, 1, 8, 8, 30 * std.time.ns_per_s);
            defer lifetime.deinit();
            var fixture: Fixture = .{ .registry_owner = &lifetime };
            const executor = http.RequestExecutor{ .ptr = &fixture, .clock_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } };
            const fence = (try Client.capture(alloc, executor, "http://peer", "rows", Fixture.route, 5, null)).?;
            const view = try fence.open(alloc, "", "", .{ .relational_query = .{ .fields = &.{ "n", "j" } }, .sql_document_preimage = true, .include_content_hashes = true, .include_range_proofs = true });
            try fence.validate();
            fence.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.capture_closed);
            const proofs = try view.rangeProofs(alloc);
            defer alloc.free(proofs);
            try std.testing.expectEqual(@as(usize, 2), proofs.len);
            try std.testing.expectEqual(null, proofs[0].generation);
            try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), proofs[1].generation);
            var page = try view.next(alloc, 1);
            defer page.deinit();
            try std.testing.expect(page.rows[0].typed_cells != null);
            try std.testing.expectEqualStrings("9007199254740993", (try page.rows[0].cell("n")).value.number_string);
            try std.testing.expect(!(try page.rows[0].cell("j")).sql_null);
            try std.testing.expectEqual(@as([32]u8, @splat(17)), page.rows[0].expected_content_digest.?);
            try std.testing.expectEqualStrings("9007199254740993", page.rows[0].document.?.object.get("n").?.number_string);
            try std.testing.expect(page.rows[0].document.?.object.get("extra").?.object.get("retained").?.bool);
            fixture.legacy_pages = true;
            var legacy_page = try view.next(alloc, 1);
            defer legacy_page.deinit();
            try std.testing.expect(legacy_page.rows[0].typed_cells == null);
            try std.testing.expectEqualStrings("9007199254740993", (try legacy_page.rows[0].cell("n")).value.number_string);
            try std.testing.expect(!(try legacy_page.rows[0].cell("j")).sql_null);
            var normalized_arena = std.heap.ArenaAllocator.init(alloc);
            defer normalized_arena.deinit();
            const normalized = try view.normalize(normalized_arena.allocator(), &.{.{ .key = "a", .value = "{\"j\":null}", .json_null_fields = &.{"j"} }});
            try std.testing.expectEqualStrings("a", normalized[0].key);
            try std.testing.expectEqualStrings("{\"j\":null}", normalized[0].value);
            try std.testing.expectEqualStrings("j", normalized[0].json_null_fields[0]);
            fixture.fail_delivery = true;
            try std.testing.expectError(error.ConnectionResetByPeer, view.next(alloc, 1));
            try std.testing.expectError(error.RetainedReadRestartRequired, view.next(alloc, 1));
            try std.testing.expectEqual(@as(usize, 3), fixture.advances);
            view.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.cursor_closed);
            // Capture completed on the owner, but its token never reached the
            // caller. Scoped connection cancellation promptly releases it.
            fixture.lose_capture = true;
            try std.testing.expectError(error.ConnectionResetByPeer, Client.capture(alloc, executor, "http://peer", "rows", Fixture.route, 5, null));
            try std.testing.expectEqual(@as(usize, 2), fixture.capture_closed);
        }
    };
}
