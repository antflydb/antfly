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

//! Production retirement consumer and root-signed physical terminal evidence.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = local.common_http_http_common;
const clock = @import("antfly_platform").time;
const retirement = @import("retained_read_retirement.zig");
const Gate = @import("remote_control_admission.zig").Gate;
const Client = @import("remote_attempt_client.zig").Client;
const attempts = @import("../common/remote_attempts.zig");
const worker = @import("../common/remote_worker.zig");
const replies = @import("../common/remote_attempt_reply.zig");
const wire = @import("../common/remote_attempt_wire.zig");
const membership = @import("../common/remote_membership.zig");
const auth = @import("internal_service_auth.zig");

fn pause(deadline: u64) !void {
    if (clock.monotonicNs() >= deadline) return error.TestRetirementProgressDeadline;
    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
}

test "remote control admission production retirement waits idle and releases only signed physical terminal evidence over TCP" {
    try runRetirement(false);
}

test "remote control admission both production consumers progress through unavailable peer and foreground pressure over TCP" {
    try runRetirement(true);
}

fn runRetirement(with_discovery: bool) !void {
    const member: membership.Binding = .{ .coordinator_node_id = 7, .worker_node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
    const App = struct {
        registry: *worker.Registry,
        signer: replies.Signer,
        calls: std.atomic.Value(usize) = .init(0),
        active: std.atomic.Value(usize) = .init(0),
        unavailable: bool = false,
        fn execute(raw: *anyopaque, allocator: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .acq_rel);
            if (self.unavailable) return .{ .status = 503 };
            const token = request.header(auth.header_name) orelse return error.TestMissingControlCredential;
            var fields = std.mem.splitScalar(u8, token, '.');
            _ = fields.next() orelse return error.TestMalformedControlCredential;
            const payload = fields.next() orelse return error.TestMalformedControlCredential;
            const decoder = std.base64.url_safe_no_pad.Decoder;
            const decoded = try allocator.alloc(u8, try decoder.calcSizeForSlice(payload));
            defer allocator.free(decoded);
            try decoder.decode(decoded, payload);
            const Claims = struct { metadata: struct { remote_attempt: []const u8 } };
            var parsed = try std.json.parseFromSlice(Claims, allocator, decoded, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const context = try wire.Context.decode(parsed.value.metadata.remote_attempt);
            if (context.kind == .capabilities) {
                const reply = try @import("../common/remote_read_capabilities.zig").Reply.sign(&self.signer, context, .{ .retained_read = true });
                return .{ .status = 200, .body = try allocator.dupe(u8, &reply.encode()) };
            }
            // Fixture membership is supplied directly. This test does not
            // qualify the production JWT/membership admission adapter.
            const verified: wire.Verified = .{ .context = context, .local_deadline_ns = clock.monotonicNs() +| context.remaining_ns };
            const reply = switch (context.kind) {
                .cancel => try self.signer.cancelWithWait(self.registry, verified, member, null),
                .status => try self.signer.status(self.registry, verified, member),
                .fence => try self.signer.fence(self.registry, verified, member),
                else => return error.TestUnexpectedControlKind,
            };
            if (reply.outcome == .active) _ = self.active.fetchAdd(1, .acq_rel);
            return .{ .status = 200, .body = try allocator.dupe(u8, &reply.encode()) };
        }
    };
    const Resource = struct {
        client: Client,
        id: attempts.Id,
        pins: std.atomic.Value(usize) = .init(0),
        releases: std.atomic.Value(usize) = .init(0),
        destroyed: std.atomic.Value(usize) = .init(0),
        fn retain(raw: *anyopaque, _: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.pins.fetchAdd(1, .acq_rel);
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.pins.fetchSub(1, .acq_rel);
            _ = self.releases.fetchAdd(1, .acq_rel);
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.pins.load(.acquire) == 1);
            _ = self.destroyed.fetchAdd(1, .acq_rel);
        }
        fn poll(raw: *anyopaque, round: retirement.Poll) !retirement.Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const control = round.control orelse return error.TestMissingCleanupAuthority;
            try control.checkLifetime();
            var client = self.client;
            client.control_allocator = round.allocator;
            client.control_wait = (try control.executionWait()) orelse return error.TestMissingCleanupWait;
            client.control_permit = round.control_permit orelse return error.TestMissingCleanupPermit;
            return switch (try client.retireAttempt(0, self.id, (try control.deadlineNs()).?, null)) {
                .retired, .duplicate => .retired,
                .retained, .fenced => .pending,
            };
        }
    };
    const scheduling = local.common_request_admission.runnable_scheduler;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer gate.tryDestroy() catch unreachable;
    const deadline = clock.monotonicNs() + 5 * std.time.ns_per_s;
    var held = try gate.acquire(.{ .deadline_ns = deadline });
    defer held.release();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try @import("../common/remote_fence_store.zig").Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    var registry = try worker.Registry.init(std.testing.allocator, &store, authority.local, .{ .coordinators = 2, .attempts = 2, .per_coordinator_attempts = 2, .duplicate_window = 8, .bytes = 16384, .per_coordinator_bytes = 16384, .resource_references = 4 }, clock.monotonicNs);
    defer registry.deinit() catch unreachable;
    try registry.completeRecovery(.{ .context = &registry, .quiescent = struct {
        fn ready(_: *const anyopaque) bool {
            return true;
        }
    }.ready });
    const signer = try replies.Signer.init(authority.local, authority.public_key, @splat(7));
    var slots: [2]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{ .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 8192 }, .{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 4096 } };
    var ledger = try store.startCoordinator(try membership.coordinatorIdentity(authority.local.metadata_incarnation, 7), 0, &slots, destinations[0..(if (with_discovery) @as(usize, 2) else 1)], 8192);
    const fence: wire.Verified = .{ .context = .{ .kind = .fence, .id = .{ .coordinator = ledger.coordinator, .generation = 1, .operation = 0, .sequence = 0 }, .worker_incarnation = 11, .remaining_ns = std.time.ns_per_s, .charged_bytes = 0, .request_digest = @splat(0) }, .local_deadline_ns = deadline };
    const ack = try signer.fence(&registry, fence, member);
    _ = try ack.reconcile(authority, fence.context, &ledger, 0, &store);
    // The second fixture destination starts with no physical work. Establish
    // its startup fence before making its HTTP service unavailable below.
    if (with_discovery) try ledger.acknowledgeFence(1, .{ .coordinator = ledger.coordinator, .generation = 1, .worker_incarnation = 22 });
    const id = try ledger.reserve(0, 71, 4096);
    try store.prepareDispatch(&ledger, id);
    ledger.transportLost(id);
    const read: wire.Verified = .{ .context = .{ .kind = .read, .id = id, .worker_incarnation = 11, .remaining_ns = std.time.ns_per_s, .charged_bytes = 4096, .request_digest = @splat(1) }, .local_deadline_ns = deadline };
    var lease = (try registry.begin(read, member)).started;
    defer lease.close();
    var physical = try lease.retainResource();
    defer physical.release();
    lease.close();
    var app: App = .{ .registry = &registry, .signer = signer };
    var listener = local.common_http_std_http_listener.StdHttpListener.init(std.testing.allocator, .{ .serve_in_connection_threads = true }, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
    defer listener.deinit();
    try listener.start();
    const uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(uri);
    var executor: @import("../common/http/std_http_executor.zig").StdHttpExecutor = undefined;
    try executor.initInPlaceFallible(std.testing.allocator, .{ .io_concurrent_limit = 8, .pool_max_connections = 1, .pool_max_per_host = 1, .pool_max_waiters = 0, .pool_max_wait_bytes = 0, .pool_max_wait_ms = 0 });
    defer executor.deinit();
    var unavailable_authority = authority;
    unavailable_authority.local.node_id = 10;
    unavailable_authority.local.store_id = 14;
    unavailable_authority.local.root_incarnation = 22;
    var unavailable_app: App = .{ .registry = &registry, .signer = signer, .unavailable = true };
    var unavailable_listener = local.common_http_std_http_listener.StdHttpListener.init(std.testing.allocator, .{ .serve_in_connection_threads = true }, .{ .ptr = &unavailable_app, .vtable = &.{ .execute = App.execute } });
    defer unavailable_listener.deinit();
    try unavailable_listener.start();
    const unavailable_uri = try unavailable_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(unavailable_uri);
    const Publication = @import("remote_capability_publication.zig").Cache;
    const cache = if (with_discovery) try Publication.create(std.testing.allocator, &scheduler, &.{ authority, unavailable_authority }, clock.monotonicNs) else null;
    defer if (cache) |value| {
        value.close();
        value.deinit() catch unreachable;
    };
    const targets = [_]@import("remote_attempt_client.zig").Target{ .{ .base_uri = uri, .authority = authority }, .{ .base_uri = unavailable_uri, .authority = unavailable_authority } };
    const client: Client = .{ .ledger = &ledger, .store = &store, .targets = targets[0..(if (with_discovery) @as(usize, 2) else 1)], .signing = .{ .secret = "control-admission-rpc-signing-secret-32", .issuer = "control-admission-test" }, .read_allocator = std.testing.allocator, .control_allocator = std.testing.allocator, .read_executor = executor.executor(), .control_executor = executor.executor(), .control_admission = gate, .capabilities = cache };
    var resource: Resource = .{ .client = client, .id = id };
    var workers = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer workers.deinit();
    const owner = try retirement.Runtime.createWithControlAdmission(std.testing.allocator, workers.io(), &scheduler, 1, 4096, 4096, gate);
    var owner_live = true;
    defer if (owner_live) {
        physical.release();
        held.release();
        const until = std.Io.Clock.Timestamp.now(std.testing.io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake });
        owner.waitDrainedUntil(std.testing.io, until) catch unreachable;
        owner.join() catch unreachable;
        owner.destroyJoined() catch unreachable;
    };
    const binding = try owner.binding(.{ .ptr = &resource, .retain = Resource.retain, .release = Resource.release });
    const ticket = try binding.registry.reserve(binding.parent, 0);
    try ticket.bind(.{ .ptr = &resource, .poll = Resource.poll, .destroy = Resource.destroy });
    try ticket.publish();
    try ticket.transfer();
    while (gate.admission.stats().queued != 1) try pause(deadline);
    var discovery_workers = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer discovery_workers.deinit();
    const Reconciliation = @import("remote_reconciliation.zig").Runtime;
    const reconciliation = if (with_discovery) try Reconciliation.create(std.testing.allocator, discovery_workers.io(), &scheduler, client, .{ .interval_ms = 1, .max_rpc_calls = 1 }) else null;
    defer if (reconciliation) |value| value.deinit();
    if (with_discovery) while (gate.admission.stats().queued != 2) {
        try pause(deadline);
    };
    for (0..64) |_| try std.testing.expectError(error.OutboundCapacityUnavailable, client.probeCapabilities(0, deadline, null));
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), app.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), executor.resolved_client.outbound.snapshot().active);
    try std.testing.expectError(error.CleanupPending, owner.join());
    try std.testing.expect(!owner.stopping.load(.acquire));
    scheduler.close();
    held.release();
    while (app.active.load(.acquire) == 0) try pause(deadline);
    try std.testing.expectEqual(@as(usize, 1), owner.registry.snapshot().retained);
    try std.testing.expectEqual(@as(usize, 4096), ledger.snapshot().bytes);
    try std.testing.expectEqual(@as(usize, 1), resource.pins.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), resource.destroyed.load(.acquire));
    if (reconciliation) |value| {
        while (!(try cache.?.snapshot(0, authority)).retained_read or value.snapshot().discovery_failures == 0 or value.snapshot().passes < 2) {
            _ = client.probeCapabilities(0, deadline, null) catch |err| switch (err) {
                error.OutboundCapacityUnavailable => {},
                else => return err,
            };
            try pause(deadline);
        }
        try std.testing.expect(unavailable_app.calls.load(.acquire) != 0);
        try std.testing.expectEqual(Publication.ProfileTag.unverified, std.meta.activeTag(try cache.?.profile(1, unavailable_authority)));
        try std.testing.expectEqual(@as(usize, 4096), ledger.snapshot().bytes);
        try std.testing.expectEqual(@as(usize, 0), executor.resolved_client.outbound.snapshot().waiting);
    }
    physical.release();
    if (with_discovery) while (owner.registry.snapshot().retained != 0) {
        _ = client.probeCapabilities(0, deadline, null) catch |err| switch (err) {
            error.OutboundCapacityUnavailable => {},
            else => return err,
        };
        try pause(deadline);
    };
    const until = std.Io.Clock.Timestamp.now(std.testing.io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake });
    try owner.waitDrainedUntil(std.testing.io, until);
    try owner.join();
    try owner.destroyJoined();
    owner_live = false;
    if (reconciliation) |value| value.join();
    try std.testing.expectEqual(@as(usize, 1), resource.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), resource.releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), resource.pins.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
    try std.testing.expectEqual(@as(usize, 0), registry.snapshot().references);
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().queued);
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().in_flight);
    try std.testing.expectEqual(gate.reservedBytes() + (if (cache) |value| value.reservedBytes() else 0) + (if (reconciliation) |value| value.reservedBytes() else 0), scheduler.stats().control_retained_bytes);
}

test "remote control admission production consumer joins cancel their own queue entries without releasing another owner permit" {
    const scheduling = local.common_request_admission.runnable_scheduler;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer gate.tryDestroy() catch unreachable;
    const deadline = clock.monotonicNs() + 5 * std.time.ns_per_s;
    var held = try gate.acquire(.{ .deadline_ns = deadline });
    defer held.release();
    var workers = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer workers.deinit();
    const owner = try retirement.Runtime.createWithControlAdmission(std.testing.allocator, workers.io(), &scheduler, 1, 4096, 4096, gate);
    var owner_live = true;
    defer if (owner_live) {
        held.release();
        owner.join() catch unreachable;
        owner.destroyJoined() catch unreachable;
    };
    while (gate.admission.stats().queued != 1) try pause(deadline);
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 64 }};
    var ledger = try attempts.Coordinator.init(7, 1, &slots, &destinations, 64);
    var reconciliation_workers = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer reconciliation_workers.deinit();
    const Reconciliation = @import("remote_reconciliation.zig").Runtime;
    // The held gate must prevent any access to the sentinel Store/transport.
    const client: Client = .{ .ledger = &ledger, .store = undefined, .targets = &.{}, .signing = .{ .secret = "control-admission-rpc-signing-secret-32", .issuer = "control-admission-test" }, .read_allocator = std.testing.allocator, .control_allocator = std.testing.allocator, .read_executor = undefined, .control_executor = undefined, .control_admission = gate };
    const reconciliation = try Reconciliation.create(std.testing.allocator, reconciliation_workers.io(), &scheduler, client, .{});
    defer reconciliation.deinit();
    const reserved = gate.reservedBytes() + owner.reservedBytes() + reconciliation.reservedBytes();
    while (gate.admission.stats().queued != 2) try pause(deadline);
    try std.testing.expectEqual(reserved, scheduler.stats().control_retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    const started = clock.monotonicNs();
    try owner.join();
    try std.testing.expect(clock.monotonicNs() - started < std.time.ns_per_s);
    try std.testing.expect(owner.joined);
    try std.testing.expectEqual(@as(usize, 1), gate.admission.stats().queued);
    try std.testing.expect(!reconciliation.stopping.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), gate.admission.stats().in_flight);
    try gate.validateBorrow(held.borrow());
    try std.testing.expectEqual(reserved, scheduler.stats().control_retained_bytes);
    try owner.destroyJoined();
    owner_live = false;
    try std.testing.expectEqual(gate.reservedBytes() + reconciliation.reservedBytes(), scheduler.stats().control_retained_bytes);
    const reconciliation_started = clock.monotonicNs();
    reconciliation.join();
    try std.testing.expect(clock.monotonicNs() - reconciliation_started < std.time.ns_per_s);
    try std.testing.expect(reconciliation.joined);
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().queued);
    try std.testing.expectEqual(@as(u64, 0), gate.admission.stats().queued_bytes);
    try std.testing.expectEqual(@as(usize, 1), gate.admission.stats().in_flight);
    try gate.validateBorrow(held.borrow());
    held.release();
    var fresh = try gate.acquire(.{ .deadline_ns = deadline });
    fresh.release();
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().in_flight);
}
