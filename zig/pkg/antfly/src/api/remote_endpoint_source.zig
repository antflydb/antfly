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

//! Publish whole endpoint views; retain an operation's original view until its
//! physical root retires. The supervisor fences callers before deinit.
const std = @import("std");
const View = @import("remote_endpoint_view.zig").View;
const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub const Source = struct {
    allocator: std.mem.Allocator,
    scheduler: *scheduling.Scheduler,
    baseline: scheduling.Scheduler.ControlBaseline,
    routes: []Route,
    mutex: std.atomic.Mutex = .unlocked,
    current_view: View.Ref,
    revision: u64 = 1,
    enabled: std.atomic.Value(u8) = .init(0),
    closed: bool = false,
    readers: usize = 0,
    producers: usize = 0,
    bindings: usize = 0,
    buckets: [64]?*Binding = @splat(null),

    pub const max_uri_bytes = 2048;
    const Authority = @import("../common/remote_attempt_reply.zig").Authority;
    const Route = struct {
        authority: Authority,
        uri: [max_uri_bytes]u8 = undefined,
        len: usize,
        fn set(self: *Route, authority: Authority, uri: []const u8) void {
            std.debug.assert(uri.len != 0 and uri.len <= max_uri_bytes);
            self.authority = authority;
            self.len = uri.len;
            @memcpy(self.uri[0..uri.len], uri);
        }
    };
    pub fn reservationBytes(count: usize) !u64 {
        if (count == 0 or count > 256) return error.InvalidArgument;
        return std.math.add(u64, @sizeOf(Source), try std.math.mul(u64, count, @sizeOf(Route)));
    }
    pub fn reservedBytes(self: *const Source) u64 {
        return self.baseline.bytes;
    }
    fn sameRoot(old: Authority, new: Authority) bool {
        var expected = old;
        expected.local.reporter_incarnation = new.local.reporter_incarnation;
        return new.local.reporter_incarnation != 0 and std.meta.eql(expected, new);
    }
    pub fn create(allocator: std.mem.Allocator, scheduler: *scheduling.Scheduler, initial: *View) !*Source {
        var baseline = scheduler.reserveControlBaseline(try reservationBytes(initial.targets.len)) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        var ref = try initial.retain();
        errdefer ref.release();
        const self = try allocator.create(Source);
        errdefer allocator.destroy(self);
        const routes = try allocator.alloc(Route, initial.targets.len);
        errdefer allocator.free(routes);
        for (routes, initial.targets) |*route, target| {
            if (target.base_uri.len == 0 or target.base_uri.len > max_uri_bytes) return error.InvalidArgument;
            route.set(target.authority, target.base_uri);
        }
        self.* = .{ .allocator = allocator, .scheduler = scheduler, .baseline = baseline, .routes = routes, .current_view = ref };
        return self;
    }
    fn lock(self: *Source) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn bucket(operation: u128) usize {
        return @intCast(operation % 64);
    }
    /// A transient physical control call holds a lease through I/O return and
    /// capability publication. Closing admission does not revoke cleanup.
    pub const Lease = struct {
        source: ?*Source,
        ref: View.Ref,
        revision: u64,
        pub fn view(self: *const Lease) *View {
            return self.ref.view.?;
        }
        pub fn release(self: *Lease) void {
            const source = self.source orelse return;
            self.source = null;
            self.ref.release();
            source.lock();
            std.debug.assert(source.readers != 0);
            source.readers -= 1;
            source.mutex.unlock();
        }
    };
    pub fn current(self: *Source) !Lease {
        self.lock();
        defer self.mutex.unlock();
        if (self.readers == std.math.maxInt(usize)) return error.CleanupCapacityUnavailable;
        const ref = try self.current_view.view.?.retain();
        self.readers += 1;
        return .{ .source = self, .ref = ref, .revision = self.revision };
    }
    pub fn find(self: *Source, operation: u128) !Lease {
        self.lock();
        defer self.mutex.unlock();
        var next = self.buckets[bucket(operation)];
        while (next) |binding| : (next = binding.next) {
            if (binding.operation != operation) continue;
            if (self.readers == std.math.maxInt(usize)) return error.CleanupCapacityUnavailable;
            const ref = try binding.ref.view.?.retain();
            self.readers += 1;
            return .{ .source = self, .ref = ref, .revision = 0 };
        }
        return error.InvalidRemoteAttempt;
    }
    pub const Control = struct {
        original: Lease,
        target: @import("remote_attempt_client.zig").Target,
        uri: [max_uri_bytes]u8 = undefined,
        uri_len: usize = 0,
        /// Form the slice only after this value reaches the caller's stable
        /// address. Returning a slice into the constructor's stack is invalid.
        pub fn endpoint(self: *const Control) @import("remote_attempt_client.zig").Target {
            var target = self.target;
            if (self.uri_len != 0) target.base_uri = self.uri[0..self.uri_len];
            return target;
        }
        pub fn release(self: *Control) void {
            self.original.release();
        }
    };
    /// Copy a bounded route while retaining the original process identity.
    /// This path allocates nothing, even when data-view publication is full.
    pub fn controlFor(self: *Source, operation: u128, destination: usize) !Control {
        var original = if (operation == 0) try self.current() else try self.find(operation);
        errdefer original.release();
        if (destination >= original.view().targets.len) return error.InvalidRemoteAttempt;
        var result: Control = .{ .original = original, .target = original.view().targets[destination] };
        self.lock();
        defer self.mutex.unlock();
        if (destination >= self.routes.len) return error.InvalidRemoteAttempt;
        const route = &self.routes[destination];
        if (std.meta.eql(result.target.authority, route.authority)) {
            result.uri_len = route.len;
            @memcpy(result.uri[0..route.len], route.uri[0..route.len]);
        }
        return result;
    }
    /// Accept one fresh authoritative cut before any data-view allocation.
    /// Missing roots preserve known cleanup routes; observed draining roots
    /// can still update addresses. Every accepted cut fences older publishers,
    /// including cuts that leave the immutable data profile unchanged.
    pub fn acceptSnapshot(self: *Source, expected_revision: u64, descriptors: anytype, authorities: []const Authority, observed: []const bool) !u64 {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed or self.revision == std.math.maxInt(u64)) return error.CleanupCapacityUnavailable;
        if (self.revision != expected_revision) return error.RemoteMembershipChanged;
        const current_view = self.current_view.view.?;
        if (descriptors.len != self.routes.len or authorities.len != descriptors.len or observed.len != descriptors.len) return error.InvalidRemoteMembership;
        for (descriptors, authorities, current_view.targets) |descriptor, authority, old| {
            if (!std.meta.eql(descriptor.local, authority.local) or !sameRoot(old.authority, authority) or descriptor.base_uri.len == 0 or descriptor.base_uri.len > max_uri_bytes) return error.InvalidRemoteMembership;
        }
        for (descriptors, authorities, observed, self.routes) |descriptor, authority, seen, *route| {
            if (seen) route.set(authority, descriptor.base_uri);
        }
        self.revision += 1;
        return self.revision;
    }
    pub const Binding = struct {
        source: *Source,
        operation: u128,
        ref: View.Ref,
        baseline: scheduling.Scheduler.ControlBaseline,
        next: ?*Binding,
        pub fn view(self: *const Binding) *View {
            return self.ref.view.?;
        }
        pub fn release(self: *Binding) void {
            const source = self.source;
            source.lock();
            var link = &source.buckets[bucket(self.operation)];
            while (link.* != self) link = &link.*.?.next;
            link.* = self.next;
            source.mutex.unlock();
            // Keep the binding count until its view and allocation physically
            // retire. A simultaneous deinit must retain the source meanwhile.
            self.ref.release();
            var baseline = self.baseline;
            source.allocator.destroy(self);
            baseline.release();
            source.lock();
            std.debug.assert(source.bindings != 0);
            source.bindings -= 1;
            source.mutex.unlock();
        }
    };
    pub fn bind(self: *Source, operation: u128) !*Binding {
        if (operation == 0) return error.InvalidRemoteAttempt;
        self.lock();
        if (self.closed or self.producers == std.math.maxInt(usize)) {
            self.mutex.unlock();
            return error.CleanupCapacityUnavailable;
        }
        self.producers += 1;
        self.mutex.unlock();
        defer {
            self.lock();
            self.producers -= 1;
            self.mutex.unlock();
        }
        var baseline = self.scheduler.reserveControlBaseline(@sizeOf(Binding)) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        const binding = try self.allocator.create(Binding);
        errdefer self.allocator.destroy(binding);
        self.lock();
        defer self.mutex.unlock();
        if (self.closed or self.bindings == std.math.maxInt(usize)) return error.CleanupCapacityUnavailable;
        const index = bucket(operation);
        var next = self.buckets[index];
        while (next) |existing| : (next = existing.next) {
            if (existing.operation == operation) return error.InvalidRemoteAttempt;
        }
        const ref = try self.current_view.view.?.retain();
        binding.* = .{ .source = self, .operation = operation, .ref = ref, .baseline = baseline, .next = self.buckets[index] };
        self.buckets[index] = binding;
        self.bindings += 1;
        return binding;
    }
    /// Stable durable destination indices cannot be changed by process/URI
    /// refresh. Adding or retiring roots requires a separate journal transition.
    pub fn publish(self: *Source, expected_revision: u64, replacement: *View) !void {
        self.lock();
        if (self.closed or self.revision == std.math.maxInt(u64)) {
            self.mutex.unlock();
            return error.CleanupCapacityUnavailable;
        }
        if (self.revision != expected_revision) {
            self.mutex.unlock();
            return error.RemoteMembershipChanged;
        }
        const previous = self.current_view.view.?;
        if (previous.targets.len != replacement.targets.len) {
            self.mutex.unlock();
            return error.InvalidRemoteMembership;
        }
        for (previous.targets, replacement.targets) |old, new| {
            if (!sameRoot(old.authority, new.authority) or new.base_uri.len == 0 or new.base_uri.len > max_uri_bytes) {
                self.mutex.unlock();
                return error.InvalidRemoteMembership;
            }
        }
        const ref = replacement.retain() catch |err| {
            self.mutex.unlock();
            return err;
        };
        // Trusted direct publication also updates eligible control routes.
        // Missing/ineligible profiles must not erase a fresher observed route.
        for (replacement.targets, self.routes) |target, *route| {
            if (target.available) route.set(target.authority, target.base_uri);
        }
        var old = self.current_view;
        self.current_view = ref;
        self.revision += 1;
        self.mutex.unlock();
        old.release();
    }
    pub fn close(self: *Source) void {
        self.lock();
        self.closed = true;
        self.mutex.unlock();
    }
    pub fn deinit(self: *Source) !void {
        self.lock();
        if (!self.closed or self.bindings != 0 or self.readers != 0 or self.producers != 0) {
            self.mutex.unlock();
            return error.CleanupPending;
        }
        const allocator = self.allocator;
        var ref = self.current_view;
        var baseline = self.baseline;
        self.mutex.unlock();
        ref.release();
        allocator.free(self.routes);
        allocator.destroy(self);
        baseline.release();
    }
};

const TestFixture = struct {
    const replies = @import("../common/remote_attempt_reply.zig");
    const local: @import("../common/remote_membership.zig").Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 22, .reporter_incarnation = 17 };
    const Descriptor = struct { local: @TypeOf(local), base_uri: []const u8 };
    fn view(allocator: std.mem.Allocator, scheduler: *scheduling.Scheduler, reporter: u64, uri: []const u8) !*View {
        var identity = local;
        identity.reporter_incarnation = reporter;
        return View.create(allocator, scheduler, &[_]Descriptor{.{ .local = identity, .base_uri = uri }}, &.{.{ .local = identity, .public_key = @splat(7) }});
    }
    fn failures(allocator: std.mem.Allocator) !void {
        var node = try scheduling.NodeOwner.init(.{ .slots = 2, .control_bytes = 1 << 20 });
        defer node.deinit();
        const initial = try view(allocator, &node.scheduler, 17, "http://old");
        defer initial.release();
        const source = try Source.create(allocator, &node.scheduler, initial);
        defer {
            source.close();
            source.deinit() catch unreachable;
        }
        const binding = try source.bind(1);
        binding.release();
    }
};

test "remote attempt client endpoint publication preserves old operation through replacement and pending shutdown" {
    const a = std.testing.allocator;
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .control_bytes = 1 << 20 });
    defer node.deinit();
    const initial = try TestFixture.view(a, &node.scheduler, 17, "http://old");
    defer initial.release();
    const source = try Source.create(a, &node.scheduler, initial);
    var source_live = true;
    defer if (source_live) {
        source.close();
        source.deinit() catch unreachable;
    };
    var snapshot = try source.current();
    defer snapshot.release();
    const old = try source.bind(11);
    var old_live = true;
    defer if (old_live) old.release();
    const replacement = try TestFixture.view(a, &node.scheduler, 18, "http://new");
    defer replacement.release();
    try source.publish(snapshot.revision, replacement);
    try std.testing.expectError(error.RemoteMembershipChanged, source.publish(snapshot.revision, initial));
    const new = try source.bind(12);
    var new_live = true;
    defer if (new_live) new.release();
    try std.testing.expectEqualStrings("http://old", old.view().targets[0].base_uri);
    try std.testing.expectEqualStrings("http://new", new.view().targets[0].base_uri);
    try std.testing.expectEqual(@as(u64, 17), old.view().targets[0].authority.local.reporter_incarnation);
    try std.testing.expectEqual(@as(u64, 18), new.view().targets[0].authority.local.reporter_incarnation);
    var found = try source.find(11);
    defer found.release();
    try std.testing.expect(found.view() == old.view());
    try std.testing.expectEqual(@as(u64, 0), found.revision);
    try std.testing.expectError(error.InvalidRemoteAttempt, source.find(99));
    try std.testing.expectError(error.InvalidRemoteAttempt, source.bind(11));
    var current = try source.current();
    defer current.release();
    const invalid = try TestFixture.view(a, &node.scheduler, 19, "http://invalid");
    defer invalid.release();
    invalid.targets[0].authority.public_key[0] ^= 1;
    try std.testing.expectError(error.InvalidRemoteMembership, source.publish(current.revision, invalid));
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
    const held = node.scheduler.stats().control_retained_bytes;
    var occupied = try node.scheduler.reserveControlBaseline(node.scheduler.config.control_bytes / 4 - held);
    try std.testing.expectError(error.CleanupCapacityUnavailable, source.bind(99));
    try std.testing.expectError(error.InvalidRemoteAttempt, source.find(99));
    occupied.release();
    try std.testing.expectEqual(held, node.scheduler.stats().control_retained_bytes);
    source.close();
    try std.testing.expectError(error.CleanupCapacityUnavailable, source.bind(13));
    try std.testing.expectError(error.CleanupPending, source.deinit());
    old_live = false;
    old.release();
    new_live = false;
    new.release();
    try std.testing.expectError(error.CleanupPending, source.deinit());
    // Closing preserves control lookup leases until their physical return.
    found.release();
    snapshot.release();
    current.release();
    source_live = false;
    try source.deinit();
    try std.testing.expectEqual(initial.reservedBytes() + replacement.reservedBytes() + invalid.reservedBytes(), node.scheduler.stats().control_retained_bytes);
    try std.testing.checkAllAllocationFailures(a, TestFixture.failures, .{});
}

test "remote attempt client endpoint capability activation remains required after view replacement" {
    const a = std.testing.allocator;
    const Cache = @import("remote_capability_publication.zig").Cache;
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .control_bytes = 1 << 20 });
    defer node.deinit();
    const initial = try TestFixture.view(a, &node.scheduler, 17, "http://old");
    defer initial.release();
    initial.capabilities = try Cache.create(a, &node.scheduler, &.{initial.targets[0].authority}, Clock.now);
    const source = try Source.create(a, &node.scheduler, initial);
    defer {
        source.close();
        source.deinit() catch unreachable;
    }
    initial.capabilities.?.shared_activation = &source.enabled;
    var probe = try initial.capabilities.?.begin(0, initial.targets[0].authority);
    try probe.publish(.{ .authority = initial.targets[0].authority, .families = .{ .group_query = true, .retained_read = true }, .deadline_ns = 200 });
    try std.testing.expect(initial.capabilities.?.everEnabled(false));
    var ticket = try source.current();
    defer ticket.release();
    const replacement = try TestFixture.view(a, &node.scheduler, 18, "http://new");
    defer replacement.release();
    replacement.capabilities = try Cache.create(a, &node.scheduler, &.{replacement.targets[0].authority}, Clock.now);
    replacement.capabilities.?.shared_activation = &source.enabled;
    try source.publish(ticket.revision, replacement);
    try std.testing.expect(replacement.capabilities.?.everEnabled(false) and replacement.capabilities.?.everEnabled(true));
    const families = try replacement.capabilities.?.snapshot(0, replacement.targets[0].authority);
    try std.testing.expect(!families.group_query and !families.retained_read);
    try std.testing.expectError(error.InvalidRemoteReply, replacement.capabilities.?.snapshot(0, initial.targets[0].authority));
}

test "remote attempt client control routes progress with full metadata and no data view allocation" {
    const a = std.testing.allocator;
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .control_bytes = 1 << 20 });
    defer node.deinit();
    const initial = try TestFixture.view(a, &node.scheduler, 17, "http://old");
    defer initial.release();
    const source = try Source.create(a, &node.scheduler, initial);
    defer {
        source.close();
        source.deinit() catch unreachable;
    }
    const binding = try source.bind(77);
    defer binding.release();
    var ticket = try source.current();
    defer ticket.release();
    const descriptors = [_]TestFixture.Descriptor{.{ .local = TestFixture.local, .base_uri = "http://new" }};
    const authorities = [_]TestFixture.replies.Authority{initial.targets[0].authority};
    const held = node.scheduler.stats().control_retained_bytes;
    var occupied = try node.scheduler.reserveControlBaseline(node.scheduler.config.control_bytes / 4 - held);
    defer occupied.release();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    {
        // Any attempt to allocate through the source fails. Accepting a cut
        // and selecting a control address must only use preallocated storage.
        const allocator = source.allocator;
        source.allocator = failing.allocator();
        defer source.allocator = allocator;
        _ = try source.acceptSnapshot(ticket.revision, &descriptors, &authorities, &.{true});
        try std.testing.expectError(error.CleanupCapacityUnavailable, View.create(failing.allocator(), &node.scheduler, &descriptors, &authorities));
        var control = try source.controlFor(77, 0);
        defer control.release();
        try std.testing.expectEqualStrings("http://new", control.endpoint().base_uri);
        try std.testing.expectEqualDeep(authorities[0], control.endpoint().authority);
        try std.testing.expectEqualStrings("http://old", binding.view().targets[0].base_uri);
        try std.testing.expectEqualStrings("http://old", ticket.view().targets[0].base_uri);
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    }
    occupied.release();
    try std.testing.expectEqual(held, node.scheduler.stats().control_retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
}

test "remote attempt client control cuts preserve copyout missing peers and unchanged view reversions" {
    const a = std.testing.allocator;
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .control_bytes = 1 << 20 });
    defer node.deinit();
    const initial = try TestFixture.view(a, &node.scheduler, 17, "http://old");
    defer initial.release();
    const source = try Source.create(a, &node.scheduler, initial);
    defer {
        source.close();
        source.deinit() catch unreachable;
    }
    const binding = try source.bind(77);
    defer binding.release();
    var ticket = try source.current();
    defer ticket.release();
    const changed = [_]TestFixture.Descriptor{.{ .local = TestFixture.local, .base_uri = "http://new" }};
    const unchanged = [_]TestFixture.Descriptor{.{ .local = TestFixture.local, .base_uri = "http://old" }};
    const authorities = [_]TestFixture.replies.Authority{initial.targets[0].authority};
    const revision = try source.acceptSnapshot(ticket.revision, &changed, &authorities, &.{true});
    var pending = try source.controlFor(77, 0);
    defer pending.release();
    const missing_revision = try source.acceptSnapshot(revision, &unchanged, &authorities, &.{false});
    {
        var after_missing = try source.controlFor(77, 0);
        defer after_missing.release();
        try std.testing.expectEqualStrings("http://new", after_missing.endpoint().base_uri);
    }
    try std.testing.expectError(error.RemoteMembershipChanged, source.acceptSnapshot(revision, &changed, &authorities, &.{true}));
    try std.testing.expectError(error.RemoteMembershipChanged, source.publish(revision, initial));
    const restored_revision = try source.acceptSnapshot(missing_revision, &unchanged, &authorities, &.{true});
    {
        var restored = try source.controlFor(77, 0);
        defer restored.release();
        try std.testing.expectEqualStrings("http://old", restored.endpoint().base_uri);
        try std.testing.expectEqualStrings("http://old", ticket.view().targets[0].base_uri);
    }
    // The already issued selection owns bytes, not a slice into the table.
    try std.testing.expectEqualStrings("http://new", pending.endpoint().base_uri);
    var replacement_local = TestFixture.local;
    replacement_local.reporter_incarnation = 18;
    const replaced = [_]TestFixture.Descriptor{.{ .local = replacement_local, .base_uri = "http://different-process" }};
    _ = try source.acceptSnapshot(restored_revision, &replaced, &.{.{ .local = replacement_local, .public_key = authorities[0].public_key }}, &.{true});
    var original_process = try source.controlFor(77, 0);
    defer original_process.release();
    try std.testing.expectEqualStrings("http://old", original_process.endpoint().base_uri);
    try std.testing.expectEqualDeep(authorities[0], original_process.endpoint().authority);
    source.close();
    try std.testing.expectError(error.CleanupPending, source.deinit());
}
