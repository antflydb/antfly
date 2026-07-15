// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Fail-closed runtime authority derived from one exact Kubernetes Lease.
//!
//! A standby may observe a Lease held by another node without poisoning its
//! local data. Once this runtime has observed itself as the valid holder,
//! however, authority is irreversible: a transfer, scope rollback, expiry, or
//! prolonged loss of the API durably fences this data generation.

const std = @import("std");
const fs_paths = @import("../../common/fs_paths.zig");
const http_common = @import("../../common/http/http_common.zig");
const std_http_executor = @import("../../common/http/std_http_executor.zig");

pub const service_account_token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token";
pub const service_account_ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
pub const max_future_renew_skew_ns: u64 = 5 * std.time.ns_per_s;

pub const Scope = struct {
    /// Stable identity of the HA topology. This is deliberately independent
    /// of timeline, epoch, current-primary, and the local AntflyCluster name:
    /// all of those legitimately change during promotion.
    topology_id: []const u8,
    node_id: []const u8,
    /// Exact materialized data generation protected by a durable fence.
    data_generation: []const u8,
};

pub const Config = struct {
    scope: Scope,
    grace_ns: u64,
    sentinel_path: []const u8,
};

pub const Decision = enum {
    /// No current exact, unexpired Lease has been validated.
    waiting,
    /// The exact, unexpired Lease was validated and is held by another node.
    observed,
    /// The Lease now names this node, but no strictly newer renewal has yet
    /// proven that the new holder is actively renewing its authority.
    pending_authority,
    authorized,
    grace,
    fence,
};

pub const FenceReason = enum {
    persisted,
    holder_changed,
    scope_changed,
    generation_rollback,
    renewal_rollback,
    lease_expired,
    api_unreachable,
};

pub const Watchdog = struct {
    cfg: Config,
    authorized_once: bool = false,
    latched: bool = false,
    last_generation: u64 = 0,
    last_renew_ns: u64 = 0,
    last_observed_holder: [128]u8 = undefined,
    last_observed_holder_len: u8 = 0,
    local_deadline_ns: u64 = 0,
    fence_reason: ?FenceReason = null,

    pub fn init(cfg: Config, sentinel_generation: ?[]const u8, allow_generation_rotation: bool) !Watchdog {
        if (cfg.grace_ns == 0 or cfg.sentinel_path.len == 0 or cfg.scope.node_id.len == 0 or
            cfg.scope.topology_id.len == 0 or cfg.scope.data_generation.len == 0)
        {
            return error.InvalidLeaseWatchdogConfig;
        }
        const sentinel_matches = if (sentinel_generation) |generation|
            std.mem.eql(u8, generation, cfg.scope.data_generation)
        else
            false;
        // A caller-selected generation string must never clear a fence. The
        // only permitted mismatch is a generation whose immutable activation
        // receipt was validated by startup before constructing this watchdog.
        if (sentinel_generation != null and !sentinel_matches and !allow_generation_rotation) {
            return .{
                .cfg = cfg,
                .authorized_once = true,
                .latched = true,
                .fence_reason = .persisted,
            };
        }
        return .{
            .cfg = cfg,
            .authorized_once = sentinel_matches,
            .latched = sentinel_matches,
            .fence_reason = if (sentinel_matches) .persisted else null,
        };
    }

    pub fn authorityGranted(self: *const Watchdog) bool {
        return self.authorized_once and !self.latched;
    }

    /// The returned slice is borrowed from mutable watchdog state. Callers
    /// must copy it while holding the mutex that serializes `observe`.
    pub fn observedHolder(self: *const Watchdog) []const u8 {
        return self.last_observed_holder[0..self.last_observed_holder_len];
    }

    pub fn observe(
        self: *Watchdog,
        alloc: std.mem.Allocator,
        body: []const u8,
        realtime_ns: u64,
        monotonic_ns: u64,
    ) !Decision {
        if (self.latched) return .fence;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
        defer parsed.deinit();
        const root = switch (parsed.value) {
            .object => |value| value,
            else => return error.InvalidLeaseResponse,
        };
        const metadata = try requiredObject(root, "metadata");
        const spec = try requiredObject(root, "spec");
        const annotations = try requiredObject(metadata, "annotations");

        const holder = try requiredString(spec, "holderIdentity");
        if (holder.len == 0 or holder.len > 128) return error.InvalidLeaseResponse;
        const generation = try requiredPositiveInt(spec, "leaseTransitions");
        const duration_seconds = try requiredPositiveInt(spec, "leaseDurationSeconds");
        const renew_time = try requiredString(spec, "renewTime");
        const renew_ns = try rfc3339UnixNs(renew_time);
        const duration_ns = std.math.mul(u64, duration_seconds, std.time.ns_per_s) catch
            return error.InvalidLeaseResponse;
        if (self.cfg.grace_ns > duration_ns) return error.LeaseGraceExceedsDuration;
        if (renew_ns > realtime_ns +| max_future_renew_skew_ns) return error.LeaseRenewTimeInFuture;

        const scope_matches = try self.scopeMatches(annotations);
        if (self.authorized_once and !scope_matches) return self.latch(.scope_changed);
        if (generation < self.last_generation) {
            if (self.authorized_once) return self.latch(.generation_rollback);
            return error.LeaseGenerationRollback;
        }
        if (self.last_renew_ns != 0 and renew_ns < self.last_renew_ns) {
            if (self.authorized_once) return self.latch(.renewal_rollback);
            return error.LeaseRenewalRollback;
        }
        if (self.authorized_once and !std.mem.eql(u8, holder, self.cfg.scope.node_id)) return self.latch(.holder_changed);
        if (renew_ns > std.math.maxInt(u64) - duration_ns or realtime_ns >= renew_ns + duration_ns) {
            if (self.authorized_once) return self.latch(.lease_expired);
            return .waiting;
        }
        if (!scope_matches) return error.LeaseScopeMismatch;

        // A standby must publish proof that it is actively monitoring this
        // exact topology before the holder transfer. Preserve the monotonic
        // Lease generation even though public authority is not yet granted.
        const newer_renewal = renew_ns > self.last_renew_ns;
        self.last_generation = @max(self.last_generation, generation);
        self.last_renew_ns = @max(self.last_renew_ns, renew_ns);
        @memcpy(self.last_observed_holder[0..holder.len], holder);
        self.last_observed_holder_len = @intCast(holder.len);
        if (!std.mem.eql(u8, holder, self.cfg.scope.node_id)) return .observed;

        if (!self.authorized_once and !newer_renewal) return .pending_authority;
        if (!self.authorized_once or newer_renewal) {
            self.authorized_once = true;
            self.local_deadline_ns = monotonic_ns +| self.cfg.grace_ns;
            return .authorized;
        }
        // Re-reading one unchanged cached Lease is not proof that the
        // authority is still progressing. It must not extend the local
        // suspend-inclusive deadline indefinitely.
        if (monotonic_ns < self.local_deadline_ns) return .grace;
        return self.latch(.api_unreachable);
    }

    pub fn noteAPIFailure(self: *Watchdog, monotonic_ns: u64) Decision {
        if (self.latched) return .fence;
        if (!self.authorized_once) return .waiting;
        if (monotonic_ns < self.local_deadline_ns) return .grace;
        return self.latch(.api_unreachable);
    }

    fn latch(self: *Watchdog, reason: FenceReason) Decision {
        self.latched = true;
        self.fence_reason = reason;
        return .fence;
    }

    fn scopeMatches(self: *const Watchdog, annotations: std.json.ObjectMap) !bool {
        return std.mem.eql(
            u8,
            try requiredString(annotations, "antfly.io/ha-fence-topology-id"),
            self.cfg.scope.topology_id,
        );
    }

    pub fn persistFence(self: *const Watchdog, alloc: std.mem.Allocator, io: std.Io) !void {
        if (!self.latched) return error.LeaseFenceNotLatched;
        const parent = std.fs.path.dirname(self.cfg.sentinel_path) orelse return error.InvalidLeaseWatchdogConfig;
        const temp = try std.fmt.allocPrint(alloc, "{s}.tmp", .{self.cfg.sentinel_path});
        defer alloc.free(temp);
        try fs_paths.createDirPathPortable(io, parent);
        const body = try std.fmt.allocPrint(
            alloc,
            "version=2\ntopology_id={s}\nnode_id={s}\ndata_generation={s}\nlease_transitions={d}\nreason={s}\n",
            .{ self.cfg.scope.topology_id, self.cfg.scope.node_id, self.cfg.scope.data_generation, self.last_generation, @tagName(self.fence_reason orelse .persisted) },
        );
        defer alloc.free(body);
        {
            var file = try fs_paths.createFilePortable(io, temp, .{ .truncate = true });
            defer file.close(io);
            var buffer: [4096]u8 = undefined;
            var writer = file.writer(io, &buffer);
            try writer.interface.writeAll(body);
            try writer.end();
            try file.sync(io);
        }
        try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), self.cfg.sentinel_path, io);
        try fs_paths.syncDirPortable(io, parent);
    }
};

pub fn sentinelExists(io: std.Io, path: []const u8) !bool {
    var file = if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openFileAbsolute(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        }
    else
        std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
    file.close(io);
    return true;
}

/// Returns an owned generation from a well-formed durable fence, or null when
/// no fence exists. Unknown/legacy contents fail closed instead of being
/// silently treated as a repair authorization.
pub fn loadSentinelGenerationAlloc(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !?[]u8 {
    const raw = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(16 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var lines = std.mem.splitScalar(u8, raw, '\n');
    var version_ok = false;
    var generation: ?[]const u8 = null;
    while (lines.next()) |line| {
        if (std.mem.eql(u8, line, "version=2")) version_ok = true;
        if (std.mem.startsWith(u8, line, "data_generation=")) generation = line["data_generation=".len..];
    }
    if (!version_ok or generation == null or generation.?.len == 0) return error.InvalidLeaseFenceSentinel;
    return try alloc.dupe(u8, generation.?);
}

pub fn leaseURLAlloc(
    alloc: std.mem.Allocator,
    host: []const u8,
    port: []const u8,
    namespace: []const u8,
    lease_name: []const u8,
) ![]u8 {
    if (!kubernetesName(host, true) or !kubernetesName(port, false) or
        !kubernetesName(namespace, false) or !kubernetesName(lease_name, false))
    {
        return error.InvalidLeaseWatchdogConfig;
    }
    return try std.fmt.allocPrint(
        alloc,
        "https://{s}:{s}/apis/coordination.k8s.io/v1/namespaces/{s}/leases/{s}",
        .{ host, port, namespace, lease_name },
    );
}

pub fn configureKubernetesCA(
    executor: *std_http_executor.StdHttpExecutor,
    ca_path: []const u8,
) !void {
    const io = executor.io_impl.io();
    const now = std.Io.Clock.real.now(io);
    executor.client.ca_bundle.deinit(executor.alloc);
    executor.client.ca_bundle = .empty;
    try executor.client.ca_bundle.addCertsFromFilePathAbsolute(executor.alloc, io, now, ca_path);
    // Prevent std.http.Client from replacing the explicitly loaded in-cluster
    // CA with the container's unrelated system trust bundle.
    executor.client.now = now;
}

pub fn fetchLeaseAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    executor: http_common.RequestExecutor,
    uri: []const u8,
    token_path: []const u8,
    timeout_ms: u32,
) ![]u8 {
    const raw_token = try std.Io.Dir.cwd().readFileAlloc(io, token_path, alloc, .limited(64 * 1024));
    defer alloc.free(raw_token);
    const token = std.mem.trim(u8, raw_token, " \t\r\n");
    if (token.len == 0) return error.KubernetesServiceAccountTokenMissing;
    const authorization = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
    defer alloc.free(authorization);
    var response = try executor.execute(alloc, .{
        .method = .GET,
        .uri = uri,
        .authorization = authorization,
        .timeout_ms = timeout_ms,
    });
    defer response.deinit(alloc);
    if (response.status != 200) return error.KubernetesLeaseRequestRejected;
    if (response.body.len == 0) return error.InvalidLeaseResponse;
    return try alloc.dupe(u8, response.body);
}

fn kubernetesName(value: []const u8, allow_ip: bool) bool {
    if (value.len == 0 or value.len > 253) return false;
    for (value) |ch| {
        if (std.ascii.isAlphanumeric(ch) or ch == '-' or ch == '.') continue;
        if (allow_ip and (ch == ':' or ch == '[' or ch == ']')) continue;
        return false;
    }
    return true;
}

fn requiredObject(object: std.json.ObjectMap, name: []const u8) !std.json.ObjectMap {
    const value = object.get(name) orelse return error.InvalidLeaseResponse;
    return switch (value) {
        .object => |result| result,
        else => error.InvalidLeaseResponse,
    };
}

fn requiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidLeaseResponse;
    return switch (value) {
        .string => |result| result,
        else => error.InvalidLeaseResponse,
    };
}

fn requiredPositiveInt(object: std.json.ObjectMap, name: []const u8) !u64 {
    const value = object.get(name) orelse return error.InvalidLeaseResponse;
    const result: u64 = switch (value) {
        .integer => |integer| std.math.cast(u64, integer) orelse return error.InvalidLeaseResponse,
        else => return error.InvalidLeaseResponse,
    };
    if (result == 0) return error.InvalidLeaseResponse;
    return result;
}

fn rfc3339UnixNs(text: []const u8) !u64 {
    if (text.len < 20 or text[text.len - 1] != 'Z' or text[4] != '-' or text[7] != '-' or
        text[10] != 'T' or text[13] != ':' or text[16] != ':') return error.InvalidLeaseResponse;
    const year = try std.fmt.parseInt(i32, text[0..4], 10);
    const month = try std.fmt.parseInt(u8, text[5..7], 10);
    const day = try std.fmt.parseInt(u8, text[8..10], 10);
    const hour = try std.fmt.parseInt(u8, text[11..13], 10);
    const minute = try std.fmt.parseInt(u8, text[14..16], 10);
    const second = try std.fmt.parseInt(u8, text[17..19], 10);
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour > 23 or minute > 59 or second > 60) return error.InvalidLeaseResponse;
    const year_adj = @as(i64, year) - @intFromBool(month <= 2);
    const era = @divFloor(year_adj, 400);
    const yoe = year_adj - era * 400;
    const month_prime = @as(i64, month) + if (month > 2) @as(i64, -3) else 9;
    const doy = @divFloor(153 * month_prime + 2, 5) + @as(i64, day) - 1;
    const days = era * 146_097 + yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy - 719_468;
    const seconds = days * 86_400 + @as(i64, hour) * 3_600 + @as(i64, minute) * 60 + @as(i64, second);
    if (seconds < 0) return error.InvalidLeaseResponse;
    var fraction_ns: u64 = 0;
    if (text.len > 20) {
        if (text[19] != '.' or text.len == 21) return error.InvalidLeaseResponse;
        const digits = text[20 .. text.len - 1];
        if (digits.len > 9) return error.InvalidLeaseResponse;
        for (digits) |digit| {
            if (!std.ascii.isDigit(digit)) return error.InvalidLeaseResponse;
            fraction_ns = fraction_ns * 10 + digit - '0';
        }
        var padding = 9 - digits.len;
        while (padding > 0) : (padding -= 1) fraction_ns *= 10;
    }
    const whole_ns = std.math.mul(u64, @intCast(seconds), std.time.ns_per_s) catch return error.InvalidLeaseResponse;
    return std.math.add(u64, whole_ns, fraction_ns) catch return error.InvalidLeaseResponse;
}

test "kubernetes lease watchdog fences transfer and API partition and never reopens" {
    const body =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:01Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, false);
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, body, realtime, 100));
    try std.testing.expect(watchdog.authorityGranted());
    try std.testing.expectEqual(Decision.grace, watchdog.noteAPIFailure(9 * std.time.ns_per_s));
    try std.testing.expectEqual(Decision.fence, watchdog.noteAPIFailure(11 * std.time.ns_per_s));
    try std.testing.expect(!watchdog.authorityGranted());
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, body, realtime, 12 * std.time.ns_per_s));
}

test "kubernetes lease watchdog standby waits for transfer then fences rollback" {
    const before =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const after =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"standby-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":4}}
    ;
    const now = try rfc3339UnixNs("2026-07-15T12:00:03Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, true);
    try std.testing.expectEqual(Decision.observed, try watchdog.observe(std.testing.allocator, before, now, 1));
    try std.testing.expectEqualStrings("primary-a", watchdog.observedHolder());
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, after, now, 2));
    try std.testing.expectEqualStrings("standby-a", watchdog.observedHolder());
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, before, now, 3));
    try std.testing.expectEqual(FenceReason.generation_rollback, watchdog.fence_reason.?);
}

test "kubernetes lease watchdog rejects expired pre-transfer lease as inactive" {
    const expired =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const after_expiry = try rfc3339UnixNs("2026-07-15T12:00:31Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, true);

    // `waiting`, rather than `observed`, tells the runtime not to publish or
    // refresh Active capability proof for this stale Lease.
    try std.testing.expectEqual(Decision.waiting, try watchdog.observe(std.testing.allocator, expired, after_expiry, 1));
    try std.testing.expectEqual(@as(u64, 0), watchdog.last_generation);
    try std.testing.expectEqual(@as(usize, 0), watchdog.observedHolder().len);
}

test "kubernetes lease watchdog duplicate renewal cannot extend authority deadline" {
    const body =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:01Z");
    const grace = 10 * std.time.ns_per_s;
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = grace, .sentinel_path = "/tmp/fence" }, null, false);

    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, body, realtime, 100));
    const first_deadline = watchdog.local_deadline_ns;
    try std.testing.expectEqual(Decision.grace, try watchdog.observe(std.testing.allocator, body, realtime, 5 * std.time.ns_per_s));
    try std.testing.expectEqual(first_deadline, watchdog.local_deadline_ns);
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, body, realtime, 11 * std.time.ns_per_s));
    try std.testing.expectEqual(FenceReason.api_unreachable, watchdog.fence_reason.?);
}

test "kubernetes lease watchdog rejects older and implausibly future renewals" {
    const initial =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const older =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:01Z","leaseTransitions":3}}
    ;
    const future =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:08Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:02Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, false);
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, initial, realtime, 1));
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, older, realtime, 2));
    try std.testing.expectEqual(FenceReason.renewal_rollback, watchdog.fence_reason.?);

    var fresh = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, false);
    try std.testing.expectError(error.LeaseRenewTimeInFuture, fresh.observe(std.testing.allocator, future, realtime, 1));
}

test "kubernetes lease watchdog strictly newer renewal extends authority deadline" {
    const initial =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const newer =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:03Z");
    const grace = 10 * std.time.ns_per_s;
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = grace, .sentinel_path = "/tmp/fence" }, null, false);
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, initial, realtime, 1));
    const first_deadline = watchdog.local_deadline_ns;
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, newer, realtime, 5 * std.time.ns_per_s));
    try std.testing.expect(watchdog.local_deadline_ns > first_deadline);
    try std.testing.expectEqual(5 * std.time.ns_per_s + grace, watchdog.local_deadline_ns);
}

test "kubernetes lease watchdog transfer requires nonregressing then strictly newer renewal" {
    const before =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const transfer_older =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"standby-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:01Z","leaseTransitions":4}}
    ;
    const transfer_equal =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"standby-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":4}}
    ;
    const renewed =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"standby-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:03Z","leaseTransitions":4}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:03Z");

    var older = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, true);
    try std.testing.expectEqual(Decision.observed, try older.observe(std.testing.allocator, before, realtime, 1));
    try std.testing.expectError(error.LeaseRenewalRollback, older.observe(std.testing.allocator, transfer_older, realtime, 2));

    var equal = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, true);
    try std.testing.expectEqual(Decision.observed, try equal.observe(std.testing.allocator, before, realtime, 1));
    try std.testing.expectEqual(Decision.pending_authority, try equal.observe(std.testing.allocator, transfer_equal, realtime, 2));
    try std.testing.expect(!equal.authorityGranted());
    try std.testing.expectEqual(Decision.authorized, try equal.observe(std.testing.allocator, renewed, realtime, 3));
    try std.testing.expect(equal.authorityGranted());
}

test "kubernetes lease watchdog persisted fence only rotates after validated new data generation" {
    const same = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "old-primary", .data_generation = "generation-a" }, .grace_ns = std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, "generation-a", true);
    try std.testing.expect(same.latched);

    const unvalidated = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "old-primary", .data_generation = "generation-b" }, .grace_ns = std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, "generation-a", false);
    try std.testing.expect(unvalidated.latched);

    var repaired = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "old-primary", .data_generation = "generation-b" }, .grace_ns = std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, "generation-a", true);
    try std.testing.expect(!repaired.latched);
    try std.testing.expect(!repaired.authorityGranted());
}
