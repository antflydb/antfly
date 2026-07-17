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
const builtin = @import("builtin");
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
    /// Exact runtime process incarnation permitted to consume a self-held
    /// Lease. Other holders' process bindings are intentionally ignored.
    process_boot_id: []const u8 = "",
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
    process_changed,
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

    pub fn init(cfg: Config, sentinel_generation: ?[]const u8, repaired_generation: ?[]const u8) !Watchdog {
        if (cfg.grace_ns == 0 or cfg.sentinel_path.len == 0 or cfg.scope.node_id.len == 0 or
            cfg.scope.topology_id.len == 0 or cfg.scope.data_generation.len == 0 or
            (!builtin.is_test and cfg.scope.process_boot_id.len == 0))
        {
            return error.InvalidLeaseWatchdogConfig;
        }
        const repair_matches = if (repaired_generation) |generation|
            std.mem.eql(u8, generation, cfg.scope.data_generation)
        else
            false;
        // A caller-selected generation string must never clear a fence. The
        // only permitted mismatch is the deterministic generation from a
        // durable repair receipt bound to this exact persisted sentinel.
        if (sentinel_generation != null and !repair_matches) {
            return .{
                .cfg = cfg,
                .authorized_once = true,
                .latched = true,
                .fence_reason = .persisted,
            };
        }
        return .{
            .cfg = cfg,
            .authorized_once = false,
            .latched = false,
            .fence_reason = null,
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

        const process_matches = if (std.mem.eql(u8, holder, self.cfg.scope.node_id) and self.cfg.scope.process_boot_id.len != 0)
            std.mem.eql(
                u8,
                try requiredString(annotations, "antfly.io/ha-fence-process-boot-id"),
                self.cfg.scope.process_boot_id,
            )
        else
            true;
        if (self.authorized_once and !process_matches) return self.latch(.process_changed);

        // A standby must publish proof that it is actively monitoring this
        // exact topology before the holder transfer. Preserve the monotonic
        // Lease generation even though public authority is not yet granted.
        // A process incarnation must first establish an observation baseline.
        // Treating an arbitrary pre-existing self-held Lease as "newer" than
        // the zero initializer lets a replacement process inherit the prior
        // process's still-live authority. The operator observes this process's
        // pod/process proof and performs a subsequent renewal; only that
        // strictly post-observation renewal may open writes.
        const newer_renewal = self.last_renew_ns != 0 and renew_ns > self.last_renew_ns;
        self.last_generation = @max(self.last_generation, generation);
        self.last_renew_ns = @max(self.last_renew_ns, renew_ns);
        @memcpy(self.last_observed_holder[0..holder.len], holder);
        self.last_observed_holder_len = @intCast(holder.len);
        if (!std.mem.eql(u8, holder, self.cfg.scope.node_id)) return .observed;
        if (!process_matches) return .pending_authority;

        if (!self.authorized_once and !newer_renewal) return .pending_authority;
        if (!self.authorized_once or newer_renewal) {
            self.authorized_once = true;
            // Never manufacture authority past the server-issued Lease
            // expiry. `realtime_ns` and `monotonic_ns` are sampled only after
            // the HTTP response has completed, so request latency is already
            // charged against the remaining Lease lifetime.
            const lease_expires_ns = renew_ns + duration_ns;
            const remaining_lease_ns = lease_expires_ns - realtime_ns;
            self.local_deadline_ns = monotonic_ns +| @min(self.cfg.grace_ns, remaining_lease_ns);
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
    var sentinel = (try loadSentinelAlloc(alloc, io, path)) orelse return null;
    defer sentinel.deinit(alloc);
    return try alloc.dupe(u8, sentinel.data_generation);
}

pub const Sentinel = struct {
    topology_id: []u8,
    node_id: []u8,
    data_generation: []u8,
    lease_transitions: u64,
    reason: []u8,

    pub fn deinit(self: *Sentinel, alloc: std.mem.Allocator) void {
        alloc.free(self.topology_id);
        alloc.free(self.node_id);
        alloc.free(self.data_generation);
        alloc.free(self.reason);
        self.* = undefined;
    }
};

pub fn loadSentinelAlloc(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !?Sentinel {
    const raw = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(16 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var lines = std.mem.splitScalar(u8, raw, '\n');
    var version_ok = false;
    var topology_id: ?[]const u8 = null;
    var node_id: ?[]const u8 = null;
    var generation: ?[]const u8 = null;
    var transitions: ?u64 = null;
    var reason: ?[]const u8 = null;
    while (lines.next()) |line| {
        if (std.mem.eql(u8, line, "version=2")) version_ok = true;
        if (std.mem.startsWith(u8, line, "topology_id=")) topology_id = line["topology_id=".len..];
        if (std.mem.startsWith(u8, line, "node_id=")) node_id = line["node_id=".len..];
        if (std.mem.startsWith(u8, line, "data_generation=")) generation = line["data_generation=".len..];
        if (std.mem.startsWith(u8, line, "lease_transitions=")) transitions = std.fmt.parseInt(u64, line["lease_transitions=".len..], 10) catch return error.InvalidLeaseFenceSentinel;
        if (std.mem.startsWith(u8, line, "reason=")) reason = line["reason=".len..];
    }
    if (!version_ok or topology_id == null or topology_id.?.len == 0 or node_id == null or node_id.?.len == 0 or
        generation == null or generation.?.len == 0 or transitions == null or transitions.? == 0 or reason == null or reason.?.len == 0)
    {
        return error.InvalidLeaseFenceSentinel;
    }
    return .{
        .topology_id = try alloc.dupe(u8, topology_id.?),
        .node_id = try alloc.dupe(u8, node_id.?),
        .data_generation = try alloc.dupe(u8, generation.?),
        .lease_transitions = transitions.?,
        .reason = try alloc.dupe(u8, reason.?),
    };
}

pub fn repairReceiptPathAlloc(alloc: std.mem.Allocator, sentinel_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}.repair", .{sentinel_path});
}

fn repairGeneration(
    topology_id: []const u8,
    node_id: []const u8,
    prior_generation: []const u8,
    prior_transitions: u64,
    target_timeline_id: u64,
    target_epoch: u64,
    applied_boundary_lsn: u64,
    evidence_sha256: []const u8,
) [71]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-ha-repair-v1\x00");
    for ([_][]const u8{ topology_id, node_id, prior_generation }) |field| {
        hash.update(field);
        hash.update("\x00");
    }
    hash.update(evidence_sha256);
    hash.update("\x00");
    var numbers: [32]u8 = undefined;
    std.mem.writeInt(u64, numbers[0..8], prior_transitions, .big);
    std.mem.writeInt(u64, numbers[8..16], target_timeline_id, .big);
    std.mem.writeInt(u64, numbers[16..24], target_epoch, .big);
    std.mem.writeInt(u64, numbers[24..32], applied_boundary_lsn, .big);
    hash.update(&numbers);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    var result: [71]u8 = undefined;
    @memcpy(result[0..7], "repair-");
    _ = std.fmt.bufPrint(result[7..], "{x}", .{digest}) catch unreachable;
    return result;
}

pub fn persistRepairReceipt(
    alloc: std.mem.Allocator,
    io: std.Io,
    sentinel_path: []const u8,
    topology_id: []const u8,
    node_id: []const u8,
    target_timeline_id: u64,
    target_epoch: u64,
    applied_boundary_lsn: u64,
    evidence_sha256: []const u8,
) ![71]u8 {
    var sentinel = (try loadSentinelAlloc(alloc, io, sentinel_path)) orelse return error.LeaseFenceSentinelMissing;
    defer sentinel.deinit(alloc);
    if (!std.mem.eql(u8, sentinel.topology_id, topology_id) or !std.mem.eql(u8, sentinel.node_id, node_id))
        return error.LeaseFenceSentinelScopeMismatch;
    const generation = repairGeneration(topology_id, node_id, sentinel.data_generation, sentinel.lease_transitions, target_timeline_id, target_epoch, applied_boundary_lsn, evidence_sha256);
    const receipt_path = try repairReceiptPathAlloc(alloc, sentinel_path);
    defer alloc.free(receipt_path);
    const temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{receipt_path});
    defer alloc.free(temp_path);
    const parent = std.fs.path.dirname(receipt_path) orelse return error.InvalidLeaseWatchdogConfig;
    const body = try std.fmt.allocPrint(alloc, "version=1\ntopology_id={s}\nnode_id={s}\nprior_data_generation={s}\nprior_lease_transitions={d}\ntarget_timeline_id={d}\ntarget_epoch={d}\napplied_boundary_lsn={d}\nevidence_sha256={s}\ndata_generation={s}\n", .{ topology_id, node_id, sentinel.data_generation, sentinel.lease_transitions, target_timeline_id, target_epoch, applied_boundary_lsn, evidence_sha256, &generation });
    defer alloc.free(body);
    {
        var file = try fs_paths.createFilePortable(io, temp_path, .{ .truncate = true });
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(body);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temp_path, std.Io.Dir.cwd(), receipt_path, io);
    try fs_paths.syncDirPortable(io, parent);
    return generation;
}

pub fn loadValidatedRepairGenerationAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    sentinel_path: []const u8,
    expected_topology_id: []const u8,
    expected_node_id: []const u8,
) !?[]u8 {
    var sentinel = (try loadSentinelAlloc(alloc, io, sentinel_path)) orelse return null;
    defer sentinel.deinit(alloc);
    if (!std.mem.eql(u8, sentinel.topology_id, expected_topology_id) or !std.mem.eql(u8, sentinel.node_id, expected_node_id)) return null;
    const receipt_path = try repairReceiptPathAlloc(alloc, sentinel_path);
    defer alloc.free(receipt_path);
    const raw = std.Io.Dir.cwd().readFileAlloc(io, receipt_path, alloc, .limited(16 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var version_ok = false;
    var topology_id: ?[]const u8 = null;
    var node_id: ?[]const u8 = null;
    var prior_generation: ?[]const u8 = null;
    var prior_transitions: ?u64 = null;
    var timeline: ?u64 = null;
    var epoch: ?u64 = null;
    var boundary: ?u64 = null;
    var evidence_sha256: ?[]const u8 = null;
    var generation: ?[]const u8 = null;
    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line| {
        if (std.mem.eql(u8, line, "version=1")) version_ok = true;
        if (std.mem.startsWith(u8, line, "topology_id=")) topology_id = line["topology_id=".len..];
        if (std.mem.startsWith(u8, line, "node_id=")) node_id = line["node_id=".len..];
        if (std.mem.startsWith(u8, line, "prior_data_generation=")) prior_generation = line["prior_data_generation=".len..];
        if (std.mem.startsWith(u8, line, "prior_lease_transitions=")) prior_transitions = std.fmt.parseInt(u64, line["prior_lease_transitions=".len..], 10) catch return error.InvalidLeaseRepairReceipt;
        if (std.mem.startsWith(u8, line, "target_timeline_id=")) timeline = std.fmt.parseInt(u64, line["target_timeline_id=".len..], 10) catch return error.InvalidLeaseRepairReceipt;
        if (std.mem.startsWith(u8, line, "target_epoch=")) epoch = std.fmt.parseInt(u64, line["target_epoch=".len..], 10) catch return error.InvalidLeaseRepairReceipt;
        if (std.mem.startsWith(u8, line, "applied_boundary_lsn=")) boundary = std.fmt.parseInt(u64, line["applied_boundary_lsn=".len..], 10) catch return error.InvalidLeaseRepairReceipt;
        if (std.mem.startsWith(u8, line, "evidence_sha256=")) evidence_sha256 = line["evidence_sha256=".len..];
        if (std.mem.startsWith(u8, line, "data_generation=")) generation = line["data_generation=".len..];
    }
    if (!version_ok or topology_id == null or node_id == null or prior_generation == null or prior_transitions == null or
        timeline == null or epoch == null or boundary == null or evidence_sha256 == null or generation == null or timeline.? == 0 or epoch.? == 0 or boundary.? == 0)
        return error.InvalidLeaseRepairReceipt;
    const expected = repairGeneration(topology_id.?, node_id.?, prior_generation.?, prior_transitions.?, timeline.?, epoch.?, boundary.?, evidence_sha256.?);
    if (!std.mem.eql(u8, generation.?, &expected)) return error.InvalidLeaseRepairReceipt;
    const prior_sentinel_matches = std.mem.eql(u8, prior_generation.?, sentinel.data_generation) and
        prior_transitions.? == sentinel.lease_transitions;
    const rotated_sentinel_matches = std.mem.eql(u8, &expected, sentinel.data_generation) and
        prior_transitions.? == sentinel.lease_transitions and std.mem.eql(u8, sentinel.reason, "repaired");
    if (!prior_sentinel_matches and !rotated_sentinel_matches) return null;
    return try alloc.dupe(u8, generation.?);
}

pub fn rotateSentinelAfterValidatedRepair(
    alloc: std.mem.Allocator,
    io: std.Io,
    sentinel_path: []const u8,
    expected_topology_id: []const u8,
    expected_node_id: []const u8,
    repaired_generation: []const u8,
) !void {
    const validated = (try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, expected_topology_id, expected_node_id)) orelse
        return error.LeaseRepairReceiptMissing;
    defer alloc.free(validated);
    if (!std.mem.eql(u8, validated, repaired_generation)) return error.InvalidLeaseRepairReceipt;
    var sentinel = (try loadSentinelAlloc(alloc, io, sentinel_path)) orelse return error.LeaseFenceSentinelMissing;
    defer sentinel.deinit(alloc);
    if (std.mem.eql(u8, sentinel.data_generation, repaired_generation) and std.mem.eql(u8, sentinel.reason, "repaired")) return;
    const parent = std.fs.path.dirname(sentinel_path) orelse return error.InvalidLeaseWatchdogConfig;
    const temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{sentinel_path});
    defer alloc.free(temp_path);
    const body = try std.fmt.allocPrint(
        alloc,
        "version=2\ntopology_id={s}\nnode_id={s}\ndata_generation={s}\nlease_transitions={d}\nreason=repaired\n",
        .{ sentinel.topology_id, sentinel.node_id, repaired_generation, sentinel.lease_transitions },
    );
    defer alloc.free(body);
    {
        var file = try fs_paths.createFilePortable(io, temp_path, .{ .truncate = true });
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(body);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temp_path, std.Io.Dir.cwd(), sentinel_path, io);
    try fs_paths.syncDirPortable(io, parent);
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
    const renewed =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:03Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.pending_authority, try watchdog.observe(std.testing.allocator, body, realtime, 50));
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, renewed, realtime, 100));
    try std.testing.expect(watchdog.authorityGranted());
    try std.testing.expectEqual(Decision.grace, watchdog.noteAPIFailure(9 * std.time.ns_per_s));
    try std.testing.expectEqual(Decision.fence, watchdog.noteAPIFailure(11 * std.time.ns_per_s));
    try std.testing.expect(!watchdog.authorityGranted());
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, body, realtime, 12 * std.time.ns_per_s));
}

test "kubernetes lease watchdog binds self-held authority to one process incarnation" {
    const bound_a =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7","antfly.io/ha-fence-process-boot-id":"process-a"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const renewed_a =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7","antfly.io/ha-fence-process-boot-id":"process-a"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const bound_b =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7","antfly.io/ha-fence-process-boot-id":"process-b"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:04Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:05Z");
    var process_a = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial", .process_boot_id = "process-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.pending_authority, try process_a.observe(std.testing.allocator, bound_a, realtime, 1));
    try std.testing.expectEqual(Decision.authorized, try process_a.observe(std.testing.allocator, renewed_a, realtime, 2));

    var process_b = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial", .process_boot_id = "process-b" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.pending_authority, try process_b.observe(std.testing.allocator, renewed_a, realtime, 1));
    try std.testing.expectEqual(Decision.fence, try process_a.observe(std.testing.allocator, bound_b, realtime, 3));
    try std.testing.expectEqual(FenceReason.process_changed, process_a.fence_reason.?);
    try std.testing.expectEqual(Decision.authorized, try process_b.observe(std.testing.allocator, bound_b, realtime, 2));
}

test "kubernetes lease watchdog standby waits for transfer then fences rollback" {
    const before =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const after =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"standby-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":4}}
    ;
    const now = try rfc3339UnixNs("2026-07-15T12:00:03Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
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
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);

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
    const renewed =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:03Z");
    const grace = 10 * std.time.ns_per_s;
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = grace, .sentinel_path = "/tmp/fence" }, null, null);

    try std.testing.expectEqual(Decision.pending_authority, try watchdog.observe(std.testing.allocator, body, realtime, 50));
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, renewed, realtime, 100));
    const first_deadline = watchdog.local_deadline_ns;
    try std.testing.expectEqual(Decision.grace, try watchdog.observe(std.testing.allocator, renewed, realtime, 5 * std.time.ns_per_s));
    try std.testing.expectEqual(first_deadline, watchdog.local_deadline_ns);
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, renewed, realtime, 11 * std.time.ns_per_s));
    try std.testing.expectEqual(FenceReason.api_unreachable, watchdog.fence_reason.?);
}

test "kubernetes lease watchdog clamps local authority to server lease expiry" {
    const baseline =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const renewed =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:01Z","leaseTransitions":3}}
    ;
    const observed_after_request = try rfc3339UnixNs("2026-07-15T12:00:29Z");
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.pending_authority, try watchdog.observe(std.testing.allocator, baseline, observed_after_request, 100));
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, renewed, observed_after_request, 200));
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_s + 200), watchdog.local_deadline_ns);
    try std.testing.expectEqual(Decision.grace, watchdog.noteAPIFailure(2 * std.time.ns_per_s + 199));
    try std.testing.expectEqual(Decision.fence, watchdog.noteAPIFailure(2 * std.time.ns_per_s + 200));
}

test "kubernetes lease watchdog rejects older and implausibly future renewals" {
    const baseline =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
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
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.pending_authority, try watchdog.observe(std.testing.allocator, baseline, realtime, 0));
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, initial, realtime, 1));
    try std.testing.expectEqual(Decision.fence, try watchdog.observe(std.testing.allocator, older, realtime, 2));
    try std.testing.expectEqual(FenceReason.renewal_rollback, watchdog.fence_reason.?);

    var fresh = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectError(error.LeaseRenewTimeInFuture, fresh.observe(std.testing.allocator, future, realtime, 1));
}

test "kubernetes lease watchdog strictly newer renewal extends authority deadline" {
    const initial =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:00Z","leaseTransitions":3}}
    ;
    const newer =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:02Z","leaseTransitions":3}}
    ;
    const newest =
        \\{"metadata":{"annotations":{"antfly.io/ha-fence-topology-id":"topology-7"}},"spec":{"holderIdentity":"primary-a","leaseDurationSeconds":30,"renewTime":"2026-07-15T12:00:04Z","leaseTransitions":3}}
    ;
    const realtime = try rfc3339UnixNs("2026-07-15T12:00:05Z");
    const grace = 10 * std.time.ns_per_s;
    var watchdog = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" }, .grace_ns = grace, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.pending_authority, try watchdog.observe(std.testing.allocator, initial, realtime, 1));
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, newer, realtime, 2));
    const first_deadline = watchdog.local_deadline_ns;
    try std.testing.expectEqual(Decision.authorized, try watchdog.observe(std.testing.allocator, newest, realtime, 5 * std.time.ns_per_s));
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

    var older = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.observed, try older.observe(std.testing.allocator, before, realtime, 1));
    try std.testing.expectError(error.LeaseRenewalRollback, older.observe(std.testing.allocator, transfer_older, realtime, 2));

    var equal = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "standby-a", .data_generation = "seed-a" }, .grace_ns = 10 * std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, null, null);
    try std.testing.expectEqual(Decision.observed, try equal.observe(std.testing.allocator, before, realtime, 1));
    try std.testing.expectEqual(Decision.pending_authority, try equal.observe(std.testing.allocator, transfer_equal, realtime, 2));
    try std.testing.expect(!equal.authorityGranted());
    try std.testing.expectEqual(Decision.authorized, try equal.observe(std.testing.allocator, renewed, realtime, 3));
    try std.testing.expect(equal.authorityGranted());
}

test "kubernetes lease watchdog persisted fence only rotates after exact repair generation" {
    const same = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "old-primary", .data_generation = "generation-a" }, .grace_ns = std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, "generation-a", null);
    try std.testing.expect(same.latched);

    const unvalidated = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "old-primary", .data_generation = "generation-b" }, .grace_ns = std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, "generation-a", null);
    try std.testing.expect(unvalidated.latched);

    var repaired = try Watchdog.init(.{ .scope = .{ .topology_id = "topology-7", .node_id = "old-primary", .data_generation = "generation-b" }, .grace_ns = std.time.ns_per_s, .sentinel_path = "/tmp/fence" }, "generation-a", "generation-b");
    try std.testing.expect(!repaired.latched);
    try std.testing.expect(!repaired.authorityGranted());
}

test "kubernetes lease repair receipts authorize only post-rewind exact incarnations across repeated failover" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const sentinel_path = try std.fs.path.join(alloc, &.{ root, "lease-fence" });
    defer alloc.free(sentinel_path);

    var first_fence = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "initial" },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, null, null);
    first_fence.latched = true;
    first_fence.authorized_once = true;
    first_fence.last_generation = 2;
    first_fence.fence_reason = .holder_changed;
    try first_fence.persistFence(alloc, io);

    // A caller-selected startup generation and a crash before the repair
    // receipt both remain fenced.
    try std.testing.expect((try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, "topology-7", "primary-a")) == null);
    const guessed = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = "repair-guessed" },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, "initial", null);
    try std.testing.expect(guessed.latched);

    const first_generation = try persistRepairReceipt(alloc, io, sentinel_path, "topology-7", "primary-a", 5, 7, 13, "");
    const loaded_first = (try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, "topology-7", "primary-a")) orelse return error.TestExpectedEqual;
    defer alloc.free(loaded_first);
    try std.testing.expectEqualStrings(&first_generation, loaded_first);
    const repaired = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = loaded_first },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, "initial", loaded_first);
    try std.testing.expect(!repaired.latched);
    try rotateSentinelAfterValidatedRepair(alloc, io, sentinel_path, "topology-7", "primary-a", loaded_first);
    var rotated = (try loadSentinelAlloc(alloc, io, sentinel_path)) orelse return error.TestExpectedEqual;
    defer rotated.deinit(alloc);
    try std.testing.expectEqualStrings(loaded_first, rotated.data_generation);
    try std.testing.expectEqualStrings("repaired", rotated.reason);
    const restart_generation = (try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, "topology-7", "primary-a")) orelse return error.TestExpectedEqual;
    defer alloc.free(restart_generation);
    const restarted_standby = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = restart_generation },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, loaded_first, restart_generation);
    try std.testing.expect(!restarted_standby.latched);

    // After A is promoted again and fenced by the next B -> A -> B cycle, the
    // old repair receipt is bound to the previous sentinel and cannot replay.
    var second_fence = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "primary-a", .data_generation = loaded_first },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, null, null);
    second_fence.latched = true;
    second_fence.authorized_once = true;
    second_fence.last_generation = 4;
    second_fence.fence_reason = .holder_changed;
    try second_fence.persistFence(alloc, io);
    try std.testing.expect((try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, "topology-7", "primary-a")) == null);

    const second_generation = try persistRepairReceipt(alloc, io, sentinel_path, "topology-7", "primary-a", 7, 9, 21, "");
    try std.testing.expect(!std.mem.eql(u8, &first_generation, &second_generation));
    const loaded_second = (try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, "topology-7", "primary-a")) orelse return error.TestExpectedEqual;
    defer alloc.free(loaded_second);
    try std.testing.expectEqualStrings(&second_generation, loaded_second);
}

test "kubernetes lease reseed rotation requires exact materialized evidence receipt" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const sentinel_path = try std.fs.path.join(alloc, &.{ root, "lease-fence" });
    defer alloc.free(sentinel_path);
    var fence = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "former-a", .data_generation = "old-volume" },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, null, null);
    fence.latched = true;
    fence.authorized_once = true;
    fence.last_generation = 6;
    fence.fence_reason = .holder_changed;
    try fence.persistFence(alloc, io);

    // Merely knowing the activation checkpoint cannot clear the sentinel.
    const checkpoint_only = try Watchdog.init(.{
        .scope = .{ .topology_id = "topology-7", .node_id = "former-a", .data_generation = "seed-generation" },
        .grace_ns = std.time.ns_per_s,
        .sentinel_path = sentinel_path,
    }, "old-volume", null);
    try std.testing.expect(checkpoint_only.latched);

    const materialized_digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const generation = try persistRepairReceipt(alloc, io, sentinel_path, "topology-7", "former-a", 11, 13, 42, materialized_digest);
    const validated = (try loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, "topology-7", "former-a")) orelse return error.TestExpectedEqual;
    defer alloc.free(validated);
    try std.testing.expectEqualStrings(&generation, validated);
    const wrong_evidence_generation = repairGeneration("topology-7", "former-a", "old-volume", 6, 11, 13, 42, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    try std.testing.expect(!std.mem.eql(u8, &generation, &wrong_evidence_generation));
}
