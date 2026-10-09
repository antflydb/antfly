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

//! Durable snapshot pins. Admission publishes a monotonic deadline before its
//! second retirement check. GC publishes retirement before reading deadlines.
//! Thus every admitted reader either protects the snapshot or fails closed.
const std = @import("std");
const local = @import("antfly_local_sources");
const objectstore = @import("objectstore");
const platform = @import("antfly_platform");
const catalog = local.serverless_external_source_mod.lake_catalog;
const Context = catalog.types.Context;
const A = std.mem.Allocator;
pub const duration_ns = 2 * std.time.ns_per_min;
pub const grace_ns = 30 * std.time.ns_per_s;
pub fn namespace(a: A, prefix: []const u8, binding: local.serverless_external_source_catalog_binding.Binding, uuid: []const u8) ![]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ binding.source_uri, uuid }, .{});
    defer a.free(identity);
    return std.fmt.allocPrint(a, "{s}/lake-readers/{s}", .{ prefix, catalog.types.digestHex(identity) });
}
pub const Store = struct {
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: Context = .{},
    fn key(self: Store, a: A, kind: []const u8, snapshot: []const u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/snapshots/{s}/{s}", .{ self.prefix, kind, catalog.types.digestHex(snapshot) });
    }
    fn get(self: Store, a: A, kind: []const u8, snapshot: []const u8) !?objectstore.GetResult {
        var client = self.client;
        const path = try self.key(a, kind, snapshot);
        defer a.free(path);
        return client.getObject(self.bucket, path, .{ .cancellation = catalog.types.contextCancellation(&self.context), .max_response_bytes = 1024 }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
            else => return err,
        };
    }
    pub fn deadline(self: Store, a: A, snapshot: []const u8) !u64 {
        var value = (try self.get(a, "pins", snapshot)) orelse return 0;
        defer value.deinit(self.client.allocator);
        return std.fmt.parseInt(u64, value.body, 10);
    }
    pub fn acquire(self: Store, a: A, snapshot: []const u8, now: u64) !u64 {
        if (now == 0 or snapshot.len == 0) return error.LakeSnapshotReadLeaseExpired;
        for (0..16) |_| {
            try self.context.ensureActive();
            if (try self.get(a, "retired", snapshot)) |value| {
                var proof = value;
                proof.deinit(self.client.allocator);
                return error.LakeSnapshotRetired;
            }
            var previous = try self.get(a, "pins", snapshot);
            defer if (previous) |*value| value.deinit(self.client.allocator);
            const old = if (previous) |value| try std.fmt.parseInt(u64, value.body, 10) else 0;
            const desired = try std.math.add(u64, now, duration_ns);
            const deadline_ns = @max(old, desired);
            if (deadline_ns != old) {
                const bytes = try std.fmt.allocPrint(a, "{d}", .{deadline_ns});
                defer a.free(bytes);
                const path = try self.key(a, "pins", snapshot);
                defer a.free(path);
                var client = self.client;
                var result = client.putObject(self.bucket, path, bytes, .{ .if_none_match = previous == null, .if_match_etag = if (previous) |value| value.metadata.etag orelse return error.MissingObjectEtag else null, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
                    error.PreconditionFailed, error.ObjectAlreadyExists => continue,
                    else => return err,
                };
                result.deinit(client.allocator);
            }
            if (try self.get(a, "retired", snapshot)) |value| {
                var proof = value;
                proof.deinit(self.client.allocator);
                return error.LakeSnapshotRetired;
            }
            // A future deadline from another host never extends local authority
            // beyond the bounded duration of this process's own acquisition.
            return @min(deadline_ns, desired);
        }
        return error.LakeSnapshotReadLeaseContended;
    }
    pub fn retire(self: Store, a: A, snapshot: []const u8, now: u64) !bool {
        if (now == 0) return false;
        if ((try self.deadline(a, snapshot)) +| grace_ns >= now) return false;
        const path = try self.key(a, "retired", snapshot);
        defer a.free(path);
        var client = self.client;
        var result = client.putObject(self.bucket, path, snapshot, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => null,
            else => return err,
        };
        defer if (result) |*value| value.deinit(client.allocator);
        // Pin publication races with retirement only before the reader's
        // final check. A race may retain extra data, never admit an unsafe read.
        return (try self.deadline(a, snapshot)) +| grace_ns < now;
    }
};
pub const Owner = struct {
    a: A,
    opened: local.serverless_object_store_support.OpenedObjectStore,
    prefix: []u8,
    snapshot: []u8,
    parent: Context,
    io: std.Io,
    unix_deadline: std.atomic.Value(u64) = .init(0),
    authority_deadline: std.atomic.Value(u64) = .init(0),
    failed: std.atomic.Value(bool) = .init(false),
    stopping: std.atomic.Value(bool) = .init(false),
    heartbeat: ?std.Io.Future(void) = null,
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Owner = @ptrCast(@alignCast(raw));
        if (self.stopping.load(.acquire)) return true;
        self.parent.ensureActive() catch return true;
        return false;
    }
    fn store(self: *Owner) Store {
        return .{ .client = self.opened.client, .bucket = self.opened.bucket, .prefix = self.prefix, .context = .{ .io = self.io, .deadline_ns = platform.time.monotonicNs() +| 10 * std.time.ns_per_s, .cancellation = .{ .ptr = self, .is_cancelled_fn = canceled } } };
    }
    fn renew(self: *Owner) !void {
        const now = platform.time.realtimeNs();
        const authority = platform.time.authorityNs();
        if (authority == 0) return error.LakeSnapshotReadLeaseExpired;
        const deadline_ns = try self.store().acquire(self.a, self.snapshot, now);
        self.unix_deadline.store(deadline_ns, .release);
        self.authority_deadline.store(authority +| @min(duration_ns, deadline_ns -| now), .release);
    }
    pub fn start(self: *Owner) !local.serverless_lake_host.SnapshotPin {
        try self.renew();
        self.heartbeat = try self.io.concurrent(run, .{self});
        return .{ .ptr = self, .check = check, .deinit = destroy };
    }
    fn run(self: *Owner) void {
        while (!self.stopping.load(.acquire)) {
            self.io.sleep(.fromSeconds(30), .awake) catch return;
            self.renew() catch {
                self.failed.store(true, .release);
                return;
            };
        }
    }
    fn check(raw: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.parent.ensureActive();
        const unix = platform.time.realtimeNs();
        const authority = platform.time.authorityNs();
        if (self.failed.load(.acquire) or unix == 0 or authority == 0 or unix >= self.unix_deadline.load(.acquire) or authority >= self.authority_deadline.load(.acquire)) return error.LakeSnapshotReadLeaseExpired;
    }
    pub fn destroy(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.stopping.store(true, .release);
        if (self.heartbeat) |*future| future.cancel(self.io);
        self.opened.deinit();
        self.a.free(self.prefix);
        self.a.free(self.snapshot);
        self.a.destroy(self);
    }
};
test "external lake snapshot retirement fences new admission and protects durable readers" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "lake", .prefix = "identity" };
    const now: u64 = 10 * duration_ns;
    const deadline_ns = try store.acquire(a, "100", now);
    try std.testing.expect(!try store.retire(a, "100", now));
    try std.testing.expect(!try store.retire(a, "100", deadline_ns + grace_ns));
    try std.testing.expect(try store.retire(a, "100", deadline_ns + grace_ns + 1));
    try std.testing.expectError(error.LakeSnapshotRetired, store.acquire(a, "100", deadline_ns + grace_ns + 1));
    try std.testing.expect(try store.retire(a, "101", now));
    try std.testing.expectError(error.LakeSnapshotRetired, store.acquire(a, "101", now));
}
