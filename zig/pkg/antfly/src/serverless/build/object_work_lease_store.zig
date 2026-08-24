// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable serverless work leases backed by conditional object-store writes.
//! The record is retained after release so fencing tokens never move backwards.

const std = @import("std");
const objectstore = @import("objectstore");
const work_lease = @import("work_lease.zig");
const head_coordination = @import("../head_coordination.zig");

const Allocator = std.mem.Allocator;

const CurrentLease = struct {
    head_version: u64,
    owner_id: ?[]u8,
    fencing_token: u64,
    expires_at_unix_ns: u64,
    released: bool,
    etag: []u8,

    fn deinit(self: *CurrentLease, alloc: Allocator) void {
        if (self.owner_id) |owner_id| alloc.free(owner_id);
        alloc.free(self.etag);
        self.* = undefined;
    }
};

pub const ObjectWorkLeaseStore = struct {
    alloc: Allocator,
    client: objectstore.Client,
    bucket: []u8,
    prefix: []u8,

    pub fn initWithClient(
        alloc: Allocator,
        client: objectstore.Client,
        bucket: []const u8,
        prefix: []const u8,
    ) !ObjectWorkLeaseStore {
        var borrowed_client = client;
        if (!(try borrowed_client.bucketExists(bucket))) {
            try borrowed_client.makeBucket(bucket);
        }
        return .{
            .alloc = alloc,
            .client = borrowed_client,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn deinit(self: *ObjectWorkLeaseStore) void {
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    pub fn provider(self: *ObjectWorkLeaseStore) work_lease.Provider {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn acquire(
        self: *ObjectWorkLeaseStore,
        namespace: []const u8,
        owner_id: []const u8,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !?work_lease.Acquisition {
        const expires_at = std.math.add(u64, now_unix_ns, ttl_ns) catch {
            return error.LeaseExpiryOverflow;
        };
        const key = try keyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);

        var current = try self.readCurrentAlloc(key);
        defer if (current) |*value| value.deinit(self.alloc);

        var took_over = false;
        var fencing_token: u64 = 1;
        if (current) |value| {
            if (!value.released and value.expires_at_unix_ns > now_unix_ns and
                (value.owner_id == null or !std.mem.eql(u8, value.owner_id.?, owner_id)))
            {
                return null;
            }
            if (value.released or value.expires_at_unix_ns <= now_unix_ns) {
                fencing_token = if (value.fencing_token == 0)
                    1
                else
                    std.math.add(u64, value.fencing_token, 1) catch {
                        return error.LeaseFencingTokenExhausted;
                    };
                took_over = !value.released;
            } else {
                fencing_token = value.fencing_token;
            }
        }

        const proposed = head_coordination.Record{
            .head_version = if (current) |value| value.head_version else 0,
            .owner_id = owner_id,
            .fencing_token = fencing_token,
            .expires_at_unix_ns = expires_at,
            .released = false,
        };
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{proposed.head_version});
        defer self.alloc.free(payload);
        const content_type = try head_coordination.contentTypeAlloc(self.alloc, proposed);
        defer self.alloc.free(content_type);

        var result = self.client.putObject(self.bucket, key, payload, .{
            .content_type = content_type,
            .if_none_match = current == null,
            .if_match_etag = if (current) |value| value.etag else null,
        }) catch |err| switch (err) {
            error.PreconditionFailed => return null,
            else => {
                if (try self.recordMatches(key, proposed)) {
                    return .{
                        .fencing_token = fencing_token,
                        .expires_at_unix_ns = expires_at,
                        .took_over = took_over,
                    };
                }
                return err;
            },
        };
        defer result.deinit(self.alloc);
        return .{
            .fencing_token = fencing_token,
            .expires_at_unix_ns = expires_at,
            .took_over = took_over,
        };
    }

    pub fn validate(
        self: *ObjectWorkLeaseStore,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
    ) !void {
        const key = try keyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);
        var current = (try self.readCurrentAlloc(key)) orelse return error.WorkLeaseLost;
        defer current.deinit(self.alloc);
        if (current.owner_id == null or
            !std.mem.eql(u8, current.owner_id.?, owner_id) or
            current.fencing_token != fencing_token or
            current.released or
            current.expires_at_unix_ns <= now_unix_ns)
        {
            return error.WorkLeaseLost;
        }
    }

    pub fn renew(
        self: *ObjectWorkLeaseStore,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !u64 {
        const expires_at = std.math.add(u64, now_unix_ns, ttl_ns) catch {
            return error.LeaseExpiryOverflow;
        };
        const key = try keyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);
        var current = (try self.readCurrentAlloc(key)) orelse return error.WorkLeaseLost;
        defer current.deinit(self.alloc);
        if (current.owner_id == null or
            !std.mem.eql(u8, current.owner_id.?, owner_id) or
            current.fencing_token != fencing_token or
            current.released)
        {
            return error.WorkLeaseLost;
        }

        const proposed = head_coordination.Record{
            .head_version = current.head_version,
            .owner_id = owner_id,
            .fencing_token = fencing_token,
            .expires_at_unix_ns = expires_at,
            .released = false,
        };
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{proposed.head_version});
        defer self.alloc.free(payload);
        const content_type = try head_coordination.contentTypeAlloc(self.alloc, proposed);
        defer self.alloc.free(content_type);
        var result = self.client.putObject(self.bucket, key, payload, .{
            .content_type = content_type,
            .if_match_etag = current.etag,
        }) catch |err| switch (err) {
            error.PreconditionFailed => return error.WorkLeaseLost,
            else => {
                if (try self.recordMatches(key, proposed)) return expires_at;
                return err;
            },
        };
        defer result.deinit(self.alloc);
        return expires_at;
    }

    pub fn release(
        self: *ObjectWorkLeaseStore,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
    ) !bool {
        const key = try keyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);
        var current = (try self.readCurrentAlloc(key)) orelse return false;
        defer current.deinit(self.alloc);
        if (current.owner_id == null or
            !std.mem.eql(u8, current.owner_id.?, owner_id) or
            current.fencing_token != fencing_token)
        {
            return false;
        }

        const released_record = head_coordination.Record{
            .head_version = current.head_version,
            .owner_id = owner_id,
            .fencing_token = fencing_token,
            .expires_at_unix_ns = 0,
            .released = true,
        };
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{released_record.head_version});
        defer self.alloc.free(payload);
        const content_type = try head_coordination.contentTypeAlloc(self.alloc, released_record);
        defer self.alloc.free(content_type);
        var result = self.client.putObject(self.bucket, key, payload, .{
            .content_type = content_type,
            .if_match_etag = current.etag,
        }) catch |err| switch (err) {
            error.PreconditionFailed => return false,
            else => {
                if (try self.recordMatches(key, released_record)) return true;
                return err;
            },
        };
        defer result.deinit(self.alloc);
        return true;
    }

    fn readCurrentAlloc(self: *ObjectWorkLeaseStore, key: []const u8) !?CurrentLease {
        var result = self.client.getObject(self.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(self.alloc);
        const etag = result.metadata.etag orelse return error.MissingLeaseObjectEtag;
        const trimmed = std.mem.trim(u8, result.body, " \t\r\n");
        var record: head_coordination.Record = undefined;
        var owner_id: ?[]u8 = null;
        if (trimmed.len != 0 and trimmed[0] == '{') {
            // Read the transitional JSON-body format so it can be migrated by
            // the next conditional write.
            var parsed = try std.json.parseFromSlice(head_coordination.Record, self.alloc, trimmed, .{
                .ignore_unknown_fields = false,
            });
            defer parsed.deinit();
            if (!head_coordination.valid(parsed.value))
                return error.InvalidWorkLeaseRecord;
            if (parsed.value.owner_id) |borrowed_owner_id| {
                owner_id = try self.alloc.dupe(u8, borrowed_owner_id);
            }
            record = parsed.value;
        } else {
            const body_head = try std.fmt.parseInt(u64, trimmed, 10);
            if (try head_coordination.parseContentTypeAlloc(self.alloc, result.metadata.content_type)) |parsed_value| {
                var parsed = parsed_value;
                defer parsed.deinit();
                if (!head_coordination.valid(parsed.value) or parsed.value.head_version != body_head)
                    return error.InvalidWorkLeaseRecord;
                if (parsed.value.owner_id) |borrowed_owner_id| {
                    owner_id = try self.alloc.dupe(u8, borrowed_owner_id);
                }
                record = parsed.value;
            } else {
                record = head_coordination.Record.fromLegacyHead(body_head);
            }
        }
        errdefer if (owner_id) |owned| self.alloc.free(owned);
        const owned_etag = try self.alloc.dupe(u8, etag);
        return .{
            .head_version = record.head_version,
            .owner_id = owner_id,
            .fencing_token = record.fencing_token,
            .expires_at_unix_ns = record.expires_at_unix_ns,
            .released = record.released,
            .etag = owned_etag,
        };
    }

    fn recordMatches(self: *ObjectWorkLeaseStore, key: []const u8, expected: head_coordination.Record) !bool {
        var current = (try self.readCurrentAlloc(key)) orelse return false;
        defer current.deinit(self.alloc);
        return current.owner_id != null and
            expected.owner_id != null and
            std.mem.eql(u8, current.owner_id.?, expected.owner_id.?) and
            current.fencing_token == expected.fencing_token and
            current.expires_at_unix_ns == expected.expires_at_unix_ns and
            current.released == expected.released;
    }

    const vtable: work_lease.Provider.VTable = .{
        .acquire = erasedAcquire,
        .validate = erasedValidate,
        .renew = erasedRenew,
        .release = erasedRelease,
    };

    fn erasedAcquire(
        ptr: *anyopaque,
        namespace: []const u8,
        owner_id: []const u8,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !?work_lease.Acquisition {
        const self: *ObjectWorkLeaseStore = @ptrCast(@alignCast(ptr));
        return try self.acquire(namespace, owner_id, now_unix_ns, ttl_ns);
    }

    fn erasedValidate(
        ptr: *anyopaque,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
    ) !void {
        const self: *ObjectWorkLeaseStore = @ptrCast(@alignCast(ptr));
        try self.validate(namespace, owner_id, fencing_token, now_unix_ns);
    }

    fn erasedRelease(
        ptr: *anyopaque,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
    ) !bool {
        const self: *ObjectWorkLeaseStore = @ptrCast(@alignCast(ptr));
        return try self.release(namespace, owner_id, fencing_token);
    }

    fn erasedRenew(
        ptr: *anyopaque,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !u64 {
        const self: *ObjectWorkLeaseStore = @ptrCast(@alignCast(ptr));
        return try self.renew(
            namespace,
            owner_id,
            fencing_token,
            now_unix_ns,
            ttl_ns,
        );
    }
};

fn keyAlloc(alloc: Allocator, prefix: []const u8, namespace: []const u8) ![]u8 {
    if (prefix.len == 0) {
        return try std.fmt.allocPrint(alloc, "{s}/HEAD", .{namespace});
    }
    return try std.fmt.allocPrint(alloc, "{s}/{s}/HEAD", .{ prefix, namespace });
}

test "serverless object work lease fences stale owners and reconciles ambiguous acquisition" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var store = try ObjectWorkLeaseStore.initWithClient(
        alloc,
        faults.client(),
        "leases",
        "tenant",
    );
    defer store.deinit();
    const provider = store.provider();

    const first = (try provider.acquire("docs", "worker-a", 100, 10)).?;
    try std.testing.expectEqual(@as(u64, 1), first.fencing_token);
    try std.testing.expect(!(first.took_over));
    var legacy_client = memory.client();
    var legacy_head = try legacy_client.getObject("leases", "tenant/docs/HEAD", .{});
    defer legacy_head.deinit(alloc);
    try std.testing.expectEqualStrings("0", legacy_head.body);
    try std.testing.expectEqual(@as(u64, 0), try std.fmt.parseInt(u64, legacy_head.body, 10));
    try std.testing.expect((try provider.acquire("docs", "worker-b", 109, 10)) == null);
    try std.testing.expectError(
        error.WorkLeaseLost,
        provider.validate("docs", "worker-a", first.fencing_token, 110),
    );

    faults.commitNextPutThenFail(error.Timeout);
    const second = (try provider.acquire("docs", "worker-b", 110, 10)).?;
    try std.testing.expectEqual(@as(u64, 2), second.fencing_token);
    try std.testing.expect(second.took_over);
    try std.testing.expectError(
        error.WorkLeaseLost,
        provider.validate("docs", "worker-a", first.fencing_token, 111),
    );
    try provider.validate("docs", "worker-b", second.fencing_token, 111);

    try std.testing.expect(try provider.release("docs", "worker-b", second.fencing_token));
    const third = (try provider.acquire("docs", "worker-c", 112, 10)).?;
    try std.testing.expectEqual(@as(u64, 3), third.fencing_token);
    try std.testing.expect(!third.took_over);
}

test "serverless head publication atomically rejects a stale fencing token" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();

    var progress_impl = try @import("../catalog/object_progress_store.zig").ObjectProgressStore.initWithClient(
        alloc,
        memory.client(),
        "coordination",
        "tenant",
    );
    var progress = progress_impl.progressStore();
    defer progress.deinit();
    var leases = try ObjectWorkLeaseStore.initWithClient(
        alloc,
        memory.client(),
        "coordination",
        "tenant",
    );
    defer leases.deinit();
    const provider = leases.provider();

    const stale = (try provider.acquire("docs", "worker-a", 100, 10)).?;
    const replacement = (try provider.acquire("docs", "worker-b", 110, 100)).?;
    try std.testing.expect(replacement.fencing_token > stale.fencing_token);
    try std.testing.expectError(
        error.WorkLeaseLost,
        progress.compareAndSwapHeadFenced("docs", null, 1, .{
            .owner_id = "worker-a",
            .fencing_token = stale.fencing_token,
        }),
    );
    try std.testing.expect(try progress.compareAndSwapHeadFenced("docs", null, 1, .{
        .owner_id = "worker-b",
        .fencing_token = replacement.fencing_token,
    }));
    try std.testing.expectEqual(@as(u64, 1), try progress.getHead("docs"));
    var legacy_client = memory.client();
    var legacy_head = try legacy_client.getObject("coordination", "tenant/docs/HEAD", .{});
    defer legacy_head.deinit(alloc);
    try std.testing.expectEqualStrings("1", legacy_head.body);
    try std.testing.expectEqual(@as(u64, 1), try std.fmt.parseInt(u64, legacy_head.body, 10));
}
