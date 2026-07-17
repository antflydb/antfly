// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Fail-closed deletion of one instance's complete portable HA seed namespace.
//!
//! The operation is deliberately bound to a canonical controller request. The
//! runtime accepts only the exact `s3://<bucket>/instances/<id>/ha-seeds/`
//! boundary, relists until that boundary is empty, and never broadens or trims
//! the supplied object prefix. A concurrent writer therefore makes cleanup
//! retry or fail rather than leaving a false-success receipt.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const object_storage = @import("../object_storage.zig");
const seed_namespace_control = @import("seed_namespace_control.zig");
const validation = @import("validation.zig");

pub const request_version: u16 = 1;
pub const request_kind = "DeleteHASeedPrefix";

pub const Store = struct {
    client: *object_storage.ObjectStorage,
    bucket: []const u8,
    /// Must be the exact, trailing-slash object prefix from `Request.location`.
    prefix: []const u8,
};

/// Field order is the canonical wire order shared with the Colony controller.
/// `request_sha256` is present but blank while its digest is calculated.
pub const Request = struct {
    version: u16,
    kind: []const u8,
    operation_id: []const u8,
    retry_token: []const u8,
    instance_id: []const u8,
    topology_id: []const u8,
    topology_generation: u64,
    location: []const u8,
    prefix_sha256: []const u8,
    credentials_secret_name: []const u8,
    delete_all: bool,
    request_sha256: []const u8,
};

/// Field order is the canonical wire order shared with the Colony controller.
/// Secret selection and the request's `delete_all` assertion are intentionally
/// not repeated in the durable public receipt.
pub const Receipt = struct {
    version: u16,
    kind: []const u8,
    operation_id: []const u8,
    retry_token: []const u8,
    instance_id: []const u8,
    topology_id: []const u8,
    topology_generation: u64,
    location: []const u8,
    prefix_sha256: []const u8,
    request_sha256: []const u8,
    deleted_generations: usize,
    deleted_objects: usize,
    retained_objects: usize,
    prefix_empty: bool,
    complete: bool,
    completed_at: []const u8,
    receipt_sha256: []const u8,
};

pub const Options = struct {
    max_keys: u32 = 1000,
    max_quiescence_rounds: usize = 8,
    /// Deterministic clock seam for unit tests. Production callers leave null.
    completed_at_override: ?[]const u8 = null,
};

pub const Result = struct {
    receipt_json: []u8,
    deleted_generations: usize,
    deleted_objects: usize,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub fn sha256HexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    return try hexAlloc(alloc, &digest);
}

/// SHA-256 over the compact canonical request with `request_sha256` blank.
pub fn requestSha256Alloc(alloc: Allocator, request: Request) ![]u8 {
    var canonical = request;
    canonical.request_sha256 = "";
    const body = try std.json.Stringify.valueAlloc(alloc, canonical, .{});
    defer alloc.free(body);
    return try sha256HexAlloc(alloc, body);
}

/// SHA-256 over the compact canonical receipt with `receipt_sha256` blank.
pub fn receiptSha256Alloc(alloc: Allocator, receipt: Receipt) ![]u8 {
    var canonical = receipt;
    canonical.receipt_sha256 = "";
    const body = try std.json.Stringify.valueAlloc(alloc, canonical, .{});
    defer alloc.free(body);
    return try sha256HexAlloc(alloc, body);
}

pub fn deleteAll(alloc: Allocator, store: Store, request: Request, options: Options) !Result {
    try validateRequestAuthority(alloc, request);
    try validateStoreBinding(alloc, store, request);
    if (options.max_keys == 0 or options.max_quiescence_rounds == 0)
        return error.InvalidSeedPrefixCleanupLimits;

    var acquisition = try seed_namespace_control.acquireDelete(alloc, .{
        .client = store.client,
        .bucket = store.bucket,
        .prefix = store.prefix,
    }, .{
        .topology_id = request.topology_id,
        .topology_generation = request.topology_generation,
    }, request.request_sha256);
    defer acquisition.deinit(alloc);
    switch (acquisition) {
        .complete => |receipt_json| {
            var parsed = std.json.parseFromSlice(Receipt, alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
                return error.InvalidSeedPrefixCleanupReceipt;
            defer parsed.deinit();
            try validateReceipt(alloc, parsed.value, request);
            return .{
                .receipt_json = try alloc.dupe(u8, receipt_json),
                .deleted_generations = parsed.value.deleted_generations,
                .deleted_objects = parsed.value.deleted_objects,
            };
        },
        .lease => {},
    }

    var deleted_keys = std.StringHashMapUnmanaged(void).empty;
    defer deinitOwnedSet(alloc, &deleted_keys);
    var deleted_generations = std.StringHashMapUnmanaged(void).empty;
    defer deinitOwnedSet(alloc, &deleted_generations);

    var empty = false;
    var round: usize = 0;
    while (round < options.max_quiescence_rounds) : (round += 1) {
        const keys = try listAllKeysAlloc(alloc, store, options.max_keys);
        defer freeKeys(alloc, keys);
        if (keys.len == 0) {
            empty = true;
            break;
        }
        for (keys) |key| {
            // A backend violating the list prefix contract must never be given
            // a delete call outside the authorized boundary.
            if (!std.mem.startsWith(u8, key, store.prefix))
                return error.SeedPrefixCleanupListEscapedPrefix;
            if (!deleted_keys.contains(key)) try rememberOwned(alloc, &deleted_keys, key);
            if (generationFromKey(store.prefix, key)) |generation| {
                if (!deleted_generations.contains(generation))
                    try rememberOwned(alloc, &deleted_generations, generation);
            }
            try deleteObjectIfPresent(store, key);
        }
    }
    if (!empty) {
        var remaining = try listFirstPage(alloc, store, options.max_keys);
        defer remaining.deinit(alloc);
        if (remaining.entries.len != 0) return error.SeedPrefixCleanupNotQuiescent;
        empty = true;
    }

    const completed_at = if (options.completed_at_override) |value| blk: {
        if (!isRfc3339Nano(value)) return error.InvalidSeedPrefixCleanupCompletedAt;
        break :blk try alloc.dupe(u8, value);
    } else try nowRfc3339NanoAlloc(alloc);
    defer alloc.free(completed_at);

    var receipt = Receipt{
        .version = request.version,
        .kind = request.kind,
        .operation_id = request.operation_id,
        .retry_token = request.retry_token,
        .instance_id = request.instance_id,
        .topology_id = request.topology_id,
        .topology_generation = request.topology_generation,
        .location = request.location,
        .prefix_sha256 = request.prefix_sha256,
        .request_sha256 = request.request_sha256,
        .deleted_generations = deleted_generations.count(),
        .deleted_objects = deleted_keys.count(),
        .retained_objects = 0,
        .prefix_empty = empty,
        .complete = empty,
        .completed_at = completed_at,
        .receipt_sha256 = "",
    };
    const receipt_sha256 = try receiptSha256Alloc(alloc, receipt);
    defer alloc.free(receipt_sha256);
    receipt.receipt_sha256 = receipt_sha256;
    const receipt_json = try std.json.Stringify.valueAlloc(alloc, receipt, .{});
    errdefer alloc.free(receipt_json);
    try seed_namespace_control.finishDelete(alloc, .{
        .client = store.client,
        .bucket = store.bucket,
        .prefix = store.prefix,
    }, .{
        .topology_id = request.topology_id,
        .topology_generation = request.topology_generation,
    }, request.request_sha256, acquisition.lease, receipt_json);
    return .{
        .receipt_json = receipt_json,
        .deleted_generations = receipt.deleted_generations,
        .deleted_objects = receipt.deleted_objects,
    };
}

pub fn validateReceipt(alloc: Allocator, receipt: Receipt, request: Request) !void {
    if (receipt.version != request.version or !std.mem.eql(u8, receipt.kind, request.kind) or
        !std.mem.eql(u8, receipt.operation_id, request.operation_id) or
        !std.mem.eql(u8, receipt.retry_token, request.retry_token) or
        !std.mem.eql(u8, receipt.instance_id, request.instance_id) or
        !std.mem.eql(u8, receipt.topology_id, request.topology_id) or
        receipt.topology_generation != request.topology_generation or
        !std.mem.eql(u8, receipt.location, request.location) or
        !std.mem.eql(u8, receipt.prefix_sha256, request.prefix_sha256) or
        !std.mem.eql(u8, receipt.request_sha256, request.request_sha256))
    {
        return error.SeedPrefixCleanupReceiptAuthorityMismatch;
    }
    if (!receipt.complete or !receipt.prefix_empty or receipt.retained_objects != 0)
        return error.SeedPrefixCleanupReceiptIncomplete;
    if (!isRfc3339Nano(receipt.completed_at)) return error.InvalidSeedPrefixCleanupCompletedAt;
    if (!isLowerSha256(receipt.receipt_sha256)) return error.InvalidSeedPrefixCleanupReceiptDigest;
    const expected = try receiptSha256Alloc(alloc, receipt);
    defer alloc.free(expected);
    if (!std.mem.eql(u8, expected, receipt.receipt_sha256))
        return error.SeedPrefixCleanupReceiptDigestMismatch;
}

/// Validates the immutable controller authority before any remote object-store
/// handle is opened. This keeps malformed bucket/prefix input side-effect free.
pub fn validateRequestAuthority(alloc: Allocator, request: Request) !void {
    if (request.version != request_version or !std.mem.eql(u8, request.kind, request_kind))
        return error.InvalidSeedPrefixCleanupSchema;
    if (!validation.isIdentifier(request.operation_id) or !validation.isIdentifier(request.retry_token) or
        !validation.isIdentifier(request.instance_id) or !validation.isIdentifier(request.topology_id) or
        request.topology_generation == 0)
    {
        return error.InvalidSeedPrefixCleanupAuthority;
    }
    if (!isDNS1123Subdomain(request.credentials_secret_name))
        return error.InvalidSeedPrefixCleanupCredentialsSecret;
    if (!request.delete_all) return error.SeedPrefixCleanupDeleteAllRequired;

    const scheme = "s3://";
    if (!std.mem.startsWith(u8, request.location, scheme))
        return error.InvalidSeedPrefixCleanupPrefix;
    const rest = request.location[scheme.len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse
        return error.InvalidSeedPrefixCleanupPrefix;
    const bucket = rest[0..slash];
    if (!isS3Bucket(bucket)) return error.InvalidSeedPrefixCleanupPrefix;
    const expected_prefix = try std.fmt.allocPrint(alloc, "instances/{s}/ha-seeds/", .{request.instance_id});
    defer alloc.free(expected_prefix);
    const expected_location = try std.fmt.allocPrint(alloc, "s3://{s}/{s}", .{ bucket, expected_prefix });
    defer alloc.free(expected_location);
    if (!std.mem.eql(u8, request.location, expected_location)) {
        return error.InvalidSeedPrefixCleanupPrefix;
    }

    if (!isLowerSha256(request.prefix_sha256) or !isLowerSha256(request.request_sha256))
        return error.InvalidSeedPrefixCleanupDigest;
    const prefix_sha256 = try sha256HexAlloc(alloc, request.location);
    defer alloc.free(prefix_sha256);
    if (!std.mem.eql(u8, prefix_sha256, request.prefix_sha256))
        return error.SeedPrefixCleanupPrefixDigestMismatch;
    const request_sha256 = try requestSha256Alloc(alloc, request);
    defer alloc.free(request_sha256);
    if (!std.mem.eql(u8, request_sha256, request.request_sha256))
        return error.SeedPrefixCleanupRequestDigestMismatch;
}

fn validateStoreBinding(alloc: Allocator, store: Store, request: Request) !void {
    const rest = request.location["s3://".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse unreachable;
    const bucket = rest[0..slash];
    const expected_prefix = try std.fmt.allocPrint(alloc, "instances/{s}/ha-seeds/", .{request.instance_id});
    defer alloc.free(expected_prefix);
    if (!std.mem.eql(u8, store.bucket, bucket) or !std.mem.eql(u8, store.prefix, expected_prefix))
        return error.InvalidSeedPrefixCleanupPrefix;
}

fn listAllKeysAlloc(alloc: Allocator, store: Store, max_keys: u32) ![][]u8 {
    var keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeKeyList(alloc, &keys);
    var continuation: ?[]u8 = null;
    defer if (continuation) |value| alloc.free(value);
    while (true) {
        var result = try store.client.listObjects(store.bucket, .{
            .prefix = store.prefix,
            .recursive = true,
            .continuation_token = continuation,
            .max_keys = max_keys,
        });
        defer result.deinit(alloc);
        for (result.entries) |entry| try keys.append(alloc, try alloc.dupe(u8, entry.key));
        const next = if (result.next_continuation_token) |value| try alloc.dupe(u8, value) else null;
        if (continuation) |value| alloc.free(value);
        continuation = next;
        if (continuation == null) break;
    }
    return try keys.toOwnedSlice(alloc);
}

fn listFirstPage(alloc: Allocator, store: Store, max_keys: u32) !object_storage.ListResult {
    var client = store.client.*;
    client.allocator = alloc;
    return try client.listObjects(store.bucket, .{
        .prefix = store.prefix,
        .recursive = true,
        .max_keys = max_keys,
    });
}

fn deleteObjectIfPresent(store: Store, key: []const u8) !void {
    store.client.deleteObject(store.bucket, key, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NoSuchKey, error.ObjectNotFound => return,
        else => return err,
    };
}

fn generationFromKey(prefix: []const u8, key: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    const generation_root = "generations/";
    const relative = key[prefix.len..];
    if (!std.mem.startsWith(u8, relative, generation_root)) return null;
    const rest = relative[generation_root.len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    const generation = rest[0..slash];
    if (!validation.isIdentifier(generation)) return null;
    return generation;
}

fn rememberOwned(alloc: Allocator, set: *std.StringHashMapUnmanaged(void), value: []const u8) !void {
    const result = try set.getOrPut(alloc, value);
    if (result.found_existing) return;
    errdefer _ = set.remove(value);
    result.key_ptr.* = try alloc.dupe(u8, value);
}

fn deinitOwnedSet(alloc: Allocator, set: *std.StringHashMapUnmanaged(void)) void {
    var it = set.keyIterator();
    while (it.next()) |key| alloc.free(key.*);
    set.deinit(alloc);
}

fn freeKeys(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn freeKeyList(alloc: Allocator, keys: *std.ArrayListUnmanaged([]u8)) void {
    for (keys.items) |key| alloc.free(key);
    keys.deinit(alloc);
}

fn isLowerSha256(value: []const u8) bool {
    if (value.len != Sha256.digest_length * 2) return false;
    for (value) |byte| {
        if ((byte < '0' or byte > '9') and (byte < 'a' or byte > 'f')) return false;
    }
    return true;
}

fn isS3Bucket(value: []const u8) bool {
    if (value.len < 3 or value.len > 255) return false;
    if (!isLowerAlphaNumeric(value[0]) or !isLowerAlphaNumeric(value[value.len - 1])) return false;
    var previous_dot = false;
    for (value) |byte| {
        if (!isLowerAlphaNumeric(byte) and byte != '-' and byte != '.') return false;
        if (byte == '.' and previous_dot) return false;
        previous_dot = byte == '.';
    }
    return true;
}

fn isDNS1123Subdomain(value: []const u8) bool {
    if (value.len == 0 or value.len > 253) return false;
    var labels = std.mem.splitScalar(u8, value, '.');
    while (labels.next()) |label| {
        if (label.len == 0 or label.len > 63) return false;
        if (!isLowerAlphaNumeric(label[0]) or !isLowerAlphaNumeric(label[label.len - 1])) return false;
        for (label) |byte| if (!isLowerAlphaNumeric(byte) and byte != '-') return false;
    }
    return true;
}

fn isLowerAlphaNumeric(byte: u8) bool {
    return (byte >= 'a' and byte <= 'z') or (byte >= '0' and byte <= '9');
}

fn isRfc3339Nano(value: []const u8) bool {
    if (value.len < 20 or value.len > 30 or value[value.len - 1] != 'Z') return false;
    if (value[4] != '-' or value[7] != '-' or value[10] != 'T' or
        value[13] != ':' or value[16] != ':') return false;
    for (value[0..19], 0..) |byte, index| switch (index) {
        4, 7, 10, 13, 16 => {},
        else => if (byte < '0' or byte > '9') return false,
    };
    if (value.len > 20) {
        if (value[19] != '.') return false;
        for (value[20 .. value.len - 1]) |byte| if (byte < '0' or byte > '9') return false;
    }
    return true;
}

fn nowRfc3339NanoAlloc(alloc: Allocator) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = std.Io.Clock.real.now(io_impl.io()).nanoseconds;
    if (raw < 0 or raw > std.math.maxInt(u64)) return error.InvalidSeedPrefixCleanupClock;
    const ns: u64 = @intCast(raw);
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = ns / std.time.ns_per_s };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
        ns % std.time.ns_per_s,
    });
}

fn hexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, index| {
        out[index * 2] = alphabet[byte >> 4];
        out[index * 2 + 1] = alphabet[byte & 0x0f];
    }
    return out;
}
