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

//! Synchronous object-store ABI for a suspending WASM host. The host must
//! resume the same stack, never replay the operation, and populate Reply with
//! buffers allocated through this module's allocator. Ownership transfers once.
const std = @import("std");
const A = std.mem.Allocator;
const types = @import("types.zig");
const Client = @import("client.zig").Client;

pub const Reply = extern struct { json_ptr: u32 = 0, json_len: u32 = 0, body_ptr: u32 = 0, body_len: u32 = 0 };
extern "antfly_host" fn object_call(request_ptr: [*]const u8, request_len: usize, body_ptr: [*]const u8, body_len: usize, reply: *Reply) u32;

pub const Host = struct {
    alloc: A,
    /// The ABI allocator can differ from a request's bounded working allocator.
    /// Null preserves the contract for hosts that transfer through `alloc`.
    transfer_allocator: ?A = null,
    pub fn client(self: *@This()) Client {
        return .{ .allocator = self.alloc, .ptr = self, .vtable = &vtable };
    }

    /// Request-local synchronization with fresh read admission. It carries no
    /// metadata or result; subsequent client reads use the admitted snapshot.
    pub fn readViewBarrier(self: *@This()) !void {
        var response = try self.call(.{ .op = "read_view_barrier" }, "");
        defer response.deinit();
        if (response.body.len != 0) return error.InvalidReadViewBarrier;
    }

    const Response = struct {
        alloc: A,
        parse_alloc: A,
        json: []u8,
        body: []u8,
        fn deinit(self: *@This()) void {
            self.alloc.free(self.json);
            self.alloc.free(self.body);
        }
        fn parse(self: *@This(), comptime T: type) !std.json.Parsed(T) {
            return std.json.parseFromSlice(T, self.parse_alloc, self.json, .{ .ignore_unknown_fields = true });
        }
    };
    fn call(self: *@This(), request: anytype, body: []const u8) !Response {
        const encoded = try std.json.Stringify.valueAlloc(self.alloc, request, .{});
        defer self.alloc.free(encoded);
        var reply: Reply = .{};
        const status = object_call(encoded.ptr, encoded.len, body.ptr, body.len, &reply);
        var response = Response{
            .alloc = self.transfer_allocator orelse self.alloc,
            .parse_alloc = self.alloc,
            .json = if (reply.json_len == 0) &.{} else @as([*]u8, @ptrFromInt(reply.json_ptr))[0..reply.json_len],
            .body = if (reply.body_len == 0) &.{} else @as([*]u8, @ptrFromInt(reply.body_ptr))[0..reply.body_len],
        };
        errdefer response.deinit();
        switch (status) {
            0 => return response,
            1 => return error.FileNotFound,
            2 => return error.PreconditionFailed,
            3 => return error.ObjectOperationUnsupported,
            4 => return error.ResponseTooLarge,
            5 => return error.Canceled,
            6 => return error.ObjectStoreRateLimited,
            8 => return error.ObjectIoBudgetExceeded,
            9 => return error.DeadlineExceeded,
            else => return error.ObjectStoreTransportFailed,
        }
    }
    fn owner(raw: *anyopaque) *@This() {
        return @ptrCast(@alignCast(raw));
    }
    fn deinit(_: A, _: *anyopaque) void {}
    fn bucketExists(raw: *anyopaque, bucket: []const u8, _: types.BucketOptions) !bool {
        var response = try owner(raw).call(.{ .op = "bucket_exists", .bucket = bucket }, "");
        defer response.deinit();
        const parsed = try response.parse(struct { exists: bool });
        defer parsed.deinit();
        return parsed.value.exists;
    }
    fn makeBucket(_: *anyopaque, _: []const u8, _: types.BucketOptions) !void {
        return error.ObjectOperationUnsupported;
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        var response = try owner(raw).call(.{
            .op = "put",
            .bucket = bucket,
            .key = key,
            .guard_key = if (opts.guard) |guard| guard.key else null,
            .guard_etag = if (opts.guard) |guard| guard.etag else null,
            .content_type = opts.content_type,
            .if_match_etag = opts.if_match_etag,
            .if_none_match = opts.if_none_match,
            .checksum_sha256_base64 = opts.checksum_sha256_base64,
        }, body);
        defer response.deinit();
        const parsed = try response.parse(struct { etag: ?[]const u8 = null });
        defer parsed.deinit();
        return .{ .etag = if (parsed.value.etag) |etag| try a.dupe(u8, etag) else null };
    }
    fn putMany(raw: *anyopaque, a: A, bucket: []const u8, items: []const types.PutObjectRequest) ![]types.PutResult {
        if (items.len == 0 or items.len > 8) return error.InvalidObjectBatch;
        const Item = struct { key: []const u8, offset: usize, length: usize, content_type: ?[]const u8, if_match_etag: ?[]const u8, if_none_match: bool, checksum_sha256_base64: ?[]const u8 };
        var headers: [8]Item = undefined;
        var total: usize = 0;
        for (items, headers[0..items.len]) |item, *header| {
            header.* = .{ .key = item.key, .offset = total, .length = item.body.len, .content_type = item.options.content_type, .if_match_etag = item.options.if_match_etag, .if_none_match = item.options.if_none_match, .checksum_sha256_base64 = item.options.checksum_sha256_base64 };
            total = try std.math.add(usize, total, item.body.len);
        }
        const body = try a.alloc(u8, total);
        defer a.free(body);
        for (items, headers[0..items.len]) |item, header| @memcpy(body[header.offset..][0..header.length], item.body);
        var response = try owner(raw).call(.{ .op = "put_many", .bucket = bucket, .items = headers[0..items.len] }, body);
        defer response.deinit();
        const parsed = try response.parse(struct { results: []const struct { etag: ?[]const u8 = null } });
        defer parsed.deinit();
        if (parsed.value.results.len != items.len) return error.InvalidObjectBatch;
        const results = try a.alloc(types.PutResult, items.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*result| result.deinit(a);
            a.free(results);
        }
        for (parsed.value.results, results) |item, *result| {
            result.* = .{ .etag = if (item.etag) |etag| try a.dupe(u8, etag) else null };
            initialized += 1;
        }
        return results;
    }

    fn get(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        if (opts.version_id != null or opts.part_number != null) return error.ObjectOperationUnsupported;
        var response = try owner(raw).call(.{
            .op = "get",
            .bucket = bucket,
            .key = key,
            .range = opts.range,
            .if_match_etag = opts.if_match_etag,
            .max_response_bytes = opts.max_response_bytes,
        }, "");
        defer response.deinit();
        const parsed = try response.parse(types.ObjectMetadata);
        defer parsed.deinit();
        var metadata = try cloneMetadata(a, parsed.value);
        errdefer metadata.deinit(a);
        const body = if (a.ptr == response.alloc.ptr and a.vtable == response.alloc.vtable) owned: {
            const bytes = response.body;
            response.body = &.{};
            break :owned bytes;
        } else try a.dupe(u8, response.body);
        return .{ .body = body, .metadata = metadata, .conditional_etag_verified = opts.if_match_etag != null };
    }
    fn stat(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        var response = try owner(raw).call(.{ .op = "head", .bucket = bucket, .key = key }, "");
        defer response.deinit();
        const parsed = try response.parse(types.ObjectMetadata);
        defer parsed.deinit();
        return cloneMetadata(a, parsed.value);
    }
    fn attributes(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        var metadata = try stat(raw, a, bucket, key);
        defer metadata.deinit(a);
        const etag = if (metadata.etag) |value| try a.dupe(u8, value) else null;
        errdefer if (etag) |value| a.free(value);
        const content_type = if (metadata.content_type) |value| try a.dupe(u8, value) else null;
        errdefer if (content_type) |value| a.free(value);
        return .{ .etag = etag, .content_type = content_type, .content_length = metadata.content_length, .parts = try a.alloc(types.ObjectPart, 0) };
    }
    fn delete(raw: *anyopaque, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        if (opts.version_id != null) return error.ObjectOperationUnsupported;
        var response = try owner(raw).call(.{ .op = "delete", .bucket = bucket, .key = key, .if_match_etag = opts.if_match_etag }, "");
        defer response.deinit();
    }
    fn list(raw: *anyopaque, a: A, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        var response = try owner(raw).call(.{
            .op = "list",
            .bucket = bucket,
            .prefix = opts.prefix,
            .recursive = opts.recursive,
            .delimiter = opts.delimiter,
            .start_after = opts.start_after,
            .continuation_token = opts.continuation_token,
            .max_keys = opts.max_keys,
        }, "");
        defer response.deinit();
        // Allocate all variable-sized fields independently: the normal Client
        // result owns its contents after the parsed response is destroyed.
        const parsed = try response.parse(types.ListResult);
        defer parsed.deinit();
        var out: types.ListResult = .{ .entries = try a.alloc(types.ListEntry, 0) };
        errdefer out.deinit(a);
        for (parsed.value.entries) |entry| {
            const key = try a.dupe(u8, entry.key);
            errdefer a.free(key);
            const etag = if (entry.etag) |value| try a.dupe(u8, value) else null;
            errdefer if (etag) |value| a.free(value);
            const next = try a.realloc(out.entries, out.entries.len + 1);
            out.entries = next;
            out.entries[out.entries.len - 1] = .{ .key = key, .etag = etag, .size = entry.size, .last_modified_unix_ms = entry.last_modified_unix_ms };
        }
        for (parsed.value.common_prefixes) |prefix| {
            const value = try a.dupe(u8, prefix);
            errdefer a.free(value);
            out.common_prefixes = try a.realloc(out.common_prefixes, out.common_prefixes.len + 1);
            out.common_prefixes[out.common_prefixes.len - 1] = value;
        }
        if (parsed.value.next_continuation_token) |value| out.next_continuation_token = try a.dupe(u8, value);
        return out;
    }
    fn statWithOptions(raw: *anyopaque, a: A, bucket: []const u8, key: []const u8, opts: types.StatOptions) !types.ObjectMetadata {
        if (opts.version_id != null) return error.ObjectOperationUnsupported;
        return stat(raw, a, bucket, key);
    }
    fn cloneMetadata(a: A, value: types.ObjectMetadata) !types.ObjectMetadata {
        const bucket = try a.dupe(u8, value.bucket);
        errdefer a.free(bucket);
        const key = try a.dupe(u8, value.key);
        errdefer a.free(key);
        const etag = if (value.etag) |v| try a.dupe(u8, v) else null;
        errdefer if (etag) |v| a.free(v);
        const version = if (value.version_id) |v| try a.dupe(u8, v) else null;
        errdefer if (version) |v| a.free(v);
        var checksum = if (value.checksum) |v| try v.clone(a) else null;
        errdefer if (checksum) |*v| v.deinit(a);
        const content_type = if (value.content_type) |v| try a.dupe(u8, v) else null;
        return .{ .bucket = bucket, .key = key, .etag = etag, .version_id = version, .revision_kind = value.revision_kind, .checksum = checksum, .checksum_scope = value.checksum_scope, .content_type = content_type, .content_length = value.content_length, .last_modified_unix_ms = value.last_modified_unix_ms };
    }
    const vtable: Client.VTable = .{
        .supports_guarded_put = true,
        .deinit = deinit,
        .bucket_exists = bucketExists,
        .make_bucket = makeBucket,
        .put_object = put,
        .put_objects = putMany,
        .get_object = get,
        .stat_object = stat,
        .stat_object_with_options = statWithOptions,
        .get_object_attributes = attributes,
        .delete_object = delete,
        .list_objects = list,
    };
};

test "WASM metadata retains generation semantics instead of turning a version into an ETag" {
    const a = std.testing.allocator;
    var bucket = "db".*;
    var key = "HEAD".*;
    var etag = "http-etag".*;
    var generation = "123".*;
    var copied = try Host.cloneMetadata(a, .{
        .bucket = &bucket,
        .key = &key,
        .etag = &etag,
        .version_id = &generation,
        .revision_kind = .generation,
        .content_length = 0,
    });
    defer copied.deinit(a);
    generation[0] = '9';
    try std.testing.expectEqualStrings("123", copied.writeRevision().?.generation);
    try std.testing.expectEqualStrings("http-etag", copied.etag.?);
}
