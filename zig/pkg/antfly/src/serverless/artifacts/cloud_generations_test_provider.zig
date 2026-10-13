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

//! Scripted generation/retention semantics; never provider qualification.
const std = @import("std");
const os = @import("objectstore");
const A = std.mem.Allocator;
pub const Provider = struct {
    memory: os.MemoryClient,
    versions: std.StringHashMapUnmanaged([]u8) = .empty,
    serial: u64 = 0,
    now_ms: u64 = 100,
    until_ms: u64 = 1_000_000,
    stats: usize = 0,
    pub fn init(a: A) Provider {
        return .{ .memory = .init(a) };
    }
    pub fn deinit(self: *Provider) void {
        var it = self.versions.iterator();
        while (it.next()) |entry| {
            self.memory.alloc.free(entry.key_ptr.*);
            self.memory.alloc.free(entry.value_ptr.*);
        }
        self.versions.deinit(self.memory.alloc);
        self.memory.deinit();
    }
    pub fn client(self: *Provider) os.Client {
        return .{ .allocator = self.memory.alloc, .ptr = self, .vtable = &vtable };
    }
    fn owner(raw: *anyopaque) *Provider {
        return @ptrCast(@alignCast(raw));
    }
    fn noop(_: A, _: *anyopaque) void {}
    fn exists(raw: *anyopaque, bucket: []const u8, _: os.BucketOptions) !bool {
        var c = owner(raw).memory.client();
        return c.bucketExists(bucket);
    }
    fn make(raw: *anyopaque, bucket: []const u8, _: os.BucketOptions) !void {
        var c = owner(raw).memory.client();
        return c.makeBucket(bucket);
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, bytes: []const u8, options: os.PutOptions) !os.PutResult {
        const self = owner(raw);
        var c = self.memory.client();
        var uploaded = try c.putObject(bucket, name, bytes, options);
        uploaded.deinit(c.allocator);
        self.serial += 1;
        const generation = try std.fmt.allocPrint(a, "{d}", .{self.serial});
        errdefer a.free(generation);
        const version_name = try std.fmt.allocPrint(a, "{s}@{s}", .{ name, generation });
        defer a.free(version_name);
        var version = try c.putObject(bucket, version_name, bytes, .{});
        version.deinit(c.allocator);
        const allocator = self.memory.alloc;
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);
        const owned_generation = try allocator.dupe(u8, generation);
        errdefer allocator.free(owned_generation);
        const entry = try self.versions.getOrPut(allocator, owned_name);
        if (entry.found_existing) {
            allocator.free(owned_name);
            allocator.free(entry.value_ptr.*);
        } else entry.key_ptr.* = owned_name;
        entry.value_ptr.* = owned_generation;
        return .{ .version_id = generation };
    }
    fn get(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, options: os.GetOptions) !os.GetResult {
        const self = owner(raw);
        var c = self.memory.client();
        c.allocator = a;
        const version = options.version_id orelse self.versions.get(name);
        const path = if (options.version_id) |value| try std.fmt.allocPrint(a, "{s}@{s}", .{ name, value }) else try a.dupe(u8, name);
        defer a.free(path);
        var result = try c.getObject(bucket, path, options);
        errdefer result.deinit(a);
        result.metadata.version_id = if (version) |value| try a.dupe(u8, value) else null;
        return result;
    }
    fn stat(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8) !os.ObjectMetadata {
        return statOptions(raw, a, bucket, name, .{});
    }
    fn statOptions(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, options: os.StatOptions) !os.ObjectMetadata {
        const self = owner(raw);
        self.stats += 1;
        var c = self.memory.client();
        c.allocator = a;
        const path = if (options.version_id) |value| try std.fmt.allocPrint(a, "{s}@{s}", .{ name, value }) else try a.dupe(u8, name);
        defer a.free(path);
        var result = try c.statObject(bucket, path);
        errdefer result.deinit(a);
        result.version_id = if (options.version_id orelse self.versions.get(name)) |value| try a.dupe(u8, value) else null;
        return result;
    }
    fn attributes(_: *anyopaque, _: A, _: []const u8, _: []const u8) !os.ObjectAttributes {
        return error.UnexpectedMetadataProbe;
    }
    fn delete(raw: *anyopaque, bucket: []const u8, name: []const u8, options: os.DeleteOptions) !void {
        const self = owner(raw);
        if (self.now_ms < self.until_ms) return error.AccessDenied;
        const version = options.version_id orelse return error.UnexpectedCurrentNameDelete;
        const path = try std.fmt.allocPrint(self.memory.alloc, "{s}@{s}", .{ name, version });
        defer self.memory.alloc.free(path);
        var c = self.memory.client();
        try c.deleteObject(bucket, path, .{});
        if (self.versions.get(name)) |current| if (std.mem.eql(u8, current, version)) {
            c.deleteObject(bucket, name, .{}) catch |err| if (err != error.FileNotFound) return err;
            const entry = self.versions.fetchRemove(name).?;
            self.memory.alloc.free(entry.key);
            self.memory.alloc.free(entry.value);
        };
    }
    fn list(_: *anyopaque, _: A, _: []const u8, _: os.ListOptions) !os.ListResult {
        return error.UnexpectedList;
    }
    fn listVersions(raw: *anyopaque, a: A, bucket: []const u8, options: os.ListObjectVersionsOptions) !os.ListObjectVersionsResult {
        const self = owner(raw);
        var c = self.memory.client();
        c.allocator = a;
        var page = try c.listObjects(bucket, .{ .prefix = options.prefix, .recursive = true, .max_keys = 10_000 });
        defer page.deinit(a);
        var entries: std.ArrayList(os.ObjectVersionEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(a);
            entries.deinit(a);
        }
        const offset = if (options.continuation_token) |token| try std.fmt.parseUnsigned(usize, token, 10) else 0;
        var eligible: usize = 0;
        var more = false;
        for (page.entries) |entry| {
            const split = std.mem.lastIndexOfScalar(u8, entry.key, '@') orelse continue;
            defer eligible += 1;
            if (eligible < offset) continue;
            if (entries.items.len == options.max_keys) {
                more = true;
                break;
            }
            const name = try a.dupe(u8, entry.key[0..split]);
            errdefer a.free(name);
            const version = try a.dupe(u8, entry.key[split + 1 ..]);
            errdefer a.free(version);
            try entries.append(a, .{ .key = name, .version_id = version, .is_delete_marker = false });
        }
        const token = if (more) try std.fmt.allocPrint(a, "{d}", .{offset + entries.items.len}) else null;
        errdefer if (token) |value| a.free(value);
        return .{ .entries = try entries.toOwnedSlice(a), .is_truncated = more, .next_continuation_token = token };
    }
    fn retention(raw: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8, _: ?os.CancellationToken) !?os.VersionRetention {
        const self = owner(raw);
        return if (self.now_ms < self.until_ms) .{ .observed_ms = self.now_ms, .until_ms = self.until_ms } else null;
    }
    fn time(raw: *anyopaque, _: A, _: []const u8, _: ?os.CancellationToken) !?u64 {
        return owner(raw).now_ms;
    }
    const vtable: os.Client.VTable = .{ .deinit = noop, .bucket_exists = exists, .make_bucket = make, .put_object = put, .get_object = get, .stat_object = stat, .stat_object_with_options = statOptions, .get_object_attributes = attributes, .delete_object = delete, .list_objects = list, .list_object_versions = listVersions, .version_retention = retention, .provider_time = time };
};
