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

//! Scripted generation/retention semantics; never provider qualification.
const std = @import("std");
const os = @import("objectstore");
const A = std.mem.Allocator;
pub const Provider = struct {
    memory: os.MemoryClient,
    mutex: std.atomic.Mutex = .unlocked,
    versions: std.StringHashMapUnmanaged([]u8) = .empty,
    serial: u64 = 0,
    stable_pagination: bool = false,
    marker_pagination: bool = false,
    null_versions: bool = false,
    now_ms: u64 = 100,
    until_ms: u64 = 1_000_000,
    stats: usize = 0,
    gets: usize = 0,
    get_hook: ?struct { ptr: *anyopaque, call: *const fn (*anyopaque) anyerror!void } = null,
    put_hook: ?struct { ptr: *anyopaque, call: *const fn (*anyopaque) anyerror!void } = null,
    retention_hook: ?struct { ptr: *anyopaque, call: *const fn (*anyopaque) anyerror!void } = null,
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
        if (self.put_hook) |hook| try hook.call(hook.ptr);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var c = self.memory.client();
        var uploaded = try c.putObject(bucket, name, bytes, options);
        uploaded.deinit(c.allocator);
        self.serial += 1;
        const generation = if (self.null_versions) try a.dupe(u8, "null") else try std.fmt.allocPrint(a, "{d}", .{self.serial});
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
        if (self.get_hook) |hook| try hook.call(hook.ptr);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.gets += 1;
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
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
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
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
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
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var c = self.memory.client();
        c.allocator = a;
        var page = try c.listObjects(bucket, .{ .prefix = options.prefix, .recursive = true, .max_keys = 10_000 });
        defer page.deinit(a);
        var entries: std.ArrayList(os.ObjectVersionEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(a);
            entries.deinit(a);
        }
        const offset = if (!self.stable_pagination) if (options.continuation_token) |token| try std.fmt.parseUnsigned(usize, token, 10) else 0 else 0;
        var eligible: usize = 0;
        var more = false;
        var last: []const u8 = "";
        const lower = if (self.marker_pagination) blk: {
            if (options.continuation_token != null or (options.version_id_marker != null and options.key_marker == null)) return error.InvalidVersionPagination;
            break :blk if (options.key_marker) |key| try std.fmt.allocPrint(a, "{s}@{s}", .{ key, options.version_id_marker orelse "" }) else null;
        } else if (options.continuation_token) |token| try a.dupe(u8, token) else null;
        defer if (lower) |value| a.free(value);
        for (page.entries) |entry| {
            const split = std.mem.lastIndexOfScalar(u8, entry.key, '@') orelse continue;
            if (self.stable_pagination) if (lower) |token| if (std.mem.order(u8, entry.key, token) != .gt) continue;
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
            last = entry.key;
        }
        const token = if (more and !self.marker_pagination) if (self.stable_pagination) try a.dupe(u8, last) else try std.fmt.allocPrint(a, "{d}", .{offset + entries.items.len}) else null;
        errdefer if (token) |value| a.free(value);
        const next_key = if (more and self.marker_pagination) try a.dupe(u8, entries.items[entries.items.len - 1].key) else null;
        errdefer if (next_key) |value| a.free(value);
        const next_version = if (more and self.marker_pagination) try a.dupe(u8, entries.items[entries.items.len - 1].version_id) else null;
        errdefer if (next_version) |value| a.free(value);
        return .{ .entries = try entries.toOwnedSlice(a), .is_truncated = more, .next_continuation_token = token, .next_key_marker = next_key, .next_version_id_marker = next_version };
    }
    fn retention(raw: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8, _: ?os.CancellationToken) !?os.VersionRetention {
        const self = owner(raw);
        if (self.retention_hook) |hook| try hook.call(hook.ptr);
        return if (self.now_ms < self.until_ms) .{ .observed_ms = self.now_ms, .until_ms = self.until_ms } else null;
    }
    fn time(raw: *anyopaque, _: A, _: []const u8, _: ?os.CancellationToken) !?u64 {
        return owner(raw).now_ms;
    }
    const vtable: os.Client.VTable = .{ .deinit = noop, .bucket_exists = exists, .make_bucket = make, .put_object = put, .get_object = get, .stat_object = stat, .stat_object_with_options = statOptions, .get_object_attributes = attributes, .delete_object = delete, .list_objects = list, .list_object_versions = listVersions, .version_retention = retention, .provider_time = time };
};
