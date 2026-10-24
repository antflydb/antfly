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

//! Explicit public CLI session integration. Never reads Cloud's private config,
//! executes a shell, persists tokens, or forwards provider credentials.
const std = @import("std");
const httpx = @import("httpx");
const A = std.mem.Allocator;
const Context = struct { url: []const u8, token: []const u8, org: ?[]const u8 = null, instance: ?[]const u8 = null, expires_at: ?[]const u8 = null };
pub const Session = struct {
    arena: std.heap.ArenaAllocator,
    io: std.Io,
    cli: []const u8,
    context: Context,
    subject: ?[]const u8 = null,
    mutex: std.Io.Mutex = .init,
    pub fn init(a: A, io: std.Io, cli: []const u8) !Session {
        var self: Session = .{ .arena = .init(a), .io = io, .cli = cli, .context = undefined };
        errdefer self.arena.deinit();
        self.context = try self.loadContext();
        return self;
    }
    pub fn jsonStringify(self: *const Session, writer: anytype) !void {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(self.context.token, &digest, .{});
        try writer.write(.{ .endpoint = self.context.url, .credential_scope = digest });
    }
    pub fn deinit(self: *Session) void {
        self.arena.deinit();
    }
    fn loadContext(self: *Session) !Context {
        const a = self.arena.allocator();
        const result = try std.process.run(a, self.io, .{
            .argv = &.{ self.cli, "context", "--json" },
            .stdout_limit = .limited(1024 * 1024),
            .stderr_limit = .limited(4096),
            .timeout = .{ .duration = .{ .raw = .fromMilliseconds(60000), .clock = .awake } },
        });
        if (!result.term.success()) return error.CloudLoginRequired;
        const ctx = try std.json.parseFromSliceLeaky(Context, a, result.stdout, .{});
        try @import("providers.zig").validateCustom(ctx.url);
        if (ctx.token.len == 0 or std.mem.indexOfAny(u8, ctx.token, "\r\n") != null or std.mem.startsWith(u8, ctx.token, "antfly_cloud_")) return error.InvalidCloudDataPlaneCredential;
        return ctx;
    }
    pub fn token(self: *Session) []const u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.context.token;
    }
    pub fn refresh(self: *Session, rejected: []const u8) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        // Coalesce concurrent refreshes. Retain earlier token buffers until all
        // workers finish; callers may still hold immutable header slices.
        if (!std.mem.eql(u8, rejected, self.context.token)) return;
        const updated = try self.loadContext();
        if (!std.mem.eql(u8, updated.url, self.context.url)) return error.CloudContextChanged;
        if (self.subject) |expected| {
            var http = httpx.Client.init(self.arena.allocator(), self.io);
            defer http.deinit();
            const endpoint = try sourceEndpoint(self.arena.allocator(), updated.url);
            const auth = try std.fmt.allocPrint(self.arena.allocator(), "Bearer {s}", .{updated.token});
            var response = try http.get(endpoint, .{ .headers = &.{.{ "Authorization", auth }}, .timeout_ms = 60000, .max_response_size = 1024 * 1024, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false });
            defer response.deinit();
            if (!response.ok()) return error.CloudLoginRequired;
            const identity = try std.json.parseFromSliceLeaky(struct { subject: []const u8 }, self.arena.allocator(), response.body orelse return error.EmptyCloudSourceDiscovery, .{ .ignore_unknown_fields = true });
            if (!std.mem.eql(u8, expected, identity.subject)) return error.CloudIdentityChanged;
        }
        self.context = updated;
    }
    pub fn discover(self: *Session, a: A, timeout_ms: u64) ![]const u8 {
        // Bound wire bytes, retained parsing memory and traversal length. A
        // partial discovery never becomes a usable search configuration.
        var memory: @import("antfly_regex").Budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        const state = arena.allocator();
        var http = httpx.Client.init(a, self.io);
        defer http.deinit();
        const endpoint = try sourceEndpoint(a, self.context.url);
        defer a.free(endpoint);
        const Discovery = struct {
            subject: []const u8,
            version: u32,
            endpoint: []const u8,
            sources: []const @import("catalog.zig").Source,
            principals: []const []const u8,
            page_size: usize = 128,
            discovery_id: ?[]const u8 = null,
            next_cursor: ?[]const u8 = null,
        };
        var tables: std.ArrayList(std.json.Value) = .empty;
        var principals: std.ArrayList([]const u8) = .empty;
        var source_ids = std.StringHashMap(void).init(state);
        var reader_ids = std.StringHashMap(void).init(state);
        var cursors = std.StringHashMap(void).init(state);
        var cursor: ?[]const u8 = null;
        var generation: ?[]const u8 = null;
        var version: ?u32 = null;
        var query_page_size: usize = 128;
        var bytes: usize = 0;
        var pages: usize = 0;
        while (true) {
            if (pages >= 1024) return error.CloudDiscoveryBudgetExceeded;
            pages += 1;
            // Cursors are opaque URL-safe bytes, never credential destinations.
            const url = if (cursor) |value| try std.fmt.allocPrint(a, "{s}?cursor={s}", .{ endpoint, value }) else try a.dupe(u8, endpoint);
            defer a.free(url);
            var response = response: {
                for (0..2) |attempt| {
                    const credential = self.token();
                    const auth = try std.fmt.allocPrint(a, "Bearer {s}", .{credential});
                    defer a.free(auth);
                    var result = try http.get(url, .{ .headers = &.{.{ "Authorization", auth }}, .timeout_ms = timeout_ms, .max_response_size = 1024 * 1024, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false });
                    if (result.status.code == 401 and attempt == 0) {
                        result.deinit();
                        try self.refresh(credential);
                        continue;
                    }
                    errdefer result.deinit();
                    if (result.status.code == 401 or result.status.code == 403) return error.CloudSourceAccessDenied;
                    if (result.status.code == 409) return error.CloudDiscoveryChanged;
                    if (!result.ok()) return error.CloudSourceDiscoveryFailed;
                    break :response result;
                }
                return error.CloudLoginRequired;
            };
            defer response.deinit();
            const body = response.body orelse return error.EmptyCloudSourceDiscovery;
            if (body.len > (32 * 1024 * 1024) -| bytes) return error.CloudDiscoveryBudgetExceeded;
            bytes += body.len;
            const page = try std.json.parseFromSliceLeaky(Discovery, state, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
            if (page.subject.len == 0) return error.InvalidCloudIdentity;
            if (self.subject) |expected| {
                if (!std.mem.eql(u8, expected, page.subject)) return error.CloudIdentityChanged;
            } else self.subject = try self.arena.allocator().dupe(u8, page.subject);
            if (page.version != 2) return error.UnsupportedCloudSourceVersion;
            if (!std.mem.eql(u8, page.endpoint, "db/v1")) return error.CloudContextChanged;
            if (version) |expected| {
                if (expected != page.version or query_page_size != page.page_size) return error.CloudDiscoveryChanged;
            } else {
                version = page.version;
                query_page_size = page.page_size;
            }
            const id = page.discovery_id orelse return error.InvalidCloudSourceDiscovery;
            if (id.len == 0 or id.len > 128) return error.InvalidCloudSourceDiscovery;
            if (generation) |expected| {
                if (!std.mem.eql(u8, expected, id)) return error.CloudDiscoveryChanged;
            } else generation = id;
            if (tables.items.len + page.sources.len > 1000) return error.CloudDiscoveryBudgetExceeded;
            for (page.sources) |source| {
                if (source.access_id == null) return error.InvalidCloudSourceDiscovery;
                const binding = @import("catalog.zig").table(source) catch return error.InvalidCloudSourceDiscovery;
                if ((try source_ids.getOrPut(source.id)).found_existing) return error.DuplicateSourceID;
                const encoded = try std.json.Stringify.valueAlloc(state, binding, .{});
                try tables.append(state, try std.json.parseFromSliceLeaky(std.json.Value, state, encoded, .{}));
            }
            for (page.principals) |reader| {
                if (!(try reader_ids.getOrPut(reader)).found_existing) {
                    if (reader_ids.count() > 262144) return error.CloudDiscoveryBudgetExceeded;
                    try principals.append(state, reader);
                }
            }
            if (page.next_cursor) |next| {
                if (page.sources.len == 0 or next.len == 0 or next.len > 256) return error.InvalidCloudSourceDiscovery;
                for (next) |char| if (!std.ascii.isAlphanumeric(char) and char != '_' and char != '-') return error.InvalidCloudSourceDiscovery;
                if ((try cursors.getOrPut(next)).found_existing) return error.InvalidCloudSourceDiscovery;
                cursor = next;
                continue;
            }
            break;
        }
        if (tables.items.len == 0) return error.NoConnectedSources;
        // Discovery v2 is independent of the existing indexed-table config v1.
        return std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 1), .endpoint = try std.fmt.allocPrint(state, "{s}/db/v1", .{std.mem.trimEnd(u8, self.context.url, "/")}), .tables = tables.items, .principals = principals.items, .page_size = query_page_size }, .{});
    }
};

// The public Cloud CLI context is the instance proxy root, shared with the
// inference API. Resolve the Cloud catalog route identically for discovery
// and identity verification during refresh; neither accepts server-supplied URLs.
fn sourceEndpoint(a: A, instance_url: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}/sources", .{std.mem.trimEnd(u8, instance_url, "/")});
}
