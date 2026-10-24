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
const A = std.mem.Allocator;
const Context = struct { url: []const u8, token: []const u8, org: ?[]const u8 = null, instance: ?[]const u8 = null, expires_at: ?[]const u8 = null };
pub const Session = struct {
    arena: std.heap.ArenaAllocator,
    io: std.Io,
    cli: []const u8,
    context: Context,
    subject: ?[]const u8 = null,
    source_proofs: std.StringHashMapUnmanaged([]const u8) = .empty,
    source_publications: std.StringHashMapUnmanaged([]const u8) = .empty,
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
        self.context = updated;
    }
    pub fn checkSubject(self: *Session, subject: ?[]const u8) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        try self.checkSubjectLocked(subject);
    }
    fn checkSubjectLocked(self: *Session, subject: ?[]const u8) !void {
        const actual = subject orelse return error.InvalidCloudIdentity;
        if (actual.len == 0 or actual.len > 512) return error.InvalidCloudIdentity;
        if (self.subject) |expected| {
            if (!std.mem.eql(u8, expected, actual)) return error.CloudIdentityChanged;
        } else self.subject = try self.arena.allocator().dupe(u8, actual);
    }
    pub fn sourceProof(self: *Session, id: []const u8) ?[]const u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.source_proofs.get(id);
    }
    pub fn sourcePublication(self: *Session, id: []const u8) ?[]const u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.source_publications.get(id);
    }
    pub fn checkSource(self: *Session, id: []const u8, subject: ?[]const u8, proof: ?[]const u8, publication: ?[]const u8) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        try self.checkSubjectLocked(subject);
        const actual = proof orelse return error.InvalidCloudSourcePolicy;
        if (actual.len == 0 or actual.len > 128) return error.InvalidCloudSourcePolicy;
        for (actual) |char| if (!std.ascii.isAlphanumeric(char) and char != '_' and char != '-') return error.InvalidCloudSourcePolicy;
        const fence = publication orelse return error.InvalidCloudSourcePublication;
        if (fence.len == 0 or fence.len > 128) return error.InvalidCloudSourcePublication;
        for (fence) |char| if (!std.ascii.isAlphanumeric(char) and char != '_' and char != '-') return error.InvalidCloudSourcePublication;
        if (self.source_publications.get(id)) |expected| {
            if (!std.mem.eql(u8, expected, fence)) return error.SourcePublicationChanged;
        }
        if (self.source_proofs.get(id)) |expected| {
            if (!std.mem.eql(u8, expected, actual)) return error.CloudSourceAccessChanged;
        } else {
            if (self.source_proofs.count() >= 1000) return error.CloudDiscoveryBudgetExceeded;
            const a = self.arena.allocator();
            try self.source_proofs.put(a, try a.dupe(u8, id), try a.dupe(u8, actual));
            try self.source_publications.put(a, try a.dupe(u8, id), try a.dupe(u8, fence));
        }
    }
    pub fn discover(self: *Session, a: A, timeout_ms: u64) ![]const u8 {
        const endpoint = try std.fmt.allocPrint(a, "{s}/db/v1", .{std.mem.trimEnd(u8, self.context.url, "/")});
        defer a.free(endpoint);
        return @import("catalog.zig").discoverWithSession(a, self.io, endpoint, "antfly_sources", null, &.{}, timeout_ms, self);
    }
};
