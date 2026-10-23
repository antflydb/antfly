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

//! One bounded provider scope per workspace. Query workers borrow resources;
//! reconfiguration happens only after every worker has joined.
const std = @import("std");
const Options = @import("options.zig").Options;
const httpx = @import("httpx");
pub const Session = struct {
    a: std.mem.Allocator,
    io: std.Io,
    files: @import("worker_pool.zig").Pool,
    providers: @import("worker_pool.zig").Pool,
    plans: @import("region_plan.zig").Cache,
    scope: ?[32]u8 = null,
    arena: ?std.heap.ArenaAllocator = null,
    options: Options = .{},
    http: ?httpx.Client = null,
    local: ?*@import("local_decider.zig").Runtime = null,
    memo: ?*@import("batch_store.zig").Memo = null,
    batches: @import("batch_store.zig").Store = .{},
    pub fn init(a: std.mem.Allocator, io: std.Io) Session {
        return .{ .a = a, .io = io, .files = .{ .io = io }, .providers = .{ .io = io }, .plans = .{ .a = a, .io = io } };
    }
    fn reset(self: *Session) void {
        if (self.local) |local| local.deinit();
        self.local = null;
        if (self.http) |*http| http.deinit();
        self.http = null;
        self.batches.deinit(self.io);
        self.batches = .{};
        if (self.memo) |memo| {
            memo.deinit();
            self.a.destroy(memo);
        }
        self.memo = null;
        if (self.arena) |*arena| arena.deinit();
        self.arena = null;
        self.scope = null;
    }
    pub fn configure(self: *Session, o: *const Options) !void {
        const identity = try std.json.Stringify.valueAlloc(self.a, .{ o.endpoint, o.token, o.backend, o.models_dir, o.library_path, o.timeout_ms, o.cache_dir }, .{});
        defer self.a.free(identity);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(identity, &digest, .{});
        if (self.scope) |scope| if (std.mem.eql(u8, &scope, &digest)) return;
        self.reset();
        errdefer self.reset();
        self.arena = .init(self.a);
        const a = self.arena.?.allocator();
        self.options = .{ .timeout_ms = o.timeout_ms };
        if (o.models_dir) |path| self.options.models_dir = try a.dupe(u8, path);
        if (o.library_path) |path| self.options.library_path = try a.dupe(u8, path);
        self.memo = try self.a.create(@import("batch_store.zig").Memo);
        self.memo.?.* = .{ .a = self.a, .io = self.io };
        self.batches.memo = self.memo;
        self.http = httpx.Client.init(self.a, self.io);
        self.local = try @import("local_decider.zig").Runtime.create(self.a, self.io, &self.options);
        self.scope = digest;
    }
    pub fn deinit(self: *Session) void {
        self.files.deinit();
        self.providers.deinit();
        self.reset();
        self.plans.deinit();
    }
};
