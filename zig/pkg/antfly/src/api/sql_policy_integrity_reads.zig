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

//! Request-owned authority cache retaining native pinned routing and fanout.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const domain = @import("antfly_local_sources").system_catalog_domain;
const http = @import("http_server.zig");

pub const Cache = struct {
    /// Borrowed from the immutable, already-authorized admission frame. This
    /// is a routing witness, never a grant: the proof provider still signs it.
    pub const Binding = struct { table_id: u64, database: []const u8, schema_version: ?u32 = null };
    pub const Bindings = struct {
        ptr: *const anyopaque,
        resolve: *const fn (*const anyopaque, []const u8) anyerror!?Binding,
    };
    server: *http.ApiHttpServer,
    context: operation.RequestContext,
    known: ?Bindings = null,
    arena: std.heap.ArenaAllocator,
    mutex: std.Io.Mutex = .init,
    entries: std.StringHashMapUnmanaged(?operation.RowPolicyIntegrityRead.Proof) = .empty,

    pub fn init(server: *http.ApiHttpServer, context: operation.RequestContext) Cache {
        return .{ .server = server, .context = context, .arena = .init(server.alloc) };
    }
    pub fn deinit(self: *Cache) void {
        self.arena.deinit();
    }
    pub fn attach(self: *Cache, context: operation.RequestContext) operation.RequestContext {
        var result = context;
        if (result.row_policy_integrity_read == null) result = self.overlay(context);
        return result;
    }
    /// A write/transaction admission owns a distinct, immutable authority
    /// frame. Initialize this cache after that scope reaches its final address.
    /// Its proofs must never share a physical-name-only cache with another
    /// frame, even when the same table appears in both.
    pub fn overlay(self: *Cache, context: operation.RequestContext) operation.RequestContext {
        var result = context;
        if (self.server.cfg.trusted_principal_secret != null and
            (context.row_policy_credential != null or context.row_policy_proof_provider != null))
            result.row_policy_integrity_read = .{ .ptr = self, .authorize = authorize };
        return result;
    }
    fn authorize(ptr: *anyopaque, _: std.mem.Allocator, physical: []const u8) !?operation.RowPolicyIntegrityRead.Proof {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        const io = self.server.embedding_provider_runtime.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        try self.context.ensureActive();
        if (self.entries.get(physical)) |cached| return cached;
        // Discovery and signing can allocate transient catalog/proof buffers.
        // Retain only the final token and database for this admission's life.
        var scratch = std.heap.ArenaAllocator.init(self.server.alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        const known = if (self.known) |bindings| try bindings.resolve(bindings.ptr, physical) else null;
        const binding = known orelse try self.discover(a, physical);
        if (binding.table_id == 0 or binding.database.len == 0) return error.RowPolicyCatalogChanged;
        const proof: ?operation.RowPolicyIntegrityRead.Proof = if (self.context.row_policy_proof_provider) |provider| scoped: {
            switch (try provider.sign(provider.ptr, a, binding.table_id, physical, binding.database, binding.schema_version, .integrity)) {
                .handled => |token| break :scoped if (token) |bytes| .{ .token = bytes, .database = binding.database } else null,
                .unhandled => if (known != null) return error.RowPolicyCatalogChanged,
            }
            // Discovered participants retain the prior admission's authority.
            // The SQL target grant cannot authorize an unrelated parent table.
            if (self.context.row_policy_integrity_read) |prior|
                break :scoped try prior.authorize(prior.ptr, a, physical);
            const token = try self.server.rowPolicyIntegrityReadProof(a, self.context, binding.table_id, physical, binding.database);
            break :scoped if (token) |bytes| .{ .token = bytes, .database = binding.database } else null;
        } else legacy: {
            if (known != null) return error.RowPolicyCatalogChanged;
            const token = try self.server.rowPolicyIntegrityReadProof(a, self.context, binding.table_id, physical, binding.database);
            break :legacy if (token) |bytes| .{ .token = bytes, .database = binding.database } else null;
        };
        try self.context.ensureActive();
        const retained = self.arena.allocator();
        const owned: ?operation.RowPolicyIntegrityRead.Proof = if (proof) |value| .{
            .token = try retained.dupe(u8, value.token),
            .database = try retained.dupe(u8, value.database),
        } else null;
        try self.entries.put(retained, try retained.dupe(u8, physical), owned);
        return owned;
    }
    fn discover(self: *Cache, a: std.mem.Allocator, physical: []const u8) !Binding {
        const reversed = try self.server.source.systemCatalog(a, self.context, .{ .resolve_many = .{ .storage_names = &.{physical} } });
        const names = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, reversed, .{});
        if (names.logical_names.len != 1) return error.RowPolicyCatalogChanged;
        const target = try domain.Target.parse(names.logical_names[0] orelse return error.RowPolicyCatalogChanged);
        const resolved_bytes = try self.server.source.systemCatalog(a, self.context, .{ .resolve_many = .{ .targets = &.{target}, .expected_revision = names.revision } });
        const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, resolved_bytes, .{});
        if (resolved.tables.len != 1) return error.RowPolicyCatalogChanged;
        const table = resolved.tables[0] orelse return error.RowPolicyCatalogChanged;
        if (!std.mem.eql(u8, table.name, physical)) return error.RowPolicyCatalogChanged;
        return .{ .table_id = table.table_id, .database = target.database };
    }
};

test "SQL privileges integrity proof caches remain bound to their admission authority" {
    const a = std.testing.allocator;
    const Source = struct {
        calls: usize = 0,
        fn call(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (input != .resolve_many) return error.UnexpectedCall;
            const request = input.resolve_many;
            if (request.storage_names.len == 1) return std.json.Stringify.valueAlloc(alloc, .{ .revision = @as(u64, 1), .tables = @as([]const ?domain.ResolvedTable, &.{}), .logical_names = .{@as([]const u8, if (std.mem.eql(u8, request.storage_names[0], "physical_docs")) "docs" else "parent")} }, .{});
            if (request.targets.len != 1) return error.UnexpectedCall;
            const docs = std.mem.eql(u8, request.targets[0].table, "docs");
            return std.json.Stringify.valueAlloc(alloc, .{ .revision = @as(u64, 1), .tables = .{.{ .table_id = @as(u64, if (docs) 7 else 8), .name = @as([]const u8, if (docs) "physical_docs" else "physical_parent") }} }, .{});
        }
    };
    const Frame = struct {
        label: []const u8,
        calls: usize = 0,
        allow: bool = true,
        bound: bool = false,
        fn binding(_: *const anyopaque, physical: []const u8) !?Cache.Binding {
            if (!std.mem.eql(u8, physical, "physical_docs")) return null;
            return .{ .table_id = 7, .database = "default", .schema_version = 3 };
        }
        fn sign(raw: *const anyopaque, alloc: std.mem.Allocator, id: u64, _: []const u8, database: []const u8, version: ?u32, purpose: @import("../usermgr/mod.zig").row_policy_authority.Purpose) !operation.RowPolicyProofProvider.Result {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (id != 7 or !self.allow) return .unhandled;
            try std.testing.expectEqualStrings("default", database);
            try std.testing.expectEqual(if (self.bound) @as(?u32, 3) else null, version);
            try std.testing.expectEqual(.integrity, purpose);
            self.calls += 1;
            return .{ .handled = try alloc.dupe(u8, self.label) };
        }
    };
    const Parent = struct {
        calls: usize = 0,
        fn read(raw: *anyopaque, _: std.mem.Allocator, physical: []const u8) !?operation.RowPolicyIntegrityRead.Proof {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, physical, "physical_parent")) return error.Forbidden;
            self.calls += 1;
            return .{ .token = "native-parent-proof", .database = "default" };
        }
    };
    var source: Source = .{};
    var server = http.ApiHttpServer.init(a, .{ .trusted_principal_secret = "01234567890123456789012345678901" }, .{ .ptr = &source, .vtable = &.{ .status = undefined, .system_catalog = Source.call } }, null, null);
    defer server.deinit();
    var parent: Parent = .{};
    var first: Frame = .{ .label = "sql-frame-a", .bound = true };
    var second: Frame = .{ .label = "sql-frame-b" };
    var denied: Frame = .{ .label = "denied", .allow = false };
    for ([_]*Frame{ &first, &second, &denied }, 0..) |frame, ordinal| {
        const context: operation.RequestContext = .{ .row_policy_integrity_read = .{ .ptr = &parent, .authorize = Parent.read }, .row_policy_proof_provider = .{ .ptr = frame, .sign = Frame.sign } };
        var cache = Cache.init(&server, context);
        defer cache.deinit();
        if (frame.bound) cache.known = .{ .ptr = frame, .resolve = Frame.binding };
        const scoped = cache.overlay(context).row_policy_integrity_read.?;
        const before = source.calls;
        if (ordinal == 2) {
            try std.testing.expectError(error.Forbidden, scoped.authorize(scoped.ptr, a, "physical_docs"));
            continue;
        }
        for (0..2) |_| {
            const proof = (try scoped.authorize(scoped.ptr, a, "physical_docs")).?;
            try std.testing.expectEqualStrings(frame.label, proof.token);
        }
        try std.testing.expectEqual(@as(usize, 1), frame.calls);
        try std.testing.expectEqual(before + @as(usize, if (frame.bound) 0 else 2), source.calls);
        for (0..2) |_| {
            const proof = (try scoped.authorize(scoped.ptr, a, "physical_parent")).?;
            try std.testing.expectEqualStrings("native-parent-proof", proof.token);
        }
    }
    try std.testing.expectEqual(@as(usize, 2), parent.calls);
    try std.testing.expectEqual(@as(usize, 0), denied.calls);
    try std.testing.expectEqual(@as(usize, 8), source.calls);
    // A binding is not permission. Declined signing cannot borrow another
    // admission frame's authority, even when it names the same table.
    const denied_context: operation.RequestContext = .{ .row_policy_integrity_read = .{ .ptr = &parent, .authorize = Parent.read }, .row_policy_proof_provider = .{ .ptr = &denied, .sign = Frame.sign } };
    var denied_cache = Cache.init(&server, denied_context);
    defer denied_cache.deinit();
    denied_cache.known = .{ .ptr = &denied, .resolve = Frame.binding };
    const denied_scoped = denied_cache.overlay(denied_context).row_policy_integrity_read.?;
    try std.testing.expectError(error.RowPolicyCatalogChanged, denied_scoped.authorize(denied_scoped.ptr, a, "physical_docs"));
    try std.testing.expectEqual(@as(usize, 8), source.calls);
    try std.testing.expectEqual(@as(usize, 2), parent.calls);
}
