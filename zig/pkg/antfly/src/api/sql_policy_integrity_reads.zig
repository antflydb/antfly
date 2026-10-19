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
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations.

//! Request-owned authority cache retaining native pinned routing and fanout.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const domain = @import("antfly_local_sources").system_catalog_domain;
const http = @import("http_server.zig");

pub const Cache = struct {
    server: *http.ApiHttpServer,
    context: operation.RequestContext,
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
        if (result.row_policy_integrity_read == null and self.server.cfg.trusted_principal_secret != null and context.row_policy_credential != null)
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
        const a = self.arena.allocator();
        const reversed = try self.server.source.systemCatalog(a, self.context, .{ .resolve_many = .{ .storage_names = &.{physical} } });
        const names = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, reversed, .{});
        if (names.logical_names.len != 1) return error.RowPolicyCatalogChanged;
        const target = try domain.Target.parse(names.logical_names[0] orelse return error.RowPolicyCatalogChanged);
        const resolved_bytes = try self.server.source.systemCatalog(a, self.context, .{ .resolve_many = .{ .targets = &.{target}, .expected_revision = names.revision } });
        const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, resolved_bytes, .{});
        if (resolved.tables.len != 1) return error.RowPolicyCatalogChanged;
        const table = resolved.tables[0] orelse return error.RowPolicyCatalogChanged;
        if (!std.mem.eql(u8, table.name, physical)) return error.RowPolicyCatalogChanged;
        const token = try self.server.rowPolicyIntegrityReadProof(a, self.context, table.table_id, physical, target.database);
        const proof: ?operation.RowPolicyIntegrityRead.Proof = if (token) |bytes| .{ .token = bytes, .database = target.database } else null;
        try self.entries.put(a, try a.dupe(u8, physical), proof);
        return proof;
    }
};
