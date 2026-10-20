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

const h = @import("handles.zig");
const std = h.std;
const engine = @import("../memory/engine.zig");
const memory = engine.service;

pub const Request = struct {
    action: memory.Action,
    id: []const u8 = "",
    idempotency_key: []const u8 = "",
    principal: []const u8 = "embedded",
    body: std.json.Value = .{ .object = .empty },
};

/// The local database owner is the authority for embedded callers. There are
/// no HTTP credentials or caller-supplied server permissions in this contract.
pub fn execute(guard: *@import("db.zig").HandleGuard, handle_ptr: ?*anyopaque, alloc: std.mem.Allocator, space: []const u8, request_json: []const u8) ![]u8 {
    const handle = guard.handle;
    if (request_json.len > 4 * 1024 * 1024) return error.MemoryInputTooLarge;
    try memory.types.validateId(space);
    if (std.mem.indexOfAny(u8, space, "/.\\%") != null) return error.InvalidMemoryId;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try std.json.parseFromSliceLeaky(Request, a, request_json, .{ .ignore_unknown_fields = false });
    const table = try std.fmt.allocPrint(a, "memory_{s}", .{space});
    const tables = @import("tables.zig");
    if (memory.mutates(request.action) and (request.idempotency_key.len == 0 or request.idempotency_key.len > 256)) return error.MemoryIdempotencyKeyRequired;
    if (request.action == .create) {
        const config = try std.json.parseFromSliceLeaky(memory.types.Config, a, try std.json.Stringify.valueAlloc(a, request.body, .{}), .{ .ignore_unknown_fields = false });
        try config.validate();
    }
    if (memory.mutates(request.action) and !h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlReadOnlyTransaction;
    if (request.action == .create) try tables.create(handle.parent_handle orelse handle, table, "{}", true);
    const database = try tables.get(handle.parent_handle orelse handle, table);
    const context: memory.Context = .{ .space = space, .principal = request.principal, .admin = true, .now_ms = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) };
    var adapter: engine.Native(h.db_mod.DB) = .{ .db = database, .context = context };
    // Provider lifetime is independent of the storage generation. Retain it
    // across every model call and writer-lease release in this operation.
    var provider_owner = handle;
    while (provider_owner.parent_handle) |parent| provider_owner = parent;
    const provider_service = provider_owner.lite_inference;
    if (provider_service) |provider| provider.retain();
    defer if (provider_service) |provider| provider.release();
    const runtime = try @import("../asset_producer_runtime.zig").Runtime.createOwned(a, h.handleLockIo(), .{ .antfly_provider = handle.liteAntflyProvider(), .source_table = table });
    defer runtime.deinit();
    const Hooks = struct {
        guard: *@import("db.zig").HandleGuard,
        handle_ptr: ?*anyopaque,
        adapter: *engine.Native(h.db_mod.DB),
        table: []const u8,
        access: @import("db.zig").HandleAccess,
        legacy_suspended: bool = false,
        fn before(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.guard.connection != null) self.guard.suspendQuery() else {
                // Registry admission still pins the directory handle while
                // releasing only its API lock around the owned model plan.
                if (self.access == .read) self.guard.lock_handle.api_lock.unlockShared(h.handleLockIo()) else self.guard.lock_handle.api_lock.unlock(h.handleLockIo());
                self.legacy_suspended = true;
            }
        }
        fn after(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.legacy_suspended) {
                if (self.access == .read) self.guard.lock_handle.api_lock.lockSharedUncancelable(h.handleLockIo()) else self.guard.lock_handle.api_lock.lockUncancelable(h.handleLockIo());
                self.legacy_suspended = false;
            }
            if (!self.guard.resumeAccess(self.handle_ptr, self.access)) return error.MemoryRevisionConflict;
            if (self.guard.entry_error != null) return error.MemoryRevisionConflict;
            const current = self.guard.handle;
            self.adapter.db = try @import("tables.zig").get(current.parent_handle orelse current, self.table);
        }
    };
    var hooks: Hooks = .{ .guard = guard, .handle_ptr = handle_ptr, .adapter = &adapter, .table = table, .access = guard.access };
    var bridge: @import("../memory/models.zig").Bridge = .{ .base = adapter.backend(), .producer = runtime.producer(), .alloc = a, .inference_ptr = &hooks, .before_inference = Hooks.before, .after_inference = Hooks.after, .embedding_options = .{ .antfly_provider = handle.liteAntflyProvider(), .io = h.handleLockIo(), .source_table = table } };
    return (memory.Service{ .backend = bridge.backend(), .context = context }).execute(alloc, request.action, request.id, try std.json.Stringify.valueAlloc(a, request.body, .{}), request.idempotency_key);
}
