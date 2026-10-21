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

//! Thin first-party HTTP adapter. Policy and persistent state are native and
//! shared with embedded memory; no memory extension or second policy engine.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const memory = local.memory_service;
pub const engine = local.memory_engine;
pub const Route = struct { action: memory.Action, space: []const u8, id: []const u8 = "" };

pub fn route(method: []const u8, path: []const u8) !Route {
    const prefix = "/memory/v1/spaces/";
    if (!std.mem.startsWith(u8, path, prefix)) return error.MemoryRouteNotFound;
    const tail = path[prefix.len..];
    var parts = std.mem.splitScalar(u8, tail, '/');
    const space = parts.next() orelse return error.MemoryRouteNotFound;
    if (space.len == 0) return error.MemoryRouteNotFound;
    const resource = parts.next() orelse return .{ .space = space, .action = if (std.mem.eql(u8, method, "GET")) .inspect else if (std.mem.eql(u8, method, "PATCH")) .configure else if (std.mem.eql(u8, method, "PUT")) .create else return error.MethodNotAllowed };
    const id = parts.next();
    if (std.mem.eql(u8, resource, "memories")) {
        if (id == null and !std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
        if (id == null) return .{ .space = space, .action = .list };
        if (parts.next()) |suffix| {
            if (parts.next() != null) return error.MemoryRouteNotFound;
            if (std.mem.eql(u8, suffix, "explain") and std.mem.eql(u8, method, "POST")) return .{ .space = space, .id = id.?, .action = .explain };
            if (!std.mem.eql(u8, suffix, "history") or !std.mem.eql(u8, method, "GET")) return error.MemoryRouteNotFound;
            return .{ .space = space, .id = id.?, .action = .history };
        }
        if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
        return .{ .space = space, .id = id.?, .action = .get };
    }
    if (std.mem.eql(u8, resource, "operations")) {
        if (id == null or parts.next() != null) return error.MemoryRouteNotFound;
        return .{ .space = space, .id = id.?, .action = if (std.mem.eql(u8, method, "GET")) .operation else if (std.mem.eql(u8, method, "POST")) .advance else return error.MethodNotAllowed };
    }
    if (std.mem.eql(u8, resource, "configurations") and id != null and parts.next() == null and std.mem.eql(u8, method, "GET")) return .{ .space = space, .action = .configuration, .id = id.? };
    if (std.mem.eql(u8, resource, "evidence") and id != null and parts.next() == null and std.mem.eql(u8, method, "GET")) return .{ .space = space, .action = .evidence, .id = id.? };
    if (std.mem.eql(u8, resource, "entities") and id != null and parts.next() == null and std.mem.eql(u8, method, "GET")) return .{ .space = space, .action = .resolve_entity, .id = id.? };
    if (id != null) return error.MemoryRouteNotFound;
    if (std.mem.eql(u8, resource, "entity-corrections") and std.mem.eql(u8, method, "POST")) return .{ .space = space, .action = .curate_entity };
    if (std.mem.eql(u8, resource, "entities") and std.mem.eql(u8, method, "GET")) return .{ .space = space, .action = .entities };
    if (!std.mem.eql(u8, method, "POST")) return error.MethodNotAllowed;
    inline for (.{ .{ "explain", memory.Action.explain }, .{ "embeddings", memory.Action.stage_embeddings }, .{ "observations", memory.Action.observe }, .{ "assertions", memory.Action.assertions }, .{ "recall", memory.Action.recall }, .{ "corrections", memory.Action.correct }, .{ "forget", memory.Action.forget }, .{ "outcomes", memory.Action.outcome }, .{ "maintenance", memory.Action.advance } }) |entry| {
        if (std.mem.eql(u8, resource, entry[0])) return .{ .space = space, .action = entry[1] };
    }
    return error.MemoryRouteNotFound;
}

pub fn status(err: anyerror) u16 {
    return switch (err) {
        error.MemoryNotFound, error.MemoryEntitySuppressed, error.MemorySpaceNotFound, error.MemoryRouteNotFound, error.TableNotFound => 404,
        error.Unauthorized => 401,
        error.Forbidden, error.MemoryVisibilityConflict, error.MemoryRowPolicyUnsupported => 403,
        error.MethodNotAllowed => 405,
        error.MemoryRevisionConflict, error.MemorySpaceBindingChanged, error.MemoryCursorStale, error.MemorySpaceExists, error.MemoryIdempotencyConflict, error.MemoryEvidenceConflict, error.MemoryAssertionConflict, error.MemoryOutcomeConflict, error.MemoryProducerConflict, error.MemoryEntityConflict, error.MemoryConfigurationTransition, error.MemoryForgotten, error.MemoryConfigurationChanged, error.MemoryResourceChanged => 409,
        error.MemoryInputTooLarge, error.MemoryWorkLimit => 413,
        error.MemoryModelRequired, error.MemoryCapabilityUnavailable, error.MemoryExecutionIdentityRequired, error.MemoryModelRevisionUnavailable, error.MemoryModelRevisionMismatch => 422,
        error.MemoryRetryNotReady, error.MemoryRetryExhausted, error.MemoryOverloaded => 429,
        error.MemoryCommitPending, error.NotLeader, error.Unavailable, error.StorageUnavailable, error.GroupLeaderUnavailable => 503,
        error.Canceled => 408,
        error.DeadlineExceeded => 504,
        error.OutOfMemory, error.MemoryCorruptState => 500,
        else => 400,
    };
}

test "memory route separates native operations and rejects unknown paths" {
    try std.testing.expectEqual(memory.Action.observe, (try route("POST", "/memory/v1/spaces/team/observations")).action);
    try std.testing.expectEqual(memory.Action.history, (try route("GET", "/memory/v1/spaces/team/memories/a/history")).action);
    try std.testing.expectError(error.MemoryRouteNotFound, route("POST", "/memory/v1/spaces/team/recall/extra"));
}

pub const Request = struct {
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    action: memory.Action,
    id: []const u8 = "",
    body: std.json.Value = .{ .object = .empty },
    idempotency_key: []const u8 = "",
};
