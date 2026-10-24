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
            if (std.mem.eql(u8, suffix, "supersession-candidates") and std.mem.eql(u8, method, "POST")) return .{ .space = space, .id = id.?, .action = .supersession_candidates };
            if (!std.mem.eql(u8, suffix, "history") or !std.mem.eql(u8, method, "GET")) return error.MemoryRouteNotFound;
            return .{ .space = space, .id = id.?, .action = .history };
        }
        if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
        return .{ .space = space, .id = id.?, .action = .get };
    }
    if (std.mem.eql(u8, resource, "episodes")) {
        if (parts.next() != null or !std.mem.eql(u8, method, "GET")) return error.MemoryRouteNotFound;
        return .{ .space = space, .id = id orelse "", .action = if (id == null) .episodes else .episode };
    }
    if (std.mem.eql(u8, resource, "evaluation-datasets")) {
        if (id) |dataset_id| {
            if (parts.next()) |suffix| {
                if (parts.next() != null or !std.mem.eql(u8, suffix, "cases") or !std.mem.eql(u8, method, "GET")) return error.MemoryRouteNotFound;
                return .{ .space = space, .id = dataset_id, .action = .evaluation_cases };
            }
            if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
            return .{ .space = space, .id = dataset_id, .action = .evaluation_dataset };
        }
        if (!std.mem.eql(u8, method, "POST")) return error.MethodNotAllowed;
        return .{ .space = space, .action = .put_evaluation_dataset };
    }
    if (std.mem.eql(u8, resource, "evaluations")) {
        if (id) |run_id| {
            if (parts.next()) |suffix| {
                if (parts.next() != null) return error.MemoryRouteNotFound;
                if (std.mem.eql(u8, suffix, "results") and std.mem.eql(u8, method, "GET")) return .{ .space = space, .id = run_id, .action = .evaluation_results };
                if (std.mem.eql(u8, suffix, "tasks") and (std.mem.eql(u8, method, "GET") or std.mem.eql(u8, method, "POST"))) return .{ .space = space, .id = run_id, .action = .evaluation_tasks };
                if (std.mem.eql(u8, suffix, "verdicts") and std.mem.eql(u8, method, "POST")) return .{ .space = space, .id = run_id, .action = .evaluation_verdict };
                return error.MemoryRouteNotFound;
            }
            if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
            return .{ .space = space, .id = run_id, .action = .evaluation_run };
        }
        if (!std.mem.eql(u8, method, "POST")) return error.MethodNotAllowed;
        return .{ .space = space, .action = .start_evaluation };
    }
    if (std.mem.eql(u8, resource, "supersession-proposals")) {
        if (id) |proposal_id| {
            if (parts.next()) |suffix| {
                if (parts.next() != null or !std.mem.eql(u8, suffix, "verdict") or !std.mem.eql(u8, method, "POST")) return error.MemoryRouteNotFound;
                return .{ .space = space, .id = proposal_id, .action = .review_supersession };
            }
            if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
            return .{ .space = space, .id = proposal_id, .action = .supersession_proposal };
        }
        return .{ .space = space, .action = if (std.mem.eql(u8, method, "GET")) .supersession_proposals else if (std.mem.eql(u8, method, "POST")) .propose_supersession else return error.MethodNotAllowed };
    }
    if (std.mem.eql(u8, resource, "relationship-proposals")) {
        if (id) |proposal_id| {
            if (parts.next()) |suffix| {
                if (parts.next() != null or !std.mem.eql(u8, suffix, "verdict") or !std.mem.eql(u8, method, "POST")) return error.MemoryRouteNotFound;
                return .{ .space = space, .id = proposal_id, .action = .review_relationship };
            }
            if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
            return .{ .space = space, .id = proposal_id, .action = .relationship_proposal };
        }
        if (!std.mem.eql(u8, method, "POST")) return error.MethodNotAllowed;
        return .{ .space = space, .action = .propose_relationship };
    }
    if (std.mem.eql(u8, resource, "entity-proposals")) {
        if (id) |proposal_id| {
            if (parts.next()) |suffix| {
                if (parts.next() != null or !std.mem.eql(u8, suffix, "verdict") or !std.mem.eql(u8, method, "POST")) return error.MemoryRouteNotFound;
                return .{ .space = space, .id = proposal_id, .action = .review_entity };
            }
            if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
            return .{ .space = space, .id = proposal_id, .action = .entity_proposal };
        }
        return .{ .space = space, .action = if (std.mem.eql(u8, method, "GET")) .entity_proposals else if (std.mem.eql(u8, method, "POST")) .propose_entity else return error.MethodNotAllowed };
    }
    if (std.mem.eql(u8, resource, "entities") and id != null) {
        if (parts.next()) |suffix| {
            if (parts.next() != null) return error.MemoryRouteNotFound;
            if (std.mem.eql(u8, suffix, "view") and std.mem.eql(u8, method, "POST")) return .{ .space = space, .id = id.?, .action = .entity_view };
            if (std.mem.eql(u8, suffix, "candidates") and std.mem.eql(u8, method, "POST")) return .{ .space = space, .id = id.?, .action = .entity_candidates };
            return error.MemoryRouteNotFound;
        }
        if (!std.mem.eql(u8, method, "GET")) return error.MethodNotAllowed;
        return .{ .space = space, .id = id.?, .action = .resolve_entity };
    }
    if (std.mem.eql(u8, resource, "operations")) {
        if (id == null or parts.next() != null) return error.MemoryRouteNotFound;
        return .{ .space = space, .id = id.?, .action = if (std.mem.eql(u8, method, "GET")) .operation else if (std.mem.eql(u8, method, "POST")) .advance else return error.MethodNotAllowed };
    }
    if (std.mem.eql(u8, resource, "configurations") and id != null and parts.next() == null and std.mem.eql(u8, method, "GET")) return .{ .space = space, .action = .configuration, .id = id.? };
    if (std.mem.eql(u8, resource, "evidence") and id != null and parts.next() == null and std.mem.eql(u8, method, "GET")) return .{ .space = space, .action = .evidence, .id = id.? };
    if (std.mem.eql(u8, resource, "lineage") and id == null) return .{ .space = space, .action = if (std.mem.eql(u8, method, "GET")) .lineage else return error.MethodNotAllowed };
    if (id == null and std.mem.eql(u8, method, "POST")) {
        inline for (.{ .{ "forks", memory.Action.fork_lineage }, .{ "checkpoints", memory.Action.checkpoint_lineage }, .{ "reverts", memory.Action.revert_lineage } }) |entry| if (std.mem.eql(u8, resource, entry[0])) return .{ .space = space, .action = entry[1] };
    }
    if (std.mem.eql(u8, resource, "changes") and id == null and (std.mem.eql(u8, method, "GET") or std.mem.eql(u8, method, "POST"))) return .{ .space = space, .action = .changes };
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
        error.MemorySourceAccessDenied, error.Forbidden, error.MemoryVisibilityConflict, error.MemoryRowPolicyUnsupported => 403,
        error.MethodNotAllowed => 405,
        error.MemoryRevisionConflict, error.MemorySpaceBindingChanged, error.MemoryCursorStale, error.MemorySpaceExists, error.MemoryIdempotencyConflict, error.MemoryEvidenceConflict, error.MemoryAssertionConflict, error.MemoryOutcomeConflict, error.MemoryProducerConflict, error.MemoryEntityConflict, error.MemoryConfigurationTransition, error.MemoryForgotten, error.MemoryConfigurationChanged, error.MemoryResourceChanged, error.ResolverCurationConflict => 409,
        error.MemoryInputTooLarge, error.MemoryWorkLimit, error.ResolverCurationLimitExceeded => 413,
        error.MemoryModelRequired, error.MemoryCapabilityUnavailable, error.MemoryExecutionIdentityRequired, error.MemoryModelRevisionUnavailable, error.MemoryModelRevisionMismatch => 422,
        error.MemoryRetryNotReady, error.MemoryRetryExhausted, error.MemoryOverloaded => 429,
        error.MemoryCommitPending, error.NotLeader, error.Unavailable, error.StorageUnavailable, error.GroupLeaderUnavailable => 503,
        error.Canceled => 408,
        error.Timeout, error.DeadlineExceeded => 504,
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
    branch: []const u8 = "",
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    action: memory.Action,
    id: []const u8 = "",
    body: std.json.Value = .{ .object = .empty },
    idempotency_key: []const u8 = "",
};
