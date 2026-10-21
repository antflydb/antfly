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

//! Versioned memory contracts. No server, model runtime, or extension imports.
//! Shared resource/decision vocabulary. Traces are ordinary versioned rows;
//! neither identity nor explanation depends on a separate trace runtime.
const std = @import("std");
pub const Kind = enum { table, record, relationship, trace, evidence, assertion, entity, outcome, decision, operation, configuration };
pub const Resource = struct {
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    table: []const u8,
    table_id: u64,
    storage_name: ?[]const u8 = null,
    kind: Kind,
    id: []const u8,
    revision: u64,
    digest: ?[]const u8 = null,
    pub fn external(self: Resource) bool {
        return self.kind == .record or self.kind == .relationship or self.kind == .trace;
    }
    pub fn validate(self: Resource) !void {
        for ([_][]const u8{ self.database, self.namespace, self.table, self.id }) |part| {
            if (part.len == 0 or part.len > 256 or !std.unicode.utf8ValidateSlice(part)) return error.InvalidResourceReference;
            for (part) |ch| if (ch < 0x20 or ch == 0x7f) return error.InvalidResourceReference;
        }
        if (self.storage_name) |name| if (name.len == 0 or name.len > 256 or !std.unicode.utf8ValidateSlice(name)) return error.InvalidResourceReference;
        if (self.revision == 0 or (self.external() and self.table_id == 0)) return error.InvalidResourceReference;
        if (self.digest) |digest| {
            if (digest.len != 64) return error.InvalidResourceReference;
            for (digest) |ch| if (!std.ascii.isHex(ch)) return error.InvalidResourceReference;
        }
    }
};
pub const Relation = enum { mentions, retrieved, used, supported_by, generated, generated_by, approved_by, supersedes, applied_to, governed_by, originated_in, depends_on, motivates, rejected_in_favor_of };
pub const Link = struct { relation: Relation, target: Resource };
pub const Actor = struct {
    id: []const u8,
    kind: enum { user, agent, service } = .user,
    assurance: enum { authenticated, declared } = .declared,
};
pub const Origin = struct {
    agent: ?Actor = null,
    approver: ?Actor = null,
    trace: ?Resource = null,
    span: ?Resource = null,
    workflow: ?[]const u8 = null,
    workflow_version: ?[]const u8 = null,
    pub fn validate(self: Origin) !void {
        for ([_]?Actor{ self.agent, self.approver }) |actor| if (actor) |value| {
            if (value.id.len == 0 or value.id.len > 256 or value.assurance != .declared) return error.InvalidResourceReference;
        };
        for ([_]?Resource{ self.trace, self.span }) |resource| if (resource) |value| {
            try value.validate();
            if (!value.external()) return error.InvalidResourceReference;
        };
        for ([_]?[]const u8{ self.workflow, self.workflow_version }) |field| if (field) |value| if (value.len > 256) return error.InvalidResourceReference;
    }
};
pub const Decision = struct {
    schema_version: u32 = 1,
    resource: Resource,
    operation: Resource,
    action: []const u8,
    status: enum { proposed, approved, applied, rejected, cancelled },
    initiator: Actor,
    executor: Actor,
    origin: ?Origin = null,
    decided_at: i64,
    applied_at: ?i64,
    effective_at: ?i64 = null,
    recorded_at: i64,
    configuration: Resource,
    ontology_version: []const u8,
    policy_version: []const u8,
};
/// Admission-only selector. The service resolves it once and persists a pinned
/// Resource; selectors never replace immutable references in explanations.
pub const Selector = struct {
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    table: []const u8,
    id: []const u8,
    kind: enum { record, relationship, trace } = .record,
    expected_revision: ?u64 = null,
};
pub const ResourceState = enum { current, changed, missing, denied };
pub const max_snapshot_bytes: usize = 64 * 1024;
pub const Snapshot = struct { payload_truncated: bool = false, storage_name: ?[]const u8 = null, table_id: u64 = 0, state: ResourceState, json: []const u8 = "", revision: u64 = 0, digest: ?[]const u8 = null };
pub const ExplainRequest = struct {
    resource: ?Resource = null,
    revision: ?u64 = null,
    valid_at: ?i64 = null,
    known_at: ?i64 = null,
    max_bytes: u32 = 65536,
    max_nodes: u16 = 64,
    max_depth: u8 = 8,
};
pub const ExplainedLink = struct { relation: Relation, target: ?Resource, status: enum { available, changed, missing_or_denied, truncated } };
pub const ExplanationNode = struct {
    resource: Resource,
    record: std.json.Value,
    payload_truncated: bool = false,
    links: []const ExplainedLink,
};
pub const Explanation = struct {
    schema_version: u32 = 1,
    nodes: []const ExplanationNode,
    complete: bool,
    truncated: bool,
    missing_evidence: bool,
    incomplete_history: bool = false,
    justification: enum { recorded } = .recorded,
};

pub fn snapshot(a: std.mem.Allocator, expected: Resource, table_id: u64, revision: u64, json: []const u8, digest: ?[32]u8) !Snapshot {
    const actual = if (digest) |value| try a.dupe(u8, &std.fmt.bytesToHex(value, .lower)) else null;
    const matches = (expected.table_id == 0 or table_id == expected.table_id) and (expected.revision == 0 or revision == expected.revision) and (expected.digest == null or (actual != null and std.ascii.eqlIgnoreCase(expected.digest.?, actual.?)));
    return .{ .payload_truncated = json.len > max_snapshot_bytes, .table_id = table_id, .state = if (expected.table_id != 0 and table_id != expected.table_id) .missing else if (matches) .current else .changed, .json = if (matches and json.len <= max_snapshot_bytes) try a.dupe(u8, json) else "", .revision = revision, .digest = actual };
}

/// This envelope is source-declared metadata in an ordinary table. Only the
/// native mutation owner can attest its own authenticated decision records.
pub const Envelope = struct { schema_version: u32 = 1, links: []const Link = &.{} };
pub const TraceEvent = struct {
    schema_version: u32 = 1,
    trace_id: []const u8,
    span_id: []const u8,
    parent_span_id: ?[]const u8 = null,
    name: []const u8,
    started_at: i64,
    ended_at: ?i64 = null,
    status: enum { running, succeeded, failed, cancelled } = .running,
    provenance: Envelope = .{},
};
