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

//! Immutable policy identity and optimistic catalog mutation. Persist the
//! returned catalog through the metadata leader's existing definition CAS;
//! computing a replacement never schedules work or mutates a local replica.
const std = @import("std");
pub const contract = @import("contract.zig");
const projection = @import("document.zig");
const A = std.mem.Allocator;
pub const key = "_privacy";
pub const Scan = struct {
    scan_id: [64]u8,
    policy_version: [64]u8,
    phase: enum { queued, running, processing, succeeded, failed, superseded } = .queued,
    from_key: []const u8 = "",
    to_key: []const u8 = "",
    limit: u32 = 100,
    next_key: ?[]const u8 = null,
    shard_cursors: std.json.Value = .null,
    scanned: u64 = 0,
    reprocessed: u64 = 0,
    skipped: u64 = 0,
    failed: u64 = 0,
    failures: std.json.Value = .null,
    last_error: ?[]const u8 = null,
    consecutive_errors: u8 = 0,
    retry_after_millis: i64 = 0,
    updated_at_millis: i64 = 0,
    verification_started_at_millis: i64 = 0,
    verification_page_size: u32 = 100,
    verification_cursor: ?[]const u8 = null,
    verification_current: u64 = 0,
    verification_pending: u64 = 0,
    verification_failed: u64 = 0,
    verification_blocked: u64 = 0,

    pub fn jsonStringify(self: Scan, writer: anytype) !void {
        try writeResource(self, writer);
    }
};
pub const coverage = @import("coverage.zig");
pub const Resource = struct {
    checkpoint_encoding: u8 = 0,
    coverage: coverage.State = .{},
    policy: contract.Policy,
    artifact_name: []const u8 = projection.artifact_name,
    policy_version: [64]u8,
    resource_version: u64,
    generation: u64,
    scan_id: [64]u8,
    scan_sequence: u64 = 0,
    scans: []const Scan = &.{},
    unprotected_indexes_json: []const u8 = "",

    pub fn jsonStringify(self: Resource, writer: anytype) !void {
        try writeResource(self, writer);
    }
};

fn writeResource(value: anytype, writer: anytype) !void {
    try writer.beginObject();
    inline for (comptime std.meta.fieldNames(@TypeOf(value))) |name| {
        try writer.objectField(name);
        if (comptime std.mem.eql(u8, name, "policy_version") or std.mem.eql(u8, name, "scan_id")) {
            try writer.write(@field(value, name)[0..]);
        } else try writer.write(@field(value, name));
    }
    try writer.endObject();
}

/// Immutable, generation-qualified key blobs use the existing term-fenced job
/// state store. Only references enter the table catalog or its configuration CAS.
pub const checkpoints = struct {
    pub const prefix = "pk1:";
    pub const chunk_bytes = 256 * 1024;
    pub const max_bytes = 65 * 1024 * 1024;
    pub const max_pending_bytes = 256 * 1024 * 1024;
    pub const gc_chunks = 8;
    pub const lease_ms = std.time.ms_per_hour;
    pub const erase = "\x00";
    pub const Header = struct {
        state: enum { staged, ready, deleting } = .staged,
        length: u32,
        created_at_millis: i64,
        attached: bool = false,
        deleted_chunks: u32 = 0,
        pub fn chunks(self: @This()) usize {
            return std.math.divCeil(usize, self.length, chunk_bytes) catch unreachable;
        }
    };
    pub const Ref = struct {
        digest: [64]u8,
        generation: [32]u8,
        pub fn token(self: @This()) [101]u8 {
            var out: [101]u8 = undefined;
            @memcpy(out[0..4], prefix);
            @memcpy(out[4..68], &self.digest);
            out[68] = ':';
            @memcpy(out[69..], &self.generation);
            return out;
        }
        pub fn storageKey(self: @This(), buffer: []u8, table: u64, chunk: ?usize) ![]const u8 {
            const base = try std.fmt.bufPrint(buffer, "privacy/{s}/{x:0>16}/{s}{s}", .{ if (chunk == null) "m" else "c", table, self.digest, self.generation });
            if (chunk) |index| {
                const suffix = try std.fmt.bufPrint(buffer[base.len..], "/{x:0>3}", .{index});
                return buffer[0 .. base.len + suffix.len];
            }
            return base;
        }
    };
    pub fn reference(value: []const u8) ?Ref {
        if (value.len != 101 or !std.mem.startsWith(u8, value, prefix) or value[68] != ':') return null;
        for (value[4..68]) |byte| if (!std.ascii.isHex(byte)) return null;
        for (value[69..]) |byte| if (!std.ascii.isHex(byte)) return null;
        return .{ .digest = value[4..68].*, .generation = value[69..101].* };
    }
    pub const Key = struct { table: u64, kind: enum { manifest, chunk, gc, quota }, ref: ?Ref = null, index: usize = 0 };
    pub fn parseKey(value: []const u8) !Key {
        var parts = std.mem.splitScalar(u8, value, '/');
        if (!std.mem.eql(u8, parts.next() orelse "", "privacy")) return error.InvalidDocumentCheckpoint;
        const kind = parts.next() orelse return error.InvalidDocumentCheckpoint;
        if (std.mem.eql(u8, kind, "g")) {
            if (parts.next() != null) return error.InvalidDocumentCheckpoint;
            return .{ .table = 0, .kind = .gc };
        }
        const table = parts.next() orelse return error.InvalidDocumentCheckpoint;
        if (table.len != 16) return error.InvalidDocumentCheckpoint;
        var result: Key = .{ .table = std.fmt.parseUnsigned(u64, table, 16) catch return error.InvalidDocumentCheckpoint, .kind = undefined };
        if (std.mem.eql(u8, kind, "q")) {
            result.kind = .quota;
        } else {
            result.kind = if (std.mem.eql(u8, kind, "m")) .manifest else if (std.mem.eql(u8, kind, "c")) .chunk else return error.InvalidDocumentCheckpoint;
            const id = parts.next() orelse return error.InvalidDocumentCheckpoint;
            if (id.len != 96) return error.InvalidDocumentCheckpoint;
            for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidDocumentCheckpoint;
            result.ref = .{ .digest = id[0..64].*, .generation = id[64..96].* };
            if (result.kind == .chunk) {
                const index = parts.next() orelse return error.InvalidDocumentCheckpoint;
                if (index.len != 3) return error.InvalidDocumentCheckpoint;
                result.index = std.fmt.parseUnsigned(usize, index, 16) catch return error.InvalidDocumentCheckpoint;
            }
        }
        if (parts.next() != null) return error.InvalidDocumentCheckpoint;
        return result;
    }
    pub fn refs(a: A, resource: Resource) ![]Ref {
        var result: std.ArrayList(Ref) = .empty;
        if (resource.checkpoint_encoding == 0) return result.toOwnedSlice(a);
        if (resource.checkpoint_encoding != 1) return error.InvalidPrivacyCatalog;
        if (resource.coverage.cursor) |value| if (reference(value)) |ref| try result.append(a, ref);
        for (resource.scans) |scan| {
            for ([_][]const u8{ scan.from_key, scan.to_key, scan.next_key orelse "", scan.verification_cursor orelse "" }) |value| {
                if (reference(value)) |ref| try result.append(a, ref);
            }
            if (scan.shard_cursors == .array) for (scan.shard_cursors.array.items) |row| {
                if (row == .object) if (row.object.get("next_key")) |value| if (value == .string) {
                    if (reference(value.string)) |ref| try result.append(a, ref);
                };
            };
        }
        return result.toOwnedSlice(a);
    }
};

/// Scheduling hints are derived from durable job state, never serving evidence.
/// Skip unrelated catalog definitions without constructing their JSON trees.
pub fn workDue(a: A, raw: []const u8) !?i64 {
    const Hint = struct { updated_at_millis: i64 = 0, retry_after_millis: i64 = 0, phase: []const u8 = "" };
    const Root = struct { _privacy: ?struct { coverage: Hint = .{}, scans: []const Hint = &.{} } = null };
    var parsed = try std.json.parseFromSlice(Root, a, if (raw.len == 0) "{}" else raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const resource = parsed.value._privacy orelse return null;
    var due = @max(resource.coverage.updated_at_millis, resource.coverage.retry_after_millis);
    for (resource.scans) |scan| {
        if (std.mem.eql(u8, scan.phase, "queued") or std.mem.eql(u8, scan.phase, "running") or std.mem.eql(u8, scan.phase, "processing"))
            due = @min(due, @max(scan.updated_at_millis, scan.retry_after_millis));
    }
    return @max(0, due);
}

/// Standalone's volatile index is rebuilt by normal catalog replay. Each
/// table owns exactly one node; updates/removal and due selection are O(log N).
pub const WorkIndex = struct {
    const Tree = std.Treap(u128, std.math.order);
    tree: Tree = .{},
    nodes: std.AutoHashMapUnmanaged(u64, *Tree.Node) = .empty,
    pub fn deinit(self: *WorkIndex, a: A) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |node| a.destroy(node.*);
        self.nodes.deinit(a);
        self.* = .{};
    }
    pub fn update(self: *WorkIndex, a: A, id: u64, due: ?i64) !void {
        var node = self.nodes.get(id);
        if (node == null and due != null) {
            try self.nodes.ensureUnusedCapacity(a, 1);
            node = try a.create(Tree.Node);
            self.nodes.putAssumeCapacity(id, node.?);
        } else if (node) |existing| {
            var old = self.tree.getEntryForExisting(existing);
            old.set(null);
        }
        if (due) |time| {
            var entry = self.tree.getEntryFor((@as(u128, @intCast(@max(0, time))) << 64) | id);
            entry.set(node.?);
        } else if (node) |old| {
            _ = self.nodes.remove(id);
            a.destroy(old);
        }
    }
    pub fn dueTime(self: *const WorkIndex, id: u64) ?i64 {
        const node = self.nodes.get(id) orelse return null;
        return @intCast(node.key >> 64);
    }
    pub fn first(self: *WorkIndex, now: i64) ?u64 {
        const node = self.tree.getMin() orelse return null;
        if (node.key >> 64 > @as(u64, @intCast(@max(0, now)))) return null;
        return @truncate(node.key);
    }
};

pub fn startScan(a: A, resource: *Resource, from_key: []const u8, to_key: []const u8, limit: u32) !Scan {
    if (limit == 0 or limit > 1000 or (to_key.len != 0 and std.mem.order(u8, from_key, to_key) == .gt)) return error.InvalidPrivacyScan;
    // Acknowledged active jobs must survive later admissions. Bound history by
    // retiring only terminal (or superseded-policy) jobs; backpressure otherwise.
    var evict: ?usize = null;
    if (resource.scans.len >= 16) {
        for (resource.scans, 0..) |prior, i| {
            if (prior.phase == .succeeded or prior.phase == .failed or prior.phase == .superseded or
                !std.mem.eql(u8, &prior.policy_version, &resource.policy_version))
            {
                evict = i;
                break;
            }
        }
        if (evict == null) return error.PrivacyScanLimitExceeded;
    }
    const sequence = try std.math.add(u64, resource.scan_sequence, 1);
    const raw = try std.fmt.allocPrint(a, "{s}:{d}:{d}", .{ resource.policy_version, resource.generation, sequence });
    defer a.free(raw);
    const scan = Scan{ .scan_id = contract.fingerprint(raw), .policy_version = resource.policy_version, .from_key = from_key, .to_key = to_key, .limit = limit };
    const retained: usize = resource.scans.len - @intFromBool(evict != null);
    const scans = try a.alloc(Scan, retained + 1);
    var n: usize = 0;
    for (resource.scans, 0..) |prior, i| {
        if (evict == i) continue;
        scans[n] = prior;
        n += 1;
    }
    scans[retained] = scan;
    resource.scans = scans;
    resource.scan_id = scan.scan_id;
    resource.scan_sequence = sequence;
    return scan;
}

pub fn replaceResource(a: A, indexes_json: []const u8, resource: Resource) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var root = (try std.json.parseFromSlice(std.json.Value, scratch, indexes_json, .{})).value;
    if (root != .object) return error.InvalidPrivacyCatalog;
    const raw = try std.json.Stringify.valueAlloc(scratch, resource, .{});
    const value = (try std.json.parseFromSlice(std.json.Value, scratch, raw, .{})).value;
    try root.object.put(scratch, key, value);
    return std.json.Stringify.valueAlloc(a, root, .{});
}

/// A progress CAS may change only these runtime fields. Compare the remaining
/// JSON, including unknown fields, so future policy/index fields fail closed.
/// Keeping this allowlist here gives every metadata topology the same boundary.
pub fn progressOnly(a: A, before: []const u8, after: []const u8) !bool {
    if (before.len == 0 or after.len == 0) return false;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const lhs = try std.json.parseFromSliceLeaky(std.json.Value, scratch, before, .{});
    const rhs = try std.json.parseFromSliceLeaky(std.json.Value, scratch, after, .{});
    if (lhs != .object or rhs != .object) return false;
    const old = lhs.object.get(key) orelse return false;
    const next_value = rhs.object.get(key) orelse return false;
    if (old != .object or next_value != .object) return false;
    // Require a valid, unchanged policy binding before granting this path.
    var prior = (try current(scratch, lhs)) orelse return false;
    defer prior.deinit();
    var next_resource = (try current(scratch, rhs)) orelse return false;
    defer next_resource.deinit();
    for ([_]*std.json.Value{ lhs.object.getPtr(key).?, rhs.object.getPtr(key).? }) |value| {
        inline for (.{ "coverage", "scans", "scan_id", "scan_sequence", "checkpoint_encoding" }) |name| {
            _ = value.object.swapRemove(name);
        }
    }
    return std.mem.eql(u8, try std.json.Stringify.valueAlloc(scratch, lhs, .{}), try std.json.Stringify.valueAlloc(scratch, rhs, .{}));
}

/// Reconciliation is not an owner of privacy configuration or progress. Retain
/// the durable resource when a cached definition carries its older runtime cut;
/// reject stale policy bindings, including an attempted resource removal.
pub fn reconcileIndexes(a: A, durable: []const u8, desired: []const u8) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, if (durable.len == 0) "{}" else durable, .{});
    if (old != .object or !old.object.contains(key)) return try a.dupe(u8, desired);
    var next_value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, if (desired.len == 0) "{}" else desired, .{});
    if (next_value != .object or !next_value.object.contains(key)) return null;
    const saved = try std.json.Stringify.valueAlloc(scratch, old.object.get(key).?, .{});
    for ([_]*std.json.Value{ old.object.getPtr(key).?, next_value.object.getPtr(key).? }) |value| {
        if (value.* != .object) return null;
        inline for (.{ "coverage", "scans", "scan_id", "scan_sequence", "checkpoint_encoding" }) |name| _ = value.object.swapRemove(name);
    }
    if (!std.mem.eql(u8, try std.json.Stringify.valueAlloc(scratch, old.object.get(key).?, .{}), try std.json.Stringify.valueAlloc(scratch, next_value.object.get(key).?, .{}))) return null;
    try next_value.object.put(scratch, key, try std.json.parseFromSliceLeaky(std.json.Value, scratch, saved, .{}));
    return try std.json.Stringify.valueAlloc(a, next_value, .{});
}

pub fn resourceVersion(a: A, raw: []const u8) !?u64 {
    const Root = struct { _privacy: ?struct { resource_version: u64 } = null };
    var parsed = try std.json.parseFromSlice(Root, a, if (raw.len == 0) "{}" else raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return if (parsed.value._privacy) |resource| resource.resource_version else null;
}

pub fn current(a: A, indexes: std.json.Value) !?std.json.Parsed(Resource) {
    if (indexes != .object) return error.InvalidPrivacyCatalog;
    const entry = indexes.object.get(key) orelse return null;
    // std.json.Value.jsonParseFromValue borrows its object/array storage even
    // with alloc_always. The resource outlives the caller's metadata DOM.
    const encoded = try std.json.Stringify.valueAlloc(a, entry, .{});
    defer a.free(encoded);
    const parsed = try std.json.parseFromSlice(Resource, a, encoded, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.resource_version == 0 or parsed.value.generation == 0 or
        !std.mem.eql(u8, &parsed.value.policy_version, &(try parsed.value.policy.identity(a)))) return error.InvalidPrivacyCatalog;
    const expected_artifact = if (parsed.value.policy.mode == .redact_for_search_and_ai) try projection.protectedArtifactName(a, parsed.value.policy_version) else try a.dupe(u8, projection.artifact_name);
    defer a.free(expected_artifact);
    if (!std.mem.eql(u8, expected_artifact, parsed.value.artifact_name)) return error.InvalidPrivacyCatalog;
    return parsed;
}

pub fn etag(a: A, resource: Resource) ![]u8 {
    return std.fmt.allocPrint(a, "\"privacy-{d}-{s}\"", .{ resource.resource_version, resource.policy_version });
}

pub fn next(a: A, before: ?Resource, policy: contract.Policy, if_match: ?[]const u8) !Resource {
    try policy.validate();
    if (before) |prior| {
        const expected = try etag(a, prior);
        defer a.free(expected);
        const supplied = if_match orelse return error.PrivacyPreconditionRequired;
        if (!std.mem.eql(u8, supplied, expected)) return error.PrivacyPreconditionFailed;
    } else if (if_match != null) return error.PrivacyPreconditionFailed;
    const version = try policy.identity(a);
    const changed = if (before) |prior| !std.mem.eql(u8, &version, &prior.policy_version) else true;
    const generation = if (before) |prior| try std.math.add(u64, prior.generation, @intFromBool(changed)) else 1;
    const scan_id = if (before != null and !changed) before.?.scan_id else scan: {
        const raw = try std.fmt.allocPrint(a, "{s}:{d}", .{ version, generation });
        defer a.free(raw);
        break :scan contract.fingerprint(raw);
    };
    return .{ .checkpoint_encoding = if (before) |prior| prior.checkpoint_encoding else 0, .coverage = if (!changed) before.?.coverage else .{}, .policy = policy, .artifact_name = if (policy.mode == .redact_for_search_and_ai) try projection.protectedArtifactName(a, version) else projection.artifact_name, .policy_version = version, .resource_version = if (before) |prior| try std.math.add(u64, prior.resource_version, 1) else 1, .generation = generation, .scan_id = scan_id, .scan_sequence = if (before) |prior| prior.scan_sequence else 0, .scans = if (before) |prior| prior.scans else &.{}, .unprotected_indexes_json = if (before) |prior| prior.unprotected_indexes_json else "" };
}

/// Preserve ordinary catalog entries and replace exactly the managed producer.
/// Protected index rewrites must be composed in this SAME metadata mutation;
/// a caller must never install protection in a later, separate update.
pub fn install(a: A, indexes_json: []const u8, resource: Resource) ![]u8 {
    // The server admits fleet capabilities before this atomic policy/consumer
    // replacement. Pre-storage needs an independent synchronous ingestion path.
    switch (resource.policy.mode) {
        .monitor, .redact_for_search_and_ai => {},
        .redact_before_storage => return error.PrivacySynchronousIngestionRequired,
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, scratch, if (resource.unprotected_indexes_json.len != 0) resource.unprotected_indexes_json else indexes_json, .{});
    if (parsed.value != .object) return error.InvalidPrivacyCatalog;
    var root = parsed.value;
    _ = root.object.swapRemove(key);
    // Save a clean logical catalog once, so policy replacement never nests
    // another protected catalog and disabling redaction can rebuild originals.
    if (root.object.getPtr("enrichments")) |definitions| {
        if (definitions.* != .array) return error.InvalidPrivacyCatalog;
        var n: usize = 0;
        while (n < definitions.array.items.len) {
            const entry = definitions.array.items[n];
            const name = if (entry == .object) entry.object.get("name") else null;
            if (name != null and name.? == .string and projection.isManagedArtifactName(name.?.string)) {
                _ = definitions.array.orderedRemove(n);
            } else n += 1;
        }
    }
    var persisted = resource;
    persisted.unprotected_indexes_json = if (resource.policy.mode == .redact_for_search_and_ai) try std.json.Stringify.valueAlloc(scratch, root, .{}) else "";
    if (resource.policy.mode == .redact_for_search_and_ai) try protect(scratch, &root, resource.policy_version);
    const managed_policy = try resource.policy.executable(scratch);
    const producer = try projection.resolve(scratch, managed_policy, true);
    const config = try std.json.Stringify.valueAlloc(scratch, .{ .type = producer.type, .config = producer.config }, .{});
    const enrichment_raw = try std.json.Stringify.valueAlloc(scratch, .{
        .name = resource.artifact_name,
        .kind = "asset",
        .field = projection.source_field,
        .content_type = "application/json",
        .producer_json = config,
        .execution = .{ .batch_items = 8 },
    }, .{});
    const enrichment = (try std.json.parseFromSlice(std.json.Value, scratch, enrichment_raw, .{})).value;
    var entries = root.object.get("enrichments") orelse std.json.Value{ .array = std.array_list.Managed(std.json.Value).init(scratch) };
    if (entries != .array) return error.InvalidPrivacyCatalog;
    var i: usize = 0;
    while (i < entries.array.items.len) {
        const entry = entries.array.items[i];
        if (entry != .object) return error.InvalidPrivacyCatalog;
        const name = entry.object.get("name") orelse return error.InvalidPrivacyCatalog;
        if (name == .string and projection.isManagedArtifactName(name.string)) {
            _ = entries.array.orderedRemove(i);
        } else i += 1;
    }
    const state_raw = try std.json.Stringify.valueAlloc(scratch, .{ .name = projection.state_artifact_name, .kind = "asset", .field = projection.state_field, .content_type = "application/json", .producer_json = config }, .{});
    const state = (try std.json.parseFromSlice(std.json.Value, scratch, state_raw, .{})).value;
    try entries.array.append(state);
    try entries.array.append(enrichment);
    try root.object.put(scratch, "enrichments", entries);
    const resource_raw = try std.json.Stringify.valueAlloc(scratch, persisted, .{});
    const stored = (try std.json.parseFromSlice(std.json.Value, scratch, resource_raw, .{})).value;
    try root.object.put(scratch, key, stored);
    return std.json.Stringify.valueAlloc(a, root, .{});
}

test "privacy policy updates require exact CAS and execution tuning does not rescan" {
    const a = std.testing.allocator;
    var policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"} };
    const first = try next(a, null, policy, null);
    try std.testing.expectEqual(@as(u64, 1), first.generation);
    try std.testing.expectError(error.PrivacyPreconditionRequired, next(a, first, policy, null));
    try std.testing.expectError(error.PrivacyPreconditionFailed, next(a, first, policy, "*"));
    const tag = try etag(a, first);
    defer a.free(tag);
    policy.execution.batch_items = 1;
    const tuned = try next(a, first, policy, tag);
    try std.testing.expectEqual(first.scan_id, tuned.scan_id);
    try std.testing.expectEqual(first.generation, tuned.generation);
    try std.testing.expectEqual(@as(u64, 2), tuned.resource_version);
    policy.sensitivity = .thorough;
    const changed = try next(a, first, policy, tag);
    try std.testing.expectEqual(@as(u64, 2), changed.generation);
    try std.testing.expect(!std.mem.eql(u8, &first.scan_id, &changed.scan_id));
}

test "privacy installation replaces consumers and guard atomically and restores the logical catalog" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = try next(a, null, .{ .mode = .redact_for_search_and_ai, .fields = &.{"body"} }, null);
    const installed = try install(a, "{\"full_text_index_v0\":{}}", first);
    const root = (try std.json.parseFromSlice(std.json.Value, a, installed, .{})).value;
    const resource = (try current(a, root)).?.value;
    try std.testing.expect(resource.unprotected_indexes_json.len != 0);
    try std.testing.expectEqualStrings(first.artifact_name, getString(root.object.get("full_text_index_v0").?.object.get("sources").?.array.items[0], "artifact"));
    const declarations = root.object.get("enrichments").?.array.items;
    try std.testing.expectEqualStrings(projection.state_artifact_name, getString(declarations[0], "name"));
    try std.testing.expectEqualStrings(first.artifact_name, getString(declarations[1], "name"));
    const monitor = try next(a, resource, .{ .mode = .monitor, .fields = &.{"body"} }, try etag(a, resource));
    const restored = (try std.json.parseFromSlice(std.json.Value, a, try install(a, installed, monitor), .{})).value;
    try std.testing.expect(!restored.object.get("full_text_index_v0").?.object.contains("sources"));
    try std.testing.expectEqualStrings(projection.artifact_name, getString(restored.object.get("enrichments").?.array.items[1], "name"));
}

test "privacy resources own custom producer JSON after metadata parsing is released" {
    const a = std.testing.allocator;
    var root = try std.json.parseFromSlice(std.json.Value, a,
        \\{"mode":"monitor","fields":["body"],"producer":{"type":"extractor","revision":"custom-v1","config":{"provider":"antfly","model":"fixture"}}}
    , .{});
    var policy = try std.json.parseFromValue(contract.Policy, a, root.value, .{});
    const first = try next(a, null, policy.value, null);
    const installed = try install(a, "{}", first);
    defer a.free(installed);
    policy.deinit();
    root.deinit();
    var metadata = try std.json.parseFromSlice(std.json.Value, a, installed, .{});
    var resource = (try current(a, metadata.value)).?;
    defer resource.deinit();
    metadata.deinit();
    const encoded = try std.json.Stringify.valueAlloc(a, resource.value.policy.producer.?.config, .{});
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "fixture") != null);
}

test "privacy catalog preserves user indexes and freezes producer configuration" {
    const a = std.testing.allocator;
    const policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"} };
    const first = try next(a, null, policy, null);
    const raw = try install(a, "{\"search\":{\"type\":\"full_text\"}}", first);
    defer a.free(raw);
    var tree = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer tree.deinit();
    var restored = (try current(a, tree.value)).?;
    defer restored.deinit();
    try std.testing.expectEqual(first.policy_version, restored.value.policy_version);
    try std.testing.expect(tree.value.object.contains("search"));
    const again = try install(a, raw, first);
    defer a.free(again);
    var replaced = try std.json.parseFromSlice(std.json.Value, a, again, .{});
    defer replaced.deinit();
    try std.testing.expectEqual(@as(usize, 2), replaced.value.object.get("enrichments").?.array.items.len);
}

test "privacy catalog rejects asynchronous substitution for pre-storage redaction" {
    const a = std.testing.allocator;
    var policy = contract.Policy{ .mode = .redact_for_search_and_ai, .fields = &.{"body"} };
    policy.mode = .redact_before_storage;
    try std.testing.expectError(error.PrivacySynchronousIngestionRequired, install(a, "{}", try next(a, null, policy, null)));
}

pub fn managedName(a: A, version: [64]u8, name: []const u8) ![]u8 {
    const digest = contract.fingerprint(name);
    return std.fmt.allocPrint(a, "_privacy_{s}_{s}", .{ version, digest[0..] });
}

fn getString(value: std.json.Value, name: []const u8) []const u8 {
    if (value != .object) return "";
    const field = value.object.get(name) orelse return "";
    return if (field == .string) field.string else "";
}
fn string(a: A, object: *std.json.Value, name: []const u8, value: []const u8) !void {
    try object.object.put(a, name, .{ .string = value });
}

fn protect(a: A, root: *std.json.Value, version: [64]u8) !void {
    const root_artifact = try projection.protectedArtifactName(a, version);
    var names: std.StringHashMapUnmanaged([]const u8) = .empty;
    var additions: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
    // Discover named streams before rewriting any dependency. Never recurse
    // into model configuration, instructions or user-authored schema objects.
    try collectNames(a, root.*, version, &names);
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        const value = entry.value_ptr;
        if (value.* != .object) continue;
        var kind = getString(value.*, "type");
        if (kind.len == 0 and std.mem.startsWith(u8, entry.key_ptr.*, "full_text_index_v")) {
            try string(a, value, "type", "full_text");
            kind = "full_text";
        }
        if (kind.len == 0) return error.PrivacyUnsupportedIndexType;
        if (!std.mem.eql(u8, kind, "full_text") and !std.mem.eql(u8, kind, "embeddings")) return error.PrivacyUnsupportedIndexType;
        if (std.mem.eql(u8, kind, "embeddings") and value.object.get("sources") == null) {
            const external_value = value.object.get("external") orelse .null;
            if (external_value == .bool and external_value.bool) return error.PrivacyExternalEmbeddingsUnsupported;
            const declared = getString(value.*, "embedding_name");
            if (declared.len != 0) {
                const mapped = names.get(declared) orelse return error.PrivacyUnknownArtifactSource;
                var sources: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
                var source: std.json.Value = .{ .object = .empty };
                try string(a, &source, "artifact", mapped);
                try sources.array.append(source);
                try value.object.put(a, "sources", sources);
                inline for (.{ "field", "template", "chunker", "chunk_size", "embedding_name", "source_artifact_name", "embedder", "external", "generator" }) |field| _ = value.object.swapRemove(field);
                try string(a, value, "coverage_policy", "strict");
                continue;
            }
            const embedder = value.object.get("embedder") orelse return error.PrivacyEmbeddingProducerRequired;
            const logical = entry.key_ptr.*;
            const embedding_name = try managedName(a, version, logical);
            const chunk_name = try std.fmt.allocPrint(a, "{s}_chunks", .{embedding_name});
            var chunk: std.json.Value = .{ .object = .empty };
            try string(a, &chunk, "name", chunk_name);
            try string(a, &chunk, "kind", "chunk");
            try string(a, &chunk, "source_artifact_name", root_artifact);
            try string(a, &chunk, "field", getString(value.*, "field"));
            try string(a, &chunk, "template", getString(value.*, "template"));
            try chunk.object.put(a, "chunk_size", value.object.get("chunk_size") orelse .{ .integer = 1024 });
            if (value.object.get("chunker")) |chunker| try string(a, &chunk, "chunker_json", try std.json.Stringify.valueAlloc(a, chunker, .{}));
            var embedding: std.json.Value = .{ .object = .empty };
            try string(a, &embedding, "name", embedding_name);
            try string(a, &embedding, "kind", "embedding");
            try string(a, &embedding, "field", "text");
            try string(a, &embedding, "source_artifact_name", chunk_name);
            try string(a, &embedding, "producer_json", try std.json.Stringify.valueAlloc(a, embedder, .{}));
            if (value.object.get("dimension")) |dims| try embedding.object.put(a, "expected_dims", dims);
            try additions.array.append(chunk);
            try additions.array.append(embedding);
            var sources: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
            var source: std.json.Value = .{ .object = .empty };
            try string(a, &source, "artifact", embedding_name);
            try sources.array.append(source);
            try value.object.put(a, "sources", sources);
            inline for (.{ "field", "template", "chunker", "chunk_size", "embedding_name", "source_artifact_name", "embedder", "external", "generator" }) |field| _ = value.object.swapRemove(field);
            try string(a, value, "coverage_policy", "strict");
        } else if (value.object.getPtr("sources")) |sources| {
            if (sources.* != .array) return error.InvalidPrivacyCatalog;
            for (sources.array.items) |*source| {
                const original = getString(source.*, "artifact");
                const mapped = if (std.mem.eql(u8, original, root_artifact)) original else names.get(original) orelse return error.PrivacyUnknownArtifactSource;
                try string(a, source, "artifact", mapped);
            }
            if (std.mem.eql(u8, kind, "embeddings")) try string(a, value, "coverage_policy", "strict");
        } else {
            var sources: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
            var source: std.json.Value = .{ .object = .empty };
            try string(a, &source, "artifact", root_artifact);
            try sources.array.append(source);
            try value.object.put(a, "sources", sources);
        }
    }
    // The implicit default index must stop accepting primary-document text too.
    if (!root.object.contains("full_text_index_v0")) {
        const raw = try std.json.Stringify.valueAlloc(a, .{ .type = "full_text", .sources = .{.{ .artifact = root_artifact }} }, .{});
        try root.object.put(a, "full_text_index_v0", (try std.json.parseFromSlice(std.json.Value, a, raw, .{})).value);
    }
    try rewriteEnrichments(a, root, &names, version, &additions);
    var existing = root.object.get("enrichments") orelse std.json.Value{ .array = std.array_list.Managed(std.json.Value).init(a) };
    try existing.array.appendSlice(additions.array.items);
    try root.object.put(a, "enrichments", existing);
}

fn collectNames(a: A, root: std.json.Value, version: [64]u8, names: *std.StringHashMapUnmanaged([]const u8)) !void {
    if (root != .object) return;
    if (root.object.get("enrichments")) |entries| {
        if (entries != .array) return error.InvalidPrivacyCatalog;
        for (entries.array.items) |entry| {
            const name = getString(entry, "name");
            if (name.len == 0 or std.mem.startsWith(u8, name, "_privacy_")) return error.InvalidPrivacyCatalog;
            if (!names.contains(name)) try names.put(a, name, try managedName(a, version, name));
        }
    }
    var it = root.object.iterator();
    while (it.next()) |entry| if (entry.value_ptr.* == .object and entry.value_ptr.object.contains("type")) try collectNames(a, entry.value_ptr.*, version, names);
}

fn rewriteEnrichments(a: A, root: *std.json.Value, names: *std.StringHashMapUnmanaged([]const u8), version: [64]u8, additions: *std.json.Value) !void {
    const root_artifact = try projection.protectedArtifactName(a, version);
    if (root.object.getPtr("enrichments")) |entries| {
        for (entries.array.items) |*entry| {
            const original_name = getString(entry.*, "name");
            const name = names.get(original_name) orelse return error.InvalidPrivacyCatalog;
            try string(a, entry, "name", name);
            const kind = getString(entry.*, "kind");
            if (std.mem.eql(u8, kind, "group") or entry.object.contains("neighbor_context")) return error.PrivacyUnsupportedArtifactInput;
            const upstream = getString(entry.*, "source_artifact_name");
            if (upstream.len != 0) {
                try string(a, entry, "source_artifact_name", names.get(upstream) orelse return error.PrivacyUnknownArtifactSource);
            } else if (std.mem.eql(u8, kind, "embedding")) {
                const chunk_name = try std.fmt.allocPrint(a, "{s}_chunks", .{try managedName(a, version, original_name)});
                var chunk: std.json.Value = .{ .object = .empty };
                try string(a, &chunk, "name", chunk_name);
                try string(a, &chunk, "kind", "chunk");
                try string(a, &chunk, "source_artifact_name", root_artifact);
                try string(a, &chunk, "field", getString(entry.*, "field"));
                try string(a, &chunk, "template", getString(entry.*, "template"));
                try chunk.object.put(a, "chunk_size", .{ .integer = 1024 });
                try additions.array.append(chunk);
                try string(a, entry, "source_artifact_name", chunk_name);
                try string(a, entry, "field", "text");
                _ = entry.object.swapRemove("template");
            } else {
                try string(a, entry, "source_artifact_name", root_artifact);
            }
        }
    }
    var it = root.object.iterator();
    while (it.next()) |entry| if (entry.value_ptr.* == .object and entry.value_ptr.object.contains("type")) try rewriteEnrichments(a, entry.value_ptr, names, version, additions);
}

test "privacy protected graph versions all named consumers and closes the implicit raw text index" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const version = contract.fingerprint("policy");
    var tree = try std.json.parseFromSlice(std.json.Value, a,
        \\{"full_text_index_v0":{},"vectors":{"type":"embeddings","dimension":3,"embedding_name":"vectors","generator":{"kind":"dense_embedding","source_field":"body","artifact_name":"vectors","chunk_size":1024}},"enrichments":[{"name":"vectors","kind":"embedding","field":"body","producer_json":"{}"}]}
    , .{});
    try protect(a, &tree.value, version);
    const text = tree.value.object.get("full_text_index_v0").?;
    try std.testing.expectEqualStrings(try projection.protectedArtifactName(a, version), getString(text.object.get("sources").?.array.items[0], "artifact"));
    const vector = tree.value.object.get("vectors").?;
    const mapped = try managedName(a, version, "vectors");
    try std.testing.expectEqualStrings(mapped, getString(vector.object.get("sources").?.array.items[0], "artifact"));
    try std.testing.expectEqualStrings("strict", getString(vector, "coverage_policy"));
    try std.testing.expect(!vector.object.contains("generator"));
    const entries = tree.value.object.get("enrichments").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings(mapped, getString(entries[0], "name"));
    try std.testing.expectEqualStrings(try projection.protectedArtifactName(a, version), getString(entries[1], "source_artifact_name"));
    try std.testing.expectEqualStrings("body", getString(entries[1], "field"));
    try std.testing.expectEqualStrings("text", getString(entries[0], "field"));
}

test "privacy protection refuses raw graph indexes and unmanaged vector inputs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const version = contract.fingerprint("policy");
    var graph = try std.json.parseFromSlice(std.json.Value, a, "{\"links\":{\"type\":\"graph\"}}", .{});
    try std.testing.expectError(error.PrivacyUnsupportedIndexType, protect(a, &graph.value, version));
    var external = try std.json.parseFromSlice(std.json.Value, a, "{\"vectors\":{\"type\":\"embeddings\",\"external\":true}}", .{});
    try std.testing.expectError(error.PrivacyExternalEmbeddingsUnsupported, protect(a, &external.value, version));
}

/// Public catalog mutations must not remove/rebind policy-owned producers or
/// introduce a raw consumer beside an active protected graph. The Privacy API
/// composes its replacement directly in one table-definition CAS.
pub fn ensureUserMutation(a: A, indexes_json: []const u8, name: []const u8) !void {
    if (std.mem.eql(u8, name, key) or std.mem.startsWith(u8, name, "_privacy_")) return error.PrivacyManagedGraph;
    var tree = try std.json.parseFromSlice(std.json.Value, a, indexes_json, .{});
    defer tree.deinit();
    var resource = try current(a, tree.value) orelse return;
    defer resource.deinit();
    if (resource.value.policy.mode != .monitor) return error.PrivacyManagedGraph;
}

pub fn validateUserConfiguration(a: A, raw: []const u8) !void {
    var tree = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer tree.deinit();
    try userNode(a, tree.value);
    if (tree.value == .object) {
        const indexes = tree.value.object.get("indexes") orelse tree.value;
        if (indexes == .object) {
            if (indexes.object.contains(key)) return error.PrivacyManagedGraph;
            var it = indexes.object.iterator();
            while (it.next()) |entry| {
                if (std.mem.startsWith(u8, entry.key_ptr.*, "_privacy_")) return error.PrivacyManagedGraph;
                try userNode(a, entry.value_ptr.*);
            }
        }
    }
}
fn userNode(a: A, node: std.json.Value) !void {
    if (node != .object) return;
    const name = getString(node, "name");
    if (std.mem.startsWith(u8, name, "_privacy_") or (std.mem.eql(u8, getString(node, "field"), projection.source_field) or std.mem.eql(u8, getString(node, "field"), projection.state_field))) return error.PrivacyManagedGraph;
    if (node.object.get("enrichments")) |entries| {
        if (entries == .array) for (entries.array.items) |entry| try userNode(a, entry);
    }
    if (node.object.get("producer")) |producer| try userProducer(producer);
    if (node.object.get("producer_json")) |producer| {
        if (producer == .string) {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, producer.string, .{});
            defer parsed.deinit();
            try userProducer(parsed.value);
        }
    }
}
fn userProducer(producer: std.json.Value) !void {
    if (producer != .object) return;
    const config = producer.object.get("config") orelse return;
    if (config == .object and config.object.contains(projection.config_key)) return error.PrivacyManagedGraph;
}

test "privacy public declarations cannot impersonate a managed producer or opaque policy" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.PrivacyManagedGraph, validateUserConfiguration(a, "{\"indexes\":{\"_privacy\":{}}}"));
    try std.testing.expectError(error.PrivacyManagedGraph, validateUserConfiguration(a, "{\"enrichments\":[{\"name\":\"_privacy_document\"}]}"));
    try std.testing.expectError(error.PrivacyManagedGraph, validateUserConfiguration(a, "{\"producer\":{\"type\":\"extractor\",\"config\":{\"_antfly_privacy\":{}}}}"));
    try validateUserConfiguration(a, "{\"schema\":{\"document_schemas\":{\"doc\":{\"schema\":{\"properties\":{\"_privacy\":{\"type\":\"string\"}}}}}}}");
}

test "privacy execution tuning keeps the managed producer catalog identical" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"} };
    const first = try next(scratch, null, policy, null);
    var baseline = (try std.json.parseFromSlice(std.json.Value, scratch, try install(scratch, "{}", first), .{})).value;
    policy.execution = .{ .batch_items = 1, .max_attempts = 5 };
    const second = try next(scratch, first, policy, try etag(scratch, first));
    var tuned = (try std.json.parseFromSlice(std.json.Value, scratch, try install(scratch, "{}", second), .{})).value;
    _ = baseline.object.swapRemove(key);
    _ = tuned.object.swapRemove(key);
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(scratch, baseline, .{}), try std.json.Stringify.valueAlloc(scratch, tuned, .{}));
}

test "privacy scan retention preserves active jobs and admits the sixteenth scan" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var resource = try next(a, null, .{ .mode = .monitor, .fields = &.{"body"} }, null);
    var ids: [16][64]u8 = undefined;
    for (&ids) |*id| id.* = (try startScan(a, &resource, "", "", 100)).scan_id;
    try std.testing.expectEqual(@as(usize, 16), resource.scans.len);
    const sequence = resource.scan_sequence;
    try std.testing.expectError(error.PrivacyScanLimitExceeded, startScan(a, &resource, "", "", 100));
    try std.testing.expectEqual(sequence, resource.scan_sequence);
    // Retire a terminal job in the middle; preserve the oldest unfinished job.
    const scans = try a.dupe(Scan, resource.scans);
    scans[7].phase = .succeeded;
    resource.scans = scans;
    _ = try startScan(a, &resource, "", "", 100);
    try std.testing.expectEqual(@as(usize, 16), resource.scans.len);
    try std.testing.expectEqualStrings(&ids[0], &resource.scans[0].scan_id);
    try std.testing.expectEqualStrings(&ids[8], &resource.scans[7].scan_id);
    // A new semantic policy can retire old-policy jobs, even if all were active.
    var policy = resource.policy;
    policy.replacement = .mask;
    resource = try next(a, resource, policy, try etag(a, resource));
    _ = try startScan(a, &resource, "", "", 100);
    try std.testing.expectEqual(@as(usize, 16), resource.scans.len);
}

test "privacy executable catalog is invariant to equivalent authored ordering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = contract.Policy{ .mode = .monitor, .fields = &.{ "z", "a" }, .category_groups = &.{.contact_details} };
    var reordered = first;
    reordered.fields = &.{ "a", "z" };
    reordered.categories = try first.labels(a);
    reordered.category_groups = &.{};
    const original = try next(a, null, first, null);
    const changed = try next(a, original, reordered, try etag(a, original));
    try std.testing.expectEqual(original.policy_version, changed.policy_version);
    const one = try std.json.parseFromSlice(std.json.Value, a, try install(a, "{}", original), .{});
    const two = try std.json.parseFromSlice(std.json.Value, a, try install(a, "{}", changed), .{});
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, one.value.object.get("enrichments").?, .{}), try std.json.Stringify.valueAlloc(a, two.value.object.get("enrichments").?, .{}));
}

test "privacy category limit applies before and after group expansion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const group = contract.groupLabels(.credentials);
    const categories = try a.alloc(contract.Category, contract.max_categories - group.len + 1);
    for (categories, 0..) |*category, i| category.* = .{ .name = try std.fmt.allocPrint(a, "custom_{d}", .{i}) };
    var policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"}, .category_groups = &.{.credentials}, .categories = categories[0 .. categories.len - 1] };
    try policy.validate();
    const executable = try policy.executable(a);
    try executable.validate();
    try std.testing.expectEqual(contract.max_categories, executable.categories.len);
    try std.testing.expectEqual(try policy.identity(a), try executable.identity(a));
    _ = try install(a, "{}", try next(a, null, policy, null));
    policy.categories = categories;
    try std.testing.expectError(error.InvalidPrivacyCategories, policy.validate());
    try std.testing.expectError(error.InvalidPrivacyCategories, next(a, null, policy, null));
    // Explicit threshold/description overrides do not add another category.
    categories[categories.len - 1] = .{ .name = group[0], .threshold = 0.8 };
    try policy.validate();
    _ = try install(a, "{}", try next(a, null, policy, null));
}

test "privacy due index replaces nodes and preserves fairness without history growth" {
    const a = std.testing.allocator;
    var index: WorkIndex = .{};
    defer index.deinit(a);
    for (0..1000) |id| try index.update(a, id, @intCast(id));
    for (0..1000) |id| {
        try std.testing.expectEqual(@as(?u64, id), index.first(2000));
        try index.update(a, id, @intCast(3000 + id));
    }
    try std.testing.expectEqual(@as(?u64, null), index.first(2000));
    try std.testing.expectEqual(@as(usize, 1000), index.nodes.count());
    for (0..1000) |id| try index.update(a, id, null);
    try std.testing.expectEqual(@as(?u64, null), index.first(9999));
    try std.testing.expectEqual(@as(usize, 0), index.nodes.count());
    try std.testing.expectEqual(@as(?i64, null), try workDue(a, "{\"enrichments\":[]}"));
    try std.testing.expectEqual(@as(?i64, 20), try workDue(a, "{\"_privacy\":{\"coverage\":{\"updated_at_millis\":10,\"retry_after_millis\":40},\"scans\":[{\"phase\":\"processing\",\"updated_at_millis\":0,\"retry_after_millis\":20},{\"phase\":\"succeeded\",\"updated_at_millis\":0}]}}"));
}

test "privacy due index preserves ownership through allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: A) !void {
            var index: WorkIndex = .{};
            defer index.deinit(a);
            for (0..40) |id| try index.update(a, id, @intCast(id));
            try index.update(a, 0, 100);
            try index.update(a, 1, null);
            try std.testing.expectEqual(@as(?u64, 2), index.first(50));
        }
    }.run, .{});
}

test "privacy progress classification retains all semantic fields" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var resource = try next(a, null, .{ .mode = .monitor, .fields = &.{"body"} }, null);
    const before = try install(a, "{}", resource);
    resource.coverage.retry_after_millis = 200;
    _ = try startScan(a, &resource, "a", "z", 50);
    const progress = try replaceResource(a, before, resource);
    try std.testing.expect(try progressOnly(a, before, progress));
    resource.resource_version += 1;
    try std.testing.expect(!try progressOnly(a, before, try replaceResource(a, before, resource)));
    resource.resource_version -= 1;
    resource.unprotected_indexes_json = "{\"changed\":{}}";
    try std.testing.expect(!try progressOnly(a, before, try replaceResource(a, before, resource)));
    try std.testing.expect(!try progressOnly(a, "{}", progress));
}

test "privacy reconciliation preserves runtime state and rejects stale policy bindings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var resource = try next(a, null, .{ .mode = .monitor, .fields = &.{"body"} }, null);
    const before = try install(a, "{}", resource);
    resource.coverage.retry_after_millis = 200;
    const durable = try replaceResource(a, before, resource);
    const retained = (try reconcileIndexes(a, durable, before)).?;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, retained, .{});
    defer parsed.deinit();
    var value = (try current(a, parsed.value)).?;
    defer value.deinit();
    try std.testing.expectEqual(@as(i64, 200), value.value.coverage.retry_after_millis);
    try std.testing.expect(try reconcileIndexes(a, durable, "{}") == null);
    resource.resource_version += 1;
    const changed = try replaceResource(a, before, resource);
    try std.testing.expect(try reconcileIndexes(a, changed, before) == null);
}
