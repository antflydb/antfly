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

const std = @import("std");
const m = @import("service.zig");
const A = std.mem.Allocator;

const Store = struct {
    arena: std.heap.ArenaAllocator,
    values: std.StringHashMapUnmanaged([]const u8) = .empty,
    revision: u64 = 0,
    table_id: u64 = 0,
    space: []const u8 = "",
    resource_state: m.types.provenance.ResourceState = .current,
    resource_revision: u64 = 7,
    resource_digest: ?[32]u8 = null,
    resource_json: []const u8 = "{\"message\":\"Approved deployment\"}",
    resource_reads: usize = 0,
    reads: usize = 0,
    scans: usize = 0,
    entity_scans: usize = 0,
    neighbor_calls: usize = 0,
    decisions: usize = 0,
    reject_decision: bool = false,
    conflict: bool = false,
    failures: u8 = 0,
    public_output: bool = false,
    required_grant: m.types.RequiredGrant = .write,
    fn init() Store {
        return .{ .arena = .init(std.testing.allocator) };
    }
    fn deinit(self: *Store) void {
        self.arena.deinit();
    }
    fn service(self: *Store, principal: []const u8, admin: bool, now: i64) m.Service {
        return .{ .backend = .{ .ptr = self, .get = get, .scan = scan, .commit = commit, .resource = resource }, .context = .{ .principal = principal, .admin = admin, .now_ms = now, .table_id = self.table_id, .space = self.space } };
    }
    fn resource(ptr: *anyopaque, a: A, reference: m.types.provenance.Resource) !m.types.provenance.Snapshot {
        const self: *Store = @ptrCast(@alignCast(ptr));
        self.resource_reads += 1;
        if ((reference.table_id != 0 and reference.table_id != 7) or !std.mem.eql(u8, reference.database, "default") or !std.mem.eql(u8, reference.namespace, "public")) return .{ .state = .denied };
        if (self.resource_state != .current) return .{ .state = self.resource_state };
        var snapshot = try m.types.provenance.snapshot(a, reference, 7, self.resource_revision, self.resource_json, self.resource_digest);
        snapshot.storage_name = "physical-business";
        return snapshot;
    }
    fn get(ptr: *anyopaque, a: A, key: []const u8) !?m.Row {
        const self: *Store = @ptrCast(@alignCast(ptr));
        self.reads += 1;
        const json = self.values.get(key) orelse return null;
        return .{ .key = try a.dupe(u8, key), .json = try a.dupe(u8, json), .version = self.revision };
    }
    fn scan(ptr: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !m.Page {
        const self: *Store = @ptrCast(@alignCast(ptr));
        self.scans += 1;
        if (std.mem.eql(u8, prefix, "entity/")) self.entity_scans += 1;
        var rows: std.ArrayList(m.Row) = .empty;
        var it = self.values.iterator();
        while (it.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.key_ptr.*, prefix)) continue;
            if (cursor) |after| if (!std.mem.lessThan(u8, after, entry.key_ptr.*)) continue;
            try rows.append(a, .{ .key = try a.dupe(u8, entry.key_ptr.*), .json = try a.dupe(u8, entry.value_ptr.*) });
        }
        std.mem.sort(m.Row, rows.items, {}, struct {
            fn less(_: void, left: m.Row, right: m.Row) bool {
                return std.mem.lessThan(u8, left.key, right.key);
            }
        }.less);
        const count = @min(rows.items.len, limit);
        return .{ .rows = rows.items[0..count], .next = if (rows.items.len > count) rows.items[count - 1].key else null };
    }
    fn commit(ptr: *anyopaque, _: A, expected: ?m.Row, changes: []const m.Change) !void {
        const self: *Store = @ptrCast(@alignCast(ptr));
        if (self.conflict or (if (expected) |row| row.version != self.revision else self.revision != 0)) return error.MemoryRevisionConflict;
        const a = self.arena.allocator();
        for (changes) |change| {
            if (change.json) |json| try self.values.put(a, try a.dupe(u8, change.key), try a.dupe(u8, json)) else _ = self.values.remove(change.key);
        }
        self.revision += 1;
    }
    fn run(self: *Store, action: m.Action, id: []const u8, body: []const u8, token: []const u8, now: i64) ![]u8 {
        return self.service("alice", true, now).execute(std.testing.allocator, action, id, body, token);
    }
};
fn discard(store: *Store, action: m.Action, id: []const u8, body: []const u8, token: []const u8, now: i64) !void {
    const result = try store.run(action, id, body, token, now);
    std.testing.allocator.free(result);
}
const source = "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}]}";
const helm = "{\"assertions\":[{\"id\":\"helm\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Helm\",\"statement\":\"Production uses Helm\",\"valid_from\":100,\"evidence\":[{\"id\":\"e1\"}]}]}";
const terraform = "{\"assertions\":[{\"id\":\"terraform\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Terraform\",\"statement\":\"Production uses Terraform\",\"valid_from\":300,\"evidence\":[{\"id\":\"e1\"}]}]}";
fn setup(store: *Store) !void {
    try discard(store, .create, "", "{\"features\":{\"reconciliation\":\"deterministic\"},\"predicates\":[{\"name\":\"deploys_with\",\"cardinality\":\"one\"}]}", "create", 1);
    try discard(store, .observe, "", source, "source", 2);
}
test "memory atomic receipts reject reused tokens and revision conflicts" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const first = try store.run(.assertions, "", helm, "claim", 3);
    defer std.testing.allocator.free(first);
    const again = try store.run(.assertions, "", helm, "claim", 4);
    defer std.testing.allocator.free(again);
    try std.testing.expectEqualStrings(first, again);
    try std.testing.expectError(error.MemoryIdempotencyConflict, store.run(.assertions, "", terraform, "claim", 5));
    store.conflict = true;
    try std.testing.expectError(error.MemoryRevisionConflict, store.run(.assertions, "", terraform, "other", 6));
}
test "memory late evidence preserves current and historical deployment state" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", terraform, "terraform", 3);
    try discard(&store, .assertions, "", helm, "helm", 4);
    const current = try store.run(.recall, "", "{\"token_budget\":8192}", "", 500);
    defer std.testing.allocator.free(current);
    try std.testing.expect(std.mem.indexOf(u8, current, "Production uses Terraform") != null);
    try std.testing.expect(std.mem.indexOf(u8, current, "Production uses Helm") == null);
    const history = try store.run(.recall, "", "{\"valid_at\":200,\"token_budget\":8192}", "", 500);
    defer std.testing.allocator.free(history);
    try std.testing.expect(std.mem.indexOf(u8, history, "Production uses Helm") != null);
    try std.testing.expect(std.mem.indexOf(u8, history, "Production uses Terraform") == null);
}
test "memory private records never widen through query or lookup" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    const bob = store.service("bob", false, 4);
    try std.testing.expectError(error.Forbidden, bob.execute(std.testing.allocator, .get, "helm", "{}", ""));
    const recall = try bob.execute(std.testing.allocator, .recall, "", "{}", "");
    defer std.testing.allocator.free(recall);
    try std.testing.expect(std.mem.indexOf(u8, recall, "Production uses Helm") == null);
}
test "memory forgetting blocks ingestion and history without resurrection" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget", 4);
    try std.testing.expectError(error.MemoryForgotten, store.run(.observe, "", source, "again", 5));
    try std.testing.expectError(error.MemoryNotFound, store.run(.history, "helm", "{}", "", 5));
    const recall = try store.run(.recall, "", "{}", "", 500);
    defer std.testing.allocator.free(recall);
    try std.testing.expect(std.mem.indexOf(u8, recall, "Production uses Helm") == null);
    try discard(&store, .advance, "", "{}", "maintenance", 6);
}
test "memory hypothetical values cannot supersede actual values" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    try std.testing.expectError(error.MemoryInvalidSupersession, store.run(.assertions, "", "{\"assertions\":[{\"id\":\"proposal\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Terraform\",\"statement\":\"Maybe Terraform\",\"modality\":\"hypothetical\",\"evidence\":[{\"id\":\"e1\"}],\"supersedes\":[\"helm\"]}]}", "proposal", 4));
}
test "memory rejects ungrounded spans and publishing private evidence to team" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try std.testing.expectError(error.InvalidMemorySpan, store.run(.assertions, "", "{\"assertions\":[{\"id\":\"bad\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Helm\",\"statement\":\"Helm\",\"evidence\":[{\"id\":\"e1\",\"span\":{\"start\":0,\"end\":999}}]}]}", "span", 3));
    try std.testing.expectError(error.MemoryVisibilityConflict, store.run(.assertions, "", "{\"assertions\":[{\"id\":\"bad\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Helm\",\"statement\":\"Helm\",\"visibility\":\"team\",\"evidence\":[{\"id\":\"e1\"}]}]}", "team", 4));
}
test "memory strict feature admission does not fake unavailable managed processing" {
    var store = Store.init();
    defer store.deinit();
    try std.testing.expectError(error.MemoryCapabilityUnavailable, store.run(.create, "", "{\"processing\":\"managed\",\"features\":{\"assertion_distillation\":true},\"models\":{\"generator\":{\"provider\":\"antfly\",\"model\":\"test\",\"revision\":\"pinned\"}}}", "create", 1));
}

test "memory create replay and same-batch reconciliation preserve atomic semantics" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .create, "", "{\"features\":{\"reconciliation\":\"deterministic\"},\"predicates\":[{\"name\":\"deploys_with\",\"cardinality\":\"one\"}]}", "create", 10);
    const batch = try std.fmt.allocPrint(std.testing.allocator, "{{\"assertions\":[{s},{s}]}}", .{ helm[15 .. helm.len - 2], terraform[15 .. terraform.len - 2] });
    defer std.testing.allocator.free(batch);
    try discard(&store, .assertions, "", batch, "batch", 400);
    const result = try store.run(.recall, "", "{\"token_budget\":8192}", "", 500);
    defer std.testing.allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "Production uses Terraform") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Production uses Helm") == null);
}

test "memory current recall excludes future validity and recorded-time revision is authoritative" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    const future = try store.run(.recall, "", "{\"token_budget\":8192}", "", 50);
    defer std.testing.allocator.free(future);
    try std.testing.expect(std.mem.indexOf(u8, future, "Production uses Helm") == null);
    try discard(&store, .correct, "", "{\"id\":\"helm\",\"expected_revision\":4,\"lifecycle\":\"retracted\",\"reason\":\"Incorrect source\"}", "correct", 400);
    const before = try store.run(.recall, "", "{\"known_at\":3,\"valid_at\":200,\"token_budget\":8192}", "", 500);
    defer std.testing.allocator.free(before);
    try std.testing.expect(std.mem.indexOf(u8, before, "Production uses Helm") != null);
    const after = try store.run(.recall, "", "{\"valid_at\":200,\"token_budget\":8192}", "", 500);
    defer std.testing.allocator.free(after);
    try std.testing.expect(std.mem.indexOf(u8, after, "Production uses Helm") == null);
}

test "memory independent support survives forgetting and dependency maintenance" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e2\",\"source_id\":\"verified\",\"source_version\":\"1\",\"text\":\"Production uses Helm\"}]}", "second-source", 3);
    try discard(&store, .assertions, "", "{\"assertions\":[{\"id\":\"independent\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Helm\",\"statement\":\"Production uses Helm\",\"evidence\":[{\"id\":\"e1\"}],\"support_sets\":[{\"evidence\":[{\"id\":\"e2\"}]}]}]}", "supported", 4);
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget", 5);
    try discard(&store, .advance, "", "{}", "cleanup", 6);
    const result = try store.run(.get, "independent", "{}", "", 7);
    defer std.testing.allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"stale\":false") != null);
}

test "memory structural enumeration supplies resumable cursors" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "helm", 3);
    try discard(&store, .assertions, "", "{\"assertions\":[{\"id\":\"other\",\"subject\":\"staging\",\"predicate\":\"deploys_with\",\"value\":\"Helm\",\"statement\":\"Staging uses Helm\",\"evidence\":[{\"id\":\"e1\"}]}]}", "other", 4);
    const first = try store.run(.list, "", "{\"limit\":1}", "", 500);
    defer std.testing.allocator.free(first);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, first, .{});
    defer parsed.deinit();
    try std.testing.expect(!parsed.value.object.get("complete").?.bool);
    const cursor = parsed.value.object.get("next_cursor").?.string;
    const body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .limit = 1, .cursor = cursor }, .{});
    defer std.testing.allocator.free(body);
    const second = try store.run(.list, "", body, "", 500);
    defer std.testing.allocator.free(second);
    var next = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, second, .{});
    defer next.deinit();
    try std.testing.expect(next.value.object.get("complete").?.bool);
    const left = parsed.value.object.get("items").?.array.items[0].object.get("assertion").?.object.get("id").?.string;
    const right = next.value.object.get("items").?.array.items[0].object.get("assertion").?.object.get("id").?.string;
    try std.testing.expect(!std.mem.eql(u8, left, right));
}

const fake_model: m.types.Model = .{ .provider = "test", .model = "fake", .revision = "v1", .dimensions = 2 };
fn modelService(store: *Store, now: i64) m.Service {
    var service = store.service("alice", true, now);
    service.backend.generate = fakeGenerate;
    service.backend.embed = fakeEmbed;
    service.backend.validate_models = fakeValidate;
    return service;
}
fn fakeValidate(_: *anyopaque, _: m.types.Config) !void {}
fn fakeEmbed(_: *anyopaque, a: A, _: m.types.Model, _: []const u8) ![]const f32 {
    return a.dupe(f32, &.{ 1, 0 });
}
fn fakeGenerate(ptr: *anyopaque, a: A, _: m.types.Config, _: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
    const store: *Store = @ptrCast(@alignCast(ptr));
    if (store.failures > 0) {
        store.failures -= 1;
        return error.TransientInferenceFailure;
    }
    const assertions = try a.alloc(m.types.Assertion, 1);
    assertions[0] = .{ .id = "managed", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = if (store.public_output) "Private context secret" else "Production uses Helm", .visibility = if (store.public_output) .team else .private, .evidence = &.{.{ .id = "e1" }} };
    return .{ .assertions = assertions };
}
fn managedSetup(store: *Store) ![]const u8 {
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    const result = try modelService(store, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"note\",\"source_version\":\"1\",\"text\":\"Production uses Helm\",\"managed\":true}]}", "observe");
    const operation = try std.json.parseFromSliceLeaky(m.types.Operation, a, result, .{});
    try std.testing.expectEqual(m.types.Stage.pending, operation.phase);
    try std.testing.expect(!operation.assertions_ready);
    return operation.id;
}
test "memory managed failures persist bounded retries and successful decisions replay" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    store.failures = 1;
    const failed = try modelService(&store, 3).execute(a, .advance, id, "{}", "try1");
    try std.testing.expect(std.mem.indexOf(u8, failed, "TransientInferenceFailure") != null);
    try std.testing.expectError(error.MemoryRetryNotReady, modelService(&store, 4).execute(a, .advance, id, "{}", "early"));
    _ = try modelService(&store, 10000).execute(a, .advance, id, "{}", "try2");
    const ready = try modelService(&store, 10001).execute(a, .operation, id, "{}", "");
    const result = try std.json.parseFromSliceLeaky(m.types.Operation, a, ready, .{});
    try std.testing.expectEqual(m.types.Stage.succeeded, result.phase);
    try std.testing.expectEqual(@as(u8, 2), result.attempts);
    _ = try modelService(&store, 10002).execute(a, .advance, id, "{}", "try2");
    const claim = try modelService(&store, 10003).execute(a, .get, "managed", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, claim, "managed:1") != null);
}
test "memory configuration transition cancels stale managed callbacks" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model }, .policy_version = "v2" };
    _ = try modelService(&store, 3).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = 3, .config = config }, .{}), "configure");
    _ = try modelService(&store, 4).execute(a, .advance, id, "{}", "advance");
    const result = try modelService(&store, 5).execute(a, .operation, id, "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, result, "cancelled") != null);
    try std.testing.expectError(error.MemoryNotFound, modelService(&store, 6).execute(a, .get, "managed", "{}", ""));
}
test "memory semantic configuration builds new vectors before serving cutover" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "helm", 3);
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    const accepted = try modelService(&store, 400).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = 4, .config = config }, .{}), "configure");
    const operation = try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{});
    try std.testing.expectEqual(m.types.Stage.pending, operation.phase);
    const before = try modelService(&store, 500).execute(a, .inspect, "", "{}", "");
    const state_before = try std.json.parseFromSliceLeaky(std.json.Value, a, before, .{});
    try std.testing.expect(!state_before.object.get("state").?.object.get("config").?.object.get("features").?.object.get("semantic_recall").?.bool);
    _ = try modelService(&store, 501).execute(a, .advance, operation.id, "{}", "reindex");
    const result = try modelService(&store, 502).execute(a, .recall, "", "{\"query\":\"Helm\",\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, result, "lexical_vector") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Production uses Helm") != null);
}

test "memory canonical identity keeps homonyms distinct and curation reversible" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"alex-team-a\",\"name\":\"Alex\",\"evidence\":[{\"id\":\"e1\"}]},{\"id\":\"alex-team-b\",\"name\":\"Alex\",\"evidence\":[{\"id\":\"e1\"}]}]}", "entities", 3);
    const original = try store.run(.resolve_entity, "alex-team-a", "{}", "", 4);
    defer std.testing.allocator.free(original);
    try std.testing.expect(std.mem.indexOf(u8, original, "\"canonical_id\":\"alex-team-a\"") != null);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"alex-team-a\",\"to\":\"alex-team-b\",\"expected_revision\":4}", "merge", 5);
    const merged = try store.run(.resolve_entity, "alex-team-a", "{}", "", 6);
    defer std.testing.allocator.free(merged);
    try std.testing.expect(std.mem.indexOf(u8, merged, "\"canonical_id\":\"alex-team-b\"") != null);
    try discard(&store, .curate_entity, "", "{\"action\":\"unmerge\",\"from\":\"alex-team-a\",\"expected_revision\":5,\"pin_distinct\":true}", "unmerge", 7);
    const restored = try store.run(.resolve_entity, "alex-team-a", "{}", "", 8);
    defer std.testing.allocator.free(restored);
    try std.testing.expect(std.mem.indexOf(u8, restored, "\"canonical_id\":\"alex-team-a\"") != null);
    try discard(&store, .curate_entity, "", "{\"action\":\"suppress\",\"from\":\"alex-team-a\",\"expected_revision\":6}", "suppress", 9);
    try std.testing.expectError(error.MemoryEntitySuppressed, store.run(.resolve_entity, "alex-team-a", "{}", "", 10));
    try discard(&store, .curate_entity, "", "{\"action\":\"unsuppress\",\"from\":\"alex-team-a\",\"expected_revision\":7}", "unsuppress", 11);
    try discard(&store, .resolve_entity, "alex-team-a", "{}", "", 12);
}

test "memory dependency corrections are immediate and recorded-time reads reconstruct premises" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "parent", 100);
    try discard(&store, .assertions, "", "{\"assertions\":[{\"id\":\"procedure\",\"subject\":\"deploy\",\"predicate\":\"procedure\",\"kind\":\"procedural\",\"value\":\"Helm deploy\",\"statement\":\"Run Helm to deploy\",\"depends_on\":[\"helm\"],\"evidence\":[{\"id\":\"e1\"}]}]}", "derived", 110);
    try discard(&store, .correct, "", "{\"id\":\"helm\",\"expected_revision\":4,\"lifecycle\":\"retracted\",\"reason\":\"Deployment guidance was incorrect\"}", "correct", 200);
    try std.testing.expectError(error.MemoryNotFound, store.run(.get, "procedure", "{}", "", 201));
    const previous = try store.run(.recall, "", "{\"kind\":\"procedural\",\"known_at\":150,\"valid_at\":150,\"token_budget\":8192}", "", 300);
    defer std.testing.allocator.free(previous);
    try std.testing.expect(std.mem.indexOf(u8, previous, "Run Helm to deploy") != null);
    const after = try store.run(.recall, "", "{\"kind\":\"procedural\",\"known_at\":250,\"valid_at\":150,\"token_budget\":8192}", "", 300);
    defer std.testing.allocator.free(after);
    try std.testing.expect(std.mem.indexOf(u8, after, "Run Helm to deploy") == null);
    const history = try store.run(.history, "procedure", "{}", "", 301);
    defer std.testing.allocator.free(history);
    try std.testing.expect(std.mem.indexOf(u8, history, "Run Helm to deploy") != null);
}

test "memory expired sources hide immediately and cleanup remains durable" {
    var store = Store.init();
    defer store.deinit();
    try discard(&store, .create, "", "{\"retention_ms\":10}", "create", 1);
    try discard(&store, .observe, "", source, "source", 2);
    try discard(&store, .assertions, "", "{\"assertions\":[{\"id\":\"temporary\",\"subject\":\"deploy\",\"predicate\":\"note\",\"value\":\"Helm\",\"statement\":\"Use Helm\",\"evidence\":[{\"id\":\"e1\"}]}]}", "claim", 3);
    try discard(&store, .get, "temporary", "{}", "", 11);
    try std.testing.expectError(error.MemoryNotFound, store.run(.get, "temporary", "{}", "", 12));
    try discard(&store, .advance, "", "{}", "cleanup", 13);
    const result = try store.run(.inspect, "", "{}", "", 14);
    defer std.testing.allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"retention_ms\":10") != null);
}

test "memory private beliefs never supersede another principal's private state" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    _ = try store.service("bob", false, 3).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"bob-source\",\"source_id\":\"bob-note\",\"source_version\":\"1\",\"text\":\"Production uses Helm\"}]}", "bob-source");
    _ = try store.service("bob", false, 4).execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"bob-helm\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Helm\",\"statement\":\"Production uses Helm\",\"valid_from\":100,\"evidence\":[{\"id\":\"bob-source\"}]}]}", "bob-claim");
    try discard(&store, .assertions, "", terraform, "alice-claim", 400);
    const record = try store.service("bob", false, 500).execute(a, .get, "bob-helm", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, record, "\"lifecycle\":\"active\"") != null);
    const recalled = try store.service("bob", false, 500).execute(a, .recall, "", "{\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, recalled, "Production uses Helm") != null);
    try std.testing.expect(std.mem.indexOf(u8, recalled, "Production uses Terraform") == null);
}

test "memory late intermediate changes close every historical interval in source order" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", terraform, "newest", 3);
    try discard(&store, .assertions, "", helm, "oldest", 4);
    try discard(&store, .assertions, "", "{\"assertions\":[{\"id\":\"middle\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Kubectl\",\"statement\":\"Production uses Kubectl\",\"valid_from\":200,\"evidence\":[{\"id\":\"e1\"}]}]}", "middle", 5);
    const a = store.arena.allocator();
    const expected = [_][]const u8{ "Helm", "Kubectl", "Terraform" };
    for ([_]i64{ 150, 250, 350 }, expected) |time, value| {
        const body = try std.json.Stringify.valueAlloc(a, .{ .valid_at = time, .token_budget = 8192 }, .{});
        const result = try store.run(.recall, "", body, "", 500);
        defer std.testing.allocator.free(result);
        const decoded = try std.json.parseFromSliceLeaky(m.types.RecallResult, a, result, .{});
        try std.testing.expectEqual(@as(usize, 1), decoded.items.len);
        try std.testing.expectEqualStrings(value, decoded.items[0].memory.assertion.value);
    }
    try discard(&store, .assertions, "", helm, "replay-old-input", 600);
    const current = try store.run(.recall, "", "{\"token_budget\":8192}", "", 700);
    defer std.testing.allocator.free(current);
    try std.testing.expect(std.mem.indexOf(u8, current, "Production uses Helm") == null);
}

test "memory all competing unknown values remain explicitly disputed" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    const ids = [_][]const u8{ "a", "b", "c" };
    for (ids, 0..) |id, i| {
        const body = try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{.{ .id = id, .subject = "production", .predicate = "deploys_with", .value = id, .statement = id, .evidence = &.{.{ .id = "e1" }} }} }, .{});
        try discard(&store, .assertions, "", body, id, @intCast(10 + i));
    }
    for (ids) |id| {
        const raw = try store.run(.get, id, "{}", "", 500);
        defer std.testing.allocator.free(raw);
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, raw, .{});
        try std.testing.expectEqual(m.types.Lifecycle.disputed, record.lifecycle);
        try std.testing.expectEqual(@as(usize, 2), record.conflicts_with.len);
    }
}

test "memory full recall rendering respects its byte token upper bound" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    const raw = try store.run(.recall, "", "{\"token_budget\":2048}", "", 500);
    defer std.testing.allocator.free(raw);
    const result = try std.json.parseFromSliceLeaky(m.types.RecallResult, store.arena.allocator(), raw, .{});
    try std.testing.expectEqual(raw.len, result.token_upper_bound);
    try std.testing.expect(result.token_upper_bound <= 2048);
}

test "memory managed defaults process observations and private context taints every output" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try modelService(&store, 1).execute(a, .create, "", "{}", "create");
    _ = try modelService(&store, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"context\",\"source_id\":\"private-note\",\"source_version\":\"1\",\"text\":\"Private context secret\"}]}", "context");
    const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(&store, 3).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = 3, .config = config }, .{}), "configure");
    const accepted = try modelService(&store, 4).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"public-note\",\"source_version\":\"1\",\"text\":\"Production uses Helm\",\"visibility\":\"team\"}],\"context_evidence_ids\":[\"context\"]}", "capture");
    const operation = try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{});
    try std.testing.expectEqual(m.types.Stage.pending, operation.phase);
    store.public_output = true;
    const rejected = try modelService(&store, 5).execute(a, .advance, operation.id, "{}", "advance");
    try std.testing.expect(std.mem.indexOf(u8, rejected, "MemoryVisibilityConflict") != null);
    try std.testing.expectError(error.MemoryNotFound, modelService(&store, 6).execute(a, .get, "managed", "{}", ""));
}

test "memory publication authorization revocation prevents commits and receipt replay" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const Revoked = struct {
        fn check(_: *anyopaque) !void {
            return error.Forbidden;
        }
    };
    var marker: u8 = 0;
    var denied = store.service("alice", true, 3);
    denied.context.final_check_ptr = &marker;
    denied.context.final_check_fn = Revoked.check;
    try std.testing.expectError(error.Forbidden, denied.execute(std.testing.allocator, .assertions, "", helm, "claim"));
    try std.testing.expectError(error.MemoryNotFound, store.run(.get, "helm", "{}", "", 4));
    // A rejected publication consumes neither the mutation nor its receipt.
    try discard(&store, .assertions, "", helm, "claim", 5);
    try std.testing.expectError(error.Forbidden, denied.execute(std.testing.allocator, .assertions, "", helm, "claim"));
    try std.testing.expectError(error.Forbidden, denied.execute(std.testing.allocator, .get, "helm", "{}", ""));
}

test "memory regression: forget all immediately removes entity lookup" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"secret\",\"name\":\"Private secret name\",\"evidence\":[{\"id\":\"e1\"}]}]}", "entity", 3);
    try discard(&store, .forget, "", "{\"all\":true}", "forget", 4);
    try std.testing.expectError(error.MemoryNotFound, store.run(.resolve_entity, "secret", "{}", "", 5));
}

test "memory regression: structural historical enumeration agrees with recall" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", terraform, "terraform", 3);
    try discard(&store, .assertions, "", helm, "helm", 4);
    const a = store.arena.allocator();
    const recalled = try store.service("alice", true, 500).execute(a, .recall, "", "{\"valid_at\":200,\"token_budget\":8192}", "");
    const listed = try store.service("alice", true, 500).execute(a, .list, "", "{\"valid_at\":200}", "");
    try std.testing.expect(std.mem.indexOf(u8, recalled, "Production uses Helm") != null);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, listed, .{});
    try std.testing.expectEqual(@as(usize, 1), parsed.object.get("items").?.array.items.len);
}

test "memory regression: canonical merge preserves single-valued reconciliation" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"left\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]},{\"id\":\"right\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]}]}", "entities", 3);
    const a = store.arena.allocator();
    const first = try std.mem.replaceOwned(u8, a, helm, "production", "left");
    const second = try std.mem.replaceOwned(u8, a, terraform, "production", "right");
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", first, "helm");
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", second, "terraform");
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"left\",\"to\":\"right\",\"expected_revision\":6}", "merge", 6);
    const result = try store.service("alice", true, 500).execute(a, .recall, "", "{\"subject\":\"right\",\"token_budget\":8192}", "");

    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, result, .{});
    try std.testing.expectEqual(@as(usize, 1), parsed.object.get("items").?.array.items.len);
}

fn reviewEmbed(ptr: *anyopaque, a: A, _: m.types.Model, _: []const u8) ![]const f32 {
    const store: *Store = @ptrCast(@alignCast(ptr));
    if (store.failures > 0) {
        store.failures -= 1;
        return error.TransientInferenceFailure;
    }
    return a.dupe(f32, &.{ 1, 0 });
}
fn reviewService(store: *Store, now: i64) m.Service {
    var service = modelService(store, now);
    service.backend.embed = reviewEmbed;
    return service;
}

test "memory regression: failed reindex generation cannot label old vectors as a different model" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    var claims: std.ArrayList(m.types.Assertion) = .empty;
    for (0..10) |i| try claims.append(a, .{ .id = try std.fmt.allocPrint(a, "claim-{d}", .{i}), .subject = "s", .predicate = "p", .value = "v", .statement = "Document unrelated to query", .evidence = &.{.{ .id = "e1" }} });
    _ = try reviewService(&store, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims.items }, .{}), "claims");
    const config_b: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    const configured_b = try reviewService(&store, 4).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = 4, .config = config_b }, .{}), "config-b");
    const op_b = try std.json.parseFromSliceLeaky(m.types.Operation, a, configured_b, .{});
    _ = try reviewService(&store, 5).execute(a, .advance, op_b.id, "{}", "b-page1");
    store.failures = 3;
    for (0..3) |i| _ = try reviewService(&store, @intCast(10000 + i * 10000)).execute(a, .advance, op_b.id, "{}", try std.fmt.allocPrint(a, "b-fail-{d}", .{i}));
    const external_c: m.types.Model = .{ .provider = "external", .model = "different-C", .revision = "C", .dimensions = 2 };
    const config_c: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = external_c } };
    const configured_c = try reviewService(&store, 40000).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = 9, .config = config_c }, .{}), "config-c");
    const op_c = try std.json.parseFromSliceLeaky(m.types.Operation, a, configured_c, .{});
    try std.testing.expect(op_c.config_generation > op_b.config_generation);
    const snapshot = try reviewService(&store, 40001).execute(a, .configuration, try std.fmt.allocPrint(a, "{d}", .{op_b.config_generation}), "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, snapshot, "fake") != null);
    const missing = try reviewService(&store, 40001).execute(a, .advance, op_c.id, "{}", "c-not-staged");
    try std.testing.expect(std.mem.indexOf(u8, missing, "MemoryExternalVectorRequired") != null);
    var staged: std.ArrayList(std.meta.Elem(@FieldType(m.types.EmbeddingBatch, "items"))) = .empty;
    var it = store.values.iterator();
    while (it.next()) |entry| {
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "assertion/")) continue;
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, entry.value_ptr.*, .{});
        try staged.append(a, .{ .id = record.assertion.id, .embedding = &.{ 0, 1 } });
    }
    try std.testing.expectEqual(@as(usize, 10), staged.items.len);
    const batch: m.types.EmbeddingBatch = .{ .generation = op_c.config_generation, .model = external_c, .items = staged.items };
    _ = try reviewService(&store, 40001).execute(a, .stage_embeddings, "", try std.json.Stringify.valueAlloc(a, batch, .{}), "stage-c-only-missing");
    _ = try reviewService(&store, 50000).execute(a, .advance, op_c.id, "{}", "c-page1");
    _ = try reviewService(&store, 50001).execute(a, .advance, op_c.id, "{}", "c-page2");
    const recall_body = try std.json.Stringify.valueAlloc(a, .{ .query = "zzzz", .embedding = &[_]f32{ 1, 0 }, .embedding_model = external_c, .token_budget = 8192 }, .{});
    const result = try reviewService(&store, 50002).execute(a, .recall, "", recall_body, "");
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, result, .{});

    try std.testing.expectEqual(@as(usize, 0), parsed.object.get("items").?.array.items.len);
}

test "memory identity unmerge restores separate groups without erasing explicit corrections" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"left\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]},{\"id\":\"right\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]}]}", "entities", 3);
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.mem.replaceOwned(u8, a, helm, "production", "left"), "helm");
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.mem.replaceOwned(u8, a, terraform, "production", "right"), "terraform");
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"left\",\"to\":\"right\",\"expected_revision\":6}", "merge", 6);
    try discard(&store, .curate_entity, "", "{\"action\":\"unmerge\",\"from\":\"left\",\"expected_revision\":7}", "unmerge", 7);
    const restored = try store.service("alice", true, 500).execute(a, .recall, "", "{\"subject\":\"left\",\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, restored, "Production uses Helm") != null);
    try discard(&store, .correct, "", "{\"id\":\"helm\",\"expected_revision\":8,\"lifecycle\":\"retracted\",\"reason\":\"Explicit correction\"}", "retract", 8);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"left\",\"to\":\"right\",\"expected_revision\":9}", "merge-again", 9);
    try discard(&store, .curate_entity, "", "{\"action\":\"unmerge\",\"from\":\"left\",\"expected_revision\":10}", "unmerge-again", 10);
    const hidden = try store.service("alice", true, 500).execute(a, .recall, "", "{\"subject\":\"left\",\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, hidden, "Production uses Helm") == null);
}

test "memory alias writes reconcile with canonical facts and suppression is reversible" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"left\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]},{\"id\":\"right\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]}]}", "entities", 3);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"left\",\"to\":\"right\",\"expected_revision\":4}", "merge", 4);
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.mem.replaceOwned(u8, a, helm, "production", "right"), "helm");
    _ = try store.service("alice", true, 6).execute(a, .assertions, "", try std.mem.replaceOwned(u8, a, terraform, "production", "left"), "terraform");
    const recalled = try store.service("alice", true, 500).execute(a, .recall, "", "{\"subject\":\"right\",\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, recalled, "Production uses Terraform") != null);
    try std.testing.expect(std.mem.indexOf(u8, recalled, "Production uses Helm") == null);
    try std.testing.expectError(error.SuppressedResolverRedirectTarget, store.service("alice", true, 7).execute(a, .curate_entity, "", "{\"action\":\"suppress\",\"from\":\"right\",\"expected_revision\":7}", "reject-absorbed"));
    try discard(&store, .curate_entity, "", "{\"action\":\"suppress\",\"from\":\"right\",\"absorbed\":\"suppress\",\"reason\":\"Hide the entire identity group\",\"expected_revision\":7}", "suppress", 7);
    const hidden = try store.service("alice", true, 500).execute(a, .recall, "", "{\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, hidden, "Production uses Terraform") == null);
    try discard(&store, .curate_entity, "", "{\"action\":\"unsuppress\",\"from\":\"right\",\"expected_revision\":8}", "unsuppress", 8);
    // Suppressing absorbed aliases deliberately removes redirects. Restoring
    // the target does not silently reattach or unsuppress its old aliases.
    try std.testing.expectError(error.MemoryEntitySuppressed, store.service("alice", true, 500).execute(a, .resolve_entity, "left", "{}", ""));
    try discard(&store, .curate_entity, "", "{\"action\":\"unsuppress\",\"from\":\"left\",\"expected_revision\":9}", "unsuppress-left", 9);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"left\",\"to\":\"right\",\"expected_revision\":10}", "restore-merge", 10);
    const restored = try store.service("alice", true, 500).execute(a, .recall, "", "{\"subject\":\"right\",\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, restored, "Production uses Terraform") != null);
}

test "memory identity curation work limit rejects the entire redirect transaction" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    const entities = [_]m.types.Entity{
        .{ .id = "left", .name = "Left", .evidence = &.{.{ .id = "e1" }} },
        .{ .id = "right", .name = "Right", .evidence = &.{.{ .id = "e1" }} },
    };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e1", .source_id = "runbook", .source_version = "v1", .text = "Production uses Helm" }}, .entities = entities }, .{}), "entities");
    var claims: std.ArrayList(m.types.Assertion) = .empty;
    for (0..m.types.max_items) |i| try claims.append(a, .{ .id = try std.fmt.allocPrint(a, "claim-{d}", .{i}), .subject = "left", .predicate = "mentions", .value = "v", .statement = "Grounded fact", .evidence = &.{.{ .id = "e1" }} });
    for (0..2) |page| _ = try store.service("alice", true, @intCast(4 + page)).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims.items[page * 128 .. (page + 1) * 128] }, .{}), try std.fmt.allocPrint(a, "claims-{d}", .{page}));
    const last = [_]m.types.Assertion{.{ .id = "last", .subject = "right", .predicate = "mentions", .value = "v", .statement = "Grounded fact", .evidence = &.{.{ .id = "e1" }} }};
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = last }, .{}), "last");
    const revision = store.revision;
    try std.testing.expectError(error.MemoryWorkLimit, store.run(.curate_entity, "", "{\"action\":\"merge\",\"from\":\"left\",\"to\":\"right\",\"expected_revision\":7}", "oversized-merge", 6));
    try std.testing.expectEqual(revision, store.revision);
    const resolved = try store.service("alice", true, 7).execute(a, .resolve_entity, "left", "{}", "");
    const identity = try std.json.parseFromSliceLeaky(std.json.Value, a, resolved, .{});
    try std.testing.expectEqualStrings("left", identity.object.get("canonical_id").?.string);
    const retained = try store.service("alice", true, 7).execute(a, .get, "last", "{}", "");
    const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, retained, .{});
    try std.testing.expectEqual(m.types.Lifecycle.active, record.lifecycle);
}

test "memory typed object identity merge resolves disputes and unmerge restores them" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .features = .{ .reconciliation = .deterministic }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one, .object_type = "tool" }} };
    _ = try store.service("alice", true, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try store.service("alice", true, 2).execute(a, .observe, "", source, "source");
    const entities = [_]m.types.Entity{
        .{ .id = "Helm", .name = "Helm", .type = "tool", .evidence = &.{.{ .id = "e1" }} },
        .{ .id = "Terraform", .name = "Terraform", .type = "tool", .evidence = &.{.{ .id = "e1" }} },
    };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e1", .source_id = "runbook", .source_version = "v1", .text = "Production uses Helm" }}, .entities = entities }, .{}), "entities");
    const claims = [_]m.types.Assertion{
        .{ .id = "a", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Production uses Helm", .evidence = &.{.{ .id = "e1" }} },
        .{ .id = "b", .subject = "production", .predicate = "deploys_with", .value = "Terraform", .statement = "Production uses Terraform", .evidence = &.{.{ .id = "e1" }} },
    };
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims");
    const disputed = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, "a", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.disputed, disputed.lifecycle);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"Helm\",\"to\":\"Terraform\",\"expected_revision\":5}", "merge", 5);
    const unified = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 6).execute(a, .get, "a", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.active, unified.lifecycle);
    try std.testing.expectEqualStrings("Helm", unified.assertion.value);
    try discard(&store, .curate_entity, "", "{\"action\":\"unmerge\",\"from\":\"Helm\",\"expected_revision\":6}", "unmerge", 6);
    const separated = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 7).execute(a, .get, "a", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.disputed, separated.lifecycle);
}

fn configureTestSpace(store: *Store, config: m.types.Config, token: []const u8, now: i64) ![]const u8 {
    const a = store.arena.allocator();
    const info = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, now).execute(a, .inspect, "", "{}", ""), .{});
    return modelService(store, now).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = info.state.revision, .config = config }, .{}), token);
}
fn recallTestCount(store: *Store, now: i64) !usize {
    const a = store.arena.allocator();
    const result = try store.service("alice", true, now).execute(a, .recall, "", "{\"token_budget\":8192}", "");
    return (try std.json.parseFromSliceLeaky(std.json.Value, a, result, .{})).object.get("items").?.array.items.len;
}
test "memory regression: forgotten entity must not revive facts after cleanup" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    _ = try configureTestSpace(&store, .{ .features = .{ .reconciliation = .deterministic }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one, .subject_type = "concept" }} }, "typed-subject", 3);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e2\",\"source_id\":\"independent\",\"source_version\":\"1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"production\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]}]}", "entities", 3);
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.mem.replaceOwned(u8, a, helm, "e1", "e2"), "claim");
    try std.testing.expectEqual(@as(usize, 1), try recallTestCount(&store, 500));
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget", 501);
    try std.testing.expectEqual(@as(usize, 0), try recallTestCount(&store, 502));
    for (0..10) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), @intCast(503 + i));
    try std.testing.expectEqual(@as(usize, 0), try recallTestCount(&store, 520));
}
test "memory regression: reindex publication must retain admin admission" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "helm", 3);
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, try configureTestSpace(&store, config, "config", 4), .{});
    var revoked = modelService(&store, 5);
    revoked.context.admin = false;
    try std.testing.expectError(error.Forbidden, revoked.execute(a, .advance, pending.id, "{}", "write-only-advance"));
    const info = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try revoked.execute(a, .inspect, "", "{}", ""), .{});
    try std.testing.expect(!info.state.config.features.semantic_recall);
}
test "memory regression: newly typed object predicates must reconcile old facts" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    const entities = [_]m.types.Entity{
        .{ .id = "Helm", .name = "Helm", .type = "tool", .evidence = &.{.{ .id = "e1" }} },
        .{ .id = "Terraform", .name = "Terraform", .type = "tool", .evidence = &.{.{ .id = "e1" }} },
    };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e1", .source_id = "runbook", .source_version = "v1", .text = "Production uses Helm" }}, .entities = entities }, .{}), "entities");
    const claims = [_]m.types.Assertion{
        .{ .id = "a", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Production uses Helm", .evidence = &.{.{ .id = "e1" }} },
        .{ .id = "b", .subject = "production", .predicate = "deploys_with", .value = "Terraform", .statement = "Production uses Terraform", .evidence = &.{.{ .id = "e1" }} },
    };
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims");
    const config: m.types.Config = .{ .features = .{ .reconciliation = .deterministic }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one, .object_type = "tool" }} };
    _ = try configureTestSpace(&store, config, "type-upgrade", 5);
    const inspection = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 6).execute(a, .inspect, "", "{}", ""), .{});
    _ = try store.service("alice", true, 6).execute(a, .curate_entity, "", try std.json.Stringify.valueAlloc(a, .{ .action = "merge", .from = "Helm", .to = "Terraform", .expected_revision = inspection.state.revision }, .{}), "merge");
    const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 7).execute(a, .get, "a", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.active, record.lifecycle);
}
test "memory regression: historical semantic recall must survive model cutover" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const model_a: m.types.Model = .{ .provider = "external", .model = "A", .revision = "A", .dimensions = 2 };
    const model_b: m.types.Model = .{ .provider = "external", .model = "B", .revision = "B", .dimensions = 2 };
    _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, m.types.Config{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = model_a } }, .{}), "create");
    try discard(&store, .observe, "", source, "source", 2);
    const claims = [_]m.types.Assertion{.{ .id = "helm", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Production uses Helm", .evidence = &.{.{ .id = "e1" }}, .embedding = &.{ 1, 0 }, .embedding_model = model_a }};
    _ = try modelService(&store, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claim");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, try configureTestSpace(&store, .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = model_b } }, "B", 5), .{});
    _ = try modelService(&store, 6).execute(a, .stage_embeddings, "", try std.json.Stringify.valueAlloc(a, m.types.EmbeddingBatch{ .generation = pending.config_generation, .model = model_b, .items = &.{.{ .id = "helm", .embedding = &.{ 0, 1 } }} }, .{}), "stage-B");
    _ = try modelService(&store, 7).execute(a, .advance, pending.id, "{}", "publish-B");
    const current = try modelService(&store, 500).execute(a, .recall, "", try std.json.Stringify.valueAlloc(a, .{ .query = "zzzz", .embedding = &[_]f32{ 0, 1 }, .embedding_model = model_b, .token_budget = 8192 }, .{}), "");
    try std.testing.expectEqual(@as(usize, 1), (try std.json.parseFromSliceLeaky(std.json.Value, a, current, .{})).object.get("items").?.array.items.len);
    const historical = try modelService(&store, 500).execute(a, .recall, "", try std.json.Stringify.valueAlloc(a, .{ .query = "zzzz", .embedding = &[_]f32{ 0, 1 }, .embedding_model = model_b, .known_at = 4, .token_budget = 8192 }, .{}), "");
    try std.testing.expectEqual(@as(usize, 1), (try std.json.parseFromSliceLeaky(std.json.Value, a, historical, .{})).object.get("items").?.array.items.len);
    // Historical vectors remain usable while old generations are reclaimed.
    for (0..10) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "retire-vector-{d}", .{i}), @intCast(501 + i));
    const after_cleanup = try modelService(&store, 520).execute(a, .recall, "", try std.json.Stringify.valueAlloc(a, .{ .query = "zzzz", .embedding = &[_]f32{ 0, 1 }, .embedding_model = model_b, .known_at = 4, .token_budget = 8192 }, .{}), "");
    try std.testing.expectEqual(@as(usize, 1), (try std.json.parseFromSliceLeaky(std.json.Value, a, after_cleanup, .{})).object.get("items").?.array.items.len);
    var vectors: usize = 0;
    var entries = store.values.iterator();
    while (entries.next()) |entry| if (std.mem.startsWith(u8, entry.key_ptr.*, "vector/")) {
        vectors += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), vectors);
}
test "memory regression: corrections must not widen private evidence into team reasons" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    const evidence = [_]m.types.Evidence{
        .{ .id = "public", .source_id = "public-source", .source_version = "1", .text = "Production uses Helm", .visibility = .team },
        .{ .id = "private", .source_id = "private-source", .source_version = "1", .text = "Private customer secret" },
    };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = evidence }, .{}), "sources");
    const claims = [_]m.types.Assertion{.{ .id = "public-claim", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Production uses Helm", .visibility = .team, .evidence = &.{.{ .id = "public" }} }};
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claim");
    const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, "public-claim", "{}", ""), .{});
    try std.testing.expectError(error.MemoryVisibilityConflict, store.service("alice", true, 5).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, m.types.Correction{ .id = "public-claim", .expected_revision = record.revision, .lifecycle = .active, .reason = "Private customer secret", .evidence = &.{.{ .id = "private" }} }, .{}), "correct-private"));
    const bob = try store.service("bob", false, 6).execute(a, .get, "public-claim", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, bob, "Private customer secret") == null);
}

test "memory correction provenance is auditable and forgotten reasons are redacted without revival" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    try discard(&store, .assertions, "", helm, "helm", 3);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"correction-source\",\"source_id\":\"correction\",\"source_version\":\"1\",\"text\":\"Private correction justification\"}]}", "correction-source", 4);
    const original = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, "helm", "{}", ""), .{});
    const request: m.types.Correction = .{ .id = "helm", .expected_revision = original.revision, .lifecycle = .retracted, .reason = "Private correction justification", .evidence = &.{.{ .id = "correction-source" }} };
    _ = try store.service("alice", true, 5).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, request, .{}), "correction");
    const corrected = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 6).execute(a, .get, "helm", "{}", ""), .{});
    try std.testing.expectEqualStrings("alice", corrected.explicit_disposition.?.provenance.?.principal);
    try std.testing.expectEqualStrings("correction-source", corrected.explicit_disposition.?.provenance.?.evidence[0].id);
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"correction-source\"]}", "forget-correction", 7);
    const redacted = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 8).execute(a, .get, "helm", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.retracted, redacted.lifecycle);
    try std.testing.expect(redacted.reason == null and redacted.explicit_disposition.?.provenance.?.forgotten);
    for ([_]m.Action{ .history, .list, .recall }) |action| {
        const body = if (action == .history) "{}" else "{\"include_history\":true,\"token_budget\":8192}";
        const result = try store.service("alice", true, 9).execute(a, action, if (action == .history) "helm" else "", body, "");
        try std.testing.expect(std.mem.indexOf(u8, result, "Private correction justification") == null);
    }
    for (0..10) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "scrub-{d}", .{i}), @intCast(10 + i));
    var rows = store.values.iterator();
    while (rows.next()) |entry| if (std.mem.startsWith(u8, entry.key_ptr.*, "assertion/") or std.mem.startsWith(u8, entry.key_ptr.*, "history/")) {
        try std.testing.expect(std.mem.indexOf(u8, entry.value_ptr.*, "Private correction justification") == null);
    };
    try std.testing.expectEqual(@as(usize, 0), try recallTestCount(&store, 500));
}

test "memory ontology cardinality migration rebuilds beliefs and rejects incompatible types atomically" {
    var store = Store.init();
    defer store.deinit();
    try discard(&store, .create, "", "{}", "create", 1);
    try discard(&store, .observe, "", source, "source", 2);
    try discard(&store, .assertions, "", helm, "helm", 3);
    try discard(&store, .assertions, "", terraform, "terraform", 4);
    try std.testing.expectEqual(@as(usize, 2), try recallTestCount(&store, 500));
    const config: m.types.Config = .{ .features = .{ .reconciliation = .deterministic }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one }} };
    _ = try configureTestSpace(&store, config, "single-value", 5);
    try std.testing.expectEqual(@as(usize, 1), try recallTestCount(&store, 500));
    const revision = store.revision;
    var incompatible = config;
    incompatible.predicates = &.{.{ .name = "deploys_with", .cardinality = .one, .object_type = "tool" }};
    try std.testing.expectError(error.MemoryEntityRequired, configureTestSpace(&store, incompatible, "missing-entities", 6));
    try std.testing.expectEqual(revision, store.revision);
    try std.testing.expectEqual(@as(usize, 1), try recallTestCount(&store, 500));
}

test "memory missing typed entity cannot be recreated after payload cleanup" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    const observation = "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"runbook\",\"source_version\":\"v1\",\"text\":\"Production uses Helm\"}],\"entities\":[{\"id\":\"production\",\"name\":\"Production\",\"evidence\":[{\"id\":\"e1\"}]}]}";
    try discard(&store, .observe, "", observation, "entity", 3);
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget", 4);
    for (0..10) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "retire-{d}", .{i}), @intCast(5 + i));
    try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 20).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"fresh\",\"source_id\":\"fresh\",\"source_version\":\"1\",\"text\":\"Fresh unrelated identity\"}],\"entities\":[{\"id\":\"production\",\"name\":\"Unrelated replacement\",\"evidence\":[{\"id\":\"fresh\"}]}]}", "reuse-retired-id"));
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 21).execute(a, .resolve_entity, "production", "{}", ""));
}

test "memory deferred admin grant is rechecked after inference and on receipt replay" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    try discard(&store, .assertions, "", helm, "claim", 3);
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, try configureTestSpace(&store, .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } }, "configuration", 4), .{});
    const Grant = struct {
        fn require(ptr: *anyopaque, grant: m.types.RequiredGrant) !void {
            const owner: *Store = @ptrCast(@alignCast(ptr));
            if (grant == .admin) owner.required_grant = .admin;
            if (owner.public_output and owner.required_grant == .admin) return error.Forbidden;
        }
        fn finish(ptr: *anyopaque) !void {
            return require(ptr, .write);
        }
        fn embed(ptr: *anyopaque, alloc: A, _: m.types.Model, _: []const u8) ![]const f32 {
            const owner: *Store = @ptrCast(@alignCast(ptr));
            owner.public_output = true;
            return alloc.dupe(f32, &.{ 1, 0 });
        }
    };
    var service = modelService(&store, 5);
    service.context.grant_ptr = &store;
    service.context.require_grant_fn = Grant.require;
    service.context.final_check_ptr = &store;
    service.context.final_check_fn = Grant.finish;
    service.backend.embed = Grant.embed;
    const revision = store.revision;
    try std.testing.expectError(error.Forbidden, service.execute(a, .advance, pending.id, "{}", "revoked-during-inference"));
    try std.testing.expectEqual(m.types.RequiredGrant.admin, store.required_grant);
    try std.testing.expectEqual(revision, store.revision);
    store.public_output = false;
    _ = try modelService(&store, 6).execute(a, .advance, pending.id, "{}", "authorized-publication");
    var revoked = modelService(&store, 7);
    revoked.context.admin = false;
    try std.testing.expectError(error.Forbidden, revoked.execute(a, .advance, pending.id, "{}", "authorized-publication"));
}

test "memory ontology transition work limit leaves configuration and beliefs untouched" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    var claims: std.ArrayList(m.types.Assertion) = .empty;
    for (0..257) |i| try claims.append(a, .{ .id = try std.fmt.allocPrint(a, "claim-{d}", .{i}), .subject = "production", .predicate = "mentions", .value = "v", .statement = "Grounded fact", .evidence = &.{.{ .id = "e1" }} });
    for (0..3) |page| _ = try store.service("alice", true, @intCast(3 + page)).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims.items[page * 128 .. @min(claims.items.len, (page + 1) * 128)] }, .{}), try std.fmt.allocPrint(a, "page-{d}", .{page}));
    const revision = store.revision;
    try std.testing.expectError(error.MemoryWorkLimit, configureTestSpace(&store, .{ .features = .{ .reconciliation = .deterministic }, .predicates = &.{.{ .name = "mentions", .cardinality = .one }} }, "oversized-ontology", 6));
    try std.testing.expectEqual(revision, store.revision);
    const info = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 7).execute(a, .inspect, "", "{}", ""), .{});
    try std.testing.expectEqualStrings("deploys_with", info.state.config.predicates[0].name);
    try std.testing.expect(info.state.pending_config == null);
}

fn contextDerivedOutput(_: *anyopaque, a: A, _: m.types.Config, _: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
    const claims = try a.alloc(m.types.Assertion, 1);
    claims[0] = .{ .id = "derived", .subject = "production", .predicate = "guidance", .value = "Secret", .statement = "Private context secret", .evidence = &.{.{ .id = "current" }}, .support_sets = &.{.{ .evidence = &.{.{ .id = "alternative" }} }} };
    const entities = try a.alloc(m.types.Entity, 1);
    entities[0] = .{ .id = "derived-entity", .name = "Private context secret", .evidence = &.{.{ .id = "current" }} };
    return .{ .assertions = claims, .entities = entities };
}
fn contextDerivedSetup(store: *Store, expires: bool) !void {
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .hybrid, .features = .{ .assertion_distillation = true, .entity_extraction = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    const context: m.types.Evidence = .{ .id = "context", .source_id = "private-note", .source_version = "1", .text = "Private context secret", .expires_at = if (expires) 50 else null };
    _ = try store.service("context-owner", false, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{context} }, .{}), "context");
    const accepted = try modelService(store, 3).execute(a, .observe, "",
        \\{"evidence":[{"id":"current","source_id":"public-note","source_version":"1","text":"Deploy it","managed":true,"visibility":"team"},{"id":"alternative","source_id":"alternative-note","source_version":"1","text":"Deploy it too","managed":true,"visibility":"team"}],"context_evidence_ids":["context"]}
    , "capture");
    const operation = try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{});
    var service = modelService(store, 4);
    service.backend.generate = contextDerivedOutput;
    _ = try service.execute(a, .advance, operation.id, "{}", "advance");
    _ = try modelService(store, 5).execute(a, .observe, "",
        \\{"evidence":[{"id":"external","source_id":"external","source_version":"1","text":"Independent child support"}],"assertions":[{"id":"child","subject":"production","predicate":"child","value":"child","statement":"Child of generated guidance","evidence":[{"id":"external"}],"depends_on":["derived"]}]}
    , "child");
}

test "memory full generation inputs fence assertions entities and descendants on forgetting or expiry" {
    for (0..3) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try contextDerivedSetup(&store, mode == 2);
        _ = try modelService(&store, 6).execute(a, .get, "derived", "{}", "");
        _ = try modelService(&store, 6).execute(a, .resolve_entity, "derived-entity", "{}", "");
        if (mode < 2) _ = try modelService(&store, 7).execute(a, .forget, "", if (mode == 0) "{\"evidence_ids\":[\"context\"]}" else "{\"source_ids\":[\"private-note\"]}", "forget");
        for ([_][]const u8{ "derived", "child" }) |id| {
            try std.testing.expectError(error.MemoryNotFound, modelService(&store, 51).execute(a, .get, id, "{}", ""));
            try std.testing.expectError(error.MemoryNotFound, modelService(&store, 51).execute(a, .history, id, "{}", ""));
        }
        try std.testing.expectError(error.MemoryNotFound, modelService(&store, 51).execute(a, .resolve_entity, "derived-entity", "{}", ""));
        for ([_]m.Action{ .list, .recall, .entities }) |action| {
            const raw = try modelService(&store, 51).execute(a, action, "", if (action == .entities) "{}" else "{\"known_at\":6,\"include_history\":true,\"token_budget\":8192}", "");
            const value = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
            try std.testing.expectEqual(@as(usize, 0), value.object.get("items").?.array.items.len);
        }
        for (0..12) |i| _ = try modelService(&store, @intCast(52 + i)).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}));
        var rows = store.values.iterator();
        while (rows.next()) |row| if (std.mem.startsWith(u8, row.key_ptr.*, "assertion/") or std.mem.startsWith(u8, row.key_ptr.*, "history/") or std.mem.startsWith(u8, row.key_ptr.*, "entity/")) {
            try std.testing.expect(std.mem.indexOf(u8, row.value_ptr.*, "Private context secret") == null);
        };
        try std.testing.expectError(error.MemoryNotFound, modelService(&store, 70).execute(a, .get, "derived", "{}", ""));
    }
}

test "memory replay cannot replace immutable generation provenance with another context plan" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try contextDerivedSetup(&store, false);
    _ = try modelService(&store, 6).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"other-context\",\"source_id\":\"other\",\"source_version\":\"1\",\"text\":\"Unrelated context\"}]}", "other-context");
    const accepted = try modelService(&store, 7).execute(a, .observe, "",
        \\{"evidence":[{"id":"current","source_id":"public-note","source_version":"1","text":"Deploy it","managed":true,"visibility":"team"},{"id":"alternative","source_id":"alternative-note","source_version":"1","text":"Deploy it too","managed":true,"visibility":"team"}],"context_evidence_ids":["other-context"]}
    , "reprocess");
    const operation = try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{});
    var service = modelService(&store, 8);
    service.backend.generate = contextDerivedOutput;
    _ = try service.execute(a, .advance, operation.id, "{}", "advance-again");
    _ = try modelService(&store, 9).execute(a, .forget, "", "{\"evidence_ids\":[\"other-context\"]}", "forget-other");
    _ = try modelService(&store, 10).execute(a, .get, "derived", "{}", "");
    _ = try modelService(&store, 10).execute(a, .resolve_entity, "derived-entity", "{}", "");
    _ = try modelService(&store, 11).execute(a, .forget, "", "{\"evidence_ids\":[\"context\"]}", "forget-original");
    try std.testing.expectError(error.MemoryNotFound, modelService(&store, 12).execute(a, .get, "derived", "{}", ""));
}

test "memory distinct constraints cover both complete identity components atomically" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    try discard(&store, .observe, "",
        \\{"evidence":[{"id":"entity-e","source_id":"entities","source_version":"1","text":"A B C D"}],"entities":[{"id":"a","name":"Alex","distinct_from":["b"],"evidence":[{"id":"entity-e"}]},{"id":"b","name":"Blair","evidence":[{"id":"entity-e"}]},{"id":"c","name":"Chris","evidence":[{"id":"entity-e"}]},{"id":"d","name":"Drew","evidence":[{"id":"entity-e"}]}]}
    , "entities", 3);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"a\",\"to\":\"d\",\"expected_revision\":4}", "ad", 4);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"b\",\"to\":\"c\",\"expected_revision\":5}", "bc", 5);
    const revision = store.revision;
    for ([_][2][]const u8{ .{ "d", "c" }, .{ "c", "d" }, .{ "a", "c" } }, 0..) |pair, i| {
        const body = try std.json.Stringify.valueAlloc(a, .{ .action = "merge", .from = pair[0], .to = pair[1], .replace = true, .expected_revision = revision + 1 }, .{});
        try std.testing.expectError(error.MemoryEntityConflict, store.service("alice", true, 6).execute(a, .curate_entity, "", body, try std.fmt.allocPrint(a, "rejected-{d}", .{i})));
        try std.testing.expectEqual(revision, store.revision);
    }
    for ([_][2][]const u8{ .{ "a", "d" }, .{ "b", "c" } }) |pair| {
        const raw = try store.service("alice", true, 7).execute(a, .resolve_entity, pair[0], "{}", "");
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        try std.testing.expectEqualStrings(pair[1], result.object.get("canonical_id").?.string);
    }
}

test "memory cleanup retires assertion identities without poisoning surviving group indexes" {
    for (0..3) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{\"features\":{\"reconciliation\":\"deterministic\"},\"predicates\":[{\"name\":\"deploys_with\",\"cardinality\":\"one\"}]}", "create", 1);
        const evidence: m.types.Evidence = .{ .id = "e1", .source_id = "runbook", .source_version = "v1", .text = "Production uses Helm", .expires_at = if (mode == 2) 5 else null };
        _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "source");
        try discard(&store, .assertions, "", helm, "claim", 3);
        if (mode < 2) try discard(&store, .forget, "", if (mode == 0) "{\"evidence_ids\":[\"e1\"]}" else "{\"source_ids\":[\"runbook\"]}", "forget", 4);
        for (0..10) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), @intCast(5 + i));
        try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e2\",\"source_id\":\"fresh\",\"source_version\":\"1\",\"text\":\"Other fact\"}]}", "fresh", 20);
        try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 21).execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"helm\",\"subject\":\"different-subject\",\"predicate\":\"deploys_with\",\"value\":\"Other\",\"statement\":\"Other fact\",\"evidence\":[{\"id\":\"e2\"}]}]}", "reuse"));
        try discard(&store, .assertions, "", "{\"assertions\":[{\"id\":\"candidate\",\"subject\":\"production\",\"predicate\":\"deploys_with\",\"value\":\"Other\",\"statement\":\"Other fact\",\"evidence\":[{\"id\":\"e2\"}]}]}", "candidate", 22);
        try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 23).execute(a, .history, "helm", "{}", ""));
        // Replaying an old receipt acknowledges admission without restoring data.
        try discard(&store, .assertions, "", helm, "claim", 24);
        try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 25).execute(a, .get, "helm", "{}", ""));
        try std.testing.expectEqual(@as(usize, 1), try recallTestCount(&store, 26));
    }
}

fn chainClaim(a: A, i: usize) !m.types.Assertion {
    const id = try std.fmt.allocPrint(a, "chain-{d}", .{i});
    const parents = try a.alloc([]const u8, if (i == 0) 0 else 1);
    if (i > 0) parents[0] = try std.fmt.allocPrint(a, "chain-{d}", .{i - 1});
    return .{ .id = id, .subject = "chain", .predicate = "fact", .value = id, .statement = id, .evidence = &.{.{ .id = "e1" }}, .depends_on = parents };
}
test "memory dependency admission includes the new root and every alternative support branch" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    for (0..32) |i| {
        const claim = try chainClaim(a, i);
        _ = try store.service("alice", true, @intCast(i + 3)).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), claim.id);
    }
    const revision = store.revision;
    var overflow = try chainClaim(a, 32);
    try std.testing.expectError(error.MemoryWorkLimit, store.service("alice", true, 40).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{overflow} }, .{}), "overflow"));
    try std.testing.expectEqual(revision, store.revision);
    overflow.depends_on = &.{};
    overflow.support_sets = &.{.{ .evidence = overflow.evidence, .depends_on = &.{"chain-31"} }};
    try std.testing.expectError(error.MemoryWorkLimit, store.service("alice", true, 41).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{overflow} }, .{}), "alternative-overflow"));
    try std.testing.expectEqual(revision, store.revision);
    _ = try store.service("alice", true, 42).execute(a, .get, "chain-31", "{}", "");
    _ = try store.service("alice", true, 42).execute(a, .recall, "", "{\"token_budget\":8192}", "");
}

test "memory over-depth batch admission rolls back all assertions and receipts" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    var claims: [33]m.types.Assertion = undefined;
    for (&claims, 0..) |*claim, i| claim.* = try chainClaim(a, i);
    const revision = store.revision;
    try std.testing.expectError(error.MemoryWorkLimit, store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &claims }, .{}), "batch"));
    try std.testing.expectEqual(revision, store.revision);
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 4).execute(a, .get, "chain-0", "{}", ""));
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims[0..32] }, .{}), "batch");
    _ = try store.service("alice", true, 6).execute(a, .recall, "", "{\"token_budget\":8192}", "");
}

test "memory short visible support cannot conceal over-depth alternative ancestors" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    for (0..31) |i| {
        const claim = try chainClaim(a, i);
        _ = try store.service("alice", true, @intCast(i + 3)).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), claim.id);
    }
    const bridge: m.types.Assertion = .{ .id = "bridge", .subject = "chain", .predicate = "fact", .value = "bridge", .statement = "Short primary with deep independent support", .evidence = &.{.{ .id = "e1" }}, .support_sets = &.{.{ .evidence = &.{.{ .id = "e1" }}, .depends_on = &.{"chain-30"} }} };
    _ = try store.service("alice", true, 40).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{bridge} }, .{}), "bridge");
    const child: m.types.Assertion = .{ .id = "overflow", .subject = "chain", .predicate = "fact", .value = "child", .statement = "Would be unreadable after support changes", .evidence = &.{.{ .id = "e1" }}, .depends_on = &.{"bridge"} };
    const revision = store.revision;
    try std.testing.expectError(error.MemoryWorkLimit, store.service("alice", true, 41).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{child} }, .{}), "overflow"));
    try std.testing.expectEqual(revision, store.revision);
    _ = try store.service("alice", true, 42).execute(a, .recall, "", "{\"token_budget\":8192}", "");
}

test "memory generation context authorization survives loss of administrative visibility" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try contextDerivedSetup(&store, false);
    const reader = store.service("alice", false, 6);
    for ([_][]const u8{ "derived", "child" }) |id| {
        try std.testing.expectError(error.MemoryNotFound, reader.execute(a, .get, id, "{}", ""));
        try std.testing.expectError(error.MemoryNotFound, reader.execute(a, .history, id, "{}", ""));
    }
    try std.testing.expectError(error.MemoryNotFound, reader.execute(a, .resolve_entity, "derived-entity", "{}", ""));
    for ([_]m.Action{ .recall, .list, .entities }) |action| {
        const raw = try reader.execute(a, action, "", "{}", "");
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        try std.testing.expectEqual(@as(usize, 0), result.object.get("items").?.array.items.len);
    }
    _ = try modelService(&store, 7).execute(a, .get, "derived", "{}", "");
}

test "memory alias replacement checks the proposed components without moving the old target" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    try discard(&store, .observe, "",
        \\{"evidence":[{"id":"identity","source_id":"identity","source_version":"1","text":"Three identities"}],"entities":[{"id":"alias","name":"Alias","evidence":[{"id":"identity"}]},{"id":"old","name":"Old","distinct_from":["target"],"evidence":[{"id":"identity"}]},{"id":"target","name":"Target","evidence":[{"id":"identity"}]}]}
    , "identities", 3);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"alias\",\"to\":\"old\",\"expected_revision\":4}", "merge", 4);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"alias\",\"to\":\"target\",\"replace\":true,\"expected_revision\":5}", "replace", 5);
    for ([_][2][]const u8{ .{ "alias", "target" }, .{ "old", "old" } }) |pair| {
        const raw = try store.service("alice", true, 6).execute(a, .resolve_entity, pair[0], "{}", "");
        const value = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        try std.testing.expectEqualStrings(pair[1], value.object.get("canonical_id").?.string);
    }
}

fn reviewDecide(_: *anyopaque, a: A, _: m.types.Config, _: m.types.Assertion, candidates: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
    return .{ .supersedes = try a.dupe([]const u8, &.{candidates[0].assertion.id}), .reason = try std.fmt.allocPrint(a, "Replaces previous statement: {s}", .{candidates[0].assertion.statement}) };
}
test "memory model decision explanations forget their inference inputs" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var svc = modelService(&store, 1);
    svc.backend.decide = reviewDecide;
    const config: m.types.Config = .{ .features = .{ .reconciliation = .model }, .models = .{ .decider = fake_model }, .predicates = &.{.{ .name = "credential", .cardinality = .one }} };
    _ = try svc.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try store.service("alice", true, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"old-source\",\"source_id\":\"old\",\"source_version\":\"1\",\"text\":\"Old password secret-123\"},{\"id\":\"new-source\",\"source_id\":\"new\",\"source_version\":\"1\",\"text\":\"Password has rotated\"}]}", "sources");
    svc = modelService(&store, 3);
    svc.backend.decide = reviewDecide;
    _ = try svc.execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"old\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"old\",\"statement\":\"Old password secret-123\",\"valid_from\":100,\"evidence\":[{\"id\":\"old-source\"}]}]}", "old");
    svc = modelService(&store, 4);
    svc.backend.decide = reviewDecide;
    _ = try svc.execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"new\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"new\",\"statement\":\"Password has rotated\",\"valid_from\":300,\"evidence\":[{\"id\":\"new-source\"}]}]}", "new");
    _ = try store.service("alice", true, 5).execute(a, .forget, "", "{\"evidence_ids\":[\"old-source\"]}", "forget");
    const raw = try store.service("alice", true, 6).execute(a, .get, "new", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, raw, "secret-123") == null);
}
test "memory explicit reconciliation remains explicit through configuration changes" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{\"predicates\":[{\"name\":\"deploys_with\",\"cardinality\":\"one\"}]}", "create", 1);
    try discard(&store, .observe, "", source, "source", 2);
    try discard(&store, .assertions, "", helm, "helm", 3);
    try discard(&store, .assertions, "", terraform, "terraform", 4);
    var before = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, "helm", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.active, before.lifecycle);
    const config: m.types.Config = .{ .policy_version = "v2", .predicates = &.{.{ .name = "deploys_with", .cardinality = .one }} };
    _ = try configureTestSpace(&store, config, "config", 6);
    before = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 7).execute(a, .get, "helm", "{}", ""), .{});
    try std.testing.expectEqual(m.types.Lifecycle.active, before.lifecycle);
}
test "memory shared dependency graphs bound point recall and historical read work" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    for (0..10) |layer| {
        var claims: [2]m.types.Assertion = undefined;
        for (&claims, 0..) |*claim, side| {
            const id = try std.fmt.allocPrint(a, "node-{d}-{d}", .{ layer, side });
            const parents = try a.alloc([]const u8, if (layer == 0) 0 else 2);
            if (layer > 0) for (parents, 0..) |*parent, j| {
                parent.* = try std.fmt.allocPrint(a, "node-{d}-{d}", .{ layer - 1, j });
            };
            claim.* = .{ .id = id, .subject = "graph", .predicate = "note", .value = id, .statement = id, .evidence = &.{.{ .id = "e1" }}, .depends_on = parents };
        }
        _ = try store.service("alice", true, @intCast(layer + 3)).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &claims }, .{}), try std.fmt.allocPrint(a, "layer-{d}", .{layer}));
    }
    store.reads = 0;
    _ = try store.service("alice", true, 20).execute(a, .get, "node-9-0", "{}", "");
    try std.testing.expect(store.reads < 300);
    for ([_]m.Action{ .recall, .list, .history }) |action| {
        store.reads = 0;
        store.scans = 0;
        _ = try store.service("alice", true, 21).execute(a, action, if (action == .history) "node-9-0" else "", if (action == .history) "{}" else "{\"known_at\":20,\"token_budget\":8192}", "");
        try std.testing.expect(store.reads < 1000);
        try std.testing.expect(store.scans < 100);
    }
}

fn decisionService(store: *Store, now: i64) m.Service {
    var service = modelService(store, now);
    service.backend.decide = lineageDecider;
    return service;
}
fn lineageDecider(ptr: *anyopaque, a: A, _: m.types.Config, _: m.types.Assertion, prior: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
    const store: *Store = @ptrCast(@alignCast(ptr));
    store.decisions += 1;
    if (store.reject_decision) return error.TransientInferenceFailure;
    return .{ .supersedes = try a.dupe([]const u8, &.{prior[0].assertion.id}), .reason = try std.json.Stringify.valueAlloc(a, prior, .{}) };
}
fn decisionConfig() m.types.Config {
    return .{ .features = .{ .reconciliation = .model }, .models = .{ .decider = fake_model }, .predicates = &.{.{ .name = "credential", .cardinality = .one }} };
}
fn decisionSetup(store: *Store, expiry: bool) !void {
    const a = store.arena.allocator();
    _ = try decisionService(store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, decisionConfig(), .{}), "create");
    const source_value: m.types.Evidence = .{ .id = "old-source", .source_id = "old", .source_version = "1", .text = "Old password secret-123", .expires_at = if (expiry) 10 else null };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{source_value} }, .{}), "old-source");
    _ = try decisionService(store, 3).execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"old\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"old\",\"statement\":\"Old password secret-123\",\"valid_from\":100,\"evidence\":[{\"id\":\"old-source\"}]}]}", "old");
    try decisionAdd(store, "new", 200, 4);
}
fn decisionAdd(store: *Store, id: []const u8, valid: i64, now: i64) !void {
    const a = store.arena.allocator();
    const evidence_value: m.types.Evidence = .{ .id = id, .source_id = id, .source_version = "1", .text = "Password rotated" };
    _ = try store.service("alice", true, now).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence_value} }, .{}), try std.fmt.allocPrint(a, "source-{s}", .{id}));
    const claim: m.types.Assertion = .{ .id = id, .subject = "production", .predicate = "credential", .value = id, .statement = "Password rotated", .valid_from = valid, .evidence = &.{.{ .id = id }} };
    _ = try decisionService(store, now).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), id);
}
test "memory decision lineage redacts all views and scrubs storage without retracting independent facts" {
    for (0..4) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try decisionSetup(&store, mode == 2);
        try decisionAdd(&store, "latest", 300, 5);
        const before = try store.service("alice", true, 6).execute(a, .get, "latest", "{}", "");
        try std.testing.expect(std.mem.indexOf(u8, before, "secret-123") != null);
        if (mode != 2) _ = try store.service("alice", true, 7).execute(a, .forget, "", if (mode == 0) "{\"evidence_ids\":[\"old-source\"]}" else if (mode == 1) "{\"assertion_ids\":[\"old\"]}" else "{\"source_ids\":[\"old\"]}", "forget");
        for ([_]m.Action{ .get, .history, .list, .recall }) |action| {
            const raw = try store.service("alice", true, 11).execute(a, action, if (action == .get or action == .history) "latest" else "", if (action == .get or action == .history) "{}" else "{\"known_at\":6,\"include_history\":true,\"token_budget\":8192}", "");
            try std.testing.expect(std.mem.indexOf(u8, raw, "secret-123") == null);
        }
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 12).execute(a, .get, "latest", "{}", ""), .{});
        try std.testing.expectEqual(m.types.Lifecycle.active, record.lifecycle);
        try std.testing.expect(record.assertion.reconciliation_reason == null);
        for (0..16) |i| _ = try store.service("alice", true, @intCast(i + 13)).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "scrub-{d}", .{i}));
        var rows = store.values.iterator();
        while (rows.next()) |row| if (std.mem.startsWith(u8, row.key_ptr.*, "assertion/") or std.mem.startsWith(u8, row.key_ptr.*, "history/")) {
            try std.testing.expect(std.mem.indexOf(u8, row.value_ptr.*, "secret-123") == null);
        };
        _ = try store.service("alice", true, 500).execute(a, .get, "latest", "{}", "");
    }
}
test "memory a new model decision never sees already redacted candidate explanations" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try decisionSetup(&store, false);
    _ = try store.service("alice", true, 5).execute(a, .forget, "", "{\"evidence_ids\":[\"old-source\"]}", "forget");
    try decisionAdd(&store, "latest", 300, 6);
    const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 7).execute(a, .get, "latest", "{}", ""), .{});
    try std.testing.expect(record.assertion.reconciliation_reason != null);
    try std.testing.expect(std.mem.indexOf(u8, record.assertion.reconciliation_reason.?, "secret-123") == null);
}
test "memory manual correction replaces model disposition lineage but preserves assertion reason fences" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try decisionSetup(&store, false);
    try decisionAdd(&store, "latest", 300, 5);
    _ = try store.service("alice", true, 6).execute(a, .forget, "", "{\"evidence_ids\":[\"old-source\"]}", "forget");
    const current = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 7).execute(a, .get, "new", "{}", ""), .{});
    const correction: m.types.Correction = .{ .id = "new", .expected_revision = current.revision, .lifecycle = .retracted, .reason = "Human reviewed the rotation", .evidence = &.{.{ .id = "new" }} };
    _ = try store.service("alice", true, 8).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, correction, .{}), "manual");
    const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 9).execute(a, .get, "new", "{}", ""), .{});
    try std.testing.expectEqualStrings("Human reviewed the rotation", record.reason.?);
    try std.testing.expect(record.assertion.reconciliation_reason == null);
}
test "memory model configuration rebuild uses the decider and fails atomically" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{\"predicates\":[{\"name\":\"credential\",\"cardinality\":\"one\"}]}", "create", 1);
    _ = try store.service("alice", true, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"note\",\"source_version\":\"1\",\"text\":\"Two unknown values\"}],\"assertions\":[{\"id\":\"a\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"a\",\"statement\":\"A\",\"evidence\":[{\"id\":\"e1\"}]},{\"id\":\"b\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"b\",\"statement\":\"B\",\"evidence\":[{\"id\":\"e1\"}]}]}", "claims");
    const inspection = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 3).execute(a, .inspect, "", "{}", ""), .{});
    const body = try std.json.Stringify.valueAlloc(a, .{ .expected_revision = inspection.state.revision, .config = decisionConfig() }, .{});
    const revision = store.revision;
    store.reject_decision = true;
    try std.testing.expectError(error.TransientInferenceFailure, decisionService(&store, 4).execute(a, .configure, "", body, "configure"));
    try std.testing.expectEqual(revision, store.revision);
    for ([_][]const u8{ "a", "b" }) |id| {
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, id, "{}", ""), .{});
        try std.testing.expectEqual(m.types.Lifecycle.active, record.lifecycle);
    }
    store.reject_decision = false;
    _ = try decisionService(&store, 6).execute(a, .configure, "", body, "configure");
    var active: usize = 0;
    var superseded: usize = 0;
    for ([_][]const u8{ "a", "b" }) |id| {
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 7).execute(a, .get, id, "{}", ""), .{});
        if (record.lifecycle == .active) active += 1;
        if (record.lifecycle == .superseded) superseded += 1;
        try std.testing.expect(record.lifecycle != .disputed);
    }
    try std.testing.expectEqual(@as(usize, 1), active);
    try std.testing.expectEqual(@as(usize, 1), superseded);
    try std.testing.expect(store.decisions >= 2);
}

test "memory administrative identity rebuild keeps private model groups partitioned by owner" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try decisionService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, decisionConfig(), .{}), "create");
    _ = try store.service("bob", false, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"bob-e\",\"source_id\":\"bob\",\"source_version\":\"1\",\"text\":\"Two identities\"}],\"entities\":[{\"id\":\"left\",\"name\":\"Left\",\"evidence\":[{\"id\":\"bob-e\"}]},{\"id\":\"right\",\"name\":\"Right\",\"evidence\":[{\"id\":\"bob-e\"}]}]}", "identities");
    var producer = decisionService(&store, 3);
    producer.context.principal = "bob";
    producer.context.admin = false;
    _ = try producer.execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"a\",\"subject\":\"left\",\"predicate\":\"credential\",\"value\":\"a\",\"statement\":\"A\",\"evidence\":[{\"id\":\"bob-e\"}]},{\"id\":\"b\",\"subject\":\"right\",\"predicate\":\"credential\",\"value\":\"b\",\"statement\":\"B\",\"evidence\":[{\"id\":\"bob-e\"}]}]}", "claims");
    const inspection = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 4).execute(a, .inspect, "", "{}", ""), .{});
    _ = try decisionService(&store, 5).execute(a, .curate_entity, "", try std.json.Stringify.valueAlloc(a, .{ .action = "merge", .from = "left", .to = "right", .expected_revision = inspection.state.revision }, .{}), "merge");
    var active: usize = 0;
    for ([_][]const u8{ "a", "b" }) |id| {
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("bob", false, 6).execute(a, .get, id, "{}", ""), .{});
        try std.testing.expectEqualStrings("bob", record.owner);
        if (record.lifecycle == .active) active += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), active);
    try std.testing.expect(store.decisions > 0);
}
test "memory support work quota rejects an expensive import without partial publication" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{\"max_input_bytes\":4194304}", "create", 1);
    try discard(&store, .observe, "", source, "source", 2);
    const parent: m.types.Assertion = .{ .id = "parent", .subject = "graph", .predicate = "note", .value = "parent", .statement = "Parent", .evidence = &.{.{ .id = "e1" }} };
    _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{parent} }, .{}), "parent");
    var parents: [256][]const u8 = undefined;
    @memset(&parents, "parent");
    var claims: [128]m.types.Assertion = undefined;
    for (&claims, 0..) |*claim, i| {
        const id = try std.fmt.allocPrint(a, "expensive-{d}", .{i});
        claim.* = .{ .id = id, .subject = "graph", .predicate = "note", .value = id, .statement = id, .evidence = &.{.{ .id = "e1" }}, .depends_on = &parents };
    }
    const revision = store.revision;
    try std.testing.expectError(error.MemoryWorkLimit, store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &claims }, .{}), "expensive"));
    try std.testing.expectEqual(revision, store.revision);
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 5).execute(a, .get, "expensive-0", "{}", ""));
    _ = try store.service("alice", true, 6).execute(a, .get, "parent", "{}", "");
}

fn declineFirstDecider(ptr: *anyopaque, a: A, config: m.types.Config, incoming: m.types.Assertion, prior: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
    if (std.mem.eql(u8, incoming.id, "a")) return .{ .supersedes = &.{}, .reason = "Keep existing alternatives" };
    return lineageDecider(ptr, a, config, incoming, prior);
}
test "memory decision inputs retain correction evidence before same-revision projection replacement" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{\"predicates\":[{\"name\":\"credential\",\"cardinality\":\"one\"}]}", "create", 1);
    _ = try store.service("alice", true, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"note\",\"source_version\":\"1\",\"text\":\"Alternatives\"},{\"id\":\"justification\",\"source_id\":\"review\",\"source_version\":\"1\",\"text\":\"Sensitive justification\"}],\"assertions\":[{\"id\":\"a\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"a\",\"statement\":\"A\",\"evidence\":[{\"id\":\"e1\"}]},{\"id\":\"b\",\"subject\":\"production\",\"predicate\":\"credential\",\"value\":\"b\",\"statement\":\"B\",\"evidence\":[{\"id\":\"e1\"}]}]}", "claims");
    const current = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 3).execute(a, .get, "a", "{}", ""), .{});
    const correction: m.types.Correction = .{ .id = "a", .expected_revision = current.revision, .lifecycle = .active, .reason = "Sensitive justification", .evidence = &.{.{ .id = "justification" }} };
    _ = try store.service("alice", true, 4).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, correction, .{}), "correct");
    const inspected = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 5).execute(a, .inspect, "", "{}", ""), .{});
    var service = decisionService(&store, 6);
    service.backend.decide = declineFirstDecider;
    _ = try service.execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = inspected.state.revision, .config = decisionConfig() }, .{}), "model");
    const before = try store.service("alice", true, 7).execute(a, .get, "b", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, before, "Sensitive justification") != null);
    _ = try store.service("alice", true, 8).execute(a, .forget, "", "{\"evidence_ids\":[\"justification\"]}", "forget");
    for ([_][]const u8{ "a", "b" }) |id| {
        const raw = try store.service("alice", true, 9).execute(a, .get, id, "{}", "");
        try std.testing.expect(std.mem.indexOf(u8, raw, "Sensitive justification") == null);
    }
}

fn repeatIncomingExplanation(_: *anyopaque, a: A, _: m.types.Config, incoming: m.types.Assertion, _: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
    return .{ .supersedes = &.{}, .reason = try a.dupe(u8, incoming.reconciliation_reason orelse "No replacement") };
}
fn requireRedactedIncomingExplanation(ptr: *anyopaque, a: A, config: m.types.Config, incoming: m.types.Assertion, prior: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
    if (incoming.reconciliation_reason) |reason| try std.testing.expect(std.mem.indexOf(u8, reason, "secret-123") == null);
    try std.testing.expect(std.mem.indexOf(u8, try std.json.Stringify.valueAlloc(a, prior, .{}), "secret-123") == null);
    return repeatIncomingExplanation(ptr, a, config, incoming, prior);
}
test "memory rebuilt incoming explanations preserve provenance across forgetting and cleanup" {
    for ([_]usize{ 1, 2 }) |rebuild_count| {
        for ([_][]const u8{ "competitor-0", "competitor-41" }) |competitor| {
            for (0..4) |mode| {
                var store = Store.init();
                defer store.deinit();
                const a = store.arena.allocator();
                try decisionSetup(&store, mode == 2);
                var service = decisionService(&store, 5);
                service.backend.decide = repeatIncomingExplanation;
                const claim: m.types.Assertion = .{ .id = competitor, .subject = "production", .predicate = "credential", .value = competitor, .statement = "Independent third value", .stability = .provisional, .valid_from = 300, .evidence = &.{.{ .id = "new" }} };
                _ = try service.execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), "competitor");
                const current = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, competitor, "{}", ""), .{});
                const correction: m.types.Correction = .{ .id = competitor, .expected_revision = current.revision, .lifecycle = .active, .stability = .settled, .reason = "Verified", .evidence = &.{.{ .id = "new" }} };
                _ = try store.service("alice", true, 6).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, correction, .{}), "settle");
                const inspected = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 6).execute(a, .inspect, "", "{}", ""), .{});
                var config = decisionConfig();
                var expected_revision = inspected.state.revision;
                const versions = [_][]const u8{ "v2", "v3" };
                for (versions[0..rebuild_count]) |version| {
                    config.policy_version = version;
                    service = decisionService(&store, 7);
                    service.backend.decide = repeatIncomingExplanation;
                    _ = try service.execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = expected_revision, .config = config }, .{}), version);
                    expected_revision += 1;
                }
                const before = try store.service("alice", true, 8).execute(a, .get, "new", "{}", "");
                try std.testing.expect(std.mem.indexOf(u8, before, "secret-123") != null);
                if (mode != 2) _ = try store.service("alice", true, 11).execute(a, .forget, "", if (mode == 0) "{\"evidence_ids\":[\"old-source\"]}" else if (mode == 1) "{\"assertion_ids\":[\"old\"]}" else "{\"source_ids\":[\"old\"]}", "forget");
                const after = try store.service("alice", true, 11).execute(a, .get, "new", "{}", "");
                try std.testing.expect(std.mem.indexOf(u8, after, "secret-123") == null);
                for ([_]m.Action{ .history, .list, .recall }) |action| {
                    const raw = try store.service("alice", true, 11).execute(a, action, if (action == .history) "new" else "", if (action == .history) "{}" else "{\"valid_at\":400,\"token_budget\":8192}", "");
                    try std.testing.expect(std.mem.indexOf(u8, raw, "secret-123") == null);
                }
                for (0..32) |step| _ = try store.service("alice", true, @intCast(step + 12)).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{step}));
                const cleaned = try store.service("alice", true, 50).execute(a, .get, "new", "{}", "");
                const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, cleaned, .{});
                try std.testing.expectEqual(m.types.Lifecycle.active, record.lifecycle);
                try std.testing.expect(record.assertion.reconciliation_reason == null);
                var rows = store.values.iterator();
                while (rows.next()) |row| if (std.mem.startsWith(u8, row.key_ptr.*, "assertion/") or std.mem.startsWith(u8, row.key_ptr.*, "history/")) {
                    try std.testing.expect(std.mem.indexOf(u8, row.value_ptr.*, "secret-123") == null);
                };
                const latest = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 51).execute(a, .inspect, "", "{}", ""), .{});
                config.policy_version = "v4";
                service = decisionService(&store, 52);
                service.backend.decide = requireRedactedIncomingExplanation;
                _ = try service.execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = latest.state.revision, .config = config }, .{}), "rebuild-after-forget");
                const refreshed = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 53).execute(a, .get, "new", "{}", ""), .{});
                try std.testing.expectEqualStrings("No replacement", refreshed.assertion.reconciliation_reason.?);
            }
        }
    }
}

test "memory source authority rules bind persisted principals and producers across policy changes" {
    for (0..3) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try setup(&store);
        const owner = if (mode == 0) "alice" else "bob";
        const bad_source: m.types.Evidence = .{ .id = "legacy", .source_id = "future-trusted", .source_version = "1", .text = "Legacy deployment", .producer = if (mode == 1) "untrusted" else "connector", .visibility = .team };
        const good_source: m.types.Evidence = .{ .id = "verified", .source_id = "verified", .source_version = "1", .text = "Verified deployment", .producer = "connector", .visibility = .team };
        _ = try store.service(owner, false, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{bad_source} }, .{}), "legacy");
        _ = try store.service("bob", false, 4).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{good_source} }, .{}), "verified");
        const old: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "deploys_with", .value = "Legacy", .statement = "Legacy deployment", .visibility = .team, .valid_from = 100, .evidence = &.{.{ .id = "legacy" }} };
        const new: m.types.Assertion = .{ .id = "new", .subject = "production", .predicate = "deploys_with", .value = "Verified", .statement = "Verified deployment", .visibility = .team, .valid_from = 200, .evidence = &.{.{ .id = "verified" }} };
        _ = try store.service(owner, false, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old");
        _ = try store.service("bob", false, 6).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{new} }, .{}), "new");
        const inspected = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 7).execute(a, .inspect, "", "{}", ""), .{});
        var config = inspected.state.config;
        config.source_rules = &.{ .{ .source_id = "future-trusted", .principal = "bob", .producer = "connector", .authority = 100 }, .{ .source_id = "verified", .principal = "bob", .producer = "connector", .authority = 10 } };
        _ = try store.service("alice", true, 8).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = inspected.state.revision, .config = config }, .{}), "policy");
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("bob", false, 9).execute(a, .get, "new", "{}", ""), .{});
        try std.testing.expectEqual(if (mode == 2) m.types.Lifecycle.disputed else .active, record.lifecycle);
    }
}

fn sourceAuthorityConfig() m.types.Config {
    return .{ .features = .{ .reconciliation = .deterministic }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one }}, .source_rules = &.{ .{ .source_id = "high", .principal = "alice", .authority = 100 }, .{ .source_id = "new", .principal = "alice", .authority = 10 } } };
}
fn authoritySources(store: *Store, expiry: bool) !void {
    const a = store.arena.allocator();
    _ = try store.service("alice", true, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, sourceAuthorityConfig(), .{}), "create");
    const sources = [_]m.types.Evidence{
        .{ .id = "high", .source_id = "high", .source_version = "1", .text = "Helm", .expires_at = if (expiry) 10 else null },
        .{ .id = "low", .source_id = "low", .source_version = "1", .text = "Helm" },
        .{ .id = "new", .source_id = "new", .source_version = "1", .text = "Terraform" },
    };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &sources }, .{}), "evidence");
}
fn authorityNewFact(store: *Store, valid: i64) !m.types.StoredAssertion {
    const a = store.arena.allocator();
    const claim: m.types.Assertion = .{ .id = "new", .subject = "production", .predicate = "deploys_with", .value = "Terraform", .statement = "Terraform", .valid_from = valid, .evidence = &.{.{ .id = "new" }} };
    _ = try store.service("alice", true, 50).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), "new");
    return std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 51).execute(a, .get, "new", "{}", ""), .{});
}
test "memory source authority forget and expiry fences apply before and after physical cleanup" {
    for (0..3) |mode| for ([_]bool{ false, true }) |cleanup| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try authoritySources(&store, mode == 2);
        const old: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Helm", .valid_from = 100, .evidence = &.{.{ .id = "high" }}, .support_sets = &.{.{ .evidence = &.{.{ .id = "low" }} }} };
        _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old");
        if (mode != 2) _ = try store.service("alice", true, 4).execute(a, .forget, "", if (mode == 0) "{\"source_ids\":[\"high\"]}" else "{\"evidence_ids\":[\"high\"]}", "forget");
        if (cleanup) for (0..16) |i| {
            _ = try store.service("alice", true, @intCast(12 + i)).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}));
        };
        try std.testing.expectEqual(m.types.Lifecycle.active, (try authorityNewFact(&store, 200)).lifecycle);
    };
}

test "memory source authority uses complete alternative branches independently of branch order" {
    for (0..3) |mode| for ([_]bool{ false, true }) |high_primary| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try authoritySources(&store, false);
        const parent: m.types.Assertion = .{ .id = "parent", .subject = "evidence", .predicate = "support", .value = "gate", .statement = "Supporting premise", .evidence = &.{.{ .id = "low" }} };
        _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{parent} }, .{}), "parent");
        const high: m.types.Support = .{ .evidence = &.{.{ .id = "high" }}, .depends_on = &.{"parent"} };
        const low: m.types.Support = .{ .evidence = &.{.{ .id = "low" }} };
        const primary = if (high_primary) high else low;
        const alternative = if (high_primary) low else high;
        const old: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Helm", .valid_from = 100, .evidence = primary.evidence, .depends_on = primary.depends_on, .support_sets = &.{alternative} };
        _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old");
        if (mode == 1) {
            const current = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, "parent", "{}", ""), .{});
            const correction: m.types.Correction = .{ .id = "parent", .expected_revision = current.revision, .lifecycle = .retracted, .reason = "Premise withdrawn", .evidence = &.{.{ .id = "low" }} };
            _ = try store.service("alice", true, 6).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, correction, .{}), "retract");
        } else if (mode == 2) _ = try store.service("alice", true, 6).execute(a, .forget, "", "{\"assertion_ids\":[\"parent\"]}", "forget-parent");
        try std.testing.expectEqual(if (mode == 0) m.types.Lifecycle.disputed else .active, (try authorityNewFact(&store, 200)).lifecycle);
    };
}

test "memory source authority evaluates historical premises at overlapping business time" {
    for (0..3) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try authoritySources(&store, false);
        const parent: m.types.Assertion = .{ .id = "parent", .subject = "premise", .predicate = "status", .value = "old", .statement = "Old premise", .valid_from = 100, .evidence = &.{.{ .id = "low" }} };
        _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{parent} }, .{}), "parent");
        const old: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Helm", .valid_from = 100, .evidence = &.{.{ .id = "high" }}, .depends_on = &.{"parent"} };
        _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old");
        var next = parent;
        next.id = "parent-next";
        next.value = "new";
        next.statement = "New premise";
        next.valid_from = 300;
        next.supersedes = &.{"parent"};
        _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{next} }, .{}), "next");
        if (mode == 2) {
            const current = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 6).execute(a, .get, "parent", "{}", ""), .{});
            const correction: m.types.Correction = .{ .id = "parent", .expected_revision = current.revision, .lifecycle = .retracted, .reason = "Premise withdrawn", .evidence = &.{.{ .id = "low" }} };
            _ = try store.service("alice", true, 7).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, correction, .{}), "retract");
        }
        try std.testing.expectEqual(if (mode == 0) m.types.Lifecycle.disputed else .active, (try authorityNewFact(&store, if (mode == 1) 400 else 200)).lifecycle);
    }
}

fn temporalDependencySetup(store: *Store, kind: m.types.Kind) !void {
    const a = store.arena.allocator();
    try authoritySources(store, false);
    const parent: m.types.Assertion = .{ .id = "parent", .subject = "premise", .predicate = "status", .value = "old", .statement = "Old premise", .valid_from = 100, .evidence = &.{.{ .id = "low" }} };
    _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{parent} }, .{}), "parent");
    const old: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "deploys_with", .kind = kind, .value = "Helm", .statement = "Helm", .valid_from = 100, .evidence = &.{.{ .id = "high" }}, .depends_on = &.{"parent"} };
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old");
    var leaf = old;
    leaf.id = "leaf";
    leaf.subject = "leaf";
    leaf.predicate = "observation";
    leaf.kind = .semantic;
    leaf.depends_on = &.{"old"};
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{leaf} }, .{}), "leaf");
    var next = parent;
    next.id = "parent-next";
    next.value = "new";
    next.statement = "New premise";
    next.valid_from = 300;
    next.supersedes = &.{"parent"};
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{next} }, .{}), "next");
}
fn temporalReadCount(store: *Store, action: m.Action, subject: []const u8, valid: i64, known: ?i64) !usize {
    const a = store.arena.allocator();
    const raw = try store.service("alice", true, 49).execute(a, action, "", try std.json.Stringify.valueAlloc(a, .{ .subject = subject, .valid_at = valid, .known_at = known, .token_budget = @as(u32, 8192) }, .{}), "");
    return (try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{})).object.get("items").?.array.items.len;
}
test "memory temporal dependency maintenance preserves history and reconciliation at every depth" {
    for ([_]m.types.Kind{ .semantic, .summary, .procedural }) |kind| for ([_]bool{ false, true }) |maintain| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try temporalDependencySetup(&store, kind);
        if (maintain) for (0..16) |step| {
            _ = try store.service("alice", true, @intCast(step + 10)).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "maintenance-{d}", .{step}));
        };
        for ([_]m.Action{ .list, .recall }) |action| for ([_][]const u8{ "production", "leaf" }) |subject| {
            try std.testing.expectEqual(@as(usize, 1), try temporalReadCount(&store, action, subject, 200, null));
            try std.testing.expectEqual(@as(usize, 1), try temporalReadCount(&store, action, subject, 200, 49));
            try std.testing.expectEqual(@as(usize, 1), try temporalReadCount(&store, action, subject, 200, 4));
            try std.testing.expectEqual(@as(usize, 0), try temporalReadCount(&store, action, subject, 400, null));
        };
        try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 49).execute(a, .get, "old", "{}", ""));
        const audit = try std.json.parseFromSliceLeaky(m.types.RecordPage, a, try store.service("alice", true, 49).execute(a, .history, "old", "{}", ""), .{});
        try std.testing.expectEqual(@as(usize, 1), audit.items.len);
        try std.testing.expect(!audit.items[0].stale);
        try std.testing.expectEqual(m.types.Lifecycle.disputed, (try authorityNewFact(&store, 200)).lifecycle);
    };
}

fn advanceTemporalMaintenance(store: *Store, prefix: []const u8) !void {
    const a = store.arena.allocator();
    for (0..24) |step| _ = try store.service("alice", true, @intCast(step + 10)).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "{s}-{d}", .{ prefix, step }));
}
test "memory maintenance preserves recorded retractions and active dependency restoration until hard retirement" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try temporalDependencySetup(&store, .semantic);
    for ([_]m.types.Lifecycle{ .retracted, .active }) |lifecycle| {
        const current = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 6).execute(a, .get, "parent", "{}", ""), .{});
        const correction: m.types.Correction = .{ .id = "parent", .expected_revision = current.revision, .lifecycle = lifecycle, .reason = "Verified premise disposition", .evidence = &.{.{ .id = "low" }} };
        _ = try store.service("alice", true, if (lifecycle == .retracted) 7 else 40).execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, correction, .{}), @tagName(lifecycle));
        for ([_]bool{ false, true }) |maintain| {
            if (maintain) try advanceTemporalMaintenance(&store, @tagName(lifecycle));
            for ([_]m.Action{ .list, .recall }) |action| for ([_][]const u8{ "production", "leaf" }) |subject| {
                try std.testing.expectEqual(@as(usize, 1), try temporalReadCount(&store, action, subject, 200, 4));
                try std.testing.expectEqual(@as(usize, 0), try temporalReadCount(&store, action, subject, 200, 8));
                try std.testing.expectEqual(@as(usize, if (lifecycle == .active) 1 else 0), try temporalReadCount(&store, action, subject, 200, null));
            };
        }
    }
    _ = try store.service("alice", true, 45).execute(a, .forget, "", "{\"assertion_ids\":[\"parent\"]}", "forget-parent");
    for ([_]bool{ false, true }) |maintain| {
        if (maintain) try advanceTemporalMaintenance(&store, "forget");
        for ([_]m.Action{ .list, .recall }) |action| for ([_][]const u8{ "production", "leaf" }) |subject| {
            try std.testing.expectEqual(@as(usize, 0), try temporalReadCount(&store, action, subject, 200, 4));
            try std.testing.expectEqual(@as(usize, 0), try temporalReadCount(&store, action, subject, 200, null));
        };
    }
    var rows = store.values.iterator();
    while (rows.next()) |row| if (std.mem.startsWith(u8, row.key_ptr.*, "assertion/") or std.mem.startsWith(u8, row.key_ptr.*, "history/")) {
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, row.value_ptr.*, .{});
        try std.testing.expect(!std.mem.eql(u8, record.assertion.id, "old"));
        try std.testing.expect(!std.mem.eql(u8, record.assertion.id, "leaf"));
    };
}

test "memory explicit semantic artifact invalidation remains independent of temporal maintenance" {
    for ([_]m.types.Kind{ .summary, .procedural }) |kind| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try temporalDependencySetup(&store, kind);
        const inspected = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 6).execute(a, .inspect, "", "{}", ""), .{});
        var config = inspected.state.config;
        config.policy_version = "v2";
        _ = try store.service("alice", true, 7).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = inspected.state.revision, .config = config }, .{}), "policy");
        for ([_]bool{ false, true }) |maintain| {
            if (maintain) try advanceTemporalMaintenance(&store, "policy");
            for ([_]m.Action{ .list, .recall }) |action| for ([_][]const u8{ "production", "leaf" }) |subject| {
                try std.testing.expectEqual(@as(usize, 0), try temporalReadCount(&store, action, subject, 200, null));
                try std.testing.expectEqual(@as(usize, 1), try temporalReadCount(&store, action, subject, 200, 4));
            };
            const audit = try std.json.parseFromSliceLeaky(m.types.RecordPage, a, try store.service("alice", true, 49).execute(a, .history, "old", "{}", ""), .{});
            var stale_count: usize = 0;
            for (audit.items) |record| if (record.stale) {
                stale_count += 1;
            };
            try std.testing.expectEqual(@as(usize, 1), stale_count);
        }
    }
}

test "memory delayed identity invalidation preserves artifacts regenerated after the identity revision" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try authoritySources(&store, false);
    const entities = [_]m.types.Entity{
        .{ .id = "left", .name = "Left", .evidence = &.{.{ .id = "low" }} },
        .{ .id = "right", .name = "Right", .evidence = &.{.{ .id = "low" }} },
    };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "low", .source_id = "low", .source_version = "1", .text = "Helm" }}, .entities = &entities }, .{}), "entities");
    const old: m.types.Assertion = .{ .id = "old", .subject = "left", .predicate = "guidance", .kind = .summary, .value = "old", .statement = "Old summary", .evidence = &.{.{ .id = "low" }} };
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old");
    const inspected = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 5).execute(a, .inspect, "", "{}", ""), .{});
    _ = try store.service("alice", true, 6).execute(a, .curate_entity, "", try std.json.Stringify.valueAlloc(a, .{ .action = "merge", .from = "left", .to = "right", .expected_revision = inspected.state.revision }, .{}), "merge");
    var fresh = old;
    fresh.id = "fresh";
    fresh.value = "fresh";
    fresh.statement = "Regenerated summary";
    _ = try store.service("alice", true, 7).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{fresh} }, .{}), "fresh");
    for ([_]bool{ false, true }) |maintain| {
        if (maintain) try advanceTemporalMaintenance(&store, "identity");
        const raw = try store.service("alice", true, 49).execute(a, .list, "", "{}", "");
        const page = try std.json.parseFromSliceLeaky(m.types.RecordPage, a, raw, .{});
        try std.testing.expectEqual(@as(usize, 1), page.items.len);
        try std.testing.expectEqualStrings("fresh", page.items[0].assertion.id);
        try std.testing.expect(!page.items[0].stale);
    }
}

test "memory supersession support uses accepted business boundaries independently of original validity" {
    for ([_]?i64{ null, 1000 }) |original_end| for ([_]?i64{ null, 300 }) |replacement_start| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try authoritySources(&store, false);
        const parent: m.types.Assertion = .{ .id = "parent", .subject = "premise", .predicate = "status", .value = "old", .statement = "Old premise", .valid_from = 100, .valid_to = original_end, .evidence = &.{.{ .id = "low" }} };
        _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{parent} }, .{}), "parent");
        const child: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Use Helm", .valid_from = 100, .evidence = &.{.{ .id = "high" }}, .depends_on = &.{"parent"} };
        _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{child} }, .{}), "child");
        var leaf = child;
        leaf.id = "leaf";
        leaf.subject = "leaf";
        leaf.depends_on = &.{"old"};
        _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{leaf} }, .{}), "leaf");
        var next = parent;
        next.id = "next";
        next.value = "new";
        next.statement = "New premise";
        next.valid_from = replacement_start;
        next.valid_to = null;
        next.supersedes = &.{"parent"};
        _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{next} }, .{}), "next");
        for (0..3) |phase| {
            if (phase == 1) try advanceTemporalMaintenance(&store, "supersession");
            if (phase == 2) {
                const inspected = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try store.service("alice", true, 40).execute(a, .inspect, "", "{}", ""), .{});
                var config = inspected.state.config;
                config.policy_version = "v2";
                _ = try store.service("alice", true, 41).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = inspected.state.revision, .config = config }, .{}), "rebuild");
            }
            const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 49).execute(a, .get, "parent", "{}", ""), .{});
            try std.testing.expectEqual(replacement_start, record.superseded_valid_at);
            try std.testing.expectEqual(replacement_start, record.explicit_disposition.?.superseded_valid_at);
            try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 500).execute(a, .get, "old", "{}", ""));
            for ([_]m.Action{ .list, .recall }) |action| for ([_][]const u8{ "production", "leaf" }) |subject| {
                const raw = try store.service("alice", true, 500).execute(a, action, "", try std.json.Stringify.valueAlloc(a, .{ .subject = subject, .token_budget = @as(u32, 8192) }, .{}), "");
                const result = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
                try std.testing.expectEqual(@as(usize, 0), result.object.get("items").?.array.items.len);
                try std.testing.expectEqual(@as(usize, if (replacement_start != null) 1 else 0), try temporalReadCount(&store, action, subject, 200, null));
                try std.testing.expectEqual(@as(usize, 1), try temporalReadCount(&store, action, subject, 200, 4));
            };
        }
        // Forgetting the replacement cannot erase an already accepted dated
        // boundary or make an undated replacement acquire a fabricated date.
        _ = try store.service("alice", true, 45).execute(a, .forget, "", "{\"assertion_ids\":[\"next\"]}", "forget-next");
        try std.testing.expectEqual(@as(usize, if (replacement_start != null) 1 else 0), try temporalReadCount(&store, .recall, "production", 200, null));
        try std.testing.expectEqual(if (replacement_start != null) m.types.Lifecycle.disputed else .active, (try authorityNewFact(&store, 200)).lifecycle);
    };
}

const ScheduledMemory = struct {
    store: *Store,
    denied: std.StringHashMapUnmanaged(void) = .empty,
    cleanup_allowed: bool = true,
    denial: anyerror = error.StoredDestinationAuthorizationRevoked,
    jobs: usize = 0,
    cleanups: usize = 0,
    now: i64 = 100,
    calls: usize = 0,

    fn generate(_: *anyopaque, a: A, _: m.types.Config, inputs: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
        const claims = try a.alloc(m.types.Assertion, 1);
        claims[0] = .{ .id = try std.fmt.allocPrint(a, "claim-{s}", .{inputs[0].evidence.id}), .subject = "scheduled", .predicate = "fact", .value = inputs[0].evidence.id, .statement = inputs[0].evidence.text, .evidence = try a.dupe(m.types.EvidenceRef, &.{.{ .id = inputs[0].evidence.id }}) };
        return .{ .assertions = claims };
    }
    fn execute(ptr: *anyopaque, item: ?m.types.Operation) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const a = self.store.arena.allocator();
        self.calls += 1;
        if (item) |pending| {
            if (self.denied.contains(pending.id)) return self.denial;
            try std.testing.expectEqualStrings("saved-binding", pending.execution_signature);
            var service = modelService(self.store, self.now);
            service.context.principal = pending.owner;
            service.context.admin = false;
            service.backend.generate = generate;
            _ = try service.execute(a, .advance, pending.id, "{}", try std.fmt.allocPrint(a, "advance-{d}", .{self.calls}));
            self.jobs += 1;
        } else {
            if (!self.cleanup_allowed) return error.StoredDestinationAuthorizationRevoked;
            _ = try self.store.service("alice", true, self.now).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{self.calls}));
            self.cleanups += 1;
        }
    }
    fn tick(self: *@This(), cursor: *@import("scheduling.zig").Cursor) !void {
        const a = self.store.arena.allocator();
        const service = self.store.service("alice", true, self.now);
        const raw = (try service.backend.get(service.backend.ptr, a, "control")).?.json;
        const state = try std.json.parseFromSliceLeaky(m.types.State, a, raw, .{});
        try @import("scheduling.zig").advance(service.backend, a, self.now, state, cursor, .{ .ptr = self, .execute = execute });
        self.now += 1;
    }
};
fn scheduledMemorySetup(store: *Store, count: usize) !void {
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .hybrid, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try store.service("alice", true, 2).execute(a, .observe, "", source, "source");
    _ = try store.service("alice", true, 3).execute(a, .assertions, "", helm, "old");
    for (0..count) |i| {
        var service = modelService(store, 4);
        service.context.principal = try std.fmt.allocPrint(a, "owner-{d}", .{i});
        service.context.execution_signature = "saved-binding";
        const evidence: m.types.Evidence = .{ .id = try std.fmt.allocPrint(a, "e-{d}", .{i}), .source_id = try std.fmt.allocPrint(a, "source-{d}", .{i}), .source_version = "1", .text = "Managed fact", .managed = true };
        _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "observe");
    }
    _ = try store.service("alice", true, 5).execute(a, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget");
}
test "memory scheduling passes revoked and delayed queue pages while independently advancing cleanup" {
    for ([_]bool{ false, true }) |delayed| {
        var store = Store.init();
        defer store.deinit();
        try scheduledMemorySetup(&store, 96);
        const a = store.arena.allocator();
        var fixture: ScheduledMemory = .{ .store = &store };
        const service = store.service("alice", true, 100);
        const page = try service.backend.scan(service.backend.ptr, a, "pending/", null, 64);
        for (page.rows) |row| {
            const queued = try std.json.parseFromSliceLeaky(struct { id: []const u8 }, a, row.json, .{});
            if (delayed) {
                const Failing = struct {
                    fn generate(_: *anyopaque, _: A, _: m.types.Config, _: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
                        return error.TransientInferenceFailure;
                    }
                };
                const raw = try service.execute(a, .operation, queued.id, "{}", "");
                const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, raw, .{});
                var processing = modelService(&store, 6);
                processing.context.principal = receipt.owner;
                processing.backend.generate = Failing.generate;
                _ = try processing.execute(a, .advance, queued.id, "{}", "delay");
            } else try fixture.denied.put(a, queued.id, {});
        }
        var cursor: @import("scheduling.zig").Cursor = .{};
        for (0..8) |_| try fixture.tick(&cursor);
        try std.testing.expect(fixture.jobs > 0 and fixture.cleanups > 0);
        try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 200).execute(a, .get, "helm", "{}", ""));
        var rows = store.values.iterator();
        while (rows.next()) |entry| if (std.mem.startsWith(u8, entry.key_ptr.*, "assertion/")) {
            const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, entry.value_ptr.*, .{});
            try std.testing.expect(!std.mem.eql(u8, record.assertion.id, "helm"));
        };
        for (page.rows) |row| {
            const queued = try std.json.parseFromSliceLeaky(struct { id: []const u8 }, a, row.json, .{});
            const raw = try store.service("alice", true, 200).execute(a, .operation, queued.id, "{}", "");
            const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, raw, .{});
            try std.testing.expectEqual(m.types.Stage.pending, receipt.phase);
            try std.testing.expectEqual(@as(u8, if (delayed) 1 else 0), receipt.attempts);
        }
        // A restart discards only selection position; durable jobs still advance.
        const before = fixture.jobs;
        cursor = .{};
        for (0..8) |_| try fixture.tick(&cursor);
        try std.testing.expect(fixture.jobs > before);
    }
}

test "memory scheduling gives cleanup a turn under ingestion and preserves jobs when cleanup binding is revoked" {
    for ([_]bool{ false, true }) |cleanup_allowed| {
        var store = Store.init();
        defer store.deinit();
        try scheduledMemorySetup(&store, 16);
        var fixture: ScheduledMemory = .{ .store = &store, .cleanup_allowed = cleanup_allowed };
        var cursor: @import("scheduling.zig").Cursor = .{};
        for (0..6) |_| try fixture.tick(&cursor);
        try std.testing.expect(fixture.jobs > 0);
        try std.testing.expectEqual(cleanup_allowed, fixture.cleanups > 0);
        try std.testing.expect(fixture.jobs < 16);
    }
}

test "memory scheduling advances its cursor before execution deadline failures" {
    var store = Store.init();
    defer store.deinit();
    try scheduledMemorySetup(&store, 4);
    const a = store.arena.allocator();
    const service = store.service("alice", true, 100);
    const page = try service.backend.scan(service.backend.ptr, a, "pending/", null, 1);
    const first = try std.json.parseFromSliceLeaky(struct { id: []const u8 }, a, page.rows[0].json, .{});
    var fixture: ScheduledMemory = .{ .store = &store, .denial = error.DeadlineExceeded };
    try fixture.denied.put(a, first.id, {});
    var cursor: @import("scheduling.zig").Cursor = .{};
    try std.testing.expectError(error.DeadlineExceeded, fixture.tick(&cursor));
    try fixture.tick(&cursor);
    try fixture.tick(&cursor);
    try std.testing.expect(fixture.jobs > 0 and fixture.cleanups > 0);
    const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try service.execute(a, .operation, first.id, "{}", ""), .{});
    try std.testing.expectEqual(m.types.Stage.pending, receipt.phase);
    try std.testing.expectEqual(@as(u8, 0), receipt.attempts);
}

// Admission must share the read/retention gate, even while payloads remain
// physically present and a bounded scrub has advanced beyond their key range.
test "memory forgotten evidence rejects new support context and lifecycle writes atomically" {
    for ([_][]const u8{ "{\"source_ids\":[\"runbook\"]}", "{\"all\":true}" }) |forget_body| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try setup(&store);
        try discard(&store, .observe, "",
            \\{"evidence":[{"id":"safe","source_id":"safe","source_version":"1","text":"Safe evidence"}]}
        , "safe", 3);
        try discard(&store, .assertions, "", helm, "old", 4);
        const original = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 5).execute(a, .get, "helm", "{}", ""), .{});
        try discard(&store, .forget, "", forget_body, "forget", 6);
        const revision = store.revision;
        const svc = store.service("alice", true, 7);
        try std.testing.expectError(error.MemoryForgotten, svc.execute(a, .assertions, "", terraform, "primary"));
        try std.testing.expectError(error.MemoryForgotten, svc.execute(a, .assertions, "",
            \\{"assertions":[{"id":"alternate","subject":"production","predicate":"deploys_with","value":"Terraform","statement":"Forgotten alternative","evidence":[{"id":"safe"}],"support_sets":[{"evidence":[{"id":"e1"}]}]}]}
        , "alternate"));
        try std.testing.expectError(error.MemoryForgotten, svc.execute(a, .observe, "",
            \\{"evidence":[{"id":"fresh","source_id":"fresh","source_version":"1","text":"Fresh evidence"}],"context_evidence_ids":["e1"]}
        , "context"));
        try std.testing.expectError(error.MemoryForgotten, svc.execute(a, .correct, "", try std.json.Stringify.valueAlloc(a, m.types.Correction{ .id = "helm", .expected_revision = original.revision, .lifecycle = .active, .reason = "Revive forgotten payload" }, .{}), "correct"));
        try std.testing.expectError(error.MemoryForgotten, svc.execute(a, .assertions, "",
            \\{"assertions":[{"id":"replacement","subject":"production","predicate":"deploys_with","value":"Terraform","statement":"Fresh replacement","valid_from":300,"evidence":[{"id":"safe"}],"supersedes":["helm"]}]}
        , "supersede"));
        try std.testing.expectEqual(revision, store.revision);
        try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "assertion/", null, 256)).rows.len);
        try std.testing.expectEqual(@as(usize, 2), (try Store.scan(&store, a, "evidence/", null, 256)).rows.len);
        if (std.mem.indexOf(u8, forget_body, "source_ids") != null) {
            // Source fences do not prohibit unrelated new evidence and claims.
            _ = try svc.execute(a, .assertions, "",
                \\{"assertions":[{"id":"unrelated","subject":"other","predicate":"fact","value":"Safe","statement":"Safe fact","evidence":[{"id":"safe"}]}]}
            , "unrelated");
        }
    }
}

test "memory source scrub cannot leave newly admitted payload behind its cursor" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    try discard(&store, .observe, "",
        \\{"evidence":[{"id":"safe","source_id":"safe","source_version":"1","text":"Safe"}]}
    , "safe", 3);
    var claims: std.ArrayList(m.types.Assertion) = .empty;
    for (0..80) |i| try claims.append(a, .{ .id = try std.fmt.allocPrint(a, "padding-{d}", .{i}), .subject = "padding", .predicate = "padding", .value = "Safe", .statement = "Safe", .evidence = &.{.{ .id = "safe" }} });
    try discard(&store, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims.items }, .{}), "padding", 4);
    try discard(&store, .forget, "", "{\"source_ids\":[\"runbook\"]}", "forget", 5);
    for (0..10) |i| {
        try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "before-{d}", .{i}), 6);
        const cleanup = try std.json.parseFromSliceLeaky(struct { cursor: ?[]const u8 = null }, a, store.values.get("cleanup") orelse return error.UnexpectedCleanupComplete, .{});
        if (cleanup.cursor) |cursor| if (std.mem.startsWith(u8, cursor, "dependency/")) break;
    }
    const position = try std.json.parseFromSliceLeaky(struct { cursor: ?[]const u8 = null }, a, store.values.get("cleanup").?, .{});
    try std.testing.expect(std.mem.startsWith(u8, position.cursor.?, "dependency/"));
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "dependency/work/", null, 256)).rows.len);
    const revision = store.revision;
    try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 7).execute(a, .assertions, "", helm, "late-forgotten-claim"));
    try std.testing.expectEqual(revision, store.revision);
    for (0..30) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "after-{d}", .{i}), 8);
    try std.testing.expect(store.values.get("cleanup") == null);
    const rows = try Store.scan(&store, a, "assertion/", null, 256);
    try std.testing.expectEqual(@as(usize, 80), rows.rows.len);
    for (rows.rows) |row| {
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, row.json, .{});
        try std.testing.expect(!std.mem.eql(u8, record.assertion.id, "helm"));
    }
    try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "evidence/", null, 256)).rows.len);
}

test "memory external producer ownership covers every support branch and ingestion path" {
    for ([_]m.Action{ .observe, .assertions }) |action| {
        for ([_]bool{ false, true }) |alternate| {
            var store = Store.init();
            defer store.deinit();
            const a = store.arena.allocator();
            const config: m.types.Config = .{ .processing = .hybrid, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
            _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
            const sources: []const m.types.Evidence = &.{
                .{ .id = "external", .source_id = "client", .source_version = "1", .text = "Production uses Helm" },
                .{ .id = "e1", .source_id = "managed", .source_version = "1", .text = "Production uses Helm", .managed = true },
            };
            var operation_id: ?[]const u8 = null;
            if (action == .assertions) {
                const accepted = try modelService(&store, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = sources }, .{}), "observe");
                operation_id = (try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{})).id;
            }
            const claim: m.types.Assertion = .{ .id = "client-claim", .subject = "production", .predicate = "deploys_with", .value = "Helm", .statement = "Production uses Helm", .evidence = if (alternate) &.{.{ .id = "external" }} else &.{.{ .id = "e1" }}, .support_sets = if (alternate) &.{.{ .evidence = &.{.{ .id = "e1" }} }} else &.{} };
            const body = if (action == .observe) try std.json.Stringify.valueAlloc(a, .{ .evidence = sources, .assertions = &.{claim} }, .{}) else try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{});
            const revision = store.revision;
            try std.testing.expectError(error.MemoryProducerConflict, modelService(&store, 3).execute(a, action, "", body, "claim"));
            try std.testing.expectEqual(revision, store.revision);
            try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "assertion/", null, 256)).rows.len);
            if (operation_id) |id| {
                // The rejected client path does not interfere with the admitted
                // managed operation, or with independent external assertions.
                _ = try modelService(&store, 4).execute(a, .advance, id, "{}", "advance");
                _ = try modelService(&store, 5).execute(a, .get, "managed", "{}", "");
                var external = claim;
                external.id = "external-only";
                external.evidence = &.{.{ .id = "external" }};
                external.support_sets = &.{};
                _ = try modelService(&store, 6).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{external} }, .{}), "external-only");
            } else {
                try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "evidence/", null, 256)).rows.len);
                try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "pending/", null, 256)).rows.len);
                try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "producer/", null, 256)).rows.len);
            }
        }
    }
}

fn ownershipOutput(ptr: *anyopaque, a: A, _: m.types.Config, _: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
    const store: *Store = @ptrCast(@alignCast(ptr));
    if (store.failures == 7) {
        const entities = try a.alloc(m.types.Entity, 1);
        entities[0] = .{ .id = "enriched", .name = "External entity", .evidence = &.{.{ .id = "external" }} };
        return .{ .entities = entities };
    }
    const claims = try a.alloc(m.types.Assertion, 1);
    claims[0] = .{ .id = "generated", .subject = "production", .predicate = "guidance", .value = "Helm", .statement = "Generated guidance", .evidence = &.{.{ .id = "e1" }} };
    switch (store.failures) {
        0 => claims[0].evidence = &.{.{ .id = "external" }},
        1 => claims[0].support_sets = &.{.{ .evidence = &.{.{ .id = "external" }} }},
        2 => claims[0].support_sets = &.{.{ .evidence = &.{.{ .id = "context" }} }},
        3 => {
            claims[0].kind = .summary;
            claims[0].evidence = &.{.{ .id = "external" }};
        },
        4 => claims[0].evidence = &.{ .{ .id = "e1" }, .{ .id = "external" }, .{ .id = "context" } },
        5 => claims[0].support_sets = &.{.{ .evidence = &.{ .{ .id = "e1" }, .{ .id = "external" } } }},
        6 => claims[0].evidence = &.{.{ .id = "context" }},
        else => unreachable,
    }
    return .{ .assertions = claims };
}

test "memory generation stages enforce ownership at publication for every independent branch" {
    for (0..8) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const config: m.types.Config = .{ .processing = .hybrid, .features = .{ .assertion_distillation = true, .entity_extraction = true, .summaries = true }, .models = .{ .generator = fake_model } };
        _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        _ = try modelService(&store, 2).execute(a, .observe, "",
            \\{"evidence":[{"id":"context","source_id":"prior","source_version":"1","text":"Prior context"}]}
        , "context");
        const accepted = try modelService(&store, 3).execute(a, .observe, "",
            \\{"evidence":[{"id":"external","source_id":"client","source_version":"1","text":"Production uses Helm"},{"id":"e1","source_id":"managed","source_version":"1","text":"An unrelated managed observation","managed":true}],"context_evidence_ids":["context"],"assertions":[{"id":"client-claim","subject":"production","predicate":"deploys_with","value":"Helm","statement":"Production uses Helm","evidence":[{"id":"external"}]}]}
        , "observe");
        const operation = try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{});
        store.failures = @intCast(mode);
        var svc = modelService(&store, 4);
        svc.backend.generate = ownershipOutput;
        _ = try svc.execute(a, .advance, operation.id, "{}", "advance");
        const status = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try svc.execute(a, .operation, operation.id, "{}", ""), .{});
        const rejected = mode < 3 or mode == 6;
        try std.testing.expectEqual(if (rejected) m.types.Stage.pending else .succeeded, status.phase);
        if (rejected) {
            try std.testing.expectEqualStrings("MemoryUngroundedOutput", status.error_code.?);
            try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "assertion/", null, 256)).rows.len);
            try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "entity/", null, 256)).rows.len);
            try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "generation-input/", null, 256)).rows.len);
        } else if (mode == 7) {
            _ = try svc.execute(a, .resolve_entity, "enriched", "{}", "");
        } else {
            _ = try svc.execute(a, .get, "generated", "{}", "");
        }
        _ = try svc.execute(a, .get, "client-claim", "{}", "");
    }
}

test "memory generation plans mark client sources as distillation context and reject stage spoofing" {
    const a = std.testing.allocator;
    const config: m.types.Config = .{ .processing = .hybrid, .features = .{ .assertion_distillation = true, .entity_extraction = true, .summaries = true } };
    const sources: []const m.types.StoredEvidence = &.{
        .{ .evidence = .{ .id = "managed", .source_id = "m", .source_version = "1", .text = "Managed", .managed = true }, .owner = "alice", .recorded_at = 1, .revision = 1 },
        .{ .evidence = .{ .id = "external", .source_id = "e", .source_version = "1", .text = "External" }, .owner = "alice", .recorded_at = 1, .revision = 1 },
        .{ .context_only = true, .evidence = .{ .id = "context", .source_id = "c", .source_version = "1", .text = "Context", .managed = true }, .owner = "alice", .recorded_at = 1, .revision = 1 },
    };
    const plan: m.generation_policy.Plan = .{ .config = config, .sources = sources };
    for ([_]m.generation_policy.Stage{ .entities, .distillation, .summaries }) |stage| {
        const inputs = try plan.inputs(a, stage);
        defer a.free(inputs);
        try std.testing.expect(!inputs[0].context_only);
        try std.testing.expectEqual(stage == .distillation, inputs[1].context_only);
        try std.testing.expect(inputs[2].context_only);
    }
    try std.testing.expect(!sources[1].context_only); // Per-stage plans cannot mutate persisted ownership.
    var claim: m.types.Assertion = .{ .id = "claim", .subject = "s", .predicate = "p", .value = "v", .statement = "Summary", .kind = .summary, .evidence = &.{.{ .id = "external" }} };
    try std.testing.expectError(error.MemoryUngroundedOutput, plan.validateOutput(.distillation, .{ .assertions = &.{claim} }));
    try plan.validateOutput(.summaries, .{ .assertions = &.{claim} });
    claim.kind = .semantic;
    try std.testing.expectError(error.MemoryUngroundedOutput, plan.validateOutput(.summaries, .{ .assertions = &.{claim} }));
    try std.testing.expectError(error.MemoryUngroundedOutput, plan.validatePublication(.{ .assertions = &.{claim} }));
    claim.evidence = &.{.{ .id = "unknown" }};
    try std.testing.expectError(error.MemoryUngroundedOutput, plan.validatePublication(.{ .assertions = &.{claim} }));
}

fn curationPayloadSetup(store: *Store, expires: bool) ![]const u8 {
    const a = store.arena.allocator();
    try discard(store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "identity", .source_id = "identity-note", .source_version = "1", .text = "Identity evidence", .expires_at = if (expires) 50 else null };
    const entities: []const m.types.Entity = &.{
        .{ .id = "left", .name = "Left", .evidence = &.{.{ .id = "identity" }} },
        .{ .id = "right", .name = "Right", .evidence = &.{.{ .id = "identity" }} },
    };
    try discard(store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = entities }, .{}), "entities", 2);
    const body =
        \\{"action":"merge","from":"left","to":"right","expected_revision":3,"reason":"PRIVATE_CURATOR_REASON"}
    ;
    try discard(store, .curate_entity, "", body, "merge", 3);
    const policy = store.values.get("entity-policy").?;
    try std.testing.expect(std.mem.indexOf(u8, policy, "PRIVATE_CURATOR_REASON") == null);
    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, policy, .{});
    const origin = value.object.get("state").?.object.get("redirect_origins").?.array.items[0].object.get("origin").?;
    try std.testing.expectEqualStrings("alice", origin.object.get("actor").?.string);
    try std.testing.expectEqual(@as(i64, 4), origin.object.get("revision").?.integer);
    try std.testing.expectEqual(@as(usize, 1), (try Store.scan(store, a, "entity-curation/", null, 256)).rows.len);
    return body;
}

test "memory curation content retires on global source evidence forgetting and expiry without losing fences" {
    for (0..4) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const replay = try curationPayloadSetup(&store, mode == 3);
        if (mode < 3) try discard(&store, .forget, "", switch (mode) {
            0 => "{\"all\":true}",
            1 => "{\"source_ids\":[\"identity-note\"]}",
            2 => "{\"evidence_ids\":[\"identity\"]}",
            else => unreachable,
        }, "forget", 4);
        const revision = (try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{})).revision;
        try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 60).execute(a, .curate_entity, "", try std.json.Stringify.valueAlloc(a, m.types.EntityCorrection{ .action = .unmerge, .from = "left", .expected_revision = revision, .reason = "LATE_FORGOTTEN_REASON" }, .{}), "late-curation"));
        if (mode == 0) {
            try std.testing.expect(store.values.get("entity-policy") == null);
            _ = try store.service("alice", true, 60).execute(a, .curate_entity, "", replay, "merge");
            try std.testing.expect(store.values.get("entity-policy") == null);
        }
        for (0..30) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 60);
        try std.testing.expect(store.values.get("cleanup") == null);
        try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "entity-curation/", null, 256)).rows.len);
        try std.testing.expectEqual(@as(usize, 2), (try Store.scan(&store, a, "forgotten-entity/", null, 256)).rows.len);
        var it = store.values.iterator();
        while (it.next()) |entry| {
            try std.testing.expect(std.mem.indexOf(u8, entry.value_ptr.*, "PRIVATE_CURATOR_REASON") == null);
            try std.testing.expect(std.mem.indexOf(u8, entry.value_ptr.*, "LATE_FORGOTTEN_REASON") == null);
        }
        if (mode != 0) {
            // Retention of decision metadata is independent of free-text audit
            // content. A future rebuild cannot silently reverse this decision.
            const policy = try std.json.parseFromSliceLeaky(std.json.Value, a, store.values.get("entity-policy").?, .{});
            const redirects = policy.object.get("redirects").?.array.items;
            try std.testing.expectEqual(@as(usize, 1), redirects.len);
            try std.testing.expectEqualStrings("left", redirects[0].object.get("key").?.string);
            try std.testing.expectEqualStrings("right", redirects[0].object.get("target").?.string);
        }
    }
}

test "memory legacy forgotten identity policy is scrubbed and unrelated retirement keeps supported curation content" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try curationPayloadSetup(&store, false);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"unrelated\",\"source_id\":\"unrelated\",\"source_version\":\"1\",\"text\":\"Unrelated\"}]}", "unrelated", 4);
    try discard(&store, .forget, "", "{\"source_ids\":[\"unrelated\"]}", "forget-unrelated", 5);
    for (0..8) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "retain-{d}", .{i}), 6);
    try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "entity-curation/", null, 256)).rows.len);
    try discard(&store, .forget, "", "{\"all\":true}", "forget-all", 7);
    // A restored snapshot from the previous format may contain inline origin
    // text behind a permanent global fence. Cleanup must remove it too.
    try store.values.put(a, "entity-policy", "{\"legacy_reason\":\"LEGACY_PRIVATE_REASON\"}");
    for (0..12) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "legacy-{d}", .{i}), 8);
    try std.testing.expect(store.values.get("entity-policy") == null);
    try std.testing.expect(store.values.get("cleanup") == null);
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "entity-curation/", null, 256)).rows.len);
}

fn fixtureKey(a: A, prefix: []const u8, id: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, id, .{}), &digest, .{});
    return std.fmt.allocPrint(a, "{s}/{s}", .{ prefix, std.fmt.bytesToHex(digest, .lower) });
}

test "memory entity support admission rejects oversized evidence atomically" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "e", .source_id = "keep", .source_version = "1", .text = "Keep" };
    const refs = try a.alloc(m.types.EvidenceRef, m.types.max_items + 1);
    @memset(refs, .{ .id = "e" });
    var entity: m.types.Entity = .{ .id = "large", .name = "Large", .evidence = refs };
    const before = store.revision;
    try std.testing.expectError(error.MemoryInputTooLarge, store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = &.{entity} }, .{}), "reject"));
    try std.testing.expectEqual(before, store.revision);
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "evidence/", null, 256)).rows.len);
    entity.evidence = refs[0..m.types.max_items];
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = &.{entity} }, .{}), "accept", 3);
    try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "entity/", null, 256)).rows.len);
}

test "memory cleanup yields and resumes across expensive legacy entities without losing forget debt" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"keep\",\"source_version\":\"1\",\"text\":\"Keep\"},{\"id\":\"retire\",\"source_id\":\"retire\",\"source_version\":\"1\",\"text\":\"PRIVATE_RETIRED_TEXT\"}]}", "sources", 2);
    const refs = try a.alloc(m.types.EvidenceRef, 10000);
    @memset(refs, .{ .id = "e" });
    for (0..7) |i| {
        const id = try std.fmt.allocPrint(a, "legacy-{d}", .{i});
        // Simulate rows admitted before the per-entity limit. Each artifact
        // fits a work quantum, but their first page exceeds the shared quota.
        try store.values.put(a, try fixtureKey(a, "entity", id), try std.json.Stringify.valueAlloc(a, m.types.EntityRecord{ .owner = "alice", .entity = .{ .id = id, .name = id, .evidence = refs } }, .{}));
    }
    try discard(&store, .forget, "", "{\"source_ids\":[\"retire\"]}", "forget", 3);
    try discard(&store, .advance, "", "{}", "first", 4);
    const debt = try std.json.parseFromSliceLeaky(std.json.Value, a, store.values.get("cleanup").?, .{});
    try std.testing.expect(debt.object.get("cursor").? == .string);
    for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "resume-{d}", .{i}), 5);
    try std.testing.expect(store.values.get("cleanup") == null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "evidence", "retire")) == null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "evidence", "e")) != null);
    try std.testing.expectEqual(@as(usize, 7), (try Store.scan(&store, a, "entity/", null, 256)).rows.len);
}

test "memory outcomes are immutable owned records with exact retries and bounded detail" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const original: m.types.Outcome = .{ .id = "result", .task_id = "task", .success = true, .used_memory_ids = &.{}, .detail = "Alice feedback" };
    const body = try std.json.Stringify.valueAlloc(a, original, .{});
    try discard(&store, .outcome, "", body, "alice", 2);
    const path = try fixtureKey(a, "outcome", original.id);
    const stored = try a.dupe(u8, store.values.get(path).?);
    const before = store.revision;
    try std.testing.expectError(error.Forbidden, store.service("bob", false, 3).execute(a, .outcome, "", body, "bob"));
    try std.testing.expectEqual(before, store.revision);
    var changed = original;
    changed.success = false;
    try std.testing.expectError(error.MemoryOutcomeConflict, store.service("alice", true, 3).execute(a, .outcome, "", try std.json.Stringify.valueAlloc(a, changed, .{}), "changed"));
    try std.testing.expectEqual(before, store.revision);
    try discard(&store, .outcome, "", body, "new-retry", 4);
    try std.testing.expectEqualStrings(stored, store.values.get(path).?);
    changed.id = "oversized";
    changed.detail = try a.alloc(u8, m.types.max_text_bytes + 1);
    @memset(@constCast(changed.detail), 'x');
    try std.testing.expectError(error.MemoryInputTooLarge, store.service("alice", true, 5).execute(a, .outcome, "", try std.json.Stringify.valueAlloc(a, changed, .{}), "oversized"));
    try std.testing.expect(store.values.get(try fixtureKey(a, "outcome", changed.id)) == null);
}

test "memory outcomes reject global forget before and after cleanup and preserve content-free receipt replay" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const body = "{\"id\":\"result\",\"task_id\":\"task\",\"success\":true,\"used_memory_ids\":[],\"detail\":\"PRIVATE_FEEDBACK\"}";
    try discard(&store, .outcome, "", body, "original", 2);
    try discard(&store, .forget, "", "{\"all\":true}", "forget", 3);
    try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 4).execute(a, .outcome, "", body, "new-before"));
    for (0..12) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 4);
    try std.testing.expect(store.values.get("cleanup") == null);
    try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 5).execute(a, .outcome, "", body, "new-after"));
    try discard(&store, .outcome, "", body, "original", 6);
    try std.testing.expect(store.values.get(try fixtureKey(a, "outcome", "result")) == null);
    var it = store.values.iterator();
    while (it.next()) |entry| try std.testing.expect(std.mem.indexOf(u8, entry.value_ptr.*, "PRIVATE_FEEDBACK") == null);
}

test "memory retired outcome IDs cannot be reused after source forgetting" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    const body = "{\"id\":\"result\",\"task_id\":\"task\",\"success\":true,\"used_memory_ids\":[\"helm\"],\"detail\":\"PRIVATE_FEEDBACK\"}";
    try discard(&store, .outcome, "", body, "original", 4);
    try discard(&store, .forget, "", "{\"source_ids\":[\"runbook\"]}", "forget", 5);
    for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 6);
    try std.testing.expect(store.values.get("cleanup") == null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "outcome", "result")) == null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "forgotten-outcome", "result")) != null);
    try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 7).execute(a, .outcome, "", "{\"id\":\"result\",\"task_id\":\"other\",\"success\":false,\"used_memory_ids\":[],\"detail\":\"Replacement\"}", "reuse"));
}

test "memory dependency maintenance commits its cursor before yielding expensive children" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"keep\",\"source_version\":\"1\",\"text\":\"Keep\"}]}", "source", 2);
    const refs = try a.alloc(m.types.EvidenceRef, 10000);
    @memset(refs, .{ .id = "e" });
    const prefix = try fixtureKey(a, "dependency/evidence", "e");
    for (0..7) |i| {
        const id = try std.fmt.allocPrint(a, "legacy-{d}", .{i});
        const claim: m.types.StoredAssertion = .{ .assertion = .{ .id = id, .subject = "s", .predicate = "p", .value = "v", .statement = "Still supported", .evidence = refs }, .owner = "alice", .recorded_from = 2, .revision = 3, .config_generation = 1 };
        try store.values.put(a, try fixtureKey(a, "assertion", id), try std.json.Stringify.valueAlloc(a, claim, .{}));
        try store.values.put(a, try fixtureKey(a, prefix, id), try std.json.Stringify.valueAlloc(a, .{ .child = id }, .{}));
    }
    const work = "dependency/work/fixture";
    try store.values.put(a, work, "{\"kind\":\"evidence\",\"id\":\"e\",\"revision\":3}");
    try discard(&store, .advance, "", "{}", "first", 3);
    const deferred = try std.json.parseFromSliceLeaky(std.json.Value, a, store.values.get(work).?, .{});
    try std.testing.expect(deferred.object.get("cursor").? == .string);
    try discard(&store, .advance, "", "{}", "second", 4);
    try std.testing.expect(store.values.get(work) == null);
    try std.testing.expectEqual(@as(usize, 7), (try Store.scan(&store, a, "assertion/", null, 256)).rows.len);
}

test "memory outcome feedback survives identity suppression and unrelated cleanup" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"keep\",\"source_version\":\"1\",\"text\":\"Keep\"}],\"entities\":[{\"id\":\"person\",\"name\":\"Person\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"claim\",\"subject\":\"person\",\"predicate\":\"p\",\"value\":\"v\",\"statement\":\"Still valid\",\"evidence\":[{\"id\":\"e\"}]}]}", "observe", 2);
    try discard(&store, .outcome, "", "{\"id\":\"feedback\",\"task_id\":\"task\",\"success\":true,\"used_memory_ids\":[\"claim\"],\"detail\":\"Retained task feedback\"}", "outcome", 3);
    const path = try fixtureKey(a, "outcome", "feedback");
    const original = try a.dupe(u8, store.values.get(path).?);
    try discard(&store, .curate_entity, "", "{\"action\":\"suppress\",\"from\":\"person\",\"expected_revision\":4}", "suppress", 4);
    try discard(&store, .forget, "", "{\"source_ids\":[\"unrelated\"]}", "unrelated", 5);
    for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 6);
    try std.testing.expect(store.values.get("cleanup") == null);
    try std.testing.expectEqualStrings(original, store.values.get(path).?);
    try std.testing.expect(store.values.get(try fixtureKey(a, "forgotten-outcome", "feedback")) == null);
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try discard(&store, .curate_entity, "", try std.json.Stringify.valueAlloc(a, m.types.EntityCorrection{ .action = .unsuppress, .from = "person", .expected_revision = state.revision }, .{}), "unsuppress", 7);
    try std.testing.expect(std.mem.indexOf(u8, try store.service("alice", true, 8).execute(a, .get, "claim", "{}", ""), "Still valid") != null);
}

test "memory admission rejects expired or forgotten typed objects and retired literal subject IDs atomically" {
    for ([_]bool{ false, true }) |expire| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{\"predicates\":[{\"name\":\"knows\",\"object_type\":\"person\"}]}", "create", 1);
        const sources = [_]m.types.Evidence{
            .{ .id = "identity", .source_id = "identity", .source_version = "1", .text = "Person", .expires_at = if (expire) 5 else null },
            .{ .id = "safe", .source_id = "safe", .source_version = "1", .text = "Independent claim evidence" },
        };
        const person: m.types.Entity = .{ .id = "person", .name = "Person", .type = "person", .evidence = &.{.{ .id = "identity" }} };
        try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &sources, .entities = &.{person} }, .{}), "observe", 2);
        if (!expire) try discard(&store, .forget, "", "{\"source_ids\":[\"identity\"]}", "forget", 3);
        const revision = store.revision;
        const claim: m.types.Assertion = .{ .id = "new", .subject = "someone", .predicate = "knows", .value = "person", .statement = "Someone knows person", .evidence = &.{.{ .id = "safe" }} };
        try std.testing.expectError(error.MemoryEntityRequired, store.service("alice", true, 6).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), "new"));
        try std.testing.expectEqual(revision, store.revision);
        for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 7);
        try std.testing.expect(store.values.get(try fixtureKey(a, "entity", "person")) == null);
        var literal = claim;
        literal.id = "literal";
        literal.subject = "person";
        literal.predicate = "untyped";
        try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 8).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{literal} }, .{}), "literal"));
        try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "assertion/", null, 256)).rows.len);
    }
}

test "memory records durable admission provenance and explains exact source revisions within bounds" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const reference: m.types.provenance.Resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "decision", .revision = 7 };
    const evidence: m.types.Evidence = .{ .id = "e", .source_id = "business", .source_version = "7", .resource = reference };
    const claim: m.types.Assertion = .{ .id = "claim", .subject = "production", .predicate = "deploys", .value = "Terraform", .statement = "Approved deployment", .evidence = &.{.{ .id = "e" }} };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .assertions = &.{claim} }, .{}), "observe", 2);
    const raw = try store.service("alice", true, 3).execute(a, .explain, "claim", "{}", "");
    const explained = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    try std.testing.expect(explained.object.get("incomplete_history").?.bool);
    try std.testing.expect(!explained.object.get("complete").?.bool);
    try std.testing.expect(std.mem.indexOf(u8, raw, "snapshot_json") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "authenticated") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "supported_by") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "policy_version") != null);
    const bounded = try store.service("alice", true, 4).execute(a, .explain, "claim", "{\"max_nodes\":1,\"max_bytes\":1024}", "");
    try std.testing.expect(bounded.len <= 1024);
    try std.testing.expect((try std.json.parseFromSliceLeaky(std.json.Value, a, bounded, .{})).object.get("truncated").?.bool);
    store.resource_revision = 8;
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 5).execute(a, .get, "claim", "{}", ""));
    const historical = try store.service("alice", true, 6).execute(a, .explain, "claim", "{\"revision\":3}", "");
    try std.testing.expect(std.mem.indexOf(u8, historical, "Approved deployment") != null);
    try std.testing.expect(std.mem.indexOf(u8, historical, "\"status\":\"changed\"") != null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "assertion", "claim")) != null);
    // Source drift invalidates current use without silently substituting v8 or
    // erasing the exact snapshot used by a previously accepted decision.
    for (0..10) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "poll-{d}", .{i}), 2000);
    try std.testing.expect(store.values.get(try fixtureKey(a, "assertion", "claim")) != null);
    store.resource_state = .denied;
    const denied = store.service("alice", true, 2001).execute(a, .explain, "claim", "{\"revision\":3}", "") catch |err| {
        try std.testing.expectEqual(error.MemoryForgotten, err);
        return;
    };
    _ = denied;
    return error.TestUnexpectedResult;
}

test "memory essential explanation survives trace sampling without treating trace origin as support" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const trace: m.types.provenance.Resource = .{ .table = "traces", .table_id = 7, .kind = .trace, .id = "span", .revision = 7 };
    const evidence: m.types.Evidence = .{ .id = "e", .source_id = "approval", .source_version = "1", .text = "Human approved the change" };
    const claim: m.types.Assertion = .{ .id = "claim", .subject = "production", .predicate = "deploys", .value = "Terraform", .statement = "Approved deployment", .evidence = &.{.{ .id = "e" }} };
    const body = try std.json.Stringify.valueAlloc(a, .{ .origin = m.types.provenance.Origin{ .trace = trace, .agent = .{ .id = "coding-agent", .kind = .agent }, .workflow = "release", .workflow_version = "v1" }, .evidence = &.{evidence}, .assertions = &.{claim} }, .{});
    try discard(&store, .observe, "", body, "observe", 2);
    const before = try store.service("alice", true, 3).execute(a, .explain, "claim", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, before, "originated_in") != null);
    store.resource_state = .missing;
    try std.testing.expect(std.mem.indexOf(u8, try store.service("alice", true, 4).execute(a, .get, "claim", "{}", ""), "Approved deployment") != null);
    const after = try store.service("alice", true, 5).execute(a, .explain, "claim", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, after, "coding-agent") != null);
    try std.testing.expect(std.mem.indexOf(u8, after, "Human approved the change") != null);
    try std.testing.expect((try std.json.parseFromSliceLeaky(std.json.Value, a, after, .{})).object.get("missing_evidence").?.bool);
    try std.testing.expect(std.mem.indexOf(u8, after, "\"id\":\"span\"") == null);
    const state_before = store.revision;
    const denied_origin: m.types.provenance.Origin = .{ .approver = .{ .id = "admin", .assurance = .authenticated } };
    try std.testing.expectError(error.InvalidResourceReference, store.service("alice", true, 6).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .origin = denied_origin, .evidence = &.{evidence} }, .{}), "forged"));
    try std.testing.expectEqual(state_before, store.revision);
}

test "memory resolves ordinary row selectors into exact immutable evidence without caller-supplied storage versions" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "selected", .resource_selector = .{ .table = "business", .id = "approved" } };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "select", 2);
    const stored = try std.json.parseFromSliceLeaky(m.types.StoredEvidence, a, store.values.get(try fixtureKey(a, "evidence", "selected")).?, .{});
    try std.testing.expectEqual(@as(u64, 7), stored.evidence.resource.?.table_id);
    try std.testing.expectEqual(@as(u64, 7), stored.evidence.resource.?.revision);
    try std.testing.expectEqualStrings("7", stored.evidence.source_version);
    try std.testing.expect(stored.evidence.resource_selector == null);
    try std.testing.expect(stored.evidence.text.len > 0);
    const before = store.revision;
    var stale = evidence;
    stale.id = "stale";
    stale.resource_selector.?.expected_revision = 6;
    try std.testing.expectError(error.MemoryResourceChanged, store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{stale} }, .{}), "stale"));
    try std.testing.expectEqual(before, store.revision);
}

test "memory cleanup cannot retire shared ordinary evidence on a worker credential denial" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const shared_source: m.types.Evidence = .{ .id = "shared", .resource_selector = .{ .table = "business", .id = "approval" } };
    const claim: m.types.Assertion = .{ .id = "claim", .subject = "deployment", .predicate = "approved", .value = "yes", .statement = "Approved", .evidence = &.{.{ .id = "shared" }} };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{shared_source}, .assertions = &.{claim} }, .{}), "observe", 2);
    store.resource_state = .denied;
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 3).execute(a, .get, "claim", "{}", ""));
    // Force an unrelated cleanup while the worker cannot see the ordinary row.
    try discard(&store, .forget, "", "{\"source_ids\":[\"unrelated\"]}", "forget", 4);
    for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "denied-cleanup-{d}", .{i}), 40000);
    try std.testing.expect(store.values.get(try fixtureKey(a, "evidence", "shared")) != null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "assertion", "claim")) != null);
    store.resource_state = .current;
    _ = try store.service("alice", true, 40001).execute(a, .get, "claim", "{}", "");
}

test "memory explanation preserves independent support and decision-time outcome usage through correction" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const ordinary: m.types.Evidence = .{ .id = "ordinary", .resource_selector = .{ .table = "business", .id = "approval" } };
    const independent: m.types.Evidence = .{ .id = "human", .source_id = "human", .source_version = "1", .text = "Human independently approved" };
    const claim: m.types.Assertion = .{ .id = "claim", .subject = "deployment", .predicate = "approved", .value = "yes", .statement = "Approved", .evidence = &.{.{ .id = "ordinary" }}, .support_sets = &.{.{ .evidence = &.{.{ .id = "human" }} }} };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{ ordinary, independent }, .assertions = &.{claim} }, .{}), "observe", 2);
    const original = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 3).execute(a, .get, "claim", "{}", ""), .{});
    try discard(&store, .outcome, "", "{\"id\":\"feedback\",\"task_id\":\"release\",\"success\":true,\"used_memory_ids\":[\"claim\"]}", "feedback", 4);
    const outcome_revision = store.revision + 1;
    try discard(&store, .correct, "", try std.json.Stringify.valueAlloc(a, m.types.Correction{ .id = "claim", .expected_revision = original.revision, .lifecycle = .retracted, .reason = "Human correction", .evidence = &.{.{ .id = "human" }} }, .{}), "correct", 5);
    const outcome_ref: m.types.provenance.Resource = .{ .table = "memory_", .table_id = 0, .kind = .outcome, .id = "feedback", .revision = outcome_revision };
    const raw = try store.service("alice", true, 6).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = outcome_ref }, .{}), "");
    const explanation = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    const used = explanation.object.get("nodes").?.array.items[0].object.get("links").?.array.items[0];
    try std.testing.expectEqualStrings("used", used.object.get("relation").?.string);
    try std.testing.expectEqual(original.revision, @as(u64, @intCast(used.object.get("target").?.object.get("revision").?.integer)));
    store.resource_revision = 8;
    const corrected = try store.service("alice", true, 7).execute(a, .explain, "claim", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, corrected, "Human correction") != null);
    try std.testing.expect(std.mem.indexOf(u8, corrected, "Human independently approved") != null);
    try std.testing.expect(std.mem.indexOf(u8, corrected, "\"status\":\"changed\"") != null);
}

test "memory source watch pages remain immediately due until the bounded round completes" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    var sources: [17]m.types.Evidence = undefined;
    for (&sources, 0..) |*item, i| item.* = .{ .id = try std.fmt.allocPrint(a, "source-{d}", .{i}), .resource_selector = .{ .table = "business", .id = "approved" } };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &sources }, .{}), "observe", 2);
    try discard(&store, .advance, "", "{}", "page-one", 40000);
    const first = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try std.testing.expect(first.resource_cursor != null);
    try std.testing.expectEqual(@as(i64, 40000), first.next_resource_check_at.?);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"new-source\",\"resource_selector\":{\"table\":\"business\",\"id\":\"new\"}}]}", "concurrent-observe", 40001);
    const admitted = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try std.testing.expectEqualStrings(first.resource_cursor.?, admitted.resource_cursor.?);
    try std.testing.expectEqual(first.next_resource_check_at, admitted.next_resource_check_at);
    try discard(&store, .advance, "", "{}", "page-two", 40001);
    const second = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try std.testing.expect(second.resource_cursor == null);
    try std.testing.expectEqual(@as(i64, 70001), second.next_resource_check_at.?);
}

test "memory explanation reports invalid source metadata and authorizes cross-database edges independently" {
    for (0..2) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{}", "create", 1);
        const target: m.types.provenance.Resource = .{ .database = if (mode == 0) "default" else "other", .table = "business", .table_id = 7, .kind = .record, .id = "target", .revision = if (mode == 0) 0 else 7 };
        store.resource_json = try std.json.Stringify.valueAlloc(a, .{ .provenance = m.types.provenance.Envelope{ .links = &.{.{ .relation = .used, .target = target }} } }, .{});
        const root: m.types.provenance.Resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "root", .revision = 7 };
        const raw = try store.service("alice", true, 2).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = root }, .{}), "");
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        try std.testing.expect(!result.object.get("complete").?.bool);
        try std.testing.expectEqual(@as(usize, 1), result.object.get("nodes").?.array.items.len);
        if (mode == 0) {
            try std.testing.expect(result.object.get("incomplete_history").?.bool);
        } else {
            try std.testing.expect(result.object.get("missing_evidence").?.bool);
            const edge = result.object.get("nodes").?.array.items[0].object.get("links").?.array.items[0];
            try std.testing.expect(edge.object.get("target").? == .null);
        }
    }
}

test "memory bounds oversized ordinary snapshots and never admits a partial source" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    store.resource_json = try a.alloc(u8, m.types.provenance.max_snapshot_bytes + 1);
    @memset(@constCast(store.resource_json), 'x');
    const root: m.types.provenance.Resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "oversized", .revision = 7 };
    const raw = try store.service("alice", true, 2).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = root, .max_bytes = 1024 }, .{}), "");
    const result = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    try std.testing.expect(result.object.get("truncated").?.bool);
    try std.testing.expect(!result.object.get("complete").?.bool);
    try std.testing.expect(raw.len <= 1024);
    const before = store.revision;
    const evidence: m.types.Evidence = .{ .id = "oversized", .resource_selector = .{ .table = "business", .id = "oversized" } };
    try std.testing.expectError(error.MemoryInputTooLarge, store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "oversized"));
    try std.testing.expectEqual(before, store.revision);
}

test "memory cleanup preserves correction rationale across a temporary source denial" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "business", .resource_selector = .{ .table = "business", .id = "approval" } };
    const claim: m.types.Assertion = .{ .id = "claim", .subject = "deployment", .predicate = "approved", .value = "yes", .statement = "Approved", .evidence = &.{.{ .id = "business" }} };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .assertions = &.{claim} }, .{}), "observe", 2);
    const initial = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, try store.service("alice", true, 3).execute(a, .get, "claim", "{}", ""), .{});
    try discard(&store, .correct, "", try std.json.Stringify.valueAlloc(a, m.types.Correction{ .id = "claim", .expected_revision = initial.revision, .lifecycle = .active, .reason = "KEEP_HUMAN_CORRECTION", .evidence = &.{.{ .id = "business" }} }, .{}), "correct", 4);
    const before = try store.service("alice", true, 5).execute(a, .get, "claim", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, before, "KEEP_HUMAN_CORRECTION") != null);
    store.resource_state = .denied;
    try discard(&store, .forget, "", "{\"source_ids\":[\"unrelated\"]}", "forget", 6);
    for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 40000);
    store.resource_state = .current;
    const after = try store.service("alice", true, 40001).execute(a, .get, "claim", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, after, "KEEP_HUMAN_CORRECTION") != null);
}

test "memory known_at excludes subsequent entity curation decisions" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const e: m.types.Evidence = .{ .id = "e", .source_id = "human", .source_version = "1", .text = "Alice and Bob" };
    const entities = [_]m.types.Entity{ .{ .id = "alice", .name = "Alice", .evidence = &.{.{ .id = "e" }} }, .{ .id = "bob", .name = "Bob", .evidence = &.{.{ .id = "e" }} } };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{e}, .entities = &entities }, .{}), "observe", 2);
    const ref: m.types.provenance.Resource = .{ .table = "memory_", .table_id = 0, .kind = .entity, .id = "alice", .revision = 3 };
    try discard(&store, .curate_entity, "", try std.json.Stringify.valueAlloc(a, m.types.EntityCorrection{ .action = .merge, .from = "alice", .to = "bob", .expected_revision = 3, .reason = "FUTURE_MERGE_AT_10" }, .{}), "merge", 10);
    const raw = try store.service("alice", true, 11).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = ref, .known_at = 3 }, .{}), "");
    try std.testing.expect(std.mem.indexOf(u8, raw, "FUTURE_MERGE_AT_10") == null);
}

test "memory historical ordinary reference resolves captured snapshots without optional routing hints" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "e", .resource_selector = .{ .table = "business", .id = "approval" } };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "observe", 2);
    store.resource_revision = 8;
    const original: m.types.provenance.Resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "approval", .revision = 7 };
    _ = try store.service("alice", true, 3).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = original }, .{}), "");
}

test "memory known_at gates every native resource and ordinary snapshots" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const e: m.types.Evidence = .{ .id = "e", .resource_selector = .{ .table = "business", .id = "approval" } };
    const person: m.types.Entity = .{ .id = "person", .name = "Person", .evidence = &.{.{ .id = "e" }} };
    const claim: m.types.Assertion = .{ .id = "claim", .subject = "person", .predicate = "approved", .value = "yes", .statement = "Approved", .evidence = &.{.{ .id = "e" }} };
    const receipt = try std.json.parseFromSliceLeaky(m.types.Operation, a, try store.service("alice", true, 10).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{e}, .entities = &.{person}, .assertions = &.{claim} }, .{}), "observe"), .{});
    try discard(&store, .outcome, "", "{\"id\":\"feedback\",\"task_id\":\"release\",\"success\":true,\"used_memory_ids\":[\"claim\"]}", "feedback", 20);
    try discard(&store, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = @as(u64, 4), .config = m.types.Config{ .policy_version = "v2" } }, .{}), "configure", 30);
    const Case = struct { kind: m.types.provenance.Kind, id: []const u8, revision: u64, known: i64 };
    for ([_]Case{
        .{ .kind = .assertion, .id = "claim", .revision = 3, .known = 9 },
        .{ .kind = .entity, .id = "person", .revision = 3, .known = 9 },
        .{ .kind = .evidence, .id = "e", .revision = 3, .known = 9 },
        .{ .kind = .decision, .id = receipt.id, .revision = 3, .known = 9 },
        .{ .kind = .operation, .id = receipt.id, .revision = 3, .known = 9 },
        .{ .kind = .configuration, .id = "2", .revision = 2, .known = 29 },
        .{ .kind = .table, .id = "memory_", .revision = 2, .known = 0 },
        .{ .kind = .outcome, .id = "feedback", .revision = 4, .known = 19 },
    }) |item| {
        const ref: m.types.provenance.Resource = .{ .table = "memory_", .table_id = 0, .kind = item.kind, .id = item.id, .revision = item.revision };
        try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 40).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = ref, .known_at = item.known }, .{}), ""));
    }
    const external: m.types.provenance.Resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "approval", .revision = 7 };
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 40).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = external, .known_at = 9 }, .{}), ""));
    _ = try store.service("alice", true, 40).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = external, .known_at = 10 }, .{}), "");
}

test "memory operation explanation pins immutable versions while live managed receipts advance" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    _ = try modelService(&store, 10).execute(a, .advance, id, "{}", "process");
    const live = try std.json.parseFromSliceLeaky(m.types.Operation, a, try modelService(&store, 11).execute(a, .operation, id, "{}", ""), .{});
    try std.testing.expectEqual(m.types.Stage.succeeded, live.phase);
    const ref: m.types.provenance.Resource = .{ .table = "memory_", .table_id = 0, .kind = .operation, .id = id, .revision = 3 };
    const raw = try modelService(&store, 11).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = ref, .known_at = 3 }, .{}), "");
    const explanation = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    const root = explanation.object.get("nodes").?.array.items[0].object.get("record").?;
    try std.testing.expectEqualStrings("pending", root.object.get("phase").?.string);
    const applied = try modelService(&store, 12).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = .{ .table = "memory_", .table_id = 0, .kind = .operation, .id = id, .revision = 4 }, .known_at = 11 }, .{}), "");
    const final = try std.json.parseFromSliceLeaky(std.json.Value, a, applied, .{});
    try std.testing.expectEqualStrings("succeeded", final.object.get("nodes").?.array.items[0].object.get("record").?.object.get("phase").?.string);
}

test "memory snapshot identity ignores routing names but enforces optional content digests" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    store.resource_digest = @splat(0xab);
    try discard(&store, .create, "", "{}", "create", 1);
    const e: m.types.Evidence = .{ .id = "e", .resource_selector = .{ .table = "business", .id = "approval" } };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{e} }, .{}), "observe", 2);
    store.resource_revision = 8;
    var ref: m.types.provenance.Resource = .{ .table = "renamed_business", .table_id = 7, .kind = .record, .id = "approval", .revision = 7 };
    _ = try store.service("alice", true, 4).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = ref }, .{}), "");
    ref.digest = "ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB";
    _ = try store.service("alice", true, 4).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = ref }, .{}), "");
    ref.digest = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expectError(error.MemoryResourceChanged, store.service("alice", true, 4).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = ref }, .{}), ""));
}

test "memory physical cleanup preserves model reason lineage across temporary source denial" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try decisionService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, decisionConfig(), .{}), "create");
    const old: m.types.Evidence = .{ .id = "old-e", .resource_selector = .{ .table = "business", .id = "approval" } };
    const current: m.types.Evidence = .{ .id = "new-e", .source_id = "human", .source_version = "1", .text = "Human approved" };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{ old, current } }, .{}), "sources", 2);
    const prior: m.types.Assertion = .{ .id = "old", .subject = "production", .predicate = "credential", .value = "old", .statement = "REASON_LINEAGE_OLD", .valid_from = 100, .evidence = &.{.{ .id = "old-e" }} };
    _ = try decisionService(&store, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{prior} }, .{}), "old");
    var incoming = prior;
    incoming.id = "new";
    incoming.value = "new";
    incoming.statement = "New";
    incoming.valid_from = 200;
    incoming.evidence = &.{.{ .id = "new-e" }};
    _ = try decisionService(&store, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{incoming} }, .{}), "new");
    store.resource_state = .denied;
    try discard(&store, .forget, "", "{\"source_ids\":[\"unrelated\"]}", "forget", 5);
    for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "model-cleanup-{d}", .{i}), 40000);
    const denied = try store.service("alice", true, 40001).execute(a, .get, "new", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, denied, "REASON_LINEAGE_OLD") == null);
    store.resource_state = .current;
    const restored = try store.service("alice", true, 40002).execute(a, .get, "new", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, restored, "REASON_LINEAGE_OLD") != null);
}

test "memory outcome explanation applies retirement fences before physical cleanup" {
    const Cause = enum { all, source, evidence, assertion, expiry };
    for (std.enums.values(Cause)) |cause| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{}", "create", 1);
        const evidence: m.types.Evidence = .{ .id = "e1", .source_id = "runbook", .source_version = "v1", .text = "Production uses Helm", .expires_at = if (cause == .expiry) 5 else null };
        _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "source");
        try discard(&store, .assertions, "", helm, "claim", 3);
        const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try store.service("alice", true, 4).execute(a, .outcome, "", "{\"id\":\"result\",\"task_id\":\"task\",\"success\":true,\"used_memory_ids\":[\"helm\"],\"detail\":\"PRIVATE_FEEDBACK\"}", "outcome"), .{});
        const request = try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = .{ .table = "memory_", .table_id = 0, .kind = .outcome, .id = "result", .revision = receipt.revision } }, .{});
        _ = try store.service("alice", true, 4).execute(a, .explain, "", request, "");
        switch (cause) {
            .all => try discard(&store, .forget, "", "{\"all\":true}", "forget", 5),
            .source => try discard(&store, .forget, "", "{\"source_ids\":[\"runbook\"]}", "forget", 5),
            .evidence => try discard(&store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget", 5),
            .assertion => try discard(&store, .forget, "", "{\"assertion_ids\":[\"helm\"]}", "forget", 5),
            .expiry => {},
        }
        try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 6).execute(a, .explain, "", request, ""));
        // Reaching the outcome through the admitting decision must redact it too.
        const decision = try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = .{ .table = "memory_", .table_id = 0, .kind = .decision, .id = receipt.id, .revision = receipt.revision } }, .{});
        const graph = try store.service("alice", true, 6).execute(a, .explain, "", decision, "");
        try std.testing.expect(std.mem.indexOf(u8, graph, "PRIVATE_FEEDBACK") == null);
        try std.testing.expect(std.mem.indexOf(u8, graph, "missing_or_denied") != null);
        for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 7);
        try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 8).execute(a, .explain, "", request, ""));
    }
}

test "memory outcome source denial hides feedback without retiring it" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "e1", .resource_selector = .{ .table = "business", .id = "approval" } };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "source");
    try discard(&store, .assertions, "", helm, "claim", 3);
    const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try store.service("alice", true, 4).execute(a, .outcome, "", "{\"id\":\"result\",\"task_id\":\"task\",\"success\":true,\"used_memory_ids\":[\"helm\"],\"detail\":\"RETAINED_FEEDBACK\"}", "outcome"), .{});
    const request = try std.json.Stringify.valueAlloc(a, m.types.provenance.ExplainRequest{ .resource = .{ .table = "memory_", .table_id = 0, .kind = .outcome, .id = "result", .revision = receipt.revision } }, .{});
    store.resource_state = .denied;
    try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 5).execute(a, .explain, "", request, ""));
    try discard(&store, .forget, "", "{\"source_ids\":[\"unrelated\"]}", "unrelated", 5);
    for (0..16) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), 6);
    store.resource_state = .current;
    const raw = try store.service("alice", true, 7).execute(a, .explain, "", request, "");
    try std.testing.expect(std.mem.indexOf(u8, raw, "RETAINED_FEEDBACK") != null);
}

test "memory reindex preserves denied coverage obligations without spending retries" {
    for ([_]bool{ false, true }) |external| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{}", "create", 1);
        const evidence: m.types.Evidence = .{ .id = "e1", .resource_selector = .{ .table = "business", .id = "approval" } };
        _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "source");
        try discard(&store, .assertions, "", helm, "claim", 3);
        const model: m.types.Model = if (external) .{ .provider = "external", .model = "B", .revision = "B", .dimensions = 2 } else fake_model;
        const pending = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try configureTestSpace(&store, .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = model } }, "config", 4), .{});
        store.resource_state = .denied;
        for (0..5) |i| try std.testing.expectError(error.MemorySourceAccessDenied, modelService(&store, 5).execute(a, .advance, pending.id, "{}", try std.fmt.allocPrint(a, "blocked-{d}", .{i})));
        const info = try std.json.parseFromSliceLeaky(m.types.Inspection, a, try modelService(&store, 6).execute(a, .inspect, "", "{}", ""), .{});
        try std.testing.expect(info.state.pending_config != null);
        try std.testing.expect(!info.state.config.features.semantic_recall);
        const blocked = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, 6).execute(a, .operation, pending.id, "{}", ""), .{});
        try std.testing.expectEqual(@as(u8, 0), blocked.attempts);
        try std.testing.expectEqual(m.types.Stage.pending, blocked.phase);
        store.resource_state = .current;
        if (external) _ = try modelService(&store, 7).execute(a, .stage_embeddings, "", try std.json.Stringify.valueAlloc(a, m.types.EmbeddingBatch{ .generation = pending.config_generation, .model = model, .items = &.{.{ .id = "helm", .embedding = &.{ 1, 0 } }} }, .{}), "stage");
        _ = try modelService(&store, 8).execute(a, .advance, pending.id, "{}", "resume");
        const raw = try modelService(&store, 500).execute(a, .recall, "", try std.json.Stringify.valueAlloc(a, .{ .query = "zzzz", .embedding = &[_]f32{ 1, 0 }, .embedding_model = model, .token_budget = 8192 }, .{}), "");
        try std.testing.expectEqual(@as(usize, 1), (try std.json.parseFromSliceLeaky(m.types.RecallResult, a, raw, .{})).items.len);
    }
}

test "memory configuration coverage blocks denied knowledge but excludes irreversible retirement" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "e1", .source_id = "runbook", .source_version = "1", .resource_selector = .{ .table = "business", .id = "approval" } };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "source");
    try discard(&store, .assertions, "", helm, "claim", 3);
    store.resource_state = .denied;
    try std.testing.expectError(error.MemorySourceAccessDenied, configureTestSpace(&store, .{ .ontology_version = "new-ontology" }, "blocked-ontology", 4));
    const model: m.types.Model = .{ .provider = "external", .model = "B", .revision = "B", .dimensions = 2 };
    const pending = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try configureTestSpace(&store, .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = model } }, "config", 4), .{});
    try discard(&store, .forget, "", "{\"source_ids\":[\"runbook\"]}", "retire", 5);
    _ = try modelService(&store, 6).execute(a, .advance, pending.id, "{}", "cutover");
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try std.testing.expect(state.pending_config == null);
    try std.testing.expect(state.config.features.semantic_recall);
    try std.testing.expectEqualStrings("memory/v1", state.config.ontology_version);
}

test "memory managed observations retain primary and contextual obligations through source denial" {
    for ([_]bool{ false, true }) |context_only| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
        _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        const ordinary: m.types.Evidence = .{ .id = if (context_only) "context" else "e1", .resource_selector = .{ .table = "business", .id = "approval" } };
        const first = try modelService(&store, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{ordinary} }, .{}), "ordinary");
        const admitted = if (context_only) try modelService(&store, 3).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"note\",\"source_version\":\"1\",\"text\":\"Production uses Helm\"}],\"context_evidence_ids\":[\"context\"]}", "observe") else first;
        const pending = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, admitted, .{});
        const before = store.revision;
        store.resource_state = .denied;
        for (0..5) |i| try std.testing.expectError(error.MemorySourceAccessDenied, modelService(&store, 4).execute(a, .advance, pending.id, "{}", try std.fmt.allocPrint(a, "denied-{d}", .{i})));
        try std.testing.expectEqual(before, store.revision);
        const blocked = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, 5).execute(a, .operation, pending.id, "{}", ""), .{});
        try std.testing.expectEqual(m.types.Stage.pending, blocked.phase);
        try std.testing.expectEqual(@as(u8, 0), blocked.attempts);
        try std.testing.expect(blocked.next_retry_at == null);
        store.resource_state = .current;
        const resumed = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, 6).execute(a, .advance, pending.id, "{}", "resume"), .{});
        try std.testing.expectEqual(m.types.Stage.succeeded, resumed.phase);
        try std.testing.expect(resumed.assertions_ready);
        const original = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, 7).execute(a, .operation, pending.id, "{}", ""), .{});
        try std.testing.expectEqual(original.phase, resumed.phase);
        try std.testing.expectEqual(original.attempts, resumed.attempts);
        _ = try modelService(&store, 7).execute(a, .get, "managed", "{}", "");
    }
}

test "memory cancellation consistently projects readiness and immutable decision status" {
    const Cause = enum { configuration, missing, changed, expiry, forgotten };
    for (std.enums.values(Cause)) |cause| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
        _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        const evidence: m.types.Evidence = .{ .id = "e1", .expires_at = if (cause == .expiry) 3 else null, .resource_selector = .{ .table = "business", .id = "approval" } };
        const pending = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "observe"), .{});
        switch (cause) {
            .configuration => {
                var replacement = config;
                replacement.policy_version = "v2";
                _ = try configureTestSpace(&store, replacement, "config", 3);
            },
            .missing => store.resource_state = .missing,
            .changed => store.resource_state = .changed,
            .expiry => {},
            .forgotten => try discard(&store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "forget", 3),
        }
        for ([_][]const u8{ "advance", "terminal-replay" }, 0..) |token, i| {
            const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, @intCast(4 + i)).execute(a, .advance, pending.id, "{}", token), .{});
            const original = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try modelService(&store, 6).execute(a, .operation, pending.id, "{}", ""), .{});
            try std.testing.expectEqual(m.types.Stage.cancelled, original.phase);
            try std.testing.expectEqual(original.phase, receipt.phase);
            try std.testing.expect(!receipt.assertions_ready);
            try std.testing.expect(!original.assertions_ready);
            try std.testing.expectEqualStrings(original.error_code.?, receipt.error_code.?);
            try std.testing.expect(receipt.next_retry_at == null);
            if (i == 0) {
                const audit = try std.json.parseFromSliceLeaky(m.types.provenance.Decision, a, store.values.get(try fixtureKey(a, "audit", receipt.id)).?, .{});
                try std.testing.expectEqual(@TypeOf(audit.status).cancelled, audit.status);
                try std.testing.expect(audit.applied_at == null);
            }
        }
    }
}

test "memory publication rechecks uncited context and preserves denied work" {
    const Model = struct {
        fn generate(ptr: *anyopaque, a: A, config: m.types.Config, sources: []const m.types.StoredEvidence, boundary: m.InferenceBoundary) !m.types.Generated {
            const output = try success(ptr, a, config, sources, boundary);
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.resource_state = .denied;
            return output;
        }
        fn success(ptr: *anyopaque, a: A, config: m.types.Config, sources: []const m.types.StoredEvidence, boundary: m.InferenceBoundary) !m.types.Generated {
            const output = try fakeGenerate(ptr, a, config, sources, boundary);
            const claims = try a.dupe(m.types.Assertion, output.assertions);
            claims[0].evidence = &.{.{ .id = "e2" }};
            return .{ .assertions = claims };
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try managedSetup(&store);
    // The model cites only e1; the ordinary record is admitted as context.
    _ = try modelService(&store, 3).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"context\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "context");
    const observed = try modelService(&store, 4).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e2\",\"source_id\":\"note2\",\"source_version\":\"1\",\"text\":\"Deployment\"}],\"context_evidence_ids\":[\"e1\",\"context\"]}", "with-context");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, observed, .{});
    const revision = store.revision;
    var service = modelService(&store, 5);
    service.backend.generate = Model.generate;
    try std.testing.expectError(error.MemorySourceAccessDenied, service.execute(a, .advance, pending.id, "{}", "attempt"));
    try std.testing.expectEqual(revision, store.revision);
    const status = try modelService(&store, 6).execute(a, .operation, pending.id, "{}", "");
    const unchanged = try std.json.parseFromSliceLeaky(m.types.Operation, a, status, .{});
    try std.testing.expectEqual(@as(u8, 0), unchanged.attempts);
    try std.testing.expectEqual(m.types.Stage.pending, unchanged.phase);
    store.resource_state = .current;
    service = modelService(&store, 7);
    service.backend.generate = Model.success;
    const retried = try service.execute(a, .advance, pending.id, "{}", "attempt");
    const ready = try std.json.parseFromSliceLeaky(m.types.Operation, a, retried, .{});
    try std.testing.expectEqual(m.types.Stage.succeeded, ready.phase);
    _ = try modelService(&store, 8).execute(a, .get, "managed", "{}", "");
}

test "memory generation rejects changed source revisions without partial publication" {
    const Model = struct {
        fn generate(ptr: *anyopaque, a: A, config: m.types.Config, sources: []const m.types.StoredEvidence, boundary: m.InferenceBoundary) !m.types.Generated {
            const output = try fakeGenerate(ptr, a, config, sources, boundary);
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.resource_revision += 1;
            return output;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    const observed = try modelService(&store, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "observe");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, observed, .{});
    var service = modelService(&store, 3);
    service.backend.generate = Model.generate;
    const result = try service.execute(a, .advance, pending.id, "{}", "attempt");
    const cancelled = try std.json.parseFromSliceLeaky(m.types.Operation, a, result, .{});
    try std.testing.expectEqual(m.types.Stage.cancelled, cancelled.phase);
    try std.testing.expect(!cancelled.assertions_ready);
    var rows = store.values.keyIterator();
    while (rows.next()) |key| try std.testing.expect(!std.mem.startsWith(u8, key.*, "assertion/"));
}

test "memory listing batches selective scans and resumes at consumed rows" {
    var store = Store.init();
    defer store.deinit();
    try setup(&store);
    const a = store.arena.allocator();
    var claims: [256]m.types.Assertion = undefined;
    for (&claims, 0..) |*claim, i| claim.* = .{ .id = try std.fmt.allocPrint(a, "claim-{d}", .{i}), .subject = try std.fmt.allocPrint(a, "subject-{d}", .{i}), .predicate = if (i % 3 == 0) "selected" else "other", .value = "Helm", .statement = "Deployment", .evidence = &.{.{ .id = "e1" }} };
    for (0..4) |batch| {
        _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims[batch * 64 ..][0..64] }, .{}), try std.fmt.allocPrint(a, "claims-{d}", .{batch}));
    }
    store.scans = 0;
    const empty = try store.service("alice", true, 4).execute(a, .list, "", "{\"predicate\":\"missing\",\"limit\":1}", "");
    const PageResult = struct { items: []const m.types.StoredAssertion, next_cursor: ?[]const u8, complete: bool, revision: u64 };
    const no_matches = try std.json.parseFromSliceLeaky(PageResult, a, empty, .{});
    try std.testing.expect(no_matches.complete);
    try std.testing.expectEqual(@as(usize, 0), no_matches.items.len);
    try std.testing.expectEqual(@as(usize, 2), store.scans);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var cursor: ?[]const u8 = null;
    while (true) {
        const body = try std.json.Stringify.valueAlloc(a, .{ .predicate = "selected", .limit = @as(u16, 7), .cursor = cursor }, .{});
        const result = try store.service("alice", true, 5).execute(a, .list, "", body, "");
        const page = try std.json.parseFromSliceLeaky(PageResult, a, result, .{});
        for (page.items) |record| {
            try std.testing.expect(!seen.contains(record.assertion.id));
            try seen.put(a, record.assertion.id, {});
        }
        cursor = page.next_cursor;
        if (page.complete) break;
        try std.testing.expect(cursor != null);
    }
    try std.testing.expectEqual(@as(usize, 86), seen.count());
}

test "memory embedding and reconciliation revalidate source authority" {
    const Model = struct {
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.resource_state = .denied;
            return fakeEmbed(ptr, a, model, text);
        }
        fn decide(ptr: *anyopaque, a: A, config: m.types.Config, claim: m.types.Assertion, candidates: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.resource_state = .denied;
            return reviewDecide(ptr, a, config, claim, candidates);
        }
    };
    for ([_]bool{ false, true }) |semantic| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        var service = modelService(&store, 1);
        service.backend.decide = reviewDecide;
        const config: m.types.Config = .{ .features = .{ .semantic_recall = semantic, .reconciliation = if (semantic) .explicit else .model }, .models = .{ .embedder = if (semantic) fake_model else null, .decider = if (semantic) null else fake_model }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one }} };
        _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "source");
        _ = try service.execute(a, .assertions, "", helm, "helm");
        const revision = store.revision;
        service.backend.embed = Model.embed;
        service.backend.decide = Model.decide;
        try std.testing.expectError(error.MemorySourceAccessDenied, service.execute(a, .assertions, "", terraform, "terraform"));
        try std.testing.expectEqual(revision, store.revision);
        store.resource_state = .current;
        const existing = try service.execute(a, .get, "helm", "{}", "");
        const record = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, existing, .{});
        try std.testing.expectEqual(m.types.Lifecycle.active, record.lifecycle);
        try std.testing.expectError(error.MemoryNotFound, service.execute(a, .get, "terraform", "{}", ""));
    }
}

test "memory inferred publication checks sources after final authorization" {
    const Authority = struct {
        fn check(ptr: *anyopaque) !void {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.resource_state = .denied;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    const observed = try modelService(&store, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "observe");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, observed, .{});
    var service = modelService(&store, 3);
    service.context.final_check_ptr = &store;
    service.context.final_check_fn = Authority.check;
    const revision = store.revision;
    try std.testing.expectError(error.MemorySourceAccessDenied, service.execute(a, .advance, pending.id, "{}", "attempt"));
    try std.testing.expectEqual(revision, store.revision);
    store.resource_state = .current;
    try std.testing.expectError(error.MemoryNotFound, modelService(&store, 4).execute(a, .get, "managed", "{}", ""));
}

test "memory reindex source changes roll back projections and release configuration admission" {
    const Model = struct {
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.resource_revision += 1;
            return fakeEmbed(ptr, a, model, text);
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try modelService(&store, 1).execute(a, .create, "", "{}", "create");
    _ = try modelService(&store, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "source");
    _ = try modelService(&store, 3).execute(a, .assertions, "", helm, "helm");
    const previous = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    const configured = try modelService(&store, 4).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = previous.revision, .config = config }, .{}), "configure");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, configured, .{});
    var service = modelService(&store, 5);
    service.backend.embed = Model.embed;
    const result = try service.execute(a, .advance, pending.id, "{}", "advance");
    const receipt = try std.json.parseFromSliceLeaky(m.types.Operation, a, result, .{});
    try std.testing.expectEqual(m.types.Stage.cancelled, receipt.phase);
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try std.testing.expect(state.pending_config == null);
    try std.testing.expectEqual(previous.config.generation, state.config.generation);
    var keys = store.values.keyIterator();
    while (keys.next()) |key| try std.testing.expect(!std.mem.startsWith(u8, key.*, "vector/"));
    _ = try modelService(&store, 6).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = state.revision, .config = previous.config }, .{}), "next-configuration");
}

test "memory embedding source checks scale with each invocation input set" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    const service = modelService(&store, 1);
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    var evidence: [32]m.types.Evidence = undefined;
    var claims: [32]m.types.Assertion = undefined;
    for (&evidence, &claims, 0..) |*source_item, *claim, i| {
        const id = try std.fmt.allocPrint(a, "source-{d}", .{i});
        source_item.* = .{ .id = id, .resource_selector = .{ .table = "business", .id = id } };
        const refs = try a.alloc(m.types.EvidenceRef, 1);
        refs[0] = .{ .id = id };
        claim.* = .{ .id = try std.fmt.allocPrint(a, "claim-{d}", .{i}), .subject = "production", .predicate = "uses", .value = id, .statement = "Deployment", .evidence = refs };
    }
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = evidence }, .{}), "sources");
    store.resource_reads = 0;
    _ = try service.execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims");
    // Admission, that invocation's boundary, and transaction publication each
    // visit a source once. Earlier invocation inputs do not accumulate here.
    try std.testing.expectEqual(@as(usize, 96), store.resource_reads);
}

test "memory final publication still fences earlier invocation inputs" {
    const Model = struct {
        fn resource(ptr: *anyopaque, a: A, reference: m.types.provenance.Resource) !m.types.provenance.Snapshot {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (store.reject_decision and std.mem.eql(u8, reference.id, "first")) return .{ .state = .denied };
            return Store.resource(ptr, a, reference);
        }
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            if (store.decisions == 2) store.reject_decision = true;
            return fakeEmbed(ptr, a, model, text);
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = modelService(&store, 1);
    service.backend.resource = Model.resource;
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"first\",\"resource_selector\":{\"table\":\"business\",\"id\":\"first\"}},{\"id\":\"second\",\"resource_selector\":{\"table\":\"business\",\"id\":\"second\"}}]}", "sources");
    service.backend.embed = Model.embed;
    const claims = [_]m.types.Assertion{
        .{ .id = "first", .subject = "production", .predicate = "uses", .value = "A", .statement = "First", .evidence = &.{.{ .id = "first" }} },
        .{ .id = "second", .subject = "production", .predicate = "uses", .value = "B", .statement = "Second", .evidence = &.{.{ .id = "second" }} },
    };
    const revision = store.revision;
    try std.testing.expectError(error.MemorySourceAccessDenied, service.execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims"));
    try std.testing.expectEqual(@as(usize, 2), store.decisions);
    try std.testing.expectEqual(revision, store.revision);
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "assertion/", null, 64)).rows.len);
}

test "memory invocation scopes retain cached parent dependencies" {
    const Model = struct {
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            store.resource_state = .denied;
            return fakeEmbed(ptr, a, model, text);
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = modelService(&store, 1);
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}},{\"id\":\"note\",\"source_id\":\"note\",\"source_version\":\"1\",\"text\":\"Deployment\"}]}", "sources");
    _ = try service.execute(a, .assertions, "", helm, "parent");
    const child: m.types.Assertion = .{ .id = "child", .subject = "production", .predicate = "child", .value = "A", .statement = "Child", .evidence = &.{.{ .id = "note" }}, .depends_on = &.{"helm"} };
    var second = child;
    second.id = "second";
    service.backend.embed = Model.embed;
    try std.testing.expectError(error.MemorySourceAccessDenied, service.execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{ child, second } }, .{}), "children"));
    // The parent source must be checked at the first child's boundary, rather
    // than discovering its denial only after both model calls at commit.
    try std.testing.expectEqual(@as(usize, 1), store.decisions);
}

test "memory timeout recovery persists bounded attempts and idempotent receipts" {
    const Worker = struct {
        fn check(ptr: *anyopaque) !void {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (store.failures == 1) return error.DeadlineExceeded;
        }
        fn generate(ptr: *anyopaque, _: A, _: m.types.Config, _: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.failures = 1;
            return error.DeadlineExceeded;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    for (0..3) |attempt| {
        const now: i64 = @intCast(10000 * (attempt + 1));
        var service = modelService(&store, now);
        const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        const token = try std.fmt.allocPrint(a, "worker-{d}", .{attempt});
        var expired = service;
        expired.backend.generate = Worker.generate;
        expired.context.check_ptr = &store;
        expired.context.check_fn = Worker.check;
        store.failures = 0;
        try std.testing.expectError(error.DeadlineExceeded, expired.advanceAtRevision(a, id, token, state.revision));
        // Only the worker creates a fresh finalization context. Reusing the
        // foreground's expired context remains forbidden and writes nothing.
        try std.testing.expectError(error.DeadlineExceeded, expired.recordAttemptFailure(a, id, token, state.revision, error.DeadlineExceeded));
        const recovered = try service.recordAttemptFailure(a, id, token, state.revision, error.DeadlineExceeded);
        const status = try std.json.parseFromSliceLeaky(m.types.Operation, a, recovered, .{});
        try std.testing.expectEqual(@as(u8, @intCast(attempt + 1)), status.attempts);
        try std.testing.expectEqual(if (attempt == 2) m.types.Stage.failed else .pending, status.phase);
        try std.testing.expectEqualStrings("DeadlineExceeded", status.error_code.?);
        try std.testing.expect(!status.assertions_ready);
        if (attempt < 2) try std.testing.expect(status.next_retry_at.? > now);
        // Receipt lookup precedes the old revision fence on ambiguous replay.
        try std.testing.expectEqualStrings(recovered, try service.recordAttemptFailure(a, id, token, state.revision, error.DeadlineExceeded));
    }
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "pending/", null, 64)).rows.len);
}

test "memory timeout recovery cannot overwrite newer state or committed success" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    _ = try modelService(&store, 3).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"other\",\"source_id\":\"other\",\"source_version\":\"1\",\"text\":\"Other\"}]}", "concurrent");
    const service = modelService(&store, 4);
    try std.testing.expectError(error.MemoryRevisionConflict, service.recordAttemptFailure(a, id, "stale", state.revision, error.DeadlineExceeded));
    const current = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    const succeeded = try service.advanceAtRevision(a, id, "committed", current.revision);
    const replay = try service.recordAttemptFailure(a, id, "committed", current.revision, error.DeadlineExceeded);
    try std.testing.expectEqualStrings(succeeded, replay);
    const status = try std.json.parseFromSliceLeaky(m.types.Operation, a, replay, .{});
    try std.testing.expectEqual(m.types.Stage.succeeded, status.phase);
    try std.testing.expectEqual(@as(u8, 1), status.attempts);
}

test "memory timeout recovery rechecks live publication authority" {
    const Authority = struct {
        fn check(ptr: *anyopaque) !void {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (store.reject_decision) return error.Forbidden;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    var service = modelService(&store, 10000);
    service.context.final_check_ptr = &store;
    service.context.final_check_fn = Authority.check;
    store.reject_decision = true;
    const revision = store.revision;
    try std.testing.expectError(error.Forbidden, service.recordAttemptFailure(a, id, "attempt", state.revision, error.DeadlineExceeded));
    try std.testing.expectEqual(revision, store.revision);
    store.reject_decision = false;
    const result = try service.recordAttemptFailure(a, id, "attempt", state.revision, error.DeadlineExceeded);
    const receipt = try std.json.parseFromSliceLeaky(m.types.Operation, a, result, .{});
    try std.testing.expectEqual(@as(u8, 1), receipt.attempts);
}

test "memory reindex timeout recovery exhausts retries without publishing configuration" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    try discard(&store, .assertions, "", helm, "claim", 3);
    const previous = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    const configured = try modelService(&store, 4).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = previous.revision, .config = config }, .{}), "configure");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, configured, .{});
    for (0..3) |attempt| {
        const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        const now: i64 = @intCast((attempt + 1) * 10000);
        const result = try modelService(&store, now).recordAttemptFailure(a, pending.id, try std.fmt.allocPrint(a, "attempt-{d}", .{attempt}), state.revision, error.DeadlineExceeded);
        const receipt = try std.json.parseFromSliceLeaky(m.types.Operation, a, result, .{});
        try std.testing.expectEqual(@as(u8, @intCast(attempt + 1)), receipt.attempts);
        try std.testing.expectEqual(if (attempt == 2) m.types.Stage.failed else .pending, receipt.phase);
        const latest = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        try std.testing.expectEqual(previous.config.generation, latest.config.generation);
        try std.testing.expectEqual(attempt < 2, latest.pending_config != null);
    }
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "vector/", null, 64)).rows.len);
}

test "memory provider timeout finalizes with backoff from completion time" {
    const Worker = struct {
        fn generate(ptr: *anyopaque, _: A, _: m.types.Config, _: []const m.types.StoredEvidence, _: m.InferenceBoundary) !m.types.Generated {
            const store: *Store = @ptrCast(@alignCast(ptr));
            return if (store.failures == 1) error.Timeout else error.DeadlineExceeded;
        }
    };
    for ([_]anyerror{ error.Timeout, error.DeadlineExceeded }) |timeout| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const id = try managedSetup(&store);
        const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        var service = modelService(&store, 3);
        service.backend.generate = Worker.generate;
        store.failures = if (timeout == error.Timeout) 1 else 0;
        const revision = store.revision;
        // The provider times out while the operation's storage grace is live.
        try std.testing.expectError(timeout, service.advanceAtRevision(a, id, "attempt", state.revision));
        try std.testing.expectEqual(revision, store.revision);
        service.context.now_ms = 120003;
        const raw = try service.recordAttemptFailure(a, id, "attempt", state.revision, timeout);
        const receipt = try std.json.parseFromSliceLeaky(m.types.Operation, a, raw, .{});
        try std.testing.expectEqual(@as(u8, 1), receipt.attempts);
        try std.testing.expect(receipt.next_retry_at.? > service.context.now_ms);
        try std.testing.expectError(error.MemoryRetryNotReady, service.advanceAtRevision(a, id, "early", receipt.revision));
    }
}

test "memory invocation epochs reauthorize cached inputs before later model use" {
    const Model = struct {
        fn resource(ptr: *anyopaque, a: A, reference: m.types.provenance.Resource) !m.types.provenance.Snapshot {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (store.reject_decision and std.mem.eql(u8, reference.id, "first")) return .{ .state = .denied };
            return Store.resource(ptr, a, reference);
        }
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            if (store.decisions == 2) store.reject_decision = true;
            return fakeEmbed(ptr, a, model, text);
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = modelService(&store, 1);
    service.backend.resource = Model.resource;
    const config: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"first\",\"resource_selector\":{\"table\":\"business\",\"id\":\"first\"}},{\"id\":\"other\",\"resource_selector\":{\"table\":\"business\",\"id\":\"other\"}}]}", "sources");
    service.backend.embed = Model.embed;
    const claims = [_]m.types.Assertion{
        .{ .id = "first", .subject = "production", .predicate = "uses", .value = "A", .statement = "First", .evidence = &.{.{ .id = "first" }} },
        .{ .id = "other", .subject = "production", .predicate = "uses", .value = "B", .statement = "Other", .evidence = &.{.{ .id = "other" }} },
        .{ .id = "reuse", .subject = "production", .predicate = "uses", .value = "A", .statement = "Reuse first input", .evidence = &.{.{ .id = "first" }} },
    };
    const revision = store.revision;
    try std.testing.expectError(error.MemorySourceAccessDenied, service.execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims"));
    // The third invocation must not receive input revoked during the unrelated
    // second invocation, even though that source already has a cached snapshot.
    try std.testing.expectEqual(@as(usize, 2), store.decisions);
    try std.testing.expectEqual(revision, store.revision);
}

test "memory failure finalization does not reacquire a timed out input plan" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .managed, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
    _ = try modelService(&store, 1).execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    const observed = try modelService(&store, 2).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "source");
    const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, observed, .{});
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    store.resource_state = .denied;
    const reads = store.resource_reads;
    // Recording the already completed processing failure needs destination
    // authority, not another read of its former inputs. It emits no artifacts.
    const raw = try modelService(&store, 10000).recordAttemptFailure(a, pending.id, "timeout", state.revision, error.Timeout);
    const receipt = try std.json.parseFromSliceLeaky(m.types.Operation, a, raw, .{});
    try std.testing.expectEqual(reads, store.resource_reads);
    try std.testing.expectEqual(@as(u8, 1), receipt.attempts);
    try std.testing.expect(!receipt.assertions_ready);
    try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "assertion/", null, 64)).rows.len);
    const latest = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    // The next real processing attempt still checks source authorization and
    // cannot consume an additional model attempt while access is denied.
    try std.testing.expectError(error.MemorySourceAccessDenied, modelService(&store, 20000).advanceAtRevision(a, pending.id, "next", latest.revision));
}

test "memory input budgets preserve control recovery erasure and authorized replay" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const created = try store.run(.create, "", "{\"max_input_bytes\":1}", "create", 1);
    defer std.testing.allocator.free(created);
    try discard(&store, .inspect, "", "{}", "", 2);
    try discard(&store, .advance, "", "{}", "maintain", 2);
    try std.testing.expectError(error.Forbidden, store.service("bob", false, 3).execute(a, .configure, "", "{\"expected_revision\":3,\"config\":{}}", "restore"));
    try discard(&store, .configure, "", "{\"expected_revision\":3,\"config\":{}}", "restore", 3);
    try discard(&store, .observe, "", source, "source", 4);
    const claim = try store.run(.assertions, "", helm, "claim", 5);
    defer std.testing.allocator.free(claim);
    try discard(&store, .configure, "", "{\"expected_revision\":6,\"config\":{\"max_input_bytes\":1}}", "shrink", 6);
    const replay = try store.run(.assertions, "", helm, "claim", 7);
    defer std.testing.allocator.free(replay);
    try std.testing.expectEqualStrings(claim, replay);
    const replay_create = try store.run(.create, "", "{\"max_input_bytes\":1}", "create", 7);
    defer std.testing.allocator.free(replay_create);
    try std.testing.expectEqualStrings(created, replay_create);
    try std.testing.expectError(error.MemoryInputTooLarge, store.run(.assertions, "", terraform, "new", 7));
    const Revoke = struct {
        fn grant(_: *anyopaque, _: m.types.RequiredGrant) !void {
            return error.Forbidden;
        }
    };
    var revoked = store.service("alice", true, 7);
    revoked.context.grant_ptr = &store;
    revoked.context.require_grant_fn = Revoke.grant;
    try std.testing.expectError(error.Forbidden, revoked.execute(a, .assertions, "", helm, "claim"));
    try discard(&store, .forget, "", "{\"all\":true}", "erase", 8);
    try discard(&store, .advance, "", "{}", "cleanup", 9);
    try discard(&store, .inspect, "", "{}", "", 10);
    try discard(&store, .configure, "", "{\"expected_revision\":9,\"config\":{}}", "recover", 11);
    // Every action still has the absolute transport/service allocation bound.
    const oversized = try a.alloc(u8, 4 * 1024 * 1024 + 1);
    @memset(oversized, ' ');
    try std.testing.expectError(error.MemoryInputTooLarge, store.run(.inspect, "", oversized, "", 12));
}

test "memory generation stage fences include uncited context without charging denied attempts" {
    const Pipeline = struct {
        fn generate(ptr: *anyopaque, a: A, config: m.types.Config, inputs: []const m.types.StoredEvidence, boundary: m.InferenceBoundary) !m.types.Generated {
            const store: *Store = @ptrCast(@alignCast(ptr));
            for (0..3) |_| {
                store.decisions += 1;
                if (store.decisions == store.failures) store.resource_state = if (store.public_output) .changed else .denied;
                try boundary.complete();
            }
            return fakeGenerate(ptr, a, config, inputs, boundary);
        }
    };
    for ([_]bool{ false, true }) |changed| for (1..3) |stage| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        _ = try managedSetup(&store);
        // The model cites only e1, but a resource-backed context source is part
        // of the admitted generation plan and must fence every provider stage.
        _ = try modelService(&store, 3).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"context\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}]}", "context");
        const accepted = try modelService(&store, 4).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e1\",\"source_id\":\"note\",\"source_version\":\"1\",\"text\":\"Production uses Helm\",\"managed\":true}],\"context_evidence_ids\":[\"context\"]}", "with-context");
        const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, accepted, .{});
        var svc = modelService(&store, 5);
        svc.backend.generate = Pipeline.generate;
        store.failures = @intCast(stage);
        store.public_output = changed;
        const before = store.revision;
        if (changed) {
            _ = try svc.execute(a, .advance, pending.id, "{}", "advance");
        } else {
            try std.testing.expectError(error.MemorySourceAccessDenied, svc.execute(a, .advance, pending.id, "{}", "advance"));
            try std.testing.expectEqual(before, store.revision);
        }
        try std.testing.expectEqual(stage, store.decisions);
        store.resource_state = .current;
        const status = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try svc.execute(a, .operation, pending.id, "{}", ""), .{});
        try std.testing.expectEqual(if (changed) m.types.Stage.cancelled else .pending, status.phase);
        try std.testing.expectEqual(@as(u8, if (changed) 1 else 0), status.attempts);
        try std.testing.expect(!status.assertions_ready);
        try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "assertion/", null, 256)).rows.len);
    };
}

test "memory generation stops between providers when evidence is concurrently forgotten" {
    const Pipeline = struct {
        fn generate(ptr: *anyopaque, _: A, _: m.types.Config, _: []const m.types.StoredEvidence, boundary: m.InferenceBoundary) !m.types.Generated {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            try discard(store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "concurrent-forget", 4);
            try boundary.complete();
            store.decisions += 1;
            return .{};
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const id = try managedSetup(&store);
    var svc = modelService(&store, 3);
    svc.backend.generate = Pipeline.generate;
    try std.testing.expectError(error.MemoryRevisionConflict, svc.execute(a, .advance, id, "{}", "attempt"));
    try std.testing.expectEqual(@as(usize, 1), store.decisions);
    const status = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try svc.execute(a, .operation, id, "{}", ""), .{});
    try std.testing.expectEqual(@as(u8, 0), status.attempts);
    try std.testing.expectEqual(m.types.Stage.pending, status.phase);
}

test "memory embedding batches and reindex stop after concurrent evidence erasure" {
    const Model = struct {
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            if (store.decisions == 1) try discard(store, .forget, "", "{\"evidence_ids\":[\"e1\"]}", "concurrent-forget", 4);
            return fakeEmbed(ptr, a, model, text);
        }
    };
    for ([_]bool{ false, true }) |reindex| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        var svc = modelService(&store, 1);
        const config: m.types.Config = .{ .features = .{ .semantic_recall = !reindex }, .models = .{ .embedder = if (reindex) null else fake_model } };
        _ = try svc.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        _ = try svc.execute(a, .observe, "", source, "source");
        var claims: [2]m.types.Assertion = undefined;
        for (&claims, 0..) |*claim, i| claim.* = .{ .id = try std.fmt.allocPrint(a, "claim{d}", .{i}), .subject = "s", .predicate = "p", .value = "v", .statement = "Private source statement", .evidence = &.{.{ .id = "e1" }} };
        const body = try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{});
        if (reindex) {
            _ = try svc.execute(a, .assertions, "", body, "batch");
            const next: m.types.Config = .{ .features = .{ .semantic_recall = true }, .models = .{ .embedder = fake_model } };
            const configured = try svc.execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = @as(u64, 4), .config = next }, .{}), "reindex");
            const pending = try std.json.parseFromSliceLeaky(m.types.Operation, a, configured, .{});
            svc.backend.embed = Model.embed;
            try std.testing.expectError(error.MemoryRevisionConflict, svc.execute(a, .advance, pending.id, "{}", "advance"));
            const status = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try svc.execute(a, .operation, pending.id, "{}", ""), .{});
            try std.testing.expectEqual(@as(u8, 0), status.attempts);
            const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
            try std.testing.expect(!state.config.features.semantic_recall);
        } else {
            svc.backend.embed = Model.embed;
            try std.testing.expectError(error.MemoryRevisionConflict, svc.execute(a, .assertions, "", body, "batch"));
        }
        try std.testing.expectEqual(@as(usize, 1), store.decisions);
        try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "vector/", null, 256)).rows.len);
    }
}

test "memory embedding and reconciliation boundaries stop on live authority revocation" {
    const Model = struct {
        fn finish(ptr: *anyopaque) !void {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (store.failures > 0) return error.Forbidden;
        }
        fn embed(ptr: *anyopaque, a: A, model: m.types.Model, text: []const u8) ![]const f32 {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            store.failures = 1;
            return fakeEmbed(ptr, a, model, text);
        }
        fn decide(ptr: *anyopaque, a: A, config: m.types.Config, claim: m.types.Assertion, prior: []const m.types.StoredAssertion) !m.types.ReconciliationDecision {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.decisions += 1;
            store.failures = 1;
            return reviewDecide(ptr, a, config, claim, prior);
        }
    };
    for ([_]bool{ false, true }) |deciding| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        var svc = modelService(&store, 1);
        svc.backend.decide = reviewDecide;
        const config: m.types.Config = .{ .features = .{ .semantic_recall = !deciding, .reconciliation = if (deciding) .model else .explicit }, .models = .{ .embedder = if (deciding) null else fake_model, .decider = if (deciding) fake_model else null }, .predicates = &.{.{ .name = "deploys_with", .cardinality = .one }} };
        _ = try svc.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        _ = try svc.execute(a, .observe, "", source, "source");
        _ = try svc.execute(a, .assertions, "", helm, "prior");
        const revision = store.revision;
        svc.context.final_check_ptr = &store;
        svc.context.final_check_fn = Model.finish;
        svc.backend.embed = Model.embed;
        svc.backend.decide = Model.decide;
        var claims: [2]m.types.Assertion = undefined;
        for (&claims, 0..) |*claim, i| claim.* = .{ .id = try std.fmt.allocPrint(a, "claim{d}", .{i}), .subject = "production", .predicate = "deploys_with", .value = "Terraform", .statement = "Production uses Terraform", .valid_from = 300, .evidence = &.{.{ .id = "e1" }} };
        try std.testing.expectError(error.Forbidden, svc.execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "batch"));
        try std.testing.expectEqual(@as(usize, 1), store.decisions);
        try std.testing.expectEqual(revision, store.revision);
    }
}

test "memory explanation truncation allocates linearly within a fixed memory ceiling" {
    const Counter = struct {
        live: usize = 0,
        peak: usize = 0,
        fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (len > 64 * 1024 * 1024 -| self.live) return null;
            const memory = std.testing.allocator.rawAlloc(len, alignment, ret_addr) orelse return null;
            self.live += len;
            self.peak = @max(self.peak, self.live);
            return memory;
        }
        fn free(ptr: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.live -= buf.len;
            std.testing.allocator.rawFree(buf, alignment, ret_addr);
        }
        fn allocator(self: *@This()) A {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try setup(&store);
    try discard(&store, .configure, "", "{\"expected_revision\":3,\"config\":{\"max_input_bytes\":4194304}}", "budget", 3);
    const text = try a.alloc(u8, 16384);
    @memset(text, 'x');
    var claims: [128]m.types.Assertion = undefined;
    var ids: [128][]const u8 = undefined;
    for (&claims, &ids, 0..) |*claim, *id, i| {
        id.* = try std.fmt.allocPrint(a, "leaf{d}", .{i});
        claim.* = .{ .id = id.*, .subject = "s", .predicate = "p", .value = id.*, .statement = text, .evidence = &.{.{ .id = "e1" }} };
    }
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "leaves");
    const root: m.types.Assertion = .{ .id = "root", .subject = "s", .predicate = "p", .value = "root", .statement = "Root assertion", .evidence = &.{.{ .id = "e1" }}, .depends_on = &ids };
    _ = try store.service("alice", true, 5).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{root} }, .{}), "root");
    var full_counter: Counter = .{};
    const full = try store.service("alice", true, 6).execute(full_counter.allocator(), .explain, "root", "{\"max_nodes\":256,\"max_depth\":1,\"max_bytes\":4194304}", "");
    defer full_counter.allocator().free(full);
    var bounded_counter: Counter = .{};
    const bounded = try store.service("alice", true, 6).execute(bounded_counter.allocator(), .explain, "root", "{\"max_nodes\":256,\"max_depth\":1,\"max_bytes\":131072}", "");
    defer bounded_counter.allocator().free(bounded);
    const graph = try std.json.parseFromSliceLeaky(m.types.provenance.Explanation, a, bounded, .{});
    try std.testing.expect(bounded.len <= 131072);
    try std.testing.expect(graph.truncated and !graph.complete);
    try std.testing.expect(graph.nodes.len > 1);
    try std.testing.expect(full.len > bounded.len);
    try std.testing.expect(bounded_counter.peak <= full_counter.peak + 1024 * 1024);
}

test "memory source polling retires and invalidates sources during continuous ingestion" {
    const scheduler = @import("scheduling.zig");
    const Worker = struct {
        store: *Store,
        now: i64,
        calls: usize = 0,
        fn resource(ptr: *anyopaque, a: A, ref: m.types.provenance.Resource) !m.types.provenance.Snapshot {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, ref.id, "old")) {
                if (store.failures == 1) return .{ .state = .missing };
                return m.types.provenance.snapshot(a, ref, 7, 8, store.resource_json, store.resource_digest);
            }
            return Store.resource(ptr, a, ref);
        }
        fn execute(ptr: *anyopaque, pending: ?m.types.Operation) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(pending == null);
            self.calls += 1;
            var svc = self.store.service("alice", true, self.now);
            svc.backend.resource = resource;
            const a = self.store.arena.allocator();
            _ = try svc.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "worker-{d}", .{self.calls}));
        }
    };
    for ([_]bool{ false, true }) |missing| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{}", "create", 1);
        try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"old\",\"resource_selector\":{\"table\":\"business\",\"id\":\"old\"}}]}", "old", 2);
        const claim: m.types.Assertion = .{ .id = "summary", .subject = "deployment", .predicate = "guidance", .value = "Helm", .statement = "Deploy with Helm", .kind = .summary, .evidence = &.{.{ .id = "old" }} };
        _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), "summary");
        store.failures = if (missing) 1 else 2;
        var cursor: scheduler.Cursor = .{};
        var worker: Worker = .{ .store = &store, .now = 0 };
        // New resource-backed evidence arrives faster than the 30-second poll
        // interval. Scheduling must still process the original lost source.
        for (0..8) |i| {
            const now: i64 = @intCast((i + 1) * 10000);
            var svc = store.service("alice", true, now);
            svc.backend.resource = Worker.resource;
            const id = try std.fmt.allocPrint(a, "new-{d}", .{i});
            const evidence: m.types.Evidence = .{ .id = id, .resource_selector = .{ .table = "business", .id = id } };
            _ = try svc.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), id);
            const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
            if (i < 4) try std.testing.expectEqual(@as(i64, 30002), state.next_resource_check_at.?);
            worker.now = now + 1;
            try scheduler.advance(svc.backend, a, worker.now, state, &cursor, .{ .ptr = &worker, .execute = Worker.execute });
        }
        try std.testing.expect(worker.calls > 0);
        const evidence_key = try fixtureKey(a, "evidence", "old");
        const assertion_key = try fixtureKey(a, "assertion", "summary");
        if (missing) {
            try std.testing.expect(!store.values.contains(evidence_key));
            try std.testing.expect(!store.values.contains(assertion_key));
            try std.testing.expect(store.values.contains(try fixtureKey(a, "forgotten-evidence", "old")));
        } else {
            try std.testing.expect(store.values.contains(evidence_key));
            const summary = try std.json.parseFromSliceLeaky(m.types.StoredAssertion, a, store.values.get(assertion_key).?, .{});
            try std.testing.expect(summary.stale);
        }
    }
}

test "memory snapshot lookup paginates independently of graph bounds and filters every capture" {
    for (0..3) |mode| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try discard(&store, .create, "", "{}", "create", 1);
        var sources: [65]m.types.Evidence = undefined;
        for (&sources, 0..) |*item, i| {
            const id = try std.fmt.allocPrint(a, "snapshot-{d}", .{i});
            item.* = .{ .id = id, .source_id = id, .resource_selector = .{ .table = "business", .id = "approval" } };
        }
        try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = sources }, .{}), "captures", 2);
        const bindings = try Store.scan(&store, a, "resource-evidence/", null, 100);
        var rejected: [64][]const u8 = undefined;
        for (bindings.rows[0..64], &rejected) |row, *id| {
            id.* = (try std.json.parseFromSliceLeaky(struct { evidence_id: []const u8 }, a, row.json, .{})).evidence_id;
        }
        if (mode == 0) {
            try discard(&store, .forget, "", try std.json.Stringify.valueAlloc(a, .{ .source_ids = rejected }, .{}), "forget", 3);
        } else {
            // Model persisted captures belonging to another principal, or
            // captures admitted after the requested knowledge horizon.
            const rows = try Store.scan(&store, a, "evidence/", null, 100);
            for (rows.rows) |row| {
                var stored = try std.json.parseFromSliceLeaky(m.types.StoredEvidence, a, row.json, .{});
                for (rejected) |id| if (std.mem.eql(u8, id, stored.evidence.id)) {
                    if (mode == 1) stored.owner = "bob" else stored.recorded_at = 20;
                    try store.values.put(a, row.key, try std.json.Stringify.valueAlloc(a, stored, .{}));
                    break;
                };
            }
        }
        store.resource_revision = 8;
        const ref: m.types.provenance.Resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "approval", .revision = 7 };
        for ([_]u16{ 1, 64 }) |max_nodes| {
            const request: m.types.provenance.ExplainRequest = .{ .resource = ref, .max_nodes = max_nodes, .known_at = if (mode == 2) 10 else null };
            const raw = try store.service("alice", false, 30).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, request, .{}), "");
            const explanation = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
            try std.testing.expect(!explanation.object.get("truncated").?.bool);
            const root = explanation.object.get("nodes").?.array.items[0];
            try std.testing.expectEqualStrings(store.resource_json, root.object.get("record").?.object.get("snapshot_json").?.string);
        }
        if (mode == 0) {
            // The full cleanup sweep also repairs an orphan from an older writer.
            const prefix = bindings.rows[0].key[0 .. std.mem.lastIndexOfScalar(u8, bindings.rows[0].key, '/').? + 1];
            try store.values.put(a, try std.fmt.allocPrint(a, "{s}orphan", .{prefix}), "{\"evidence_id\":\"absent\"}");
            for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), @intCast(31 + i));
            try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "resource-evidence/", null, 100)).rows.len);
            try std.testing.expectEqual(@as(usize, 1), (try Store.scan(&store, a, "resource-watch/", null, 100)).rows.len);
            const last = try std.json.parseFromSliceLeaky(struct { evidence_id: []const u8 }, a, bindings.rows[64].json, .{});
            try discard(&store, .forget, "", try std.json.Stringify.valueAlloc(a, .{ .evidence_ids = &.{last.evidence_id} }, .{}), "forget-last", 60);
            try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "resource-evidence/", null, 100)).rows.len);
            try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, "resource-watch/", null, 100)).rows.len);
        }
    }
}

test "memory snapshot lookup exhaustion reports an incomplete root instead of absence" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const source_evidence: m.types.Evidence = .{ .id = "capture", .resource_selector = .{ .table = "business", .id = "approval" } };
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{source_evidence} }, .{}), "capture", 2);
    const bindings = try Store.scan(&store, a, "resource-evidence/", null, 1);
    const prefix = bindings.rows[0].key[0 .. std.mem.lastIndexOfScalar(u8, bindings.rows[0].key, '/').? + 1];
    // Opaque binding keys sort before the genuine retained capture. These
    // represent stale index entries left by an earlier writer or restore.
    for (0..4096) |i| try store.values.put(a, try std.fmt.allocPrint(a, "{s}!{d:0>5}", .{ prefix, i }), "{\"evidence_id\":\"absent\"}");
    store.resource_revision = 8;
    for ([_]?i64{ null, 10 }) |known| {
        const request: m.types.provenance.ExplainRequest = .{ .resource = .{ .table = "business", .table_id = 7, .kind = .record, .id = "approval", .revision = 7 }, .max_nodes = 1, .known_at = known };
        const scans_before = store.scans;
        const raw = try store.service("alice", false, 30).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, request, .{}), "");
        try std.testing.expectEqual(@as(usize, 64), store.scans - scans_before);
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
        try std.testing.expect(result.object.get("truncated").?.bool);
        try std.testing.expect(result.object.get("incomplete_history").?.bool);
        try std.testing.expect(!result.object.get("complete").?.bool);
        try std.testing.expect(!result.object.get("missing_evidence").?.bool);
        const record = result.object.get("nodes").?.array.items[0].object.get("record").?;
        try std.testing.expect(record.object.get("snapshot_json").? == .null);
        try std.testing.expect(record.object.get("payload_truncated").?.bool);
    }
}

fn externalReviewSetup(store: *Store) !void {
    try discard(store, .create, "", "{}", "create", 1);
    try discard(store, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"fixture\",\"source_version\":\"1\",\"text\":\"Antfly is the Antfly database\"}],\"entities\":[{\"id\":\"a\",\"name\":\"Antfly\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"b\",\"name\":\"Antfly database\",\"evidence\":[{\"id\":\"e\"}]}]}", "source", 2);
}
fn externalProposal(store: *Store) !m.types.OperationStatus {
    const a = store.arena.allocator();
    return std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try store.service("alice", true, 3).execute(a, .propose_entity, "", "{\"action\":\"merge\",\"from\":\"a\",\"to\":\"b\",\"actor\":\"codex/consolidation\",\"expected_revision\":3}", "proposal"), .{});
}
test "memory external entity verdicts defer accept and reject through native curation with authenticated provenance" {
    for ([_]bool{ true, false }) |accept| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try externalReviewSetup(&store);
        const candidates = try store.service("alice", true, 3).execute(a, .entity_candidates, "a", "{}", "");
        const listed = try std.json.parseFromSliceLeaky(std.json.Value, a, candidates, .{});
        try std.testing.expectEqual(@as(usize, 1), listed.object.get("items").?.array.items.len);
        const proposal = try externalProposal(&store);
        const id = proposal.proposal_id.?;
        try std.testing.expectEqualStrings("a", (try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 4).execute(a, .resolve_entity, "a", "{}", ""), .{})).object.get("canonical_id").?.string);
        try std.testing.expectError(error.Forbidden, store.service("reader", false, 4).execute(a, .entity_proposal, id, "{}", ""));
        _ = try store.service("alice", true, 5).execute(a, .review_entity, id, "{\"expected_revision\":4,\"actor\":\"codex/consolidation\",\"reason\":\"Unsure; preserve for review\"}", "defer");
        const request: m.types.EntityVerdict = .{ .expected_revision = 5, .accept = accept, .pin_distinct = !accept, .actor = "codex/user", .reason = "External labelled verdict", .model = .{ .provider = "external", .model = "hosted-judge", .revision = "fixture-v1" } };
        const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
        const first = try store.service("alice", true, 6).execute(a, .review_entity, id, encoded, "verdict");
        try std.testing.expectEqualStrings(first, try store.service("alice", true, 7).execute(a, .review_entity, id, encoded, "verdict"));
        const inspected = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 7).execute(a, .entity_proposal, id, "{}", ""), .{});
        const reviewed = inspected.object.get("proposal").?;
        try std.testing.expectEqualStrings(if (accept) "accepted" else "rejected", reviewed.object.get("state").?.string);
        try std.testing.expectEqualStrings("alice", reviewed.object.get("decided").?.object.get("actor").?.string);
        try std.testing.expectEqualStrings("codex/user", inspected.object.get("inputs").?.object.get("verdict_actor").?.string);
        try std.testing.expectEqualStrings("External labelled verdict", inspected.object.get("reason").?.string);
        const resolved = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 7).execute(a, .resolve_entity, "a", "{}", ""), .{});
        try std.testing.expectEqualStrings(if (accept) "b" else "a", resolved.object.get("canonical_id").?.string);
        try std.testing.expectError(error.MemoryEntityConflict, store.service("alice", true, 8).execute(a, .review_entity, id, "{\"expected_revision\":6,\"accept\":true}", "terminal-retry"));
        const after = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 8).execute(a, .entity_candidates, "a", "{}", ""), .{});
        try std.testing.expectEqual(@as(usize, 0), after.object.get("items").?.array.items.len);
    }
}

test "memory external verdicts fence configuration changes and scrub forgotten review evidence" {
    for ([_]bool{ true, false }) |configure| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try externalReviewSetup(&store);
        const proposal = try externalProposal(&store);
        const id = proposal.proposal_id.?;
        if (configure) {
            try discard(&store, .configure, "", "{\"expected_revision\":4,\"config\":{\"policy_version\":\"new-policy\"}}", "configure", 5);
            try std.testing.expectError(error.MemoryConfigurationChanged, store.service("alice", true, 6).execute(a, .review_entity, id, "{\"expected_revision\":5,\"accept\":true}", "verdict"));
        } else {
            try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"review-e\",\"source_id\":\"review\",\"source_version\":\"1\",\"text\":\"Review proof\"}]}", "proof", 5);
            _ = try store.service("alice", true, 6).execute(a, .review_entity, id, "{\"expected_revision\":5,\"accept\":true,\"reason\":\"PRIVATE_REVIEW_REASON\",\"evidence\":[{\"id\":\"review-e\"}]}", "verdict");
            try discard(&store, .forget, "", "{\"evidence_ids\":[\"review-e\"]}", "forget-review", 7);
            const raw = try store.service("alice", true, 8).execute(a, .entity_proposal, id, "{}", "");
            try std.testing.expect(std.mem.indexOf(u8, raw, "PRIVATE_REVIEW_REASON") == null);
            for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-review-{d}", .{i}), @intCast(9 + i));
            var values = store.values.valueIterator();
            while (values.next()) |value| try std.testing.expect(std.mem.indexOf(u8, value.*, "PRIVATE_REVIEW_REASON") == null);
        }
    }
}

test "memory replays pinned Codex concept merge verdicts without hosted credentials" {
    for ([_][]const u8{ @embedFile("fixtures/codex-a2d59cfb01/concept_pairs.jsonl"), @embedFile("fixtures/codex-a2d59cfb01/concept_merge.jsonl") }) |fixture| {
        var lines = std.mem.tokenizeScalar(u8, fixture, '\n');
        while (lines.next()) |line| {
            var store = Store.init();
            defer store.deinit();
            const a = store.arena.allocator();
            const row = try std.json.parseFromSliceLeaky(struct { a: []const u8, b: []const u8, same: bool }, a, line, .{ .ignore_unknown_fields = true });
            try discard(&store, .create, "", "{}", "create", 1);
            const evidence = m.types.Evidence{ .id = "e", .source_id = "codex-fixture", .source_version = "a2d59cfb01", .text = line };
            const entities = [_]m.types.Entity{ .{ .id = "a", .name = row.a, .evidence = &.{.{ .id = "e" }} }, .{ .id = "b", .name = row.b, .evidence = &.{.{ .id = "e" }} } };
            try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = entities }, .{}), "fixture", 2);
            const proposal = try externalProposal(&store);
            _ = try store.service("alice", true, 4).execute(a, .review_entity, proposal.proposal_id.?, try std.json.Stringify.valueAlloc(a, m.types.EntityVerdict{ .expected_revision = 4, .accept = row.same, .pin_distinct = !row.same, .actor = "codex/labelled-fixture" }, .{}), "labelled-verdict");
            const result = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 5).execute(a, .resolve_entity, "a", "{}", ""), .{});
            try std.testing.expectEqualStrings(if (row.same) "b" else "a", result.object.get("canonical_id").?.string);
        }
    }
}

test "memory replays pinned Codex supersession and typed relationship verdicts" {
    var cases = std.mem.tokenizeScalar(u8, @embedFile("fixtures/codex-a2d59cfb01/supersession.jsonl"), '\n');
    while (cases.next()) |line| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const item = try std.json.parseFromSliceLeaky(struct { earlier: []const u8, later: []const u8, replaces: bool }, a, line, .{});
        try discard(&store, .create, "", "{}", "create", 1);
        const evidence: m.types.Evidence = .{ .id = "e", .source_id = "codex", .source_version = "a2d59cfb01", .text = line };
        try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "source", 2);
        const old: m.types.Assertion = .{ .id = "old", .subject = "codex", .predicate = "guidance", .value = item.earlier, .statement = item.earlier, .evidence = &.{.{ .id = "e" }} };
        var next = old;
        next.id = "new";
        next.value = item.later;
        next.statement = item.later;
        next.supersedes = if (item.replaces) &.{"old"} else &.{};
        try discard(&store, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{old} }, .{}), "old", 3);
        try discard(&store, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{next} }, .{}), "new", 4);
        try std.testing.expectEqual(@as(usize, if (item.replaces) 1 else 2), try recallTestCount(&store, 5));
    }
    var relations = std.mem.tokenizeScalar(u8, @embedFile("fixtures/codex-a2d59cfb01/concept_relations.jsonl"), '\n');
    while (relations.next()) |line| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        const item = try std.json.parseFromSliceLeaky(struct { a: []const u8, b: []const u8, relation: []const u8 }, a, line, .{ .ignore_unknown_fields = true });
        const predicates = [_]m.types.Predicate{ .{ .name = "is_a", .subject_type = "concept", .object_type = "concept" }, .{ .name = "part_of", .subject_type = "concept", .object_type = "concept" }, .{ .name = "alternative_to", .subject_type = "concept", .object_type = "concept" } };
        try discard(&store, .create, "", try std.json.Stringify.valueAlloc(a, m.types.Config{ .predicates = &predicates }, .{}), "create", 1);
        const evidence: m.types.Evidence = .{ .id = "e", .source_id = "codex", .source_version = "a2d59cfb01", .text = line };
        const entities = [_]m.types.Entity{ .{ .id = "a", .name = item.a, .type = "concept", .evidence = &.{.{ .id = "e" }} }, .{ .id = "b", .name = item.b, .type = "concept", .evidence = &.{.{ .id = "e" }} } };
        try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = entities }, .{}), "source", 2);
        const positive = !std.mem.eql(u8, item.relation, "none");
        if (positive) {
            const claim: m.types.Assertion = .{ .id = "relation", .subject = "a", .predicate = item.relation, .value = "b", .statement = line, .producer = "codex/external-relation-judge", .role = .inferred, .evidence = &.{.{ .id = "e" }} };
            try discard(&store, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = &.{claim} }, .{}), "verdict", 3);
        }
        try std.testing.expectEqual(@as(usize, if (positive) 1 else 0), try recallTestCount(&store, 4));
    }
}

test "memory native software ontology derives descriptions and retrieves linked concepts and ordered episodes" {
    var store = Store.init();
    defer store.deinit();
    try discard(&store, .create, "", "{\"ontology_pack\":\"software_team_v1\"}", "create", 1);
    try discard(&store, .observe, "",
        \\{"evidence":[{"id":"later","source_id":"history","source_version":"2","text":"The storage engine uses a WAL","thread_id":"thread-1","turn_id":"turn-2","ordinal":2},{"id":"earlier","source_id":"history","source_version":"1","text":"Antfly is our database","thread_id":"thread-1","turn_id":"turn-1","ordinal":-1}],"entities":[{"id":"antfly","name":"Antfly","evidence":[{"id":"earlier"}]},{"id":"storage","name":"storage engine","evidence":[{"id":"later"}]}],"assertions":[{"id":"relation","subject":"storage","predicate":"part_of","value":"antfly","statement":"The storage engine is part of Antfly","evidence":[{"id":"later"}]},{"id":"wal","subject":"storage","predicate":"uses","value":"WAL","statement":"Storage uses a write-ahead log","evidence":[{"id":"later"}]}],"mentions":[{"id":"again","entity_id":"antfly","evidence":[{"id":"later"}]}]}
    , "observe", 2);
    const episode_json = try store.run(.episode, "thread-1", "{\"limit\":1}", "", 3);
    defer std.testing.allocator.free(episode_json);
    const episode = try std.json.parseFromSlice(m.types.Episode, std.testing.allocator, episode_json, .{});
    defer episode.deinit();
    try std.testing.expectEqualStrings("earlier", episode.value.turns[0].evidence.evidence.id);
    try std.testing.expect(!episode.value.complete);
    const next_body = try std.json.Stringify.valueAlloc(std.testing.allocator, m.types.EpisodeRequest{ .cursor = episode.value.next_cursor, .limit = 1 }, .{});
    defer std.testing.allocator.free(next_body);
    const next = try store.run(.episode, "thread-1", next_body, "", 3);
    defer std.testing.allocator.free(next);
    try std.testing.expect(std.mem.indexOf(u8, next, "later") != null);
    const view_json = try store.run(.entity_view, "storage", "{}", "", 3);
    defer std.testing.allocator.free(view_json);
    const view = try std.json.parseFromSlice(m.types.EntityView, std.testing.allocator, view_json, .{});
    defer view.deinit();
    try std.testing.expectEqual(@as(usize, 3), view.value.dependencies.len);
    try std.testing.expect(std.mem.indexOf(u8, view.value.description, "write-ahead") != null);
    const recall_json = try store.run(.recall, "", "{\"graph\":{\"entity_seeds\":[\"antfly\"],\"max_hops\":3},\"token_budget\":8192}", "", 3);
    defer std.testing.allocator.free(recall_json);
    const recalled = try std.json.parseFromSlice(m.types.RecallResult, std.testing.allocator, recall_json, .{});
    defer recalled.deinit();
    try std.testing.expectEqual(.graph, recalled.value.retrieval);
    try std.testing.expectEqual(@as(usize, 2), recalled.value.items.len);
    try std.testing.expectEqual(@as(usize, 2), recalled.value.entities.len);
    try std.testing.expectEqual(@as(usize, 1), recalled.value.episodes.len);
    try std.testing.expectEqual(@as(usize, 0), recalled.value.scanned); // Indexed adjacency, no global assertion scan.
    try std.testing.expectEqual(recall_json.len, recalled.value.token_upper_bound);
    try std.testing.expect(recall_json.len <= 8192);
    try discard(&store, .correct, "", "{\"id\":\"wal\",\"expected_revision\":3,\"lifecycle\":\"retracted\",\"reason\":\"Correction\"}", "retract", 4);
    const revised = try store.run(.entity_view, "storage", "{}", "", 5);
    defer std.testing.allocator.free(revised);
    try std.testing.expect(std.mem.indexOf(u8, revised, "write-ahead") == null);
    const revised_view = try std.json.parseFromSlice(m.types.EntityView, std.testing.allocator, revised, .{});
    defer revised_view.deinit();
    try std.testing.expect(!std.mem.eql(u8, view.value.fingerprint, revised_view.value.fingerprint));
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"later\"]}", "forget", 6);
    try std.testing.expectError(error.MemoryNotFound, store.run(.entity_view, "storage", "{}", "", 7));
    const after = try store.run(.episode, "thread-1", "{}", "", 7);
    defer std.testing.allocator.free(after);
    try std.testing.expect(std.mem.indexOf(u8, after, "WAL") == null);
    const bob = try store.service("bob", false, 7).execute(std.testing.allocator, .recall, "", "{\"graph\":{\"entity_seeds\":[\"antfly\"]},\"token_budget\":2048}", "");
    defer std.testing.allocator.free(bob);
    try std.testing.expect(std.mem.indexOf(u8, bob, "Antfly") == null);
    try std.testing.expectError(error.InvalidMemoryRequest, store.run(.recall, "", "{\"graph\":{\"max_nodes\":65535}}", "", 7));
}

test "memory native graph budgets include views episodes and metadata and ontology rejects untyped objects" {
    var store = Store.init();
    defer store.deinit();
    try externalReviewSetup(&store);
    const result = try store.run(.recall, "", "{\"graph\":{\"entity_seeds\":[\"a\"]},\"token_budget\":600}", "", 5);
    defer std.testing.allocator.free(result);
    try std.testing.expect(result.len <= 600);
    const known = try store.run(.episodes, "", "{\"known_at\":0}", "", 5);
    defer std.testing.allocator.free(known);
    try std.testing.expect(std.mem.indexOf(u8, known, "thread_id") == null);
    try discard(&store, .configure, "", "{\"expected_revision\":3,\"config\":{\"ontology_pack\":\"software_team_v1\"}}", "pack", 6);
    try std.testing.expectError(error.MemoryEntityRequired, store.run(.assertions, "", "{\"assertions\":[{\"id\":\"invalid-relation\",\"subject\":\"a\",\"predicate\":\"is_a\",\"value\":\"missing\",\"statement\":\"Invalid\",\"evidence\":[{\"id\":\"e\"}]}]}", "bad", 7));
}

test "memory native relationship proposals defer and apply external verdicts under policy and causal retention" {
    var store = Store.init();
    defer store.deinit();
    try externalReviewSetup(&store);
    try discard(&store, .configure, "", "{\"expected_revision\":3,\"config\":{\"ontology_pack\":\"software_team_v1\"}}", "pack", 3);
    try discard(&store, .propose_relationship, "", "{\"id\":\"relation-review\",\"expected_revision\":4,\"actor\":\"codex/relations\",\"assertion\":{\"id\":\"link\",\"subject\":\"a\",\"predicate\":\"is_a\",\"value\":\"b\",\"statement\":\"Antfly is an Antfly database\",\"evidence\":[{\"id\":\"e\"}]}}", "propose", 4);
    try std.testing.expectError(error.MemoryNotFound, store.run(.get, "link", "{}", "", 5));
    try discard(&store, .review_relationship, "relation-review", "{\"expected_revision\":5,\"accept\":null,\"actor\":\"codex/judge\",\"reason\":\"Unsure\"}", "defer", 5);
    const deferred = try store.run(.relationship_proposal, "relation-review", "{}", "", 6);
    defer std.testing.allocator.free(deferred);
    try std.testing.expect(std.mem.indexOf(u8, deferred, "\"state\":\"open\"") != null);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"review\",\"source_id\":\"judge\",\"source_version\":\"1\",\"text\":\"External judge inputs\"}]}", "review-evidence", 6);
    try discard(&store, .review_relationship, "relation-review", "{\"expected_revision\":7,\"accept\":true,\"actor\":\"codex/judge\",\"model\":{\"provider\":\"external\",\"model\":\"fixture\",\"revision\":\"pinned\"},\"evidence\":[{\"id\":\"review\"}]}", "accept", 7);
    const assertion = try store.run(.get, "link", "{}", "", 8);
    defer std.testing.allocator.free(assertion);
    try std.testing.expect(std.mem.indexOf(u8, assertion, "is_a") != null);
    try std.testing.expectError(error.MemoryEntityConflict, store.run(.review_relationship, "relation-review", "{\"expected_revision\":8,\"accept\":false}", "repeat", 8));
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"review\"]}", "forget-review", 9);
    try std.testing.expectError(error.MemoryNotFound, store.run(.get, "link", "{}", "", 10));
}

test "memory candidate discovery preserves canonical distinctions and version boundaries" {
    var store = Store.init();
    defer store.deinit();
    try externalReviewSetup(&store);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e2\",\"source_id\":\"fixture\",\"source_version\":\"2\",\"text\":\"Database concepts and distinct versions\"}],\"entities\":[{\"id\":\"alias\",\"name\":\"Antfly alias\",\"distinct_from\":[\"b\"],\"evidence\":[{\"id\":\"e2\"}]},{\"id\":\"v2\",\"name\":\"Antfly 2\",\"evidence\":[{\"id\":\"e2\"}]}]}", "more", 3);
    try discard(&store, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"alias\",\"to\":\"a\",\"expected_revision\":4}", "alias", 4);
    const candidates = try store.run(.entity_candidates, "a", "{}", "", 5);
    defer std.testing.allocator.free(candidates);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, candidates, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.object.get("items").?.array.items.len);
    try discard(&store, .configure, "", "{\"expected_revision\":5,\"config\":{\"ontology_pack\":\"software_team_v1\"}}", "pack", 5);
    const relationships = try store.run(.entity_candidates, "a", "{\"kind\":\"relationship\"}", "", 6);
    defer std.testing.allocator.free(relationships);
    const related = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, relationships, .{});
    defer related.deinit();
    try std.testing.expectEqual(@as(usize, 2), related.value.object.get("items").?.array.items.len);
}

test "memory relationship candidates respect heterogeneous ontology domains" {
    var store = Store.init();
    defer store.deinit();
    try discard(&store, .create, "", "{\"predicates\":[{\"name\":\"operates\",\"subject_type\":\"person\",\"object_type\":\"service\"}]}", "create", 1);
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"source\",\"source_version\":\"1\",\"text\":\"The Antfly operator maintains the Antfly service\"}],\"entities\":[{\"id\":\"operator\",\"name\":\"Antfly operator\",\"type\":\"person\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"service\",\"name\":\"Antfly service\",\"type\":\"service\",\"evidence\":[{\"id\":\"e\"}]}]}", "observe", 2);
    const result = try store.run(.entity_candidates, "operator", "{\"kind\":\"relationship\"}", "", 3);
    defer std.testing.allocator.free(result);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, result, .{});
    defer parsed.deinit();
    const items = parsed.value.object.get("items").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqualStrings("operates", items[0].object.get("predicates").?.array.items[0].string);
    try std.testing.expectError(error.InvalidMemoryConfig, (m.types.Config{ .ontology_pack = .software_team_v1, .predicates = &.{.{ .name = "is_a", .object_type = "service" }} }).validate());
}

test "memory graph recall does not spend assertion work on disabled episode expansion" {
    var store = Store.init();
    defer store.deinit();
    try externalReviewSetup(&store);
    const a = store.arena.allocator();
    const mentions = try a.alloc(m.types.EntityMention, 70);
    for (mentions, 0..) |*mention, i| mention.* = .{ .id = try std.fmt.allocPrint(a, "mention-{d}", .{i}), .entity_id = "a", .evidence = &.{.{ .id = "e" }} };
    const observation = try std.json.Stringify.valueAlloc(a, m.types.ObservationRequest{ .evidence = &.{.{ .id = "turn", .source_id = "history", .source_version = "1", .text = "A later observation" }}, .mentions = mentions }, .{});
    try discard(&store, .observe, "", observation, "mentions", 3);
    const result = try store.run(.recall, "", "{\"graph\":{\"entity_seeds\":[\"a\"],\"include_episodes\":false},\"token_budget\":8192}", "", 4);
    defer std.testing.allocator.free(result);
    const parsed = try std.json.parseFromSlice(m.types.RecallResult, std.testing.allocator, result, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.graph.?.complete);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.episodes.len);
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"e\"]}", "forget", 5);
    for (0..128) |i| {
        if (!store.values.contains("cleanup")) break;
        try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}), @intCast(6 + i));
    }
    try std.testing.expect(!store.values.contains("cleanup"));
    var rows = store.values.keyIterator();
    while (rows.next()) |row| {
        try std.testing.expect(!std.mem.startsWith(u8, row.*, "mention/"));
        try std.testing.expect(!std.mem.startsWith(u8, row.*, "dependency/mention-"));
    }
}

test "memory graph descriptions share recall filters rather than widening accepted claims" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try externalReviewSetup(&store);
    const claims = [_]m.types.Assertion{
        .{ .id = "allowed", .subject = "a", .predicate = "allowed", .value = "yes", .statement = "Accepted settled fact", .scope = "wanted", .kind = .decision, .evidence = &.{.{ .id = "e" }} },
        .{ .id = "provisional", .subject = "a", .predicate = "allowed", .value = "maybe", .statement = "EXCLUDED_PROVISIONAL", .scope = "wanted", .kind = .decision, .stability = .provisional, .evidence = &.{.{ .id = "e" }} },
        .{ .id = "predicate", .subject = "a", .predicate = "other", .value = "no", .statement = "EXCLUDED_PREDICATE", .scope = "wanted", .kind = .decision, .evidence = &.{.{ .id = "e" }} },
        .{ .id = "scope", .subject = "a", .predicate = "allowed", .value = "no", .statement = "EXCLUDED_SCOPE", .scope = "elsewhere", .kind = .decision, .evidence = &.{.{ .id = "e" }} },
        .{ .id = "kind", .subject = "a", .predicate = "allowed", .value = "no", .statement = "EXCLUDED_KIND", .scope = "wanted", .evidence = &.{.{ .id = "e" }} },
    };
    _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims");
    const result = try store.service("alice", false, 4).execute(a, .recall, "", "{\"predicate\":\"allowed\",\"scope\":\"wanted\",\"kind\":\"decision\",\"include_provisional\":false,\"graph\":{\"entity_seeds\":[\"a\"],\"include_episodes\":false},\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, result, "Accepted settled fact") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "EXCLUDED_") == null);
    const parsed = try std.json.parseFromSliceLeaky(m.types.RecallResult, a, result, .{});
    try std.testing.expectEqual(@as(usize, 1), parsed.entities.len);
    try std.testing.expectEqual(@as(usize, 2), parsed.entities[0].dependencies.len);
    // The standalone derived view still exposes all ordinarily eligible facts.
    const full = try store.service("alice", false, 4).execute(a, .entity_view, "a", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, full, "EXCLUDED_PROVISIONAL") != null);
}

test "memory external entity verdicts revalidate original proposal context without deleting changed history" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try externalReviewSetup(&store);
    const evidence: m.types.Evidence = .{ .id = "review-context", .resource_selector = .{ .table = "business", .id = "approval" } };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence} }, .{}), "context");
    const proposal: m.types.EntityProposalRequest = .{ .action = .merge, .from = "a", .to = "b", .expected_revision = 4, .evidence = &.{.{ .id = "review-context" }} };
    const raw = try store.service("alice", true, 4).execute(a, .propose_entity, "", try std.json.Stringify.valueAlloc(a, proposal, .{}), "proposal");
    const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, raw, .{});
    store.resource_state = .changed;
    for ([_]?bool{ true, false, null }) |accept| {
        const verdict: m.types.EntityVerdict = .{ .expected_revision = 5, .accept = accept };
        try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 5).execute(a, .review_entity, receipt.proposal_id.?, try std.json.Stringify.valueAlloc(a, verdict, .{}), "verdict"));
    }
    const identity = try store.service("alice", true, 5).execute(a, .resolve_entity, "a", "{}", "");
    const entity = try std.json.parseFromSliceLeaky(std.json.Value, a, identity, .{});
    try std.testing.expectEqualStrings("a", entity.object.get("canonical_id").?.string);
    _ = try store.service("alice", true, 40000).execute(a, .advance, "", "{}", "cleanup");
    const history = try store.service("alice", true, 40001).execute(a, .entity_proposal, receipt.proposal_id.?, "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, history, "review-context") != null);
    // A current source allows the same still-open proposal to be judged.
    store.resource_state = .current;
    const verdict: m.types.EntityVerdict = .{ .expected_revision = 6, .accept = true };
    _ = try store.service("alice", true, 40002).execute(a, .review_entity, receipt.proposal_id.?, try std.json.Stringify.valueAlloc(a, verdict, .{}), "verdict");
}

test "memory foreign private episodes have the same point and catalog absence as missing episodes" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try store.service("alice", true, 1).execute(a, .create, "", "{}", "create");
    var sources: [130]m.types.Evidence = undefined;
    for (&sources, 0..) |*value, i| value.* = .{ .id = try std.fmt.allocPrint(a, "private-{d}", .{i}), .source_id = "history", .source_version = "1", .text = "PRIVATE_EPISODE", .thread_id = "private-thread", .ordinal = @intCast(i) };
    _ = try store.service("alice", false, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = sources }, .{}), "observe");
    const before = store.scans;
    for ([_][]const u8{ "private-thread", "missing-thread" }) |id| try std.testing.expectError(error.MemoryNotFound, store.service("bob", false, 3).execute(a, .episode, id, "{\"limit\":1}", ""));
    try std.testing.expect(store.scans - before <= 4); // No scan of Alice's 130 private turns.
    const raw = try store.service("bob", false, 3).execute(a, .episodes, "", "{}", "");
    const result = try std.json.parseFromSliceLeaky(m.types.EpisodePage, a, raw, .{});
    try std.testing.expectEqual(@as(usize, 0), result.items.len);
    try std.testing.expect(result.complete);
    try std.testing.expect(result.next_cursor == null);
}

test "memory audience episode indexes merge ordered turns deduplicate catalogs and retire exact membership" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try store.service("alice", true, 1).execute(a, .create, "", "{}", "create");
    const alice_sources = [_]m.types.Evidence{
        .{ .id = "private", .source_id = "alice-history", .source_version = "1", .text = "ALICE_ONLY", .thread_id = "mixed-thread", .ordinal = -1 },
        .{ .id = "team", .source_id = "alice-history", .source_version = "1", .text = "TEAM_TURN", .thread_id = "mixed-thread", .ordinal = 1, .visibility = .team },
    };
    const body = try std.json.Stringify.valueAlloc(a, .{ .evidence = alice_sources }, .{});
    _ = try store.service("alice", false, 2).execute(a, .observe, "", body, "alice");
    const bob_source: m.types.Evidence = .{ .id = "bob", .source_id = "bob-history", .source_version = "1", .text = "BOB_ONLY", .thread_id = "mixed-thread", .ordinal = 2 };
    _ = try store.service("bob", false, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{bob_source} }, .{}), "bob");
    _ = try store.service("alice", false, 4).execute(a, .observe, "", body, "repeat");
    const list = try store.service("bob", false, 5).execute(a, .episodes, "", "{}", "");
    const catalog = try std.json.parseFromSliceLeaky(m.types.EpisodePage, a, list, .{});
    try std.testing.expectEqual(@as(usize, 1), catalog.items.len);
    try std.testing.expectEqualStrings("team", catalog.items[0].turns[0].evidence.evidence.id);
    try std.testing.expect(std.mem.indexOf(u8, list, "ALICE_ONLY") == null);
    const first = try store.service("bob", false, 5).execute(a, .episode, "mixed-thread", "{\"limit\":1}", "");
    const page = try std.json.parseFromSliceLeaky(m.types.Episode, a, first, .{});
    const resume_body = try std.json.Stringify.valueAlloc(a, m.types.EpisodeRequest{ .limit = 1, .cursor = page.next_cursor }, .{});
    const next = try store.service("bob", false, 5).execute(a, .episode, "mixed-thread", resume_body, "");
    const second = try std.json.parseFromSliceLeaky(m.types.Episode, a, next, .{});
    try std.testing.expectEqualStrings("bob", second.turns[0].evidence.evidence.id);
    try std.testing.expect(second.complete);
    try std.testing.expectError(error.InvalidMemoryCursor, store.service("eve", false, 5).execute(a, .episode, "mixed-thread", resume_body, ""));
    const admin = try store.service("alice", true, 5).execute(a, .episode, "mixed-thread", "{}", "");
    const all = try std.json.parseFromSliceLeaky(m.types.Episode, a, admin, .{});
    try std.testing.expectEqual(@as(usize, 3), all.turns.len);
    const counts = try Store.scan(&store, a, "episode/", null, 64);
    try std.testing.expectEqual(@as(u64, 3), (try std.json.parseFromSliceLeaky(struct { thread_id: []const u8, members: u64 }, a, counts.rows[0].json, .{})).members);
    _ = try store.service("alice", true, 6).execute(a, .forget, "", "{\"evidence_ids\":[\"private\",\"bob\"]}", "forget-private");
    try std.testing.expectError(error.MemoryCursorStale, store.service("bob", false, 7).execute(a, .episode, "mixed-thread", resume_body, ""));
    for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "retire-private-{d}", .{i}), @intCast(8 + i));
    const remaining = try Store.scan(&store, a, "episode/", null, 64);
    try std.testing.expectEqual(@as(u64, 1), (try std.json.parseFromSliceLeaky(struct { thread_id: []const u8, members: u64 }, a, remaining.rows[0].json, .{})).members);
    _ = try store.service("alice", true, 30).execute(a, .forget, "", "{\"evidence_ids\":[\"team\"]}", "forget-team");
    for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "retire-team-{d}", .{i}), @intCast(31 + i));
    for ([_][]const u8{ "episode/", "episode-turn/", "episode-access/" }) |prefix| try std.testing.expectEqual(@as(usize, 0), (try Store.scan(&store, a, prefix, null, 64)).rows.len);
}

test "memory episode catalog resumes bounded expired prefixes without losing accessible later members" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try store.service("alice", true, 1).execute(a, .create, "", "{}", "create");
    var sources: [130]m.types.Evidence = undefined;
    for (&sources, 0..) |*value, i| value.* = .{ .id = try std.fmt.allocPrint(a, "turn-{d}", .{i}), .source_id = "history", .source_version = "1", .text = if (i < 128) "EXPIRED_TEXT" else "SURVIVING_TEXT", .thread_id = "UNPROVEN_THREAD_NAME", .ordinal = @intCast(i), .expires_at = if (i < 128) 10 else null };
    _ = try store.service("alice", false, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = sources }, .{}), "observe");
    var cursor: ?[]const u8 = null;
    for (0..3) |i| {
        const before = store.scans;
        const raw = try store.service("alice", false, 20).execute(a, .episodes, "", try std.json.Stringify.valueAlloc(a, m.types.EpisodeRequest{ .limit = 1, .cursor = cursor }, .{}), "");
        try std.testing.expect(store.scans - before <= 8);
        try std.testing.expect(std.mem.indexOf(u8, raw, "EXPIRED_TEXT") == null);
        const page = try std.json.parseFromSliceLeaky(m.types.EpisodePage, a, raw, .{});
        if (i < 2) {
            try std.testing.expectEqual(@as(usize, 0), page.items.len);
            try std.testing.expect(!page.complete and page.next_cursor != null);
            try std.testing.expect(!std.mem.eql(u8, cursor orelse "", page.next_cursor.?));
            cursor = page.next_cursor;
            // Cursor encoding is not encryption. Even after decoding it,
            // an empty progress page must not disclose the unproven name.
            var pieces = std.mem.splitScalar(u8, cursor.?, ':');
            _ = pieces.next();
            _ = pieces.next();
            const encoded = pieces.next().?["episode-list/".len..];
            const decoder = std.base64.url_safe_no_pad.Decoder;
            const decoded = try a.alloc(u8, try decoder.calcSizeForSlice(encoded));
            try decoder.decode(decoded, encoded);
            try std.testing.expect(std.mem.indexOf(u8, decoded, "UNPROVEN_THREAD_NAME") == null);
            const foreign = try std.json.Stringify.valueAlloc(a, m.types.EpisodeRequest{ .cursor = cursor }, .{});
            try std.testing.expectError(error.InvalidMemoryCursor, store.service("bob", false, 20).execute(a, .episodes, "", foreign, ""));
        } else {
            try std.testing.expectEqual(@as(usize, 1), page.items.len);
            try std.testing.expectEqualStrings("turn-128", page.items[0].turns[0].evidence.evidence.id);
            try std.testing.expect(page.complete and page.next_cursor == null);
            const next = try store.service("alice", false, 20).execute(a, .episode, "UNPROVEN_THREAD_NAME", try std.json.Stringify.valueAlloc(a, m.types.EpisodeRequest{ .cursor = page.items[0].next_cursor }, .{}), "");
            const turns = try std.json.parseFromSliceLeaky(m.types.Episode, a, next, .{});
            try std.testing.expectEqualStrings("turn-129", turns.turns[0].evidence.evidence.id);
        }
    }
}

test "memory episode catalog pagination merges audience lanes without duplicate or missing threads" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try store.service("alice", true, 1).execute(a, .create, "", "{}", "create");
    var alice: [40]m.types.Evidence = undefined;
    var bob: [20]m.types.Evidence = undefined;
    for (&alice, 0..) |*value, i| value.* = .{ .id = try std.fmt.allocPrint(a, "alice-{d}", .{i}), .source_id = "alice", .source_version = "1", .text = if (i < 20) "TEAM" else "ALICE_PRIVATE", .thread_id = try std.fmt.allocPrint(a, "thread-{d}", .{i}), .ordinal = 0, .visibility = if (i < 20) .team else .private };
    for (&bob, 0..) |*value, i| value.* = .{ .id = try std.fmt.allocPrint(a, "bob-{d}", .{i}), .source_id = "bob", .source_version = "1", .text = "BOB_PRIVATE", .thread_id = try std.fmt.allocPrint(a, "thread-{d}", .{i + 10}), .ordinal = 1 };
    _ = try store.service("alice", false, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = alice }, .{}), "alice");
    _ = try store.service("bob", false, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = bob }, .{}), "bob");
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var cursor: ?[]const u8 = null;
    for (0..11) |i| {
        const raw = try store.service("bob", false, 4).execute(a, .episodes, "", try std.json.Stringify.valueAlloc(a, m.types.EpisodeRequest{ .limit = 3, .cursor = cursor }, .{}), "");
        try std.testing.expect(std.mem.indexOf(u8, raw, "ALICE_PRIVATE") == null);
        const page = try std.json.parseFromSliceLeaky(m.types.EpisodePage, a, raw, .{});
        for (page.items) |episode| {
            try std.testing.expect(!seen.contains(episode.thread_id));
            try seen.put(a, episode.thread_id, {});
        }
        cursor = page.next_cursor;
        if (cursor == null) {
            try std.testing.expect(page.complete);
            try std.testing.expect(i < 10);
            break;
        }
    }
    try std.testing.expect(cursor == null);
    try std.testing.expectEqual(@as(usize, 30), seen.count());
    for (0..30) |i| try std.testing.expect(seen.contains(try std.fmt.allocPrint(a, "thread-{d}", .{i})));
}

fn proposalOperationExplanation(store: *Store, operation: m.types.OperationStatus, now: i64, known: ?i64) ![]const u8 {
    const a = store.arena.allocator();
    const request: m.types.provenance.ExplainRequest = .{ .resource = .{ .table = "memory_", .table_id = 0, .kind = .operation, .id = operation.id, .revision = operation.revision }, .max_nodes = 1, .max_bytes = 65536, .known_at = known };
    return store.service("alice", true, now).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, request, .{}), "");
}

test "memory stale open proposal replacement preserves immutable attempts and fences old verdicts" {
    for ([_]bool{ false, true }) |legacy| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try externalReviewSetup(&store);
        const old: m.types.Evidence = .{ .id = "old-context", .resource_selector = .{ .table = "business", .id = "approval" } };
        _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{old} }, .{}), "old-context");
        var proposal: m.types.EntityProposalRequest = .{ .action = .merge, .from = "a", .to = "b", .expected_revision = 4, .actor = "old-judge", .evidence = &.{.{ .id = "old-context" }} };
        const first = try store.service("alice", true, 4).execute(a, .propose_entity, "", try std.json.Stringify.valueAlloc(a, proposal, .{}), "first-proposal");
        const original = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, first, .{});
        if (legacy) {
            const path = try fixtureKey(a, "proposal-input", original.proposal_id.?);
            var value = try std.json.parseFromSliceLeaky(std.json.Value, a, store.values.get(path).?, .{});
            _ = value.object.swapRemove("revision");
            _ = value.object.swapRemove("operation");
            try store.values.put(a, path, try std.json.Stringify.valueAlloc(a, value, .{}));
            _ = store.values.remove(try fixtureKey(a, "proposal-attempt", original.id));
        }
        store.resource_revision = 8;
        try std.testing.expectError(error.MemoryForgotten, store.service("alice", true, 5).execute(a, .review_entity, original.proposal_id.?, "{\"expected_revision\":5,\"accept\":true}", "stale-verdict"));
        const fresh: m.types.Evidence = .{ .id = "fresh-context", .resource_selector = .{ .table = "business", .id = "approval" } };
        _ = try store.service("alice", true, 6).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{fresh} }, .{}), "fresh-context");
        proposal.expected_revision = 6;
        proposal.evidence = &.{.{ .id = "fresh-context" }};
        proposal.actor = "new-judge";
        const second = try store.service("alice", true, 7).execute(a, .propose_entity, "", try std.json.Stringify.valueAlloc(a, proposal, .{}), "second-proposal");
        const latest = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, second, .{});
        try std.testing.expectEqualStrings(original.proposal_id.?, latest.proposal_id.?);
        const old_history = try proposalOperationExplanation(&store, original, 8, null);
        try std.testing.expect(std.mem.indexOf(u8, old_history, "old-context") != null);
        try std.testing.expect(std.mem.indexOf(u8, old_history, "fresh-context") == null);
        const limited_request: m.types.provenance.ExplainRequest = .{ .resource = .{ .table = "memory_", .table_id = 0, .kind = .operation, .id = original.id, .revision = original.revision }, .max_nodes = 1 };
        const limited = try store.service("alice", false, 8).execute(a, .explain, "", try std.json.Stringify.valueAlloc(a, limited_request, .{}), "");
        try std.testing.expect(std.mem.indexOf(u8, limited, "old-context") == null);
        const new_history = try proposalOperationExplanation(&store, latest, 8, null);
        try std.testing.expect(std.mem.indexOf(u8, new_history, "fresh-context") != null);
        try std.testing.expect(std.mem.indexOf(u8, new_history, "new-judge") != null);
        try std.testing.expectError(error.MemoryNotFound, proposalOperationExplanation(&store, latest, 8, 5));
        try std.testing.expectError(error.MemoryRevisionConflict, store.service("alice", true, 8).execute(a, .review_entity, latest.proposal_id.?, "{\"expected_revision\":5,\"accept\":true}", "old-judge-result"));
        _ = try store.service("alice", true, 8).execute(a, .review_entity, latest.proposal_id.?, "{\"expected_revision\":7,\"accept\":true}", "fresh-verdict");
        proposal.expected_revision = 8;
        _ = try store.service("alice", true, 9).execute(a, .propose_entity, "", try std.json.Stringify.valueAlloc(a, proposal, .{}), "terminal-retry");
        try std.testing.expectError(error.MemoryEntityConflict, store.service("alice", true, 9).execute(a, .review_entity, latest.proposal_id.?, "{\"expected_revision\":9,\"accept\":false}", "reopen"));
        _ = try store.service("alice", true, 10).execute(a, .forget, "", "{\"evidence_ids\":[\"old-context\"]}", "forget-old");
        const forgotten = try proposalOperationExplanation(&store, original, 11, null);
        try std.testing.expect(std.mem.indexOf(u8, forgotten, "old-context") == null);
        const retained = try store.service("alice", true, 11).execute(a, .entity_proposal, latest.proposal_id.?, "{}", "");
        try std.testing.expect(std.mem.indexOf(u8, retained, "fresh-context") != null);
        for (0..25) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "attempt-cleanup-{d}", .{i}), @intCast(12 + i));
        try std.testing.expect(store.values.get(try fixtureKey(a, "proposal-attempt", original.id)) == null);
    }
}

test "memory open proposals refresh configuration and recover after old context retirement" {
    for ([_]bool{ false, true }) |retire| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        try externalReviewSetup(&store);
        if (retire) {
            _ = try store.service("alice", true, 3).execute(a, .observe, "", "{\"evidence\":[{\"id\":\"old\",\"source_id\":\"old\",\"source_version\":\"1\",\"text\":\"OLD_REVIEW_CONTEXT\"}]}", "old");
        }
        var state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        var request: m.types.EntityProposalRequest = .{ .action = .merge, .from = "a", .to = "b", .expected_revision = state.revision, .evidence = if (retire) &.{.{ .id = "old" }} else &.{} };
        const raw = try store.service("alice", true, 4).execute(a, .propose_entity, "", try std.json.Stringify.valueAlloc(a, request, .{}), "proposal");
        const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, raw, .{});
        if (retire) {
            _ = try store.service("alice", true, 5).execute(a, .forget, "", "{\"evidence_ids\":[\"old\"]}", "forget");
            for (0..20) |i| try discard(&store, .advance, "", "{}", try std.fmt.allocPrint(a, "retire-binding-{d}", .{i}), @intCast(6 + i));
            try std.testing.expect(store.values.get(try fixtureKey(a, "proposal-input", receipt.proposal_id.?)) == null);
        } else {
            state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
            _ = try store.service("alice", true, 5).execute(a, .configure, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = state.revision, .config = m.types.Config{ .policy_version = "new-policy" } }, .{}), "config");
        }
        state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        request.expected_revision = state.revision;
        request.evidence = &.{.{ .id = "e" }};
        _ = try store.service("alice", true, 30).execute(a, .propose_entity, "", try std.json.Stringify.valueAlloc(a, request, .{}), "refresh");
        _ = try store.service("alice", true, 31).execute(a, .review_entity, receipt.proposal_id.?, try std.json.Stringify.valueAlloc(a, m.types.EntityVerdict{ .expected_revision = state.revision + 1, .accept = false }, .{}), "reject");
    }
}

test "memory historical mentions apply canonical entity temporal admission in every episode projection" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    _ = try store.service("alice", true, 1).execute(a, .create, "", "{}", "create");
    const early: m.types.Evidence = .{ .id = "early", .source_id = "history", .source_version = "1", .text = "Original entity", .thread_id = "thread", .ordinal = 0 };
    const original: m.types.Entity = .{ .id = "a", .name = "Original", .evidence = &.{.{ .id = "early" }} };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{early}, .entities = &.{original} }, .{}), "early");
    const late: m.types.Evidence = .{ .id = "late", .source_id = "notes", .source_version = "2", .text = "Later entity" };
    const future: m.types.Entity = .{ .id = "future-entity", .name = "Future", .evidence = &.{.{ .id = "late" }} };
    _ = try store.service("alice", true, 10).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{late}, .entities = &.{future} }, .{}), "late");
    _ = try store.service("alice", true, 20).execute(a, .curate_entity, "", "{\"action\":\"merge\",\"from\":\"a\",\"to\":\"future-entity\",\"expected_revision\":4}", "merge");
    for ([_]m.Action{ .episode, .episodes }) |action| {
        const raw = try store.service("alice", true, 30).execute(a, action, if (action == .episode) "thread" else "", "{\"known_at\":3}", "");
        try std.testing.expect(std.mem.indexOf(u8, raw, "Original entity") != null);
        try std.testing.expect(std.mem.indexOf(u8, raw, "future-entity") == null);
    }
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 30).execute(a, .entity_view, "a", "{\"known_at\":3}", ""));
    const graph = try store.service("alice", true, 30).execute(a, .recall, "", "{\"known_at\":3,\"graph\":{\"entity_seeds\":[\"a\"]},\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, graph, "future-entity") == null);
    const current = try store.service("alice", true, 30).execute(a, .episode, "thread", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, current, "future-entity") != null);
    // A target already known at the cutoff may be projected normally.
    const later = try store.service("alice", true, 30).execute(a, .episode, "thread", "{\"known_at\":11}", "");
    try std.testing.expect(std.mem.indexOf(u8, later, "future-entity") != null);
}

test "memory indexed candidates avoid primary scans and description noise and share production views" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{\"max_output_items\":256}", "create", 1);
    const entities = try a.alloc(m.types.Entity, 256);
    for (entities, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "unrelated-{d}", .{i}), .name = try std.fmt.allocPrint(a, "unrelated-{d}", .{i}), .evidence = &.{.{ .id = "e" }} };
    for (0..4) |batch| {
        for (entities, 0..) |*entity, i| {
            entity.id = try std.fmt.allocPrint(a, "unrelated-{d}", .{batch * 256 + i});
            entity.name = entity.id;
        }
        const source_evidence: m.types.Evidence = .{ .id = "e", .source_id = "source", .source_version = "1", .text = "The approval reviewer is a review system for approval checks" };
        _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{source_evidence}, .entities = entities }, .{}), try std.fmt.allocPrint(a, "batch-{d}", .{batch}));
    }
    const special = [_]m.types.Entity{
        .{ .id = "seed", .name = "approval reviewer", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "match", .name = "approvals reviewer", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "noise", .name = "banana", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "alias", .name = "independent", .aliases = &.{"approval reviewer"}, .evidence = &.{.{ .id = "e" }} },
    };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{m.types.Evidence{ .id = "e", .source_id = "source", .source_version = "1", .text = "The approval reviewer is a review system for approval checks" }}, .entities = &special }, .{}), "special");
    const claims = try a.alloc(m.types.Assertion, 12);
    for (claims, 0..) |*claim, i| claim.* = .{ .id = try std.fmt.allocPrint(a, "description-{d}", .{i}), .subject = "match", .predicate = try std.fmt.allocPrint(a, "describes-{d}", .{i}), .value = "details", .statement = try std.fmt.allocPrint(a, "The reviewer has detail {d}", .{i}), .evidence = &.{.{ .id = "e" }} };
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "descriptions");
    _ = try store.service("alice", true, 4).execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"noise-description\",\"subject\":\"noise\",\"predicate\":\"description\",\"value\":\"noise\",\"statement\":\"The approval reviewer is a review system for approval checks\",\"evidence\":[{\"id\":\"e\"}]}]}", "noise-description");
    const scans = store.entity_scans;
    const before = store.scans;
    const result = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 5).execute(a, .entity_candidates, "seed", "{}", ""), .{});
    try std.testing.expectEqual(scans, store.entity_scans);
    // The number of unrelated entities does not add primary or posting probes.
    try std.testing.expect(store.scans - before < 64);
    const items = result.object.get("items").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), items.len);
    try std.testing.expectEqualStrings("alias", items[0].object.get("entity").?.object.get("entity").?.object.get("id").?.string);
    const view = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 5).execute(a, .entity_view, "match", "{}", ""), .{});
    const candidate_view = items[1].object.get("view").?;
    try std.testing.expectEqualStrings(view.object.get("description").?.string, candidate_view.object.get("description").?.string);
    try std.testing.expectEqualStrings(view.object.get("fingerprint").?.string, candidate_view.object.get("fingerprint").?.string);
    try std.testing.expectEqual(@as(usize, 13), candidate_view.object.get("dependencies").?.array.items.len);
}

test "memory indexed candidate pages cover dense postings and bind seed kind principal and revision" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{\"max_output_items\":256}", "create", 1);
    const entities = try a.alloc(m.types.Entity, 141);
    for (entities, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "entity-{d}", .{i}), .name = "shared", .visibility = .team, .evidence = &.{.{ .id = "e" }} };
    const evidence: m.types.Evidence = .{ .id = "e", .source_id = "source", .source_version = "1", .text = "Shared entity observations", .visibility = .team };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = entities }, .{}), "entities");
    var cursor: ?[]const u8 = null;
    var first_cursor: ?[]const u8 = null;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var finished = false;
    for (0..32) |_| {
        const request: m.types.EntityCandidateRequest = .{ .limit = 64, .cursor = cursor };
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 3).execute(a, .entity_candidates, "entity-0", try std.json.Stringify.valueAlloc(a, request, .{}), ""), .{});
        for (result.object.get("items").?.array.items) |item| try seen.put(a, item.object.get("entity").?.object.get("entity").?.object.get("id").?.string, {});
        const next = result.object.get("next_cursor").?;
        if (next == .null) {
            finished = true;
            break;
        }
        cursor = next.string;
        if (first_cursor == null) first_cursor = cursor;
    }
    try std.testing.expect(finished);
    try std.testing.expectEqual(@as(usize, 140), seen.count());
    const encoded = try std.json.Stringify.valueAlloc(a, m.types.EntityCandidateRequest{ .cursor = first_cursor }, .{});
    try std.testing.expectError(error.InvalidMemoryCursor, store.service("alice", true, 3).execute(a, .entity_candidates, "entity-1", encoded, ""));
    try std.testing.expectError(error.InvalidMemoryCursor, store.service("bob", false, 3).execute(a, .entity_candidates, "entity-0", encoded, ""));
    const wrong_kind = try std.json.Stringify.valueAlloc(a, m.types.EntityCandidateRequest{ .cursor = first_cursor, .kind = .relationship }, .{});
    try std.testing.expectError(error.InvalidMemoryCursor, store.service("alice", true, 3).execute(a, .entity_candidates, "entity-0", wrong_kind, ""));
    try discard(&store, .observe, "", "{\"evidence\":[{\"id\":\"later\",\"source_id\":\"later\",\"source_version\":\"1\",\"text\":\"Later\"}]}", "later", 4);
    try std.testing.expectError(error.MemoryCursorStale, store.service("alice", true, 5).execute(a, .entity_candidates, "entity-0", encoded, ""));
}

test "memory candidate index backfill resumes through owner scheduling and retirement removes postings" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try externalReviewSetup(&store);
    const extra = try a.alloc(m.types.Entity, 40);
    for (extra, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "filler-{d}", .{i}), .name = try std.fmt.allocPrint(a, "filler-{d}", .{i}), .evidence = &.{.{ .id = "e" }} };
    _ = try store.service("alice", true, 3).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{m.types.Evidence{ .id = "e", .source_id = "fixture", .source_version = "1", .text = "Antfly is the Antfly database" }}, .entities = extra }, .{}), "fillers");
    // Simulate reopening a space written before the candidate index existed.
    var remove_keys: std.ArrayList([]const u8) = .empty;
    var iterator = store.values.keyIterator();
    while (iterator.next()) |path| if (std.mem.startsWith(u8, path.*, "entity-candidate-")) {
        try remove_keys.append(a, path.*);
    };
    for (remove_keys.items) |path| _ = store.values.remove(path);
    try std.testing.expectError(error.MemoryCapabilityUnavailable, store.service("alice", false, 3).execute(a, .entity_candidates, "a", "{}", ""));
    const scans = store.entity_scans;
    const legacy = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 3).execute(a, .entity_candidates, "a", "{}", ""), .{});
    try std.testing.expectEqual(@as(usize, 1), legacy.object.get("items").?.array.items.len);
    try std.testing.expectEqual(scans + 1, store.entity_scans);
    var harness: ScheduledMemory = .{ .store = &store, .now = 4 };
    var scheduling_cursor: @import("scheduling.zig").Cursor = .{};
    try harness.tick(&scheduling_cursor);
    try std.testing.expectEqual(@as(usize, 1), harness.cleanups);
    const partial = try std.json.parseFromSliceLeaky(std.json.Value, a, store.values.get("entity-candidate-index").?, .{});
    try std.testing.expect(!partial.object.get("complete").?.bool);
    try harness.tick(&scheduling_cursor);
    try harness.tick(&scheduling_cursor);
    try std.testing.expectEqual(@as(usize, 3), harness.cleanups);
    const index = try std.json.parseFromSliceLeaky(std.json.Value, a, store.values.get("entity-candidate-index").?, .{});
    try std.testing.expect(index.object.get("complete").?.bool);
    const after_backfill = store.entity_scans;
    _ = try store.service("alice", true, 5).execute(a, .entity_candidates, "a", "{}", "");
    try std.testing.expectEqual(after_backfill, store.entity_scans);
    try discard(&store, .forget, "", "{\"evidence_ids\":[\"e\"]}", "forget", 6);
    try std.testing.expectError(error.MemoryNotFound, store.service("alice", true, 7).execute(a, .entity_candidates, "a", "{}", ""));
    for (0..20) |i| _ = try store.service("alice", true, 8).execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "retire-{d}", .{i}));
    iterator = store.values.keyIterator();
    while (iterator.next()) |path| try std.testing.expect(!std.mem.startsWith(u8, path.*, "entity-candidate-term/"));
}

test "memory candidate overflow preserves wide alias sets without index write amplification" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const aliases = try a.alloc([]const u8, 128);
    for (aliases, 0..) |*alias, i| alias.* = try std.fmt.allocPrint(a, "vocabulary-{d}", .{i});
    aliases[127] = "needle";
    const evidence: m.types.Evidence = .{ .id = "e", .source_id = "source", .source_version = "1", .text = "Wide vocabularies and a needle" };
    const entities = [_]m.types.Entity{
        .{ .id = "needle", .name = "needle", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "wide", .name = "wide", .aliases = aliases, .evidence = &.{.{ .id = "e" }} },
    };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = &entities }, .{}), "entities");
    var postings: usize = 0;
    var iterator = store.values.keyIterator();
    while (iterator.next()) |path| if (std.mem.startsWith(u8, path.*, "entity-candidate-term/")) {
        postings += 1;
    };
    try std.testing.expectEqual(@as(usize, 7), postings);
    // Many foreign private matches cannot affect wide-seed page shape.
    const foreign = try a.alloc(m.types.Entity, 100);
    for (foreign, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "foreign-{d}", .{i}), .name = "needle", .evidence = &.{.{ .id = "foreign-evidence" }} };
    const foreign_evidence: m.types.Evidence = .{ .id = "foreign-evidence", .source_id = "foreign", .source_version = "1", .text = "Private foreign entities" };
    _ = try store.service("bob", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{foreign_evidence}, .entities = foreign }, .{}), "foreign");
    const primary_scans = store.entity_scans;
    for ([_][]const u8{ "needle", "wide" }, [_][]const u8{ "wide", "needle" }) |seed, expected| {
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", false, 3).execute(a, .entity_candidates, seed, "{}", ""), .{});
        try std.testing.expect(result.object.get("next_cursor").? == .null);
        try std.testing.expectEqual(primary_scans, store.entity_scans);
        const items = result.object.get("items").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), items.len);
        try std.testing.expectEqualStrings(expected, items[0].object.get("entity").?.object.get("entity").?.object.get("id").?.string);
        try std.testing.expectEqualStrings(if (std.mem.eql(u8, seed, "wide")) "bounded_audience_fallback" else "indexed_lexical", result.object.get("retrieval").?.string);
    }
}

test "memory indexed spelling neighbors preserve compound names without substring description admission" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    const evidence: m.types.Evidence = .{ .id = "e", .source_id = "source", .source_version = "1", .text = "Compound database names and sandbox permissions" };
    const entities = [_]m.types.Entity{
        .{ .id = "antfly", .name = "Antfly", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "antflydb", .name = "AntflyDB", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "elevated", .name = "elevated sandbox permission", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "escalated", .name = "escalated permissions", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "cat", .name = "cat", .evidence = &.{.{ .id = "e" }} },
        .{ .id = "concatenate", .name = "concatenate", .evidence = &.{.{ .id = "e" }} },
    };
    _ = try store.service("alice", true, 2).execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &.{evidence}, .entities = &entities }, .{}), "entities");
    for ([_][]const u8{ "antfly", "elevated", "cat" }, [_]?[]const u8{ "antflydb", "escalated", null }) |seed, expected| {
        const result = try std.json.parseFromSliceLeaky(std.json.Value, a, try store.service("alice", true, 3).execute(a, .entity_candidates, seed, "{}", ""), .{});
        const items = result.object.get("items").?.array.items;
        try std.testing.expectEqual(@as(usize, if (expected != null) 1 else 0), items.len);
        if (expected) |id| try std.testing.expectEqualStrings(id, items[0].object.get("entity").?.object.get("entity").?.object.get("id").?.string);
    }
}

test "memory recall prepares a large query once within a bounded request allocator" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    try discard(&store, .create, "", "{}", "create", 1);
    try discard(&store, .observe, "", source, "source", 2);
    const claims = try a.alloc(m.types.Assertion, 128);
    for (claims, 0..) |*claim, i| claim.* = .{ .id = try std.fmt.allocPrint(a, "claim-{d}", .{i}), .subject = try std.fmt.allocPrint(a, "subject-{d}", .{i}), .predicate = "fact", .value = "term0", .statement = "term0 TERM0", .evidence = &.{.{ .id = "e1" }} };
    _ = try store.service("alice", true, 3).execute(a, .assertions, "", try std.json.Stringify.valueAlloc(a, .{ .assertions = claims }, .{}), "claims");
    var query: std.ArrayList(u8) = .empty;
    for (0..4096) |i| try query.appendSlice(a, try std.fmt.allocPrint(a, "term{d} ", .{i}));
    const body = try std.json.Stringify.valueAlloc(a, m.types.Recall{ .query = query.items, .limit = 1, .token_budget = 2048 }, .{});
    const buffer = try a.alloc(u8, 16 * 1024 * 1024);
    var fixed = std.heap.FixedBufferAllocator.init(buffer);
    const result = try store.service("alice", false, 4).execute(fixed.allocator(), .recall, "", body, "");
    const decoded = try std.json.parseFromSliceLeaky(m.types.RecallResult, a, result, .{});
    try std.testing.expectEqual(@as(usize, 1), decoded.items.len);
    try std.testing.expectEqual(@as(usize, 128), decoded.scanned);
    try std.testing.expectEqual(@as(usize, 127), decoded.omitted);
    try std.testing.expectEqual(@as(f64, 1.0 / 4096.0), decoded.items[0].score);
    try std.testing.expect(result.len <= 2048);
}

test "memory discovery and proposal wire fixtures are actual typed service responses" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    store.table_id = 7;
    store.space = "wire";
    try externalReviewSetup(&store);
    const candidates = try store.service("alice", true, 3).execute(a, .entity_candidates, "a", "{}", "");
    _ = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, candidates, .{});
    const proposal = try externalProposal(&store);
    const details = try store.service("alice", true, 4).execute(a, .entity_proposal, proposal.proposal_id.?, "{}", "");
    _ = try std.json.parseFromSliceLeaky(m.types.EntityProposalDetail, a, details, .{});
    const proposals = try store.service("alice", true, 4).execute(a, .entity_proposals, "", "{}", "");
    _ = try std.json.parseFromSliceLeaky(m.types.EntityProposalPage, a, proposals, .{});
    inline for (.{
        .{ m.types.EntityCandidatePage, candidates, @embedFile("fixtures/wire/candidates.json") },
        .{ m.types.EntityProposalDetail, details, @embedFile("fixtures/wire/proposal.json") },
        .{ m.types.EntityProposalPage, proposals, @embedFile("fixtures/wire/proposals.json") },
    }) |fixture| {
        const expected = try std.json.parseFromSliceLeaky(fixture[0], a, fixture[2], .{});
        const actual = try std.json.parseFromSliceLeaky(fixture[0], a, fixture[1], .{});
        try std.testing.expectEqualDeep(expected, actual);
    }
}

test "memory curator publication and receipt replay revalidate the narrow grant" {
    var store = Store.init();
    defer store.deinit();
    try externalReviewSetup(&store);
    const a = store.arena.allocator();
    const Authority = struct {
        fn require(ptr: *anyopaque, grant: m.types.RequiredGrant) !void {
            const current: *Store = @ptrCast(@alignCast(ptr));
            if (grant != .curator or current.public_output) return error.Forbidden;
            current.required_grant = grant;
        }
        fn finish(ptr: *anyopaque) !void {
            const current: *Store = @ptrCast(@alignCast(ptr));
            if (current.reject_decision) current.public_output = true;
            try require(ptr, .curator);
        }
    };
    var curator = store.service("alice", false, 3);
    curator.context.curator = true;
    curator.context.grant_ptr = &store;
    curator.context.require_grant_fn = Authority.require;
    curator.context.final_check_ptr = &store;
    curator.context.final_check_fn = Authority.finish;
    _ = try curator.execute(a, .entity_candidates, "a", "{}", "");
    const revision = store.revision;
    const body = try std.fmt.allocPrint(a, "{{\"expected_revision\":{d},\"action\":\"merge\",\"from\":\"a\",\"to\":\"b\",\"evidence\":[{{\"id\":\"e\"}}]}}", .{revision + 1});
    store.reject_decision = true;
    try std.testing.expectError(error.Forbidden, curator.execute(a, .propose_entity, "", body, "proposal"));
    try std.testing.expectEqual(revision, store.revision);
    store.reject_decision = false;
    store.public_output = false;
    const receipt = try std.json.parseFromSliceLeaky(m.types.OperationStatus, a, try curator.execute(a, .propose_entity, "", body, "proposal"), .{});
    _ = try curator.execute(a, .entity_proposals, "", "{}", "");
    _ = try curator.execute(a, .entity_proposal, receipt.proposal_id.?, "{}", "");
    const verdict = try std.fmt.allocPrint(a, "{{\"expected_revision\":{d},\"accept\":true}}", .{store.revision + 1});
    _ = try curator.execute(a, .review_entity, receipt.proposal_id.?, verdict, "review");
    try std.testing.expectEqual(m.types.RequiredGrant.curator, store.required_grant);
    for ([_]struct { action: m.Action, body: []const u8 }{
        .{ .action = .configure, .body = "{}" },
        .{ .action = .forget, .body = "{\"source_ids\":[\"fixture\"]}" },
        .{ .action = .stage_embeddings, .body = "{}" },
    }) |denied| try std.testing.expectError(error.Forbidden, curator.execute(a, denied.action, "", denied.body, @tagName(denied.action)));
    store.public_output = true;
    try std.testing.expectError(error.Forbidden, curator.execute(a, .review_entity, receipt.proposal_id.?, verdict, "review"));
    try std.testing.expectError(error.Forbidden, curator.execute(a, .entity_proposals, "", "{}", ""));
}

test "memory curator reviews another owner's team relationship proposal without acquiring private access" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var owner = store.service("alice", true, 1);
    _ = try owner.execute(a, .create, "", "{\"ontology_pack\":\"software_team_v1\"}", "create");
    _ = try owner.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Antfly is a database\",\"visibility\":\"team\"}],\"entities\":[{\"id\":\"a\",\"name\":\"Antfly\",\"visibility\":\"team\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"b\",\"name\":\"database\",\"visibility\":\"team\",\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    _ = try owner.execute(a, .propose_relationship, "", "{\"id\":\"shared\",\"expected_revision\":3,\"assertion\":{\"id\":\"link\",\"subject\":\"a\",\"predicate\":\"is_a\",\"value\":\"b\",\"statement\":\"Antfly is a database\",\"visibility\":\"team\",\"evidence\":[{\"id\":\"e\"}]}}", "shared");
    _ = try owner.execute(a, .propose_relationship, "", "{\"id\":\"private\",\"expected_revision\":4,\"assertion\":{\"id\":\"private-link\",\"subject\":\"a\",\"predicate\":\"is_a\",\"value\":\"b\",\"statement\":\"Private proposed relation\",\"evidence\":[{\"id\":\"e\"}]}}", "private");
    var curator = store.service("bob", false, 2);
    curator.context.curator = true;
    _ = try curator.execute(a, .relationship_proposal, "shared", "{}", "");
    try std.testing.expectError(error.Forbidden, curator.execute(a, .relationship_proposal, "private", "{}", ""));
    _ = try curator.execute(a, .review_relationship, "shared", "{\"expected_revision\":5,\"accept\":true}", "review");
    const claim = try curator.execute(a, .get, "link", "{}", "");
    try std.testing.expect(std.mem.indexOf(u8, claim, "Antfly is a database") != null);
}

// A deterministic neighbor source tests policy and fences, not ANN quality or
// scaling. Native adapter tests exercise the actual vector index separately.
const SemanticFixture = struct {
    fn service(store: *Store, now: i64) m.Service {
        var result = modelService(store, now);
        result.backend.embed_document = fakeEmbed;
        result.backend.neighbors = neighbors;
        result.backend.ensure_vector_index = ensure;
        return result;
    }
    fn ensure(_: *anyopaque, _: A, _: []const u8, _: u32) !void {}
    fn neighbors(ptr: *anyopaque, a: A, request: m.NeighborRequest) ![]const m.Neighbor {
        const store: *Store = @ptrCast(@alignCast(ptr));
        store.neighbor_calls += 1;
        var result: std.ArrayList(m.Neighbor) = .empty;
        const prefix = try std.fmt.allocPrint(a, "entity-semantic/{s}/", .{request.index});
        var entries = store.values.iterator();
        while (entries.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.key_ptr.*, prefix)) continue;
            // Different embedding roles deliberately have self-similarity 0.8.
            try result.append(a, .{ .key = entry.key_ptr.*, .similarity = 0.8 });
            if (result.items.len == request.limit) break;
        }
        return result.items;
    }
    fn query(_: *anyopaque, a: A, _: m.types.Model, _: []const u8) ![]const f32 {
        return a.dupe(f32, &.{ 0.8, 0.6 });
    }
};

test "memory semantic readiness survives ambiguous publication and fences the readiness acknowledgment" {
    const Fault = struct {
        fn commit(ptr: *anyopaque, a: A, expected: ?m.Row, changes: []const m.Change) !void {
            const store: *Store = @ptrCast(@alignCast(ptr));
            try Store.commit(ptr, a, expected, changes);
            if (store.failures != 0) {
                store.failures -= 1;
                return error.MemoryCommitPending;
            }
        }
        fn finish(ptr: *anyopaque) !void {
            const store: *Store = @ptrCast(@alignCast(ptr));
            if (store.reject_decision and store.values.contains("entity-semantic-index")) {
                store.reject_decision = false;
                return error.Forbidden;
            }
        }
    };
    inline for (.{ false, true }) |revoke_ack| {
        var store = Store.init();
        defer store.deinit();
        const a = store.arena.allocator();
        var service = SemanticFixture.service(&store, 1);
        const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
        _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
        _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Knowledge graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
        if (revoke_ack) {
            store.reject_decision = true;
            service.context.final_check_ptr = &store;
            service.context.final_check_fn = Fault.finish;
        } else {
            store.failures = 1;
            service.backend.commit = Fault.commit;
        }
        try std.testing.expectError(if (revoke_ack) error.Forbidden else error.MemoryCommitPending, service.execute(a, .advance, "", "{}", "interrupted"));
        const index = try std.json.parseFromSliceLeaky(m.types.SemanticIndex, a, store.values.get("entity-semantic-index").?, .{});
        try std.testing.expect(index.backfill_complete);
        try std.testing.expect(!index.complete);
        var page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
        try std.testing.expectEqual(.not_ready, page.semantic_discovery);
        // Replaying the committed operation cannot turn ambiguity into readiness.
        _ = try service.execute(a, .advance, "", "{}", "interrupted");
        page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
        try std.testing.expectEqual(.not_ready, page.semantic_discovery);
        // Fresh owner maintenance republishes the barrier before acknowledging it.
        _ = try service.execute(a, .advance, "", "{}", "recover");
        page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
        try std.testing.expectEqual(.indexed, page.semantic_discovery);
    }
}

test "memory semantic projections backfill union role-normalized neighbors and invalidate without cleanup" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"A knowledge graph (KG) stores linked facts.\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"kg\",\"name\":\"KG\",\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    var page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.not_ready, page.semantic_discovery);
    _ = try service.execute(a, .advance, "", "{}", "backfill");
    service.backend.embed = SemanticFixture.query;
    page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
    try std.testing.expectEqual(@as(usize, 1), page.items.len);
    try std.testing.expectEqualStrings("kg", page.items[0].entity.entity.id);
    try std.testing.expectEqual(@as(f64, 0), page.items[0].match_score);
    try std.testing.expect(page.items[0].semantic_discovered);
    try std.testing.expectApproxEqAbs(@as(f64, 1), page.items[0].name_similarity.?, 0.00001);
    try std.testing.expectApproxEqAbs(@as(f64, 1), page.items[0].view_similarity.?, 0.00001);
    const scans = store.entity_scans;
    _ = try service.execute(a, .entity_candidates, "graph", "{}", "");
    try std.testing.expectEqual(scans, store.entity_scans);
    _ = try service.execute(a, .assertions, "", "{\"assertions\":[{\"id\":\"description\",\"subject\":\"graph\",\"predicate\":\"description\",\"value\":\"graph\",\"statement\":\"An edited graph description\",\"evidence\":[{\"id\":\"e\"}]}]}", "description");
    page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.not_ready, page.semantic_discovery);
    _ = try service.execute(a, .advance, "", "{}", "refresh-description");
    page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
    try std.testing.expect(std.mem.indexOf(u8, page.seed_view.description, "edited") != null);
    inline for (.{ .{ "merge", "graph", @as(usize, 0) }, .{ "unmerge", @as(?[]const u8, null), @as(usize, 1) }, .{ "suppress", @as(?[]const u8, null), @as(usize, 0) }, .{ "unsuppress", @as(?[]const u8, null), @as(usize, 1) } }) |transition| {
        _ = try service.execute(a, .curate_entity, "", try std.json.Stringify.valueAlloc(a, .{ .expected_revision = (try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{})).revision, .action = transition[0], .from = "kg", .to = transition[1] }, .{}), transition[0]);
        try drainSemantic(service, a, "refresh-" ++ transition[0]);
        page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
        try std.testing.expectEqual(.indexed, page.semantic_discovery);
        try std.testing.expectEqual(transition[2], page.items.len);
    }
    _ = try service.execute(a, .curate_entity, "", try std.fmt.allocPrint(a, "{{\"expected_revision\":{d},\"action\":\"distinct\",\"from\":\"graph\",\"to\":\"kg\"}}", .{(try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{})).revision}), "pin");
    page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(@as(usize, 0), page.items.len);
    _ = try service.execute(a, .forget, "", "{\"source_ids\":[\"s\"]}", "forget");
    try std.testing.expectError(error.MemoryNotFound, service.execute(a, .entity_candidates, "graph", "{}", ""));
}

test "memory Codex discovery fixture reports the indexed lexical baseline" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const Case = struct { a: []const u8, b: []const u8, same: bool, a_description: []const u8, b_description: []const u8 };
    var cases: std.ArrayList(Case) = .empty;
    var names: std.StringHashMapUnmanaged(usize) = .empty;
    var entities: std.ArrayList(m.types.Entity) = .empty;
    var claims: std.ArrayList(m.types.Assertion) = .empty;
    var lines = std.mem.tokenizeScalar(u8, @embedFile("fixtures/codex-a2d59cfb01/concept_merge.jsonl"), '\n');
    while (lines.next()) |line| {
        const row = try std.json.parseFromSliceLeaky(Case, a, line, .{ .ignore_unknown_fields = true });
        try cases.append(a, row);
        inline for (.{ .{ row.a, row.a_description }, .{ row.b, row.b_description } }) |concept| {
            const entry = try names.getOrPut(a, concept[0]);
            if (!entry.found_existing) {
                entry.value_ptr.* = entities.items.len;
                const id = try std.fmt.allocPrint(a, "concept-{d}", .{entities.items.len});
                try entities.append(a, .{ .id = id, .name = concept[0], .evidence = &.{.{ .id = "fixture" }} });
                try claims.append(a, .{ .id = id, .subject = id, .predicate = "description", .value = concept[0], .statement = concept[1], .evidence = &.{.{ .id = "fixture" }} });
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 84), entities.items.len);
    try discard(&store, .create, "", "{\"max_output_items\":256,\"max_input_bytes\":1048576}", "create", 1);
    try discard(&store, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "fixture", .source_id = "codex-a2d59cfb01", .source_version = "1", .text = @embedFile("fixtures/codex-a2d59cfb01/concept_merge.jsonl") }}, .entities = entities.items, .assertions = claims.items }, .{}), "fixture", 2);
    var discovered: [84][84]bool = @splat(@splat(false));
    var top5: [84][84]bool = @splat(@splat(false));
    var total: usize = 0;
    const scans = store.entity_scans;
    for (entities.items, 0..) |entity, i| {
        var cursor: ?[]const u8 = null;
        var first = true;
        while (true) {
            const result = try store.service("alice", false, 3).execute(a, .entity_candidates, entity.id, try std.json.Stringify.valueAlloc(a, .{ .cursor = cursor, .limit = @as(u16, 64) }, .{}), "");
            const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, result, .{});
            try std.testing.expectEqual(@as(usize, 0), page.omitted);
            for (page.items, 0..) |hit, position| {
                const j = try std.fmt.parseInt(usize, hit.entity.entity.id["concept-".len..], 10);
                discovered[i][j] = true;
                if (first and position < 5) top5[i][j] = true;
            }
            cursor = page.next_cursor;
            first = false;
            if (cursor == null) break;
        }
        for (discovered[i]) |found| if (found) {
            total += 1;
        };
    }
    var recalled: usize = 0;
    var recalled_top5: usize = 0;
    var non_synonyms: usize = 0;
    for (cases.items) |row| {
        const i = names.get(row.a).?;
        const j = names.get(row.b).?;
        const found = discovered[i][j] or discovered[j][i];
        if (row.same) {
            if (found) recalled += 1;
            if (top5[i][j] or top5[j][i]) recalled_top5 += 1;
        } else if (found) non_synonyms += 1;
    }
    try std.testing.expectEqual(scans, store.entity_scans);
    try std.testing.expect(recalled >= 17 and recalled_top5 >= 17);
    try std.testing.expect(total < 5 * entities.items.len);
    std.debug.print("Codex lexical: {d}/18 synonym pairs, {d}/18 top5, {d}/46 labelled non-synonyms, {d:.3} candidates/seed, 0 primary scans\n", .{ recalled, recalled_top5, non_synonyms, @as(f64, @floatFromInt(total)) / 84 });
}

fn drainSemantic(service: m.Service, a: A, token: []const u8) !void {
    for (0..64) |i| {
        const backend = service.backend;
        const state = try std.json.parseFromSliceLeaky(m.types.State, a, (try backend.get(backend.ptr, a, "control")).?.json, .{});
        const row = try backend.get(backend.ptr, a, "entity-semantic-index");
        const index = if (row) |value| try std.json.parseFromSliceLeaky(m.types.SemanticIndex, a, value.json, .{}) else m.types.SemanticIndex{};
        if (index.version == m.types.semantic_index_version and index.complete and index.generation == state.config.generation and (try backend.scan(backend.ptr, a, "entity-semantic-work/", null, 1)).rows.len == 0 and !try @import("scheduling.zig").retentionDue(backend, a, service.context.now_ms, state)) return;
        _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "{s}-{d}", .{ token, i }));
    }
    return error.TestUnexpectedResult;
}
test "memory semantic continuation drains the union without repeating ANN" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    var entities: [41]m.types.Entity = undefined;
    for (&entities, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "entity-{d}", .{i}), .name = if (i == 0) "radiance" else "opaque", .evidence = &.{.{ .id = "e" }} };
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e", .source_id = "s", .source_version = "1", .text = "evidence" }}, .entities = entities }, .{}), "observe");
    for (0..8) |i| _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "advance-{d}", .{i}));
    var cursor: ?[]const u8 = null;
    var first_cursor: ?[]const u8 = null;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    const scans = store.entity_scans;
    for (0..16) |_| {
        const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "entity-0", try std.json.Stringify.valueAlloc(a, .{ .cursor = cursor }, .{}), ""), .{});
        try std.testing.expectEqual(.indexed, page.semantic_discovery);
        try std.testing.expectEqual(@as(usize, 0), page.omitted);
        for (page.items) |hit| {
            try std.testing.expect(hit.semantic_discovered);
            try std.testing.expect(!seen.contains(hit.entity.entity.id));
            try seen.put(a, hit.entity.entity.id, {});
        }
        cursor = page.next_cursor;
        if (first_cursor == null) first_cursor = cursor;
        if (cursor == null) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(seen.count() > 16);
    try std.testing.expectEqual(@as(usize, 2), store.neighbor_calls);
    try std.testing.expectEqual(scans, store.entity_scans);
    const body = try std.json.Stringify.valueAlloc(a, .{ .cursor = first_cursor.? }, .{});
    var other = service;
    other.context.principal = "bob";
    try std.testing.expectError(error.InvalidMemoryCursor, other.execute(a, .entity_candidates, "entity-0", body, ""));
    _ = try service.execute(a, .advance, "", "{}", "change-revision");
    try std.testing.expectError(error.MemoryCursorStale, service.execute(a, .entity_candidates, "entity-0", body, ""));
}
test "memory semantic backfill retries temporarily unavailable sources" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e", .resource_selector = .{ .table = "business", .id = "approval" } }}, .entities = &[_]m.types.Entity{.{ .id = "graph", .name = "knowledge graph", .evidence = &.{.{ .id = "e" }} }} }, .{}), "observe");
    store.resource_state = .denied;
    _ = try service.execute(a, .advance, "", "{}", "denied");
    const blocked = try std.json.parseFromSliceLeaky(m.types.SemanticIndex, a, store.values.get("entity-semantic-index").?, .{});
    try std.testing.expect(!blocked.complete);
    try std.testing.expectEqual(@as(usize, 1), (try service.backend.scan(service.backend.ptr, a, "entity-semantic-blocked/", null, 8)).rows.len);
    const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
    try std.testing.expect(!try @import("scheduling.zig").semanticDue(service.backend, a, state));
    try std.testing.expect(!try @import("scheduling.zig").retentionDue(service.backend, a, service.context.now_ms, state));
    store.resource_state = .current;
    service.context.now_ms = 40000;
    for (0..8) |i| _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "restore-{d}", .{i}));
    const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    const index = try std.json.parseFromSliceLeaky(m.types.SemanticIndex, a, store.values.get("entity-semantic-index").?, .{});
    try std.testing.expect(index.complete);
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
}
test "memory semantic timers refresh validity boundaries without a primary scan" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"d\",\"subject\":\"graph\",\"predicate\":\"description\",\"value\":\"graph\",\"statement\":\"An active graph description\",\"valid_from\":100,\"valid_to\":300,\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    _ = try service.execute(a, .advance, "", "{}", "backfill");
    service.context.now_ms = 200;
    for (0..8) |i| _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "time-{d}", .{i}));
    const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
    try std.testing.expect(std.mem.indexOf(u8, page.seed_view.description, "active graph") != null);
    const scans = store.entity_scans;
    service.context.now_ms = 400;
    try drainSemantic(service, a, "expired");
    const expired = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, expired.semantic_discovery);
    try std.testing.expect(std.mem.indexOf(u8, expired.seed_view.description, "active graph") == null);
    try std.testing.expectEqual(scans, store.entity_scans);
}

test "memory semantic inference failures cannot roll back retention cleanup" {
    const Fail = struct {
        fn embed(_: *anyopaque, _: A, _: m.types.Model, _: []const u8) ![]const f32 {
            return error.TransientInferenceFailure;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"keep\",\"source_id\":\"keep\",\"source_version\":\"1\",\"text\":\"Keep graph\"},{\"id\":\"retire\",\"source_id\":\"retire\",\"source_version\":\"1\",\"text\":\"FORGET_THIS_SECRET\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"keep\"}]}],\"assertions\":[{\"id\":\"secret\",\"subject\":\"secret\",\"predicate\":\"p\",\"value\":\"secret\",\"statement\":\"FORGET_THIS_SECRET\",\"evidence\":[{\"id\":\"retire\"}]}]}", "observe");
    _ = try service.execute(a, .forget, "", "{\"source_ids\":[\"retire\"]}", "forget");
    service.backend.embed_document = Fail.embed;
    for (0..64) |i| {
        const state = try std.json.parseFromSliceLeaky(m.types.State, a, store.values.get("control").?, .{});
        if (!try @import("scheduling.zig").retentionDue(service.backend, a, service.context.now_ms, state)) break;
        _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "cleanup-{d}", .{i}));
    } else return error.TestUnexpectedResult;
    try std.testing.expect(store.values.get(try fixtureKey(a, "assertion", "secret")) == null);
    try std.testing.expect(store.values.get(try fixtureKey(a, "evidence", "retire")) == null);
    try std.testing.expectError(error.TransientInferenceFailure, service.execute(a, .advance, "", "{}", "inference"));
}

test "memory semantic continuation spans two disjoint maximum neighbor windows" {
    const Disjoint = struct {
        fn neighbors(ptr: *anyopaque, a: A, request: m.NeighborRequest) ![]const m.Neighbor {
            const store: *Store = @ptrCast(@alignCast(ptr));
            store.neighbor_calls += 1;
            const names = std.mem.startsWith(u8, request.index, "memory_names_");
            const prefix = try std.fmt.allocPrint(a, "entity-semantic/{s}/", .{request.index});
            var result: std.ArrayList(m.Neighbor) = .empty;
            var rows = store.values.iterator();
            while (rows.next()) |row| {
                if (!std.mem.startsWith(u8, row.key_ptr.*, prefix)) continue;
                const value = try std.json.parseFromSliceLeaky(m.types.EntityProjection, a, row.value_ptr.*, .{});
                const number = try std.fmt.parseInt(usize, value.entity_id["entity-".len..], 10);
                if (number == 0 or (if (names) number > 64 else number <= 64)) continue;
                try result.append(a, .{ .key = row.key_ptr.*, .similarity = 1 });
                if (result.items.len == request.limit) break;
            }
            return result.items;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    service.backend.neighbors = Disjoint.neighbors;
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true, .neighbors = 64 }, .max_output_items = 256 };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    var entities: [129]m.types.Entity = undefined;
    for (&entities, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "entity-{d}", .{i}), .name = if (i == 0) "radiance" else "opaque", .evidence = &.{.{ .id = "e" }} };
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e", .source_id = "s", .source_version = "1", .text = "evidence" }}, .entities = entities }, .{}), "observe");
    for (0..20) |i| _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "advance-{d}", .{i}));
    var cursor: ?[]const u8 = null;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (0..4) |_| {
        const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "entity-0", try std.json.Stringify.valueAlloc(a, .{ .limit = @as(u16, 64), .cursor = cursor }, .{}), ""), .{});
        try std.testing.expectEqual(@as(usize, 0), page.omitted);
        for (page.items) |hit| {
            try std.testing.expect(!seen.contains(hit.entity.entity.id));
            try seen.put(a, hit.entity.entity.id, {});
        }
        cursor = page.next_cursor;
        if (cursor == null) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 128), seen.count());
    try std.testing.expectEqual(@as(usize, 2), store.neighbor_calls);
}

test "memory semantic deferred sources cannot pin healthy work" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    var entities: [20]m.types.Entity = undefined;
    for (&entities, 0..) |*entity, i| entity.* = .{ .id = try std.fmt.allocPrint(a, "blocked-{d}", .{i}), .name = "blocked", .evidence = &.{.{ .id = "blocked" }} };
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "blocked", .resource_selector = .{ .table = "business", .id = "approval" } }}, .entities = entities }, .{}), "blocked");
    store.resource_state = .denied;
    for (0..3) |i| _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "initial-{d}", .{i}));
    const first = try service.backend.scan(service.backend.ptr, a, "entity-semantic-blocked/", null, 8);
    const id = for (0..1024) |i| {
        const candidate = try std.fmt.allocPrint(a, "healthy-{d}", .{i});
        if (std.mem.lessThan(u8, first.rows[7].key, try fixtureKey(a, "entity-semantic-blocked", candidate))) break candidate;
    } else return error.TestUnexpectedResult;
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "healthy", .source_id = "healthy", .source_version = "1", .text = "available" }}, .entities = &[_]m.types.Entity{.{ .id = id, .name = "healthy", .evidence = &.{.{ .id = "healthy" }} }} }, .{}), "healthy");
    for (0..4) |i| _ = try service.execute(a, .advance, "", "{}", try std.fmt.allocPrint(a, "rotate-{d}", .{i}));
    var count: usize = 0;
    const rows = try service.backend.scan(service.backend.ptr, a, "entity-semantic/", null, 64);
    for (rows.rows) |row| if (std.mem.eql(u8, (try std.json.parseFromSliceLeaky(m.types.EntityProjection, a, row.json, .{})).entity_id, id)) {
        count += 1;
    };
    try std.testing.expectEqual(@as(usize, 2), count);
    store.resource_state = .current;
    service.context.now_ms = 40000;
    try drainSemantic(service, a, "restored");
}
test "memory semantic timers include transitive assertion boundaries" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"parent\",\"subject\":\"other\",\"predicate\":\"p\",\"value\":\"other\",\"statement\":\"Timed support\",\"valid_from\":100,\"valid_to\":300,\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"child\",\"subject\":\"graph\",\"predicate\":\"p\",\"value\":\"graph\",\"statement\":\"Derived graph description\",\"depends_on\":[\"parent\"],\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    try drainSemantic(service, a, "initial");
    for ([_]i64{ 200, 400 }) |now| {
        service.context.now_ms = now;
        try drainSemantic(service, a, try std.fmt.allocPrint(a, "at-{d}", .{now}));
        const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
        try std.testing.expectEqual(.indexed, page.semantic_discovery);
        try std.testing.expectEqual(now == 200, std.mem.indexOf(u8, page.seed_view.description, "Derived graph") != null);
    }
}
test "memory semantic redacted views refresh after support access is restored" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"entity\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"},{\"id\":\"support\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"entity\"}]}],\"assertions\":[{\"id\":\"d\",\"subject\":\"graph\",\"predicate\":\"p\",\"value\":\"graph\",\"statement\":\"Restored graph description\",\"evidence\":[{\"id\":\"support\"}]}]}", "observe");
    store.resource_state = .denied;
    try drainSemantic(service, a, "redacted");
    store.resource_state = .current;
    service.context.now_ms = 40000;
    try drainSemantic(service, a, "restored");
    const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
    try std.testing.expect(std.mem.indexOf(u8, page.seed_view.description, "Restored graph") != null);
}
test "memory semantic forgetting retires timer and retry identities" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    const service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"d\",\"subject\":\"graph\",\"predicate\":\"p\",\"value\":\"graph\",\"statement\":\"Future graph description\",\"valid_from\":1000000,\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    try drainSemantic(service, a, "initial");
    try std.testing.expectEqual(@as(usize, 1), (try service.backend.scan(service.backend.ptr, a, "entity-semantic-due/", null, 8)).rows.len);
    _ = try service.execute(a, .forget, "", "{\"source_ids\":[\"s\"]}", "forget");
    try drainSemantic(service, a, "cleanup");
    for ([_][]const u8{ "entity-semantic-due/", "entity-semantic-timer/", "entity-semantic-work/", "entity-semantic-blocked/", "entity-semantic/" }) |prefix| try std.testing.expectEqual(@as(usize, 0), (try service.backend.scan(service.backend.ptr, a, prefix, null, 8)).rows.len);
}

test "memory semantic historical source pins do not block current backfill" {
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", try std.json.Stringify.valueAlloc(a, .{ .evidence = &[_]m.types.Evidence{.{ .id = "e", .resource_selector = .{ .table = "business", .id = "approval" } }}, .entities = &[_]m.types.Entity{.{ .id = "graph", .name = "knowledge graph", .evidence = &.{.{ .id = "e" }} }} }, .{}), "observe");
    store.resource_state = .changed;
    try drainSemantic(service, a, "changed");
    try std.testing.expectEqual(@as(usize, 1), (try service.backend.scan(service.backend.ptr, a, "entity-semantic-due/", null, 8)).rows.len);
    store.resource_state = .current;
    service.context.now_ms = 40000;
    try drainSemantic(service, a, "restored-pin");
    const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
}

test "memory semantic scheduler upgrade installs deadlines without reembedding unchanged text" {
    const Fail = struct {
        fn embed(_: *anyopaque, _: A, _: m.types.Model, _: []const u8) ![]const f32 {
            return error.TransientInferenceFailure;
        }
    };
    var store = Store.init();
    defer store.deinit();
    const a = store.arena.allocator();
    var service = SemanticFixture.service(&store, 1);
    const config: m.types.Config = .{ .models = .{ .embedder = fake_model }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(a, .create, "", try std.json.Stringify.valueAlloc(a, config, .{}), "create");
    _ = try service.execute(a, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"d\",\"subject\":\"graph\",\"predicate\":\"p\",\"value\":\"graph\",\"statement\":\"Future graph description\",\"valid_from\":100,\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    try drainSemantic(service, a, "initial");
    // Simulate the pre-deadline on-disk certificate and missing timer coverage.
    for ([_][]const u8{ "entity-semantic-due/", "entity-semantic-timer/" }) |prefix| {
        const rows = try service.backend.scan(service.backend.ptr, a, prefix, null, 8);
        for (rows.rows) |row| _ = store.values.remove(row.key);
    }
    var index = try std.json.parseFromSliceLeaky(m.types.SemanticIndex, a, store.values.get("entity-semantic-index").?, .{});
    index.version = 0;
    try store.values.put(a, "entity-semantic-index", try std.json.Stringify.valueAlloc(a, index, .{}));
    service.backend.embed_document = Fail.embed;
    const old = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.not_ready, old.semantic_discovery);
    try drainSemantic(service, a, "upgrade");
    try std.testing.expectEqual(@as(usize, 1), (try service.backend.scan(service.backend.ptr, a, "entity-semantic-due/", null, 8)).rows.len);
    service.backend.embed_document = fakeEmbed;
    service.context.now_ms = 200;
    try drainSemantic(service, a, "due");
    const page = try std.json.parseFromSliceLeaky(m.types.EntityCandidatePage, a, try service.execute(a, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
    try std.testing.expect(std.mem.indexOf(u8, page.seed_view.description, "Future graph") != null);
}
