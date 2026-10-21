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
    reads: usize = 0,
    scans: usize = 0,
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
        return .{ .backend = .{ .ptr = self, .get = get, .scan = scan, .commit = commit }, .context = .{ .principal = principal, .admin = admin, .now_ms = now } };
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
fn fakeGenerate(ptr: *anyopaque, a: A, _: m.types.Config, _: []const m.types.StoredEvidence) !m.types.Generated {
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
    try discard(&store, .curate_entity, "", "{\"action\":\"suppress\",\"from\":\"right\",\"expected_revision\":7}", "suppress", 7);
    const hidden = try store.service("alice", true, 500).execute(a, .recall, "", "{\"token_budget\":8192}", "");
    try std.testing.expect(std.mem.indexOf(u8, hidden, "Production uses Terraform") == null);
    try discard(&store, .curate_entity, "", "{\"action\":\"unsuppress\",\"from\":\"right\",\"expected_revision\":8}", "unsuppress", 8);
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

fn contextDerivedOutput(_: *anyopaque, a: A, _: m.types.Config, _: []const m.types.StoredEvidence) !m.types.Generated {
    const claims = try a.alloc(m.types.Assertion, 1);
    claims[0] = .{ .id = "derived", .subject = "production", .predicate = "guidance", .value = "Secret", .statement = "Private context secret", .evidence = &.{.{ .id = "current" }}, .support_sets = &.{.{ .evidence = &.{.{ .id = "alternative" }} }} };
    const entities = try a.alloc(m.types.Entity, 1);
    entities[0] = .{ .id = "derived-entity", .name = "Private context secret", .evidence = &.{.{ .id = "current" }} };
    return .{ .assertions = claims, .entities = entities };
}
fn contextDerivedSetup(store: *Store, expires: bool) !void {
    const a = store.arena.allocator();
    const config: m.types.Config = .{ .processing = .hybrid, .features = .{ .assertion_distillation = true }, .models = .{ .generator = fake_model } };
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

    fn generate(_: *anyopaque, a: A, _: m.types.Config, inputs: []const m.types.StoredEvidence) !m.types.Generated {
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
                    fn generate(_: *anyopaque, _: A, _: m.types.Config, _: []const m.types.StoredEvidence) !m.types.Generated {
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
