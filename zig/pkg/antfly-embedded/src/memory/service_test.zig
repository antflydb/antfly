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
    conflict: bool = false,
    failures: u8 = 0,
    public_output: bool = false,
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
        const json = self.values.get(key) orelse return null;
        return .{ .key = try a.dupe(u8, key), .json = try a.dupe(u8, json), .version = self.revision };
    }
    fn scan(ptr: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !m.Page {
        const self: *Store = @ptrCast(@alignCast(ptr));
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
