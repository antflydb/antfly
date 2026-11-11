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
const objects = @import("objectstore");
const api = @import("mod.zig");
const t = api.types;
const a = std.testing.allocator;
const dataset: t.Id = @splat(1);
const namespace = std.fmt.bytesToHex(dataset, .lower);
const FakeClock = struct {
    now: u64 = 1000,
    authority: u64 = 1000,
    fn unix(ptr: *const anyopaque) u64 {
        return (@as(*const @This(), @ptrCast(@alignCast(ptr)))).now;
    }
    fn monotonic(ptr: *const anyopaque) u64 {
        return (@as(*const @This(), @ptrCast(@alignCast(ptr)))).authority;
    }
    fn clock(self: *const @This()) api.catalog.Clock {
        return .{ .ptr = self, .unix_fn = unix, .authority_fn = monotonic };
    }
};
fn digest(bytes: []const u8) t.Id {
    var value: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
    return value;
}
fn definition(kind: t.Kind) t.Definition {
    return .{ .id = dataset, .generation = 1, .kind = kind, .schema = digest("{}"), .schema_json = "{}", .authorization = @splat(3), .retention_ns = 100 * std.time.ns_per_day, .local_residence_ns = 0, .summary = if (kind == .summary) .{ .source = @splat(9), .timestamp = "ts", .interval_ns = 60, .mutable_for_ns = 100, .measures = &.{.{ .name = "n", .law = .count }} } else null };
}
fn segment() t.Segment {
    var checksum: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("states", &checksum, .{});
    return .{ .id = @splat(4), .dataset = dataset, .definition_generation = 1, .schema = digest("{}"), .state_encoding = 1, .coverage = .{ .start_ns = "0", .end_ns = "60" }, .source = .{ .partition = @splat(5), .incarnation = 1, .sequence = 20 }, .finalized = true, .checksum = checksum, .byte_len = 6, .location = @splat(6) };
}
fn catalog(memory: *objects.memory.MemoryClient, clock: *const FakeClock) api.Catalog {
    return .{ .context = .{ .alloc = a, .client = memory.client(), .bucket = "meta", .prefix = "catalog", .namespace = &namespace, .io = std.testing.io }, .dataset = dataset, .owner = @splat(1), .clock = clock.clock() };
}
fn location(memory: *objects.memory.MemoryClient, cold: bool) api.Location {
    return .{ .id = if (cold) @splat(7) else @splat(6), .storage_identity = if (cold) @splat(17) else @splat(16), .client = memory.client(), .bucket = if (cold) "cold" else "hot", .prefix = "segments", .access = if (cold) .queryable else .local };
}
fn setup(memory: *objects.memory.MemoryClient) !void {
    var client = memory.client();
    for ([_][]const u8{ "meta", "hot", "cold" }) |bucket| try client.makeBucket(bucket);
}

test "retained dataset migration preserves pinned readers and independently expires history" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    const cold = location(&memory, true);
    try store.create(definition(.summary));
    try store.publish(segment(), hot, "states");
    const old = try store.acquire(segment().id);
    const before = try store.read(old, hot);
    a.free(before);
    try store.migrate(segment().id, 1, hot, cold);
    const after = try store.read(old, hot);
    defer a.free(after);
    try std.testing.expectEqualStrings("states", after);
    const fresh = try store.acquire(segment().id);
    const archived = try store.read(fresh, cold);
    defer a.free(archived);
    try std.testing.expectEqualStrings("states", archived);
    try std.testing.expectError(error.DatasetReadersActive, store.reclaimLocal(segment().id, hot));
    try store.release(old);
    try std.testing.expect(!try store.reclaimLocal(segment().id, hot));
    try std.testing.expect(try store.reclaimLocal(segment().id, hot));
    const still_remote = try store.read(fresh, cold);
    a.free(still_remote);
    try std.testing.expectError(error.DatasetRetentionPending, store.retire(segment().id, 2));
    try store.release(fresh);
    clock.now = definition(.summary).retention_ns + 1000;
    clock.authority = clock.now;
    try store.retire(segment().id, 2);
    try std.testing.expectError(error.DatasetSegmentRetired, store.acquire(segment().id));
    try store.collect(segment().id, cold);
    try store.collect(segment().id, cold);
    var restarted = store;
    restarted.owner = @splat(9);
    const retired = try restarted.getSegment(segment().id);
    defer retired.deinit();
    try std.testing.expect(retired.value.retiring);
    try std.testing.expectError(error.DatasetSegmentAlreadyExists, restarted.publish(segment(), hot, "states"));
}

test "retained dataset rejected copies leave authoritative placement unchanged" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.records));
    try std.testing.expectError(error.DatasetSegmentCorrupt, store.publish(segment(), hot, "broken"));
    try std.testing.expectError(error.DatasetSegmentNotFound, store.getSegment(segment().id));
    try store.publish(segment(), hot, "states");
    var fault = objects.scripted_fault.Client.init(a, memory.client());
    defer fault.deinit();
    var missing = location(&memory, true);
    missing.client = fault.client();
    fault.failNextPutBefore(error.DatasetCopyFault);
    try std.testing.expectError(error.DatasetCopyFault, store.migrate(segment().id, 1, hot, missing));
    const current = try store.getSegment(segment().id);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 1), current.value.placement_revision);
    try std.testing.expectEqual(t.Access.local, current.value.access);
    try std.testing.expect(current.value.previous_local == null);
    try std.testing.expectError(error.DatasetPlacementConflict, store.migrate(segment().id, 2, hot, location(&memory, true)));
}

test "retained dataset reclamation requires exact source lineage and durable finalized coverage" {
    const def = definition(.summary);
    var value = segment();
    var required = t.RequiredSummary{ .dataset = def.id, .definition_generation = def.generation, .source = value.source, .coverage = value.coverage };
    try t.requireSourceReclamation(required, def, value);
    required.source.sequence += 1;
    try std.testing.expectError(error.DatasetDependencyPending, t.requireSourceReclamation(required, def, value));
    required.source = value.source;
    required.source.incarnation += 1;
    try std.testing.expectError(error.DatasetDependencyPending, t.requireSourceReclamation(required, def, value));
    required.source = value.source;
    value.finalized = false;
    try std.testing.expectError(error.DatasetDependencyPending, t.requireSourceReclamation(required, def, value));
}

test "retained dataset temporal fingerprints validate signed boundaries without overflow" {
    var def = definition(.summary);
    var value = segment();
    try value.validate(def);
    value.coverage.start_ns = "-170141183460469231731687303715884105728";
    def.summary.?.origin_ns = "100";
    try std.testing.expectError(error.InvalidDatasetCoverage, value.validate(def));
    def.id = @splat(0);
    try std.testing.expectError(error.InvalidDatasetDefinition, def.validate());
}

test "retained dataset archive restore preserves definitions and dependent history" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    var archive = location(&memory, true);
    archive.access = .restore_required;
    try store.create(definition(.summary));
    try store.publish(segment(), hot, "states");
    try store.migrate(segment().id, 1, hot, archive);
    try std.testing.expectError(error.DatasetRestoreRequired, store.acquire(segment().id));
    try std.testing.expect(!try store.reclaimLocal(segment().id, hot));
    try std.testing.expect(try store.reclaimLocal(segment().id, hot));
    try store.restore(segment().id, 2, archive, hot);
    const lease = try store.acquire(segment().id);
    const restored = try store.read(lease, hot);
    a.free(restored);
    try store.release(lease);
    var next = definition(.summary);
    next.generation = 2;
    next.schema_json = "{\"type\":\"object\"}";
    next.schema = digest(next.schema_json);
    try store.evolve(1, next);
    const old = try store.getSegment(segment().id);
    defer old.deinit();
    try std.testing.expectEqual(@as(u64, 1), old.value.definition_generation);
    try std.testing.expectEqual(segment().schema, old.value.schema);
    const dependency: api.catalog.Dependency = .{ .id = @splat(10), .consumer = @splat(11), .consumer_generation = 1 };
    try store.hold(segment().id, dependency);
    clock.now = definition(.summary).retention_ns + 1000;
    clock.authority = clock.now;
    try std.testing.expectError(error.DatasetDependencyPending, store.retire(segment().id, 3));
    var stale = dependency;
    stale.consumer_generation = 2;
    try std.testing.expectError(error.DatasetDependencyConflict, store.releaseHold(segment().id, stale));
    try store.releaseHold(segment().id, dependency);
    try store.retire(segment().id, 3);
    try store.collect(segment().id, hot);
    try store.collect(segment().id, archive);
}

test "retained dataset stale publication fences cannot advance placement" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.records));
    try store.publish(segment(), hot, "states");
    var replacement = store;
    replacement.owner = @splat(42);
    const Callback = struct {
        fn fence(raw: *anyopaque) void {
            const other: *api.Catalog = @ptrCast(@alignCast(raw));
            other.create(definition(.records)) catch |err| {
                std.debug.assert(err == error.DatasetAlreadyExists);
                return;
            };
            unreachable;
        }
    };
    var fault = objects.scripted_fault.Client.init(a, memory.client());
    defer fault.deinit();
    fault.next_put = .{ .commit_then_callback = .{ .ptr = &replacement, .run = Callback.fence } };
    var cold = location(&memory, true);
    cold.client = fault.client();
    if (store.migrate(segment().id, 1, hot, cold)) |_| return error.ExpectedFencedMigration else |err| {
        try std.testing.expect(err == error.ImmutableLogFenced or err == error.PreconditionFailed);
    }
    const current = try replacement.getSegment(segment().id);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 1), current.value.placement_revision);
    try replacement.migrate(segment().id, 1, hot, cold);
    const committed = try replacement.getSegment(segment().id);
    defer committed.deinit();
    try std.testing.expectEqual(@as(u64, 2), committed.value.placement_revision);
}

test "retained dataset eviction resumes after ambiguous deletion and rejects rebound locations" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    var hot = location(&memory, false);
    const cold = location(&memory, true);
    try store.create(definition(.records));
    try store.publish(segment(), hot, "states");
    try store.migrate(segment().id, 1, hot, cold);
    try std.testing.expect(!try store.reclaimLocal(segment().id, hot));
    var wrong = hot;
    wrong.storage_identity = @splat(99);
    try std.testing.expectError(error.DatasetLocationConflict, store.reclaimLocal(segment().id, wrong));
    var fault = objects.scripted_fault.Client.init(a, memory.client());
    defer fault.deinit();
    fault.commitNextPutThenFail(error.DatasetDeleteFault);
    hot.client = fault.client();
    try std.testing.expectError(error.DatasetDeleteFault, store.reclaimLocal(segment().id, hot));
    try std.testing.expect(try store.reclaimLocal(segment().id, hot));
    const lease = try store.acquire(segment().id);
    const bytes = try store.read(lease, cold);
    a.free(bytes);
    clock.now = 500;
    clock.authority = lease.authority_deadline;
    try std.testing.expectError(error.DatasetLeaseExpired, store.read(lease, cold));
    try store.release(lease);
}

test "retained dataset listing is bounded and does not mix placement replicas" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    try store.create(definition(.records));
    for (0..5) |i| {
        var value = segment();
        value.id = @splat(@as(u8, @intCast(i + 1)));
        value.source.partition = value.id;
        try store.publish(value, location(&memory, false), "states");
    }
    try store.migrate(segment().id, 1, location(&memory, false), location(&memory, true));
    const first = try store.listSegments("", 3);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 3), first.value.results[0].len);
    const second = try store.listSegments(first.value.results[0][2].key, 3);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 2), second.value.results[0].len);
    try std.testing.expectError(error.DatasetPageBudgetExceeded, store.listSegments("", 1000));
}

test "retained dataset range planning seeks disjoint coverage and refuses gaps or resolution loss" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.summary));
    for (0..10) |i| {
        var value = segment();
        value.id = @splat(@as(u8, @intCast(i + 1)));
        const start = try std.fmt.allocPrint(a, "{d}", .{@as(i128, @intCast(i)) * 60 - 300});
        defer a.free(start);
        const end = try std.fmt.allocPrint(a, "{d}", .{@as(i128, @intCast(i + 1)) * 60 - 300});
        defer a.free(end);
        value.coverage = .{ .start_ns = start, .end_ns = end };
        try store.publish(value, hot, "states");
    }
    var plan = try store.planRange(1, segment().source.partition, 1, .{ .start_ns = "-60", .end_ns = "120" }, 3);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 3), plan.segments.len);
    try std.testing.expectEqualStrings("-60", plan.segments[0].coverage.start_ns);
    try std.testing.expectEqualStrings("120", plan.segments[2].coverage.end_ns);
    try std.testing.expectError(error.DatasetPageBudgetExceeded, store.planRange(1, segment().source.partition, 1, .{ .start_ns = "-60", .end_ns = "120" }, 2));
    try std.testing.expectError(error.DatasetCoverageUnavailable, store.planRange(1, segment().source.partition, 1, .{ .start_ns = "-600", .end_ns = "120" }, 10));
    try std.testing.expectError(error.DatasetResolutionUnavailable, store.planRange(1, segment().source.partition, 1, .{ .start_ns = "0", .end_ns = "65" }, 10));
    var duplicate = segment();
    duplicate.id = @splat(99);
    try std.testing.expectError(error.DatasetCoverageOverlap, store.publish(duplicate, hot, "states"));
    duplicate.coverage = .{ .start_ns = "-60", .end_ns = "120" };
    try std.testing.expectError(error.DatasetCoverageOverlap, store.publish(duplicate, hot, "states"));
}

test "retained dataset leases reject new reads at logical expiry independently of physical holds" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    try store.create(definition(.records));
    try store.publish(segment(), location(&memory, false), "states");
    const dependency: api.catalog.Dependency = .{ .id = @splat(10), .consumer = @splat(11), .consumer_generation = 1 };
    try store.hold(segment().id, dependency);
    clock.now = definition(.records).retention_ns + 1000;
    try std.testing.expectError(error.DatasetSegmentExpired, store.acquire(segment().id));
    try std.testing.expectError(error.DatasetDependencyPending, store.retire(segment().id, 1));
    const retained = try store.getSegment(segment().id);
    defer retained.deinit();
    try std.testing.expect(!retained.value.retiring);
}

test "retained dataset lifecycle releases every allocation on failed admission" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var memory = objects.memory.MemoryClient.init(alloc);
            defer memory.deinit();
            var client = memory.client();
            for ([_][]const u8{ "meta", "hot", "cold" }) |bucket| try client.makeBucket(bucket);
            var io_vtable = std.testing.io.vtable.*;
            io_vtable.random = struct {
                fn fill(_: ?*anyopaque, bytes: []u8) void {
                    @memset(bytes, 17);
                }
            }.fill;
            const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &io_vtable };
            var clock: FakeClock = .{};
            var store = catalog(&memory, &clock);
            store.context.alloc = alloc;
            store.context.io = io;
            try store.create(definition(.records));
            try store.publish(segment(), location(&memory, false), "states");
            const lease = try store.acquire(segment().id);
            const bytes = try store.read(lease, location(&memory, false));
            alloc.free(bytes);
            try store.release(lease);
            try store.migrate(segment().id, 1, location(&memory, false), location(&memory, true));
            try std.testing.expect(!try store.reclaimLocal(segment().id, location(&memory, false)));
            try std.testing.expect(try store.reclaimLocal(segment().id, location(&memory, false)));
        }
    };
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{});
}

test "retained dataset narrow range work is bounded across retained history" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    try store.create(definition(.records));
    for (0..64) |i| {
        var value = segment();
        value.id = @splat(@as(u8, @intCast(i + 1)));
        const start = try std.fmt.allocPrint(a, "{d}", .{@as(i128, @intCast(i)) * 60});
        defer a.free(start);
        const end = try std.fmt.allocPrint(a, "{d}", .{@as(i128, @intCast(i + 1)) * 60});
        defer a.free(end);
        value.coverage = .{ .start_ns = start, .end_ns = end };
        try store.publish(value, location(&memory, false), "states");
    }
    memory.resetOperationCount();
    var plan = try store.planRange(1, segment().source.partition, 1, .{ .start_ns = "3720", .end_ns = "3780" }, 1);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 1), plan.segments.len);
    const reads = memory.operationCount();
    std.debug.print("DATASET_RANGE retained=64 selected=1 provider_operations={d}\n", .{reads});
    try std.testing.expect(reads < 32);
}

test "retained dataset wire preserves full nanosecond durations and source revisions" {
    var def = definition(.summary);
    def.retention_ns = std.math.maxInt(u64);
    def.summary.?.mutable_for_ns = std.math.maxInt(u64);
    const encoded = try std.json.Stringify.valueAlloc(a, def, .{});
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retention_ns\":\"18446744073709551615\"") != null);
    const decoded = try std.json.parseFromSlice(t.Definition, a, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(std.math.maxInt(u64), decoded.value.retention_ns);
    try std.testing.expectEqual(std.math.maxInt(u64), decoded.value.summary.?.mutable_for_ns);
    const dom = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer dom.deinit();
    const from_value = try std.json.parseFromValue(t.Definition, a, dom.value, .{});
    defer from_value.deinit();
    try std.testing.expectEqual(std.math.maxInt(u64), from_value.value.retention_ns);
    var position = segment().source;
    position.sequence = std.math.maxInt(u64);
    const checkpoint = try std.json.Stringify.valueAlloc(a, position, .{});
    defer a.free(checkpoint);
    const restored = try std.json.parseFromSlice(t.SourcePosition, a, checkpoint, .{});
    defer restored.deinit();
    try std.testing.expectEqual(position.sequence, restored.value.sequence);
    def.generation = t.max_public_counter + 1;
    try std.testing.expectError(error.InvalidDatasetDefinition, def.validate());
}

test "retained dataset compaction bounds tombstones without resurrecting history" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.records));
    var later = segment();
    later.id = @splat(8);
    later.coverage = .{ .start_ns = "60", .end_ns = "120" };
    try store.publish(segment(), hot, "states");
    try store.publish(later, hot, "states");
    clock.now = definition(.records).retention_ns + 1000;
    try store.retire(segment().id, 1);
    try store.retire(later.id, 1);
    try store.collect(segment().id, hot);
    try store.collect(later.id, hot);
    try std.testing.expectError(error.DatasetCompactionOrder, store.compact(later.id));
    try store.compact(segment().id);
    try store.compact(later.id);
    try store.compact(later.id);
    try std.testing.expectError(error.DatasetSegmentRetired, store.publish(segment(), hot, "states"));
    try std.testing.expectError(error.DatasetSegmentRetired, store.publish(later, hot, "states"));
    const page = try store.listSegments("", 10);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 0), page.value.results[0].len);
    var fresh = segment();
    fresh.id = @splat(20);
    fresh.coverage = .{ .start_ns = "120", .end_ns = "180" };
    try store.publish(fresh, hot, "states");
}

test "retained dataset colocated logical locations retain independent copies" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    var cold = location(&memory, true);
    cold.bucket = hot.bucket;
    cold.prefix = hot.prefix;
    cold.storage_identity = hot.storage_identity;
    try store.create(definition(.records));
    try store.publish(segment(), hot, "states");
    try store.migrate(segment().id, 1, hot, cold);
    try std.testing.expect(!try store.reclaimLocal(segment().id, hot));
    try std.testing.expect(try store.reclaimLocal(segment().id, hot));
    const lease = try store.acquire(segment().id);
    defer store.release(lease) catch unreachable;
    const bytes = try store.read(lease, cold);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("states", bytes);
}

test "retained dataset cancelled copy cannot publish or authorize eviction" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    var store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.records));
    try store.publish(segment(), hot, "states");
    var signal = std.atomic.Value(bool).init(false);
    store.context.cancellation = objects.CancellationToken.fromAtomic(&signal);
    const Callback = struct {
        fn cancel(raw: *anyopaque) void {
            const flag: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
            flag.store(true, .release);
        }
    };
    var fault = objects.scripted_fault.Client.init(a, memory.client());
    defer fault.deinit();
    fault.next_put = .{ .commit_then_callback = .{ .ptr = &signal, .run = Callback.cancel } };
    var cold = location(&memory, true);
    cold.client = fault.client();
    try std.testing.expectError(error.Canceled, store.migrate(segment().id, 1, hot, cold));
    signal.store(false, .release);
    const current = try store.getSegment(segment().id);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 1), current.value.placement_revision);
    try std.testing.expect(current.value.previous_local == null);
    try store.migrate(segment().id, 1, hot, cold);
}

test "retained dataset reader admission is bounded and reclaims expired pins" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    try store.create(definition(.records));
    try store.publish(segment(), location(&memory, false), "states");
    for (0..64) |_| _ = try store.acquire(segment().id);
    try std.testing.expectError(error.DatasetReaderBudgetExceeded, store.acquire(segment().id));
    clock.now += api.catalog.pin_duration_ns + api.catalog.pin_grace_ns + 1;
    clock.authority = clock.now;
    const lease = try store.acquire(segment().id);
    try store.release(lease);
}

test "retained dataset identities always use byte arrays on the JSON wire" {
    const values = [_]t.Id{ @splat(7), @splat('a'), @splat(255) };
    for (values) |id| {
        var value = segment();
        value.location = id;
        value.archive = id;
        const encoded = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(encoded);
        const dom = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
        defer dom.deinit();
        for ([_][]const u8{ "id", "dataset", "schema", "checksum", "location", "archive" }) |field| {
            try std.testing.expectEqual(std.json.Value.array, std.meta.activeTag(dom.value.object.get(field).?));
        }
        const restored = try std.json.parseFromSlice(t.Segment, a, encoded, .{});
        defer restored.deinit();
        try std.testing.expectEqual(id, restored.value.location);
    }
}

test "retained dataset large buckets split into disjoint key spans and compact without losing siblings" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    const def = definition(.summary);
    try store.create(def);
    const split = t.rowKey(0, @splat(128));
    var left = segment();
    left.key_span = .{ .start = t.rowKey(0, @splat(0)), .end = split };
    var right = segment();
    right.id = @splat(8);
    right.key_span = .{ .start = split, .end = t.rowKey(60, @splat(0)) };
    try store.publish(right, hot, "states");
    try std.testing.expectError(error.DatasetCoverageUnavailable, store.planRange(1, left.source.partition, 1, left.coverage, 10));
    try store.publish(left, hot, "states");
    var plan = try store.planRange(1, left.source.partition, 1, left.coverage, 10);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.segments.len);
    try std.testing.expectEqual(left.id, plan.segments[0].id);
    try std.testing.expectEqual(right.id, plan.segments[1].id);
    var partial = try store.planCoveringKeys(1, left.source.partition, 1, try t.KeySpan.fromRange(left.coverage), 1, true);
    defer partial.deinit();
    try std.testing.expectEqual(@as(usize, 1), partial.segments.len);
    try std.testing.expectEqual(split, partial.next.?);
    var rest = try store.planCoveringKeys(1, left.source.partition, 1, .{ .start = split, .end = t.rowKey(60, @splat(0)) }, 1, true);
    defer rest.deinit();
    try std.testing.expectEqual(right.id, rest.segments[0].id);
    try std.testing.expectEqual(@as(?t.RowKey, null), rest.next);

    var overlap = segment();
    overlap.id = @splat(12);
    try std.testing.expectError(error.DatasetCoverageOverlap, store.publish(overlap, hot, "states"));
    const required = t.RequiredSummary{ .dataset = def.id, .definition_generation = 1, .source = left.source, .coverage = left.coverage };
    try std.testing.expectError(error.DatasetDependencyPending, t.requireSourceReclamation(required, def, left));
    var sliced = required;
    sliced.key_span = left.key_span;
    try t.requireSourceReclamation(sliced, def, left);
    clock.now = def.retention_ns + 1000;
    clock.authority = clock.now;
    try store.retire(left.id, 1);
    try store.collect(left.id, hot);
    try store.compact(left.id);
    try std.testing.expectError(error.DatasetSegmentRetired, store.publish(left, hot, "states"));
    const sibling = try store.getSegment(right.id);
    defer sibling.deinit();
    try std.testing.expectEqual(right.id, sibling.value.id);
    try store.retire(right.id, 1);
    try store.collect(right.id, hot);
    try store.compact(right.id);
}

test "retained dataset covering plans permit narrow ranges but reject interior holes" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.summary));
    var value = segment();
    value.coverage.end_ns = "180";
    try store.publish(value, hot, "states");
    const narrow = t.Range{ .start_ns = "60", .end_ns = "120" };
    try std.testing.expectError(error.DatasetCoverageUnavailable, store.planRange(1, value.source.partition, 1, narrow, 10));
    var plan = try store.planCoveringRange(1, value.source.partition, 1, narrow, 10);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 1), plan.segments.len);
    try std.testing.expectError(error.DatasetCoverageUnavailable, store.planCoveringRange(1, value.source.partition, 1, .{ .start_ns = "60", .end_ns = "240" }, 10));
}

test "retained dataset archived summary serving checks exact state and filters covering files" {
    const shared = @import("antfly_local_sources");
    const series = shared.storage_retained_series;
    const codec = shared.storage_retained_series_archive;
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    const cold = location(&memory, true);
    var def = definition(.summary);
    def.retention_ns = 1000;
    try store.create(def);
    const spec = series.Spec{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    var metrics = [_]series.Metric{.{ .count = 9_007_199_254_740_993 }};
    const rows = [_]series.Bucket{
        .{ .start_ns = 0, .dimensions = "[]", .rows = metrics[0].count, .metrics = &metrics },
        .{ .start_ns = 60, .dimensions = "[]", .rows = metrics[0].count, .metrics = &metrics },
        .{ .start_ns = 120, .dimensions = "[]", .rows = metrics[0].count, .metrics = &metrics },
    };
    var value = segment();
    value.state_encoding = codec.encoding;
    value.coverage.end_ns = "180";
    const receipt = codec.Receipt{ .source_sequence = value.source.sequence, .admitted_ns = 1000, .complete_from_ns = 0, .span = try value.span() };
    const bytes = try codec.encode(a, spec, receipt, &rows);
    defer a.free(bytes);
    value.byte_len = bytes.len;
    value.checksum = digest(bytes);
    try store.publish(value, hot, bytes);
    try store.migrate(value.id, 1, hot, cold);
    const Resolver = struct {
        hot: api.Location,
        cold: api.Location,
        pub fn location(self: @This(), id: t.Id) !api.Location {
            if (std.mem.eql(u8, &id, &self.hot.id)) return self.hot;
            if (std.mem.eql(u8, &id, &self.cold.id)) return self.cold;
            return error.InvalidDatasetLocation;
        }
    };
    const resolver = Resolver{ .hot = hot, .cold = cold };
    const range = t.Range{ .start_ns = "60", .end_ns = "120" };
    var result = try api.summary_read.read(a, store, def, spec, range, .{}, resolver);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i128, 60), result.rows[0].start_ns);
    try std.testing.expectEqual(metrics[0].count, (try result.rows[0].metrics[0].finalize(.count)).integer);
    var first_page = try api.summary_read.read(a, store, def, spec, .{ .start_ns = "0", .end_ns = "180" }, .{ .rows = 1, .page = true }, resolver);
    defer first_page.deinit();
    try std.testing.expectEqual(@as(usize, 1), first_page.rows.len);
    try std.testing.expectEqual(codec.key(rows[0]), first_page.next.?);
    var next_page = try api.summary_read.read(a, store, def, spec, .{ .start_ns = "0", .end_ns = "180" }, .{ .rows = 2, .page = true, .after = first_page.next }, resolver);
    defer next_page.deinit();
    try std.testing.expectEqual(@as(usize, 2), next_page.rows.len);
    try std.testing.expectEqual(@as(?t.RowKey, null), next_page.next);

    try std.testing.expectError(error.DatasetPageBudgetExceeded, api.summary_read.read(a, store, def, spec, range, .{ .bytes = 1 }, resolver));
    clock.now = 1100;
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, api.summary_read.read(a, store, def, spec, .{ .start_ns = "0", .end_ns = "60" }, .{}, resolver));
    var live = try api.summary_read.read(a, store, def, spec, range, .{}, resolver);
    defer live.deinit();
    try std.testing.expectEqual(@as(i128, 60), live.rows[0].start_ns);
    // A file containing a live later bucket cannot resurrect its expired
    // earlier bucket after wall-clock rollback or reopening the catalog.
    clock.now = 1050;
    const reopened_clock = catalog(&memory, &clock);
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, api.summary_read.read(a, reopened_clock, def, spec, .{ .start_ns = "0", .end_ns = "60" }, .{}, resolver));
    var still_live = try api.summary_read.read(a, reopened_clock, def, spec, range, .{}, resolver);
    defer still_live.deinit();
    try std.testing.expectEqual(@as(usize, 1), still_live.rows.len);
    var changed = spec;
    changed.source_incarnation += 1;
    try std.testing.expectError(error.DatasetCoverageUnavailable, api.summary_read.read(a, store, def, changed, range, .{}, resolver));
    var restore = cold;
    restore.id = @splat(20);
    restore.access = .restore_required;
    // A separately bound location is required when access semantics change.
    restore.storage_identity = @splat(21);
    var unavailable = value;
    unavailable.id = @splat(22);
    unavailable.coverage = .{ .start_ns = "180", .end_ns = "240" };
    unavailable.location = restore.id;
    unavailable.access = .restore_required;
    var empty_receipt = receipt;
    empty_receipt.span = try unavailable.span();
    const empty_bytes = try codec.encode(a, spec, empty_receipt, &.{});
    defer a.free(empty_bytes);
    unavailable.byte_len = empty_bytes.len;
    unavailable.checksum = digest(empty_bytes);
    try store.publish(unavailable, restore, empty_bytes);
    try std.testing.expectError(error.DatasetRestoreRequired, api.summary_read.read(a, store, def, spec, unavailable.coverage, .{}, resolver));
}

test "retained dataset sliced coverage cannot extend logical lifetime through exaggerated event bounds" {
    var value = segment();
    value.key_span = .{ .start = t.rowKey(0, @splat(0)), .end = t.rowKey(0, @splat(128)) };
    try value.validate(definition(.summary));
    value.coverage.end_ns = "120";
    try std.testing.expectError(error.InvalidDatasetCoverage, value.validate(definition(.summary)));
    value.coverage = .{ .start_ns = "-60", .end_ns = "60" };
    try std.testing.expectError(error.InvalidDatasetCoverage, value.validate(definition(.summary)));
}

test "retained dataset closed export publishes disjoint retry-safe pages within a bucket" {
    const export_page = @import("closure_export.zig");
    const local = @import("antfly_local_sources");
    const series = local.storage_retained_series;
    const codec = local.storage_retained_series_archive;
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const cold = location(&memory, true);
    const def = definition(.summary);
    try store.create(def);
    const spec: series.Spec = .{ .id = def.id, .generation = def.generation, .definition = try def.fingerprint(a), .source_partition = @splat(5), .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    // Both rows are after the closure clock. The source has stopped, so these
    // observations are immutable even though the event-time window was open.
    var counts = [_]series.Metric{ .{ .count = 1 }, .{ .count = 1 }, .{ .count = 2 } };
    var rows = [_]series.Bucket{
        .{ .start_ns = 1200, .dimensions = "[1]", .rows = 1, .metrics = counts[0..1] },
        .{ .start_ns = 1200, .dimensions = "[2]", .rows = 1, .metrics = counts[1..2] },
    };
    if (std.mem.order(u8, &codec.key(rows[0]), &codec.key(rows[1])) == .gt) std.mem.swap(series.Bucket, &rows[0], &rows[1]);
    const cut: export_page.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    const start = t.rowKey(0, @splat(0));
    const first = (try export_page.encode(a, def, spec, cut, start, rows[0..1], true, cold)).?;
    defer export_page.deinit(a, first);
    const second = (try export_page.encode(a, def, spec, cut, (try first.segment.span()).end, rows[1..2], false, cold)).?;
    defer export_page.deinit(a, second);
    try std.testing.expectError(error.InvalidDatasetCoverage, export_page.publish(store, spec, cut, second, cold));
    try export_page.publish(store, spec, cut, first, cold);
    try export_page.publish(store, spec, cut, first, cold);
    const half = (try store.getClosedExport(spec, cut)).?;
    try std.testing.expectEqual(@as(u64, 1), half.pages);
    try std.testing.expectError(error.DatasetDependencyPending, half.receipt());
    try std.testing.expectError(error.InvalidDatasetCoverage, store.sealClosedExport(spec, cut, half.next));
    try export_page.publish(store, spec, cut, second, cold);
    var plan = try store.planCoveringRange(1, spec.source_partition, 1, .{ .start_ns = "1200", .end_ns = "1260" }, 2);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.segments.len);
    try std.testing.expectEqualDeep((try plan.segments[0].span()).end, (try plan.segments[1].span()).start);
    var changed = rows[0];
    changed.rows = 2;
    changed.metrics = counts[2..3];
    const conflict = (try export_page.encode(a, def, spec, cut, start, &.{changed}, true, cold)).?;
    defer export_page.deinit(a, conflict);
    try std.testing.expectEqualDeep(first.segment.id, conflict.segment.id);
    try std.testing.expectError(error.InvalidDatasetCoverage, export_page.publish(store, spec, cut, conflict, cold));
    try std.testing.expect((try export_page.encode(a, def, spec, cut, start, &.{}, false, cold)) == null);
    const tail = t.rowKey(1260, @splat(0));
    const sealed = try store.sealClosedExport(spec, cut, tail);
    try std.testing.expectEqual(@as(u64, 2), sealed.pages);
    const reopened = catalog(&memory, &clock);
    try std.testing.expectEqualDeep(try sealed.receipt(), try (try reopened.getClosedExport(spec, cut)).?.receipt());
    try std.testing.expectEqualDeep(try sealed.receipt(), try (try reopened.sealClosedExport(spec, cut, tail)).receipt());
    const Resolve = struct {
        cold: api.Location,
        pub fn location(self: @This(), id: t.Id) !api.Location {
            if (!std.mem.eql(u8, &id, &self.cold.id)) return error.DatasetLocationUnavailable;
            return self.cold;
        }
    };
    const resolve: Resolve = .{ .cold = cold };
    const receipt = try sealed.receipt();
    var mixed = try api.summary_read.readClosed(a, reopened, def, spec, .{ .start_ns = "1200", .end_ns = "1320" }, .{}, cut, receipt, resolve);
    defer mixed.deinit();
    try std.testing.expectEqual(@as(usize, 2), mixed.rows.len);
    var empty = try api.summary_read.readClosed(a, reopened, def, spec, .{ .start_ns = "1260", .end_ns = "1320" }, .{}, cut, receipt, resolve);
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    try std.testing.expectError(error.InvalidDatasetCoverage, api.summary_read.readClosed(a, reopened, def, spec, .{ .start_ns = "1260", .end_ns = "1320" }, .{}, cut, @splat(0), resolve));
    const prior_clock = clock.now;
    try std.testing.expect((try reopened.nextExpiry(null)) == null);
    clock.now = @as(u64, @intCast(def.retention_ns)) + 1260;
    const first_due = (try reopened.nextExpiry(null)).?;
    const second_due = (try reopened.nextExpiry(first_due.cursor)).?;
    try std.testing.expect(!std.mem.eql(u8, &first_due.segment, &second_due.segment));
    try std.testing.expect((try reopened.nextExpiry(second_due.cursor)) == null);
    try reopened.retire(first.segment.id, 1);
    clock.now = prior_clock;
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, api.summary_read.readClosed(a, reopened, def, spec, .{ .start_ns = "1200", .end_ns = "1260" }, .{}, cut, receipt, resolve));
    clock.now = @as(u64, @intCast(def.retention_ns)) + 1260;
    _ = try reopened.collectExpired(first.segment.id, cold);
    try std.testing.expectEqualDeep(second.segment.id, (try reopened.nextExpiry(null)).?.segment);
    _ = try reopened.collectExpired(second.segment.id, cold);
    try std.testing.expect((try reopened.nextExpiry(null)) == null);
    var wrong_cut = cut;
    wrong_cut.sequence += 1;
    try std.testing.expectError(error.InvalidDatasetCoverage, reopened.getClosedExport(spec, wrong_cut));
}

test "retained dataset pin acquisition rebases after a concurrent reader and release retries CAS" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    try store.create(definition(.records));
    try store.publish(segment(), location(&memory, false), "states");
    const Competitor = struct {
        store: api.Catalog,
        lease: ?api.catalog.Lease = null,
        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lease = self.store.acquire(segment().id) catch unreachable;
        }
        fn root(_: *anyopaque, _: []const u8, key: []const u8) bool {
            return std.mem.endsWith(u8, key, "/ROOT");
        }
        fn rootPut(_: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            return std.mem.endsWith(u8, key, "/ROOT");
        }
    };
    var competitor: Competitor = .{ .store = store };
    var fault = objects.scripted_fault.Client.init(a, memory.client());
    defer fault.deinit();
    fault.get_filter = .{ .ptr = &competitor, .matches = Competitor.root };
    fault.next_get = .{ .complete_then_callback = .{ .ptr = &competitor, .run = Competitor.run } };
    var reader = store;
    reader.context.client = fault.client();
    const lease = try reader.acquire(segment().id);
    try std.testing.expect(competitor.lease != null);
    const other_bytes = try store.read(competitor.lease.?, location(&memory, false));
    a.free(other_bytes);
    fault.put_filter = .{ .ptr = &competitor, .matches = Competitor.rootPut };
    fault.next_put = .{ .fail_before = error.PreconditionFailed };
    try reader.release(lease);
    try store.release(competitor.lease.?);
    clock.now = definition(.records).retention_ns + 1000;
    try store.retire(segment().id, 1);
}

test "retained dataset retention watermark advances once per bucket and survives rollback" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    var def = definition(.summary);
    def.retention_ns = 1000;
    try store.create(def);
    const series = @import("antfly_local_sources").storage_retained_series;
    const spec: series.Spec = .{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = 1000, .laws = &.{.count} };
    _ = try store.admitRetentionClock(spec, 1100);
    const before = try store.admitRetentionClock(spec, 1101);
    try std.testing.expectEqual(@as(i128, 1101), before);
    try std.testing.expectEqual(@as(i128, 1060), try store.admitRetentionClock(spec, 1050));
    try std.testing.expect(try spec.expired(0, try catalog(&memory, &clock).admitRetentionClock(spec, 1050)));
    _ = try store.admitRetentionClock(spec, 1120);
    try std.testing.expect(try spec.expired(60, try store.admitRetentionClock(spec, 1050)));
}

test "retained dataset rejected archive receipts cannot advance durable retention" {
    const shared = @import("antfly_local_sources");
    const series = shared.storage_retained_series;
    const codec = shared.storage_retained_series_archive;
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    var def = definition(.summary);
    def.retention_ns = 1000;
    try store.create(def);
    const spec: series.Spec = .{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = 1000, .laws = &.{.count} };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    const rows = [_]series.Bucket{.{ .start_ns = 0, .dimensions = "[]", .rows = 1, .metrics = &metrics }};
    var value = segment();
    value.state_encoding = codec.encoding;
    const bytes = try codec.encode(a, spec, .{ .source_sequence = value.source.sequence + 1, .admitted_ns = 2000, .complete_from_ns = 0, .span = try value.span() }, &rows);
    defer a.free(bytes);
    value.byte_len = bytes.len;
    value.checksum = digest(bytes);
    const hot = location(&memory, false);
    try store.publish(value, hot, bytes);
    const Resolve = struct {
        hot: api.Location,
        pub fn location(self: @This(), _: t.Id) !api.Location {
            return self.hot;
        }
    };
    try std.testing.expectError(error.InvalidDatasetCoverage, api.summary_read.read(a, store, def, spec, value.coverage, .{}, Resolve{ .hot = hot }));
    try std.testing.expect(!try spec.expired(0, try store.admitRetentionClock(spec, 1000)));
}

test "retained dataset tied expiry pages compact in one bounded sweep" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const cold = location(&memory, true);
    const def = definition(.summary);
    try store.create(def);
    const shared = @import("antfly_local_sources");
    const series = shared.storage_retained_series;
    const codec = shared.storage_retained_series_archive;
    const export_page = @import("closure_export.zig");
    const spec: series.Spec = .{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    const cut: export_page.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    const count = 16;
    var metrics = [_]series.Metric{.{ .count = 1 }};
    var rows: [count]series.Bucket = undefined;
    var dimensions: [count][]u8 = undefined;
    for (&rows, &dimensions, 0..) |*row, *dim, i| {
        dim.* = try std.fmt.allocPrint(a, "[{d}]", .{i});
        row.* = .{ .start_ns = 0, .dimensions = dim.*, .rows = 1, .metrics = &metrics };
    }
    defer for (dimensions) |dim| a.free(dim);
    std.mem.sort(series.Bucket, &rows, {}, struct {
        fn less(_: void, l: series.Bucket, r: series.Bucket) bool {
            return std.mem.order(u8, &codec.key(l), &codec.key(r)) == .lt;
        }
    }.less);
    var frontier = t.rowKey(0, @splat(0));
    for (0..count) |i| {
        const page = (try export_page.encode(a, def, spec, cut, frontier, rows[i .. i + 1], i + 1 < count, cold)).?;
        defer export_page.deinit(a, page);
        try export_page.publish(store, spec, cut, page, cold);
        frontier = (try page.segment.span()).end;
    }
    _ = try store.sealClosedExport(spec, cut, frontier);
    clock.now = def.retention_ns + 1000;
    var attempts: usize = 0;
    var passes: usize = 0;
    var removed: usize = 0;
    while (removed < count and passes < count + 1) {
        passes += 1;
        var cursor: ?api.catalog.ExpiryCursor = null;
        while (try store.nextExpiry(cursor)) |work| {
            cursor = work.cursor;
            attempts += 1;
            _ = store.collectExpired(work.segment, cold) catch |err| switch (err) {
                error.DatasetCompactionOrder => continue,
                else => return err,
            };
            removed += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), passes);
    try std.testing.expectEqual(@as(usize, count), attempts);
    try std.testing.expectEqual(@as(usize, count), removed);
}

test "retained dataset discard fences publication and drains pins and dependencies before physical cleanup" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    const def = definition(.summary);
    try store.create(def);
    try store.publish(segment(), hot, "states");
    const pin = try store.acquire(segment().id);
    const hold: api.catalog.Dependency = .{ .id = @splat(8), .consumer = @splat(9), .consumer_generation = 1 };
    try store.hold(segment().id, hold);
    try store.beginDiscard(def);
    try store.beginDiscard(def);
    try std.testing.expectError(error.DatasetSegmentRetired, store.acquire(segment().id));
    try std.testing.expectError(error.DatasetSegmentRetired, store.publish(segment(), hot, "states"));
    try std.testing.expectError(error.DatasetSegmentRetired, store.hold(segment().id, hold));
    try std.testing.expectError(error.DatasetDependencyPending, store.discardOne(segment().id, hot));
    try store.releaseHold(segment().id, hold);
    try std.testing.expectError(error.DatasetReadersActive, store.discardOne(segment().id, hot));
    try store.release(pin);
    try std.testing.expectEqual(segment().id, (try store.nextDiscard(null)).?);
    try std.testing.expect(try store.discardOne(segment().id, hot));
    try std.testing.expectEqual(@as(?t.Id, null), try store.nextDiscard(null));
    const reopened = catalog(&memory, &clock);
    try std.testing.expectEqual(@as(?t.Id, null), try reopened.nextDiscard(null));
    try std.testing.expectError(error.DatasetSegmentRetired, reopened.publish(segment(), hot, "states"));
}

test "retained dataset complete reads traverse bounded files and byte-limited pages resume without loss" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const cold = location(&memory, true);
    const def = definition(.summary);
    try store.create(def);
    const shared = @import("antfly_local_sources");
    const series = shared.storage_retained_series;
    const export_page = @import("closure_export.zig");
    const spec: series.Spec = .{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    const cut: export_page.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    var first_segment: t.Id = undefined;
    var frontier = t.rowKey(0, @splat(0));
    var first_bytes: usize = 0;
    for (0..33) |i| {
        const row: series.Bucket = .{ .start_ns = @intCast(i * 60), .dimensions = "[]", .rows = 1, .metrics = &metrics };
        const page = (try export_page.encode(a, def, spec, cut, frontier, &.{row}, false, cold)).?;
        defer export_page.deinit(a, page);
        if (i == 0) {
            first_bytes = page.bytes.len;
            first_segment = page.segment.id;
        }
        try export_page.publish(store, spec, cut, page, cold);
        frontier = (try page.segment.span()).end;
    }
    const manifest = try store.sealClosedExport(spec, cut, frontier);
    const receipt = try manifest.receipt();
    const Resolver = struct {
        loc: api.Location,
        pub fn location(self: @This(), _: t.Id) !api.Location {
            return self.loc;
        }
    };
    const resolver: Resolver = .{ .loc = cold };
    var single = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "60" }, .{ .dimensions_json = "[]" }, cut, receipt, resolver);
    defer single.deinit();
    try std.testing.expectEqual(@as(usize, 1), single.rows.len);
    for ([_]?[]const u8{ null, "[]" }) |dimensions| {
        var complete = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "1980" }, .{ .segments = 2, .dimensions_json = dimensions }, cut, receipt, resolver);
        defer complete.deinit();
        try std.testing.expectEqual(@as(usize, 33), complete.rows.len);
        try std.testing.expect(complete.next == null);
        try std.testing.expectError(error.DatasetPageBudgetExceeded, api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "1980" }, .{ .rows = 32, .dimensions_json = dimensions }, cut, receipt, resolver));
        var page = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "120" }, .{ .rows = 2, .bytes = first_bytes * 2 - 1, .page = true, .dimensions_json = dimensions }, cut, receipt, resolver);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expect(page.next != null);
        var next = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "120" }, .{ .rows = 2, .bytes = first_bytes * 2 - 1, .page = true, .dimensions_json = dimensions, .after = page.next }, cut, receipt, resolver);
        defer next.deinit();
        try std.testing.expectEqual(@as(usize, 1), next.rows.len);
        try std.testing.expectEqual(@as(i128, 60), next.rows[0].start_ns);
        try std.testing.expect(next.next == null);
    }
    const hold: api.catalog.Dependency = .{ .id = @splat(8), .consumer = @splat(9), .consumer_generation = 1 };
    try store.hold(first_segment, hold);
    clock.now = def.retention_ns + 10000;
    const first_work = (try store.nextMaintenanceExpiry()).?;
    try std.testing.expectEqual(first_segment, first_work.segment);
    try std.testing.expectError(error.DatasetDependencyPending, store.collectExpired(first_work.segment, cold));
    const reopened = catalog(&memory, &clock);
    const second_work = (try reopened.nextMaintenanceExpiry()).?;
    try std.testing.expect(!std.mem.eql(u8, &first_work.segment, &second_work.segment));
}

test "retained dataset durable maintenance seeks past held work across catalog reopen" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    const def = definition(.summary);
    try store.create(def);
    var second = segment();
    second.id = @splat(5);
    second.coverage = .{ .start_ns = "60", .end_ns = "120" };
    try store.publish(segment(), hot, "states");
    try store.publish(second, hot, "states");
    const hold: api.catalog.Dependency = .{ .id = @splat(8), .consumer = @splat(9), .consumer_generation = 1 };
    try store.hold(segment().id, hold);
    // Each call opens a fresh durable catalog; no process-local cursor survives.
    const reopened = catalog(&memory, &clock);
    try store.beginDiscard(def);
    const selected = (try store.nextMaintenanceDiscard()).?;
    try std.testing.expectEqual(segment().id, selected);
    try std.testing.expectError(error.DatasetDependencyPending, store.discardOne(selected, hot));
    try std.testing.expectEqual(second.id, (try reopened.nextMaintenanceDiscard()).?);
    try std.testing.expect(try reopened.discardOne(second.id, hot));
    try std.testing.expectEqual(@as(?t.Id, null), try reopened.nextMaintenanceDiscard());
    try std.testing.expect((try reopened.discardReceipt()) == null);
    try store.releaseHold(segment().id, hold);
    try std.testing.expectEqual(segment().id, (try reopened.nextMaintenanceDiscard()).?);
    try std.testing.expect(try reopened.discardOne(segment().id, hot));
    // Wrap, then observe the empty catalog and publish terminal evidence.
    try std.testing.expectEqual(@as(?t.Id, null), try reopened.nextMaintenanceDiscard());
    try std.testing.expectEqual(@as(?t.Id, null), try reopened.nextMaintenanceDiscard());
    const receipt = (try reopened.discardReceipt()).?;
    try std.testing.expectEqual(receipt, (try store.discardReceipt()).?);
    try std.testing.expectError(error.DatasetSegmentRetired, store.publish(segment(), hot, "states"));
}

test "retained dataset tuple ranges decode spanning files once within the exact byte budget" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const cold = location(&memory, true);
    const def = definition(.summary);
    try store.create(def);
    const series = @import("antfly_local_sources").storage_retained_series;
    const export_page = @import("closure_export.zig");
    const spec: series.Spec = .{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    const cut: export_page.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    var rows: [33]series.Bucket = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .start_ns = @intCast(i * 60), .dimensions = "[]", .rows = 1, .metrics = &metrics };
    const page = (try export_page.encode(a, def, spec, cut, t.rowKey(0, @splat(0)), &rows, false, cold)).?;
    defer export_page.deinit(a, page);
    try export_page.publish(store, spec, cut, page, cold);
    const manifest = try store.sealClosedExport(spec, cut, (try page.segment.span()).end);
    const Resolver = struct {
        loc: api.Location,
        pub fn location(self: @This(), _: t.Id) !api.Location {
            return self.loc;
        }
    };
    var result = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "1980" }, .{ .dimensions_json = "[]", .bytes = page.bytes.len }, cut, try manifest.receipt(), Resolver{ .loc = cold });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 33), result.rows.len);
    try std.testing.expectEqual(page.bytes.len, result.bytes_read);
    var missing = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "1980" }, .{ .dimensions_json = "[404]", .segments = 1, .page = true, .bytes = page.bytes.len }, cut, try manifest.receipt(), Resolver{ .loc = cold });
    defer missing.deinit();
    try std.testing.expectEqual(@as(usize, 0), missing.rows.len);
    try std.testing.expect(missing.next == null);
    try std.testing.expectEqual(page.bytes.len, missing.bytes_read);
}
test "retained dataset tuple metadata work scales with files rather than bucket count" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const cold = location(&memory, true);
    const def = definition(.summary);
    try store.create(def);
    const series = @import("antfly_local_sources").storage_retained_series;
    const export_page = @import("closure_export.zig");
    const spec: series.Spec = .{ .id = def.id, .definition = try def.fingerprint(a), .generation = 1, .source_partition = segment().source.partition, .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    const cut: export_page.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    var rows: [512]series.Bucket = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .start_ns = @intCast(i * 60), .dimensions = "[]", .rows = 1, .metrics = &metrics };
    const page = (try export_page.encode(a, def, spec, cut, t.rowKey(0, @splat(0)), &rows, false, cold)).?;
    defer export_page.deinit(a, page);
    try export_page.publish(store, spec, cut, page, cold);
    const manifest = try store.sealClosedExport(spec, cut, (try page.segment.span()).end);
    const Resolver = struct {
        loc: api.Location,
        pub fn location(self: @This(), _: t.Id) !api.Location {
            return self.loc;
        }
    };
    _ = try store.admitRetentionClock(spec, 1000);
    memory.resetOperationCount();
    var result = try api.summary_read.readClosed(a, store, def, spec, .{ .start_ns = "0", .end_ns = "30720" }, .{ .dimensions_json = "[]", .bytes = page.bytes.len }, cut, try manifest.receipt(), Resolver{ .loc = cold });
    defer result.deinit();
    const operations = memory.operationCount();
    std.debug.print("RETAINED_TUPLE buckets=512 files=1 provider_operations={d}\n", .{operations});
    try std.testing.expect(operations < 64);
    try std.testing.expectEqual(@as(usize, 512), result.rows.len);
    try std.testing.expectEqual(page.bytes.len, result.bytes_read);
}

test "retained dataset abandoned uploads are fenced before discard acknowledges collection" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    try store.create(definition(.records));
    var fault = objects.scripted_fault.Client.init(a, memory.client());
    defer fault.deinit();
    var failed_location = hot;
    failed_location.client = fault.client();
    fault.commitNextPutThenFail(error.DatasetCopyFault);
    try std.testing.expectError(error.DatasetCopyFault, store.publish(segment(), failed_location, "states"));
    try std.testing.expectError(error.DatasetSegmentNotFound, store.getSegment(segment().id));
    var client = memory.client();
    var blobs = try client.listObjects(hot.bucket, .{ .prefix = "segments/", .recursive = true });
    defer blobs.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), blobs.entries.len);
    const key = blobs.entries[0].key;
    try std.testing.expect(!try store.reclaimUpload(hot));
    try store.beginDiscard(definition(.records));
    try std.testing.expectError(error.DatasetDependencyPending, store.nextDiscard(null));
    try std.testing.expect(try store.reclaimUpload(hot));
    try std.testing.expect(!try store.reclaimUpload(hot));
    var tombstone = try client.getObject(hot.bucket, key, .{});
    defer tombstone.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), tombstone.body.len);
    try std.testing.expectError(error.PreconditionFailed, client.putObject(hot.bucket, key, "states", .{ .if_none_match = true }));
    try std.testing.expectEqual(@as(?t.Id, null), try store.nextDiscard(null));
    try std.testing.expect((try store.discardReceipt()) != null);
}

test "retained dataset terminal empty proof seals and serves a partial bucket suffix" {
    const local = @import("antfly_local_sources");
    const series = local.storage_retained_series;
    const exported = @import("closure_export.zig");
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const cold = location(&memory, true);
    const def = definition(.summary);
    try store.create(def);
    const fingerprint = try def.fingerprint(a);
    const spec: series.Spec = .{ .id = def.id, .generation = 1, .definition = fingerprint, .source_partition = @splat(5), .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = def.retention_ns, .laws = &.{.count} };
    var metrics = [_]series.Metric{.{ .count = 1 }};
    const row: series.Bucket = .{ .start_ns = 1200, .dimensions = "[]", .rows = 1, .metrics = &metrics };
    const cut: exported.Cut = .{ .closure = @splat(8), .sequence = 20, .complete_from_ns = 0, .admitted_ns = 1000 };
    const page = (try exported.encode(a, def, spec, cut, t.rowKey(0, @splat(0)), &.{row}, true, cold)).?;
    defer exported.deinit(a, page);
    try exported.publish(store, spec, cut, page, cold);
    const manifest = try store.sealClosedExport(spec, cut, t.rowKey(1260, @splat(0)));
    try std.testing.expect(manifest.empty_from != null);
    const reopened = (try store.getClosedExport(spec, cut)).?;
    try std.testing.expectEqualDeep(try manifest.receipt(), try reopened.receipt());
    const Resolver = struct {
        value: api.Location,
        pub fn location(self: @This(), _: t.Id) !api.Location {
            return self.value;
        }
    };
    var result = try @import("summary_read.zig").readClosed(a, store, def, spec, .{ .start_ns = "1200", .end_ns = "1320" }, .{}, cut, try manifest.receipt(), Resolver{ .value = cold });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
}

test "retained dataset abandoned uploads collect independently by location" {
    var memory = objects.memory.MemoryClient.init(a);
    defer memory.deinit();
    try setup(&memory);
    var clock: FakeClock = .{};
    const store = catalog(&memory, &clock);
    const hot = location(&memory, false);
    const cold = location(&memory, true);
    try store.create(definition(.records));
    var first_fault = objects.scripted_fault.Client.init(a, memory.client());
    defer first_fault.deinit();
    var failed_hot = hot;
    failed_hot.client = first_fault.client();
    first_fault.commitNextPutThenFail(error.DatasetCopyFault);
    try std.testing.expectError(error.DatasetCopyFault, store.publish(segment(), failed_hot, "states"));
    var second_fault = objects.scripted_fault.Client.init(a, memory.client());
    defer second_fault.deinit();
    var failed_cold = cold;
    failed_cold.client = second_fault.client();
    var second = segment();
    second.id = @splat(24);
    second.location = cold.id;
    second.access = cold.access;
    clock.now += 1;
    second_fault.commitNextPutThenFail(error.DatasetCopyFault);
    try std.testing.expectError(error.DatasetCopyFault, store.publish(second, failed_cold, "states"));
    clock.now += 6 * std.time.ns_per_min;
    first_fault.failNextPutBefore(error.DatasetCopyFault);
    try std.testing.expectError(error.DatasetCopyFault, store.reclaimUpload(failed_hot));
    try std.testing.expect(try store.reclaimUpload(cold));
    try std.testing.expect(!try store.reclaimUpload(cold));
    try store.beginDiscard(definition(.records));
    try std.testing.expectError(error.DatasetDependencyPending, store.nextDiscard(null));
    try std.testing.expect(try store.reclaimUpload(hot));
    try std.testing.expectEqual(@as(?t.Id, null), try store.nextDiscard(null));
    try std.testing.expect(try store.discardReceipt() != null);
}
