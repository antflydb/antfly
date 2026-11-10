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
    fault.commitNextDeleteThenFail(error.DatasetDeleteFault);
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
