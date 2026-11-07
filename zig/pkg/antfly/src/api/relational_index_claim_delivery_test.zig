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
const local = @import("antfly_local_sources");
const jobs = local.storage_db_relational_index_owner_job;
const claims = jobs.private_claims;
const Source = claims.SourceStream;
const delivery = claims.delivery;
const Cohort = delivery.Cohort;
const Target = delivery.Target;
const View = delivery.View;
const Plan = delivery.Plan;
const progress = local.storage_db_relational_index_publication_progress;
const Allocator = std.mem.Allocator;
const Digest = [32]u8;

fn testOwner(ordinal: u16, group_id: u64) progress.Owner {
    return .{ .plan_id = @splat(1), .plan_digest = @splat(2), .ordinal = ordinal, .group_id = group_id, .namespace = .{ .table_id = 101, .shard_id = group_id, .range_id = group_id }, .authority = .native, .fence_digest = @splat(3) };
}

fn testBinding(owner: progress.Owner) jobs.Binding {
    return .{
        .id = owner.plan_id,
        .plan_digest = owner.plan_digest,
        .owner_digest = owner.fence_digest,
        .ordinal = owner.ordinal,
        .authority = owner.authority,
        .phase = .reserved,
        .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .admission_epoch = 1, .owner_group_id = owner.group_id, .peer_group_id = owner.group_id, .namespace = owner.namespace, .catalog_digest = @splat(4) },
        .integrity_before = @splat(4),
        .integrity_reserved = @splat(5),
        .integrity_candidate = @splat(6),
        .schema_before = @splat(7),
        .physical_before = null,
        .physical_reserved = .{ .revision = 2, .schema_version = 1, .blob_digest = @splat(8) },
        .physical_candidate = .{ .revision = 2, .schema_version = 2, .blob_digest = @splat(9) },
        .index = .{ .generation = 2, .slot = 1 },
        .inputs_digest = @splat(10),
    };
}

fn testHeader(prepared: *const Source.PreparedSource, binding: jobs.Binding) !Source.Header {
    var raw = prepared.header;
    std.mem.writeInt(u64, raw[104..112], 1, .little);
    const position: local.storage_db_receipt_position.Position = switch (binding.authority) {
        .native => .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 4 } },
        .raft => .{ .raft = .{ .term = 2, .index = 4 } },
    };
    @memcpy(raw[112..145], &try position.encode());
    std.crypto.hash.Blake3.hash(raw[0..288], raw[288..320], .{});
    return Source.Header.decode(&raw, binding, 1);
}

fn cohortAllocation(alloc: Allocator) !void {
    var cohort = try Cohort.init(alloc, &.{
        .{ .owner = testOwner(1, 12), .start = "\x80" },
        .{ .owner = testOwner(0, 11), .end = "\x80" },
    });
    defer cohort.deinit();
    var comparisons: usize = 0;
    try std.testing.expectEqual(@as(u16, 0), try cohort.route(@splat(0), &comparisons));
    try std.testing.expectEqual(@as(u16, 1), try cohort.route(@splat(255), &comparisons));
    try std.testing.expectEqual(@as(u16, 1), try cohort.route(@splat(128), &comparisons));
    try std.testing.expect(comparisons <= 6);
    try std.testing.expectEqual(@as(u16, 0), cohort.by_ordinal[0]);
    try std.testing.expectEqual(@as(u16, 1), cohort.by_ordinal[1]);
}

test "relational index publication claim delivery cohort owns exact membership and logarithmic routing" {
    try cohortAllocation(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, cohortAllocation, .{});
    const owner = testOwner(0, 11);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Cohort.init(std.testing.allocator, &.{
        .{ .owner = owner }, .{ .owner = testOwner(1, 12) },
    }));
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Cohort.init(std.testing.allocator, &.{
        .{ .owner = owner, .end = "a" }, .{ .owner = testOwner(1, 12), .start = "b" },
    }));
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Cohort.init(std.testing.allocator, &.{
        .{ .owner = owner, .end = "b" }, .{ .owner = testOwner(1, 12), .start = "a" },
    }));
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Cohort.init(std.testing.allocator, &.{
        .{ .owner = owner, .end = "a" }, .{ .owner = testOwner(1, 11), .start = "a" },
    }));
}

test "relational index publication claim delivery splits actual target bytes and proves exact coverage" {
    try splitDelivery(.native);
    try splitDelivery(.raft);
}

fn splitDelivery(authority: progress.Authority) !void {
    const alloc = std.testing.allocator;
    var owner = testOwner(0, 11);
    owner.authority = authority;
    var cohort = try Cohort.init(alloc, &.{.{ .owner = owner }});
    defer cohort.deinit();
    const binding = testBinding(cohort.targets[0].owner);
    const scope: Source.Scope = .{ .table_name = "items", .generation = @splat(1), .schema_version = 2, .tuple_fingerprint = @splat(2), .nulls_not_distinct = false };
    const tuple = try alloc.alloc(u8, claims.max_bytes / 2 - 64);
    defer alloc.free(tuple);
    @memset(tuple, 'x');
    const other = try alloc.dupe(u8, tuple);
    defer alloc.free(other);
    other[other.len - 1] = 'y';
    const rows = [_]Source.Row{ .{ .key = "a", .tuple = tuple }, .{ .key = "b", .tuple = other } };
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, Source.PreparedSource.init(alloc, binding, scope, @splat(9), .ready, &.{ rows[0], rows[0] }));
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, Source.PreparedSource.init(alloc, binding, scope, @splat(9), .ready, &.{ rows[1], rows[0] }));
    var prepared = try Source.PreparedSource.init(alloc, binding, scope, @splat(9), .ready, &rows);
    defer prepared.deinit();
    const view: View = .{ .header = try testHeader(&prepared, binding), .scope = scope, .rows = &rows };
    const plan = try Plan.prepare(&cohort, binding, view);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, Plan.prepareControlled(&cohort, binding, view, .{ .cancellation = .fromAtomic(&canceled) }));
    try std.testing.expectError(error.DeadlineExceeded, Plan.prepareControlled(&cohort, binding, view, .{ .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 2), plan.group_count);
    try std.testing.expect(plan.groups[0].bytes <= claims.max_bytes);
    try std.testing.expect(plan.groups[0].bytes + plan.groups[1].bytes > claims.max_bytes);
    try std.testing.expectEqual(plan.digest, try plan.coverage(&.{ 1, 0 }));
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, plan.coverage(&.{0}));
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, plan.coverage(&.{ 0, 0 }));
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, plan.coverage(&.{ 0, 2 }));
    try plan.requireTarget(&cohort, 0, binding);
    const publication = @import("../metadata/relational_index_publication.zig");
    const control = local.storage_db_relational_index_owner_control;
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    const native_binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7 };
    var delivered: [2]@import("../metadata/relational_index_source_delivery.zig").Delivered = undefined;
    for (0..plan.group_count) |i| {
        const command: control.ClaimCommand = .{ .binding = binding, .claims = try plan.groupClaims(i) };
        const outcome: control.DeliveredClaims = .{ .binding = binding, .claims_digest = try command.digest(), .position = switch (authority) {
            .native => .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 8 + i } },
            .raft => .{ .raft = .{ .term = 3, .index = 8 + i } },
        }, .duplicate = false, .claims_added = 1 };
        const signed: publication.SignedClaimDelivery = .{ .outcome = outcome, .proof = try (publication.RootProof{
            .identity = .{ .metadata_incarnation = native_binding.metadata_incarnation, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
            .reporter_incarnation = 4,
            .authority = switch (authority) {
                .native => .{ .native = try native_binding.digest() },
                .raft => .{ .raft = 17 },
            },
            .root_generation = 9,
            .signature = undefined,
        }).sign(root, try outcome.deliveryDigest()) };
        delivered[i] = .{ .index = @intCast(i), .signed = signed };
        try signed.verifyPartition(&plan, &cohort, i);
        // Both byte-split partitions have the same target. One target's valid
        // signature cannot acknowledge a different subset from that target.
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, signed.verifyPartition(&plan, &cohort, 1 - i));
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, signed.verifyPartition(&plan, &cohort, plan.group_count));
    }
    const source_delivery = @import("../metadata/relational_index_source_delivery.zig");
    var subset = try delivery.Subset.init(alloc, owner, 1, cohort.targets);
    defer subset.deinit();
    var read: control.SourceRead = .{ .binding = binding, .batch = try Source.OwnedBatch.takePayload(alloc, binding, view.header, try alloc.dupe(u8, prepared.payload)) };
    defer read.deinit();
    const closure: source_delivery.Request = .{ .ordinal = 1, .batch_digest = view.header.digest(), .targets = .{ .items = &delivered } };
    var complete = try source_delivery.prepare(alloc, &read, &subset, closure, .{});
    defer complete.deinit();
    _ = try complete.data.digest();
    {
        // Qualify bounded inventory aggregation, not live owner enrollment;
        // the metadata-store integration tests verify that independently.
        const journal = @import("../metadata/relational_index_delivery_journal.zig");
        const Reader = struct {
            owner_key: []const u8,
            owner_bytes: [progress.OwnerState.encoded_len]u8,
            reads: usize = 0,
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                self.reads += 1;
                if (std.mem.eql(u8, key, self.owner_key)) return &self.owner_bytes;
                return error.NotFound;
            }
        };
        const Authority = struct {
            pub fn verifyDelivery(_: @This(), _: anytype, _: u64, signed: source_delivery.Signed) !Digest {
                try signed.verify();
                return signed.proof.commitment(try signed.data.digest());
            }
            pub fn verifyConsumption(_: @This(), _: anytype, _: u64, _: jobs.Binding, _: journal.Consumed) !void {
                return error.InvalidPrivateIndexClaimReceipt;
            }
        };
        var state: progress.OwnerState = .{ .owner = owner, .flags = 3, .position = switch (authority) {
            .native => .{ .native = .{ .namespace = owner.namespace, .sequence = 3 } },
            .raft => .{ .raft = .{ .term = 2, .index = 3 } },
        } };
        state.receipts[0] = @splat(1);
        state.receipts[1] = @splat(2);
        var key_buf: [192]u8 = undefined;
        var reader: Reader = .{ .owner_key = try publication.ownerKey(&key_buf, 13, owner.namespace.table_id, owner.ordinal), .owner_bytes = try state.encode() };
        const signed: source_delivery.Signed = .{ .data = complete.data, .proof = try delivered[0].signed.proof.sign(root, try complete.data.digest()) };
        var accepted = try journal.prepare(alloc, &reader, 13, .{ .signed = signed }, Authority{});
        defer accepted.deinit();
        try std.testing.expectEqual(@as(usize, 1), accepted.targets.len);
        try std.testing.expectEqual(@as(usize, 3), reader.reads);
        const inventory = (try journal.TargetRecord.decode(&accepted.targets[0].bytes)).inventory;
        try std.testing.expectEqual(@as(u64, 2), inventory.rows);
        try std.testing.expectEqual(@as(u64, 1), inventory.batches);
        var expected: Digest = @splat(0);
        for (complete.data.parts.items) |part| for (&expected, part.inventory_xor) |*byte, contribution| {
            byte.* ^= contribution;
        };
        try std.testing.expectEqual(expected, inventory.inventory_xor);
        var retained = try journal.SourceRecord.decode(alloc, accepted.source_bytes.?);
        defer retained.deinit();
        const record: Source.Consumption = .{ .accepted = accepted.head.accepted(), .header = view.header, .position = switch (authority) {
            .native => .{ .native = .{ .namespace = owner.namespace, .sequence = 10 } },
            .raft => .{ .raft = .{ .term = 3, .index = 10 } },
        }, .previous_digest = @splat(0) };
        try record.requireFor(binding);
        const consumed = try journal.Consumed.fromRecord(record, try signed.proof.sign(root, try record.prefix().digest()));
        try consumed.verify();
        const completed = try journal.SourceRecord.completed(alloc, accepted.source_bytes.?, consumed);
        defer alloc.free(completed);
        const prefix_ = try journal.SourceRecord.peek(alloc, completed);
        try std.testing.expectEqualDeep(record.position, prefix_.completion.?.position);
        var wrong_cut = consumed;
        wrong_cut.position = if (authority == .raft) .{ .raft = .{ .term = 1, .index = 10 } } else .{ .native = .{ .namespace = owner.namespace, .sequence = 4 } };
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, wrong_cut.requireSource(binding, view.header));
        wrong_cut = consumed;
        wrong_cut.position = if (authority == .raft) .{ .native = .{ .namespace = owner.namespace, .sequence = 10 } } else .{ .raft = .{ .term = 3, .index = 10 } };
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, wrong_cut.digest());
    }
    var incomplete = closure;
    incomplete.targets.items = delivered[0..1];
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, source_delivery.prepare(alloc, &read, &subset, incomplete, .{}));
    var duplicate = delivered;
    duplicate[1] = duplicate[0];
    incomplete.targets.items = &duplicate;
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, source_delivery.prepare(alloc, &read, &subset, incomplete, .{}));
    var wrong = binding;
    wrong.integrity_reserved[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, plan.requireTarget(&cohort, 0, wrong));
    wrong = binding;
    wrong.inputs_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, plan.requireTarget(&cohort, 0, wrong));
    wrong = binding;
    wrong.schema_before[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, plan.requireTarget(&cohort, 0, wrong));
    var corrupt = view;
    corrupt.rows = &.{ .{ .key = "changed", .tuple = tuple }, rows[1] };
    try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, Plan.prepare(&cohort, binding, corrupt));
    var failed = try Source.PreparedSource.init(alloc, binding, scope, @splat(9), .failed, &.{});
    defer failed.deinit();
    try std.testing.expectError(error.InvalidRelationalIndexProgress, Plan.prepare(&cohort, binding, .{ .header = try testHeader(&failed, binding), .scope = scope, .rows = &.{} }));
    const limit = claims.max_bytes - claims.claim_entry_overhead - scope.table_name.len - 1;
    try std.testing.expectEqual(claims.max_bytes, try Source.targetBytes(scope.table_name.len, 1, limit));
    try std.testing.expectError(error.RelationalIndexKeyTooLarge, Source.targetBytes(scope.table_name.len, 1, limit + 1));
}

test "relational index publication claim delivery maximum cohort bounds routing work" {
    const alloc = std.testing.allocator;
    const input = try alloc.alloc(Target, progress.max_owners);
    defer alloc.free(input);
    const cuts = try alloc.alloc([2]u8, progress.max_owners);
    defer alloc.free(cuts);
    for (cuts, 0..) |*cut, i| std.mem.writeInt(u16, cut, @intCast(i), .big);
    for (input, 0..) |*target, i| target.* = .{
        .owner = testOwner(@intCast(i), i + 1),
        .start = if (i == 0) "" else &cuts[i],
        .end = if (i + 1 == input.len) "" else &cuts[i + 1],
    };
    var cohort = try Cohort.init(alloc, input);
    defer cohort.deinit();
    // Owned ranges remain valid when the producer's input buffers change.
    @memset(std.mem.sliceAsBytes(cuts), 255);
    var comparisons: usize = 0;
    for (0..progress.max_owners) |i| {
        var key: Digest = @splat(0);
        std.mem.writeInt(u16, key[0..2], @intCast(i), .big);
        try std.testing.expectEqual(@as(u16, @intCast(i)), try cohort.route(key, &comparisons));
    }
    try std.testing.expect(comparisons <= progress.max_owners * 13);
}

fn subsetAllocation(alloc: Allocator) !void {
    const source_owner = testOwner(3999, 1000);
    var subset = try delivery.Subset.init(alloc, source_owner, 4096, &.{
        .{ .owner = testOwner(4095, 12), .start = "\x80" },
        .{ .owner = testOwner(17, 11), .end = "\x80" },
    });
    defer subset.deinit();
    try subset.requireBinding(testBinding(source_owner));
    try subset.requireBinding(testBinding(testOwner(17, 11)));
    var comparisons: usize = 0;
    try std.testing.expectEqual(@as(u16, 0), try subset.route(@splat(0), &comparisons));
    try std.testing.expectEqual(@as(u16, 1), try subset.route(@splat(255), &comparisons));
    try std.testing.expect(comparisons <= 4);
}

test "relational index publication touched destinations preserve sparse ordinals and reject missing coverage" {
    try subsetAllocation(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, subsetAllocation, .{});
    const source_owner = testOwner(3999, 1000);
    var subset = try delivery.Subset.init(std.testing.allocator, source_owner, 4096, &.{.{ .owner = testOwner(17, 11), .end = "\x80" }});
    defer subset.deinit();
    var comparisons: usize = 0;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, subset.route(@splat(255), &comparisons));
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, subset.requireBinding(testBinding(testOwner(18, 12))));
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, delivery.Subset.init(std.testing.allocator, source_owner, 4096, &.{
        .{ .owner = testOwner(17, 11), .end = "\x90" }, .{ .owner = testOwner(4095, 12), .start = "\x80" },
    }));
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, delivery.Subset.init(std.testing.allocator, source_owner, 4096, &.{.{ .owner = testOwner(17, 1000) }}));
}

test "relational index publication routing bounds preserve all fixed-key prefix comparisons" {
    var bound: [256]u8 = @splat(127);
    for (0..256) |last| {
        var key: Digest = @splat(127);
        key[31] = @intCast(last);
        for (0..bound.len + 1) |len| {
            try std.testing.expectEqual(std.mem.order(u8, &key, bound[0..len]), std.mem.order(u8, &key, delivery.routingBound(bound[0..len])));
            try std.testing.expect(delivery.routingBound(bound[0..len]).len <= 33);
        }
    }
}

/// Serialization-only upper bound, deliberately not an authenticated delivery.
/// Maximize fixed-width values (including invalid ordinals/masks) so this bound
/// covers every valid statement without relaxing a single producer validation.
fn widestJson(comptime T: type, value: T) T {
    switch (@typeInfo(T)) {
        .int => return std.math.maxInt(T),
        .bool => return false,
        .array => |info| {
            var result: T = undefined;
            for (&result) |*item| item.* = widestJson(info.child, std.mem.zeroes(info.child));
            return result;
        },
        .@"struct" => |info| {
            var result = value;
            inline for (info.field_names) |name| @field(result, name) = widestJson(@FieldType(T, name), @field(value, name));
            return result;
        },
        .optional => |info| return if (@typeInfo(info.child) == .@"struct") widestJson(info.child, value orelse std.mem.zeroes(info.child)) else value,
        .@"union" => |info| {
            if (info.tag_type == null) return value;
            inline for (info.field_names) |name| if (std.mem.eql(u8, @tagName(std.meta.activeTag(value)), name)) return @unionInit(T, name, widestJson(@FieldType(T, name), @field(value, name)));
            unreachable;
        },
        else => return value,
    }
}

test "relational index publication maximum typed delivery footprint fits its transport cap" {
    const complete = @import("../metadata/relational_index_source_delivery.zig");
    const journal = @import("../metadata/relational_index_delivery_journal.zig");
    const publication = @import("../metadata/relational_index_publication.zig");
    const binding = testBinding(testOwner(0, 1));
    const proof: publication.RootProof = .{ .identity = .{ .metadata_incarnation = @splat(1), .node_id = 1, .store_id = 1, .root_incarnation = 1, .public_key = @splat(1) }, .reporter_incarnation = 1, .authority = .{ .native = @splat(1) }, .root_generation = 1, .signature = @splat(1) };
    var parts: [complete.max_parts]complete.Part = undefined;
    for (&parts) |*part| part.* = widestJson(complete.Part, .{ .index = 0, .rows = 1, .row_mask = @splat(0), .inventory_xor = @splat(1), .target = .{ .outcome = .{ .binding = binding, .claims_digest = @splat(1), .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 1 } }, .duplicate = false, .claims_added = 1 }, .proof = proof } });
    const signed = widestJson(complete.Signed, .{ .data = .{ .source = binding, .header = .{ .bytes = @splat(0) }, .routes_digest = @splat(1), .parts = .{ .items = &parts } }, .proof = proof });
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, signed, .{});
    defer std.testing.allocator.free(bytes);
    const command: journal.Request = .{ .signed = signed, .consumed = widestJson(journal.Consumed, .{ .metadata_incarnation = @splat(1), .binding_digest = @splat(1), .ordinal = 1, .batch_digest = @splat(1), .decision_digest = @splat(1), .rows = 1, .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 1 } }, .proof = proof }) };
    const command_bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, command, .{});
    defer std.testing.allocator.free(command_bytes);
    std.debug.print("typed delivery upper bound: {d} bytes; acceptance {d} bytes\n", .{ bytes.len, command_bytes.len });
    try std.testing.expect(bytes.len <= complete.max_wire_bytes);
    try std.testing.expect(command_bytes.len <= journal.max_command_bytes);
}
