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

//! Explicit table transaction origins cross only reviewed callback contracts.
const std = @import("std");
const local = @import("antfly_local_sources");
const types = local.storage_db_selected_root.db.types;
const contract = local.api_distributed_txn_contract;
const Source = local.api_table_write_source.TableWriteSource;
pub fn run(a: std.mem.Allocator) !void {
    const Fake = struct {
        calls: usize = 0,
        expected: @FieldType(types.BatchRequest, "source_producer") = .legacy_cdc,
        accepted: bool = false,
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.BatchRequest) !?void {
            return error.TestUnexpectedResult;
        }
        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8, context: contract.PreDecisionContext) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(self.expected, context.source_producer);
            return {};
        }
        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: types.TxnId, _: u64, input: types.TransactionIntentRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(input.completion_legacy_producer);
            return {};
        }
        fn commit(ptr: *anyopaque, _: std.mem.Allocator, tables: []const contract.TableCommitRequest, _: types.SyncLevel) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            for (tables) |table| try std.testing.expectEqual(self.expected, table.source_producer);
            return if (self.accepted) .{ .committed = .{ .participant_count = 1 } } else null;
        }
    };
    var fake: Fake = .{};
    var source: Source = .{ .ptr = &fake, .vtable = &.{ .batch = Fake.batch, .commit_batch = Fake.commit } };
    const request = [_]contract.TableCommitRequest{.{ .table_name = "docs", .source_producer = .legacy_cdc, .writes = &.{.{ .key = "row", .value = "{}" }} }};
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, source.commitBatch(a, &request, .write));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    source.supports_transaction_producers = true;
    // A reviewed provider's null/unsupported response is never permission to
    // retry a classified transaction through an unlabelled lower interface.
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, source.commitBatchWithCancellation(a, &request, .write, .none));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    fake.accepted = true;
    const result = (try source.commitBatch(a, &request, .write)).?;
    try std.testing.expect(result == .committed);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, source.commitBatch(a, &.{ request[0], .{ .table_name = "other", .source_producer = .ordinary } }, .write));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, source.commitTransactionWithId(a, @splat(1), 10, &request, .write));
    // Default, older callers retain the established unsupported/null contract.
    var old = request[0];
    old.source_producer = .unlabelled;
    source.supports_transaction_producers = false;
    fake.expected = .unlabelled;
    fake.accepted = false;
    try std.testing.expect(try source.commitBatch(a, &.{old}, .write) == null);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    fake.expected = .legacy_cdc;
    var phases: Source = .{ .ptr = &fake, .vtable = &.{ .batch = Fake.batch, .txn_begin_group_local_with_pre_decision_context = Fake.begin, .txn_prepare_group_local = Fake.prepare } };
    const before = fake.calls;
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, phases.txnBeginGroupLocalWithPreDecisionContext(a, 1, "docs", @splat(1), 10, 1, true, &.{"participant"}, .{ .source_producer = .legacy_cdc }));
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, phases.txnPrepareGroupLocal(a, 1, "docs", @splat(1), 1, .{ .completion_legacy_producer = true }));
    try std.testing.expectEqual(before, fake.calls);
    phases.supports_transaction_producer_phases = true;
    try std.testing.expect((try phases.txnBeginGroupLocalWithPreDecisionContext(a, 1, "docs", @splat(1), 10, 1, true, &.{"participant"}, .{ .source_producer = .legacy_cdc })) != null);
    try std.testing.expect((try phases.txnPrepareGroupLocal(a, 1, "docs", @splat(1), 1, .{ .completion_legacy_producer = true })) != null);
    try std.testing.expectEqual(before + 2, fake.calls);
    const Wire = struct {
        tables: []const contract.TableCommitRequest,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            try local.storage_db_relational_integrity_json.write(self.tables, stream);
        }
    };
    const old_bytes = try std.json.Stringify.valueAlloc(a, Wire{ .tables = &.{old} }, .{});
    defer a.free(old_bytes);
    try std.testing.expect(std.mem.indexOf(u8, old_bytes, "source_producer") == null);
    var restored = try std.json.parseFromSlice([]const contract.TableCommitRequest, a, old_bytes, .{ .allocate = .alloc_always });
    defer restored.deinit();
    try std.testing.expectEqual(@as(@FieldType(types.BatchRequest, "source_producer"), .unlabelled), restored.value[0].source_producer);
    const labelled_bytes = try std.json.Stringify.valueAlloc(a, Wire{ .tables = &request }, .{});
    defer a.free(labelled_bytes);
    try std.testing.expect(std.mem.indexOf(u8, labelled_bytes, "\"source_producer\":\"legacy_cdc\"") != null);
}
