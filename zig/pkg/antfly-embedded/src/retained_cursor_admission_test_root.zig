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

pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const std = @import("std");
const rows = @import("storage/db/relational_rows.zig");
const docstore = @import("storage/docstore.zig");
const schema = @import("storage/schema.zig");
const registry = @import("storage/db/schema_registry.zig");
const mapper = @import("storage/db/document_mapper.zig");
const internal = @import("storage/internal_keys.zig");
const admission = @import("common/read_admission.zig");
const scheduling = @import("common/runnable_scheduler.zig");
const Session = @import("storage/db/relational_read_session.zig").Session;

test "retained relational page admission releases CPU for cold reads and clears failed call borrows" {
    const a = std.testing.allocator;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("retained-cursor-admission");
    defer directory.cleanup();
    const path = directory.path();
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .required = true }} };
    var view: registry.SchemaView = .{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer view.release();
    {
        var store = try docstore.DocStore.open(a, path, .{});
        defer store.close();
        for (0..8) |ordinal| {
            const key = try std.fmt.allocPrint(a, "row:{d}", .{ordinal});
            defer a.free(key);
            const primary = try internal.relationalRowKeyAlloc(a, key);
            defer a.free(primary);
            const json = try std.fmt.allocPrint(a, "{{\"n\":{d}}}", .{ordinal});
            defer a.free(json);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
            defer parsed.deinit();
            const encoded = try mapper.buildRelationalRowValueForSchemaFromParsedAlloc(a, parsed.value, view.tableSchema().*);
            defer a.free(encoded);
            try store.put(primary, encoded);
        }
        try store.owned_lsm_backend.?.backend.flushBufferedWritesWithOptions(.{});
    }

    for ([_]bool{ false, true }) |reject| {
        var store = try docstore.DocStore.open(a, path, .{});
        defer store.close();
        const session = try a.create(Session);
        session.* = .{ .alloc = a, .reader = try rows.Reader.open(a, &store, view, null, .{ .fields = &.{"n"} }, 0), .cancellation = null, .deadline_ns = null };
        defer session.deinit();
        const reader = &session.reader;
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var execution: scheduling.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
        defer execution.finish();
        var budget: Budget = .{ .scheduler = &scheduler, .execution = &execution, .reject_resume = reject };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const result = session.nextTypedPageWithAdmission(a, null, .{ .rows = 2 }, admission.Port.init(&payload));
        if (reject) {
            try std.testing.expectError(error.SchedulerClosed, result);
        } else {
            var page = try result;
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 2), page.rows.len);
            try std.testing.expectEqualStrings("row:0", page.rows[0].key);
            try std.testing.expectEqual(@as(i64, 0), page.rows[0].typed.?.object.get("n").?.integer);
            try std.testing.expect(page.more);
            execution.finish();
            budget.poisoned = true;
            const checks = budget.checks;
            var next_execution: scheduling.Execution = .{};
            try next_execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
            defer next_execution.finish();
            var next_budget: Budget = .{ .scheduler = &scheduler, .execution = &next_execution, .reject_resume = false };
            var next_adapter = admission.Adapter(*Budget){ .budget = &next_budget };
            const next_payload = next_adapter.payload();
            var continuation = try session.nextTypedPageWithAdmission(a, null, .{ .rows = 2 }, admission.Port.init(&next_payload));
            defer continuation.deinit();
            try std.testing.expectEqualStrings("row:2", continuation.rows[0].key);
            try std.testing.expectEqual(@as(i64, 2), continuation.rows[0].typed.?.object.get("n").?.integer);
            try std.testing.expect(next_budget.checks > 0);
            try std.testing.expectEqual(checks, budget.checks);
        }
        try std.testing.expect(budget.waits > 0);
        try std.testing.expect(budget.checks > 0);
        try std.testing.expect(reader.page_admission.control == null);
    }
}

const Budget = struct {
    scheduler: *scheduling.Scheduler,
    execution: *scheduling.Execution,
    reject_resume: bool,
    waits: usize = 0,
    checks: usize = 0,
    poisoned: bool = false,
    pub fn checkpoint(self: *@This()) !void {
        if (self.poisoned) return error.TestPageBorrowEscaped;
        self.checks += 1;
        try self.execution.checkpoint();
    }
    pub fn checkLifetime(self: *@This()) !void {
        if (self.poisoned) return error.TestPageBorrowEscaped;
        try self.execution.checkLifetime();
    }
    pub fn waitForProgress(_: *@This()) !void {
        return error.ExecutionWaitUnsupported;
    }
    pub fn executionBoundary(self: *@This()) ?@import("common/execution_wait.zig").Boundary {
        return .{ .ptr = self, .begin = begin, .end = end };
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.execution.park(.resource_blocked);
        self.waits += 1;
        try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
        var control: scheduling.Execution = .{};
        try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
        defer control.finish();
        try control.checkpoint();
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.reject_resume) self.scheduler.close();
        try self.execution.enter(try self.execution.retainedClass());
    }
};

test "retained relational page admission creation borrows only the opening execution and unwinds rejected storage resume" {
    const a = std.testing.allocator;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("retained-cursor-open-admission");
    defer directory.cleanup();
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .required = true }} };
    var view: registry.SchemaView = .{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer view.release();
    {
        var store = try docstore.DocStore.open(a, directory.path(), .{});
        defer store.close();
        try store.put(@import("storage/db/range_state.zig").range_key, &(@as([8]u8, @splat(0))));
        try store.owned_lsm_backend.?.backend.flushBufferedWritesWithOptions(.{});
    }
    for ([_]bool{ false, true }) |reject| {
        var store = try docstore.DocStore.open(a, directory.path(), .{});
        defer store.close();
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var execution: scheduling.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
        defer execution.finish();
        var budget: Budget = .{ .scheduler = &scheduler, .execution = &execution, .reject_resume = reject };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const opened = rows.Reader.openSnapshotWithAdmission(a, try store.beginReadTxn(), store.valuesAreAuthenticated(), view, null, .{ .fields = &.{"n"} }, 0, admission.Port.init(&payload));
        if (reject) {
            try std.testing.expectError(error.SchedulerClosed, opened);
        } else {
            var reader = try opened;
            defer reader.deinit();
            try std.testing.expect(reader.page_admission.control == null);
            const checks = budget.checks;
            execution.finish();
            budget.poisoned = true;
            var page = try reader.nextTypedPage(a, null, .{ .rows = 1 });
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 0), page.rows.len);
            try std.testing.expectEqual(checks, budget.checks);
        }
        try std.testing.expect(budget.waits > 0);
    }
}

test "retained document page admission preserves snapshots and clears creation and page borrows" {
    const a = std.testing.allocator;
    const Documents = @import("storage/db/document_rows.zig").Session;
    const Check = struct {
        fn pages(session: *Documents, initial: @import("storage/relational_read_view.zig").View.Page, control: admission.Port, first: usize) !void {
            var pending: ?@import("storage/relational_read_view.zig").View.Page = initial;
            defer if (pending) |*page| page.deinit();
            var count: usize = 0;
            // Time-bounded pages may legitimately contain fewer rows on cold
            // I/O, including an empty page after an orphan owner. Verify the
            // retained continuation instead of assuming a full first page.
            for (0..32) |_| {
                var page = if (pending) |value| blk: {
                    pending = null;
                    break :blk value;
                } else try session.nextWithAdmission(std.testing.allocator, @intCast(2 - count), control);
                defer page.deinit();
                for (page.rows) |row| {
                    const ordinal = first + count;
                    const id = try std.fmt.allocPrint(std.testing.allocator, "row:{d}", .{ordinal});
                    defer std.testing.allocator.free(id);
                    const number = try std.fmt.allocPrint(std.testing.allocator, "{d}", .{ordinal});
                    defer std.testing.allocator.free(number);
                    try std.testing.expectEqualStrings(id, row.id);
                    try std.testing.expectEqual(@as(u64, ordinal + 10), row.version);
                    try std.testing.expectEqualStrings(number, row.document.?.object.get("n").?.number_string);
                    count += 1;
                }
                try std.testing.expect(page.after != null);
                if (count == 2) return;
            }
            return error.TestNoPageProgress;
        }
    };
    var directory = try @import("common/test_directory.zig").TestDirectory.init("retained-document-admission");
    defer directory.cleanup();
    {
        var store = try docstore.DocStore.open(a, directory.path(), .{});
        defer store.close();
        for (0..8) |ordinal| {
            const key = try std.fmt.allocPrint(a, "row:{d}", .{ordinal});
            defer a.free(key);
            const primary = try internal.documentKeyAlloc(a, key);
            defer a.free(primary);
            const json = try std.fmt.allocPrint(a, "{{\"n\":{d}}}", .{ordinal});
            defer a.free(json);
            try store.put(primary, json);
            const ttl_key = try internal.ttlKeyAlloc(a, key);
            defer a.free(ttl_key);
            var timestamp: [8]u8 = undefined;
            std.mem.writeInt(u64, &timestamp, ordinal + 10, .little);
            try store.put(ttl_key, &timestamp);
        }
        // An owner without a primary row must be skipped with admission intact.
        const orphan = try internal.ttlKeyAlloc(a, "aaa-orphan");
        defer a.free(orphan);
        try store.put(orphan, &(@as([8]u8, @splat(0))));
        try store.owned_lsm_backend.?.backend.flushBufferedWritesWithOptions(.{});
    }
    for ([_]bool{ false, true }) |reject| {
        var store = try docstore.DocStore.open(a, directory.path(), .{});
        defer store.close();
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var opening: scheduling.Execution = .{};
        try opening.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
        defer opening.finish();
        var opening_budget: Budget = .{ .scheduler = &scheduler, .execution = &opening, .reject_resume = false };
        var opening_adapter = admission.Adapter(*Budget){ .budget = &opening_budget };
        const opening_payload = opening_adapter.payload();
        const session = try Documents.openSnapshotWithAdmission(a, &store, try store.beginReadTxn(), null, .{ .start = "", .end = "" }, "", "", .{ .relational_query = .{ .fields = &.{"_id"} }, .sql_document_preimage = true }, 0, admission.Port.init(&opening_payload));
        defer session.deinit();
        try std.testing.expect(opening_budget.checks > 0);
        opening.finish();
        opening_budget.poisoned = true;
        const opening_checks = opening_budget.checks;
        var execution: scheduling.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
        defer execution.finish();
        var budget: Budget = .{ .scheduler = &scheduler, .execution = &execution, .reject_resume = reject };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const result = session.nextWithAdmission(a, 2, admission.Port.init(&payload));
        if (reject) {
            try std.testing.expectError(error.SchedulerClosed, result);
            try std.testing.expect(session.failed);
            try std.testing.expectError(error.InvalidSqlBackendResponse, session.next(a, 2));
        } else {
            try Check.pages(session, try result, admission.Port.init(&payload), 0);
            // Writes after capture cannot change the retained continuation.
            const key = try internal.documentKeyAlloc(a, "row:2");
            defer a.free(key);
            try store.put(key, "{\"n\":99}");
            execution.finish();
            budget.poisoned = true;
            const checks = budget.checks;
            var next_execution: scheduling.Execution = .{};
            try next_execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
            defer next_execution.finish();
            var next_budget: Budget = .{ .scheduler = &scheduler, .execution = &next_execution, .reject_resume = false };
            var next_adapter = admission.Adapter(*Budget){ .budget = &next_budget };
            const next_payload = next_adapter.payload();
            try Check.pages(session, try session.nextWithAdmission(a, 2, admission.Port.init(&next_payload)), admission.Port.init(&next_payload), 2);
            try std.testing.expectEqual(checks, budget.checks);
        }
        try std.testing.expect(budget.waits > 0);
        try std.testing.expectEqual(opening_checks, opening_budget.checks);
        try std.testing.expect(session.page_admission.control == null);
    }
}

test "retained metadata read admission preserves range and index authority through cold I/O and rejected resume" {
    const a = std.testing.allocator;
    const tracking = @import("storage/range_protection.zig");
    const jobs = @import("storage/db/relational_index_jobs.zig");
    const maintenance = @import("storage/db/relational_index_maintenance_contract.zig");
    const plans = @import("storage/db/relational_index_plan.zig");
    var directory = try @import("common/test_directory.zig").TestDirectory.init("retained-metadata-admission");
    defer directory.cleanup();
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .required = true }} };
    var view: registry.SchemaView = .{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer view.release();
    var plan = try plans.View.init(a, view, &.{.{ .name = "n_idx", .generation = 7, .keys = &.{.{ .column = "n" }} }});
    defer plan.release();
    const index = plan.boundIndexes()[0];
    const span = try tracking.indexTupleSpanDigest(index.id().encode(), "tuple");
    var owner: [32]u8 = undefined;
    {
        var store = try docstore.DocStore.open(a, directory.path(), .{});
        defer store.close();
        try store.put(@import("storage/db/range_state.zig").range_key, &(@as([8]u8, @splat(0))));
        try store.put(tracking.activation_key, tracking.activation_value);
        var generation: [8]u8 = undefined;
        std.mem.writeInt(u64, &generation, 19, .little);
        try store.put(&tracking.counterKey(tracking.bucket("r")), &generation);
        std.mem.writeInt(u64, &generation, 77, .little);
        try store.put(&tracking.indexCounterKey(span), &generation);
        {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            owner = try jobs.ownership(&txn);
        }
        const progress: jobs.Progress = .{ .id = index.id(), .owner = owner, .comparison = index.tuple.fingerprint, .state = .ready, .rows_scanned = 17, .attempt = 42, .last_maintenance = @splat(6) };
        const encoded = try progress.encode(a);
        defer a.free(encoded);
        try store.put(&jobs.progressKey(index.id()), encoded);
        const control: maintenance.Control = .{ .epoch = 42, .last_request = @splat(6) };
        const control_bytes = try control.encode(a);
        defer a.free(control_bytes);
        try store.put(&maintenance.controlKey(index.id()), control_bytes);
        try store.owned_lsm_backend.?.backend.flushBufferedWritesWithOptions(.{});
    }
    const Kind = enum { range, index_range, readiness };
    for ([_]Kind{ .range, .index_range, .readiness }) |kind| for ([_]bool{ false, true }) |reject| {
        var store = try docstore.DocStore.open(a, directory.path(), .{});
        defer store.close();
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var execution: scheduling.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
        defer execution.finish();
        var budget: Budget = .{ .scheduler = &scheduler, .execution = &execution, .reject_resume = reject };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var metadata = admission.MetadataRead(*docstore.DocStore.Txn).init(a, &txn, admission.Port.init(&payload));
        defer metadata.deinit();
        switch (kind) {
            .range => {
                const result = tracking.capture(a, &metadata, "r", "r");
                if (reject) {
                    try std.testing.expectError(error.SchedulerClosed, result);
                } else {
                    const proofs = try result;
                    defer a.free(proofs);
                    try std.testing.expectEqual(@as(usize, 1), proofs.len);
                    try std.testing.expectEqual(@as(?u64, 19), proofs[0].generation);
                    var later: [8]u8 = undefined;
                    std.mem.writeInt(u64, &later, 99, .little);
                    try store.put(&tracking.counterKey(tracking.bucket("r")), &later);
                    const captured_again = try tracking.capture(a, &metadata, "r", "r");
                    defer a.free(captured_again);
                    try std.testing.expectEqual(@as(?u64, 19), captured_again[0].generation);
                }
            },
            .index_range => {
                const result = tracking.captureIndex(a, &metadata, index.id().encode(), "tuple");
                if (reject) {
                    try std.testing.expectError(error.SchedulerClosed, result);
                } else {
                    const proofs = try result;
                    defer a.free(proofs);
                    try std.testing.expectEqual(@as(usize, 1), proofs.len);
                    try std.testing.expectEqual(@as(?u64, 77), proofs[0].generation);
                    try std.testing.expectEqualSlices(u8, &span, &proofs[0].index.?.digest);
                }
            },
            .readiness => {
                const result = jobs.status(&metadata, index);
                if (reject) {
                    try std.testing.expectError(error.SchedulerClosed, result);
                } else {
                    const progress = try result;
                    try std.testing.expectEqual(jobs.State.ready, progress.state);
                    try std.testing.expectEqual(@as(u64, 17), progress.rows_scanned);
                    try std.testing.expectEqual(@as(u64, 42), progress.attempt);
                    try std.testing.expectEqualSlices(u8, &owner, &progress.owner);
                }
            },
        }
        try std.testing.expect(budget.waits > 0);
    };
}

test "retained metadata read admission owns reused values and discards a partially captured proof set on cancellation" {
    const a = std.testing.allocator;
    const tracking = @import("storage/range_protection.zig");
    const Cancellation = struct {
        canceled: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.ExecutionWaitUnsupported;
        }
    };
    const Transaction = struct {
        budget: *Cancellation,
        buffer: [8]u8 = @splat(0),
        calls: usize = 0,
        cancel_at: usize = 0,
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.TestUnadmittedMetadataRead;
        }
        pub fn getManySortedWithAdmission(self: *@This(), keys: []const []const u8, values: []?[]const u8, control: admission.Port) !void {
            try control.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            const key = keys[0];
            self.calls += 1;
            const bytes: []const u8 = if (std.mem.eql(u8, key, "first")) "one" else if (std.mem.eql(u8, key, "second")) "two" else if (std.mem.eql(u8, key, tracking.activation_key)) tracking.activation_value else &.{ 1, 0, 0, 0, 0, 0, 0, 0 };
            @memcpy(self.buffer[0..bytes.len], bytes);
            values[0] = self.buffer[0..bytes.len];
            if (self.calls == self.cancel_at) self.budget.canceled = true;
            try control.checkpoint();
        }
    };
    var budget: Cancellation = .{};
    var adapter = admission.Adapter(*Cancellation){ .budget = &budget };
    const payload = adapter.payload();
    var transaction: Transaction = .{ .budget = &budget };
    var metadata = admission.MetadataRead(*Transaction).init(a, &transaction, admission.Port.init(&payload));
    defer metadata.deinit();
    const first = try metadata.get("first");
    const second = try metadata.get("second");
    try std.testing.expectEqualStrings("one", first);
    try std.testing.expectEqualStrings("two", second);
    transaction.calls = 0;
    transaction.cancel_at = 3; // Activation and first proof succeeded.
    try std.testing.expectError(error.Canceled, tracking.capture(a, &metadata, "", ""));
    try std.testing.expectEqual(@as(usize, 3), transaction.calls);
    try std.testing.expectEqualStrings("one", first);
}

test "retained capture admission publication wait releases CPU and unwinds every rejected lock acquisition" {
    const Lock = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Case = enum { success, canceled, rejected_resume, acquired_then_canceled };
    const State = struct {
        scheduler: *scheduling.Scheduler,
        execution: *scheduling.Execution,
        lock: *Lock,
        held: *bool,
        mode: Case,
        canceled: bool = false,
        checks: usize = 0,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.canceled or (self.mode == .acquired_then_canceled and self.checks == 2)) return error.Canceled;
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            var control: scheduling.Execution = .{};
            try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            control.finish();
            switch (self.mode) {
                .success => {
                    self.lock.unlockExclusive();
                    self.held.* = false;
                },
                .canceled => self.canceled = true,
                .rejected_resume => self.scheduler.close(),
                .acquired_then_canceled => return error.TestUnexpectedWait,
            }
            try self.execution.enter(.query);
        }
    };
    for ([_]Case{ .success, .canceled, .rejected_resume, .acquired_then_canceled }) |mode| {
        var lock: Lock = .{};
        var held = mode != .acquired_then_canceled;
        if (held) lock.lockExclusive();
        defer if (held) lock.unlockExclusive();
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var execution: scheduling.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
        defer execution.finish();
        var state: State = .{ .scheduler = &scheduler, .execution = &execution, .lock = &lock, .held = &held, .mode = mode };
        var adapter = admission.Adapter(*State){ .budget = &state };
        const payload = adapter.payload();
        const result = admission.lockShared(&lock, admission.Port.init(&payload));
        switch (mode) {
            .success => {
                try result;
                try std.testing.expect(!lock.tryLockExclusive());
                lock.unlockShared();
            },
            .canceled, .acquired_then_canceled => try std.testing.expectError(error.Canceled, result),
            .rejected_resume => try std.testing.expectError(error.SchedulerClosed, result),
        }
        if (held) {
            lock.unlockExclusive();
            held = false;
        }
        try std.testing.expect(lock.tryLockExclusive());
        lock.unlockExclusive();
        try std.testing.expectEqual(@as(usize, if (mode == .acquired_then_canceled) 0 else 1), state.waits);
    }
}

test "retained capture admission intent probe preserves uncertainty without lending under statement fences" {
    const a = std.testing.allocator;
    const Transactions = @import("storage/transactions.zig");
    for ([_]bool{ false, true }) |has_intent| {
        var directory = try @import("common/test_directory.zig").TestDirectory.init("retained-capture-intent");
        defer directory.cleanup();
        {
            var store = try docstore.DocStore.open(a, directory.path(), .{});
            defer store.close();
            try store.put("\x00\x00__txn_intents__;sentinel", "not-an-intent");
            if (has_intent) {
                var manager = try Transactions.TxnManager.init(a, &store);
                defer manager.deinit();
                try manager.initTransaction(@splat(1), 1);
                try manager.writeIntents(@splat(1), &.{.{ .key = "pending", .value = "value" }}, &.{});
            }
            try store.owned_lsm_backend.?.backend.flushBufferedWritesWithOptions(.{});
        }
        for ([_]bool{ false, true }) |reject| {
            var store = try docstore.DocStore.open(a, directory.path(), .{});
            defer store.close();
            var manager = try Transactions.TxnManager.init(a, &store);
            defer manager.deinit();
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
            var execution: scheduling.Execution = .{};
            try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
            defer execution.finish();
            var budget: Budget = .{ .scheduler = &scheduler, .execution = &execution, .reject_resume = reject };
            var adapter = admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            const result = manager.hasUnresolvedWriteIntentsWithAdmission(admission.Port.init(&payload));
            try std.testing.expectEqual(has_intent, try result);
            try std.testing.expectEqual(@as(usize, 0), budget.waits);
            try std.testing.expectEqual(@as(usize, 0), budget.checks);
        }
    }
}

test "retained normalization admission cleans partial output and preserves the session across cancellation and allocation failures" {
    const a = std.testing.allocator;
    const Documents = @import("storage/db/document_rows.zig").Session;
    const types = @import("storage/db/types.zig");
    const tracking = @import("storage/range_protection.zig");
    const writes: []const types.BatchWrite = &.{
        .{ .key = "first", .value = "{\"n\":1,\"payload\":null}", .json_null_fields = &.{"payload"} },
        .{ .key = "second", .value = "{\"n\":2,\"payload\":null}", .json_null_fields = &.{"payload"} },
    };
    const CancelBudget = struct {
        checks: usize = 0,
        cancel_at: usize = 0,
        poisoned: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.poisoned) return error.TestBorrowEscaped;
            self.checks += 1;
            if (self.checks == self.cancel_at) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.ExecutionWaitUnsupported;
        }
    };
    const Harness = struct {
        fn For(comptime S: type) type {
            return struct {
                fn run(backing: std.mem.Allocator, session: *S) !void {
                    // Arena growth must exercise mandatory allocate/copy paths;
                    // opportunistic in-place growth depends on backing addresses
                    // and cannot define a deterministic OOM inventory.
                    var stable: @import("storage/lite/test_allocator.zig").NoResizeAllocator = .{ .backing = backing };
                    const alloc = stable.allocator();
                    var budget: CancelBudget = .{};
                    var adapter = admission.Adapter(*CancelBudget){ .budget = &budget };
                    const payload = adapter.payload();
                    const normalized = try session.normalizeRowsWithAdmission(alloc, writes, admission.Port.init(&payload));
                    defer types.freeNormalizedWrites(alloc, normalized);
                    try std.testing.expectEqual(@as(usize, 2), normalized.len);
                    for (normalized, writes) |out, input| {
                        try std.testing.expectEqualStrings(input.key, out.key);
                        try std.testing.expectEqual(@as(usize, 1), out.json_null_fields.len);
                        try std.testing.expectEqualStrings("payload", out.json_null_fields[0]);
                        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out.value, .{});
                        defer parsed.deinit();
                        try std.testing.expect(parsed.value.object.get("payload").? == .null);
                    }
                }
                fn exercise(session: *S) !void {
                    try std.testing.checkAllAllocationFailures(std.testing.allocator, run, .{session});
                    for ([_]usize{ 4, 5 }) |cancel_at| {
                        var budget: CancelBudget = .{ .cancel_at = cancel_at };
                        var adapter = admission.Adapter(*CancelBudget){ .budget = &budget };
                        const payload = adapter.payload();
                        try std.testing.expectError(error.Canceled, session.normalizeRowsWithAdmission(std.testing.allocator, writes, admission.Port.init(&payload)));
                        if (comptime S == Documents) {
                            try std.testing.expect(session.page_admission.control == null);
                            try std.testing.expect(!session.failed);
                        } else try std.testing.expect(session.call_admission.control == null);
                        budget.poisoned = true;
                        try run(std.testing.allocator, session);
                    }
                    var budget: CancelBudget = .{ .cancel_at = 3 };
                    var adapter = admission.Adapter(*CancelBudget){ .budget = &budget };
                    const payload = adapter.payload();
                    try std.testing.expectError(error.Canceled, session.rangeProofsWithAdmission(std.testing.allocator, admission.Port.init(&payload)));
                    budget.poisoned = true;
                    var healthy: CancelBudget = .{};
                    var healthy_adapter = admission.Adapter(*CancelBudget){ .budget = &healthy };
                    const healthy_payload = healthy_adapter.payload();
                    const proofs = try session.rangeProofsWithAdmission(std.testing.allocator, admission.Port.init(&healthy_payload));
                    defer std.testing.allocator.free(proofs);
                    try std.testing.expectEqual(@as(usize, 1), proofs.len);
                    try std.testing.expectEqual(@as(?u64, 7), proofs[0].generation);
                }
            };
        }
    };
    var directory = try @import("common/test_directory.zig").TestDirectory.init("retained-normalization");
    defer directory.cleanup();
    var store = try docstore.DocStore.open(a, directory.path(), .{});
    defer store.close();
    const docs = try Documents.openSnapshot(a, &store, try store.beginReadTxn(), null, .{ .start = "", .end = "" }, "", "", .{ .relational_query = .{ .fields = &.{"_id"} } }, 0);
    defer docs.deinit();
    // Supply the declared SQL JSON-column contract directly to the native
    // session fixture; production derives it from the pinned public validator.
    docs.table.columns = &.{.{ .name = "payload", .path = "payload", .type = .json }};
    const doc_proofs = try docs.arena.allocator().alloc(tracking.Proof, 1);
    doc_proofs[0] = .{ .bucket = 1, .generation = 7 };
    docs.range_proofs = doc_proofs;
    try Harness.For(Documents).exercise(docs);
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .integer, .required = true },
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    var view: registry.SchemaView = .{ .epoch = try registry.Epoch.createCloned(a, table) };
    defer view.release();
    const relational = try a.create(Session);
    relational.* = .{ .alloc = a, .reader = try rows.Reader.open(a, &store, view, null, .{ .fields = &.{"n"} }, 0), .cancellation = null, .deadline_ns = null };
    defer relational.deinit();
    const relational_proofs = try a.alloc(tracking.Proof, 1);
    relational_proofs[0] = .{ .bucket = 1, .generation = 7 };
    relational.range_proofs = relational_proofs;
    try Harness.For(Session).exercise(relational);
}

test "retained capture admission releases payload policy during primary capture and rejects ABA" {
    try docstore.qualifyPayloadPolicyCapture();
}

test "retained capture admission publisher lends CPU and withdraws exclusion without losing readers" {
    try docstore.qualifyPortablePublicationAdmission();
}

test "retained capture admission checkpoints never yield while owning backend mutex" {
    try @import("storage/lsm_backend.zig").qualifyLockedCaptureCheckpoints();
}

test "retained capture admission run version builder coalesces peers and survives legacy publication races" {
    try @import("storage/lsm_backend.zig").qualifyReadVersionBuildAdmission();
}

test "retained capture admission concurrent run version waiters lend their original CPU" {
    try @import("storage/lsm_backend.zig").qualifyConcurrentReadVersionBuild();
}

test "retained capture admission predecision mutators cannot occupy their holder's resume capacity" {
    try @import("storage/db/snapshot_admission.zig").qualifyPreDecisionCapacityCycle();
}

test "retained capture admission predecision apply rejects capacity cycles and preserves scoped rejection" {
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const mutation = @import("storage/db/mutation_admission.zig");
    const Mode = enum { busy, can_wait, canceled, expired, closed, late_rejection, late_gate, busy_canceled, mandatory, legacy };
    const Core = struct {
        apply_mutex: *Apply,
        pub fn unlockApply(self: *@This()) void {
            self.apply_mutex.unlockExclusive();
        }
    };
    const Fixture = struct {
        core: Core,
        execution: *scheduling.Execution,
        scheduler: *scheduling.Scheduler,
        mode: Mode,
        holding: bool = true,
        checks: usize = 0,
        gates: usize = 0,
        parks: usize = 0,
        fn releaseHolder(self: *@This()) void {
            if (self.holding) self.core.apply_mutex.unlockShared();
            self.holding = false;
        }
        pub fn enforcePortableRuntimeGate(self: *@This()) !void {
            self.gates += 1;
            if (self.mode == .late_gate and self.gates == 2) return error.PortableRuntimeActivationPending;
        }
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            try self.enforcePortableRuntimeGate();
            self.core.apply_mutex.lockExclusive();
        }
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            switch (self.mode) {
                .canceled => return error.Canceled,
                .expired => return error.DeadlineExceeded,
                .closed => return error.SchedulerClosed,
                .late_rejection, .busy_canceled => if (self.checks == 2) return error.Canceled,
                // Legacy mandatory waiting still follows its original scope.
                // This simulates external holder progress, not a general proof
                // that mandatory callers can safely retain runnable capacity.
                .mandatory => if (self.checks == 2) self.releaseHolder(),
                else => {},
            }
            try self.execution.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parks += 1;
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            self.releaseHolder();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.write);
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var reader: scheduling.Execution = .{};
        try reader.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
        defer reader.finish();
        var apply: Apply = .{ .io = std.testing.io };
        apply.lockShared();
        try reader.park(.resource_blocked);
        var writer: scheduling.Execution = .{};
        try writer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
        defer writer.finish();
        var fixture: Fixture = .{ .core = .{ .apply_mutex = &apply }, .execution = &writer, .scheduler = &scheduler, .mode = mode };
        defer fixture.releaseHolder();
        if (mode == .late_rejection or mode == .late_gate or mode == .legacy) fixture.releaseHolder();
        const controls: Apply.Acquisition = .{ .lifetime = .{ .context = &fixture, .check_fn = Fixture.check }, .execution_wait = if (mode == .can_wait) .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } else null };
        const result = if (mode == .mandatory)
            mutation.lockApply(&fixture, controls)
        else
            mutation.lockApplyPreDecision(&fixture, if (mode == .legacy) null else controls);
        if (mode == .can_wait or mode == .mandatory or mode == .legacy) {
            try result;
            fixture.core.unlockApply();
        } else {
            try std.testing.expectError(switch (mode) {
                .busy => error.Backpressured,
                .expired => error.DeadlineExceeded,
                .closed => error.SchedulerClosed,
                .late_gate => error.PortableRuntimeActivationPending,
                else => error.Canceled,
            }, result);
        }
        if (mode == .busy or mode == .busy_canceled) {
            try std.testing.expectEqual(@as(u64, 1), apply.exclusive_lock_calls.load(.acquire));
            try std.testing.expectEqual(@as(u64, 1), apply.exclusive_contended_calls.load(.acquire));
        }
        try std.testing.expectEqual(@as(usize, if (mode == .can_wait) 1 else 0), fixture.parks);
        try std.testing.expectEqual(@as(u64, 0), apply.exclusive_waiters.load(.acquire));
        try std.testing.expectEqual(@as(u32, 0), apply.parked_waiters.load(.acquire));
        writer.finish();
        // The suspended original reader can resume as soon as the rejected
        // non-lending writer returns capacity; no replacement root is needed.
        try reader.enter(.query);
        fixture.releaseHolder();
        try std.testing.expect(apply.tryLockExclusive());
        apply.unlockExclusive();
        reader.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "retained capture admission predecision replication gate preserves original roots and holder resume capacity" {
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Boundary = @import("common/execution_wait.zig").Boundary;
    const mutation = @import("storage/db/mutation_admission.zig");
    const Mode = enum { busy, can_wait, canceled, expired, closed, late_rejection, busy_canceled, mandatory, legacy, held, late_held, foreign_legacy };
    const Noop = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?Boundary {
            return null;
        }
    };
    const Fixture = struct {
        core: struct { apply_mutex: *Apply },
        barrier: *Barrier,
        capture: ?Barrier.ExclusiveLease = null,
        execution: *scheduling.Execution,
        scheduler: *scheduling.Scheduler,
        mode: Mode,
        checks: usize = 0,
        parks: usize = 0,
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
        fn releaseCapture(self: *@This()) void {
            if (self.capture) |*capture| capture.release();
            self.capture = null;
        }
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            switch (self.mode) {
                .canceled => return error.Canceled,
                .expired => return error.DeadlineExceeded,
                .closed => return error.SchedulerClosed,
                .late_rejection, .busy_canceled, .late_held => if (self.checks == 2) return error.Canceled,
                // External holder progress keeps the old mandatory acquisition
                // test finite. It is not full mandatory liveness qualification.
                .mandatory => if (self.checks == 2) self.releaseCapture(),
                else => {},
            }
            try self.execution.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parks += 1;
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            self.releaseCapture();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.write);
        }
    };
    for ([_]bool{ false, true }) |rooted| {
        for (std.enums.values(Mode)) |mode| {
            if (!rooted and (mode == .held or mode == .late_held)) continue;
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
            var reader: scheduling.Execution = .{};
            try reader.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
            defer reader.finish();
            var barrier: Barrier = .{};
            var other: Barrier = .{};
            var foreign = if (mode == .foreign_legacy) other.acquireShared() else null;
            defer if (foreign) |*lease| lease.release();
            var root = Barrier.RootSharedLease.initRetired(&barrier);
            defer root.release();
            var apply: Apply = .{ .io = std.testing.io };
            try reader.park(.resource_blocked);
            var writer: scheduling.Execution = .{};
            try writer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
            defer writer.finish();
            var fixture: Fixture = .{ .core = .{ .apply_mutex = &apply }, .barrier = &barrier, .execution = &writer, .scheduler = &scheduler, .mode = mode };
            defer fixture.releaseCapture();
            const parent_held = mode == .held or mode == .late_held;
            if (parent_held) {
                var noop: Noop = .{};
                root = try barrier.acquireSharedRootControlled(std.testing.io, &noop);
                // A queued capture closes new admission. The child must borrow
                // its actual parent and never queue behind that parent's reader.
                try std.testing.expect(barrier.reader_gate.tryLock());
            } else if (mode != .late_rejection and mode != .legacy and mode != .foreign_legacy) {
                fixture.capture = barrier.acquireExclusive();
            }
            const controls: ?Apply.Acquisition = if (mode == .legacy) null else .{ .lifetime = .{ .context = &fixture, .check_fn = Fixture.check }, .execution_wait = if (mode == .can_wait) .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } else null };
            const result = if (mode == .mandatory)
                mutation.acquireReplicationMutationSharedScoped(&fixture, controls, if (rooted) &root else null)
            else
                mutation.acquireReplicationMutationSharedPreDecisionScoped(&fixture, controls, if (rooted) &root else null);
            if (mode == .can_wait or mode == .mandatory or mode == .legacy or mode == .held or (mode == .foreign_legacy and rooted)) {
                var lease = (try result) orelse return error.MissingMutationReader;
                if (rooted) try std.testing.expect(lease.root == &root);
                lease.release();
            } else {
                try std.testing.expectError(switch (mode) {
                    .busy => error.Backpressured,
                    .expired => error.DeadlineExceeded,
                    .closed => error.SchedulerClosed,
                    .foreign_legacy => error.InvalidArgument,
                    else => error.Canceled,
                }, result);
                if (mode == .busy or mode == .busy_canceled) try std.testing.expect(fixture.capture != null);
            }
            try std.testing.expectEqual(parent_held, root.held);
            try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
            if (parent_held) {
                barrier.reader_gate.unlock();
                try root.retirePhase();
            }
            try std.testing.expectEqual(@as(usize, if (mode == .can_wait) 1 else 0), fixture.parks);
            try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
            writer.finish();
            try reader.enter(.query);
            fixture.releaseCapture();
            var next = barrier.tryAcquireExclusive() orelse return error.LeakedMutationReader;
            next.release();
            if (foreign) |*lease| {
                try std.testing.expect(other.tryAcquireExclusive() == null);
                lease.release();
                foreign = null;
                var other_capture = other.tryAcquireExclusive() orelse return error.LeakedForeignReader;
                other_capture.release();
            }
            reader.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}

test "retained capture admission predecision pressure preserves producer capacity and allocation hard guards" {
    const Manager = @import("storage/resource_manager.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Collection = @import("storage/db/mutation_admission.zig").IntentCollection;
    const Mode = enum { normal, soft, hard, can_wait, soft_wait, child_busy, parent_busy, canceled, expired, closed, late_rejection, impossible, reject_work };
    const Noop = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?@import("common/execution_wait.zig").Boundary {
            return null;
        }
    };
    const Fixture = struct {
        manager: *Manager.ResourceManager,
        producer: *scheduling.Execution,
        writer: *scheduling.Execution,
        scheduler: *scheduling.Scheduler,
        mode: Mode,
        charged: u64 = 0,
        checks: usize = 0,
        waits: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks > 4096) return error.TestUnexpectedCoarseWait;
            switch (self.mode) {
                .canceled => return error.Canceled,
                .expired => return error.DeadlineExceeded,
                .closed => return error.SchedulerClosed,
                .late_rejection => {
                    if (!self.manager.mutex.tryLock()) return error.Canceled;
                    self.manager.mutex.unlock();
                },
                else => {},
            }
            try self.writer.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            try self.writer.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            // Neither parent nor child accounting can be held at a progress wait.
            try std.testing.expect(self.manager.mutex.tryLock());
            self.manager.mutex.unlock();
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            try self.producer.enter(.query);
            if (self.mode == .can_wait) self.manager.observeUsage(.lsm_in_memory_state, &self.charged, 0);
            try self.producer.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.writer.enter(.write);
        }
    };
    for ([_]bool{ false, true }) |parent_held| {
        for (std.enums.values(Mode)) |mode| {
            var budgets = Manager.Options.defaultBudgets();
            budgets[@backingInt(Manager.Slice.lsm_in_memory_state)] = .{ .soft_limit_bytes = 10, .hard_limit_bytes = 20 };
            var policies = Manager.Options.defaultPolicies();
            if (mode == .reject_work) policies[@backingInt(Manager.Slice.lsm_in_memory_state)].hard_action = .reject_work;
            var parent = Manager.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer parent.deinit(std.testing.allocator);
            var manager = Manager.ResourceManager.init(.{ .budgets = budgets, .policies = policies, .identity_allocator = std.testing.allocator });
            var manager_live = true;
            defer if (manager_live) manager.deinit(std.testing.allocator);
            try manager.installMemoryParent(parent.memoryParentPort());
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
            var producer: scheduling.Execution = .{};
            try producer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
            defer producer.finish();
            try producer.park(.resource_blocked);
            var writer: scheduling.Execution = .{};
            try writer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
            defer writer.finish();
            var barrier: Barrier = .{};
            var root = Barrier.RootSharedLease.initRetired(&barrier);
            defer root.release();
            if (parent_held) {
                var noop: Noop = .{};
                root = try barrier.acquireSharedRootControlled(std.testing.io, &noop);
            }
            var fixture: Fixture = .{ .manager = &manager, .producer = &producer, .writer = &writer, .scheduler = &scheduler, .mode = mode };
            manager.observeUsage(.lsm_in_memory_state, &fixture.charged, switch (mode) {
                .normal, .child_busy, .parent_busy, .late_rejection => 0,
                .soft, .soft_wait => 12,
                else => 21,
            });
            defer if (manager_live) manager.observeUsage(.lsm_in_memory_state, &fixture.charged, 0);
            const target = if (mode == .parent_busy) &parent.mutex else &manager.mutex;
            var target_held = mode == .child_busy or mode == .parent_busy;
            if (target_held) try std.testing.expect(target.tryLock());
            defer if (target_held) target.unlock();
            const controls: Apply.Acquisition = .{ .lifetime = .{ .context = &fixture, .check_fn = Fixture.check }, .execution_wait = if (mode == .can_wait or mode == .soft_wait) .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } else null };
            var collection: Collection = .{};
            const control = collection.bindPreDecisionBeforeMutation(controls, std.testing.io, &root).?;
            const can_lend = !parent_held and (mode == .can_wait or mode == .soft_wait);
            try std.testing.expectEqual(can_lend, try control.hasExecutionWait());
            const result = manager.awaitAdmissionPreDecisionWithAdmission(.lsm_in_memory_state, if (mode == .impossible) 21 else 1, control);
            if (mode == .normal or mode == .soft or mode == .soft_wait or (mode == .can_wait and !parent_held)) {
                try result;
            } else {
                try std.testing.expectError(switch (mode) {
                    .canceled, .late_rejection => error.Canceled,
                    .expired => error.DeadlineExceeded,
                    .closed => error.SchedulerClosed,
                    .impossible, .reject_work => error.ResourceBudgetExceeded,
                    else => error.Backpressured,
                }, result);
            }
            try std.testing.expectEqual(parent_held, root.held);
            try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
            try std.testing.expectEqual(@as(usize, if (can_lend) (if (mode == .soft_wait) 10 else 1) else 0), fixture.waits);
            if (target_held) {
                try std.testing.expect(!target.tryLock());
            } else {
                try std.testing.expect(manager.mutex.tryLock());
                manager.mutex.unlock();
            }
            if (mode == .soft) {
                // Skipping advisory pacing never grants an allocation over the
                // authoritative slice limit or consumes a reservation on failure.
                try std.testing.expectError(error.ResourceBudgetExceeded, manager.reserve(.lsm_in_memory_state, 9));
                try std.testing.expectEqual(@as(u64, 12), fixture.charged);
            }
            try root.retirePhase();
            writer.finish();
            // Rejection returns CPU to the same original producer, which can
            // release its actual held ledger and charge without a new root.
            try producer.enter(.query);
            if (target_held) {
                target.unlock();
                target_held = false;
            }
            manager.observeUsage(.lsm_in_memory_state, &fixture.charged, 0);
            // Payload retirement leaves the manager's explicitly owned identity
            // tables charged in both child and parent until actual destruction.
            try Manager.metadata_fixture.expectUsage(0, &manager, &.{&manager});
            try Manager.metadata_fixture.expectUsage(0, &parent, &.{ &parent, &manager });
            producer.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
            manager.deinit(std.testing.allocator);
            manager_live = false;
            try Manager.metadata_fixture.expectUsage(0, &parent, &.{&parent});
        }
    }
}

test "retained capture admission startup empty probes preserve obligations and release producer capacity" {
    const startup = @import("storage/db/replication_startup_admission.zig");
    const mutations = @import("storage/db/mutation_admission.zig");
    const outbox = @import("storage/db/durable_outbox.zig");
    const outbox_store = @import("storage/db/durable_outbox_store.zig");
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Backend = @import("storage/mem_backend.zig").Backend;
    const Atomic = std.atomic.Value(bool);
    const Mode = enum { idle, empty, flush_busy, apply_busy, backend_busy, policy_busy, batch, replay, schema, mutation, append, primary_hint, after_apply_append, canceled, expired, closed, late_empty, held_parent, lend_flush };
    const Owner = struct {
        const Core = struct {
            store: *docstore.DocStore,
            apply_mutex: *Apply,
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        };
        core: Core,
        backend_runtime: struct {
            pub fn io(_: *const @This()) ?std.Io {
                return std.testing.io;
            }
        } = .{},
        async_context: struct { primary_replication_append_pending: Atomic = .init(false), primary_replication_outbox_pending: Atomic = .init(false) } = .{},
        local_execution: struct {
            durable_replication_flush_mutex: std.Io.Mutex = .init,
            durable_replication_startup_barrier_pending: Atomic = .init(true),
            row_policy_replication_outbox_pending: Atomic = .init(true),
            durable_replication_outbox_maybe: Atomic = .init(true),
        } = .{},
        schedules: usize = 0,
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            self.core.apply_mutex.lockExclusive();
        }
        pub fn scheduleDurableReplicationOutboxRecovery(self: *@This()) void {
            // Scheduling must occur after retiring both probe-owned fences.
            if (!self.core.apply_mutex.tryLockExclusive()) @panic("scheduled under apply");
            self.core.unlockApply();
            if (!self.local_execution.durable_replication_flush_mutex.tryLock()) @panic("scheduled under flush");
            self.local_execution.durable_replication_flush_mutex.unlock(std.testing.io);
            self.schedules += 1;
        }
        pub fn flushDurableReplicationOutboxesLockedWithAdmissionAndRoot(self: *@This(), controls: ?Apply.Acquisition, _: ?*Barrier.RootSharedLease) !void {
            try mutations.lockApply(self, controls);
            defer self.core.unlockApply();
            var collection: mutations.IntentCollection = .{};
            const control = collection.bind(controls, std.testing.io).?;
            if (try outbox_store.hasPendingWithAdmission(self.core.store, control)) return error.ReplicationPublisherUnavailable;
            self.local_execution.durable_replication_outbox_maybe.store(false, .release);
        }
        pub fn flushDurableReplicationOutboxesWithAdmissionAndRoot(self: *@This(), controls: ?Apply.Acquisition, root: ?*Barrier.RootSharedLease) !void {
            try startup.lockFlush(&self.local_execution.durable_replication_flush_mutex, std.testing.io, controls);
            defer self.local_execution.durable_replication_flush_mutex.unlock(std.testing.io);
            try self.flushDurableReplicationOutboxesLockedWithAdmissionAndRoot(controls, root);
        }
    };
    const Fixture = struct {
        owner: *Owner,
        backend: *Backend,
        producer: *scheduling.Execution,
        writer: *scheduling.Execution,
        scheduler: *scheduling.Scheduler,
        mode: Mode,
        guard_held: bool = false,
        reader_seen: bool = false,
        checks: usize = 0,
        waits: usize = 0,
        fn releaseGuard(self: *@This()) void {
            if (!self.guard_held) return;
            switch (self.mode) {
                .flush_busy, .lend_flush => self.owner.local_execution.durable_replication_flush_mutex.unlock(std.testing.io),
                .apply_busy => self.owner.core.unlockApply(),
                .backend_busy => self.backend.mutex.unlock(),
                .policy_busy => self.owner.core.store.payload_policy_mutex.unlock(std.testing.io),
                else => unreachable,
            }
            self.guard_held = false;
        }
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks > 4096) return error.TestUnexpectedCoarseWait;
            switch (self.mode) {
                .canceled => return error.Canceled,
                .expired => return error.DeadlineExceeded,
                .closed => return error.SchedulerClosed,
                else => {},
            }
            const applying = self.owner.core.apply_mutex.state.load(.acquire) != 0;
            const reading = self.owner.core.store.portable_import_reader_state.load(.acquire) != 0;
            if (reading) self.reader_seen = true;
            if (self.mode == .late_empty and applying and self.reader_seen and !reading) return error.Canceled;
            if (self.mode == .after_apply_append and applying) self.owner.async_context.primary_replication_append_pending.store(true, .release);
            try self.writer.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            try self.writer.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            try self.producer.enter(.background);
            self.releaseGuard();
            try self.producer.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.writer.enter(.write);
        }
    };
    const alloc = std.testing.allocator;
    for (std.enums.values(Mode)) |mode| {
        var backend = Backend.init(alloc, .{});
        defer backend.close();
        var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        const mutation_key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .batch, 1, 1, "pending");
        defer alloc.free(mutation_key);
        const key: ?[]const u8 = switch (mode) {
            .batch => outbox.replication_batch_outbox_key,
            .replay => outbox.replication_replay_outbox_key,
            .schema => outbox.replication_schema_outbox_key,
            .mutation => mutation_key,
            else => null,
        };
        if (key) |pending_key| try store.put(pending_key, "pending");
        var apply: Apply = .{ .io = std.testing.io };
        var owner: Owner = .{ .core = .{ .store = &store, .apply_mutex = &apply } };
        if (mode == .idle) {
            owner.local_execution.durable_replication_startup_barrier_pending.store(false, .release);
            owner.local_execution.row_policy_replication_outbox_pending.store(false, .release);
            owner.local_execution.durable_replication_outbox_maybe.store(false, .release);
        }
        if (mode == .append) owner.async_context.primary_replication_append_pending.store(true, .release);
        if (mode == .primary_hint) owner.async_context.primary_replication_outbox_pending.store(true, .release);
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var producer: scheduling.Execution = .{};
        try producer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .background);
        defer producer.finish();
        try producer.park(.resource_blocked);
        var writer: scheduling.Execution = .{};
        try writer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
        defer writer.finish();
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        if (mode == .held_parent) {
            const Noop = struct {
                pub fn checkpoint(_: *@This()) !void {}
                pub fn executionBoundary(_: *@This()) ?@import("common/execution_wait.zig").Boundary {
                    return null;
                }
            };
            var noop: Noop = .{};
            root = try barrier.acquireSharedRootControlled(std.testing.io, &noop);
        }
        var fixture: Fixture = .{ .owner = &owner, .backend = &backend, .producer = &producer, .writer = &writer, .scheduler = &scheduler, .mode = mode };
        switch (mode) {
            .flush_busy, .lend_flush => owner.local_execution.durable_replication_flush_mutex.lockUncancelable(std.testing.io),
            .apply_busy => apply.lockExclusive(),
            .backend_busy => try std.testing.expect(backend.mutex.tryLock()),
            .policy_busy => store.payload_policy_mutex.lockUncancelable(std.testing.io),
            else => {},
        }
        fixture.guard_held = mode == .flush_busy or mode == .lend_flush or mode == .apply_busy or mode == .backend_busy or mode == .policy_busy;
        defer fixture.releaseGuard();
        const controls: Apply.Acquisition = .{ .lifetime = .{ .context = &fixture, .check_fn = Fixture.check }, .execution_wait = if (mode == .lend_flush) .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } else null };
        const result = if (mode == .lend_flush)
            startup.ensurePreDecisionBeforePolicyWithRoot(&owner, controls, null)
        else
            startup.ensurePreDecisionWithRoot(&owner, controls, &root);
        const healthy = mode == .idle or mode == .empty or mode == .primary_hint or mode == .held_parent or mode == .lend_flush;
        if (healthy) {
            try result;
            try std.testing.expect(!owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire));
            try std.testing.expect(!owner.local_execution.row_policy_replication_outbox_pending.load(.acquire));
            try std.testing.expect(!owner.local_execution.durable_replication_outbox_maybe.load(.acquire));
            try std.testing.expect(!owner.async_context.primary_replication_outbox_pending.load(.acquire));
        } else {
            try std.testing.expectError(switch (mode) {
                .canceled, .late_empty => error.Canceled,
                .expired => error.DeadlineExceeded,
                .closed => error.SchedulerClosed,
                else => error.Backpressured,
            }, result);
            try std.testing.expect(owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire));
            try std.testing.expect(owner.local_execution.row_policy_replication_outbox_pending.load(.acquire));
        }
        try std.testing.expectEqual(mode == .held_parent, root.held);
        try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
        try std.testing.expectEqual(@as(usize, if (mode == .lend_flush) 1 else 0), fixture.waits);
        try std.testing.expectEqual(@as(usize, if (key != null or mode == .append or mode == .after_apply_append) 1 else 0), owner.schedules);
        try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
        if (key) |pending_key| {
            const value = try store.get(alloc, pending_key);
            defer alloc.free(value);
            try std.testing.expectEqualStrings("pending", value);
        }
        try root.retirePhase();
        try writer.park(.resource_blocked);
        try producer.enter(.background);
        fixture.releaseGuard();
        try producer.park(.resource_blocked);
        try writer.enter(.write);
        if (mode == .flush_busy or mode == .apply_busy or mode == .backend_busy or mode == .policy_busy) {
            try startup.ensurePreDecisionWithRoot(&owner, controls, &root);
            try std.testing.expect(!owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire));
        }
        try std.testing.expect(owner.local_execution.durable_replication_flush_mutex.tryLock());
        owner.local_execution.durable_replication_flush_mutex.unlock(std.testing.io);
        try std.testing.expect(apply.tryLockExclusive());
        apply.unlockExclusive();
        writer.finish();
        producer.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "retained capture admission mutation traces preserve progress lifetime deadlines and lending eligibility" {
    const Trace = @import("storage/transactions.zig").MutationAdmissionTrace;
    const State = struct {
        checks: usize = 0,
        lives: usize = 0,
        waits: usize = 0,
        cause: ?anyerror = null,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
        fn life(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lives += 1;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            if (self.cause) |cause| return cause;
        }
        fn deadline(_: *anyopaque) !?u64 {
            return 123;
        }
        fn transfer(_: *anyopaque) !void {
            return error.UnexpectedCpuTransfer;
        }
    };
    for ([_]?anyerror{ null, error.Backpressured, error.NotFound, error.TxnNotFound, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        var state: State = .{ .cause = cause };
        const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.life, .wait_for_progress = State.wait, .deadline_ns = State.deadline } };
        var trace: Trace = .{};
        const port = trace.bind(admission.Port.init(&payload)).?;
        try std.testing.expectEqual(@as(?u64, 123), try port.deadlineNs());
        const checks = state.checks;
        try port.checkLifetime();
        try std.testing.expectEqual(checks, state.checks);
        try std.testing.expect(!try port.hasExecutionWait());
        if (cause) |failure| {
            try std.testing.expectError(failure, port.waitForProgress());
            const counts = .{ state.checks, state.lives, state.waits };
            state.cause = null;
            try std.testing.expectError(failure, port.checkpoint());
            try std.testing.expectError(failure, port.checkLifetime());
            try std.testing.expectError(failure, port.waitForProgress());
            try std.testing.expectError(failure, port.deadlineNs());
            try std.testing.expectEqual(counts, .{ state.checks, state.lives, state.waits });
            try std.testing.expectEqual(failure, trace.failure.?);
        } else {
            try port.waitForProgress();
            try std.testing.expectEqual(@as(usize, 1), state.waits);
            try std.testing.expect(trace.failure == null);
        }
    }
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.life }, .execution_wait = .{ .ptr = &state, .begin = State.transfer, .end = State.transfer } };
    var trace: Trace = .{};
    const port = trace.bind(admission.Port.init(&payload)).?;
    try std.testing.expect(try port.hasExecutionWait());
    var backend = @import("storage/mem_backend.zig").Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(std.testing.allocator, try backend.runtimeStore(std.testing.allocator, .{}));
    defer store.close();
    var captured = try store.beginReadTxnWithAdmission(.transient, port);
    try std.testing.expectEqual(@as(usize, 1), store.portable_import_reader_state.load(.acquire));
    captured.abort();
    try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
    var invalid = admission.Port.init(&payload);
    invalid.contract.size +%= 1;
    var invalid_trace: Trace = .{};
    const invalid_port = invalid_trace.bind(invalid).?;
    try std.testing.expectError(error.UnsupportedVersion, invalid_port.checkpoint());
}

test "retained capture admission BEGIN and prepare reject dependency contention under the original producer scope" {
    const Transactions = @import("storage/transactions.zig");
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Collection = @import("storage/db/mutation_admission.zig").IntentCollection;
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |prepare| {
        for ([_]?anyerror{ null, error.NotFound, error.TxnNotFound, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
            var backend = @import("storage/mem_backend.zig").Backend.init(a, .{});
            defer backend.close();
            var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
            defer store.close();
            var manager = try Transactions.TxnManager.init(a, &store);
            defer manager.deinit();
            const id: Transactions.TxnId = @splat(88);
            if (prepare) try manager.initTransaction(id, 1);
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
            defer scheduler.close();
            var producer: scheduling.Execution = .{};
            defer producer.finish();
            var writer: scheduling.Execution = .{};
            defer writer.finish();
            try producer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .background);
            try std.testing.expect(store.payload_policy_mutex.tryLock());
            var policy_held = true;
            defer if (policy_held) store.payload_policy_mutex.unlock(std.testing.io);
            try producer.park(.resource_blocked);
            try writer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
            const State = struct {
                writer: *scheduling.Execution,
                cause: ?anyerror = null,
                checks: usize = 0,
                fn life(raw: *const anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
                    self.checks += 1;
                    if (self.cause) |failure| return failure;
                    try self.writer.checkLifetime();
                }
                fn transfer(_: *anyopaque) !void {
                    return error.UnexpectedCpuTransfer;
                }
            };
            var state: State = .{ .writer = &writer };
            const controls: Apply.Acquisition = .{ .lifetime = .{ .context = &state, .check_fn = State.life }, .execution_wait = .{ .ptr = &state, .begin = State.transfer, .end = State.transfer } };
            var collection: Collection = .{};
            const control = collection.bindPreDecisionBeforeMutation(controls, std.testing.io, null).?;
            const result = if (prepare)
                manager.writeIntentsExtraBatchWithAdmission(id, &.{.{ .key = "row", .value = "{}" }}, &.{}, .{}, control)
            else
                manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(id, 1, 1, &.{}, true, true, control);
            try std.testing.expectError(error.Backpressured, result);
            if (cause) |failure| {
                // This invocation has its own rejected lifetime. It must not
                // turn a missing-record-shaped rejection into a fresh BEGIN.
                var scoped: State = .{ .writer = &writer, .cause = failure };
                var rejected = controls;
                rejected.lifetime.context = &scoped;
                var rejected_collection: Collection = .{};
                const rejected_control = rejected_collection.bindPreDecisionBeforeMutation(rejected, std.testing.io, null).?;
                if (prepare) {
                    try std.testing.expectError(failure, manager.writeIntentsExtraBatchWithAdmission(id, &.{.{ .key = "row", .value = "{}" }}, &.{}, .{}, rejected_control));
                } else {
                    try std.testing.expectError(failure, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(id, 1, 1, &.{}, true, true, rejected_control));
                }
            }
            var health: scheduling.Execution = .{};
            try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
            try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
            try writer.park(.resource_blocked);
            try producer.enter(.background);
            store.payload_policy_mutex.unlock(std.testing.io);
            policy_held = false;
            try producer.park(.resource_blocked);
            try writer.enter(.write);
            if (prepare) {
                try std.testing.expectEqual(Transactions.TxnStatus.pending, try manager.getTransactionStatus(id));
                try manager.writeIntentsExtraBatchWithAdmission(id, &.{.{ .key = "row", .value = "{}" }}, &.{}, .{}, control);
            } else {
                try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(id));
                try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(id, 1, 1, &.{}, true, true, control);
            }
            try std.testing.expectEqual(Transactions.TxnStatus.pending, try manager.getTransactionStatus(id));
            writer.finish();
            producer.finish();
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}

test "retained capture admission run source opens preserve original producer progress and independent authority" {
    try @import("storage/lsm_backend.zig").qualifyRunSourceOpenAdmission();
}

test "retained capture admission native artifact opens preserve physical lifetime after original scope retirement" {
    try @import("storage/lsm_backend.zig").qualifyNativeRunSourceOpenAdmission();
}

test "retained capture admission portable publication retires apply policy and rechecks target after original waits" {
    const acquire = @import("storage/db/portable_publication_admission.zig").acquire;
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Gate = @import("storage/db/row_policy_gate.zig").Gate;
    const Mode = enum { empty, success, changed_target, namespace_changed, range_changed, end_range_changed, destination_oom, destination_end_oom, empty_recovery, policy_changed, non_lending, canceled, expired, shutdown, io_canceled, begin_rejected, late_rejected, apply_busy, apply_busy_non_lending, initial_canceled, claimed_rejected, nonempty, competing, recovery };
    const Owner = struct {
        core: struct {
            store: *docstore.DocStore,
            apply_mutex: *Apply,
            identity_namespace: @import("storage/db/doc_identity_namespace.zig").Namespace = .{},
            range: @import("storage/byte_range.zig").ByteRange = .{ .start = "a", .end = "z" },
            pub fn byteRange(self: *@This()) @import("storage/byte_range.zig").ByteRange {
                return self.range;
            }
            pub fn lockApply(self: *@This()) void {
                self.apply_mutex.lockExclusive();
            }
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        },
        backend_runtime: struct {
            lane: std.Io,
            pub fn io(self: *@This()) ?std.Io {
                return self.lane;
            }
            pub fn filesystemIo(_: *@This()) ?std.Io {
                return null;
            }
        },
        local_execution: struct { row_policy_gate: Gate = .{} } = .{},
        checks: usize = 0,
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            self.core.lockApply();
        }
        pub fn pristine(self: *@This(), a: std.mem.Allocator, controls: ?Apply.Acquisition) !bool {
            if (controls) |original| try original.lifetime.check();
            self.checks += 1;
            try std.testing.expect(self.core.apply_mutex.state.load(.acquire) != 0);
            const value = self.core.store.get(a, "row") catch |err| switch (err) {
                error.NotFound => return true,
                else => return err,
            };
            defer a.free(value);
            return false;
        }
    };
    const Fixture = struct {
        owner: *Owner,
        scheduler: *scheduling.Scheduler,
        producer: *scheduling.Execution,
        publisher: *scheduling.Execution,
        prepared: *Gate.Lease,
        producer_policy: *Gate.Lease,
        reader: *docstore.DocStore.Txn,
        reader_open: bool,
        apply_held: bool,
        mode: Mode,
        failure: ?anyerror = null,
        waits: usize = 0,
        publication_waits: usize = 0,
        life_checks: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.life_checks += 1;
            if (self.life_checks > 4096) return error.UnboundedPublicationRetry;
            if (self.mode == .initial_canceled) return error.Canceled;
            if (self.mode == .empty_recovery and self.owner.core.store.portableImportPublicationInProgress()) self.owner.core.store.requirePortableImportRecovery();
            if (self.mode == .claimed_rejected and self.owner.core.store.portableImportPublicationInProgress()) return error.Canceled;
            if (self.failure) |cause| return cause;
            try self.publisher.checkLifetime();
        }
        fn retire(self: *@This()) void {
            if (self.reader_open) {
                self.reader.abort();
                self.reader_open = false;
            }
            self.producer_policy.release();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            try std.testing.expect(self.prepared.released);
            try std.testing.expectEqual(@as(usize, if (self.producer_policy.released) 0 else 1), self.owner.local_execution.row_policy_gate.readers.load(.acquire));
            if (self.mode == .begin_rejected) return error.Canceled;
            const publishing = self.owner.core.store.portableImportPublicationInProgress();
            if (publishing) {
                self.publication_waits += 1;
                try std.testing.expect(self.owner.core.apply_mutex.tryLockExclusive());
                self.owner.core.unlockApply();
                try std.testing.expectError(error.PortableImportPublicationInProgress, self.owner.core.store.beginProbeTxn());
                try std.testing.expectError(error.PortableImportPublicationInProgress, self.owner.core.store.claimPortableImportPublicationWithAdmission(null));
            } else try std.testing.expect(self.apply_held);
            try self.publisher.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            switch (self.mode) {
                .canceled, .expired, .shutdown, .io_canceled => return,
                else => {},
            }
            try self.producer.enter(.background);
            if (!publishing) {
                self.owner.core.unlockApply();
                self.apply_held = false;
            } else {
                if (self.mode == .changed_target) {
                    try std.testing.expect(self.owner.core.apply_mutex.tryLockExclusive());
                    self.owner.core.store.put("row", "new") catch |cause| {
                        self.owner.core.unlockApply();
                        return cause;
                    };
                    self.owner.core.unlockApply();
                }
                if (self.mode == .namespace_changed) self.owner.core.identity_namespace.table_id += 1;
                if (self.mode == .range_changed) @constCast(self.owner.core.range.start)[0] = 'b';
                if (self.mode == .end_range_changed) @constCast(self.owner.core.range.end)[0] = 'y';
                if (self.mode == .policy_changed) {
                    self.owner.local_execution.row_policy_gate.phase.store(@backingInt(@import("storage/db/table_catalog.zig").RowPolicyPhase.preparing), .release);
                    try std.testing.expectError(error.RowPolicyCatalogChanged, self.producer_policy.checkAt(0));
                }
                self.retire();
            }
            try self.producer.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            switch (self.mode) {
                .canceled => self.failure = error.Canceled,
                .expired => self.failure = error.DeadlineExceeded,
                .shutdown => self.scheduler.close(),
                else => {},
            }
            if (self.failure) |cause| return cause;
            try self.publisher.enter(.write);
            if (self.mode == .late_rejected) self.failure = error.Canceled;
        }
        fn sleepCanceled(_: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    for (std.enums.values(Mode)) |mode| {
        var backend = @import("storage/mem_backend.zig").Backend.init(a, .{});
        defer backend.close();
        var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        try store.put("seed", "old");
        if (mode == .nonempty) try store.put("row", "existing");
        var apply: Apply = .{ .io = std.testing.io };
        var io_table = std.testing.io.vtable.*;
        io_table.sleep = Fixture.sleepCanceled;
        const io: std.Io = if (mode == .io_canceled) .{ .userdata = std.testing.io.userdata, .vtable = &io_table } else std.testing.io;
        var lower = [_]u8{'a'};
        var upper = [_]u8{'z'};
        var owner: Owner = .{ .core = .{ .store = &store, .apply_mutex = &apply, .range = .{ .start = &lower, .end = &upper } }, .backend_runtime = .{ .lane = io } };
        var producer_policy = try owner.local_execution.row_policy_gate.enterRawRead();
        defer producer_policy.release();
        var prepared = try owner.local_execution.row_policy_gate.enterRaw();
        defer prepared.release();
        var reader = try store.beginReadTxn();
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var producer: scheduling.Execution = .{};
        defer producer.finish();
        var publisher: scheduling.Execution = .{};
        defer publisher.finish();
        try producer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .background);
        var fixture: Fixture = .{ .owner = &owner, .scheduler = &scheduler, .producer = &producer, .publisher = &publisher, .prepared = &prepared, .producer_policy = &producer_policy, .reader = &reader, .reader_open = true, .apply_held = false, .mode = mode };
        defer fixture.retire();
        if (mode == .empty or mode == .empty_recovery) fixture.retire();
        if (mode == .apply_busy or mode == .apply_busy_non_lending) {
            try std.testing.expect(apply.tryLockExclusive());
            fixture.apply_held = true;
        }
        defer if (fixture.apply_held) owner.core.unlockApply();
        if (mode == .competing) try store.claimPortableImportPublicationWithAdmission(null);
        var other_publication = mode == .competing;
        defer if (other_publication) store.cancelPortableImportPublication();
        if (mode == .recovery) store.requirePortableImportRecovery();
        try producer.park(.resource_blocked);
        try publisher.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
        const controls: Apply.Acquisition = .{ .lifetime = .{ .context = &fixture, .check_fn = Fixture.check }, .execution_wait = if (mode == .non_lending or mode == .apply_busy_non_lending) null else .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } };
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = if (mode == .destination_end_oom) 1 else 0 });
        const allocator = if (mode == .destination_oom or mode == .destination_end_oom) failing.allocator() else a;
        const result = acquire(&owner, allocator, &prepared, controls, Owner.pristine);
        const expected: ?anyerror = switch (mode) {
            .empty, .success, .apply_busy => null,
            .changed_target, .nonempty => error.LiteImportTargetNotEmpty,
            .namespace_changed, .range_changed, .end_range_changed => error.CatalogGenerationChanged,
            .destination_oom, .destination_end_oom => error.OutOfMemory,
            .policy_changed => error.RowPolicyAuthenticationRequired,
            .non_lending, .apply_busy_non_lending => error.Backpressured,
            .expired => error.DeadlineExceeded,
            .shutdown => error.SchedulerClosed,
            .competing => error.PortableImportPublicationInProgress,
            .recovery, .empty_recovery => error.PortableImportRecoveryRequired,
            else => error.Canceled,
        };
        if (expected) |cause| {
            try std.testing.expectError(cause, result);
            try std.testing.expectEqual(mode == .competing, store.portableImportPublicationInProgress());
            if (!fixture.apply_held) {
                try std.testing.expect(apply.tryLockExclusive());
                owner.core.unlockApply();
            }
            if (mode != .initial_canceled) try std.testing.expect(prepared.released);
        } else {
            try result;
            try std.testing.expect(store.portableImportPublicationInProgress());
            try std.testing.expect(!store.portableImportHasReaders());
            try std.testing.expect(!prepared.released);
            try std.testing.expect(!apply.tryLockExclusive());
            store.finishPortableImportPublication();
            owner.core.unlockApply();
        }
        if (mode == .changed_target) {
            const value = try store.get(a, "row");
            defer a.free(value);
            try std.testing.expectEqualStrings("new", value);
        }
        if (mode == .success or mode == .changed_target or mode == .policy_changed or mode == .apply_busy) try std.testing.expect(owner.checks >= 2 or mode == .policy_changed);
        if (mode == .success or mode == .changed_target or mode == .policy_changed) try std.testing.expectEqual(@as(usize, 1), fixture.publication_waits);
        if (mode == .apply_busy) try std.testing.expectEqual(@as(usize, 2), fixture.waits);
        if (fixture.reader_open) {
            try std.testing.expect(store.portableImportHasReaders());
            try std.testing.expectEqualStrings("old", try reader.get("seed"));
        }
        if (other_publication) {
            store.cancelPortableImportPublication();
            other_publication = false;
        }
        fixture.retire();
        prepared.release();
        publisher.finish();
        producer.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.local_execution.row_policy_gate.readers.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "retained capture admission pristine target probes bound history and preserve original rejection" {
    try @import("storage/db/portable_pristine_target.zig").qualify();
}

test "retained capture admission cursor exhaustion preserves original rejection and mandatory resume" {
    const erased = @import("storage/backend_erased.zig");
    const Mode = enum { empty, checkpoint, lifetime, progress, deadline, begin, end, parked };
    const State = struct {
        mode: Mode,
        cause: anyerror,
        checks: usize = 0,
        ends: usize = 0,
        closes: usize = 0,
        fn state(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn checkpoint(raw: *anyopaque) !void {
            const self = state(raw);
            self.checks += 1;
            if (self.mode == .checkpoint and self.checks > 1) return self.cause;
        }
        fn life(raw: *anyopaque) !void {
            const self = state(raw);
            if (self.mode == .lifetime or self.mode == .parked) return self.cause;
        }
        fn progress(raw: *anyopaque) !void {
            return state(raw).cause;
        }
        fn deadline(raw: *anyopaque) !?u64 {
            return state(raw).cause;
        }
        fn begin(raw: *anyopaque) !void {
            const self = state(raw);
            if (self.mode == .begin) return self.cause;
        }
        fn end(raw: *anyopaque) !void {
            const self = state(raw);
            self.ends += 1;
            if (self.mode == .end) return self.cause;
            if (self.mode == .parked) return error.Canceled;
        }
    };
    const Handle = struct {
        state: *State,
        pub fn close(self: *@This()) void {
            self.state.closes += 1;
        }
        pub fn first(_: *@This()) !erased.Entry {
            return error.NotFound;
        }
        pub const last = first;
        pub const next = first;
        pub const prev = first;
        pub fn seekAtOrAfter(_: *@This(), _: []const u8) !erased.Entry {
            return error.NotFound;
        }
        pub const seekAtOrBefore = seekAtOrAfter;
        pub fn setUpperBound(_: *@This(), _: ?[]const u8) void {}
        pub fn nextWithAdmission(self: *@This(), port: admission.Port) !erased.Entry {
            switch (self.state.mode) {
                .empty => {},
                .checkpoint => try port.checkpoint(),
                .lifetime => try port.checkLifetime(),
                .progress => try port.waitForProgress(),
                .deadline => _ = try port.deadlineNs(),
                .begin, .end, .parked => {
                    const boundary = (try port.executionWait()).?;
                    try boundary.beginWait();
                    if (self.state.mode == .parked) port.checkLifetime() catch {};
                    try boundary.endWait();
                },
            }
            return error.NotFound;
        }
        pub fn seekAtOrAfterWithAdmission(self: *@This(), _: []const u8, port: admission.Port) !erased.Entry {
            return self.nextWithAdmission(port);
        }
    };
    for (std.enums.values(Mode)) |mode| {
        for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.DeadlineExceeded }) |cause| {
            for ([_]bool{ false, true }) |seek| {
                var state: State = .{ .mode = mode, .cause = cause };
                const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint, .check_lifetime = State.life, .wait_for_progress = State.progress, .deadline_ns = State.deadline }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } };
                var cursor = try erased.cursorFrom(std.testing.allocator, Handle{ .state = &state });
                const result = if (seek) cursor.seekAtOrAfterWithAdmission("missing", admission.Port.init(&payload)) else cursor.nextWithAdmission(admission.Port.init(&payload));
                if (mode == .empty) try std.testing.expectEqual(@as(?erased.Entry, null), try result) else try std.testing.expectError(cause, result);
                try std.testing.expectEqual(@as(usize, if (mode == .end or mode == .parked) 1 else 0), state.ends);
                cursor.close();
                try std.testing.expectEqual(@as(usize, 1), state.closes);
            }
        }
    }
}
