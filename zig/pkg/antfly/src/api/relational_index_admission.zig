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

//! One admission, exact read-only recovery, and terminal SQL completion.
const std = @import("std");
const local = @import("antfly_local_sources");
const publication = @import("../metadata/relational_index_publication.zig");
const builder = @import("relational_index_plan_builder.zig");
const catalog = local.sql_catalog;
const operation = local.api_operation;
const Head = local.storage_db_relational_index_publication_progress.Head;

fn unknown(input: catalog.DdlReceipt) catalog.DdlOutcome {
    var receipt = input;
    receipt.state = .admission_unknown;
    receipt.diagnostic = "Index build admission is unresolved. Retain the index publication token, observe its exact job, and do not replay the DDL.";
    return .{ .mutation_outcome = null, .receipt = receipt };
}

fn pending(input: catalog.DdlReceipt) catalog.DdlOutcome {
    var receipt = input;
    receipt.state = .pending;
    receipt.diagnostic = "Index build was durably admitted but has not confirmed terminal publication. Observe its exact job using the retained token; do not replay the DDL.";
    return .{ .mutation_outcome = .committed_pending, .receipt = receipt };
}

fn observation(alloc: std.mem.Allocator, bytes: []const u8, identity: publication.AdmissionRequest) !?Head {
    if (bytes.len == 0 or bytes.len > publication.AdmissionStatus.max_wire_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(publication.AdmissionStatus, alloc, bytes, .{});
    defer parsed.deinit();
    if (!try parsed.value.isAdmitted(identity)) return null;
    return parsed.value.head;
}

fn terminal(input: catalog.DdlReceipt, head: Head) !?catalog.DdlOutcome {
    switch (head.phase) {
        .published => {
            var receipt = input;
            receipt.state = .ready;
            receipt.diagnostic = "The exact index build completed publication and released its owner fences. Later DDL may supersede this generation.";
            return .{ .mutation_outcome = .committed, .receipt = receipt };
        },
        .canceled => return switch (head.failure) {
            .duplicate_key => error.UniqueConstraintViolation,
            .requested => error.Canceled,
            .invalid_candidate, .none => error.InvalidIndexPublicationCandidate,
        },
        else => return null,
    }
}

fn interrupted(source: anytype, alloc: std.mem.Allocator, io: std.Io, original: operation.RequestContext, identity: publication.AdmissionRequest, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion, known: bool) !catalog.DdlOutcome {
    const unresolved = if (known) pending(receipt) else unknown(receipt);
    if (completion != .until_deadline) return unresolved;
    // Executor/transport errors alone are not a client cancellation request.
    if (original.ensureActive()) |_| {
        return unresolved;
    } else |err| {
        if (err != error.Canceled and err != error.DeadlineExceeded) return unresolved;
    }
    var context = original;
    context.cancellation = .none;
    context.deadline_io = null;
    context.deadline_ns = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s;
    var attempted_revision: ?u64 = null;
    var admitted = known;
    while (true) {
        context.ensureActive() catch return if (admitted) pending(receipt) else unknown(receipt);
        const bytes = source.systemCatalog(alloc, context, .{ .relational_index_admission_status = identity }) catch return if (admitted) pending(receipt) else unknown(receipt);
        const observed = observation(alloc, bytes, identity);
        alloc.free(bytes);
        const head = (observed catch return if (admitted) pending(receipt) else unknown(receipt)) orelse return if (admitted) pending(receipt) else unknown(receipt);
        admitted = true;
        if (try terminal(receipt, head)) |done| return done;
        switch (head.phase) {
            .installing, .releasing => {
                var outcome = pending(receipt);
                outcome.receipt.?.diagnostic = "Cancellation raced irreversible index publication. Retain the publication token and reconcile completion; rollback is not confirmed.";
                return outcome;
            },
            .fencing, .reserving, .building => {
                // A strictly newer committed PRE-publication head proves an
                // old revision-CAS cannot have canceled this job or do so later.
                // At the same revision a missing ACK never permits replay.
                if (attempted_revision == null or head.revision > attempted_revision.?) {
                    attempted_revision = head.revision;
                    const reply = source.systemCatalog(alloc, context, .{ .relational_index_cancel = .{ .table_id = identity.table_id, .id = identity.id, .expected_revision = head.revision, .failure = .requested, .expected = .{ .metadata_incarnation = identity.metadata_incarnation, .plan_digest = identity.plan_digest } } }) catch null;
                    if (reply) |value| alloc.free(value);
                    // Exact admission observations, not the cancel ACK, prove
                    // metadata incarnation, plan commitment and cleanup state.
                }
            },
            .canceling, .cancel_releasing => {},
            .published, .canceled => unreachable,
        }
        io.sleep(.fromMilliseconds(20), .awake) catch return pending(receipt);
    }
}

fn awaitCompletion(source: anytype, alloc: std.mem.Allocator, io: std.Io, original: operation.RequestContext, identity: publication.AdmissionRequest, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion, initial: Head) !catalog.DdlOutcome {
    if (try terminal(receipt, initial)) |done| return done;
    // Admission is already known. A later unavailable/unmirrored observation
    // cannot revoke that fact, but cannot prove successful completion either.
    var context = original.platformDeadline() catch return interrupted(source, alloc, io, original, identity, receipt, completion, true);
    if (completion == .bounded) context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s);
    while (true) {
        context.ensureActive() catch return interrupted(source, alloc, io, original, identity, receipt, completion, true);
        io.sleep(.fromMilliseconds(20), .awake) catch return interrupted(source, alloc, io, original, identity, receipt, completion, true);
        context.ensureActive() catch return interrupted(source, alloc, io, original, identity, receipt, completion, true);
        const bytes = source.systemCatalog(alloc, context, .{ .relational_index_admission_status = identity }) catch return interrupted(source, alloc, io, original, identity, receipt, completion, true);
        defer alloc.free(bytes);
        const head = (observation(alloc, bytes, identity) catch return interrupted(source, alloc, io, original, identity, receipt, completion, true)) orelse return interrupted(source, alloc, io, original, identity, receipt, completion, true);
        if (try terminal(receipt, head)) |done| return done;
    }
}

fn recover(source: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, identity: publication.AdmissionRequest, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion) !catalog.DdlOutcome {
    context.ensureActive() catch return interrupted(source, alloc, io, context, identity, receipt, completion, false);
    const bytes = source.systemCatalog(alloc, context, .{ .relational_index_admission_status = identity }) catch return interrupted(source, alloc, io, context, identity, receipt, completion, false);
    defer alloc.free(bytes);
    const head = (observation(alloc, bytes, identity) catch return unknown(receipt)) orelse return unknown(receipt);
    return awaitCompletion(source, alloc, io, context, identity, receipt, completion, head);
}

/// The result-owned receipt (including its token) must exist before entering
/// this function. Response allocation, transport loss and cancellation after
/// begin can therefore never erase recovery identity or cause a second begin.
/// Consumes owned on EVERY outcome, releasing the schema/catalog/cohort before
/// any recovery read or potentially long SQL wait. Only fixed identity remains.
pub fn submitOwned(source: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, owned: *builder.Owned, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion) !catalog.DdlOutcome {
    var released = false;
    defer if (!released) owned.deinit();
    if (!context.setting_admin or !context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    if (!std.mem.eql(u8, receipt.index_publication_token orelse return error.InvalidGenerationPublication, &owned.recovery_token) or
        !std.meta.eql(try owned.request.observation(&owned.prepared), owned.observation)) return error.InvalidGenerationPublication;
    const identity = owned.observation;
    const bytes = source.systemCatalog(alloc, context, .{ .relational_index_begin = owned.request }) catch |err| switch (@as(anyerror, err)) {
        // These coordinator errors are definite pre-proposal rejections. All
        // apply/wait/post-commit failures use MetadataMutationOutcomeUnknown.
        error.MetadataReplicationPending => return error.CatalogPublicationProofPending,
        error.Forbidden, error.CatalogAlreadyExists, error.CatalogGenerationChanged, error.GenerationPublicationChanged, error.UnsupportedNativeIndexOwner, error.TableTopologyProtocolUpgradeRequired, error.CatalogOccupancyNotReady, error.CatalogPublicationProofPending => return err,
        else => {
            owned.deinit();
            released = true;
            return recover(source, alloc, io, context, identity, receipt, completion);
        },
    };
    owned.deinit();
    released = true;
    defer alloc.free(bytes);
    const head = (observation(alloc, bytes, identity) catch return recover(source, alloc, io, context, identity, receipt, completion)) orelse return recover(source, alloc, io, context, identity, receipt, completion);
    return awaitCompletion(source, alloc, io, context, identity, receipt, completion, head);
}

const Testing = if (@import("builtin").is_test) struct {
    fn owned(a: std.mem.Allocator) !builder.Owned {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        var plan = try publication.testing.plan(arena.allocator());
        const owners = try arena.allocator().dupe(publication.Owner, plan.owners);
        owners[0].receipt_authority = .native;
        plan.owners = owners;
        var prepared = try plan.prepare(a);
        errdefer prepared.deinit();
        const request: publication.BeginRequest = .{ .metadata_incarnation = "01010101010101010101010101010101".*, .begin = .{ .plan = plan } };
        const identity = try request.observation(&prepared);
        return .{ .arena = arena, .request = request, .prepared = prepared, .observation = identity, .recovery_token = try @import("index_publication_recovery.zig").Token.encode(identity) };
    }
    const Source = struct {
        identity: publication.AdmissionRequest,
        head: Head,
        memory: *std.testing.FailingAllocator,
        mode: enum { published, lost_reply, oom_reply, absent, foreign, unmirrored, canceled_request, duplicate, cleanup, progress, definite, replication_blocked, corrupt, oversize },
        begins: usize = 0,
        reads: usize = 0,
        canceled: std.atomic.Value(bool) = .init(false),
        fn isCanceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.canceled.load(.acquire);
        }
        pub fn systemCatalog(self: *@This(), a: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.setting_admin and context.index_publication_authority);
            const beginning = call == .relational_index_begin;
            if (beginning) {
                self.begins += 1;
                try std.testing.expectEqual(@as(usize, 1), self.begins);
                if (self.mode == .definite) return error.CatalogAlreadyExists;
                if (self.mode == .replication_blocked) return error.MetadataReplicationPending;
                if (self.mode == .oom_reply) return error.OutOfMemory;
                if (self.mode == .lost_reply or self.mode == .absent or self.mode == .foreign) return error.MetadataMutationOutcomeUnknown;
            } else {
                try std.testing.expect(call == .relational_index_admission_status);
                try std.testing.expectEqualDeep(self.identity, call.relational_index_admission_status);
                try std.testing.expect(self.memory.allocated_bytes - self.memory.freed_bytes <= publication.AdmissionStatus.max_wire_bytes + 1);
                self.reads += 1;
            }
            if (self.mode == .corrupt) return a.dupe(u8, "{}");
            if (self.mode == .oversize) return a.alloc(u8, publication.AdmissionStatus.max_wire_bytes + 1);
            var head = self.head;
            head.revision = 10;
            head.phase = .published;
            head.completed_sources = head.owner_count;
            head.completion_revision = 3;
            head.counts = .{ 1, 1, 1, 1, 1, 0, 0 };
            if (self.mode == .canceled_request) {
                head = self.head;
                self.canceled.store(true, .release);
            }
            if (self.mode == .duplicate or self.mode == .cleanup) {
                head.phase = if (self.mode == .cleanup and beginning) .cancel_releasing else .canceled;
                head.failure = .duplicate_key;
                head.cancellation_revision = 8;
                head.completed_sources = 0;
                head.completion_revision = 0;
                head.counts = .{ 1, 1, 0, 0, 0, 1, if (head.phase == .canceled) 1 else 0 };
            }
            if (self.mode == .progress and (beginning or self.reads == 1)) {
                head.phase = .releasing;
                head.counts[4] = 0;
            }
            var identity = self.identity.metadata_incarnation;
            if (self.mode == .foreign) identity[0] = '2';
            return std.json.Stringify.valueAlloc(a, publication.AdmissionStatus{ .metadata_incarnation = identity, .head = if (self.mode == .absent) null else head, .replication_pending = self.mode == .unmirrored }, .{});
        }
    };
} else struct {};

test "relational index publication SQL admission preserves preowned handles and never repeats an uncertain begin" {
    const a = std.testing.allocator;
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    for ([_]@FieldType(Testing.Source, "mode"){ .published, .lost_reply, .oom_reply, .absent, .foreign, .unmirrored, .canceled_request, .duplicate, .cleanup, .progress, .definite, .replication_blocked, .corrupt, .oversize }) |mode| {
        var memory = std.testing.FailingAllocator.init(a, .{});
        const scratch = memory.allocator();
        var owned = try Testing.owned(scratch);
        const token = owned.recovery_token;
        try std.testing.expect(memory.allocated_bytes - memory.freed_bytes > publication.AdmissionStatus.max_wire_bytes + 1);
        const receipt: catalog.DdlReceipt = .{ .database = "default", .namespace = "public", .table = "items", .table_id = "101", .schema_version = 2, .state = .pending, .index_publication_token = &token };
        var source: Testing.Source = .{ .identity = owned.observation, .head = owned.prepared.head, .memory = &memory, .mode = mode };
        const context: operation.RequestContext = .{ .setting_admin = true, .index_publication_authority = true, .cancellation = .{ .ptr = &source, .is_cancelled_fn = Testing.Source.isCanceled } };
        const result = submitOwned(&source, scratch, threaded.io(), context, &owned, receipt, if (mode == .canceled_request) .bounded else .until_deadline);
        try std.testing.expectEqual(memory.allocated_bytes, memory.freed_bytes);
        if (mode == .duplicate or mode == .cleanup) {
            try std.testing.expectError(error.UniqueConstraintViolation, result);
            try std.testing.expectEqual(@as(usize, if (mode == .cleanup) 1 else 0), source.reads);
        } else if (mode == .definite) {
            try std.testing.expectError(error.CatalogAlreadyExists, result);
            try std.testing.expectEqual(@as(usize, 0), source.reads);
        } else if (mode == .replication_blocked) {
            try std.testing.expectError(error.CatalogPublicationProofPending, result);
            try std.testing.expectEqual(@as(usize, 0), source.reads);
        } else {
            const outcome = try result;
            const state: catalog.DdlReceipt.State = switch (mode) {
                .published, .lost_reply, .oom_reply, .progress => .ready,
                .canceled_request => .pending,
                else => .admission_unknown,
            };
            try std.testing.expectEqual(state, outcome.receipt.?.state);
            try std.testing.expectEqualStrings(&token, outcome.receipt.?.index_publication_token.?);
            try std.testing.expectEqual(if (state == .ready) catalog.MutationOutcome.committed else if (state == .pending) catalog.MutationOutcome.committed_pending else @as(?catalog.MutationOutcome, null), outcome.mutation_outcome);
            if (mode == .progress) try std.testing.expectEqual(@as(usize, 2), source.reads);
            if (mode == .lost_reply or mode == .oom_reply) try std.testing.expectEqual(@as(usize, 1), source.reads);
        }
        try std.testing.expectEqual(@as(usize, 1), source.begins);
    }
}

test "relational index publication SQL interruption cancels only exact prepublication jobs and waits for cleanup" {
    const Source = struct {
        identity: publication.AdmissionRequest,
        base: Head,
        mode: enum { cleanup, lost_ack, stale_cas, unknown_ack, irreversible, published, unmirrored, foreign, absent },
        reads: usize = 0,
        cancels: usize = 0,
        fn canceled(_: *const anyopaque) bool {
            return true;
        }
        pub fn systemCatalog(self: *@This(), a: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.setting_admin and context.index_publication_authority);
            try std.testing.expect(!context.cancellation.isCancelled());
            try context.ensureActive();
            try std.testing.expect(context.deadline_io == null and context.deadline_ns != null);
            if (call == .relational_index_cancel) {
                self.cancels += 1;
                const request = call.relational_index_cancel;
                try std.testing.expectEqualDeep(self.identity.metadata_incarnation, request.expected.?.metadata_incarnation);
                try std.testing.expectEqualDeep(self.identity.plan_digest, request.expected.?.plan_digest);
                try std.testing.expectEqualDeep(self.identity.id, request.id);
                try std.testing.expectEqual(self.identity.table_id, request.table_id);
                try std.testing.expectEqual(local.storage_db_relational_index_publication_progress.Failure.requested, request.failure);
                try std.testing.expectEqual(@as(u64, if (self.mode == .stale_cas and self.cancels == 2) 2 else 1), request.expected_revision);
                if (self.mode == .stale_cas and self.cancels == 1) return error.GenerationPublicationChanged;
                if (self.mode == .lost_ack or self.mode == .unknown_ack) return error.MetadataMutationOutcomeUnknown;
                // A cancel reply alone never establishes cleanup. Deliberately
                // leave its body unusable; only exact status proves completion.
                return a.dupe(u8, "{}");
            }
            try std.testing.expect(call == .relational_index_admission_status);
            try std.testing.expectEqualDeep(self.identity, call.relational_index_admission_status);
            self.reads += 1;
            if (self.mode == .unknown_ack and self.reads == 3) return error.ResourceTemporarilyUnavailable;
            var head = self.base;
            if (self.mode == .stale_cas and self.reads >= 2) head.revision = 2;
            if (self.mode == .irreversible or self.mode == .published) {
                head.phase = if (self.mode == .published) .published else .installing;
                head.revision = 10;
                head.counts = .{ 1, 1, 1, if (self.mode == .published) 1 else 0, if (self.mode == .published) 1 else 0, 0, 0 };
            } else if (self.cancels != 0 and self.mode != .unknown_ack and !(self.mode == .lost_ack and self.reads == 2) and !(self.mode == .stale_cas and self.cancels == 1)) {
                const complete = self.reads >= (if (self.mode == .lost_ack) @as(usize, 4) else 3);
                head.phase = if (complete) .canceled else .cancel_releasing;
                head.revision = 10;
                head.failure = .requested;
                head.cancellation_revision = 8;
                head.counts = .{ 1, 1, 0, 0, 0, 1, if (complete) 1 else 0 };
            }
            var incarnation = self.identity.metadata_incarnation;
            if (self.mode == .foreign) incarnation[0] = '2';
            return std.json.Stringify.valueAlloc(a, publication.AdmissionStatus{ .metadata_incarnation = incarnation, .head = if (self.mode == .absent) null else head, .replication_pending = self.mode == .unmirrored }, .{});
        }
    };
    const a = std.testing.allocator;
    var owned = try Testing.owned(a);
    const identity = owned.observation;
    const base = owned.prepared.head;
    const token = owned.recovery_token;
    owned.deinit();
    const receipt: catalog.DdlReceipt = .{ .database = "default", .namespace = "public", .table = "items", .table_id = "101", .schema_version = 2, .state = .pending, .index_publication_token = &token };
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    for ([_]@FieldType(Source, "mode"){ .cleanup, .lost_ack, .stale_cas, .unknown_ack, .irreversible, .published, .unmirrored, .foreign, .absent }) |mode| {
        var source: Source = .{ .identity = identity, .base = base, .mode = mode };
        const context: operation.RequestContext = .{ .setting_admin = true, .index_publication_authority = true, .cancellation = .{ .ptr = &source, .is_cancelled_fn = Source.canceled } };
        const known = mode != .absent;
        const result = interrupted(&source, a, threaded.io(), context, identity, receipt, .until_deadline, known);
        if (mode == .cleanup or mode == .lost_ack or mode == .stale_cas) {
            try std.testing.expectError(error.Canceled, result);
            try std.testing.expectEqual(@as(usize, if (mode == .stale_cas) 2 else 1), source.cancels);
            try std.testing.expectEqual(@as(usize, if (mode == .lost_ack) 4 else 3), source.reads);
        } else {
            const outcome = try result;
            try std.testing.expectEqual(if (mode == .published) catalog.DdlReceipt.State.ready else if (mode == .absent) catalog.DdlReceipt.State.admission_unknown else catalog.DdlReceipt.State.pending, outcome.receipt.?.state);
            try std.testing.expectEqualStrings(&token, outcome.receipt.?.index_publication_token.?);
            try std.testing.expectEqual(@as(usize, if (mode == .unknown_ack) 1 else 0), source.cancels);
        }
    }
    var http: Source = .{ .identity = identity, .base = base, .mode = .cleanup };
    const expired: operation.RequestContext = .{ .setting_admin = true, .index_publication_authority = true, .deadline_ns = 0 };
    try std.testing.expectEqual(catalog.DdlReceipt.State.pending, (try interrupted(&http, a, threaded.io(), expired, identity, receipt, .bounded, true)).receipt.?.state);
    try std.testing.expectEqual(@as(usize, 0), http.reads + http.cancels);
    _ = interrupted(&http, a, threaded.io(), expired, identity, receipt, .until_deadline, true) catch |err| {
        try std.testing.expectEqual(error.Canceled, err);
        return;
    };
    return error.TestExpectedError;
}

test "relational index publication SQL admission consumes rejected plans before any IO" {
    const a = std.testing.allocator;
    const Mode = enum { forbidden, canceled, wrong_token };
    for ([_]Mode{ .forbidden, .canceled, .wrong_token }) |mode| {
        var memory = std.testing.FailingAllocator.init(a, .{});
        const scratch = memory.allocator();
        var owned = try Testing.owned(scratch);
        var token = owned.recovery_token;
        if (mode == .wrong_token) token[0] = if (token[0] == 'A') 'B' else 'A';
        const receipt: catalog.DdlReceipt = .{ .database = "default", .namespace = "public", .table = "items", .table_id = "101", .schema_version = 2, .state = .pending, .index_publication_token = &token };
        var source: Testing.Source = .{ .identity = owned.observation, .head = owned.prepared.head, .memory = &memory, .mode = .published };
        source.canceled.store(mode == .canceled, .release);
        const context: operation.RequestContext = .{ .setting_admin = mode != .forbidden, .index_publication_authority = true, .cancellation = .{ .ptr = &source, .is_cancelled_fn = Testing.Source.isCanceled } };
        const expected: anyerror = switch (mode) {
            .forbidden => error.Forbidden,
            .canceled => error.Canceled,
            .wrong_token => error.InvalidGenerationPublication,
        };
        try std.testing.expectError(expected, submitOwned(&source, scratch, std.testing.io, context, &owned, receipt, .until_deadline));
        try std.testing.expectEqual(memory.allocated_bytes, memory.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), source.begins);
        try std.testing.expectEqual(@as(usize, 0), source.reads);
    }
}
