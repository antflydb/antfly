// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Single-attempt admission and exact durable completion. Once a begin may
//! have crossed the proposal boundary, neither cancellation nor transport loss
//! permits replaying it. Only immutable job observations resolve uncertainty.
const std = @import("std");
const local = @import("antfly_local_sources");
const ledger = @import("../metadata/schema_validation_ledger.zig");
const receipts = @import("../metadata/schema_validation_receipt.zig");
const barriers = @import("../metadata/schema_validation_publication.zig");
const builder = @import("schema_validation_plan_builder.zig");
const catalog = local.sql_catalog;
const operation = local.api_operation;
const max_observation_bytes = 16 * 1024;

const BeginAttempt = union(enum) { reply: []u8, rejected: anyerror };

/// A source/report lag is a definite pre-proposal rejection, not an uncertain
/// admission. Keep the SAME owned plan and signed physical cuts while waiting
/// for authority reports to catch up. An unknown delivery is never retried.
fn beginWhenReady(source: anytype, a: std.mem.Allocator, io: std.Io, input: operation.RequestContext, command: []const u8) !BeginAttempt {
    var context = input.platformDeadline() catch |err| return .{ .rejected = err };
    const deadline = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s);
    context.deadline_ns = deadline;
    var retry_ns: u64 = 20 * std.time.ns_per_ms;
    while (true) {
        input.ensureActive() catch |err| return .{ .rejected = err };
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return .{ .rejected = error.CatalogPublicationProofPending };
        const reply = source.systemCatalog(a, context, .{ .schema_validation_submit = command }) catch |err| switch (@as(anyerror, err)) {
            // submitSchemaValidation wraps ALL post-proposal wait/read errors
            // in MetadataMutationOutcomeUnknown. Remote transports return
            // these names only with the explicit not-proposed disposition.
            error.IndexOwnerReceiptChanged, error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => {
                input.ensureActive() catch |inactive| return .{ .rejected = inactive };
                const remaining = deadline -| @import("antfly_platform").time.monotonicNs();
                if (remaining == 0) return .{ .rejected = error.CatalogPublicationProofPending };
                io.sleep(.fromNanoseconds(@min(remaining, retry_ns)), .awake) catch return .{ .rejected = error.Canceled };
                retry_ns = @min(retry_ns * 2, 250 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        return .{ .reply = reply };
    }
}

fn unresolved(input: catalog.DdlReceipt, known: bool) catalog.DdlOutcome {
    var receipt = input;
    receipt.state = if (known) .pending else .admission_unknown;
    receipt.diagnostic = if (known)
        "Schema validation was durably admitted. Retain the schema validation token and observe completion without replaying DDL."
    else
        "Schema validation admission is unresolved. Retain the schema validation token; absent or unavailable observations do not permit replaying DDL.";
    return .{ .mutation_outcome = if (known) .committed_pending else null, .receipt = receipt };
}

fn terminal(input: catalog.DdlReceipt, head: barriers.Head) !?catalog.DdlOutcome {
    try head.validate();
    return switch (head.phase) {
        .succeeded => blk: {
            var receipt = input;
            receipt.state = .ready;
            receipt.diagnostic = "Schema validation, installation, publication and all owner fence releases completed. Later DDL may supersede this schema.";
            break :blk .{ .mutation_outcome = .committed, .receipt = receipt };
        },
        .canceled => switch (head.failure) {
            .not_null_violation => error.SqlNotNullViolation,
            .requested => error.Canceled,
            .owner_failed, .none => error.SchemaValidationJobChanged,
        },
        else => null,
    };
}

fn observation(a: std.mem.Allocator, bytes: []const u8, lookup: ledger.Lookup) !?barriers.Head {
    if (bytes.len == 0 or bytes.len > max_observation_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(receipts.Observation, a, bytes, .{});
    defer parsed.deinit();
    if (!try parsed.value.isAcknowledged(.{ .lookup = lookup, .expected = .admission })) return null;
    return parsed.value.head;
}

fn read(source: anytype, a: std.mem.Allocator, context: operation.RequestContext, lookup: ledger.Lookup) !?barriers.Head {
    const bytes = try source.systemCatalog(a, context, .{ .schema_validation_receipt_read = .{ .lookup = lookup, .expected = .admission } });
    defer a.free(bytes);
    return observation(a, bytes, lookup);
}

/// Only explicit caller cancellation/deadline can request cancellation. Give
/// cleanup its own short budget, but never return rollback before the complete
/// terminal cohort proves that every physical fence has been released.
fn interrupted(source: anytype, a: std.mem.Allocator, io: std.Io, original: operation.RequestContext, lookup: ledger.Lookup, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion, initial_known: bool) !catalog.DdlOutcome {
    if (completion != .until_deadline) return unresolved(receipt, initial_known);
    if (original.ensureActive()) |_| {
        return unresolved(receipt, initial_known);
    } else |err| if (err != error.Canceled and err != error.DeadlineExceeded) return unresolved(receipt, initial_known);
    var context = original;
    context.cancellation = .none;
    context.deadline_io = null;
    context.deadline_ns = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s;
    var attempted_revision: ?u64 = null;
    var known = initial_known;
    while (true) {
        context.ensureActive() catch return unresolved(receipt, known);
        const head = (read(source, a, context, lookup) catch return unresolved(receipt, known)) orelse return unresolved(receipt, known);
        known = true;
        if (try terminal(receipt, head)) |outcome| return outcome;
        switch (head.phase) {
            .installing, .installed, .releasing => return unresolved(receipt, true),
            .validating, .validated => if (attempted_revision == null or head.revision > attempted_revision.?) {
                attempted_revision = head.revision;
                const command = try std.json.Stringify.valueAlloc(a, ledger.Command{ .cancel = .{ .decision = .{ .lookup = lookup, .revision = head.revision }, .failure = .requested } }, .{});
                defer a.free(command);
                const reply = source.systemCatalog(a, context, .{ .schema_validation_submit = command }) catch null;
                if (reply) |bytes| a.free(bytes);
                // A newer durable head, not the cancel reply, is the only
                // reason another revision-CAS may become eligible.
            },
            .canceling => {},
            .succeeded, .canceled => unreachable,
        }
        io.sleep(.fromMilliseconds(20), .awake) catch return unresolved(receipt, known);
    }
}

fn awaitCompletion(source: anytype, a: std.mem.Allocator, io: std.Io, original: operation.RequestContext, lookup: ledger.Lookup, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion, initial: ?barriers.Head) !catalog.DdlOutcome {
    var head = initial;
    var known = initial != null;
    var context = original.platformDeadline() catch return interrupted(source, a, io, original, lookup, receipt, completion, known);
    if (completion == .bounded) context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s);
    while (true) {
        if (head) |value| if (try terminal(receipt, value)) |outcome| return outcome;
        context.ensureActive() catch return interrupted(source, a, io, original, lookup, receipt, completion, known);
        head = read(source, a, context, lookup) catch return interrupted(source, a, io, original, lookup, receipt, completion, known);
        if (head == null) return unresolved(receipt, known);
        known = true;
        if (try terminal(receipt, head.?)) |outcome| return outcome;
        io.sleep(.fromMilliseconds(20), .awake) catch return interrupted(source, a, io, original, lookup, receipt, completion, known);
    }
}

/// Consumes owned on every outcome. Receipt/token storage must already belong
/// to the result allocator, so post-admission OOM cannot erase recovery identity.
pub fn submitOwned(source: anytype, a: std.mem.Allocator, io: std.Io, context: operation.RequestContext, owned: *builder.Owned, receipt: catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion) !catalog.DdlOutcome {
    var released = false;
    defer if (!released) owned.deinit();
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    if (!std.mem.eql(u8, receipt.schema_validation_token orelse return error.InvalidGenerationPublication, &owned.recovery_token)) return error.InvalidGenerationPublication;
    const lookup = owned.lookup;
    const attempt = beginWhenReady(source, a, io, context, owned.command) catch |err| switch (@as(anyerror, err)) {
        // Definite pre-admission rejections. Errors after proposal/wait use
        // MetadataMutationOutcomeUnknown; never infer non-send from an error.
        error.MetadataReplicationPending => return error.CatalogPublicationProofPending,
        error.Forbidden, error.CatalogGenerationChanged, error.GenerationPublicationChanged, error.MetadataIncarnationMismatch, error.TableTopologyProtocolUpgradeRequired, error.CatalogOccupancyNotReady, error.CatalogPublicationProofPending, error.UnsupportedNativeIndexOwner, error.RowPolicyUnsupported => return err,
        else => {
            owned.deinit();
            released = true;
            return awaitCompletion(source, a, io, context, lookup, receipt, completion, null);
        },
    };
    const reply = switch (attempt) {
        .reply => |bytes| bytes,
        .rejected => |err| return err,
    };
    owned.deinit();
    released = true;
    defer a.free(reply);
    const initial = observation(a, reply, lookup) catch null;
    return awaitCompletion(source, a, io, context, lookup, receipt, completion, initial);
}

test "system catalog schema validation admission waits for known nonsends with one prepared identity and never retries uncertainty" {
    const Source = struct {
        mode: enum { lag, unknown, lag_then_unknown },
        calls: usize = 0,
        pub fn systemCatalog(self: *@This(), a: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(call == .schema_validation_submit);
            try std.testing.expectEqualStrings("same-owned-command", call.schema_validation_submit);
            self.calls += 1;
            if (self.mode != .unknown and self.calls == 1) return error.IndexOwnerReceiptChanged;
            if (self.mode != .lag) return error.MetadataMutationOutcomeUnknown;
            return a.dupe(u8, "accepted");
        }
    };
    const a = std.testing.allocator;
    for ([_]@FieldType(Source, "mode"){ .lag, .unknown, .lag_then_unknown }) |mode| {
        var source: Source = .{ .mode = mode };
        if (mode == .lag) {
            const attempt = try beginWhenReady(&source, a, std.testing.io, .{}, "same-owned-command");
            try std.testing.expect(attempt == .reply);
            defer a.free(attempt.reply);
            try std.testing.expectEqualStrings("accepted", attempt.reply);
        } else try std.testing.expectError(error.MetadataMutationOutcomeUnknown, beginWhenReady(&source, a, std.testing.io, .{}, "same-owned-command"));
        try std.testing.expectEqual(@as(usize, if (mode == .unknown) 1 else 2), source.calls);
    }
    var expired: Source = .{ .mode = .lag };
    const attempt = try beginWhenReady(&expired, a, std.testing.io, .{ .deadline_ns = 0 }, "same-owned-command");
    try std.testing.expect(attempt == .rejected and attempt.rejected == error.DeadlineExceeded);
    try std.testing.expectEqual(@as(usize, 0), expired.calls);
}
