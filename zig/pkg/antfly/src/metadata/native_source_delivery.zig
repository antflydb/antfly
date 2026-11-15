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

//! Single-root native destination adapter. Distributed destinations require
//! their own coordinator/participant authority and cannot use this capability.
//! Allocating methods borrow an attempt arena, except release which owns its
//! temporary arena. Receipts contain bounded recovery authority, never row WAL.
const std = @import("std");
const local = @import("antfly_local_sources").api_local_table_writes;
const control = @import("source_control.zig");
const delivery = @import("source_delivery.zig");
const admission = local.source_admission;
const A = std.mem.Allocator;
const Digest = control.Digest;
const Table = local.distributed_txn.TableCommitRequest;

const Manifest = struct {
    version: u32 = 1,
    namespace: [24]u8,
    original_table_name: []const u8,
    effects: Digest,
};
const Receipt = struct {
    version: u32 = 1,
    binding: Digest,
    namespace: [24]u8,
    original_table_name: []const u8,
    reservation: Digest,
    effects: Digest,
    transaction_id: [16]u8,
    batch: []const u8,
    checkpoint: []const u8,
    previous: ?[]const u8,
    payload: Digest,
    sequence: u64,
    accepted_at_ns: u64,
};
fn encode64(a: A, bytes: []const u8) ![]const u8 {
    return std.base64.standard.Encoder.encode(try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len)), bytes);
}
fn decode64(a: A, bytes: []const u8, max: usize) ![]const u8 {
    const size = std.base64.standard.Decoder.calcSizeForSlice(bytes) catch return error.SourceAdmissionCorrupt;
    if (size == 0 or size > max) return error.SourceAdmissionCorrupt;
    const out = try a.alloc(u8, size);
    std.base64.standard.Decoder.decode(out, bytes) catch return error.SourceAdmissionCorrupt;
    return out;
}
fn validName(name: []const u8) bool {
    return name.len > 0 and name.len <= 255 and std.unicode.utf8ValidateSlice(name);
}
pub fn binding(namespace: [24]u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:native-source-destination:v1:");
    hash.update(&namespace);
    return hash.finalResult();
}
fn planDigest(scope: control.Scope, bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:native-source-plan:v1:");
    hash.update(&scope.identity());
    hash.update(bytes);
    return hash.finalResult();
}

pub const Adapter = struct {
    source: local.TableWriteSource,
    namespace: [24]u8,
    table_name: []const u8,

    pub fn init(source: local.TableWriteSource, table_name: []const u8) !Adapter {
        if (!validName(table_name)) return error.InvalidSourceAdmission;
        return .{ .source = source, .namespace = try source.nativeSourceAdmissionNamespace(), .table_name = table_name };
    }
    pub fn validateScope(self: *Adapter, scope: control.Scope) !void {
        try scope.validate();
        const actual = try self.source.nativeSourceAdmissionNamespace();
        if (scope.destination != .native or scope.table_id != std.mem.readInt(u64, self.namespace[0..8], .big) or
            !std.mem.eql(u8, &actual, &self.namespace) or !std.mem.eql(u8, &scope.destination_binding, &binding(actual))) return error.SourceBindingConflict;
        if (self.source.vtable.resolve_source_admission == null or self.source.vtable.release_source_admission == null or
            self.source.vtable.admit_source_transaction == null) return error.UnsupportedOperation;
    }
    fn manifest(self: *Adapter, a: A, scope: control.Scope, pending: control.Reservation) !Manifest {
        try self.validateScope(scope);
        try pending.validate();
        if (pending.destination != .native or !std.mem.eql(u8, &pending.plan_digest, &planDigest(scope, pending.delivery_manifest))) return error.SourcePayloadConflict;
        const value = std.json.parseFromSliceLeaky(Manifest, a, pending.delivery_manifest, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.SourceAdmissionCorrupt,
        };
        if (value.version != 1 or !std.mem.eql(u8, &value.namespace, &self.namespace) or !validName(value.original_table_name)) return error.SourceBindingConflict;
        return value;
    }
    fn command(a: A, scope: control.Scope, batch: []const u8, checkpoint: []const u8, previous: ?[]const u8, payload: Digest, effects: Digest, sequence: u64, timestamp: u64) !admission.Command {
        const result: admission.Command = .{
            .table_id = scope.table_id,
            .generation = scope.generation,
            .source = try a.dupe(u8, &std.fmt.bytesToHex(scope.source_id, .lower)),
            .epoch = try a.dupe(u8, &std.fmt.bytesToHex(scope.identity(), .lower)),
            .definition_digest = scope.definition,
            .batch_id = batch,
            .source_sequence = sequence,
            .checkpoint = checkpoint,
            .expected_checkpoint = previous,
            .payload_digest = payload,
            .participant_digest = effects,
            .accepted_at_ns = timestamp,
        };
        try result.validate();
        return result;
    }
    fn pendingCommand(a: A, scope: control.Scope, pending: control.Reservation, original: Manifest) !admission.Command {
        const result = try command(a, scope, pending.batch_id, pending.checkpoint, pending.expected_checkpoint, pending.payload_digest, original.effects, pending.sequence, pending.accepted_at_ns);
        if (!std.mem.eql(u8, &result.transactionId(), &pending.transaction_id)) return error.SourceTransactionIdentityConflict;
        return result;
    }
    pub fn plan(self: *Adapter, a: A, scope: control.Scope, input: delivery.Input, table: Table) !delivery.Plan {
        try self.validateScope(scope);
        if (table.source_admission != null or table.range_guards.len != 0 or !std.mem.eql(u8, table.table_name, self.table_name)) return error.UnsupportedReplicationRoute;
        const original: Manifest = .{ .namespace = self.namespace, .original_table_name = self.table_name, .effects = admission.operationsDigest(table) };
        const bytes = try std.json.Stringify.valueAlloc(a, original, .{});
        const cmd = try command(a, scope, input.batch_id, input.checkpoint, input.expected_checkpoint, input.payload_digest, original.effects, 1, input.accepted_at_ns);
        return .{ .digest = planDigest(scope, bytes), .transaction_id = cmd.transactionId(), .manifest = bytes };
    }
    fn accepted(a: A, scope: control.Scope, pending: control.Reservation, original: Manifest, cmd: admission.Command) !delivery.Acceptance {
        const record: Receipt = .{
            .binding = scope.identity(),
            .namespace = original.namespace,
            .original_table_name = original.original_table_name,
            .reservation = pending.identity(scope),
            .effects = original.effects,
            .transaction_id = cmd.transactionId(),
            .batch = try encode64(a, cmd.batch_id),
            .checkpoint = try encode64(a, cmd.checkpoint),
            .previous = if (cmd.expected_checkpoint) |previous| try encode64(a, previous) else null,
            .payload = cmd.payload_digest,
            .sequence = cmd.source_sequence,
            .accepted_at_ns = cmd.accepted_at_ns,
        };
        const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
        // Base64 provider strings bound expansion even for control characters.
        if (bytes.len > 8192) return error.InvalidSourceAdmission;
        return .{ .reservation_digest = record.reservation, .receipt = bytes, .commit_reference = try std.fmt.allocPrint(a, "native:{s}", .{std.fmt.bytesToHex(cmd.fingerprint(), .lower)}) };
    }
    pub fn resolve(self: *Adapter, a: A, scope: control.Scope, pending: control.Reservation, request: anytype) !?delivery.Acceptance {
        try request.ensureActive();
        const original = try self.manifest(a, scope, pending);
        const cmd = try pendingCommand(a, scope, pending, original);
        const outcome = (try self.source.resolveSourceAdmission(a, original.original_table_name, cmd, .write, request.cancellation)) orelse return null;
        if (outcome != .committed) return error.SourceOutcomeUnresolved;
        return try accepted(a, scope, pending, original, cmd);
    }
    pub fn admit(self: *Adapter, a: A, scope: control.Scope, pending: control.Reservation, table: Table, request: anytype) !delivery.Acceptance {
        try request.ensureActive();
        const original = try self.manifest(a, scope, pending);
        if (table.source_admission != null or table.range_guards.len != 0 or !std.mem.eql(u8, &admission.operationsDigest(table), &original.effects)) return error.SourcePayloadConflict;
        const cmd = try pendingCommand(a, scope, pending, original);
        var actual = table;
        // Names are locators. The persisted original locator identifies the
        // coordinator, while the immutable namespace authenticates the root.
        actual.table_name = original.original_table_name;
        actual.source_admission = cmd;
        const outcome = try self.source.admitSourceTransaction(a, actual, request.cancellation);
        if (outcome != .committed) return error.SourceOutcomeUnresolved;
        return accepted(a, scope, pending, original, cmd);
    }
    fn receipt(self: *Adapter, a: A, scope: control.Scope, bytes: []const u8) !Receipt {
        try self.validateScope(scope);
        if (bytes.len == 0 or bytes.len > 8192) return error.SourceAdmissionCorrupt;
        const value = std.json.parseFromSliceLeaky(Receipt, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.SourceAdmissionCorrupt,
        };
        if (value.version != 1 or !std.mem.eql(u8, &value.binding, &scope.identity()) or !std.mem.eql(u8, &value.namespace, &self.namespace) or
            !validName(value.original_table_name) or std.mem.allEqual(u8, &value.reservation, 0)) return error.SourceBindingConflict;
        return value;
    }
    fn receiptCommand(a: A, scope: control.Scope, value: Receipt) !admission.Command {
        const cmd = try command(a, scope, try decode64(a, value.batch, 256), try decode64(a, value.checkpoint, 1024), if (value.previous) |previous| try decode64(a, previous, 1024) else null, value.payload, value.effects, value.sequence, value.accepted_at_ns);
        if (!std.mem.eql(u8, &cmd.transactionId(), &value.transaction_id)) return error.SourceTransactionIdentityConflict;
        return cmd;
    }
    pub fn inspect(self: *Adapter, a: A, scope: control.Scope, bytes: []const u8) !delivery.ReceiptIdentity {
        const value = try self.receipt(a, scope, bytes);
        const cmd = try receiptCommand(a, scope, value);
        return .{ .batch_id = cmd.batch_id, .checkpoint = cmd.checkpoint, .expected_checkpoint = cmd.expected_checkpoint, .payload_digest = cmd.payload_digest };
    }
    pub fn release(self: *Adapter, a: A, scope: control.Scope, bytes: []const u8, request: anytype) !void {
        try request.ensureActive();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const value = try self.receipt(arena.allocator(), scope, bytes);
        const cmd = try receiptCommand(arena.allocator(), scope, value);
        // Verifies the persisted engine receipt before acknowledging anything.
        try self.source.releaseSourceAdmission(arena.allocator(), value.original_table_name, cmd);
    }
};

test "bound source admission receipt bounds escaped checkpoints and original coordinator names" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const namespace: [24]u8 = @splat(255);
    const name: [255]u8 = @splat(0);
    const batch: [256]u8 = @splat(0);
    const cp: [1024]u8 = @splat(0);
    const previous: [1024]u8 = @splat(1);
    const scope: control.Scope = .{ .table_id = std.math.maxInt(u64), .source_id = @splat(255), .source_ordinal = 0, .generation = std.math.maxInt(u64), .definition = @splat(255), .provider = @splat(255), .destination = .native, .destination_binding = binding(namespace) };
    const original: Manifest = .{ .namespace = namespace, .original_table_name = &name, .effects = @splat(255) };
    const manifest_bytes = try std.json.Stringify.valueAlloc(scratch, original, .{});
    const cmd = try Adapter.command(scratch, scope, &batch, &cp, &previous, @splat(255), original.effects, std.math.maxInt(u64), std.math.maxInt(u64) - 1);
    const pending: control.Reservation = .{
        .sequence = cmd.source_sequence,
        .batch_id = &batch,
        .checkpoint = &cp,
        .expected_checkpoint = &previous,
        .accepted_at_ns = cmd.accepted_at_ns,
        .payload_digest = cmd.payload_digest,
        .plan_digest = planDigest(scope, manifest_bytes),
        .transaction_id = cmd.transactionId(),
        .destination = .native,
        .delivery_manifest = manifest_bytes,
    };
    try pending.validate();
    const result = try Adapter.accepted(scratch, scope, pending, original, cmd);
    try std.testing.expect(result.receipt.len <= 8192);
    const value = try std.json.parseFromSliceLeaky(Receipt, scratch, result.receipt, .{ .allocate = .alloc_always });
    const decoded = try Adapter.receiptCommand(scratch, scope, value);
    try std.testing.expectEqualStrings(&batch, decoded.batch_id);
    try std.testing.expectEqualStrings(&cp, decoded.checkpoint);
    try std.testing.expectEqualStrings(&previous, decoded.expected_checkpoint.?);
    try std.testing.expectEqual(cmd.fingerprint(), decoded.fingerprint());
}
