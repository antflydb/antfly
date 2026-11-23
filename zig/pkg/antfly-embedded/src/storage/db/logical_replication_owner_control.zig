// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Publication capture control shared by native and ordered storage owners.
//! The caller admits the control and owns the enclosing atomic transaction.
const std = @import("std");
const A = std.mem.Allocator;
const capture = @import("../../system_catalog/logical_replication_capture.zig");
const positions = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");
const identity = @import("doc_identity_namespace.zig");
const table_catalog = @import("table_catalog.zig");
const operation = @import("../../api/operation.zig");
const outbox = @import("logical_replication_outbox.zig");

pub const Clock = union(enum) {
    ordered: @import("apply_receipts.zig").OrderedApplyReceipt,
    native: ?positions.Native,
};
pub const Staged = struct { receipt: ?capture.Receipt, position: positions.Position, duplicate: bool };

pub fn stage(a: A, namespace: identity.Namespace, txn: anytype, control: capture.Control, root: u128, clock: Clock, context: operation.RequestContext) !Staged {
    try context.ensureActive();
    try control.validate();
    if (root == 0 or !namespace.eql(control.binding.namespace)) return error.IdentityNamespaceMismatch;
    const catalog = try table_catalog.Catalog.decode(try txn.get(table_catalog.key));
    if (!catalog.mode_initialized or catalog.storage_mode != .relational or catalog.active_schema_version != control.binding.schema_version or catalog.sql_persistence != null)
        return error.LogicalCaptureControlChanged;
    const physical = @import("online_source_contract.zig").namespaceBytes(namespace);
    _ = try authority.require(txn, if (clock == .ordered) .raft else .native, physical);
    try @import("artifact_catalog_mutability.zig").requireMutable(a, txn, physical, null);
    try @import("relational/integrity/topology.zig").requireUnfenced(txn);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(try txn.get(@import("../schema.zig").schema_key), &digest, .{});
    if (!std.mem.eql(u8, &digest, &control.binding.schema_digest)) return error.LogicalCaptureControlChanged;
    std.crypto.hash.Blake3.hash(try txn.get(@import("relational/integrity/catalog.zig").key), &digest, .{});
    if (!std.mem.eql(u8, &digest, &control.binding.catalog_digest)) return error.LogicalCaptureControlChanged;
    const prior = try outbox.loadGuard(a, txn);
    var imported_guard = false;
    if (prior) |value| {
        if (value.physical_root_incarnation != root) {
            // Restore can carry a terminal guard from the predecessor image.
            // Its old root/namespace/position never become this owner's guard
            // history. Metadata separately authorizes the exact destination
            // and CURRENT memberships; an older copied guard is not authority
            // for heads that may have changed since the snapshot.
            if (value.phase != .active and value.phase != .canceled) return error.LogicalCaptureControlChanged;
            imported_guard = control.action == .prepare and !value.binding.eql(control.binding) and
                control.binding.scope.subject == .table_change and control.binding.scope.subject.table_change.kind == .image_replacement and
                (value.phase == .active or value.phase == .canceled) and control.binding.previous.empty() and
                value.binding.physical_table_id != control.binding.physical_table_id and
                !value.binding.namespace.eql(control.binding.namespace) and
                value.binding.logical_relation_id == control.binding.logical_relation_id and
                value.binding.scope.realm.eql(control.binding.scope.realm);
            if (!imported_guard) return error.LogicalCaptureControlChanged;
        }
        if (value.binding.eql(control.binding)) {
            const already = switch (control.action) {
                .prepare => value.phase == .prepared,
                .activate => value.phase == .active,
                .cancel => value.phase == .canceled,
            };
            if (already) return .{ .receipt = .{ .binding = value.binding, .action = control.action, .position = value.position, .physical_root_incarnation = root }, .position = value.position, .duplicate = true };
            if (value.phase == .canceled or value.phase == .active) return error.LogicalCaptureControlChanged;
        } else {
            if (control.action != .prepare or (value.phase != .active and value.phase != .canceled)) return error.LogicalCapturePublicationPending;
            const installed = if (value.phase == .active) value.binding.next else value.binding.previous;
            if (!imported_guard and !installed.eql(control.binding.previous)) return error.LogicalCaptureControlChanged;
        }
    } else if (control.action == .activate or !control.binding.previous.empty()) return error.LogicalCaptureControlChanged;
    const phase: capture.Phase = switch (control.action) {
        .prepare => if (try @import("../transactions.zig").TxnManager.hasPublicationCaptureDebtInView(a, txn)) .preparing else .prepared,
        .activate => active: {
            const previous = prior orelse return error.LogicalCaptureControlChanged;
            if (!previous.binding.eql(control.binding) or previous.phase != .prepared) return error.LogicalCaptureControlChanged;
            break :active .active;
        },
        .cancel => .canceled,
    };
    const position: positions.Position = switch (clock) {
        .ordered => |entry| .{ .raft = .{ .term = entry.term, .index = entry.index } },
        .native => |replay| .{ .native = .{ .namespace = control.binding.namespace, .sequence = try authority.advance(txn, physical, if (replay) |value| value.sequence else null) } },
    };
    try position.requireNamespace(control.binding.namespace);
    if (prior) |value| if (value.binding.eql(control.binding) and !position.strictlyAfter(value.position)) return error.LogicalCaptureControlChanged;
    const previous_binding: ?capture.Binding = if (phase == .active or imported_guard) null else if (prior) |value|
        (if (value.binding.eql(control.binding)) value.previous_binding else try value.effectiveBinding())
    else
        null;
    try outbox.storeGuard(a, txn, .{ .binding = control.binding, .previous_binding = previous_binding, .phase = phase, .position = position, .physical_root_incarnation = root });
    try context.ensureActive();
    return .{ .receipt = if (phase == .preparing) null else .{ .binding = control.binding, .action = control.action, .position = position, .physical_root_incarnation = root }, .position = position, .duplicate = false };
}

const core_mod = @import("core.zig");
const wire = @import("logical_replication_control_frame.zig");
const Payload = wire.Payload;
pub const Frame = wire.Frame;

pub const Outcome = capture.NativeOutcome;

pub const Prepared = struct {
    pub const metadata_control = true;
    pub const ApplyOutcome = Outcome;
    alloc: A,
    command: capture.Control,
    root: u128,
    context: operation.RequestContext,
    payload: ?Payload = null,
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.command.validate();
        if (self.root == 0) return error.InvalidLogicalCaptureControl;
    }
    pub fn namespace(self: *const @This()) @import("doc_identity_namespace.zig").Namespace {
        return self.command.binding.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: positions.Native) !void {
        try position.validate();
        if (!position.namespace.eql(self.namespace())) return error.IdentityNamespaceMismatch;
    }
    pub fn replayDuplicateOutcome(_: *const @This(), position: positions.Native) Outcome {
        return .{ .receipt = null, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), core: *core_mod.DBCore, txn: anytype, replay: ?positions.Native) !Outcome {
        const result = try @import("logical_replication_owner_control.zig").stage(self.alloc, core.identity_namespace, txn, self.command, self.root, .{ .native = replay }, self.context);
        if (replay) |expected| if (!std.meta.eql(result.position.native, expected)) return error.InvalidControlReceiptPosition;
        return .{ .receipt = result.receipt, .position = result.position.native, .duplicate = result.duplicate };
    }
    pub fn afterCommit(_: *@This(), core: *core_mod.DBCore) void {
        core.table_catalog.logical_capture_format = @max(core.table_catalog.logical_capture_format, 1);
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.payload == null) self.payload = try Payload.init(self.alloc, self.command, self.root);
    }
    pub fn finalizePayload(self: *@This(), position: positions.Native) ![]const u8 {
        return (if (self.payload) |*value| value else return error.InvalidLogicalCaptureControl).finalize(position);
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return false;
    }
    pub fn populateReceipt(_: *const @This(), _: anytype, _: *Outcome) !void {
        return error.InvalidLogicalCaptureControl;
    }
    pub fn deinit(self: *@This()) void {
        if (self.payload) |*value| value.deinit();
    }
};
