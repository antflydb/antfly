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

//! Authenticated facts about one independently audited immutable owner image.
//! This is not an enrollment, quorum-completion proof, transaction decision,
//! root-adoption grant, or permission to publish a schema. Metadata must check
//! those authorities independently against its exact durable predecessor.
const std = @import("std");
const job = @import("relational_nullability_job.zig");
const contract = @import("relational_schema_owner_contract.zig");
const rows = @import("relational_rows.zig");
const checkpoint = @import("relational_schema_audit_checkpoint.zig");
const checkpoints = @import("root_audit_checkpoint_io.zig");
const origin = @import("native_backup_origin.zig");
const debt = @import("../transaction_debt.zig");
const history = @import("relational_schema_apply_outcome.zig");
const Read = *@FieldType(rows.Reader, "read");
const DirectoryAudit = debt.SnapshotAudit(Read);
const TransactionAudit = @import("../transactions.zig").TxnManager.TopologySnapshotAudit(Read);
const HistoryAudit = history.SnapshotAudit(Read);
const A = std.mem.Allocator;
const wire = @import("relational_schema_root_contract.zig");
pub const max_encoded = wire.max_encoded;
pub const Qualification = wire.Qualification;
pub const Facts = wire.Facts;
pub const Evidence = wire.Evidence;
const errors: checkpoints.Errors = .{ .invalid = error.InvalidSchemaValidationJob, .scope_changed = error.SchemaValidationRuntimeRecoveryRequired };

/// Only owned completed auditors can produce facts. Pointer identity binds
/// all four independent traversals to the exact retained MVCC read, not a
/// matching-looking header, root UUID or caller-supplied counters.
pub fn capture(guard: *const checkpoint.Guard, physical_root_dir: []const u8, audit: *job.RowAudit, directory: *const DirectoryAudit, transactions: *const TransactionAudit, outcomes: *const HistoryAudit) !Facts {
    const image = &audit.reader.read;
    if (directory.read != image or transactions.read != image or outcomes.read != image or
        !std.meta.eql(directory.header, transactions.head) or !outcomes.namespace.eql(audit.header.binding.fence.namespace)) return error.SchemaValidationRuntimeRecoveryRequired;
    const row = try guard.evidence(audit);
    const directory_stats = try directory.result();
    const transaction_stats = try transactions.result();
    const history_stats = try outcomes.result();
    var points = try image.openReadScope(guard.alloc);
    defer points.close();
    const provenance = try origin.Origin.capture(&points, row.physical_root, row.rows.header.binding.fence.namespace);
    if (!std.meta.eql(provenance.applied, outcomes.marker)) return error.SchemaValidationRuntimeRecoveryRequired;
    const ns = row.rows.header.binding.fence.namespace;
    const installation = try origin.loadInstallation(guard.alloc, guard.io, physical_root_dir, row.rows.header.binding.fence.owner_group_id, @import("online_source_contract.zig").namespaceBytes(ns), row.physical_root, guard.context);
    const anchor: ?history.Anchor = if (installation) |value| try value.historyAnchor(provenance.applied) else null;
    // A sidecar added, removed or changed after history preparation cannot
    // re-label the completed traversal as a source-prefix qualification.
    if (!std.meta.eql(anchor, outcomes.anchor)) return error.SchemaValidationRuntimeRecoveryRequired;
    const source_head = if (installation != null) try outcomes.anchoredHead() else null;
    const result: Facts = .{
        .signing_root = row.signing_root,
        .physical_root = row.physical_root,
        .rows = row.rows,
        .checkpoint_digest = row.checkpoint_digest,
        .qualification = .{ .origin = provenance, .directory_head = directory.header, .directory = directory_stats, .transactions = transaction_stats, .history_head = outcomes.expected, .history = history_stats, .installation = installation, .source_history_head = source_head },
    };
    try result.validate();
    return result;
}

/// Row-checkpoint lock precedes the proof-slot lock everywhere. Both roots and
/// keys are checked again before atomic publication. There is one fixed slot
/// per namespace, not one file per attempt or phase, bounding churn storage.
pub fn seal(guard: *const checkpoint.Guard, physical_root_dir: []const u8, audit: *job.RowAudit, directory: *const DirectoryAudit, transactions: *const TransactionAudit, outcomes: *const HistoryAudit) !Evidence {
    const facts = try capture(guard, physical_root_dir, audit, directory, transactions, outcomes);
    var opened = try checkpoints.open(guard.alloc, guard.io, guard.root_dir, guard.root.root_incarnation, @import("online_source_contract.zig").namespaceBytes(facts.rows.header.binding.fence.namespace), ".schema-validation-proofs", guard.context, errors);
    defer checkpoints.close(&opened);
    if (!std.mem.eql(u8, &opened.root.public_key, &guard.root.public_key)) return error.SchemaValidationRuntimeRecoveryRequired;
    if (try checkpoints.read(&opened, max_encoded, errors)) |previous| {
        defer opened.alloc.free(previous);
        const retained = try Evidence.decode(opened.alloc, previous, opened.root.public_key);
        if (retained.value.signing_root != opened.root.root_incarnation or
            !retained.value.rows.header.binding.fence.namespace.eql(facts.rows.header.binding.fence.namespace)) return error.InvalidSchemaValidationJob;
        // A receiver record is immutable for a physical root. Restarting
        // without it must not downgrade previously signed lineage to absence.
        if (retained.value.physical_root == facts.physical_root and
            !std.meta.eql(retained.value.qualification.installation, facts.qualification.installation)) return error.SchemaValidationRuntimeRecoveryRequired;
        if (std.meta.eql(retained.value, facts)) {
            // Repeated completion polls reuse verified bytes: no second
            // signature, new temporary file, rename or fsync is needed.
            try checkpoints.requireRoot(guard, errors);
            return retained;
        }
        opened.checkpoint_digest = job.hash(previous);
    }
    var result: Evidence = .{ .value = facts, .signature = undefined };
    result.signature = try opened.root.sign(&try result.signatureDigest());
    const encoded = try result.encode(opened.alloc);
    defer opened.alloc.free(encoded);
    try checkpoints.requireRoot(guard, errors);
    try checkpoints.write(&opened, encoded, errors);
    return result;
}

/// Loads existing signed facts; never mints evidence. A caller still needs live
/// enrollment, source provenance and exact metadata predecessor authorization.
pub fn load(a: A, io: std.Io, root_dir: []const u8, root_incarnation: u128, namespace: [24]u8, context: @import("../../api/operation.zig").RequestContext) !?Evidence {
    var opened = try checkpoints.open(a, io, root_dir, root_incarnation, namespace, ".schema-validation-proofs", context, errors);
    defer checkpoints.close(&opened);
    const bytes = (try checkpoints.read(&opened, max_encoded, errors)) orelse return null;
    defer a.free(bytes);
    const result = try Evidence.decode(a, bytes, opened.root.public_key);
    if (result.value.signing_root != root_incarnation or
        !std.mem.eql(u8, &@import("online_source_contract.zig").namespaceBytes(result.value.rows.header.binding.fence.namespace), &namespace)) return error.InvalidSchemaValidationJob;
    try checkpoints.requireRoot(&opened, errors);
    return result;
}
