// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Shared durability expectations that higher layers can reason about without
/// depending on backend-specific flags like LMDB's no_sync/no_meta_sync.
pub const Durability = enum {
    /// The backend may defer all durable flush work.
    none,
    /// User data is durable, but metadata publication may lag.
    data,
    /// Commit is fully durable and visible after a crash/reopen.
    full,
};

/// Snapshot visibility expected from a read transaction.
pub const ReadVisibility = enum {
    /// Reads observe a stable point-in-time snapshot for the life of the txn.
    snapshot,
    /// Reads may observe later commits while the txn remains open.
    read_committed,
};

/// Transaction mode requested from a backend.
pub const TxnMode = enum {
    read,
    write,
};

/// Write execution style a backend can provide.
pub const WriteBatchMode = enum {
    /// Only single write transactions are available.
    none,
    /// Multiple writes can be committed atomically as one batch.
    atomic,
};

/// A logical namespace/partition hint within a backend.
///
/// A backend may realize this as:
/// - a native named database or column-family-like partition
/// - a prefixed key range in a shared ordered KV space
/// - or a single default namespace when no partitioning is needed
pub const Namespace = struct {
    /// `null` represents the unnamed/default namespace.
    name: ?[]const u8 = null,
};

/// Range bounds used by ordered scans.
pub const KeyRange = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
    start_inclusive: bool = true,
    end_inclusive: bool = false,
};

/// Ordered range scan direction.
pub const ScanOrder = enum {
    forward,
    reverse,
};

/// Initial cursor seek behavior independent of any specific backend.
pub const CursorStart = union(enum) {
    /// Start from the first key in the chosen direction.
    first,
    /// Seek to the first key greater than or equal to `key`.
    at_or_after: []const u8,
    /// Seek to the last key less than or equal to `key`.
    at_or_before: []const u8,
};

/// Transaction-open request independent of any specific backend.
pub const TxnOptions = struct {
    mode: TxnMode = .read,
    visibility: ReadVisibility = .snapshot,
};

pub const BatchMode = enum {
    default,
    bulk_ingest,
};

pub const BatchOptions = struct {
    mode: BatchMode = .default,
};

pub const BulkIngestFinishOptions = struct {
    pub const ProgressPhase = enum(u8) {
        begin,
        split,
        publish,
        complete,
    };

    pub const Progress = struct {
        phase: ProgressPhase,
        publish_window: u64 = 0,
        split_steps: u64 = 0,
        deferred_leaf_splits: u64 = 0,
        elapsed_ns: u64 = 0,
    };

    compact: bool = true,
    flush: bool = false,
    /// Maintenance target/hint for L0 debt left behind by bulk publish.
    ///
    /// This does not by itself authorize foreground compaction. Foreground
    /// publish paths must also set `max_foreground_compaction_steps`, otherwise
    /// the target is left for scheduled maintenance.
    max_deferred_l0_runs: ?usize = null,
    max_foreground_compaction_steps: usize = 0,
    max_foreground_compaction_input_bytes: ?u64 = null,
    max_foreground_compaction_ns: ?u64 = null,
    max_deferred_hbc_leaf_splits_per_publish: ?usize = null,
    max_deferred_hbc_leaf_split_members_per_publish: ?usize = null,
    bulk_rebuild_hbc_leaf_min_members: ?usize = null,
    progress_ctx: ?*anyopaque = null,
    progress_fn: ?*const fn (*anyopaque, Progress) void = null,
};

/// Common range-scan request shape for higher-level storage layers.
pub const RangeScanOptions = struct {
    namespace: Namespace = .{},
    range: KeyRange = .{},
    order: ScanOrder = .forward,
    limit: ?usize = null,
};

/// Common cursor-open request shape for higher-level storage layers.
pub const CursorOptions = struct {
    namespace: Namespace = .{},
    order: ScanOrder = .forward,
    start: CursorStart = .first,
};

/// Capabilities a backend can advertise while the abstraction is still
/// evolving. Higher layers should only depend on capabilities they actively
/// require.
pub const Capabilities = struct {
    ordered_ranges: bool = true,
    reverse_ranges: bool = false,
    cursors: bool = true,
    ordered_append_puts: bool = false,
    native_namespaces: bool = false,
    duplicate_values: bool = false,
    nested_write_transactions: bool = false,
    write_batches: WriteBatchMode = .atomic,
    single_writer: bool = true,
    read_snapshots: ReadVisibility = .snapshot,
};

/// Transitional alias while higher-level code migrates terminology away from
/// backend-shaped "keyspace" wording.
pub const Keyspace = Namespace;

/// Shared backend open options that are independent of any specific engine.
pub const OpenOptions = struct {
    read_only: bool = false,
    durability: Durability = .full,
    create_if_missing: bool = true,
};

pub const ReplayEntry = struct {
    sequence: u64,
    payload: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.payload);
        self.* = undefined;
    }
};

test "default backend capabilities assume ordered single-writer snapshots" {
    const caps: Capabilities = .{};
    try std.testing.expect(caps.ordered_ranges);
    try std.testing.expect(!caps.reverse_ranges);
    try std.testing.expect(caps.cursors);
    try std.testing.expect(!caps.ordered_append_puts);
    try std.testing.expect(!caps.native_namespaces);
    try std.testing.expect(caps.single_writer);
    try std.testing.expectEqual(WriteBatchMode.atomic, caps.write_batches);
    try std.testing.expectEqual(ReadVisibility.snapshot, caps.read_snapshots);
}

test "default namespace refers to the shared unnamed partition" {
    const namespace: Namespace = .{};
    try std.testing.expectEqual(@as(?[]const u8, null), namespace.name);
}

test "transaction defaults request snapshot reads" {
    const txn: TxnOptions = .{};
    try std.testing.expectEqual(TxnMode.read, txn.mode);
    try std.testing.expectEqual(ReadVisibility.snapshot, txn.visibility);
}

test "range scan defaults target the default namespace in forward order" {
    const scan: RangeScanOptions = .{};
    try std.testing.expectEqual(@as(?[]const u8, null), scan.namespace.name);
    try std.testing.expectEqual(ScanOrder.forward, scan.order);
    try std.testing.expectEqual(@as(?usize, null), scan.limit);
}

test "cursor defaults start at the first key in forward order" {
    const cursor: CursorOptions = .{};
    try std.testing.expectEqual(@as(?[]const u8, null), cursor.namespace.name);
    try std.testing.expectEqual(ScanOrder.forward, cursor.order);
    try std.testing.expectEqual(CursorStart.first, cursor.start);
}
