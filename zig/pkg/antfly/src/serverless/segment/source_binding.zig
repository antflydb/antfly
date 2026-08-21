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

//! Source binding metadata for serverless sidecar segments built over RowSource.

const std = @import("std");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const SidecarKind = enum(u8) {
    text = 1,
    vector = 2,
    sparse = 3,
    graph = 4,
    algebraic = 5,
};

pub const RowRefKind = enum(u8) {
    relational_key = 1,
    serverless = 2,
    external = 3,
};

/// Hash/equality semantics for RowRef values. Maps using this context borrow the
/// byte slices stored in their keys; callers must keep those slices alive until
/// the map is deinitialized.
pub const RowRefContext = struct {
    pub fn hash(_: RowRefContext, row_ref: rowsource.RowRef) u64 {
        var hasher = std.hash.Wyhash.init(0);
        const tag: u8 = @intFromEnum(std.meta.activeTag(row_ref));
        hasher.update(std.mem.asBytes(&tag));
        switch (row_ref) {
            .relational_key => |key| hashBytes(&hasher, key),
            .serverless => |value| {
                hashBytes(&hasher, value.fragment_id);
                hasher.update(std.mem.asBytes(&value.row_ordinal));
            },
            .external => |value| {
                hashBytes(&hasher, value.source_id);
                hashBytes(&hasher, value.snapshot_id);
                hashBytes(&hasher, value.file_id);
                hasher.update(std.mem.asBytes(&value.row_group_ordinal));
                hasher.update(std.mem.asBytes(&value.row_ordinal));
            },
        }
        return hasher.final();
    }

    pub fn eql(_: RowRefContext, lhs: rowsource.RowRef, rhs: rowsource.RowRef) bool {
        return rowRefsEqual(lhs, rhs);
    }
};

pub const RowRefSetMap = std.HashMapUnmanaged(
    rowsource.RowRef,
    void,
    RowRefContext,
    std.hash_map.default_max_load_percentage,
);

pub fn rowRefsEqual(lhs: rowsource.RowRef, rhs: rowsource.RowRef) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;
    return switch (lhs) {
        .relational_key => |lhs_key| std.mem.eql(u8, lhs_key, rhs.relational_key),
        .serverless => |lhs_ref| std.mem.eql(u8, lhs_ref.fragment_id, rhs.serverless.fragment_id) and
            lhs_ref.row_ordinal == rhs.serverless.row_ordinal,
        .external => |lhs_ref| std.mem.eql(u8, lhs_ref.source_id, rhs.external.source_id) and
            std.mem.eql(u8, lhs_ref.snapshot_id, rhs.external.snapshot_id) and
            std.mem.eql(u8, lhs_ref.file_id, rhs.external.file_id) and
            lhs_ref.row_group_ordinal == rhs.external.row_group_ordinal and
            lhs_ref.row_ordinal == rhs.external.row_ordinal,
    };
}

fn hashBytes(hasher: *std.hash.Wyhash, value: []const u8) void {
    const len = value.len;
    hasher.update(std.mem.asBytes(&len));
    hasher.update(value);
}

pub const Binding = struct {
    sidecar_kind: SidecarKind,
    source_kind: rowsource.SourceKind,
    row_ref_kind: RowRefKind,
    source_id: []const u8 = &.{},
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    column_bindings: []const []const u8 = &.{},
    column_kinds: []const rowsource.ColumnKind = &.{},
    index_config_hash: []const u8,

    pub fn validate(self: Binding) !void {
        if (self.snapshot_id.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.schema_fingerprint.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.index_config_hash.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.column_kinds.len != 0 and self.column_kinds.len != self.column_bindings.len) {
            return error.InvalidSidecarSourceBinding;
        }
        for (self.column_bindings) |column| {
            if (column.len == 0) return error.InvalidSidecarSourceBinding;
        }
        switch (self.source_kind) {
            .external_parquet, .external_iceberg, .external_lance => {
                if (self.row_ref_kind != .external) return error.InvalidSidecarSourceBinding;
                if (self.source_id.len == 0) return error.InvalidSidecarSourceBinding;
            },
            .serverless_fragment => {
                if (self.row_ref_kind != .serverless) return error.InvalidSidecarSourceBinding;
            },
            .relational_store, .json_materialized => {
                if (self.row_ref_kind != .relational_key) return error.InvalidSidecarSourceBinding;
            },
        }
    }
};

pub fn rowRefKindForSourceKind(source_kind: rowsource.SourceKind) RowRefKind {
    return switch (source_kind) {
        .relational_store, .json_materialized => .relational_key,
        .serverless_fragment => .serverless,
        .external_parquet, .external_iceberg, .external_lance => .external,
    };
}

pub fn bindingFromSnapshot(
    sidecar_kind: SidecarKind,
    source_kind: rowsource.SourceKind,
    snapshot: rowsource.SnapshotRef,
    schema_fingerprint: []const u8,
    column_bindings: []const []const u8,
    index_config_hash: []const u8,
) Binding {
    return .{
        .sidecar_kind = sidecar_kind,
        .source_kind = source_kind,
        .row_ref_kind = rowRefKindForSourceKind(source_kind),
        .source_id = snapshot.table_id,
        .snapshot_id = snapshot.snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .column_bindings = column_bindings,
        .index_config_hash = index_config_hash,
    };
}

pub fn bindingFromSnapshotWithColumnKinds(
    sidecar_kind: SidecarKind,
    source_kind: rowsource.SourceKind,
    snapshot: rowsource.SnapshotRef,
    schema_fingerprint: []const u8,
    column_bindings: []const []const u8,
    column_kinds: []const rowsource.ColumnKind,
    index_config_hash: []const u8,
) Binding {
    var binding = bindingFromSnapshot(
        sidecar_kind,
        source_kind,
        snapshot,
        schema_fingerprint,
        column_bindings,
        index_config_hash,
    );
    binding.column_kinds = column_kinds;
    return binding;
}

pub fn validateBatchAgainstBinding(binding: Binding, batch: rowsource.ColumnBatch) !void {
    try validateBatchSnapshotAgainstBinding(binding, batch);
    for (binding.column_bindings, 0..) |column, idx| {
        const batch_column = batch.findColumn(column) orelse return error.SidecarSourceBindingMismatch;
        if (binding.column_kinds.len != 0 and batch_column.kind() != binding.column_kinds[idx]) {
            return error.SidecarSourceBindingMismatch;
        }
    }
}

pub fn validateBatchSnapshotAgainstBinding(binding: Binding, batch: rowsource.ColumnBatch) !void {
    try binding.validate();
    try batch.validate();
    if (binding.source_id.len != 0 and !std.mem.eql(u8, binding.source_id, batch.snapshot.table_id)) {
        return error.SidecarSourceBindingMismatch;
    }
    if (!std.mem.eql(u8, binding.snapshot_id, batch.snapshot.snapshot_id)) {
        return error.SidecarSourceBindingMismatch;
    }
    for (batch.row_refs) |row_ref| {
        try validateRowRefAgainstBinding(binding, row_ref);
    }
}

pub fn validateCandidateRowRefsAgainstBinding(
    binding: Binding,
    row_refs: []const rowsource.RowRef,
) !void {
    try binding.validate();
    for (row_refs) |row_ref| try validateRowRefAgainstBinding(binding, row_ref);
}

pub fn sameSourceSnapshot(a: Binding, b: Binding) bool {
    return a.source_kind == b.source_kind and
        a.row_ref_kind == b.row_ref_kind and
        std.mem.eql(u8, a.source_id, b.source_id) and
        std.mem.eql(u8, a.snapshot_id, b.snapshot_id) and
        std.mem.eql(u8, a.schema_fingerprint, b.schema_fingerprint);
}

pub fn sameColumnKinds(a: Binding, b: Binding) bool {
    if (a.column_kinds.len != b.column_kinds.len) return false;
    for (a.column_kinds, b.column_kinds) |left, right| {
        if (left != right) return false;
    }
    return true;
}

pub fn cloneAlloc(alloc: std.mem.Allocator, binding: Binding) !Binding {
    const source_id = try alloc.dupe(u8, binding.source_id);
    errdefer alloc.free(source_id);
    const snapshot_id = try alloc.dupe(u8, binding.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, binding.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    const index_config_hash = try alloc.dupe(u8, binding.index_config_hash);
    errdefer alloc.free(index_config_hash);
    const column_bindings = try alloc.alloc([]const u8, binding.column_bindings.len);
    errdefer alloc.free(column_bindings);
    var initialized: usize = 0;
    errdefer {
        for (column_bindings[0..initialized]) |column| alloc.free(column);
    }
    for (binding.column_bindings, 0..) |column, idx| {
        column_bindings[idx] = try alloc.dupe(u8, column);
        initialized += 1;
    }
    const column_kinds = try alloc.dupe(rowsource.ColumnKind, binding.column_kinds);
    errdefer alloc.free(column_kinds);

    return .{
        .sidecar_kind = binding.sidecar_kind,
        .source_kind = binding.source_kind,
        .row_ref_kind = binding.row_ref_kind,
        .source_id = source_id,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .column_bindings = column_bindings,
        .column_kinds = column_kinds,
        .index_config_hash = index_config_hash,
    };
}

pub fn freeOwned(alloc: std.mem.Allocator, binding: Binding) void {
    alloc.free(@constCast(binding.source_id));
    alloc.free(@constCast(binding.snapshot_id));
    alloc.free(@constCast(binding.schema_fingerprint));
    for (binding.column_bindings) |column| alloc.free(@constCast(column));
    alloc.free(@constCast(binding.column_bindings));
    alloc.free(@constCast(binding.column_kinds));
    alloc.free(@constCast(binding.index_config_hash));
}

pub fn rowRefMatchesKind(row_ref: rowsource.RowRef, kind: RowRefKind) bool {
    return switch (row_ref) {
        .relational_key => kind == .relational_key,
        .serverless => kind == .serverless,
        .external => kind == .external,
    };
}

pub fn validateRowRefAgainstBinding(binding: Binding, row_ref: rowsource.RowRef) !void {
    if (!rowRefMatchesKind(row_ref, binding.row_ref_kind)) return error.SidecarSourceBindingMismatch;
    switch (row_ref) {
        .external => |value| {
            if (!std.mem.eql(u8, value.source_id, binding.source_id)) return error.SidecarSourceBindingMismatch;
            if (!std.mem.eql(u8, value.snapshot_id, binding.snapshot_id)) return error.SidecarSourceBindingMismatch;
        },
        .serverless, .relational_key => {},
    }
}

pub fn rowRefKeyAlloc(alloc: std.mem.Allocator, row_ref: rowsource.RowRef) ![]u8 {
    return switch (row_ref) {
        .relational_key => |key| std.fmt.allocPrint(alloc, "rel:{d}:{s}", .{ key.len, key }),
        .serverless => |value| std.fmt.allocPrint(
            alloc,
            "srv:{d}:{s}:{d}",
            .{ value.fragment_id.len, value.fragment_id, value.row_ordinal },
        ),
        .external => |value| std.fmt.allocPrint(
            alloc,
            "ext:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{d}",
            .{
                value.source_id.len,
                value.source_id,
                value.snapshot_id.len,
                value.snapshot_id,
                value.file_id.len,
                value.file_id,
                value.row_group_ordinal,
                value.row_ordinal,
            },
        ),
    };
}

pub fn rowRefFromKeyAlloc(alloc: std.mem.Allocator, key: []const u8) !rowsource.RowRef {
    return try cloneRowRefAlloc(alloc, try rowRefFromKey(key));
}

fn rowRefFromKey(key: []const u8) !rowsource.RowRef {
    var parser = RowRefKeyParser.init(key);
    if (parser.consumePrefix("rel:")) {
        const relational_key = try parser.readLengthPrefixed();
        try parser.expectEof();
        return .{ .relational_key = relational_key };
    }
    if (parser.consumePrefix("srv:")) {
        const fragment_id = try parser.readLengthPrefixed();
        try parser.expectByte(':');
        const row_ordinal = try parser.readU64ToEnd();
        return .{ .serverless = .{
            .fragment_id = fragment_id,
            .row_ordinal = row_ordinal,
        } };
    }
    if (parser.consumePrefix("ext:")) {
        const source_id = try parser.readLengthPrefixed();
        try parser.expectByte(':');
        const snapshot_id = try parser.readLengthPrefixed();
        try parser.expectByte(':');
        const file_id = try parser.readLengthPrefixed();
        try parser.expectByte(':');
        const row_group_ordinal_u64 = try parser.readU64Field();
        const row_group_ordinal = std.math.cast(u32, row_group_ordinal_u64) orelse return error.InvalidSidecarRowRefKey;
        const row_ordinal = try parser.readU64ToEnd();
        return .{ .external = .{
            .source_id = source_id,
            .snapshot_id = snapshot_id,
            .file_id = file_id,
            .row_group_ordinal = row_group_ordinal,
            .row_ordinal = row_ordinal,
        } };
    }
    return error.InvalidSidecarRowRefKey;
}

/// Return the exact number of bytes retained by an owned row-ref result,
/// including the tagged-union slice. Keys are fully parsed and checked against
/// the declaration binding before the caller performs any output allocation.
pub fn rowRefsOwnedAllocationBytesFromKeys(
    binding: Binding,
    keys: []const []const u8,
) !usize {
    var total = std.math.mul(usize, keys.len, @sizeOf(rowsource.RowRef)) catch
        return error.InvalidSidecarRowRefKey;
    for (keys) |key| {
        const row_ref = try rowRefFromKey(key);
        try validateRowRefAgainstBinding(binding, row_ref);
        const retained_bytes = switch (row_ref) {
            .relational_key => |value| value.len,
            .serverless => |value| value.fragment_id.len,
            .external => |value| blk: {
                var bytes = std.math.add(usize, value.source_id.len, value.snapshot_id.len) catch
                    return error.InvalidSidecarRowRefKey;
                bytes = std.math.add(usize, bytes, value.file_id.len) catch
                    return error.InvalidSidecarRowRefKey;
                break :blk bytes;
            },
        };
        total = std.math.add(usize, total, retained_bytes) catch
            return error.InvalidSidecarRowRefKey;
    }
    return total;
}

pub fn rowRefsFromKeysAlloc(
    alloc: std.mem.Allocator,
    binding: Binding,
    keys: []const []const u8,
) ![]rowsource.RowRef {
    const refs = try alloc.alloc(rowsource.RowRef, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |row_ref| freeOwnedRowRef(alloc, row_ref);
        alloc.free(refs);
    }
    for (keys, 0..) |key, idx| {
        refs[idx] = try rowRefFromKeyAlloc(alloc, key);
        initialized += 1;
        try validateRowRefAgainstBinding(binding, refs[idx]);
    }
    return refs;
}

pub fn cloneRowRefAlloc(alloc: std.mem.Allocator, row_ref: rowsource.RowRef) !rowsource.RowRef {
    return switch (row_ref) {
        .relational_key => |key| .{ .relational_key = try alloc.dupe(u8, key) },
        .serverless => |value| .{ .serverless = .{
            .fragment_id = try alloc.dupe(u8, value.fragment_id),
            .row_ordinal = value.row_ordinal,
        } },
        .external => |value| blk: {
            const source_id = try alloc.dupe(u8, value.source_id);
            errdefer alloc.free(source_id);
            const snapshot_id = try alloc.dupe(u8, value.snapshot_id);
            errdefer alloc.free(snapshot_id);
            break :blk .{ .external = .{
                .source_id = source_id,
                .snapshot_id = snapshot_id,
                .file_id = try alloc.dupe(u8, value.file_id),
                .row_group_ordinal = value.row_group_ordinal,
                .row_ordinal = value.row_ordinal,
            } };
        },
    };
}

pub fn freeOwnedRowRef(alloc: std.mem.Allocator, row_ref: rowsource.RowRef) void {
    switch (row_ref) {
        .relational_key => |key| alloc.free(@constCast(key)),
        .serverless => |value| alloc.free(@constCast(value.fragment_id)),
        .external => |value| {
            alloc.free(@constCast(value.source_id));
            alloc.free(@constCast(value.snapshot_id));
            alloc.free(@constCast(value.file_id));
        },
    }
}

pub fn freeOwnedRowRefs(alloc: std.mem.Allocator, row_refs: []const rowsource.RowRef) void {
    for (row_refs) |row_ref| freeOwnedRowRef(alloc, row_ref);
    alloc.free(@constCast(row_refs));
}

const RowRefKeyParser = struct {
    input: []const u8,
    pos: usize = 0,

    fn init(input: []const u8) RowRefKeyParser {
        return .{ .input = input };
    }

    fn consumePrefix(self: *RowRefKeyParser, prefix: []const u8) bool {
        if (!std.mem.startsWith(u8, self.input[self.pos..], prefix)) return false;
        self.pos += prefix.len;
        return true;
    }

    fn expectByte(self: *RowRefKeyParser, byte: u8) !void {
        if (self.pos >= self.input.len or self.input[self.pos] != byte) return error.InvalidSidecarRowRefKey;
        self.pos += 1;
    }

    fn expectEof(self: RowRefKeyParser) !void {
        if (self.pos != self.input.len) return error.InvalidSidecarRowRefKey;
    }

    fn readLengthPrefixed(self: *RowRefKeyParser) ![]const u8 {
        const len = try self.readU64Field();
        const value_len = std.math.cast(usize, len) orelse return error.InvalidSidecarRowRefKey;
        if (value_len == 0) return error.InvalidSidecarRowRefKey;
        if (self.pos > self.input.len or value_len > self.input.len - self.pos) return error.InvalidSidecarRowRefKey;
        const value = self.input[self.pos..][0..value_len];
        self.pos += value_len;
        return value;
    }

    fn readU64Field(self: *RowRefKeyParser) !u64 {
        const start = self.pos;
        while (self.pos < self.input.len and self.input[self.pos] != ':') {
            self.pos += 1;
        }
        if (self.pos == start or self.pos >= self.input.len) return error.InvalidSidecarRowRefKey;
        const value = try parseU64(self.input[start..self.pos]);
        self.pos += 1;
        return value;
    }

    fn readU64ToEnd(self: *RowRefKeyParser) !u64 {
        const start = self.pos;
        if (start >= self.input.len) return error.InvalidSidecarRowRefKey;
        self.pos = self.input.len;
        return try parseU64(self.input[start..]);
    }

    fn parseU64(bytes: []const u8) !u64 {
        if (bytes.len == 0) return error.InvalidSidecarRowRefKey;
        for (bytes) |byte| {
            if (byte < '0' or byte > '9') return error.InvalidSidecarRowRefKey;
        }
        return std.fmt.parseUnsigned(u64, bytes, 10) catch return error.InvalidSidecarRowRefKey;
    }
};

test "sidecar source binding validates serverless and external sources" {
    const serverless_binding = Binding{
        .sidecar_kind = .text,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    try serverless_binding.validate();

    const external_binding = Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-123",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    try external_binding.validate();
    try std.testing.expect(!sameSourceSnapshot(serverless_binding, external_binding));

    var invalid = external_binding;
    invalid.row_ref_kind = .serverless;
    try std.testing.expectError(error.InvalidSidecarSourceBinding, invalid.validate());

    const invalid_relational = Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .relational_store,
        .row_ref_kind = .external,
        .snapshot_id = "relational-1",
        .schema_fingerprint = "schema-v3",
        .column_bindings = &[_][]const u8{"tenant"},
        .index_config_hash = "sha256:algebraic",
    };
    try std.testing.expectError(error.InvalidSidecarSourceBinding, invalid_relational.validate());
}

test "sidecar row-ref cloning owns every string and is allocation-failure safe" {
    const alloc = std.testing.allocator;
    const original = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-123",
        .file_id = "part-a.parquet",
        .row_group_ordinal = 7,
        .row_ordinal = 42,
    } };

    const cloned = try cloneRowRefAlloc(alloc, original);
    defer freeOwnedRowRef(alloc, cloned);
    try std.testing.expectEqualStrings(original.external.source_id, cloned.external.source_id);
    try std.testing.expectEqualStrings(original.external.snapshot_id, cloned.external.snapshot_id);
    try std.testing.expectEqualStrings(original.external.file_id, cloned.external.file_id);
    try std.testing.expect(@intFromPtr(original.external.source_id.ptr) != @intFromPtr(cloned.external.source_id.ptr));
    try std.testing.expect(@intFromPtr(original.external.snapshot_id.ptr) != @intFromPtr(cloned.external.snapshot_id.ptr));
    try std.testing.expect(@intFromPtr(original.external.file_id.ptr) != @intFromPtr(cloned.external.file_id.ptr));

    const AllocationRunner = struct {
        fn run(a: std.mem.Allocator, row_ref: rowsource.RowRef) !void {
            const owned = try cloneRowRefAlloc(a, row_ref);
            defer freeOwnedRowRef(a, owned);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{original});
}

test "sidecar row-ref list decoding frees the original allocation on every failure" {
    const alloc = std.testing.allocator;
    const binding = Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-123",
        .schema_fingerprint = "schema-v1",
        .index_config_hash = "sha256:text:v1",
    };
    const keys = [_][]const u8{
        "ext:6:events:11:iceberg-123:14:part-a.parquet:7:42",
        "ext:6:events:11:iceberg-123:14:part-b.parquet:8:43",
    };

    const AllocationRunner = struct {
        fn run(a: std.mem.Allocator, source_binding: Binding, encoded: []const []const u8) !void {
            const refs = try rowRefsFromKeysAlloc(a, source_binding, encoded);
            defer freeOwnedRowRefs(a, refs);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{ binding, keys[0..] });

    const invalid_keys = [_][]const u8{ keys[0], "not-a-row-ref" };
    try std.testing.expectError(
        error.InvalidSidecarRowRefKey,
        rowRefsFromKeysAlloc(alloc, binding, invalid_keys[0..]),
    );
}

test "sidecar source binding validates batches and creates stable row-ref keys" {
    const alloc = std.testing.allocator;

    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-a", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-a", .row_ordinal = 1 } },
    };
    const bodies = [_][]const u8{ "alpha", "bravo" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-9" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const binding = bindingFromSnapshot(
        .text,
        .serverless_fragment,
        batch.snapshot,
        "schema-v1",
        &[_][]const u8{"body"},
        "sha256:text",
    );

    try validateBatchAgainstBinding(binding, batch);
    const key = try rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key);
    try std.testing.expectEqualStrings("srv:6:frag-a:1", key);
    const decoded = try rowRefFromKeyAlloc(alloc, key);
    defer freeOwnedRowRef(alloc, decoded);
    try validateRowRefAgainstBinding(binding, decoded);
    try std.testing.expectEqualStrings("frag-a", decoded.serverless.fragment_id);
    try std.testing.expectEqual(@as(u64, 1), decoded.serverless.row_ordinal);

    var wrong_snapshot = batch;
    wrong_snapshot.snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-10" };
    try std.testing.expectError(error.SidecarSourceBindingMismatch, validateBatchAgainstBinding(binding, wrong_snapshot));

    const typed_binding = bindingFromSnapshotWithColumnKinds(
        .text,
        .serverless_fragment,
        batch.snapshot,
        "schema-v1",
        &[_][]const u8{"body"},
        &[_]rowsource.ColumnKind{.bytes},
        "sha256:text",
    );
    try validateBatchAgainstBinding(typed_binding, batch);

    const wrong_kind_binding = bindingFromSnapshotWithColumnKinds(
        .text,
        .serverless_fragment,
        batch.snapshot,
        "schema-v1",
        &[_][]const u8{"body"},
        &[_]rowsource.ColumnKind{.json},
        "sha256:text",
    );
    try std.testing.expectError(error.SidecarSourceBindingMismatch, validateBatchAgainstBinding(wrong_kind_binding, batch));

    const cloned = try cloneAlloc(alloc, typed_binding);
    defer freeOwned(alloc, cloned);
    try std.testing.expect(sameColumnKinds(typed_binding, cloned));
    try validateBatchAgainstBinding(cloned, batch);
}

test "sidecar source binding validates external row-ref batches" {
    const alloc = std.testing.allocator;
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 2,
            .row_ordinal = 42,
        } },
    };
    const embeddings = [_][]const f32{&[_]f32{ 1.0, 0.0 }};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .vector_f32 = &embeddings } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "events", .snapshot_id = "iceberg-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const binding = bindingFromSnapshot(
        .vector,
        .external_iceberg,
        batch.snapshot,
        "schema-v2",
        &[_][]const u8{"embedding"},
        "sha256:vector",
    );

    try validateBatchAgainstBinding(binding, batch);
    const key = try rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key);
    try std.testing.expectEqualStrings("ext:6:events:9:iceberg-7:14:file-a.parquet:2:42", key);
    const decoded = try rowRefFromKeyAlloc(alloc, key);
    defer freeOwnedRowRef(alloc, decoded);
    try validateRowRefAgainstBinding(binding, decoded);
    try std.testing.expectEqualStrings("events", decoded.external.source_id);
    try std.testing.expectEqualStrings("iceberg-7", decoded.external.snapshot_id);
    try std.testing.expectEqualStrings("file-a.parquet", decoded.external.file_id);
    try std.testing.expectEqual(@as(u32, 2), decoded.external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 42), decoded.external.row_ordinal);

    const keys = [_][]const u8{key};
    const decoded_refs = try rowRefsFromKeysAlloc(alloc, binding, &keys);
    defer freeOwnedRowRefs(alloc, decoded_refs);
    try std.testing.expectEqual(@as(usize, 1), decoded_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", decoded_refs[0].external.file_id);

    var missing_column = batch;
    missing_column.columns = &.{};
    try std.testing.expectError(error.SidecarSourceBindingMismatch, validateBatchAgainstBinding(binding, missing_column));

    const stale_refs = [_]rowsource.RowRef{.{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 2,
        .row_ordinal = 42,
    } }};
    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        validateCandidateRowRefsAgainstBinding(binding, &stale_refs),
    );
}

test "sidecar row-ref key decoder rejects malformed and stale candidate keys" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidSidecarRowRefKey, rowRefFromKeyAlloc(alloc, "ext:6:events"));
    try std.testing.expectError(error.InvalidSidecarRowRefKey, rowRefFromKeyAlloc(alloc, "srv:6:frag-a:not-a-number"));
    try std.testing.expectError(error.InvalidSidecarRowRefKey, rowRefFromKeyAlloc(alloc, "rel:0:"));
    try std.testing.expectError(error.InvalidSidecarRowRefKey, rowRefFromKeyAlloc(alloc, "ext:6:events:9:iceberg-7:14:file-a.parquet:4294967296:42"));

    const stale_key = "ext:6:events:9:iceberg-8:14:file-a.parquet:2:42";
    const binding = Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        rowRefsFromKeysAlloc(alloc, binding, &[_][]const u8{stale_key}),
    );
}
