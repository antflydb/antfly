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

//! Retained local relational read session. DB acquires the admitted snapshot;
//! this owner holds its reader, policy lease, cancellation and output cleanup.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const mapper = @import("document_mapper.zig");
const row_policy_gate_mod = @import("row_policy_gate.zig");
const RelationalRows = @import("relational_rows.zig");
const platform_time = @import("antfly_platform").time;

pub const Session = struct {
    row_policy_lease: ?row_policy_gate_mod.Gate.Lease = null,
    range_proofs: ?[]@import("../range_protection.zig").Proof = null,
    page_bytes: usize = 16 * 1024 * 1024,
    alloc: Allocator,
    reader: RelationalRows.Reader = undefined,
    filter_context: ?*anyopaque = null,
    destroy_filter: ?*const fn (Allocator, *anyopaque) void = null,
    cancellation: @FieldType(types.ScanOptions, "cancellation"),
    deadline_ns: ?u64,

    pub fn deinit(session: *Session) void {
        if (session.range_proofs) |proofs| session.alloc.free(proofs);
        session.reader.deinit();
        if (session.filter_context) |filter| session.destroy_filter.?(session.alloc, filter);
        if (session.row_policy_lease) |*lease| lease.release();
        const owner = session.alloc;
        owner.destroy(session);
    }

    pub fn checkpoint(session: *const Session) !void {
        if (session.cancellation) |cancellation| try cancellation.check();
        if (session.deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (session.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    }
    pub fn rangeProofs(session: *Session, alloc: Allocator) ![]@import("../range_protection.zig").Proof {
        try session.checkpoint();
        return alloc.dupe(@import("../range_protection.zig").Proof, session.range_proofs orelse return error.SqlRangeTrackingRequired);
    }

    pub fn nextTypedPage(session: *Session, alloc: Allocator, io: ?std.Io, budget: RelationalRows.Budget) !RelationalRows.Page {
        try session.checkpoint();
        var bounded = budget;
        bounded.target_bytes = @min(bounded.output_bytes, if (bounded.target_bytes == 0) session.page_bytes else @min(bounded.target_bytes, session.page_bytes));
        var page = try session.reader.nextTypedPage(alloc, io, bounded);
        errdefer page.deinit();
        try session.checkpoint();
        return page;
    }

    /// Prepare logical rows under this reader's immutable schema epoch,
    /// without publishing primary rows, index effects or transaction state.
    /// SQL staging uses the same defaults/generated/CHECK pipeline as commit.
    /// Each returned key/value and the outer slice belong to alloc.
    pub fn normalizeRows(session: *Session, alloc: Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        if (writes.len > 4096) return error.InvalidArgument;
        try session.checkpoint();
        const view = session.reader.active;
        const normalized = try alloc.alloc(types.BatchWrite, writes.len);
        var initialized: usize = 0;
        errdefer {
            for (normalized[0..initialized]) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
                for (write.json_null_fields) |name| alloc.free(name);
                if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
            }
            alloc.free(normalized);
        }
        var output_bytes: usize = 0;
        for (writes, normalized) |write, *out| {
            try session.checkpoint();
            var prepared = try mapper.PreparedRelationalWrite.initTyped(alloc, alloc, alloc, false, write.key, write.value, view.validator(), view.tableSchema().*, view.physicalLayout(), write.json_null_fields, false);
            defer prepared.deinit(alloc);
            try prepared.requireLogicalRoot();
            const value = try std.json.Stringify.valueAlloc(alloc, prepared.parsedValue(), .{});
            errdefer alloc.free(value);
            output_bytes = std.math.add(usize, output_bytes, value.len +| write.key.len) catch return error.RelationalRowResultTooLarge;
            if (output_bytes > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
            const key = try alloc.dupe(u8, write.key);
            errdefer alloc.free(key);
            // Generated values can replace a submitted literal JSON null
            // with SQL NULL. Return provenance from the prepared authority.
            const typed = try prepared.typedView(view.tableSchema().*, view.physicalLayout());
            var null_names = std.ArrayListUnmanaged([]const u8).empty;
            defer null_names.deinit(alloc);
            for (write.json_null_fields) |name| {
                const ordinal = typed.ordinalForName(name) orelse return error.InvalidBatchRequest;
                const cell = (try typed.findCell(ordinal)) orelse continue;
                if (!cell.is_null) try null_names.append(alloc, name);
            }
            const nulls = try types.cloneJsonNullFields(alloc, null_names.items);
            out.* = .{ .key = key, .value = value, .json_null_fields = nulls };
            initialized += 1;
        }
        try session.checkpoint();
        return normalized;
    }
};
