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

//! Snapshot-local score reads. Physical identity is the complete encoded
//! metric/generation prefix, never just equivalent configuration. Callers
//! resolve freshness policies before allocating results or entering this layer.
const std = @import("std");
const keys = @import("../storage/internal_keys.zig");
pub const max_keys_per_read = 4096;
pub const max_key_bytes_per_read = 1024 * 1024;
pub const Stats = struct { keys: usize = 0, batches: usize = 0, plan_identity: u64 = 0 };

pub const PreparedRequest = @import("../storage/prepared_score_request.zig").PreparedScoreRequest;

pub const PreparedChunks = @import("../storage/prepared_score_request.zig").PreparedScoreChunks;

pub fn populateChunks(a: std.mem.Allocator, txn: anytype, request: *const PreparedChunks, columns: []const []?f64) !Stats {
    try request.validateColumns(columns);
    request.clearAbsentColumns(columns);
    errdefer for (columns) |column| @memset(column, null);
    var stats = Stats{};
    for (request.chunkPlans()) |*chunk| {
        var output = PreparedChunks.Output{ .owner = request, .chunk = chunk, .columns = columns };
        const read = try visitPrepared(a, txn, &chunk.request, output.sink());
        stats.keys += read.keys;
        stats.batches += read.batches;
    }
    request.finishColumns(columns);
    return stats;
}

/// Explicit callers declare repeated work. Clone a matching exclusively leased
/// canonical plan, retaining its identity without rebuilding or sorting keys.
pub fn prepareRequestForReuse(alloc: std.mem.Allocator, txn: anytype, prefixes: []const ?[]const u8, nodes: []const []const u8) !PreparedRequest {
    if (comptime @hasDecl(@typeInfo(@TypeOf(txn)).pointer.child, "openReadScope")) {
        var scope = txn.openReadScope(alloc) catch |err| switch (@as(anyerror, err)) {
            error.ReadOnly, error.Unsupported => return PreparedRequest.init(alloc, prefixes, nodes),
            else => return err,
        };
        defer scope.close();
        if (comptime @hasDecl(@TypeOf(scope), "preparedScorePlan")) {
            if (scope.preparedScorePlan()) |plan| {
                if (plan.matches(prefixes, nodes)) return PreparedRequest.fromPlan(alloc, plan);
            }
        }
    }
    return PreparedRequest.init(alloc, prefixes, nodes);
}

/// Reuse explicitly owned input identity. Resolve freshness before selecting
/// the request: its physical generation prefixes do not track policy changes.
/// Existing mutable-input callers keep populate(); unsupported backends retain
/// its bounded streaming fallback.
pub fn populatePrepared(caller_alloc: std.mem.Allocator, txn: anytype, request: *const PreparedRequest, columns: []const []?f64) !Stats {
    const plan = request.plan();
    if (!plan.valid) return error.InvalidBatch;
    if (columns.len != plan.prefixes.len) return error.InvalidBatch;
    for (columns) |column| if (column.len != plan.nodes.len) return error.InvalidBatch;
    errdefer for (columns) |column| @memset(column, null);
    if (plan.read_keys.len == 0) {
        for (columns) |column| @memset(column, null);
        return .{};
    }
    // Clear logical columns, not canonical prefixes: cloned column maps can
    // place null columns at different logical positions.
    for (plan.prefixes, 0..) |prefix, column| if (prefix == null) {
        @memset(columns[if (plan.column_order_identity) column else plan.column_remap[column]], null);
    };
    var output = @import("../storage/prepared_score_plan.zig").PreparedScorePlan.Output{ .plan = plan, .columns = columns };
    return visitPrepared(caller_alloc, txn, request, output.completeSink());
}

/// Read an immutable request without comparing caller inputs or rebuilding keys.
/// The sink assigns by canonical index; retries may overwrite partial delivery.
pub fn visitPrepared(caller_alloc: std.mem.Allocator, txn: anytype, request: *const PreparedRequest, sink: @import("../storage/backend_types.zig").CompleteScoreSink) !Stats {
    const plan = request.plan();
    if (!plan.valid) return error.InvalidBatch;
    if (plan.read_keys.len == 0) return .{};
    const scoped = comptime @hasDecl(@typeInfo(@TypeOf(txn)).pointer.child, "openReadScope");
    if (scoped) {
        var scope = txn.openReadScope(caller_alloc) catch |err| switch (@as(anyerror, err)) {
            error.ReadOnly, error.Unsupported => return streamPrepared(caller_alloc, txn, request, sink),
            else => return err,
        };
        var closed = false;
        defer if (!closed) scope.close();
        const Scope = @TypeOf(scope);
        if (comptime @hasDecl(Scope, "visitPreparedRequest")) {
            const supported = if (comptime @hasDecl(Scope, "supportsPreparedRequest")) scope.supportsPreparedRequest() else true;
            if (supported) {
                scope.visitPreparedRequest(request, sink) catch |err| switch (@as(anyerror, err)) {
                    error.OutOfMemory, error.ResourceBudgetExceeded => {
                        scope.close();
                        closed = true;
                        return streamPrepared(caller_alloc, txn, request, sink);
                    },
                    else => return err,
                };
                return .{ .keys = plan.read_keys.len, .batches = 1 };
            }
        }
    }
    return streamPrepared(caller_alloc, txn, request, sink);
}

// Canonical keys and fanout are already owned. Replaying original input arrays
// loses clone remaps and repeats encoding/sorting. Keep the same pinned txn and
// stream the owned key spans through a bounded transport instead.
fn streamPrepared(a: std.mem.Allocator, txn: anytype, request: *const PreparedRequest, sink: @import("../storage/backend_types.zig").CompleteScoreSink) !Stats {
    const read_keys = request.plan().read_keys;
    const scoped = comptime @hasDecl(@typeInfo(@TypeOf(txn)).pointer.child, "openReadScope");
    const Scope = if (scoped) @typeInfo(@TypeOf(txn.openReadScope(a))).error_union.payload else void;
    var scope: ?Scope = if (scoped) txn.openReadScope(a) catch |err|
        (if (err == error.ReadOnly or err == error.Unsupported) null else return err) else null;
    defer if (scoped) if (scope) |*active| active.close();
    var stats = Stats{ .keys = read_keys.len };
    const visitor = comptime scoped and @hasDecl(Scope, "visitManySortedScores");
    const use_visitor = visitor and scope != null and (if (comptime scoped and @hasDecl(Scope, "supportsScoreVisitor")) scope.?.supportsScoreVisitor() else true);
    const capacity: usize = @min(read_keys.len, 256);
    const values: []?[]const u8 = if (!use_visitor) try a.alloc(?[]const u8, capacity) else &.{};
    defer if (!use_visitor) a.free(values);
    var offset: usize = 0;
    while (offset < read_keys.len) {
        const count = @min(if (use_visitor) max_keys_per_read else capacity, read_keys.len - offset);
        const batch = read_keys[offset..][0..count];
        if (visitor and use_visitor) {
            const Output = struct {
                sink: @import("../storage/backend_types.zig").CompleteScoreSink,
                offset: usize,
                fn write(ctx: *anyopaque, i: usize, score: f64) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    self.sink.write(self.sink.context, self.offset + i, score);
                }
            };
            // Missing keys must overwrite any partial delivery from an earlier
            // failed attempt, as must tombstones in an overlay.
            for (0..count) |i| sink.write(sink.context, offset + i, null);
            var output = Output{ .sink = sink, .offset = offset };
            try scope.?.visitManySortedScores(batch, .{ .context = &output, .write = Output.write });
        } else {
            @memset(values[0..count], null);
            if (scoped and scope != null) try scope.?.getManySorted(batch, values[0..count]) else try txn.getManySorted(batch, values[0..count]);
            for (values[0..count], 0..) |raw, i| {
                const score = if (raw) |bytes| try decodePreparedScore(bytes) else null;
                sink.write(sink.context, offset + i, score);
            }
        }
        if (scoped and @hasDecl(Scope, "reset")) if (scope) |*active| {
            if (@typeInfo(@TypeOf(active.reset())) == .error_union) {
                active.reset() catch |err| if (err != error.ReadScopeResetUnsupported) {
                    return err;
                };
            } else active.reset();
        };
        offset += count;
        stats.batches += 1;
    }
    return stats;
}

fn decodePreparedScore(bytes: []const u8) !f64 {
    if (bytes.len != 8) return error.InvalidGraphMetricScore;
    const score: f64 = @bitCast(std.mem.readInt(u64, bytes[0..8], .little));
    if (!std.math.isFinite(score)) return error.InvalidGraphMetricScore;
    return score;
}

pub fn populate(
    caller_alloc: std.mem.Allocator,
    txn: anytype,
    prefixes: []const ?[]const u8,
    nodes: []const []const u8,
    columns: []const []?f64,
) !Stats {
    std.debug.assert(prefixes.len == columns.len);
    for (columns) |column| {
        std.debug.assert(column.len == nodes.len);
    }
    errdefer for (columns) |column| @memset(column, null);
    if (nodes.len == 0) return .{};
    var has_prefix = false;
    for (prefixes) |prefix| if (prefix != null) {
        has_prefix = true;
        break;
    };
    if (!has_prefix) {
        for (columns) |column| @memset(column, null);
        return .{};
    }
    const scoped = comptime @hasDecl(@typeInfo(@TypeOf(txn)).pointer.child, "openReadScope");
    const Scope = if (scoped) @typeInfo(@TypeOf(txn.openReadScope(caller_alloc))).error_union.payload else void;
    // Probe-only wrappers expose the method but cannot open a pinned scope.
    // Keep their existing batched view, and propagate allocation/storage errors.
    var scope: ?Scope = if (scoped) txn.openReadScope(caller_alloc) catch |err|
        (if (err == error.ReadOnly or err == error.Unsupported) null else return err) else null;
    defer if (scoped) {
        if (scope) |*active| active.close();
    };
    if (comptime scoped and @hasDecl(Scope, "preparedScorePlan") and @hasDecl(Scope, "visitManySortedScores")) {
        if (scope) |*active| {
            const supported = if (comptime @hasDecl(Scope, "supportsScoreVisitor")) active.supportsScoreVisitor() else true;
            if (supported) if (active.preparedScorePlan()) |plan| {
                const prepared = plan.prepare(active.preparedScoreAllocator(), prefixes, nodes) catch false;
                if (prepared) prepared_read: {
                    var output = @import("../storage/prepared_score_plan.zig").PreparedScorePlan.Output{ .plan = plan, .columns = columns };
                    const complete = if (comptime @hasDecl(Scope, "visitPreparedScores"))
                        (if (comptime @hasDecl(Scope, "supportsPreparedScores")) active.supportsPreparedScores() else true)
                    else
                        false;
                    if (complete) {
                        for (columns, prefixes) |column, prefix| if (prefix == null) {
                            @memset(column, null);
                        };
                    } else {
                        for (columns) |column| @memset(column, null);
                    }
                    const read = if (comptime @hasDecl(Scope, "visitPreparedScores"))
                        (if (complete) active.visitPreparedScores(plan, output.completeSink()) else active.visitManySortedScores(plan.read_keys, output.sink()))
                    else
                        active.visitManySortedScores(plan.read_keys, output.sink());
                    read catch |err| switch (@as(anyerror, err)) {
                        error.OutOfMemory, error.ResourceBudgetExceeded => {
                            // Prepared ownership must not raise the minimum
                            // memory needed by the streaming read path.
                            plan.deinit();
                            // A failed read may leave scratch capacity in this
                            // scope. Close it before admitting the streaming retry.
                            active.close();
                            scope = null;
                            scope = try txn.openReadScope(caller_alloc);
                            for (columns) |column| @memset(column, null);
                            break :prepared_read;
                        },
                        else => return err,
                    };
                    return .{ .keys = plan.read_keys.len, .batches = if (plan.read_keys.len == 0) 0 else 1, .plan_identity = plan.identity };
                } else {
                    // Plan construction itself can fail after partial work.
                    active.close();
                    scope = null;
                    scope = try txn.openReadScope(caller_alloc);
                }
            };
        }
    }
    for (columns) |column| @memset(column, null);
    const alloc = if (scoped and scope != null and @hasDecl(Scope, "workingAllocator")) scope.?.workingAllocator() else caller_alloc;
    const physical = try alloc.alloc(usize, prefixes.len);
    defer alloc.free(physical);
    var physical_count: usize = 0;
    for (prefixes, 0..) |prefix, i| if (prefix != null) {
        physical[physical_count] = i;
        physical_count += 1;
    };
    if (physical_count == 0) return .{};
    const Order = struct {
        prefixes: []const ?[]const u8,
        fn less(self: @This(), a: usize, b: usize) bool {
            const order = std.mem.order(u8, self.prefixes[a].?, self.prefixes[b].?);
            return order == .lt or (order == .eq and a < b);
        }
    };
    std.mem.sort(usize, physical[0..physical_count], Order{ .prefixes = prefixes }, Order.less);
    // Retain the sorted logical columns for the final independently-owned
    // result fanout; deduplicate physical columns in a separate compact list.
    const owners = try alloc.alloc(usize, physical_count);
    defer alloc.free(owners);
    var owner_count: usize = 0;
    for (physical[0..physical_count]) |column| {
        if (owner_count != 0 and std.mem.eql(u8, prefixes[owners[owner_count - 1]].?, prefixes[column].?)) continue;
        owners[owner_count] = column;
        owner_count += 1;
    }
    const rows = try alloc.alloc(usize, nodes.len);
    defer alloc.free(rows);
    for (rows, 0..) |*row, i| row.* = i;
    const RowOrder = struct {
        nodes: []const []const u8,
        fn less(self: @This(), a: usize, b: usize) bool {
            const order = std.mem.order(u8, self.nodes[a], self.nodes[b]);
            return order == .lt or (order == .eq and a < b);
        }
    };
    std.mem.sort(usize, rows, RowOrder{ .nodes = nodes }, RowOrder.less);
    var unique_count: usize = 1;
    for (rows[1..], rows[0 .. rows.len - 1]) |row, prior| {
        if (!std.mem.eql(u8, nodes[row], nodes[prior])) unique_count += 1;
    }
    const unique = if (unique_count == rows.len) rows else try alloc.alloc(usize, unique_count);
    defer if (unique_count != rows.len) alloc.free(unique);
    if (unique_count != rows.len) {
        var i: usize = 0;
        for (rows) |row| {
            if (i != 0 and std.mem.eql(u8, nodes[unique[i - 1]], nodes[row])) continue;
            unique[i] = row;
            i += 1;
        }
    }
    const total = std.math.mul(usize, owner_count, unique_count) catch return error.GraphMetricQueryBudgetExceeded;
    const batch_capacity = @min(total, max_keys_per_read);
    const read_keys = try alloc.alloc([]const u8, batch_capacity);
    defer alloc.free(read_keys);
    const direct = comptime if (scoped) @hasDecl(Scope, "getManySortedInto") else false;
    const visitor_direct = comptime if (scoped) @hasDecl(Scope, "visitManySortedScores") else false;
    const use_visitor = visitor_direct and scope != null and (if (comptime scoped and @hasDecl(Scope, "supportsScoreVisitor")) scope.?.supportsScoreVisitor() else true);
    const score_direct = comptime if (scoped) @hasDecl(Scope, "getManySortedScores") else false;
    const use_scores = !use_visitor and score_direct and scope != null and (if (comptime scoped and @hasDecl(Scope, "supportsDirectScores")) scope.?.supportsDirectScores() else true);
    const decoded_scores: []?f64 = if (use_scores) try alloc.alloc(?f64, batch_capacity) else &.{};
    defer if (use_scores) alloc.free(decoded_scores);
    const use_buffers = !use_visitor and !use_scores and direct and scope != null and (if (comptime scoped and @hasDecl(Scope, "supportsDirectBatchInto")) scope.?.supportsDirectBatchInto() else true);
    const score_buffers: [][8]u8 = if (use_buffers) try alloc.alloc([8]u8, batch_capacity) else &.{};
    defer if (use_buffers) alloc.free(score_buffers);
    const output_buffers: [][]u8 = if (use_buffers) try alloc.alloc([]u8, batch_capacity) else &.{};
    defer if (use_buffers) alloc.free(output_buffers);
    for (score_buffers, output_buffers) |*buffer, *out| out.* = buffer;
    const read_values: []?[]const u8 = if (!use_visitor and !use_scores) try alloc.alloc(?[]const u8, batch_capacity) else &.{};
    defer if (!use_visitor and !use_scores) alloc.free(read_values);
    var key_bytes = std.ArrayListUnmanaged(u8).empty;
    defer key_bytes.deinit(alloc);
    var stats = Stats{ .keys = total };
    var offset: usize = 0;
    while (offset < total) {
        var len: usize = 0;
        var bytes: usize = 0;
        for (0..@min(batch_capacity, total - offset)) |i| {
            const flat = offset + i;
            const prefix = prefixes[owners[flat / unique_count]].?;
            const node = nodes[unique[flat % unique_count]];
            const key_len = std.math.add(usize, prefix.len, keys.encodedComponentLen(node)) catch return error.GraphMetricQueryBudgetExceeded;
            // One oversized key may progress, subject to the caller's live
            // allocation budget. Never multiply long IDs by the key-count cap.
            if (len != 0 and key_len > max_key_bytes_per_read -| bytes) break;
            bytes = std.math.add(usize, bytes, key_len) catch return error.GraphMetricQueryBudgetExceeded;
            len += 1;
        }
        key_bytes.clearRetainingCapacity();
        try key_bytes.ensureTotalCapacity(alloc, bytes);
        for (read_keys[0..len], 0..) |*key, i| {
            const flat = offset + i;
            const start = key_bytes.items.len;
            try key_bytes.appendSlice(alloc, prefixes[owners[flat / unique_count]].?);
            try keys.appendEncodedComponent(&key_bytes, alloc, nodes[unique[flat % unique_count]]);
            key.* = key_bytes.items[start..];
        }
        if (visitor_direct and use_visitor) {
            const Output = struct {
                columns: []const []?f64,
                owners: []const usize,
                unique: []const usize,
                offset: usize,
                fn write(ctx: *anyopaque, index: usize, score: f64) void {
                    const out: *@This() = @ptrCast(@alignCast(ctx));
                    const flat = out.offset + index;
                    out.columns[out.owners[flat / out.unique.len]][out.unique[flat % out.unique.len]] = score;
                }
            };
            var output = Output{ .columns = columns, .owners = owners[0..owner_count], .unique = unique, .offset = offset };
            try scope.?.visitManySortedScores(read_keys[0..len], .{ .context = &output, .write = Output.write });
        } else if (score_direct and use_scores) {
            try scope.?.getManySortedScores(read_keys[0..len], decoded_scores[0..len]);
            for (decoded_scores[0..len], 0..) |score, i| {
                const flat = offset + i;
                columns[owners[flat / unique_count]][unique[flat % unique_count]] = score;
            }
        } else {
            @memset(read_values[0..len], null);
            if (scoped and scope != null) {
                if (direct) {
                    if (use_buffers) scope.?.getManySortedInto(read_keys[0..len], output_buffers[0..len], read_values[0..len]) catch |err| switch (@as(anyerror, err)) {
                        error.BufferTooSmall => return error.InvalidGraphMetricScore,
                        else => return err,
                    } else try scope.?.getManySorted(read_keys[0..len], read_values[0..len]);
                } else try scope.?.getManySorted(read_keys[0..len], read_values[0..len]);
            } else try txn.getManySorted(read_keys[0..len], read_values[0..len]);
            for (read_values[0..len], 0..) |maybe_raw, i| {
                const raw = maybe_raw orelse continue;
                if (raw.len != 8) return error.InvalidGraphMetricScore;
                const score: f64 = @bitCast(std.mem.readInt(u64, raw[0..8], .little));
                if (!std.math.isFinite(score)) return error.InvalidGraphMetricScore;
                const flat = offset + i;
                columns[owners[flat / unique_count]][unique[flat % unique_count]] = score;
            }
        }
        if (scoped and @hasDecl(Scope, "reset")) {
            if (scope) |*active| {
                if (@typeInfo(@TypeOf(active.reset())) == .error_union) {
                    active.reset() catch |err| {
                        if (err != error.ReadScopeResetUnsupported) return err;
                    };
                } else active.reset();
            }
        }
        offset += len;
        stats.batches += 1;
    }
    if (unique_count != rows.len) for (owners[0..owner_count]) |owner| {
        var representative = rows[0];
        for (rows[1..]) |row| {
            if (std.mem.eql(u8, nodes[row], nodes[representative])) {
                columns[owner][row] = columns[owner][representative];
            } else representative = row;
        }
    };
    var owner = physical[0];
    for (physical[1..physical_count]) |column| {
        if (std.mem.eql(u8, prefixes[owner].?, prefixes[column].?)) {
            @memcpy(columns[column], columns[owner]);
        } else owner = column;
    }
    return stats;
}

test "graph metric physical score reads bound encoded bytes as well as key count" {
    const alloc = std.testing.allocator;
    const buffers = try alloc.alloc([4096]u8, 600);
    defer alloc.free(buffers);
    const nodes = try alloc.alloc([]const u8, buffers.len);
    defer alloc.free(nodes);
    for (buffers, nodes, 0..) |*buffer, *node, i| {
        @memset(buffer, 'x');
        _ = try std.fmt.bufPrint(buffer[4090..], "{d:0>6}", .{i});
        node.* = buffer;
    }
    const column = try alloc.alloc(?f64, nodes.len);
    defer alloc.free(column);
    const Txn = struct {
        raw: [8]u8 = @bitCast(@as(f64, 3)),
        pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
            var bytes: usize = 0;
            for (requested, values) |key, *value| {
                bytes += key.len;
                value.* = &self.raw;
            }
            try std.testing.expect(bytes <= max_key_bytes_per_read or requested.len == 1);
        }
    };
    var txn = Txn{};
    const result = try populate(alloc, &txn, &.{"prefix/"}, nodes, &.{column});
    try std.testing.expectEqual(@as(usize, 3), result.batches);
    try std.testing.expectEqual(nodes.len, result.keys);
    for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
}

test "graph metric physical score reads deduplicate keys and retain logical ownership" {
    const Runner = struct {
        const Txn = struct {
            raw: [8]u8 = @bitCast(@as(f64, 1.5)),
            seen: usize = 0,
            pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
                for (requested, 0..) |key, i| {
                    if (i > 0) try std.testing.expect(std.mem.order(u8, requested[i - 1], key) == .lt);
                    values[i] = if (std.mem.endsWith(u8, key, "missing\x00\x00")) null else &self.raw;
                }
                self.seen += requested.len;
            }
        };
        fn run(alloc: std.mem.Allocator) !void {
            const nodes = [_][]const u8{ "z", "a\x00b", "z", "missing", "" };
            const prefixes = [_]?[]const u8{ "b\x00\x00", "a\x00\x00", "b\x00\x00", null };
            var columns: [prefixes.len][]?f64 = undefined;
            var initialized: usize = 0;
            defer for (columns[0..initialized]) |column| alloc.free(column);
            for (&columns) |*column| {
                column.* = try alloc.alloc(?f64, nodes.len);
                initialized += 1;
            }
            var txn = Txn{};
            const stats = try populate(alloc, &txn, &prefixes, &nodes, &columns);
            try std.testing.expectEqual(@as(usize, 8), stats.keys);
            try std.testing.expectEqual(stats.keys, txn.seen);
            for (columns[0..3]) |column| try std.testing.expectEqualSlices(?f64, &.{ 1.5, 1.5, 1.5, null, 1.5 }, column);
            for (columns[3]) |score| try std.testing.expectEqual(@as(?f64, null), score);
            columns[0][0] = 9;
            try std.testing.expectEqual(@as(?f64, 1.5), columns[2][0]);
            txn.raw = @bitCast(std.math.inf(f64));
            _ = populate(alloc, &txn, &prefixes, &nodes, &columns) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidGraphMetricScore, err);
                return;
            };
            return error.TestUnexpectedResult;
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric physical score reads use caller buffers and close scope on errors" {
    const backend = @import("../storage/backend_erased.zig");
    const Txn = struct {
        calls: usize = 0,
        closed: usize = 0,
        resets: usize = 0,
        length: usize = 8,
        raw: [9]u8 = .{ 0, 0, 0, 0, 0, 0, 8, 64, 0 }, // 3.0, little endian
        const Self = @This();
        const Scope = struct {
            parent: *Self,
            pub fn get(_: *@This(), _: []const u8) ![]const u8 {
                return error.TestUnexpectedResult;
            }
            pub fn getManySortedInto(self: *@This(), requested: []const []const u8, buffers: []const []u8, values: []?[]const u8) !void {
                self.parent.calls += 1;
                for (requested, buffers, values) |_, buffer, *value| {
                    if (self.parent.length > buffer.len) return error.BufferTooSmall;
                    @memcpy(buffer[0..self.parent.length], self.parent.raw[0..self.parent.length]);
                    value.* = buffer[0..self.parent.length];
                }
            }
            pub fn reset(self: *@This()) void {
                self.parent.resets += 1;
            }
            pub fn close(self: *@This()) void {
                self.parent.closed += 1;
            }
        };
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
        pub fn openReadScope(self: *@This(), a: std.mem.Allocator) !backend.ReadScope {
            return backend.readScopeFrom(a, Scope{ .parent = self });
        }
    };
    var txn = Txn{};
    var scores: [3]?f64 = undefined;
    _ = try populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 3, 3, 3 }, &scores);
    try std.testing.expectEqual(@as(usize, 1), txn.calls);
    try std.testing.expectEqual(@as(usize, 1), txn.resets);
    try std.testing.expectEqual(@as(usize, 1), txn.closed);
    txn.length = 9;
    try std.testing.expectError(error.InvalidGraphMetricScore, populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores}));
    try std.testing.expectEqual(@as(usize, 2), txn.closed);
    txn.length = 7;
    try std.testing.expectError(error.InvalidGraphMetricScore, populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores}));
    try std.testing.expectEqual(@as(usize, 3), txn.closed);
}

test "graph metric physical score reads preserve probe-only and legacy scope fallbacks" {
    const Probe = struct {
        pub fn openReadScope(_: *@This(), _: std.mem.Allocator) !@import("../storage/backend_erased.zig").ReadScope {
            return error.ReadOnly;
        }
        raw: [8]u8 = @bitCast(@as(f64, 3)),
        pub fn getManySorted(self: *@This(), _: []const []const u8, values: []?[]const u8) !void {
            for (values) |*value| value.* = &self.raw;
        }
    };
    var probe = Probe{};
    var scores: [2]?f64 = undefined;
    _ = try populate(std.testing.allocator, &probe, &.{"p/"}, &.{ "b", "a" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 3, 3 }, &scores);
    const Legacy = struct {
        const Scope = struct {
            raw: [8]u8 = @bitCast(@as(f64, 4)),
            pub fn getManySorted(self: *@This(), _: []const []const u8, values: []?[]const u8) !void {
                for (values) |*value| value.* = &self.raw;
            }
            pub fn reset(_: *@This()) void {}
            pub fn close(_: *@This()) void {}
        };
        pub fn openReadScope(_: *@This(), _: std.mem.Allocator) !Scope {
            return .{};
        }
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var legacy = Legacy{};
    _ = try populate(std.testing.allocator, &legacy, &.{"p/"}, &.{ "b", "a" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 4, 4 }, &scores);
}

test "graph metric physical score reads respect erased direct capability" {
    const erased = @import("../storage/backend_erased.zig");
    const Borrowed = struct {
        raw: [8]u8 = @bitCast(@as(f64, 7)),
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return &self.raw;
        }
        pub fn getManySorted(self: *@This(), _: []const []const u8, values: []?[]const u8) !void {
            for (values) |*value| value.* = &self.raw;
        }
        pub fn reset(_: *@This()) void {}
        pub fn close(_: *@This()) void {}
    };
    const Txn = struct {
        pub fn openReadScope(_: *@This(), a: std.mem.Allocator) !erased.ReadScope {
            return erased.readScopeFrom(a, Borrowed{});
        }
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var txn = Txn{};
    var scope = try txn.openReadScope(std.testing.allocator);
    defer scope.close();
    try std.testing.expect(!scope.supportsDirectBatchInto());
    var columns: [2]?f64 = undefined;
    _ = try populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "a", "b" }, &.{&columns});
    try std.testing.expectEqualSlices(?f64, &.{ 7, 7 }, &columns);
    // Presence of the method is insufficient: a copying adapter may expose it.
    const DirectOffTxn = struct {
        const Scope = struct {
            borrowed: Borrowed = .{},
            pub fn supportsDirectBatchInto(_: *const @This()) bool {
                return false;
            }
            pub fn getManySorted(self: *@This(), keys_: []const []const u8, values: []?[]const u8) !void {
                return self.borrowed.getManySorted(keys_, values);
            }
            pub fn getManySortedInto(_: *@This(), _: []const []const u8, _: []const []u8, _: []?[]const u8) !void {
                return error.TestUnexpectedResult;
            }
            pub fn reset(_: *@This()) void {}
            pub fn close(_: *@This()) void {}
        };
        pub fn openReadScope(_: *@This(), _: std.mem.Allocator) !Scope {
            return .{};
        }
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var direct_off = DirectOffTxn{};
    _ = try populate(std.testing.allocator, &direct_off, &.{"p/"}, &.{ "a", "b" }, &.{&columns});
    try std.testing.expectEqualSlices(?f64, &.{ 7, 7 }, &columns);
    // The public portable API still copies into caller buffers when requested.
    var slots: [2][8]u8 = undefined;
    var copied: [2]?[]const u8 = undefined;
    try scope.getManySortedInto(&.{ "a", "b" }, &.{ &slots[0], &slots[1] }, &copied);
    try scope.reset();
    for (copied, &slots) |value, *slot| try std.testing.expectEqual(slot.ptr, value.?.ptr);
}

test "graph metric physical review cloned permutation fallback preserves logical rows" {
    const a = std.testing.allocator;
    const Plan = @import("../storage/prepared_score_plan.zig").PreparedScorePlan;
    var source = Plan{};
    defer source.deinit();
    try std.testing.expect(try source.prepare(a, &.{""}, &.{ "a", "b" }));
    try std.testing.expect(try source.prepare(a, &.{""}, &.{ "b", "a" }));
    var request = try PreparedRequest.fromPlan(a, &source);
    defer request.deinit();
    const Probe = struct {
        raw: [2][8]u8 = undefined,
        pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
            for (requested, values) |key, *value| {
                const row: usize = if (key[0] == 'a') 0 else 1;
                std.mem.writeInt(u64, &self.raw[row], @bitCast(@as(f64, if (row == 0) 1 else 2)), .little);
                value.* = &self.raw[row];
            }
        }
    };
    var txn = Probe{};
    var column: [2]?f64 = undefined;
    _ = try populatePrepared(a, &txn, &request, &.{&column});
    try std.testing.expectEqualSlices(?f64, &.{ 2, 1 }, &column);
}

test "graph metric physical cloned fallback preserves null column remaps and clears partial pressure reads" {
    const a = std.testing.allocator;
    const Plan = @import("../storage/prepared_score_plan.zig").PreparedScorePlan;
    var source = Plan{};
    defer source.deinit();
    try std.testing.expect(try source.prepare(a, &.{ "p/", null, "q/", "p/" }, &.{ "a", "b", "a" }));
    try std.testing.expect(try source.prepare(a, &.{ null, "q/", "p/", "p/" }, &.{ "b", "a", "a" }));
    var request = try PreparedRequest.fromPlan(a, &source);
    defer request.deinit();
    const Probe = struct {
        raw: [4][8]u8 = undefined,
        pressure: bool,
        closed: usize = 0,
        const Self = @This();
        const Scope = struct {
            parent: *Self,
            pub fn close(self: *@This()) void {
                self.parent.closed += 1;
            }
            pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
                return self.parent.getManySorted(requested, values);
            }
            pub fn supportsPreparedRequest(self: *const @This()) bool {
                return self.parent.pressure;
            }
            pub fn visitPreparedRequest(_: *@This(), _: *const PreparedRequest, sink: @import("../storage/backend_types.zig").CompleteScoreSink) !void {
                sink.write(sink.context, 0, 99);
                return error.ResourceBudgetExceeded;
            }
        };
        pub fn openReadScope(self: *@This(), _: std.mem.Allocator) !Scope {
            return .{ .parent = self };
        }
        pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
            for (requested, values) |key, *value| {
                const index: usize = (if (key[0] == 'p') @as(usize, 0) else 2) + (if (key[2] == 'a') @as(usize, 0) else 1);
                std.mem.writeInt(u64, &self.raw[index], @bitCast(@as(f64, @floatFromInt(index + 1))), .little);
                value.* = &self.raw[index];
            }
        }
    };
    for ([_]bool{ false, true }) |pressure| {
        var txn = Probe{ .pressure = pressure };
        var columns: [4][3]?f64 = @splat(@splat(77));
        _ = try populatePrepared(a, &txn, &request, &.{ &columns[0], &columns[1], &columns[2], &columns[3] });
        try std.testing.expectEqualSlices(?f64, &.{ null, null, null }, &columns[0]);
        try std.testing.expectEqualSlices(?f64, &.{ 4, 3, 3 }, &columns[1]);
        try std.testing.expectEqualSlices(?f64, &.{ 2, 1, 1 }, &columns[2]);
        try std.testing.expectEqualSlices(?f64, &columns[2], &columns[3]);
        try std.testing.expectEqual(@as(usize, 2), txn.closed);
    }
}
