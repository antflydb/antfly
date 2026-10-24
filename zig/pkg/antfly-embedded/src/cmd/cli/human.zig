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

//! Human presentation is a projection of the public result contract. JSON is
//! always an explicit escape hatch; presentation never changes execution.
const std = @import("std");
const agent = @import("agent.zig");
const Value = std.json.Value;
const Allocator = std.mem.Allocator;

pub const Options = struct { width: usize = 100 };
const Row = []const []const u8;

fn get(value: Value, key: []const u8) Value {
    return if (value == .object) value.object.get(key) orelse .null else .null;
}
fn string(value: Value) ?[]const u8 {
    return if (value == .string) value.string else null;
}
fn array(value: Value) []const Value {
    return if (value == .array) value.array.items else &.{};
}
fn typeName(value: Value) []const u8 {
    return switch (value) {
        .number_string, .integer, .float => "number",
        .bool => "boolean",
        else => @tagName(value),
    };
}
fn truth(value: Value) bool {
    return value == .bool and value.bool;
}

// Terminal data must never execute escape sequences, move the cursor, inject
// new rows or reverse visible text. Narrative newlines are handled separately.
fn clean(a: Allocator, raw: []const u8) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(a);
    var i: usize = 0;
    while (i < raw.len) {
        const n = std.unicode.utf8ByteSequenceLength(raw[i]) catch 1;
        const end = @min(raw.len, i + n);
        const cp = std.unicode.utf8Decode(raw[i..end]) catch {
            try output.writer.print("\\x{X:0>2}", .{raw[i]});
            i += 1;
            continue;
        };
        switch (cp) {
            '\n' => try output.writer.writeAll("\\n"),
            '\r' => try output.writer.writeAll("\\r"),
            '\t' => try output.writer.writeAll("\\t"),
            0...8, 11...12, 14...31, 127...159, 0x202a...0x202e, 0x2066...0x2069 => try output.writer.print("\\u{X:0>4}", .{cp}),
            else => try output.writer.writeAll(raw[i..end]),
        }
        i = end;
    }
    return output.written();
}

fn codepointWidth(cp: u21) usize {
    if ((cp >= 0x300 and cp <= 0x36f) or (cp >= 0x1ab0 and cp <= 0x1aff) or
        (cp >= 0x1dc0 and cp <= 0x1dff) or cp == 0x200d or
        (cp >= 0xfe00 and cp <= 0xfe0f) or (cp >= 0xfe20 and cp <= 0xfe2f)) return 0;
    if (cp >= 0x1100 and (cp <= 0x115f or cp == 0x2329 or cp == 0x232a or
        (cp >= 0x2e80 and cp <= 0xa4cf and cp != 0x303f) or
        (cp >= 0xac00 and cp <= 0xd7a3) or (cp >= 0xf900 and cp <= 0xfaff) or
        (cp >= 0xfe10 and cp <= 0xfe19) or (cp >= 0xfe30 and cp <= 0xfe6f) or
        (cp >= 0xff00 and cp <= 0xff60) or (cp >= 0xffe0 and cp <= 0xffe6) or
        (cp >= 0x1f300 and cp <= 0x1faff) or (cp >= 0x20000 and cp <= 0x3fffd))) return 2;
    return 1;
}
fn width(raw: []const u8) usize {
    var it = std.unicode.Utf8View.initUnchecked(raw).iterator();
    var result: usize = 0;
    while (it.nextCodepoint()) |cp| result += codepointWidth(cp);
    return result;
}

const State = struct {
    a: Allocator,
    out: std.Io.Writer.Allocating,
    limits: agent.Limits,
    columns: usize,
    rows_left: usize,
    clipped: bool = false,
    omitted_rows: bool = false,
    byte_limit: bool = false,
    exempt: bool = false,
    table_rows_shown: usize = 0,

    fn emit(self: *State, bytes: []const u8, force: bool) !void {
        if (!force and !self.exempt and (self.byte_limit or bytes.len > (self.limits.bytes -| 512) -| self.out.written().len)) {
            self.byte_limit = true;
            return;
        }
        try self.out.writer.writeAll(bytes);
    }
    fn line(self: *State, comptime fmt: []const u8, args: anytype) !void {
        try self.emit(try std.fmt.allocPrint(self.a, fmt ++ "\n", args), false);
    }
    fn text(self: *State, value: Value) ![]const u8 {
        return switch (value) {
            .string => |raw| if (raw.len == 0) "\"\"" else try clean(self.a, raw),
            .null => "null",
            .bool => |v| if (v) "true" else "false",
            .number_string => |v| v,
            else => try clean(self.a, try std.json.Stringify.valueAlloc(self.a, value, .{})),
        };
    }
    fn cell(self: *State, writer: *std.Io.Writer, raw: []const u8, available: usize) !void {
        const must_clip = width(raw) > available or raw.len > self.limits.cell_bytes;
        const cap = if (must_clip) available -| 3 else available;
        var used: usize = 0;
        var consumed: usize = 0;
        var it = std.unicode.Utf8View.initUnchecked(raw).iterator();
        while (it.nextCodepointSlice()) |slice| {
            const w = codepointWidth(std.unicode.utf8Decode(slice) catch unreachable);
            if (used + w > cap or consumed + slice.len > self.limits.cell_bytes) break;
            try writer.writeAll(slice);
            used += w;
            consumed += slice.len;
        }
        if (must_clip) {
            try writer.writeAll("...");
            used += 3;
            self.clipped = true;
        }
        try writer.splatByteAll(' ', available -| used);
    }
    fn gridLine(self: *State, values: Row, widths: []const usize) !void {
        var row: std.Io.Writer.Allocating = .init(self.a);
        try row.writer.writeByte('|');
        for (widths, 0..) |w, i| {
            try row.writer.writeByte(' ');
            try self.cell(&row.writer, if (i < values.len) values[i] else "", w);
            try row.writer.writeAll(" |");
        }
        try row.writer.writeByte('\n');
        try self.emit(row.written(), false);
    }
    fn rule(self: *State, widths: []const usize) !void {
        var row: std.Io.Writer.Allocating = .init(self.a);
        try row.writer.writeByte('+');
        for (widths) |w| {
            try row.writer.splatByteAll('-', w + 2);
            try row.writer.writeByte('+');
        }
        try row.writer.writeByte('\n');
        try self.emit(row.written(), false);
    }
    fn grid(self: *State, headers: Row, rows: []const Row) !void {
        if (headers.len == 0) {
            try self.line("(no columns)", .{});
            return;
        }
        const n = @min(headers.len, @max(@as(usize, 1), (self.columns -| 1) / 6));
        self.clipped = self.clipped or n < headers.len;
        const widths = try self.a.alloc(usize, n);
        var total: usize = 1 + n * 3;
        for (widths, 0..) |*w, i| {
            w.* = @max(@as(usize, 3), @min(@as(usize, 48), width(headers[i])));
            for (rows) |row| if (i < row.len) {
                w.* = @max(w.*, @min(@as(usize, 48), width(row[i])));
            };
            total += w.*;
        }
        while (total > self.columns) {
            var largest: usize = 0;
            for (widths, 0..) |w, i| if (w > widths[largest]) {
                largest = i;
            };
            if (widths[largest] <= 3) break;
            widths[largest] -= 1;
            total -= 1;
        }
        try self.rule(widths);
        try self.gridLine(headers, widths);
        try self.rule(widths);
        for (rows) |row| {
            const before = self.out.written().len;
            try self.gridLine(row, widths);
            if (self.out.written().len > before) self.table_rows_shown += 1;
        }
        try self.rule(widths);
    }
    fn take(self: *State, received: usize) usize {
        const n = @min(self.rows_left, received);
        self.rows_left -= n;
        self.omitted_rows = self.omitted_rows or n < received;
        return n;
    }
    fn required(self: *State, name: []const u8, value: Value) anyerror!void {
        // Recovery controls are outside display budgets and cannot disappear
        // inside an abbreviated table cell.
        if (value == .null) return;
        if (value == .object) {
            var fields = value.object.iterator();
            while (fields.next()) |field| try self.required(try std.fmt.allocPrint(self.a, "{s}.{s}", .{ name, field.key_ptr.* }), field.value_ptr.*);
        } else {
            const safe_name = try clean(self.a, name);
            try self.emit(try std.fmt.allocPrint(self.a, "{s}: {s}\n", .{ safe_name, try self.text(value) }), true);
        }
    }
    fn controls(self: *State, value: Value) !void {
        for ([_][]const u8{ "command_tag", "rows_affected", "mutation_outcome", "transaction_id", "transaction_status", "session_id", "connection_id", "ddl_receipt", "receipt", "receipts" }) |key| try self.required(key, get(value, key));
    }
    fn sql(self: *State, value: Value) !void {
        const cols = array(get(value, "columns"));
        const all_rows = array(get(value, "rows"));
        const masks = array(get(value, "sql_nulls"));
        const count = self.take(all_rows.len);
        const headers = try self.a.alloc([]const u8, cols.len);
        for (cols, headers) |col, *h| h.* = try clean(self.a, string(get(col, "name")) orelse "?");
        const rows = try self.a.alloc(Row, count);
        for (rows, 0..) |*row, i| {
            const cells = array(all_rows[i]);
            const mask = if (i < masks.len) array(masks[i]) else &.{};
            const values = try self.a.alloc([]const u8, cols.len);
            for (values, 0..) |*text_value, j| {
                const cell_value = if (j < cells.len) cells[j] else Value.null;
                const sql_null = if (j < mask.len) truth(mask[j]) else cell_value == .null;
                const json_column = std.mem.eql(u8, string(get(cols[j], "type")) orelse "", "json");
                text_value.* = if (sql_null) "NULL" else if (json_column) blk: {
                    if (cell_value == .null) break :blk "json:null";
                    break :blk try clean(self.a, try std.json.Stringify.valueAlloc(self.a, cell_value, .{}));
                } else if (cell_value == .string and (std.mem.eql(u8, cell_value.string, "NULL") or cell_value.string.len == 0))
                    try std.json.Stringify.valueAlloc(self.a, cell_value, .{})
                else
                    try self.text(cell_value);
            }
            row.* = values;
        }
        if (cols.len > 0) try self.grid(headers, rows);
        try self.line("{d} of {d} returned row(s) shown.", .{ count, all_rows.len });
        try self.controls(value);
    }
    fn object(self: *State, value: Value, preserve: bool) !void {
        if (value != .object) {
            try self.line("{s}", .{try self.text(value)});
            return;
        }
        const n = if (preserve) value.object.count() else self.take(value.object.count());
        const rows = try self.a.alloc(Row, n);
        for (rows, 0..) |*row, i| {
            const field = value.object.values()[i];
            row.* = try self.a.dupe([]const u8, &.{ try clean(self.a, value.object.keys()[i]), typeName(field), try self.text(field) });
        }
        try self.grid(&.{ "FIELD", "TYPE", "VALUE" }, rows);
        if (!preserve) try self.line("{d} of {d} field(s) shown.", .{ n, value.object.count() });
        if (preserve) {
            // A generic administrative response can contain recovery data
            // unknown to this CLI version. Print every field in full too when
            // its table representation was abbreviated.
            if (self.clipped) {
                var fields = value.object.iterator();
                while (fields.next()) |field| try self.required(field.key_ptr.*, field.value_ptr.*);
            }
        }
    }
    fn objects(self: *State, items: []const Value) !void {
        const count = self.take(items.len);
        var keys: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (items[0..count]) |item| if (item == .object) {
            for (item.object.keys()) |key| try keys.put(self.a, key, {});
        };
        if (keys.count() == 0) {
            const rows = try self.a.alloc(Row, count);
            for (rows, items[0..count]) |*row, item| row.* = try self.a.dupe([]const u8, &.{try self.text(item)});
            try self.grid(&.{"VALUE"}, rows);
        } else {
            const headers = try self.a.alloc([]const u8, keys.count());
            for (headers, keys.keys()) |*h, key| h.* = try clean(self.a, key);
            const rows = try self.a.alloc(Row, count);
            for (rows, items[0..count]) |*row, item| {
                const values = try self.a.alloc([]const u8, keys.count());
                for (values, keys.keys()) |*v, key| v.* = try self.text(get(item, key));
                row.* = values;
            }
            try self.grid(headers, rows);
        }
        try self.line("{d} of {d} returned item(s) shown.", .{ count, items.len });
    }
    fn hits(self: *State, value: Value, sources_only: bool) !void {
        const all_hits = array(value);
        const count = self.take(all_hits.len);
        var fields: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (all_hits[0..count]) |hit| {
            const source = get(hit, "_source");
            if (source == .object) for (source.object.keys()) |key| {
                if (!sources_only or std.mem.eql(u8, key, "title") or std.mem.eql(u8, key, "url")) try fields.put(self.a, key, {});
            };
        }
        const headers = try self.a.alloc([]const u8, fields.count() + 2);
        headers[0] = "ID";
        headers[1] = "SCORE";
        for (fields.keys(), headers[2..]) |field, *h| h.* = try clean(self.a, field);
        const rows = try self.a.alloc(Row, count);
        for (rows, all_hits[0..count]) |*row, hit| {
            const values = try self.a.alloc([]const u8, headers.len);
            values[0] = try self.text(get(hit, "_id"));
            const score = get(hit, "_score");
            values[1] = if (score == .number_string) blk: {
                const number = std.fmt.parseFloat(f64, score.number_string) catch break :blk try self.text(score);
                break :blk try std.fmt.allocPrint(self.a, "{d:.4}", .{number});
            } else try self.text(score);
            for (fields.keys(), values[2..]) |field, *v| v.* = try self.text(get(get(hit, "_source"), field));
            row.* = values;
        }
        try self.grid(headers, rows);
        try self.line("{d} of {d} returned hit(s) shown.", .{ count, all_hits.len });
    }
    fn search(self: *State, value: Value) !void {
        for (array(get(value, "responses"))) |response| {
            if (string(get(response, "table"))) |table| try self.line("Table: {s}", .{try clean(self.a, table)});
            const hits_value = get(response, "hits");
            if (hits_value != .null) try self.hits(get(hits_value, "hits"), false);
            const total = get(hits_value, "total");
            if (get(total, "value") != .null) {
                const relation = string(get(total, "relation")) orelse "unknown";
                try self.line("Matches {s} {s} (server relation: {s}).", .{ if (std.mem.eql(u8, relation, "eq")) "=" else if (std.mem.eql(u8, relation, "gte")) ">=" else "?", try self.text(get(total, "value")), try clean(self.a, relation) });
            }
            try self.line("Requested top-k; {s} ms; status {s}.", .{ try self.text(get(response, "took")), try self.text(get(response, "status")) });
            if (get(response, "error") != .null) try self.line("Error: {s}", .{try self.text(get(response, "error"))});
            if (get(response, "aggregations") != .null) {
                try self.line("Aggregations", .{});
                try self.object(get(response, "aggregations"), false);
            }
        }
    }
    fn schema(self: *State, tables: []const Value) !void {
        for (tables) |table| {
            try self.line("Table: {s}.{s}.{s}", .{ try self.text(get(table, "database")), try self.text(get(table, "namespace")), try self.text(get(table, "name")) });
            try self.line("Columns", .{});
            try self.objects(array(get(table, "columns")));
            try self.line("Indexes", .{});
            try self.objects(array(get(table, "indexes")));
            const caps = array(get(table, "field_capabilities"));
            if (caps.len > 0) {
                try self.line("Search capabilities", .{});
                const n = self.take(caps.len);
                const rows = try self.a.alloc(Row, n);
                for (rows, caps[0..n]) |*row, cap| row.* = try self.a.dupe([]const u8, &.{ try self.text(get(cap, "field")), try self.text(get(cap, "value_operations")), try self.text(get(cap, "sortable")) });
                try self.grid(&.{ "FIELD", "VALUE OPERATIONS", "SORTABLE" }, rows);
            }
        }
    }
    fn plan(self: *State, value: Value, depth: usize) anyerror!void {
        if (depth > 32) {
            self.clipped = true;
            return;
        }
        if (get(value, "node_type") == .null) {
            try self.object(value, false);
            return;
        }
        const indent = try self.a.alloc(u8, depth * 2);
        @memset(indent, ' ');
        try self.line("{s}{s}{s}{s}", .{ indent, try self.text(get(value, "node_type")), if (get(value, "relation") != .null) " on " else "", if (get(value, "relation") != .null) try self.text(get(value, "relation")) else "" });
        if (get(value, "index") != .null) try self.line("{s}  Index: {s}", .{ indent, try self.text(get(value, "index")) });
        if (get(value, "estimates") == .object) {
            const estimate = get(value, "estimates");
            try self.line("{s}  Estimate: rows={s}, upper={s}, local work={s}", .{ indent, try self.text(get(estimate, "rows")), try self.text(get(estimate, "rows_upper")), try self.text(get(estimate, "local_work_units")) });
        }
        if (get(value, "fields") != .null) try self.line("{s}  Fields: {s}", .{ indent, try self.text(get(value, "fields")) });
        for (array(get(value, "plans"))) |child| try self.plan(child, depth + 1);
    }
    fn explain(self: *State, value: Value) !void {
        var root = get(value, "plan");
        const rows = array(get(value, "rows"));
        if (root == .null and rows.len == 1) {
            const cells = array(rows[0]);
            if (cells.len == 1 and cells[0] == .string) {
                const parsed = std.json.parseFromSlice(Value, self.a, cells[0].string, .{ .parse_numbers = false }) catch {
                    try self.line("{s}", .{try self.text(cells[0])});
                    return;
                };
                root = parsed.value;
            }
        }
        const runtime = get(root, "runtime");
        try self.line("{s}", .{if (runtime == .object) @as([]const u8, "EXPLAIN ANALYZE (executed read-only)") else "EXPLAIN (planning only)"});
        try self.plan(if (get(root, "Plan") != .null) get(root, "Plan") else root, 0);
        if (get(root, "cost_model") == .object) try self.line("Estimates use logical work units, not predicted time; null means unknown.", .{});
        if (runtime == .object) {
            try self.line("Runtime: {s} result rows; {s} ns; {s} peak retained bytes; {s} spill bytes written", .{ try self.text(get(runtime, "result_rows")), try self.text(get(runtime, "elapsed_ns")), try self.text(get(runtime, "retained_peak_bytes")), try self.text(get(runtime, "spill_written_bytes")) });
            try self.objects(array(get(runtime, "nodes")));
            try self.line("Operator times are inclusive and overlap.", .{});
        }
    }
    fn narrative(self: *State, raw: []const u8) !void {
        var lines = std.mem.splitScalar(u8, raw, '\n');
        while (lines.next()) |line_value| try self.line("{s}", .{try clean(self.a, line_value)});
    }
    fn retrieval(self: *State, value: Value) !void {
        self.exempt = false;
        try self.line("Status: {s}", .{try self.text(get(value, "status"))});
        if (get(value, "model") != .null) try self.line("Model: {s}", .{try self.text(get(value, "model"))});
        if (string(get(value, "generation"))) |answer| {
            try self.line("", .{});
            try self.narrative(answer);
        }
        if (get(value, "incomplete_details") != .null) {
            try self.line("Incomplete execution", .{});
            try self.object(get(value, "incomplete_details"), false);
        }
        if (array(get(value, "hits")).len > 0) {
            try self.line("Sources", .{});
            try self.hits(get(value, "hits"), true);
        }
        for (array(get(value, "followup_questions"))) |question| try self.line("- {s}", .{try self.text(question)});
        try self.controls(value);
    }
};

pub fn render(allocator: Allocator, policy: agent.Policy, context: agent.Context, input: anytype, options: Options) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const encoded = try std.json.Stringify.valueAlloc(a, input, .{});
    const parsed = try std.json.parseFromSlice(Value, a, encoded, .{ .parse_numbers = false });
    const value = parsed.value;
    var state = State{ .a = a, .out = .init(a), .limits = policy.limits, .columns = std.math.clamp(options.width, 40, 240), .rows_left = policy.limits.rows, .exempt = context.scope == .operation };
    if (context.execution == .partial) try state.line("PARTIAL EXECUTION: some requested work failed.", .{});
    switch (context.scope) {
        .sql => try state.sql(value),
        .search => try state.search(value),
        .lookup => try state.object(value, false),
        .plan => try state.explain(value),
        else => {
            const items = array(value);
            if (value == .array and items.len > 0 and get(items[0], "document_type") == .null and get(items[0], "columns") == .array and get(items[0], "indexes") == .array and get(items[0], "database") != .null) {
                try state.schema(items);
            } else if (value == .array) {
                if (context.scope == .operation) state.rows_left = items.len;
                try state.objects(items);
                if (context.scope == .operation) for (items, 0..) |item, i| try state.required(try std.fmt.allocPrint(a, "result[{d}]", .{i}), item);
            } else if (get(value, "hits") == .array and get(value, "status") != .null) {
                try state.retrieval(value);
            } else {
                try state.object(value, context.scope == .operation);
                if (context.scope == .operation) try state.controls(value);
            }
        },
    }
    if (context.scope == .workflow) {
        const controls = try agent.recoveryControls(a, .workflow, value);
        var fields = controls.iterator();
        while (fields.next()) |field| try state.required(field.key_ptr.*, field.value_ptr.*);
    }
    if (state.byte_limit) try state.emit(try std.fmt.allocPrint(a, "Display byte budget reached after {d} table row(s).\n", .{state.table_rows_shown}), true);
    if (state.clipped or state.omitted_rows or state.byte_limit) try state.emit("Display shortened; use --output json for full read values.\n", true);
    if (policy.fingerprint) |ordering| {
        const digest = try agent.fingerprint.make(a, ordering, @tagName(context.scope), context.execution == .complete, value);
        if (digest.digest) |hex| try state.emit(try std.fmt.allocPrint(a, "Fingerprint ({s}, {s}, {d} units): {s}\n", .{ digest.scope, @tagName(ordering), digest.units, hex }), true) else try state.emit("Fingerprint unavailable: execution or result scope is incomplete.\n", true);
    }
    return allocator.dupe(u8, state.out.written());
}

test "human SQL preserves exact integers and distinguishes SQL NULL from JSON null" {
    const a = std.testing.allocator;
    const input = .{ .columns = .{.{ .name = "value", .type = "json" }}, .rows = .{ .{@as(?bool, null)}, .{@as(?bool, null)}, .{"null"} }, .sql_nulls = .{ .{true}, .{false}, .{false} }, .transaction_id = "receipt" };
    const output = try render(a, .{}, .{ .scope = .sql, .execution = .complete }, input, .{});
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "json:null") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"null\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "transaction_id: receipt") != null);
    const exact = try render(a, .{}, .{ .scope = .sql }, .{ .columns = .{.{ .name = "n", .type = "integer" }}, .rows = .{.{"9007199254740993"}} }, .{});
    defer a.free(exact);
    try std.testing.expect(std.mem.indexOf(u8, exact, "9007199254740993") != null);
}

test "human tables bound rows and escape terminal injection" {
    const a = std.testing.allocator;
    const output = try render(a, .{ .limits = .{ .rows = 1 } }, .{ .scope = .lookup }, .{ .title = "hello\x1b[2J\nforged row", .body = "second" }, .{});
    defer a.free(output);
    try std.testing.expect(std.mem.indexOfScalar(u8, output, 0x1b) == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\\u001B") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "1 of 2 field(s)") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Display shortened") != null);
}

test "human grid handles wide unicode and clips without splitting UTF8" {
    try std.testing.expectEqual(@as(usize, 4), width("漢字"));
    try std.testing.expectEqual(@as(usize, 1), width("é"));
    const a = std.testing.allocator;
    const output = try render(a, .{}, .{ .scope = .discovery }, .{.{ .name = "漢字漢字漢字漢字漢字", .long_column = "long value long value long value long value" }}, .{ .width = 40 });
    defer a.free(output);
    try std.testing.expect(std.unicode.utf8ValidateSlice(output));
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line_value| if (std.mem.startsWith(u8, line_value, "|")) try std.testing.expect(width(line_value) <= 40);
}

test "human search shows projected fields and lower bounds without transport metadata" {
    const a = std.testing.allocator;
    const output = try render(a, .{}, .{ .scope = .search, .execution = .partial }, .{ .responses = .{.{ .table = "docs", .status = 503, .took = 12, .hits = .{ .total = .{ .value = 100, .relation = "gte" }, .hits = .{.{ ._id = "doc:1", ._score = 0.5, ._source = .{ .title = "One" }, .hierarchy = .{ .secret = "hidden implementation detail" } }} } }} }, .{});
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "title") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "doc:1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Matches >= 100") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "PARTIAL EXECUTION") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "hierarchy") == null);
}

test "human byte budgets still retain mutation receipts" {
    const a = std.testing.allocator;
    const receipt = try a.alloc(u8, 2000);
    defer a.free(receipt);
    @memset(receipt, 'r');
    const output = try render(a, .{ .limits = .{ .bytes = 1024, .cell_bytes = 8 } }, .{ .scope = .sql }, .{ .columns = .{.{ .name = "x", .type = "text" }}, .rows = .{.{"large value to truncate"}}, .transaction_id = receipt, .mutation_outcome = "committed_pending" }, .{});
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, receipt) != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "committed_pending") != null);
}

test "human retrieval prints answer and sources instead of raw protocol events" {
    const a = std.testing.allocator;
    const output = try render(a, .{}, .{}, .{ .status = "completed", .generation = "An answer.\nSecond line.", .hits = .{.{ ._id = "1", ._score = 1, ._source = .{ .title = "Source", .body = "large context not shown" } }} }, .{});
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "An answer.\nSecond line.") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Source") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "large context not shown") == null);
}

test "nested JSON cells escape bidi controls too" {
    const a = std.testing.allocator;
    const output = try render(a, .{}, .{ .scope = .lookup }, .{ .nested = .{ .title = "hello\u{202e}hidden" } }, .{});
    defer a.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "\u{202e}") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\\u202E") != null);
}
