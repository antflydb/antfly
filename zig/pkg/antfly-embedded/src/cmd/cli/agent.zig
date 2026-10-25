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

//! Transport-independent presentation contract. Execution facts are supplied
//! by command adapters, never inferred from the size of a displayed sample.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;

pub const fingerprint = @import("fingerprint.zig");
pub const Format = enum { auto, json, compact, table };
pub const Scope = enum { operation, sql, search, lookup, discovery, plan, workflow };
pub const Execution = enum { complete, partial, unknown };
pub const Limits = struct {
    rows: usize = 20,
    bytes: usize = 16 * 1024,
    cell_bytes: usize = 2048,
};
pub const Policy = struct {
    enabled: bool = false,
    fingerprint: ?fingerprint.Ordering = null,
    format: Format = .auto,
    limits: Limits = .{},

    pub fn enveloped(self: Policy) bool {
        return self.enabled and self.format == .auto;
    }
};
pub const Context = struct {
    scope: Scope = .operation,
    execution: Execution = .unknown,
    /// Survives plan display omission; ANALYZE executed a read, EXPLAIN did not.
    planning_only: ?bool = null,
};

pub fn activation(flag: ?bool, environment: ?[]const u8, recognized_agent: bool, stdout_is_tty: bool) !bool {
    if (flag) |value| return value;
    if (environment) |value| {
        if (std.mem.eql(u8, value, "1") or std.ascii.eqlIgnoreCase(value, "true")) return true;
        if (std.mem.eql(u8, value, "0") or std.ascii.eqlIgnoreCase(value, "false")) return false;
        return error.InvalidAgentMode;
    }
    return recognized_agent and !stdout_is_tty;
}

/// Only authoritative server diagnostics can attest stopped execution. A
/// timeout or lost response alone never proves whether a write committed.
pub fn failureStatus(code: []const u8, diagnostic: ?Value) []const u8 {
    if (diagnostic) |value| if (value == .object) if (value.object.get("code")) |state| {
        if (state == .string) {
            if (std.mem.eql(u8, state.string, "40003")) return "unconfirmed";
            if (std.mem.eql(u8, state.string, "57014") or std.mem.eql(u8, state.string, "54000")) return "stopped";
        }
    };
    if (std.mem.eql(u8, code, "PartialQueryExecution")) return "partial";
    return "unconfirmed";
}

fn size(value: anytype) !usize {
    var buffer: [256]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(value, .{}, &counter.writer);
    return std.math.cast(usize, counter.fullCount()) orelse error.OutputTooLarge;
}

fn countRows(value: Value, scope: Scope) usize {
    if (scope == .lookup) return 1;
    if (scope == .sql or scope == .plan) {
        if (value == .object) if (value.object.get("rows")) |rows| {
            if (rows == .array) return rows.array.items.len;
        };
    }
    if (scope == .search) {
        var count: usize = 0;
        if (value == .object) if (value.object.get("responses")) |responses| {
            if (responses == .array) for (responses.array.items) |response| {
                if (response != .object) continue;
                const hits = response.object.get("hits") orelse continue;
                if (hits != .object) continue;
                const rows = hits.object.get("hits") orelse continue;
                if (rows == .array) count += rows.array.items.len;
            };
        };
        return count;
    }
    return if (value == .array) value.array.items.len else 1;
}

// Whole display units are omitted. Retained values, including exact integers,
// JSON nulls, SQL null masks and JSON-column objects, are never rewritten.
fn fitsCells(value: Value, limit: usize) !bool {
    if (value == .array) {
        for (value.array.items) |cell| if (try size(cell) > limit) return false;
        return true;
    }
    if (value == .object) {
        var fields = value.object.iterator();
        while (fields.next()) |entry| if (try size(entry.value_ptr.*) > limit) return false;
        return true;
    }
    return try size(value) <= limit;
}

const Sample = struct {
    remaining_rows: usize,
    displayed: usize = 0,
    omitted: bool = false,
    limits: Limits,

    fn rows(self: *Sample, array: *Value) !usize {
        if (array.* != .array) return 0;
        const all = array.array.items;
        var kept: usize = 0;
        var used_bytes: usize = 0;
        for (all) |row| {
            const row_bytes = try size(row);
            if (self.remaining_rows == 0 or !try fitsCells(row, self.limits.cell_bytes) or
                row_bytes > self.limits.bytes -| used_bytes) break;
            used_bytes += row_bytes;
            kept += 1;
            self.remaining_rows -= 1;
        }
        self.displayed += kept;
        self.omitted = self.omitted or kept < all.len;
        array.array.items = all[0..kept];
        return kept;
    }
};

fn protected(key: []const u8) bool {
    for ([_][]const u8{
        "transaction_id",   "transaction_status", "session_id", "connection_id",
        "mutation_outcome", "ddl_receipt",        "receipt",    "receipts",
        "command_tag",      "rows_affected",
    }) |name| if (std.mem.eql(u8, key, name)) return true;
    return false;
}

/// Only command-declared response shapes may supply recovery controls. User
/// documents and search hits must never be interpreted as execution receipts.
pub fn recoveryControls(allocator: Allocator, scope: Scope, value: Value) !std.json.ObjectMap {
    var controls: std.json.ObjectMap = .empty;
    if (value != .object) return controls;
    if (scope == .sql or scope == .plan or scope == .operation or scope == .workflow) {
        var fields = value.object.iterator();
        while (fields.next()) |field| {
            const key = field.key_ptr.*;
            const workflow_control = scope == .workflow and
                (std.mem.eql(u8, key, "job_id") or std.mem.eql(u8, key, "state") or
                    std.mem.eql(u8, key, "status") or std.mem.eql(u8, key, "phase") or
                    std.mem.eql(u8, key, "advances"));
            if (protected(key) or workflow_control) try controls.put(allocator, key, field.value_ptr.*);
        }
    }
    return controls;
}

// Remove only a trailing display unit; SQL NULL masks stay ordinally aligned.
fn trimLast(data: *Value, scope: Scope) bool {
    if (data.* == .array and data.array.items.len > 0) {
        data.array.items.len -= 1;
        return true;
    }
    if (data.* != .object) return false;
    if (scope == .sql or scope == .plan) {
        const rows = data.object.getPtr("rows") orelse return false;
        if (rows.* != .array or rows.array.items.len == 0) return false;
        rows.array.items.len -= 1;
        if (data.object.getPtr("sql_nulls")) |mask| {
            if (mask.* == .array) mask.array.items.len = @min(mask.array.items.len, rows.array.items.len);
        }
        return true;
    }
    if (scope == .workflow) {
        var i = data.object.count();
        while (i > 0) {
            i -= 1;
            const field = &data.object.values()[i];
            if (field.* == .array and field.array.items.len > 0) {
                field.array.items.len -= 1;
                return true;
            }
        }
        if (data.object.count() > 0) {
            const key = data.object.keys()[data.object.count() - 1];
            _ = data.object.orderedRemove(key);
            return true;
        }
    }
    if (scope == .search) {
        const responses = data.object.getPtr("responses") orelse return false;
        if (responses.* != .array) return false;
        var i = responses.array.items.len;
        while (i > 0) {
            i -= 1;
            const response = &responses.array.items[i];
            if (response.* != .object) continue;
            const hits = response.object.getPtr("hits") orelse continue;
            if (hits.* != .object) continue;
            const rows = hits.object.getPtr("hits") orelse continue;
            if (rows.* != .array or rows.array.items.len == 0) continue;
            rows.array.items.len -= 1;
            return true;
        }
    }
    return false;
}

pub fn encode(allocator: Allocator, policy: Policy, context: Context, input: anytype) ![]u8 {
    if (!policy.enveloped()) return std.json.Stringify.valueAlloc(allocator, input, .{
        .whitespace = if (policy.format == .compact) .minified else .indent_2,
    });
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try std.json.Stringify.valueAlloc(a, input, .{});
    const parsed = try std.json.parseFromSlice(Value, a, raw, .{ .parse_numbers = false });
    const result_fingerprint = if (policy.fingerprint) |ordering| try fingerprint.make(a, ordering, @tagName(context.scope), context.execution == .complete, parsed.value) else null;
    var data = parsed.value;
    const returned: ?usize = if (context.scope == .operation or context.scope == .workflow) null else countRows(data, context.scope);
    const controls = try recoveryControls(a, context.scope, data);
    if (data == .object) for (controls.keys()) |key| {
        _ = data.object.orderedRemove(key);
    };
    const exempt = context.scope == .operation;
    var sample = Sample{ .remaining_rows = policy.limits.rows, .limits = policy.limits };
    if (exempt) {
        sample.displayed = returned orelse 0;
    } else switch (context.scope) {
        .plan => sample.displayed = returned orelse 0,
        .workflow => if (data == .object) {
            // Answers and source collections are independent display units.
            // Drop whole oversized values; never truncate JSON or citations.
            var i: usize = 0;
            while (i < data.object.count()) {
                const field = &data.object.values()[i];
                if (field.* == .array) {
                    _ = try sample.rows(field);
                    i += 1;
                } else if (try size(field.*) > policy.limits.cell_bytes) {
                    const key = data.object.keys()[i];
                    _ = data.object.orderedRemove(key);
                    sample.omitted = true;
                } else i += 1;
            }
        },
        .sql => if (data == .object) {
            if (data.object.getPtr("rows")) |rows| {
                const kept = try sample.rows(rows);
                if (data.object.getPtr("sql_nulls")) |mask| {
                    if (mask.* == .array) mask.array.items = mask.array.items[0..@min(kept, mask.array.items.len)];
                }
            }
        },
        .search => if (data == .object) {
            if (data.object.getPtr("responses")) |responses| {
                if (responses.* == .array) for (responses.array.items) |*response| {
                    if (response.* != .object) continue;
                    const hits = response.object.getPtr("hits") orelse continue;
                    if (hits.* != .object) continue;
                    const rows = hits.object.getPtr("hits") orelse continue;
                    _ = try sample.rows(rows);
                };
            }
        },
        else => {
            if (data == .array) {
                _ = try sample.rows(&data);
            } else if (!try fitsCells(data, policy.limits.cell_bytes)) {
                data = .null;
                sample.omitted = true;
            } else sample.displayed = returned orelse 0;
        },
    }
    var execution: std.json.ObjectMap = .empty;
    try execution.put(a, "status", .{ .string = @tagName(context.execution) });
    try execution.put(a, "scope", .{ .string = @tagName(context.scope) });
    try execution.put(a, "result_scope", .{ .string = if (context.scope == .search) "requested_top_k" else @tagName(context.scope) });
    if (context.planning_only) |planning| try execution.put(a, "planning_only", .{ .bool = planning });
    var envelope = .{
        .agent_contract = @as(u32, 1),
        .execution = Value{ .object = execution },
        .display = .{
            .omitted = sample.omitted,
            .returned_rows = returned,
            .displayed_rows = if (returned != null) @as(?usize, sample.displayed) else null,
            .limits = policy.limits,
            .budget_exempt = exempt,
            .retrieve = if (sample.omitted) @as(?[]const u8, "For read-only commands use --output json or narrow the projection. Never replay mutations to retrieve omitted output; reconcile control receipts and issue a separate read.") else null,
        },
        .fingerprint = result_fingerprint,
        .control = Value{ .object = controls },
        .data = data,
    };
    // Count the exact envelope, including omission guidance and the newline.
    // Only irreducible metadata/controls may exceed the presentation ceiling.
    while (!exempt and try size(envelope) + 1 > policy.limits.bytes and data != .null) {
        sample.omitted = true;
        if (trimLast(&data, context.scope)) {
            sample.displayed -|= 1;
        } else {
            data = .null;
            sample.displayed = 0;
        }
        envelope.data = data;
        envelope.display.omitted = true;
        envelope.display.displayed_rows = if (returned != null) sample.displayed else null;
        envelope.display.retrieve = "For read-only commands use --output json or narrow the projection. Never replay mutations to retrieve omitted output; reconcile control receipts and issue a separate read.";
    }
    envelope.display.budget_exempt = exempt or try size(envelope) + 1 > policy.limits.bytes;
    return std.json.Stringify.valueAlloc(allocator, envelope, .{});
}

test "agent activation precedence and opt out" {
    try std.testing.expect(try activation(true, "false", false, true));
    try std.testing.expect(!try activation(false, "true", true, false));
    try std.testing.expect(!try activation(null, "false", true, false));
    try std.testing.expect(try activation(null, null, true, false));
    try std.testing.expect(!try activation(null, null, true, true));
    try std.testing.expectError(error.InvalidAgentMode, activation(null, "maybe", true, false));
}

test "agent budgets preserve SQL values null alignment and receipts" {
    const a = std.testing.allocator;
    const input = .{
        .columns = .{.{ .name = "value", .type = "json" }},
        .rows = .{ .{"9007199254740993"}, .{@as(?bool, null)}, .{"large cell that cannot fit"} },
        .sql_nulls = .{ .{false}, .{false}, .{false} },
        .transaction_id = "receipt-must-survive",
        .mutation_outcome = "committed_pending",
    };
    const encoded = try encode(a, .{ .enabled = true, .limits = .{ .cell_bytes = 24 } }, .{ .scope = .sql, .execution = .complete }, input);
    defer a.free(encoded);
    var result = try std.json.parseFromSlice(Value, a, encoded, .{});
    defer result.deinit();
    const data = result.value.object.get("data").?.object;
    try std.testing.expectEqual(@as(usize, 2), data.get("rows").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 2), data.get("sql_nulls").?.array.items.len);
    try std.testing.expectEqualStrings("9007199254740993", data.get("rows").?.array.items[0].array.items[0].string);
    try std.testing.expect(data.get("rows").?.array.items[1].array.items[0] == .null);
    try std.testing.expect(result.value.object.get("display").?.object.get("omitted").?.bool);
    try std.testing.expectEqualStrings("receipt-must-survive", result.value.object.get("control").?.object.get("transaction_id").?.string);
}

test "agent byte ceiling includes oversized keys and nested documents" {
    const a = std.testing.allocator;
    const huge = try a.alloc(u8, 20000);
    defer a.free(huge);
    @memset(huge, 'x');
    for ([_]Scope{ .lookup, .discovery, .search, .sql }) |scope| {
        const encoded = try encode(a, .{ .enabled = true, .limits = .{ .bytes = 1024 } }, .{ .scope = scope }, .{ .document = huge });
        defer a.free(encoded);
        try std.testing.expect(encoded.len <= 1024);
        var parsed = try std.json.parseFromSlice(Value, a, encoded, .{});
        defer parsed.deinit();
        try std.testing.expect(parsed.value.object.get("display").?.object.get("omitted").?.bool);
    }
}

test "agent explicit formats preserve the original result contract" {
    const a = std.testing.allocator;
    const encoded = try encode(a, .{ .enabled = true, .format = .compact }, .{}, .{ .value = @as(?bool, null) });
    defer a.free(encoded);
    try std.testing.expectEqualStrings("{\"value\":null}", encoded);
}

test "agent search counts received hits without claiming exhaustive matching" {
    const a = std.testing.allocator;
    const input = .{ .responses = .{.{ .status = 200, .took = 1, .hits = .{ .total = .{ .value = 100, .relation = "gte" }, .hits = .{ .{ .id = "one" }, .{ .id = "two" } } } }} };
    const encoded = try encode(a, .{ .enabled = true, .limits = .{ .rows = 1 } }, .{ .scope = .search, .execution = .partial }, input);
    defer a.free(encoded);
    var result = try std.json.parseFromSlice(Value, a, encoded, .{});
    defer result.deinit();
    const root = result.value.object;
    try std.testing.expectEqualStrings("partial", root.get("execution").?.object.get("status").?.string);
    try std.testing.expectEqualStrings("requested_top_k", root.get("execution").?.object.get("result_scope").?.string);
    try std.testing.expectEqual(@as(i64, 2), root.get("display").?.object.get("returned_rows").?.integer);
    try std.testing.expectEqual(@as(i64, 1), root.get("display").?.object.get("displayed_rows").?.integer);
    const hits = root.get("data").?.object.get("responses").?.array.items[0].object.get("hits").?.object;
    try std.testing.expectEqualStrings("gte", hits.get("total").?.object.get("relation").?.string);
}

test "agent opaque operations exempt outcomes and do not invent row totals" {
    const a = std.testing.allocator;
    const receipt = try a.alloc(u8, 3000);
    defer a.free(receipt);
    @memset(receipt, 'x');
    const encoded = try encode(a, .{ .enabled = true, .limits = .{ .bytes = 1024, .cell_bytes = 1 } }, .{}, .{ .outcome = receipt });
    defer a.free(encoded);
    var result = try std.json.parseFromSlice(Value, a, encoded, .{});
    defer result.deinit();
    const display = result.value.object.get("display").?.object;
    try std.testing.expect(display.get("budget_exempt").?.bool);
    try std.testing.expect(display.get("returned_rows").? == .null);
    try std.testing.expectEqualStrings(receipt, result.value.object.get("data").?.object.get("outcome").?.string);
}

test "agent lookup never treats document fields as transaction controls" {
    const a = std.testing.allocator;
    const encoded = try encode(a, .{ .enabled = true }, .{ .scope = .lookup, .execution = .complete }, .{ .transaction_id = "business-field", .rows_affected = 42 });
    defer a.free(encoded);
    var result = try std.json.parseFromSlice(Value, a, encoded, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("business-field", result.value.object.get("data").?.object.get("transaction_id").?.string);
    try std.testing.expectEqual(@as(usize, 0), result.value.object.get("control").?.object.count());
}

test "agent stopped execution requires an authoritative diagnostic" {
    const a = std.testing.allocator;
    var stopped = try std.json.parseFromSlice(Value, a, "{\"code\":\"57014\"}", .{});
    defer stopped.deinit();
    var uncertain = try std.json.parseFromSlice(Value, a, "{\"code\":\"40003\"}", .{});
    defer uncertain.deinit();
    try std.testing.expectEqualStrings("stopped", failureStatus("HttpFailure", stopped.value));
    try std.testing.expectEqualStrings("unconfirmed", failureStatus("HttpFailure", uncertain.value));
    try std.testing.expectEqualStrings("unconfirmed", failureStatus("Timeout", null));
    // Error spelling is not evidence of rejection after an operation starts.
    try std.testing.expectEqualStrings("unconfirmed", failureStatus("InvalidApiResponse", null));
    try std.testing.expectEqualStrings("unconfirmed", failureStatus("UnknownTransactionOutcome", null));
}

test "agent fingerprint covers received rows before sampling and stays bounded" {
    const a = std.testing.allocator;
    const input = .{ .columns = .{.{ .name = "n", .type = "integer" }}, .rows = .{ .{"1"}, .{"2"} } };
    const output = try encode(a, .{ .enabled = true, .fingerprint = .ordered, .limits = .{ .rows = 1, .bytes = 1024 } }, .{ .scope = .sql, .execution = .complete }, input);
    defer a.free(output);
    var parsed = try std.json.parseFromSlice(Value, a, output, .{});
    defer parsed.deinit();
    try std.testing.expect(output.len + 1 <= 1024);
    const record = parsed.value.object.get("fingerprint").?.object;
    try std.testing.expectEqual(@as(i64, 2), record.get("units").?.integer);
    try std.testing.expectEqualStrings("complete_sql_result", record.get("scope").?.string);
    try std.testing.expect(record.get("digest").? == .string);
}

test "plan execution fact survives an omitted plan" {
    const a = std.testing.allocator;
    const huge = try a.alloc(u8, 20000);
    defer a.free(huge);
    @memset(huge, 'p');
    const output = try encode(a, .{ .enabled = true, .limits = .{ .bytes = 1024 } }, .{ .scope = .plan, .execution = .complete, .planning_only = false }, .{ .plan = huge, .planning_only = false });
    defer a.free(output);
    var parsed = try std.json.parseFromSlice(Value, a, output, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("data").? == .null);
    try std.testing.expect(!parsed.value.object.get("execution").?.object.get("planning_only").?.bool);
    try std.testing.expect(output.len + 1 <= 1024);
}

test "workflow budgets answers and sources independently and preserves lifecycle controls" {
    const a = std.testing.allocator;
    const huge = try a.alloc(u8, 50000);
    defer a.free(huge);
    @memset(huge, 'x');
    const output = try encode(a, .{ .enabled = true, .fingerprint = .ordered, .limits = .{ .bytes = 4096, .rows = 1 } }, .{ .scope = .workflow, .execution = .complete }, .{
        .status = "completed",
        .job_id = "job:1",
        .session_id = "session:1",
        .generation = "A short answer.",
        .hits = .{ .{ ._id = "doc:1", ._source = .{ .body = "small", .transaction_id = "user-data" } }, .{ ._id = "doc:2", ._source = .{ .body = huge } } },
        .context = huge,
    });
    defer a.free(output);
    var parsed = try std.json.parseFromSlice(Value, a, output, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    const display = root.get("display").?.object;
    const data = root.get("data").?.object;
    try std.testing.expect(output.len + 1 <= 4096);
    try std.testing.expect(display.get("omitted").?.bool);
    try std.testing.expect(!display.get("budget_exempt").?.bool);
    try std.testing.expect(display.get("returned_rows").? == .null);
    try std.testing.expectEqualStrings("A short answer.", data.get("generation").?.string);
    try std.testing.expectEqual(@as(usize, 1), data.get("hits").?.array.items.len);
    try std.testing.expect(data.get("context") == null);
    const controls = root.get("control").?.object;
    try std.testing.expectEqualStrings("job:1", controls.get("job_id").?.string);
    try std.testing.expectEqualStrings("session:1", controls.get("session_id").?.string);
    try std.testing.expect(controls.get("transaction_id") == null);
    try std.testing.expect(root.get("fingerprint").?.object.get("digest").? == .null);
}
