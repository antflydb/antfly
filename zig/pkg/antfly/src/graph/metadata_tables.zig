// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Root-only endpoint routing tags. Unescaped names borrow the edge's JSON;
//! escaped names live in query scratch. Unrelated values are scanned, never
//! materialized. Scratch and scanner allocations share the graph work budget.
const std = @import("std");
const work = @import("work_budget.zig");

pub const Scratch = struct {
    memory: work.RetainedAllocator,
    strings: std.ArrayListUnmanaged([]u8) = .empty,

    pub fn init(alloc: std.mem.Allocator, budget: ?*work.WorkBudget) Scratch {
        return .{ .memory = .{ .backing = alloc, .budget = budget } };
    }

    pub fn deinit(self: *Scratch) void {
        const alloc = self.memory.allocator();
        for (self.strings.items) |value| alloc.free(value);
        self.strings.deinit(alloc);
    }

    pub fn table(self: *Scratch, raw: []const u8, field: []const u8) !?[]const u8 {
        if (raw.len == 0) return null;
        return self.parse(raw, field) catch |err| switch (err) {
            error.OutOfMemory => if (self.memory.denied) error.GraphWorkBudgetExceeded else err,
            // Malformed metadata has no authority to change node identity.
            else => null,
        };
    }

    fn parse(self: *Scratch, raw: []const u8, field: []const u8) !?[]const u8 {
        const alloc = self.memory.allocator();
        var stack = std.heap.stackFallback(512, alloc);
        const scanning = stack.get();
        var scanner = std.json.Scanner.initCompleteInput(scanning, raw);
        defer scanner.deinit();
        if (try scanner.next() != .object_begin) return null;
        var result: ?[]const u8 = null;
        var owned: ?[]u8 = null;
        defer if (owned) |value| alloc.free(value);
        var seen = false;
        while (try scanner.peekNextTokenType() != .object_end) {
            const token = try scanner.nextAllocMax(scanning, .alloc_if_needed, raw.len);
            const key = switch (token) {
                .string, .allocated_string => |value| value,
                else => return error.InvalidMetadata,
            };
            defer if (token == .allocated_string) scanning.free(token.allocated_string);
            if (!std.mem.eql(u8, key, field)) {
                try scanner.skipValue();
                continue;
            }
            if (seen) return error.DuplicateField;
            seen = true;
            if (try scanner.peekNextTokenType() != .string) return error.InvalidMetadata;
            const value = try scanner.nextAllocMax(scanning, .alloc_if_needed, raw.len);
            switch (value) {
                .string => |text| result = text,
                .allocated_string => |text| {
                    defer scanning.free(text);
                    owned = try alloc.dupe(u8, text);
                    result = owned.?;
                },
                else => return error.InvalidMetadata,
            }
            if (result.?.len == 0) return error.InvalidMetadata;
            for (result.?) |byte| if (std.ascii.isControl(byte)) return error.InvalidMetadata;
        }
        _ = try scanner.next();
        if (try scanner.next() != .end_of_document) return error.InvalidMetadata;
        if (owned) |text| {
            try self.strings.append(alloc, text);
            owned = null;
        }
        return result;
    }
};

test "graph metadata table routing uses only decoded unique root fields" {
    var scratch = Scratch.init(std.testing.allocator, null);
    defer scratch.deinit();
    try std.testing.expect((try scratch.table("{\"evidence\":{\"source_table\":\"wrong\"}}", "source_table")) == null);
    try std.testing.expectEqualStrings("entities", (try scratch.table("{\"source_table\" : \"entities\",\"evidence\":[{\"source_table\":\"wrong\"}]}", "source_table")).?);
    try std.testing.expectEqualStrings("entities", (try scratch.table("{\"source_\\u0074able\":\"\\u0065ntities\"}", "source_table")).?);
    try std.testing.expectEqualStrings("a\"b\\c", (try scratch.table("{\"target_table\":\"a\\\"b\\\\c\"}", "target_table")).?);
    for ([_][]const u8{ "{\"source_table\":\"a\",\"source_table\":\"b\"}", "{\"source_table\":\"a\",\"source_\\u0074able\":\"b\"}", "{\"source_table\":\"a\"} trailing", "{\"source_table\":[]}", "{\"source_table\":\"\"}", "{\"source_table\":\"a\",\"bad\":[}" }) |raw| {
        try std.testing.expect((try scratch.table(raw, "source_table")) == null);
    }
}

test "graph metadata table routing bounds decoded scratch" {
    var budget = work.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = 1 });
    var scratch = Scratch.init(std.testing.allocator, &budget);
    defer scratch.deinit();
    try std.testing.expectError(error.GraphWorkBudgetExceeded, scratch.table("{\"source_table\":\"\\u0065ntities\"}", "source_table"));
}

test "graph metadata table routing borrows plain tags without heap allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var scratch = Scratch.init(failing.allocator(), null);
    defer scratch.deinit();
    const raw = "{\"unused\":[{\"target_table\":\"wrong\",\"array\":[1,2,3]}],\"target_table\" : \"entities\"}";
    const table = (try scratch.table(raw, "target_table")).?;
    try std.testing.expectEqualStrings("entities", table);
    try std.testing.expect(@intFromPtr(table.ptr) >= @intFromPtr(raw.ptr) and @intFromPtr(table.ptr) < @intFromPtr(raw.ptr) + raw.len);
    const unused = try std.testing.allocator.alloc(u8, 512 * 1024);
    defer std.testing.allocator.free(unused);
    @memset(unused, 'x');
    const large = try std.fmt.allocPrint(std.testing.allocator, "{{\"unused\":\"{s}\",\"target_table\":\"entities\"}}", .{unused});
    defer std.testing.allocator.free(large);
    try std.testing.expectEqualStrings("entities", (try scratch.table(large, "target_table")).?);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

fn exerciseRoutingAllocations(alloc: std.mem.Allocator) !void {
    var scratch = Scratch.init(alloc, null);
    defer scratch.deinit();
    try std.testing.expectEqualStrings("entities", (try scratch.table("{\"source_\\u0074able\":\"\\u0065ntities\"}", "source_table")).?);
}

test "graph metadata table routing frees partial allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseRoutingAllocations, .{});
}

const graph = @import("graph.zig");

pub const Endpoint = struct {
    connected: bool = true,
    key: []const u8,
    table: ?[]const u8,
    direction: ?graph.EdgeDirection,
};

/// Resolve orientation before selecting an endpoint tag. Equal keys are not
/// self-loops when their table namespaces differ. Missing tags refer to the
/// physical index table; callers canonicalize the result in their namespace.
pub fn adjacent(scratch: *Scratch, edge: anytype, current_key: []const u8, current_table: ?[]const u8, index_table: ?[]const u8, requested: graph.EdgeDirection) !Endpoint {
    const source_table = (try scratch.table(edge.metadata, "source_table")) orelse index_table;
    const target_table = (try scratch.table(edge.metadata, "target_table")) orelse index_table;
    const here = current_table orelse index_table;
    // Legacy callers without a table context retain their key-only contract.
    // Qualified callers must match the departing endpoint, not just its key.
    const at_source = std.mem.eql(u8, current_key, edge.source) and (here == null or optionalTableEql(here, source_table));
    const at_target = std.mem.eql(u8, current_key, edge.target) and (here == null or optionalTableEql(here, target_table));
    const connected = switch (requested) {
        .out => at_source,
        .in => at_target,
        .both => at_source or at_target,
    };
    const forward = switch (requested) {
        .out => true,
        .in => false,
        .both => at_source,
    };
    const direction: ?graph.EdgeDirection = if (requested == .both and at_source and at_target) null else if (forward) .out else .in;
    return .{
        .connected = connected,
        .key = if (forward) edge.target else edge.source,
        .table = if (forward) target_table else source_table,
        .direction = direction,
    };
}

fn optionalTableEql(a: ?[]const u8, b: ?[]const u8) bool {
    if (a) |left| return if (b) |right| std.mem.eql(u8, left, right) else false;
    return b == null;
}

/// Custom templates may supply a unique valid root tag. Default item metadata
/// uses resolved routing instead. Copy other member spans verbatim, retaining
/// exact numeric literals and avoiding a materialized JSON tree.
pub fn withTableAlloc(alloc: std.mem.Allocator, tag: []const u8, table_name: []const u8, raw: []const u8, preserve_explicit: bool) ![]u8 {
    var scanner = std.json.Scanner.initCompleteInput(alloc, raw);
    defer scanner.deinit();
    if (try scanner.next() != .object_begin) return alloc.dupe(u8, raw);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var members: usize = 0;
    var explicit = false;
    while (try scanner.peekNextTokenType() != .object_end) {
        const start = scanner.cursor;
        const token = try scanner.nextAllocMax(alloc, .alloc_if_needed, raw.len);
        const key = switch (token) {
            .string, .allocated_string => |value| value,
            else => return error.InvalidMetadata,
        };
        defer if (token == .allocated_string) alloc.free(token.allocated_string);
        const selected = std.mem.eql(u8, key, tag);
        if (selected and preserve_explicit) {
            if (explicit) return error.DuplicateField;
            if (try scanner.peekNextTokenType() != .string) return error.InvalidMetadata;
            const value = try scanner.nextAllocMax(alloc, .alloc_if_needed, raw.len);
            const text = switch (value) {
                .string, .allocated_string => |v| v,
                else => return error.InvalidMetadata,
            };
            defer if (value == .allocated_string) alloc.free(value.allocated_string);
            if (text.len == 0) return error.InvalidMetadata;
            for (text) |byte| if (std.ascii.isControl(byte)) return error.InvalidMetadata;
            explicit = true;
        } else try scanner.skipValue();
        if (!selected or preserve_explicit) {
            if (members > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, raw[start..scanner.cursor]);
            members += 1;
        }
    }
    _ = try scanner.next();
    if (try scanner.next() != .end_of_document) return error.InvalidMetadata;
    if (!explicit) {
        if (members > 0) try out.append(alloc, ',');
        const quoted_tag = try std.json.Stringify.valueAlloc(alloc, tag, .{});
        defer alloc.free(quoted_tag);
        const quoted_table = try std.json.Stringify.valueAlloc(alloc, table_name, .{});
        defer alloc.free(quoted_table);
        try out.appendSlice(alloc, quoted_tag);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, quoted_table);
    }
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

test "graph metadata table writers preserve root authority and exact unrelated values" {
    const alloc = std.testing.allocator;
    var scratch = Scratch.init(alloc, null);
    defer scratch.deinit();
    const nested = try withTableAlloc(alloc, "source_table", "people", "  { \"evidence\":{\"source_table\":\"wrong\"}, \"n\":1.00000000000000000000001 }  ", true);
    defer alloc.free(nested);
    try std.testing.expectEqualStrings("people", (try scratch.table(nested, "source_table")).?);
    try std.testing.expect(std.mem.indexOf(u8, nested, "1.00000000000000000000001") != null);
    const explicit = try withTableAlloc(alloc, "target_table", "resolved", "{\"target_\\u0074able\" : \"custom\",\"n\":1}", true);
    defer alloc.free(explicit);
    try std.testing.expectEqualStrings("custom", (try scratch.table(explicit, "target_table")).?);
    const replaced = try withTableAlloc(alloc, "target_table", "companies", "{\"target_table\":\"wrong\",\"target_\\u0074able\":null,\"evidence\":{\"target_table\":\"nested\"}}", false);
    defer alloc.free(replaced);
    try std.testing.expectEqualStrings("companies", (try scratch.table(replaced, "target_table")).?);
    for ([_][]const u8{ "{\"target_table\":null}", "{\"target_table\":\"\"}", "{\"target_table\":\"a\",\"target_table\":\"b\"}", "{\"n\":1} trailing" }) |raw| {
        if (withTableAlloc(alloc, "target_table", "companies", raw, true)) |unexpected| {
            alloc.free(unexpected);
            return error.TestExpectedError;
        } else |err| try std.testing.expect(err == error.InvalidMetadata or err == error.DuplicateField or err == error.SyntaxError);
    }
    const empty = try withTableAlloc(alloc, "source_table", "people", " { } ", true);
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("people", (try scratch.table(empty, "source_table")).?);
}

fn exerciseWriterAllocations(alloc: std.mem.Allocator) !void {
    const raw = try withTableAlloc(alloc, "source_table", "people", "{\"source_\\u0074able\":\"\\u0070eople\",\"evidence\":[1,2]}", true);
    defer alloc.free(raw);
}

test "graph metadata table writers release partial allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseWriterAllocations, .{});
}

test "graph qualified endpoint orientation distinguishes equal keys from self loops" {
    var scratch = Scratch.init(std.testing.allocator, null);
    defer scratch.deinit();
    const edge = .{ .source = "same", .target = "same", .metadata = "{\"source_table\":\"people\",\"target_table\":\"companies\"}" };
    for ([_]graph.EdgeDirection{ .in, .both }) |direction| {
        const endpoint = try adjacent(&scratch, edge, "same", null, "companies", direction);
        try std.testing.expectEqualStrings("people", endpoint.table.?);
        try std.testing.expectEqual(graph.EdgeDirection.in, endpoint.direction.?);
    }
    for ([_]graph.EdgeDirection{ .out, .both }) |direction| {
        const endpoint = try adjacent(&scratch, edge, "same", null, "people", direction);
        try std.testing.expectEqualStrings("companies", endpoint.table.?);
        try std.testing.expectEqual(graph.EdgeDirection.out, endpoint.direction.?);
    }
    const self = try adjacent(&scratch, .{ .source = "same", .target = "same", .metadata = "{}" }, "same", null, "people", .both);
    try std.testing.expect(self.direction == null);
    try std.testing.expectEqualStrings("people", self.table.?);
}

test "qualified endpoint eligibility checks both keys and departing tables" {
    var scratch = Scratch.init(std.testing.allocator, null);
    defer scratch.deinit();
    const edge = .{ .source = "a", .target = "b", .metadata = "{\"source_table\":\"people\",\"target_table\":\"events\"}" };
    for ([_]graph.EdgeDirection{ .out, .both }) |direction| {
        try std.testing.expect(!(try adjacent(&scratch, edge, "a", "companies", "facts", direction)).connected);
        try std.testing.expect((try adjacent(&scratch, edge, "a", "people", "facts", direction)).connected);
    }
    for ([_]graph.EdgeDirection{ .in, .both }) |direction| {
        try std.testing.expect(!(try adjacent(&scratch, edge, "b", "companies", "facts", direction)).connected);
        try std.testing.expect((try adjacent(&scratch, edge, "b", "events", "facts", direction)).connected);
    }
    try std.testing.expect(!(try adjacent(&scratch, .{ .source = "same", .target = "same", .metadata = edge.metadata }, "same", "companies", "facts", .both)).connected);
}
