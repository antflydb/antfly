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
