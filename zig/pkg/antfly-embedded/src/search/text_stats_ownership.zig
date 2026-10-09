// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const stats = @import("distributed_stats.zig");
const ownership = @This();

/// Consume the owned map key. Publish entries and term keys only after all
/// allocations succeed, so failure never leaves borrowed or undefined keys.
pub fn appendTerm(comptime Entry: type, alloc: std.mem.Allocator, map: anytype, key: []const u8, index_name: ?[]const u8, field: []const u8, term: []const u8) !void {
    var key_owned = true;
    defer if (key_owned) alloc.free(key);
    if (map.getPtr(key)) |entry| {
        if (entry.terms.contains(term)) return;
        const owned_term = try alloc.dupe(u8, term);
        errdefer alloc.free(owned_term);
        try entry.terms.putNoClobber(alloc, owned_term, {});
        return;
    }
    var entry: Entry = .{ .field = try alloc.dupe(u8, field), .index_name = index_name };
    errdefer {
        var it = entry.terms.keyIterator();
        while (it.next()) |owned_term| alloc.free(owned_term.*);
        entry.terms.deinit(alloc);
        alloc.free(entry.field);
    }
    const owned_term = try alloc.dupe(u8, term);
    entry.terms.putNoClobber(alloc, owned_term, {}) catch |err| {
        alloc.free(owned_term);
        return err;
    };
    try map.putNoClobber(alloc, key, entry);
    key_owned = false;
}

/// Requests borrow fields and term bytes from the immutable map. Only the
/// arrays are owned here; partial construction retires exactly its prefix.
pub fn requestsFromMap(comptime Request: type, alloc: std.mem.Allocator, map: anytype) ![]Request {
    const requests = try alloc.alloc(Request, map.count());
    var initialized: usize = 0;
    errdefer freeRequests(alloc, requests, initialized);
    var it = map.iterator();
    while (it.next()) |entry| {
        const terms = try alloc.alloc([]const u8, entry.value_ptr.terms.count());
        var term_it = entry.value_ptr.terms.keyIterator();
        var i: usize = 0;
        while (term_it.next()) |term| : (i += 1) terms[i] = term.*;
        requests[initialized] = .{ .index_name = entry.value_ptr.index_name, .field = entry.value_ptr.field, .terms = terms };
        initialized += 1;
    }
    return requests;
}

pub fn freeRequests(alloc: std.mem.Allocator, requests: anytype, initialized: usize) void {
    for (requests[0..initialized]) |request| alloc.free(request.terms);
    alloc.free(requests);
}

/// Copy both result labels before publishing either. The caller takes both
/// allocations together; failure leaves no partially owned result.
pub fn backgroundLabels(alloc: std.mem.Allocator, aggregation_name: []const u8, field: []const u8) !struct { aggregation_name: []const u8, field: []const u8 } {
    const owned_name = try alloc.dupe(u8, aggregation_name);
    errdefer alloc.free(owned_name);
    return .{ .aggregation_name = owned_name, .field = try alloc.dupe(u8, field) };
}

pub fn termDocFreq(alloc: std.mem.Allocator, source: anytype, field: []const u8, term: []const u8) !stats.TermDocFreq {
    const owned_term = try alloc.dupe(u8, term);
    errdefer alloc.free(owned_term);
    return .{ .term = owned_term, .doc_freq = try source.termDocFreq(alloc, field, term) };
}

test "statistics ownership survives every allocation failure and failed frequency read" {
    const Entry = struct { field: []const u8, index_name: ?[]const u8, terms: std.StringHashMapUnmanaged(void) = .{} };
    const Request = struct { index_name: ?[]const u8, field: []const u8, terms: []const []const u8 };
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var map = std.StringHashMapUnmanaged(Entry){};
            defer {
                var it = map.iterator();
                while (it.next()) |entry| {
                    var terms = entry.value_ptr.terms.keyIterator();
                    while (terms.next()) |term| alloc.free(term.*);
                    entry.value_ptr.terms.deinit(alloc);
                    alloc.free(entry.value_ptr.field);
                    alloc.free(entry.key_ptr.*);
                }
                map.deinit(alloc);
            }
            for (0..9) |i| {
                const key = try std.fmt.allocPrint(alloc, "field{d}", .{i});
                try appendTerm(Entry, alloc, &map, key, "index", "body", "alpha");
                const repeated = try std.fmt.allocPrint(alloc, "field{d}", .{i});
                try appendTerm(Entry, alloc, &map, repeated, "index", "body", "beta");
                const duplicate = try std.fmt.allocPrint(alloc, "field{d}", .{i});
                try appendTerm(Entry, alloc, &map, duplicate, "index", "body", "alpha");
            }
            const requests = try requestsFromMap(Request, alloc, &map);
            defer freeRequests(alloc, requests, requests.len);
            try std.testing.expectEqual(@as(usize, 9), requests.len);
            for (requests) |request| {
                try std.testing.expectEqualStrings("body", request.field);
                try std.testing.expectEqualStrings("index", request.index_name.?);
                try std.testing.expectEqual(@as(usize, 2), request.terms.len);
                var alpha = false;
                var beta = false;
                for (request.terms) |term| {
                    alpha = alpha or std.mem.eql(u8, term, "alpha");
                    beta = beta or std.mem.eql(u8, term, "beta");
                }
                try std.testing.expect(alpha and beta);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    const Source = struct {
        fail: bool,
        pub fn termDocFreq(self: @This(), _: std.mem.Allocator, _: []const u8, _: []const u8) !u32 {
            if (self.fail) return error.Timeout;
            return 17;
        }
    };
    try std.testing.expectError(error.Timeout, termDocFreq(std.testing.allocator, Source{ .fail = true }, "body", "alpha"));
    var result = try termDocFreq(std.testing.allocator, Source{ .fail = false }, "body", "alpha");
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("alpha", result.term);
    try std.testing.expectEqual(@as(u32, 17), result.doc_freq);
}

test "statistics frequency result retires on allocation denial" {
    const Case = struct {
        pub fn termDocFreq(_: @This(), _: std.mem.Allocator, _: []const u8, _: []const u8) !u32 {
            return 17;
        }
        fn run(alloc: std.mem.Allocator) !void {
            var result = try ownership.termDocFreq(alloc, @This(){}, "body", "alpha");
            defer result.deinit(alloc);
            try std.testing.expectEqualStrings("alpha", result.term);
            try std.testing.expectEqual(@as(u32, 17), result.doc_freq);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "statistics background labels retire every partial allocation" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const labels = try ownership.backgroundLabels(alloc, "significant_terms", "body");
            defer alloc.free(labels.aggregation_name);
            defer alloc.free(labels.field);
            try std.testing.expectEqualStrings("significant_terms", labels.aggregation_name);
            try std.testing.expectEqualStrings("body", labels.field);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
