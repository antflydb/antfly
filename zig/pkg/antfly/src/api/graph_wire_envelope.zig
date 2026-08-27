// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! API-local ownership and validation for the transitional public graph wire.
//! Canonical graph execution never depends on this dialect marker.

const std = @import("std");
const ant_json = @import("antfly-json");

pub const Dialect = enum { canonical, legacy };
pub const deprecation_header_name = "Deprecation";
pub const deprecation_header_value = "@1787702400";

pub const ParsedEnvelope = struct {
    parsed: std.json.Parsed(std.json.Value),
    dialect: Dialect,

    pub fn deinit(self: *@This()) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn operations(self: *const @This()) std.json.Value {
        const field = switch (self.dialect) {
            .canonical => "graph_queries",
            .legacy => "graph_searches",
        };
        return self.parsed.value.object.get(field).?;
    }
};

/// Parse the exact single-field envelope retained after public admission.
/// Callers map InvalidGraphWireEnvelope to their boundary-specific public or
/// internal error without duplicating dialect and operation-set invariants.
pub fn parseEnvelopeAlloc(
    alloc: std.mem.Allocator,
    raw: []const u8,
    expected_operations: anytype,
) !ParsedEnvelope {
    const envelope = std.mem.trim(u8, raw, &std.ascii.whitespace);
    var parsed = ant_json.parseFromSlice(std.json.Value, alloc, envelope, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidGraphWireEnvelope,
    };
    errdefer parsed.deinit();
    if (parsed.value != .object or parsed.value.object.count() != 1)
        return error.InvalidGraphWireEnvelope;

    const canonical = parsed.value.object.get("graph_queries");
    const legacy = parsed.value.object.get("graph_searches");
    if ((canonical == null) == (legacy == null)) return error.InvalidGraphWireEnvelope;
    const operations = canonical orelse legacy.?;
    if (operations != .object or operations.object.count() != expected_operations.len)
        return error.InvalidGraphWireEnvelope;
    for (expected_operations) |operation| {
        if (operations.object.get(operation.name) == null)
            return error.InvalidGraphWireEnvelope;
    }
    return .{
        .parsed = parsed,
        .dialect = if (legacy != null) .legacy else .canonical,
    };
}

/// Retain only the populated public graph field from a complete QueryRequest.
/// Optional explicit nulls have omission semantics, matching generated request
/// models, while two populated dialects fail closed before graph execution.
pub fn captureRequestEnvelopeAlloc(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = ant_json.parseFromSlice(std.json.Value, alloc, body, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidGraphWireEnvelope,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGraphWireEnvelope;

    const canonical_value = parsed.value.object.get("graph_queries");
    const legacy_value = parsed.value.object.get("graph_searches");
    const canonical = if (canonical_value != null and canonical_value.? != .null) canonical_value else null;
    const legacy = if (legacy_value != null and legacy_value.? != .null) legacy_value else null;
    if (canonical != null and legacy != null) return error.InvalidGraphWireEnvelope;
    const operations = canonical orelse legacy orelse return error.InvalidGraphWireEnvelope;
    if (operations != .object) return error.InvalidGraphWireEnvelope;

    var envelope = std.json.ObjectMap.empty;
    defer envelope.deinit(alloc);
    try envelope.put(
        alloc,
        if (canonical != null) "graph_queries" else "graph_searches",
        operations,
    );
    return std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = envelope }, .{});
}

test "graph wire envelope capture normalizes nulls and escaped dialect names" {
    const alloc = std.testing.allocator;
    const canonical = try captureRequestEnvelopeAlloc(
        alloc,
        "{\"graph_queries\":{\"walk\":{}},\"graph_searches\":null,\"limit\":1}",
    );
    defer alloc.free(canonical);
    try std.testing.expectEqualStrings("{\"graph_queries\":{\"walk\":{}}}", canonical);

    const legacy = try captureRequestEnvelopeAlloc(
        alloc,
        "{\"graph_\\u0073earches\":{\"walk\":{}},\"limit\":1}",
    );
    defer alloc.free(legacy);
    try std.testing.expectEqualStrings("{\"graph_searches\":{\"walk\":{}}}", legacy);

    try std.testing.expectError(
        error.InvalidGraphWireEnvelope,
        captureRequestEnvelopeAlloc(
            alloc,
            "{\"graph_queries\":{},\"graph_searches\":{}}",
        ),
    );
}

test "graph wire envelope validates dialect and exact operation set once" {
    const Named = struct { name: []const u8 };
    const expected = [_]Named{.{ .name = "walk" }};

    var canonical = try parseEnvelopeAlloc(
        std.testing.allocator,
        "{ \n \t\"graph_queries\" : {\"walk\":{}} }",
        &expected,
    );
    defer canonical.deinit();
    try std.testing.expectEqual(Dialect.canonical, canonical.dialect);
    try std.testing.expect(canonical.operations().object.get("walk") != null);

    var legacy = try parseEnvelopeAlloc(
        std.testing.allocator,
        "{\"graph_searches\":{\"walk\":{}}}",
        &expected,
    );
    defer legacy.deinit();
    try std.testing.expectEqual(Dialect.legacy, legacy.dialect);

    try std.testing.expectError(
        error.InvalidGraphWireEnvelope,
        parseEnvelopeAlloc(
            std.testing.allocator,
            "{\"graph_queries\":{}}",
            &expected,
        ),
    );
    try std.testing.expectError(
        error.InvalidGraphWireEnvelope,
        parseEnvelopeAlloc(
            std.testing.allocator,
            "{\"graph_queries\":{\"walk\":{}},\"graph_searches\":{}}",
            &expected,
        ),
    );
}

fn expectEnvelopeCaptureAllocationSafe(alloc: std.mem.Allocator) !void {
    const captured = try captureRequestEnvelopeAlloc(
        alloc,
        "{\"graph_queries\":{\"walk\":{}},\"limit\":1}",
    );
    defer alloc.free(captured);
}

fn expectEnvelopeParseAllocationSafe(alloc: std.mem.Allocator) !void {
    const Named = struct { name: []const u8 };
    const expected = [_]Named{.{ .name = "walk" }};
    var parsed = try parseEnvelopeAlloc(
        alloc,
        "{\"graph_queries\":{\"walk\":{}}}",
        &expected,
    );
    defer parsed.deinit();
}

test "graph wire envelope preserves allocator failures" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        expectEnvelopeCaptureAllocationSafe,
        .{},
    );
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        expectEnvelopeParseAllocationSafe,
        .{},
    );
}
