// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! API-local ownership and validation for the transitional public graph wire.
//! Canonical graph execution never depends on this dialect marker.

const std = @import("std");
const ant_json = @import("antfly-json");
const db_mod = @import("../storage/db/mod.zig");

pub const Dialect = db_mod.types.GraphQueryWireDialect;
pub const deprecation_header_name = "Deprecation";
pub const deprecation_header_value = "@1787702400";

/// Capture an immutable, normalized transport sidecar once at public admission.
/// The exact operation set is checked against the canonical execution plan here
/// so proxy and response boundaries can validate it without rebuilding a JSON
/// tree for every remote shard and execution phase.
pub fn captureRequestTransportAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    expected_operations: anytype,
) !db_mod.types.GraphQueryTransport {
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
    if ((canonical == null) == (legacy == null)) return error.InvalidGraphWireEnvelope;
    const operations = canonical orelse legacy.?;
    return captureOperationsAlloc(
        alloc,
        operations,
        expected_operations,
        if (legacy != null) .legacy else .canonical,
    );
}

/// Capture an already-parsed canonical operation map. Serverless admission uses
/// this after rejecting the legacy field, avoiding another parse of the public
/// body while keeping canonical response encoding bound to the admitted plan.
pub fn captureCanonicalOperationsAlloc(
    alloc: std.mem.Allocator,
    operations: std.json.Value,
    expected_operations: anytype,
) !db_mod.types.GraphQueryTransport {
    return captureOperationsAlloc(alloc, operations, expected_operations, .canonical);
}

fn captureOperationsAlloc(
    alloc: std.mem.Allocator,
    operations: std.json.Value,
    expected_operations: anytype,
    dialect: Dialect,
) !db_mod.types.GraphQueryTransport {
    if (operations != .object or operations.object.count() != expected_operations.len)
        return error.InvalidGraphWireEnvelope;
    for (expected_operations) |operation| {
        if (operations.object.get(operation.name) == null)
            return error.InvalidGraphWireEnvelope;
    }
    const operations_json = try std.json.Stringify.valueAlloc(alloc, operations, .{});
    errdefer alloc.free(operations_json);

    return .{
        .dialect = dialect,
        .operations_json = operations_json,
        .admitted_operations_ptr = @ptrCast(expected_operations.ptr),
        .admitted_operations_len = expected_operations.len,
    };
}

test "graph wire envelope capture normalizes nulls and escaped dialect names" {
    const alloc = std.testing.allocator;
    const Named = struct { name: []const u8 };
    const expected = [_]Named{.{ .name = "walk" }};
    var canonical = try captureRequestTransportAlloc(
        alloc,
        "{\"graph_queries\":{\"walk\":{}},\"graph_searches\":null,\"limit\":1}",
        &expected,
    );
    defer canonical.deinit(alloc);
    try std.testing.expectEqual(Dialect.canonical, canonical.dialect);
    try std.testing.expectEqualStrings("{\"walk\":{}}", canonical.operations_json);
    try std.testing.expectEqual(
        @as(*const anyopaque, @ptrCast(expected[0..].ptr)),
        canonical.admitted_operations_ptr,
    );
    try std.testing.expectEqual(expected.len, canonical.admitted_operations_len);

    var legacy = try captureRequestTransportAlloc(
        alloc,
        "{\"graph_\\u0073earches\":{\"walk\":{}},\"limit\":1}",
        &expected,
    );
    defer legacy.deinit(alloc);
    try std.testing.expectEqual(Dialect.legacy, legacy.dialect);
    try std.testing.expectEqualStrings("{\"walk\":{}}", legacy.operations_json);

    try std.testing.expectError(
        error.InvalidGraphWireEnvelope,
        captureRequestTransportAlloc(
            alloc,
            "{\"graph_queries\":{},\"graph_searches\":{}}",
            &expected,
        ),
    );
}

test "graph wire envelope validates dialect and exact operation set once" {
    const Named = struct { name: []const u8 };
    const expected = [_]Named{.{ .name = "walk" }};

    var canonical = try captureRequestTransportAlloc(
        std.testing.allocator,
        "{ \n \t\"graph_queries\" : {\"walk\":{}} }",
        &expected,
    );
    defer canonical.deinit(std.testing.allocator);
    try std.testing.expectEqual(Dialect.canonical, canonical.dialect);
    try std.testing.expectEqualStrings("{\"walk\":{}}", canonical.operations_json);

    var legacy = try captureRequestTransportAlloc(
        std.testing.allocator,
        "{\"graph_searches\":{\"walk\":{}}}",
        &expected,
    );
    defer legacy.deinit(std.testing.allocator);
    try std.testing.expectEqual(Dialect.legacy, legacy.dialect);

    try std.testing.expectError(
        error.InvalidGraphWireEnvelope,
        captureRequestTransportAlloc(
            std.testing.allocator,
            "{\"graph_queries\":{}}",
            &expected,
        ),
    );
    try std.testing.expectError(
        error.InvalidGraphWireEnvelope,
        captureRequestTransportAlloc(
            std.testing.allocator,
            "{\"graph_queries\":{\"walk\":{}},\"graph_searches\":{}}",
            &expected,
        ),
    );
}

fn expectEnvelopeCaptureAllocationSafe(alloc: std.mem.Allocator) !void {
    const Named = struct { name: []const u8 };
    const expected = [_]Named{.{ .name = "walk" }};
    var captured = try captureRequestTransportAlloc(
        alloc,
        "{\"graph_queries\":{\"walk\":{}},\"limit\":1}",
        &expected,
    );
    defer captured.deinit(alloc);
}

test "graph wire envelope preserves allocator failures" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        expectEnvelopeCaptureAllocationSafe,
        .{},
    );
}
