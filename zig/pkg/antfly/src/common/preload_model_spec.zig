// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

pub const Spec = struct {
    kind: []const u8,
    name: []const u8,
    backend: ?[]const u8,
};

fn isBackend(value: []const u8) bool {
    return std.mem.eql(u8, value, "native") or
        std.mem.eql(u8, value, "onnx") or
        std.mem.eql(u8, value, "metal") or
        std.mem.eql(u8, value, "cuda") or
        std.mem.eql(u8, value, "xla") or
        std.mem.eql(u8, value, "pjrt") or
        std.mem.eql(u8, value, "wasm") or
        std.mem.eql(u8, value, "webgpu");
}

pub fn parse(value: []const u8) !Spec {
    const separator = std.mem.indexOfScalar(u8, value, ':') orelse return error.InvalidArguments;
    const kind = value[0..separator];
    if (kind.len == 0) return error.InvalidArguments;

    var name = value[separator + 1 ..];
    var backend: ?[]const u8 = null;
    if (std.mem.indexOfScalar(u8, name, ':')) |backend_separator| {
        const candidate = name[0..backend_separator];
        if (isBackend(candidate)) {
            backend = candidate;
            name = name[backend_separator + 1 ..];
        }
    }
    if (name.len == 0) return error.InvalidArguments;
    return .{ .kind = kind, .name = name, .backend = backend };
}

test "preload model spec parser categorizes registry variants and backends" {
    const variant = try parse("embedder:BAAI/bge-small-en-v1.5:i8");
    try std.testing.expectEqualStrings("embedder", variant.kind);
    try std.testing.expectEqualStrings("BAAI/bge-small-en-v1.5:i8", variant.name);
    try std.testing.expectEqual(null, variant.backend);

    const multi_component_variant = try parse("generator:owner/model:gguf:Q4_K_M");
    try std.testing.expectEqualStrings("owner/model:gguf:Q4_K_M", multi_component_variant.name);
    try std.testing.expectEqual(null, multi_component_variant.backend);

    for ([_][]const u8{ "native", "onnx", "metal", "cuda", "xla", "pjrt", "wasm", "webgpu" }) |backend| {
        var buffer: [96]u8 = undefined;
        const value = try std.fmt.bufPrint(&buffer, "embedder:{s}:owner/model:i8", .{backend});
        const spec = try parse(value);
        try std.testing.expectEqualStrings(backend, spec.backend.?);
        try std.testing.expectEqualStrings("owner/model:i8", spec.name);
    }

    try std.testing.expectError(error.InvalidArguments, parse("embedder:"));
    try std.testing.expectError(error.InvalidArguments, parse("embedder:metal:"));
}
