// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const source = @import("gliner_boundary_training_source.zig");
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const artifact = @import("../models/gliner_boundary_artifact.zig");
const compat = @import("../io/compat.zig");

// Test-only pins from scripts/gliner25/oracle_manifest.json. Production accepts
// an explicitly supplied identity and never imports this qualification data.
fn expected(backbone: model.Backbone) bundle.Identity {
    const shared_tokenizer = bundle.Digest{ .size_bytes = 8341713, .sha256 = "cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3".* };
    const tokenizer_config = bundle.Digest{ .size_bytes = 645, .sha256 = "0bf3ea0873234bd9bfdd3853c440395009ac6365a925b91654daed5396d655e1".* };
    return switch (backbone) {
        .small => .{ .backbone = backbone, .precision = .fp32, .weight = .{ .size_bytes = 295567700, .sha256 = "4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b".* }, .sidecars = .{
            .{ .size_bytes = 3152, .sha256 = "0b7d9e1401ceeb83e992ec66d2f93bff7e5646428f1b4706ec527cf88f53578a".* },
            .{ .size_bytes = 856, .sha256 = "db837d0dc587f5858687ef860c1f400de10f3c3e44f88daef8cbda80d74e4c9c".* },
            shared_tokenizer,
            tokenizer_config,
        } },
        .base => .{ .backbone = backbone, .precision = .fp32, .weight = .{ .size_bytes = 774366564, .sha256 = "7274094de2e0c2a37a386f55fc4e23061a954da5bd7a335e7dfe56f2743c277a".* }, .sidecars = .{
            .{ .size_bytes = 3150, .sha256 = "0eb92d00584d613aab32b2178f84a85176b62c87ae3689ce9084e83f6eba64d1".* },
            .{ .size_bytes = 857, .sha256 = "d36a845b9f25dcaf1ec45a1c4bdf65ea4ac20596537e14530ec9f660a63aeca4".* },
            shared_tokenizer,
            tokenizer_config,
        } },
        .multi => .{ .backbone = backbone, .precision = .fp32, .weight = .{ .size_bytes = 1149461028, .sha256 = "c1ff4ec0bc00031c15530b8f3c33d3677f27949e6a0cb52e1247a6224b6c5395".* }, .sidecars = .{
            .{ .size_bytes = 3151, .sha256 = "8b59a0f426a65859c89cd1ea850c3529c09aa3be3a6fafd8eddfdd17b1bf0146".* },
            .{ .size_bytes = 857, .sha256 = "fa4f9ef2903b5369ab172333aae4574e6a476511d7465845cf59f8360ee18716".* },
            .{ .size_bytes = 16035853, .sha256 = "c62446df87ae18ec98b133f8f84fc449a07cc89bbf8ef192a4cb5f9c53777a7a".* },
            tokenizer_config,
        } },
    };
}

fn exercisePublished(backbone: model.Backbone, variable: [:0]const u8) !void {
    const path = @import("antfly_platform").env.getenv(variable) orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const reserved = try source.reservation(compat.io(), path, .{}, null);
    const loaded = try source.Source.open(a, compat.io(), path, .{ .expected_identity = expected(backbone), .limits = .{ .max_source_bytes = reserved } }, null);
    defer loaded.deinit();
    try std.testing.expectEqual(expected(backbone), loaded.identity);
    try std.testing.expectEqual(backbone, loaded.config.backbone);
    try std.testing.expectEqual(@as(usize, 334), loaded.parameters.len);
    try std.testing.expectEqual(@as(usize, 334), loaded.store.resident_weights.count());
    const specs = artifact.specs(backbone);
    for (loaded.parameters, specs) |parameter, spec| {
        try std.testing.expectEqualStrings(spec.name, parameter.canonical_name);
        try std.testing.expectEqual(spec.shape.len, parameter.dimensions.len);
        for (spec.shape, parameter.dimensions) |wide, narrow| try std.testing.expectEqual(wide, narrow);
        const tensor = loaded.store.resident_weights.get(parameter.name).?.tensor;
        try std.testing.expectEqual(@intFromPtr(parameter.values.ptr), @intFromPtr(tensor.data.ptr));
        try std.testing.expect(!tensor.owns_data and !tensor.owns_shape);
        try std.testing.expectEqual(parameter.values.len * 4, tensor.data.len);
    }
    const marker = try loaded.tokenizer().encode(a, "[P]");
    defer a.free(marker);
    const id: i32 = if (backbone == .multi) 250104 else 128003;
    try std.testing.expectEqualSlices(i32, &.{id}, marker);
    for (loaded.identity.sidecars, 0..) |pin, index| try std.testing.expectEqual(pin, bundle.Digest.of(try loaded.sidecar(index)));
    const usage = loaded.usage();
    try std.testing.expectEqual(reserved, loaded.reservedBytes());
    try std.testing.expect(usage.peak_bytes <= reserved and usage.live_bytes <= usage.peak_bytes);
    try std.testing.expect(usage.live_bytes > loaded.identity.weight.size_bytes + loaded.identity.sidecars[2].size_bytes);
    std.debug.print("boundary training source {s}: live={d}, peak={d}, reserved={d} bytes\n", .{ @tagName(backbone), usage.live_bytes, usage.peak_bytes, reserved });
}

test "boundary training source published small original FP32 all five pins and complete inventory" {
    try exercisePublished(.small, "ANTFLY_GLINER25_SMALL_MODEL_DIR");
}

test "boundary training source published base original FP32 all five pins and complete inventory" {
    try exercisePublished(.base, "ANTFLY_GLINER25_BASE_MODEL_DIR");
}

test "boundary training source published multi original FP32 all five pins and complete inventory" {
    try exercisePublished(.multi, "ANTFLY_GLINER25_MULTI_MODEL_DIR");
}
