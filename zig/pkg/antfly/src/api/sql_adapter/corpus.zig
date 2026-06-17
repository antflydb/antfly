// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const diagnostics = @import("diagnostics.zig");

pub const UnsupportedPlanFamily = enum {
    query,
    read,
    ddl,
    write,
    insert,
    update,
    update_source,
    delete,
    update_joined_source,
    delete_joined_source,
};

pub fn unsupportedPlanFamilyToken(family: UnsupportedPlanFamily) []const u8 {
    return @tagName(family);
}

pub fn unsupportedFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "unsupported:{s}:requires={s}", .{
        unsupportedPlanFamilyToken(family),
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn adapterNoopFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "adapter_noop:{s}:reason={s}", .{
        family,
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn unsupportedPlanMatchesFamily(plan: []const u8, family: UnsupportedPlanFamily) bool {
    const prefix = "unsupported:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const family_token = unsupportedPlanFamilyToken(family);
    const rest = plan[prefix.len..];
    return std.mem.startsWith(u8, rest, family_token) and
        rest.len > family_token.len and
        rest[family_token.len] == ':';
}

pub fn unsupportedPlanMatchesReason(
    plan: []const u8,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return false;
    if (!unsupportedPlanMatchesFamily(plan, family)) return false;
    return planHasExactStringToken(plan, ":requires=", diagnostics.classificationReasonToken(reason));
}

pub fn adapterNoopPlanMatchesReason(
    plan: []const u8,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return false;
    const prefix = "adapter_noop:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const rest = plan[prefix.len..];
    if (!std.mem.startsWith(u8, rest, family) or rest.len <= family.len or rest[family.len] != ':') return false;
    return planHasExactStringToken(plan, ":reason=", diagnostics.classificationReasonToken(reason));
}

fn planHasExactStringToken(plan: []const u8, token: []const u8, expected: []const u8) bool {
    var index: usize = 0;
    var matches: usize = 0;
    while (std.mem.indexOf(u8, plan[index..], token)) |relative| {
        const value_start = index + relative + token.len;
        var value_end = value_start;
        while (value_end < plan.len and plan[value_end] != ':') value_end += 1;
        if (std.mem.eql(u8, plan[value_start..value_end], expected)) matches += 1;
        index = value_end;
    }
    return matches == 1;
}

test "sql adapter corpus fingerprints unsupported and adapter no-op reasons" {
    const alloc = std.testing.allocator;
    const unsupported = try unsupportedFingerprintAlloc(alloc, .write, .multi_table_generation_barrier);
    defer alloc.free(unsupported);
    try std.testing.expectEqualStrings("unsupported:write:requires=multi_table_generation_barrier", unsupported);
    try std.testing.expect(unsupportedPlanMatchesReason(unsupported, .write, .multi_table_generation_barrier));
    try std.testing.expect(!unsupportedPlanMatchesReason(unsupported, .write, .session_setting));
    try std.testing.expect(!unsupportedPlanMatchesReason("unsupported:write:requires=multi_table_generation_barrier_extra", .write, .multi_table_generation_barrier));

    const noop = try adapterNoopFingerprintAlloc(alloc, "ddl", .session_setting);
    defer alloc.free(noop);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=session_setting", noop);
    try std.testing.expect(adapterNoopPlanMatchesReason(noop, "ddl", .session_setting));
    try std.testing.expect(!adapterNoopPlanMatchesReason(noop, "ddl", .set_operation_plan));
    try std.testing.expect(!adapterNoopPlanMatchesReason("adapter_noop:ddl:reason=session_setting_extra", "ddl", .session_setting));

    try std.testing.expectError(error.UnsupportedSqlShape, unsupportedFingerprintAlloc(alloc, .write, .session_setting));
    try std.testing.expectError(error.UnsupportedSqlShape, adapterNoopFingerprintAlloc(alloc, "ddl", .set_operation_plan));
}
