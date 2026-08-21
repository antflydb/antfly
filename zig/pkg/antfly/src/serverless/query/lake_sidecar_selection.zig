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

//! Query-time sidecar selection policy for lake-native execution. This sits
//! between manifest freshness checks and runtime hydration: callers can request
//! sidecars by name/kind, fail closed on stale artifacts, or explicitly ignore
//! stale artifacts and fall back to scanning.

const std = @import("std");
const Allocator = std.mem.Allocator;
const base_source = @import("../manifest/base_source.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const StalePolicy = enum {
    reject,
    ignore,
};

pub const Policy = struct {
    stale: StalePolicy = .reject,
    require_requested: bool = false,
};

pub const DesiredSidecar = struct {
    name: []const u8 = &.{},
    kind: ?source_binding.SidecarKind = null,
};

pub const Action = enum {
    use,
    ignore_not_requested,
    ignore_stale,
};

pub const Decision = struct {
    name: []u8,
    sidecar_kind: source_binding.SidecarKind,
    action: Action,
    reason: []u8,
    artifact_id: []u8,

    pub fn deinit(self: *Decision, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.reason);
        alloc.free(self.artifact_id);
        self.* = undefined;
    }
};

pub const Plan = struct {
    decisions: []Decision,
    selected_count: u32 = 0,
    stale_ignored_count: u32 = 0,
    not_requested_count: u32 = 0,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.decisions) |*decision| decision.deinit(alloc);
        alloc.free(self.decisions);
        self.* = undefined;
    }

    pub fn find(self: Plan, name: []const u8) ?Decision {
        for (self.decisions) |decision| {
            if (std.mem.eql(u8, decision.name, name)) return decision;
        }
        return null;
    }
};

pub const Summary = struct {
    selected_count: u32 = 0,
    stale_ignored_count: u32 = 0,
    not_requested_count: u32 = 0,
};

pub fn planAlloc(
    alloc: Allocator,
    descriptor: base_source.BaseSourceDescriptor,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    desired: []const DesiredSidecar,
    policy: Policy,
) !Plan {
    const summary = try summarize(descriptor, declarations, desired, policy);

    var decisions = std.ArrayListUnmanaged(Decision).empty;
    errdefer {
        for (decisions.items) |*decision| decision.deinit(alloc);
        decisions.deinit(alloc);
    }

    for (declarations) |decl| {
        const selected = desired.len == 0 or matchesDesired(decl, desired);
        if (!selected) {
            try decisions.append(alloc, try makeDecision(
                alloc,
                decl,
                .ignore_not_requested,
                "sidecar was not requested",
            ));
            continue;
        }

        if (!try matchesBaseSource(descriptor, decl.binding)) {
            switch (policy.stale) {
                .reject => return error.StaleLakeSidecar,
                .ignore => {
                    try decisions.append(alloc, try makeDecision(
                        alloc,
                        decl,
                        .ignore_stale,
                        "sidecar source snapshot does not match pinned base source",
                    ));
                    continue;
                },
            }
        }

        try decisions.append(alloc, try makeDecision(
            alloc,
            decl,
            .use,
            "sidecar matches pinned base source",
        ));
    }

    std.mem.sort(Decision, decisions.items, {}, compareDecision);
    return .{
        .decisions = try decisions.toOwnedSlice(alloc),
        .selected_count = summary.selected_count,
        .stale_ignored_count = summary.stale_ignored_count,
        .not_requested_count = summary.not_requested_count,
    };
}

pub fn summarize(
    descriptor: base_source.BaseSourceDescriptor,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    desired: []const DesiredSidecar,
    policy: Policy,
) !Summary {
    try descriptor.validate();
    try (sidecar_manifest.Manifest{ .artifacts = declarations }).validate();
    try validateDesired(desired);

    var summary = Summary{};
    for (declarations) |decl| {
        const selected = desired.len == 0 or matchesDesired(decl, desired);
        if (!selected) {
            summary.not_requested_count += 1;
            continue;
        }

        if (!try matchesBaseSource(descriptor, decl.binding)) {
            switch (policy.stale) {
                .reject => return error.StaleLakeSidecar,
                .ignore => {
                    summary.stale_ignored_count += 1;
                    continue;
                },
            }
        }

        summary.selected_count += 1;
    }

    if (policy.require_requested) {
        for (desired) |want| {
            if (!(try hasUsableDeclaration(descriptor, declarations, want))) return error.MissingRequiredLakeSidecar;
        }
    }

    return summary;
}

pub fn declarationMatchesDesired(
    decl: sidecar_manifest.DeclaredArtifact,
    desired: []const DesiredSidecar,
) !bool {
    try validateDesired(desired);
    return desired.len == 0 or matchesDesired(decl, desired);
}

pub fn declarationMatchesBaseSource(
    descriptor: base_source.BaseSourceDescriptor,
    binding: source_binding.Binding,
) !bool {
    try descriptor.validate();
    try binding.validate();
    return try matchesBaseSource(descriptor, binding);
}

fn validateDesired(desired: []const DesiredSidecar) !void {
    for (desired) |want| {
        if (want.name.len == 0 and want.kind == null) return error.InvalidLakeSidecarSelection;
    }
}

fn matchesDesired(
    decl: sidecar_manifest.DeclaredArtifact,
    desired: []const DesiredSidecar,
) bool {
    for (desired) |want| {
        if (want.name.len != 0 and !std.mem.eql(u8, want.name, decl.name)) continue;
        if (want.kind) |kind| {
            if (decl.binding.sidecar_kind != kind) continue;
        }
        return true;
    }
    return false;
}

fn hasUsableDeclaration(
    descriptor: base_source.BaseSourceDescriptor,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    desired: DesiredSidecar,
) !bool {
    for (declarations) |decl| {
        if (!matchesDesired(decl, &[_]DesiredSidecar{desired})) continue;
        if (try matchesBaseSource(descriptor, decl.binding)) return true;
    }
    return false;
}

fn makeDecision(
    alloc: Allocator,
    decl: sidecar_manifest.DeclaredArtifact,
    action: Action,
    reason: []const u8,
) !Decision {
    return .{
        .name = try alloc.dupe(u8, decl.name),
        .sidecar_kind = decl.binding.sidecar_kind,
        .action = action,
        .reason = try alloc.dupe(u8, reason),
        .artifact_id = try alloc.dupe(u8, decl.artifact.artifact_id),
    };
}

fn compareDecision(_: void, lhs: Decision, rhs: Decision) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn matchesBaseSource(
    descriptor: base_source.BaseSourceDescriptor,
    binding: source_binding.Binding,
) !bool {
    try binding.validate();
    const expected = try sourceInfoFromBaseSource(descriptor);
    return binding.source_kind == expected.source_kind and
        std.mem.eql(u8, binding.snapshot_id, expected.snapshot_id) and
        std.mem.eql(u8, binding.schema_fingerprint, expected.schema_fingerprint);
}

const SourceInfo = struct {
    source_kind: rowsource.SourceKind,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
};

fn sourceInfoFromBaseSource(descriptor: base_source.BaseSourceDescriptor) !SourceInfo {
    try descriptor.validate();
    return switch (descriptor) {
        .antfly_row_fragments => |source| .{
            .source_kind = .serverless_fragment,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .external_parquet => |source| .{
            .source_kind = .external_parquet,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .external_iceberg => |source| .{
            .source_kind = .external_iceberg,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .external_lance => |source| .{
            .source_kind = .external_lance,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .antfly_document_segments, .antfly_lsm_overlay => return error.UnsupportedLakeSidecarBaseSource,
    };
}

test "lake sidecar selection uses requested fresh sidecars" {
    const alloc = std.testing.allocator;
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    } };
    const declarations = [_]sidecar_manifest.DeclaredArtifact{
        makeTestDeclaration("events.embedding.vector", .vector, "iceberg-9", .vector_segment, "vec-1"),
        makeTestDeclaration("events.body.text", .text, "iceberg-9", .text_segment, "text-1"),
    };

    var plan = try planAlloc(
        alloc,
        descriptor,
        &declarations,
        &[_]DesiredSidecar{.{ .kind = .vector }},
        .{ .require_requested = true },
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), plan.selected_count);
    try std.testing.expectEqual(@as(u32, 1), plan.not_requested_count);
    try std.testing.expectEqual(Action.use, plan.find("events.embedding.vector").?.action);
    try std.testing.expectEqual(Action.ignore_not_requested, plan.find("events.body.text").?.action);
}

test "lake sidecar selection rejects or ignores stale requested sidecars" {
    const alloc = std.testing.allocator;
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    } };
    const declarations = [_]sidecar_manifest.DeclaredArtifact{
        makeTestDeclaration("events.embedding.vector", .vector, "iceberg-8", .vector_segment, "vec-old"),
    };
    const desired = [_]DesiredSidecar{.{ .name = "events.embedding.vector" }};

    try std.testing.expectError(error.StaleLakeSidecar, planAlloc(
        alloc,
        descriptor,
        &declarations,
        &desired,
        .{},
    ));

    var ignored = try planAlloc(
        alloc,
        descriptor,
        &declarations,
        &desired,
        .{ .stale = .ignore },
    );
    defer ignored.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), ignored.selected_count);
    try std.testing.expectEqual(@as(u32, 1), ignored.stale_ignored_count);
    try std.testing.expectEqual(Action.ignore_stale, ignored.find("events.embedding.vector").?.action);

    const summary = try summarize(
        descriptor,
        &declarations,
        &desired,
        .{ .stale = .ignore },
    );
    try std.testing.expectEqual(@as(u32, 0), summary.selected_count);
    try std.testing.expectEqual(@as(u32, 1), summary.stale_ignored_count);
}

test "lake sidecar selection can require requested sidecars" {
    const alloc = std.testing.allocator;
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    } };

    try std.testing.expectError(error.MissingRequiredLakeSidecar, planAlloc(
        alloc,
        descriptor,
        &.{},
        &[_]DesiredSidecar{.{ .kind = .algebraic }},
        .{ .require_requested = true },
    ));
}

fn makeTestDeclaration(
    name: []const u8,
    sidecar_kind: source_binding.SidecarKind,
    snapshot_id: []const u8,
    artifact_kind: @import("../manifest/artifact_ref.zig").ArtifactKind,
    artifact_id: []const u8,
) sidecar_manifest.DeclaredArtifact {
    return .{
        .name = name,
        .binding = .{
            .sidecar_kind = sidecar_kind,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = snapshot_id,
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"payload"},
            .index_config_hash = "sha256:index",
        },
        .artifact = .{
            .kind = artifact_kind,
            .name = name,
            .artifact_id = artifact_id,
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
}
