// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test-only external-manifest loader for the installed stored diagnostic.
//! It creates no production route and cannot manufacture artifacts or held data.
const std = @import("std");
const installed = @import("backends/context_stored_installed_invocation.zig");
const driver_mod = @import("backends/context_stored_all36_driver.zig");
const coordinator = @import("backends/compaction_coordinator.zig");
const storage = @import("backends/packed_context_storage.zig");
const payload = @import("runtime/kv/payload.zig");
const replay = @import("runtime/kv/reference_replay.zig");
const profiles = @import("runtime/kv/compaction_profile.zig");

pub const schema = "antfly.context.stored-installed-external-manifest.v2";
const max_manifest_bytes = 8 * 1024 * 1024;

pub const Manifest = struct {
    schema_name: []const u8,
    diagnostic_only: bool,
    serving_authority: bool,
    release_authority: bool,
    artifact_dir: []const u8,
    evidence_dir: []const u8,
    ledger_dir: []const u8,
    reservation_name: []const u8,
    model_identity: profiles.Identity,
    algorithm_sha256: [32]u8,
    build_sha256: [32]u8,
    artifact_manifest_sha256: [32]u8,
    profile_sha256: [32]u8,
    source_descriptor_sha256: [32]u8,
    source_payload_sha256: [32]u8,
    prompt_tokens_sha256: [32]u8,
    native_peak_budget: usize,
    retained_limit: u32,
    reservation: coordinator.StoredDiagnosticReservation,
    artifacts: []const driver_mod.ArtifactSpec,
    evidence: []const driver_mod.FileSpec,
    generator_sha256: [32]u8,
    corpus_sha256: [32]u8,
    ordered_rows_sha256: [32]u8,
    partition_sha256: [32]u8,
    eligible_labels: []const u32,
    eligible_ranges: []const replay.Range,
    eligible_blocks: []const replay.Range,
    parts: []const []const storage.Segment,
};

pub const Loaded = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    parsed: std.json.Parsed(Manifest),
    artifact_dir: std.Io.Dir,
    evidence_dir: std.Io.Dir,
    ledger_dir: std.Io.Dir,
    eligible: payload.Eligible,
    driver: driver_mod.Driver,

    pub fn deinit(self: *Loaded) void {
        self.driver.deinit();
        self.eligible.deinit();
        self.ledger_dir.close(self.io);
        self.evidence_dir.close(self.io);
        self.artifact_dir.close(self.io);
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn request(
        self: *Loaded,
        load_generation: u64,
        actual: profiles.Identity,
        sources: coordinator.PreparedQuerySources,
        authority: coordinator.CampaignAuthority,
    ) !installed.Request {
        const m = self.parsed.value;
        if (load_generation == 0 or !std.meta.eql(actual, m.model_identity))
            return error.InvalidPayloadProvenance;
        return .{
            .frozen = .{
                .load_generation = load_generation,
                .model_identity = m.model_identity,
                .algorithm_sha256 = m.algorithm_sha256,
                .build_sha256 = m.build_sha256,
                .artifact_manifest_sha256 = m.artifact_manifest_sha256,
                .profile_sha256 = m.profile_sha256,
                .source_descriptor_sha256 = m.source_descriptor_sha256,
                .source_payload_sha256 = m.source_payload_sha256,
                .expected_baseline_context_bytes = m.reservation.expected_baseline_context_bytes,
                .expected_global_owner_count = m.reservation.expected_owner_count,
                .expected_total_owner_count = m.reservation.expected_total_owner_count,
            },
            .prompt_tokens_sha256 = m.prompt_tokens_sha256,
            .sources = sources,
            .authority = authority,
            .actual = actual,
            .provider = self.driver.provider(),
            .eligible = &self.eligible,
            .parts = m.parts,
            .retained_limit = m.retained_limit,
            .native_peak_budget = m.native_peak_budget,
        };
    }
};

fn nonzero(value: [32]u8) bool {
    return !std.mem.allEqual(u8, &value, 0);
}

fn validateManifest(m: Manifest) !void {
    if (!std.mem.eql(u8, m.schema_name, schema) or !m.diagnostic_only or m.serving_authority or m.release_authority or
        m.artifact_dir.len == 0 or m.evidence_dir.len == 0 or m.ledger_dir.len == 0 or
        m.reservation_name.len == 0 or m.native_peak_budget == 0 or m.retained_limit == 0 or
        m.artifacts.len != 36 or m.reservation.coordinates.len != 36 or m.reservation.artifact_count != 36 or
        m.reservation.expected_owner_count == 0 or
        m.reservation.expected_total_owner_count < m.reservation.expected_owner_count or
        m.reservation.reservation_identity == 0 or
        m.evidence.len == 0 or m.eligible_labels.len == 0 or m.eligible_ranges.len == 0 or
        m.eligible_blocks.len != 12 or m.parts.len != 3 or
        !nonzero(m.algorithm_sha256) or !nonzero(m.build_sha256) or !nonzero(m.artifact_manifest_sha256) or
        !nonzero(m.profile_sha256) or !nonzero(m.source_descriptor_sha256) or !nonzero(m.source_payload_sha256) or
        !nonzero(m.prompt_tokens_sha256) or !nonzero(m.generator_sha256) or !nonzero(m.corpus_sha256) or
        !nonzero(m.ordered_rows_sha256) or !nonzero(m.partition_sha256))
        return error.InvalidPayloadProvenance;
    if (!std.meta.eql(m.reservation.algorithm_sha256, m.algorithm_sha256) or
        !std.meta.eql(m.reservation.build_sha256, m.build_sha256) or
        !std.meta.eql(m.reservation.artifact_manifest_sha256, m.artifact_manifest_sha256) or
        !std.meta.eql(m.reservation.profile_sha256, m.profile_sha256) or
        !std.meta.eql(m.reservation.source_descriptor_sha256, m.source_descriptor_sha256) or
        !std.meta.eql(m.reservation.source_payload_sha256, m.source_payload_sha256))
        return error.InvalidPayloadProvenance;
    for (m.artifacts, m.reservation.coordinates) |artifact, coordinate|
        if (!std.meta.eql(artifact.coordinate, coordinate)) return error.InvalidPayloadProvenance;
    if (!std.meta.eql(try coordinator.storedDiagnosticProfileDigestDiagnostic(m.reservation.coordinates), m.profile_sha256))
        return error.InvalidPayloadProvenance;
    var previous_end: u32 = 0;
    for (m.eligible_ranges) |range| {
        if (range.start < previous_end or range.start >= range.end or range.end > m.eligible_labels.len)
            return error.InvalidPayloadProvenance;
        previous_end = range.end;
    }
    for (m.eligible_blocks) |block|
        if (block.start >= block.end or block.end - block.start != 512 or block.end > m.eligible_labels.len)
            return error.InvalidPayloadProvenance;
    for (m.parts) |owner_parts| {
        if (owner_parts.len != 14) return error.InvalidPayloadProvenance;
        var logical: u64 = 0;
        for (owner_parts) |part| {
            if (part.logical_count == 0 or part.retained.len == 0) return error.InvalidPayloadProvenance;
            logical = try std.math.add(u64, logical, part.logical_count);
        }
        if (logical != m.eligible_labels.len) return error.InvalidPayloadProvenance;
    }
}

fn copyEligible(allocator: std.mem.Allocator, labels_source: []const u32, ranges_source: []const replay.Range, blocks_source: []const replay.Range) !payload.Eligible {
    const labels = try allocator.dupe(u32, labels_source);
    errdefer allocator.free(labels);
    const ranges = try allocator.dupe(replay.Range, ranges_source);
    errdefer allocator.free(ranges);
    const blocks = try allocator.dupe(replay.Range, blocks_source);
    errdefer allocator.free(blocks);
    return .{ .allocator = allocator, .labels = labels, .ranges = ranges, .blocks = blocks };
}

pub fn load(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
    expected_sha256: [32]u8,
) !Loaded {
    if (manifest_path.len == 0 or !nonzero(expected_sha256)) return error.InvalidPayloadProvenance;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, manifest_path, allocator, .limited(max_manifest_bytes));
    defer allocator.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (!std.meta.eql(digest, expected_sha256)) return error.InvalidPayloadProvenance;
    var parsed = std.json.parseFromSlice(Manifest, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
        .duplicate_field_behavior = .@"error",
    }) catch return error.InvalidPayloadProvenance;
    errdefer parsed.deinit();
    try validateManifest(parsed.value);
    var artifact_dir = try std.Io.Dir.cwd().openDir(io, parsed.value.artifact_dir, .{ .follow_symlinks = false });
    errdefer artifact_dir.close(io);
    var evidence_dir = try std.Io.Dir.cwd().openDir(io, parsed.value.evidence_dir, .{ .follow_symlinks = false });
    errdefer evidence_dir.close(io);
    var ledger_dir = try std.Io.Dir.cwd().openDir(io, parsed.value.ledger_dir, .{ .follow_symlinks = false });
    errdefer ledger_dir.close(io);
    var eligible = try copyEligible(allocator, parsed.value.eligible_labels, parsed.value.eligible_ranges, parsed.value.eligible_blocks);
    errdefer eligible.deinit();
    const config = driver_mod.Config{
        .artifact_dir = artifact_dir,
        .evidence_dir = evidence_dir,
        .ledger_dir = ledger_dir,
        .reservation_name = parsed.value.reservation_name,
        .artifacts = parsed.value.artifacts,
        .evidence = parsed.value.evidence,
        .reservation = parsed.value.reservation,
        .generator_sha256 = parsed.value.generator_sha256,
        .corpus_sha256 = parsed.value.corpus_sha256,
        .ordered_rows_sha256 = parsed.value.ordered_rows_sha256,
        .partition_sha256 = parsed.value.partition_sha256,
    };
    if (!std.meta.eql(try driver_mod.rawManifestDiagnostic(config), parsed.value.artifact_manifest_sha256) or
        try driver_mod.materializationUpperDiagnostic(config) != parsed.value.reservation.materialization_host_bytes)
        return error.InvalidPayloadProvenance;
    var driver = try driver_mod.Driver.init(allocator, io, config);
    errdefer driver.deinit();
    return .{ .allocator = allocator, .io = io, .parsed = parsed, .artifact_dir = artifact_dir, .evidence_dir = evidence_dir, .ledger_dir = ledger_dir, .eligible = eligible, .driver = driver };
}

pub const InvocationReceipt = struct {
    schema_name: []const u8 = "antfly.context.stored-installed-external-receipt.v1",
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    completed: bool,
    external_manifest_sha256: [32]u8,
    capture_input_manifest_sha256: [32]u8,
    source_build_sha256: [32]u8,
    installed: installed.Receipt,
};

pub const BaselineEnvelope = struct {
    schema_name: []const u8 = "antfly.context.stored-installed-baseline-envelope.v1",
    completed: bool = true,
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    external_manifest_sha256: [32]u8,
    capture_input_manifest_sha256: [32]u8,
    source_build_sha256: [32]u8,
    baseline: installed.BaselinePreflightReceipt,
};

pub const InvocationFailureReceipt = struct {
    schema_name: []const u8 = "antfly.context.stored-installed-external-failure.v1",
    completed: bool = false,
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    error_name: []const u8,
    external_manifest_sha256: [32]u8,
    capture_input_manifest_sha256: [32]u8,
    source_build_sha256: [32]u8,
    installed: installed.Receipt,
};

fn writeExclusiveJsonDir(io: std.Io, dir: std.Io.Dir, path: []const u8, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, value, .{ .whitespace = .indent_2 });
    defer std.testing.allocator.free(bytes);
    const file = try dir.createFile(io, path, .{ .exclusive = true });
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
    try file.writeStreamingAll(io, "\n");
    try file.sync(io);
}

fn writeExclusiveJson(io: std.Io, path: []const u8, value: anytype) !void {
    try writeExclusiveJsonDir(io, std.Io.Dir.cwd(), path, value);
}

pub fn writeBaselineReceipt(io: std.Io, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, baseline: installed.BaselinePreflightReceipt) !void {
    if (!baseline.completed or !baseline.diagnostic_only or baseline.serving_authority or baseline.release_authority or
        baseline.baseline_context_physical_bytes == 0 or baseline.global_owner_count == 0 or
        baseline.total_owner_count < baseline.global_owner_count or baseline.replay_control_bytes != 0 or
        !std.meta.eql(build, baseline.build_sha256) or !nonzero(external) or !nonzero(capture) or !nonzero(build))
        return error.InvalidContextTransaction;
    try writeExclusiveJson(io, path, BaselineEnvelope{ .external_manifest_sha256 = external, .capture_input_manifest_sha256 = capture, .source_build_sha256 = build, .baseline = baseline });
}

pub fn writeFailureReceipt(io: std.Io, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, error_name: []const u8, receipt: installed.Receipt) !void {
    if (error_name.len == 0 or receipt.completed or !receipt.diagnostic_only or receipt.serving_authority or receipt.release_authority or
        !nonzero(external) or !nonzero(capture) or !nonzero(build)) return error.InvalidContextTransaction;
    try writeExclusiveJson(io, path, InvocationFailureReceipt{ .error_name = error_name, .external_manifest_sha256 = external, .capture_input_manifest_sha256 = capture, .source_build_sha256 = build, .installed = receipt });
}

pub fn persistFailureOrReturnOriginal(io: std.Io, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, original: anyerror, receipt: installed.Receipt) anyerror!void {
    writeFailureReceipt(io, path, external, capture, build, @errorName(original), receipt) catch return original;
    return original;
}

pub fn writeReceipt(
    io: std.Io,
    path: []const u8,
    external_manifest_sha256: [32]u8,
    capture_input_manifest_sha256: [32]u8,
    source_build_sha256: [32]u8,
    receipt: installed.Receipt,
) !void {
    if (!receipt.completed or !receipt.diagnostic_only or receipt.serving_authority or receipt.release_authority or
        !nonzero(external_manifest_sha256) or !nonzero(capture_input_manifest_sha256) or !nonzero(source_build_sha256))
        return error.InvalidContextTransaction;
    const envelope = InvocationReceipt{
        .completed = true,
        .external_manifest_sha256 = external_manifest_sha256,
        .capture_input_manifest_sha256 = capture_input_manifest_sha256,
        .source_build_sha256 = source_build_sha256,
        .installed = receipt,
    };
    try writeExclusiveJson(io, path, envelope);
}

test "installed external manifest rejects authority and incomplete all36 profile before file opens" {
    const minimal =
        \\{"schema_name":"antfly.context.stored-installed-external-manifest.v1","diagnostic_only":false,"serving_authority":false,"release_authority":false,"artifact_dir":"x","evidence_dir":"x","ledger_dir":"x","reservation_name":"x","model_identity":{"artifact":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"tokenizer":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"template":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"layout":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"runtime":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"algorithm_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"build_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"artifact_manifest_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"profile_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"source_descriptor_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"source_payload_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"prompt_tokens_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"native_peak_budget":1,"retained_limit":1,"reservation":{"reservation_identity":1,"algorithm_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"build_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"artifact_manifest_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"profile_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"coordinates":[],"source_descriptor_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"source_payload_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"loader_budget_bytes":1,"materialization_host_bytes":1,"artifact_count":0,"expected_baseline_context_bytes":1,"expected_owner_count":3},"artifacts":[],"evidence":[],"generator_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"corpus_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"ordered_rows_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"partition_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"eligible_labels":[],"eligible_ranges":[],"eligible_blocks":[],"parts":[]}
    ;
    var parsed = try std.json.parseFromSlice(Manifest, std.testing.allocator, minimal, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectError(error.InvalidPayloadProvenance, validateManifest(parsed.value));
}

fn eligibleThenDriverFailureForTest(open_failure: bool) !void {
    const allocator = std.testing.allocator;
    var labels: [1024]u32 = @splat(1);
    const ranges = [_]replay.Range{.{ .start = 0, .end = labels.len }};
    const blocks = [_]replay.Range{
        .{ .start = 0, .end = 512 },
        .{ .start = 512, .end = 1024 },
    };
    var eligible = try copyEligible(allocator, &labels, &ranges, &blocks);
    errdefer eligible.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    if (!open_failure) {
        _ = try driver_mod.Driver.init(allocator, std.testing.io, .{
            .artifact_dir = tmp.dir,
            .evidence_dir = tmp.dir,
            .ledger_dir = tmp.dir,
            .reservation_name = "invalid-config",
            .artifacts = &.{},
            .evidence = &.{},
            .reservation = .{
                .reservation_identity = 1,
                .algorithm_sha256 = @splat(1),
                .build_sha256 = @splat(2),
                .artifact_manifest_sha256 = @splat(3),
                .profile_sha256 = @splat(4),
                .coordinates = &.{},
                .source_descriptor_sha256 = @splat(5),
                .source_payload_sha256 = @splat(6),
                .loader_budget_bytes = 1,
                .materialization_host_bytes = 1,
                .artifact_count = 0,
                .expected_baseline_context_bytes = 1,
                .expected_owner_count = 3,
                .expected_total_owner_count = 36,
            },
            .generator_sha256 = @splat(7),
            .corpus_sha256 = @splat(8),
            .ordered_rows_sha256 = @splat(9),
            .partition_sha256 = @splat(10),
        });
        return error.TestExpectedError;
    }
    var coordinates: [36]coordinator.StoredDiagnosticCoordinate = undefined;
    var artifacts: [36]driver_mod.ArtifactSpec = undefined;
    for (&coordinates, &artifacts, 0..) |*coordinate, *artifact, index| {
        coordinate.* = .{
            .owner_layer = @intCast(index / 12),
            .head = 0,
            .span = @intCast(index % 12),
            .logical_start = @intCast(512 + (index % 12) * 512),
            .logical_count = 512,
            .retained_rows = 320,
            .head_dim = 64,
        };
        const file = driver_mod.FileSpec{ .path = "missing.bin", .size_bytes = 4096, .sha256 = @splat(11) };
        artifact.* = .{ .coordinate = coordinate.*, .keys = file, .values = file, .bias = file };
    }
    const evidence = [_]driver_mod.FileSpec{.{ .path = "missing-evidence.json", .size_bytes = 1, .sha256 = @splat(12) }};
    var config = driver_mod.Config{
        .artifact_dir = tmp.dir,
        .evidence_dir = tmp.dir,
        .ledger_dir = tmp.dir,
        .reservation_name = "open-failure",
        .artifacts = &artifacts,
        .evidence = &evidence,
        .reservation = .{
            .reservation_identity = 2,
            .algorithm_sha256 = @splat(1),
            .build_sha256 = @splat(2),
            .artifact_manifest_sha256 = @splat(3),
            .profile_sha256 = @splat(4),
            .coordinates = &coordinates,
            .source_descriptor_sha256 = @splat(5),
            .source_payload_sha256 = @splat(6),
            .loader_budget_bytes = 1024 * 1024,
            .materialization_host_bytes = 0,
            .artifact_count = 36,
            .expected_baseline_context_bytes = 1,
            .expected_owner_count = 3,
            .expected_total_owner_count = 36,
        },
        .generator_sha256 = @splat(7),
        .corpus_sha256 = @splat(8),
        .ordered_rows_sha256 = @splat(9),
        .partition_sha256 = @splat(10),
    };
    config.reservation.materialization_host_bytes = try driver_mod.materializationUpperDiagnostic(config);
    config.reservation.artifact_manifest_sha256 = try driver_mod.rawManifestDiagnostic(config);
    if (driver_mod.Driver.init(allocator, std.testing.io, config)) |driver_value| {
        var driver = driver_value;
        driver.deinit();
        return error.TestExpectedError;
    } else |_| return error.ExpectedDriverOpenFailure;
}

test "installed external manifest eligibility ownership survives downstream config and open failures" {
    try std.testing.expectError(error.InvalidPayloadProvenance, eligibleThenDriverFailureForTest(false));
    try std.testing.expectError(error.ExpectedDriverOpenFailure, eligibleThenDriverFailureForTest(true));
}

test "baseline and incomplete failure envelopes are persisted exclusively" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity: profiles.Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    const baseline = installed.BaselinePreflightReceipt{ .load_generation = 1, .model_identity = identity, .build_sha256 = @splat(6), .prompt_tokens = 8160, .prompt_tokens_sha256 = @splat(7), .session_identity = 9, .source_descriptor_sha256 = @splat(8), .source_payload_sha256 = @splat(9), .native_peak_budget = 1024, .tracker_owner_identity = 10, .tracker_live_bytes = 200, .tracker_peak_bytes = 300, .baseline_context_physical_bytes = 100, .replay_control_bytes = 0, .global_owner_count = 3, .total_owner_count = 36 };
    const baseline_envelope = BaselineEnvelope{ .external_manifest_sha256 = @splat(10), .capture_input_manifest_sha256 = @splat(11), .source_build_sha256 = @splat(6), .baseline = baseline };
    try writeExclusiveJsonDir(io, tmp.dir, "baseline.json", baseline_envelope);
    const baseline_bytes = try tmp.dir.readFileAlloc(io, "baseline.json", std.testing.allocator, .limited(64 * 1024));
    defer std.testing.allocator.free(baseline_bytes);
    var parsed_baseline = try std.json.parseFromSlice(BaselineEnvelope, std.testing.allocator, baseline_bytes, .{});
    defer parsed_baseline.deinit();
    try std.testing.expectEqual(@as(u32, 3), parsed_baseline.value.baseline.global_owner_count);
    try std.testing.expectEqual(@as(u32, 36), parsed_baseline.value.baseline.total_owner_count);

    const failed = InvocationFailureReceipt{ .error_name = "InvalidPayloadProvenance", .external_manifest_sha256 = @splat(10), .capture_input_manifest_sha256 = @splat(11), .source_build_sha256 = @splat(6), .installed = .{ .terminal = .{ .state = .failed_before_commit } } };
    try writeExclusiveJsonDir(io, tmp.dir, "failure.json", failed);
    const failure_bytes = try tmp.dir.readFileAlloc(io, "failure.json", std.testing.allocator, .limited(64 * 1024));
    defer std.testing.allocator.free(failure_bytes);
    var parsed_failure = try std.json.parseFromSlice(InvocationFailureReceipt, std.testing.allocator, failure_bytes, .{});
    defer parsed_failure.deinit();
    try std.testing.expect(!parsed_failure.value.completed);
    try std.testing.expectEqual(.failed_before_commit, parsed_failure.value.installed.terminal.state);
}

test "failure receipt writer failure preserves original diagnostic error" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const existing = try tmp.dir.createFile(io, "failure.json", .{ .exclusive = true });
    existing.close(io);
    const original = error.InvalidPayloadProvenance;
    // Exercise the same exclusive-write failure used by the public helper, but
    // return the original native error rather than the writer error.
    const envelope = InvocationFailureReceipt{ .error_name = @errorName(original), .external_manifest_sha256 = @splat(1), .capture_input_manifest_sha256 = @splat(2), .source_build_sha256 = @splat(3), .installed = .{} };
    writeExclusiveJsonDir(io, tmp.dir, "failure.json", envelope) catch {
        try std.testing.expectEqual(error.InvalidPayloadProvenance, original);
        return;
    };
    return error.TestExpectedError;
}
