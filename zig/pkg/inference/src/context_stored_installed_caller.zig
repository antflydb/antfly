// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    prompt_tokens: u32 = 0,
    baseline_preflight: ?driver_mod.FileSpec = null,
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

pub const LoaderBudgetManifestEdge = struct {
    path: []const u8,
    sha256: [32]u8,
};

pub const LoaderBudgetPreflightReceipt = struct {
    schema_name: []const u8 = "antfly.context.stored-loader-budget-preflight.v1",
    completed: bool = true,
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    input_manifest: LoaderBudgetManifestEdge,
    geometry_sha256: [32]u8,
    coordinates: []const coordinator.StoredDiagnosticCoordinate,
    destination_spans: [36]u32,
    layout_validated: bool,
    artifact_count: u32,
    host_page_size_bytes: usize,
    entries_logical_bytes: usize,
    entries_physical_upper_bytes: usize,
    buffers_shared_bytes: usize,
    physical_upper_bytes: usize,
    declared_loader_budget_bytes: usize,
    declared_materialization_host_bytes: usize,
    buffers_match_declared_materialization: bool,
    total_matches_declared_loader_budget: bool,
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
                .expected_prompt_tokens = m.prompt_tokens,
                .expected_baseline_context_bytes = m.reservation.expected_baseline_context_bytes,
                .expected_global_owner_count = m.reservation.expected_owner_count,
                .expected_total_owner_count = m.reservation.expected_total_owner_count,
            },
            .prompt_tokens_sha256 = m.prompt_tokens_sha256,
            .sources = sources,
            .authority = authority,
            .actual = actual,
            .provider = self.driver.provider(),
            .coordinates = m.reservation.coordinates,
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

fn parseDigest32(text: []const u8) ![32]u8 {
    if (text.len != 64) return error.InvalidPayloadProvenance;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, text) catch return error.InvalidPayloadProvenance;
    return result;
}

fn fileSpecEql(a: driver_mod.FileSpec, b: driver_mod.FileSpec) bool {
    return std.mem.eql(u8, a.path, b.path) and
        a.size_bytes == b.size_bytes and
        std.meta.eql(a.sha256, b.sha256);
}

fn containsFileSpec(items: []const driver_mod.FileSpec, expected: driver_mod.FileSpec) bool {
    for (items) |item| if (fileSpecEql(item, expected)) return true;
    return false;
}

fn validateManifest(m: Manifest) !void {
    if (!std.mem.eql(u8, m.schema_name, schema) or !m.diagnostic_only or m.serving_authority or m.release_authority or
        m.artifact_dir.len == 0 or m.evidence_dir.len == 0 or m.ledger_dir.len == 0 or
        m.reservation_name.len == 0 or m.native_peak_budget == 0 or m.retained_limit == 0 or m.prompt_tokens == 0 or
        m.artifacts.len != 36 or m.reservation.coordinates.len != 36 or m.reservation.artifact_count != 36 or
        m.reservation.expected_owner_count == 0 or
        m.reservation.expected_total_owner_count < m.reservation.expected_owner_count or
        m.reservation.reservation_identity == 0 or
        m.evidence.len == 0 or m.eligible_labels.len == 0 or m.eligible_labels.len != @as(usize, m.prompt_tokens) or m.eligible_ranges.len == 0 or
        m.eligible_blocks.len != 12 or m.parts.len != 3 or
        !nonzero(m.algorithm_sha256) or !nonzero(m.build_sha256) or !nonzero(m.artifact_manifest_sha256) or
        !nonzero(m.profile_sha256) or !nonzero(m.source_descriptor_sha256) or !nonzero(m.source_payload_sha256) or
        !nonzero(m.prompt_tokens_sha256) or !nonzero(m.generator_sha256) or !nonzero(m.corpus_sha256) or
        m.baseline_preflight == null or
        !nonzero(m.ordered_rows_sha256) or !nonzero(m.partition_sha256))
        return error.InvalidPayloadProvenance;
    const baseline_spec = m.baseline_preflight orelse return error.InvalidPayloadProvenance;
    if (!containsFileSpec(m.evidence, baseline_spec) or
        !std.meta.eql(m.reservation.algorithm_sha256, m.algorithm_sha256) or
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
    try validateCoordinateParts(m.reservation.coordinates, m.parts);
}

fn validateCoordinateParts(coordinates: []const coordinator.StoredDiagnosticCoordinate, parts: []const []const storage.Segment) !void {
    _ = try driver_mod.layoutForCoordinatesDiagnostic(coordinates, parts);
}

fn loaderGeometryBudget(m: Manifest) !driver_mod.LoaderGeometryBudget {
    return driver_mod.loaderBudgetForCoordinatesDiagnostic(m.reservation.coordinates);
}

fn validateLoaderBudgetValues(coordinates: []const coordinator.StoredDiagnosticCoordinate, materialization_host_bytes: usize, loader_budget_bytes: usize) !void {
    const measured = try driver_mod.loaderBudgetForCoordinatesDiagnostic(coordinates);
    if (measured.budget.buffers_shared_bytes != materialization_host_bytes or
        measured.budget.physical_upper_bytes != loader_budget_bytes)
        return error.InvalidPayloadProvenance;
}

fn validateExactLoaderBudget(m: Manifest) !void {
    try validateLoaderBudgetValues(
        m.reservation.coordinates,
        m.reservation.materialization_host_bytes,
        m.reservation.loader_budget_bytes,
    );
}

fn parseManifestBase(allocator: std.mem.Allocator, io: std.Io, manifest_path: []const u8, expected_sha256: [32]u8) !std.json.Parsed(Manifest) {
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
    return parsed;
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
    expected_capture_sha256: [32]u8,
) !Loaded {
    if (manifest_path.len == 0 or !nonzero(expected_sha256) or !nonzero(expected_capture_sha256)) return error.InvalidPayloadProvenance;
    var parsed = try parseManifestBase(allocator, io, manifest_path, expected_sha256);
    errdefer parsed.deinit();
    // This uses the private native LoadedStoredArtifact size and the exact
    // packed-loader page rounding. Reject stale external arithmetic before any
    // artifact/provider/ledger handle is opened.
    try validateExactLoaderBudget(parsed.value);
    var artifact_dir = try std.Io.Dir.cwd().openDir(io, parsed.value.artifact_dir, .{ .follow_symlinks = false });
    errdefer artifact_dir.close(io);
    var evidence_dir = try std.Io.Dir.cwd().openDir(io, parsed.value.evidence_dir, .{ .follow_symlinks = false });
    errdefer evidence_dir.close(io);
    const baseline_spec = parsed.value.baseline_preflight orelse return error.InvalidPayloadProvenance;
    if (baseline_spec.path.len == 0 or baseline_spec.size_bytes == 0 or !nonzero(baseline_spec.sha256)) return error.InvalidPayloadProvenance;
    const baseline_bytes = try evidence_dir.readFileAlloc(io, baseline_spec.path, allocator, .limited(max_manifest_bytes));
    defer allocator.free(baseline_bytes);
    if (baseline_bytes.len != baseline_spec.size_bytes) return error.InvalidPayloadProvenance;
    var baseline_sha: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(baseline_bytes, &baseline_sha, .{});
    if (!std.meta.eql(baseline_sha, baseline_spec.sha256)) return error.InvalidPayloadProvenance;
    var baseline = std.json.parseFromSlice(BaselineEnvelope, allocator, baseline_bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
        .duplicate_field_behavior = .@"error",
    }) catch return error.InvalidPayloadProvenance;
    defer baseline.deinit();
    try validateBaselineEnvelope(parsed.value, expected_capture_sha256, baseline.value);
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
    schema_name: []const u8 = "antfly.context.stored-installed-external-receipt.v2",
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
    capture_input_manifest_sha256: [32]u8,
    source_build_sha256: [32]u8,
    baseline: installed.BaselinePreflightReceipt,
};

fn validateBaselineReceiptShape(baseline: installed.BaselinePreflightReceipt) !void {
    if (!std.mem.eql(u8, baseline.schema_name, "antfly.context.stored-installed-baseline-preflight.v1") or
        !baseline.completed or !baseline.diagnostic_only or baseline.serving_authority or baseline.release_authority or
        baseline.load_generation == 0 or baseline.prompt_tokens == 0 or baseline.session_identity == 0 or
        baseline.native_peak_budget == 0 or baseline.tracker_owner_identity == 0 or
        baseline.baseline_context_physical_bytes == 0 or baseline.global_owner_count == 0 or
        baseline.total_owner_count < baseline.global_owner_count or baseline.replay_control_bytes != 0 or
        baseline.tracker_live_bytes < baseline.baseline_context_physical_bytes or
        baseline.tracker_peak_bytes < baseline.tracker_live_bytes or
        baseline.tracker_peak_bytes > baseline.native_peak_budget or
        !nonzero(baseline.build_sha256) or !nonzero(baseline.prompt_tokens_sha256) or
        !nonzero(baseline.source_descriptor_sha256) or !nonzero(baseline.source_payload_sha256))
        return error.InvalidPayloadProvenance;
}

fn validateBaselineCaptureBinding(expected_capture_sha256: [32]u8, envelope: BaselineEnvelope) !void {
    if (!nonzero(expected_capture_sha256) or
        !std.meta.eql(envelope.capture_input_manifest_sha256, expected_capture_sha256))
        return error.InvalidPayloadProvenance;
}

fn validateBaselineEnvelope(manifest: Manifest, expected_capture_sha256: [32]u8, envelope: BaselineEnvelope) !void {
    const baseline = envelope.baseline;
    try validateBaselineReceiptShape(baseline);
    try validateBaselineCaptureBinding(expected_capture_sha256, envelope);
    if (!std.mem.eql(u8, envelope.schema_name, "antfly.context.stored-installed-baseline-envelope.v1") or
        !envelope.completed or !envelope.diagnostic_only or envelope.serving_authority or envelope.release_authority or
        !std.meta.eql(baseline.model_identity, manifest.model_identity) or
        !std.meta.eql(baseline.build_sha256, manifest.build_sha256) or
        !std.meta.eql(envelope.source_build_sha256, manifest.build_sha256) or
        !std.meta.eql(baseline.prompt_tokens_sha256, manifest.prompt_tokens_sha256) or
        baseline.prompt_tokens != manifest.prompt_tokens or
        !std.meta.eql(baseline.source_descriptor_sha256, manifest.source_descriptor_sha256) or
        !std.meta.eql(baseline.source_payload_sha256, manifest.source_payload_sha256) or
        baseline.native_peak_budget != manifest.native_peak_budget or baseline.replay_control_bytes != 0 or
        baseline.baseline_context_physical_bytes != manifest.reservation.expected_baseline_context_bytes or
        baseline.global_owner_count != manifest.reservation.expected_owner_count or
        baseline.total_owner_count != manifest.reservation.expected_total_owner_count)
        return error.InvalidPayloadProvenance;
}

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

/// Test-binary-only metadata probe. It deliberately reports mismatches rather
/// than rejecting them, so a stale manifest can produce durable causal
/// evidence without opening artifacts, providers, or reservation ledgers.
pub fn writeLoaderBudgetPreflightReceipt(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
    manifest_sha256: [32]u8,
    output_path: []const u8,
) !void {
    if (!@import("builtin").is_test or manifest_path.len == 0 or output_path.len == 0 or !nonzero(manifest_sha256))
        return error.DiagnosticOnly;
    var parsed = try parseManifestBase(allocator, io, manifest_path, manifest_sha256);
    defer parsed.deinit();
    const measured = try loaderGeometryBudget(parsed.value);
    const layout = try driver_mod.layoutForCoordinatesDiagnostic(parsed.value.reservation.coordinates, parsed.value.parts);
    const budget = measured.budget;
    try writeExclusiveJson(io, output_path, LoaderBudgetPreflightReceipt{
        .input_manifest = .{ .path = manifest_path, .sha256 = manifest_sha256 },
        .geometry_sha256 = measured.geometry_sha256,
        .coordinates = parsed.value.reservation.coordinates,
        .destination_spans = layout.destination_spans,
        .layout_validated = true,
        .artifact_count = parsed.value.reservation.artifact_count,
        .host_page_size_bytes = std.heap.pageSize(),
        .entries_logical_bytes = budget.entries_logical_bytes,
        .entries_physical_upper_bytes = budget.entries_physical_upper_bytes,
        .buffers_shared_bytes = budget.buffers_shared_bytes,
        .physical_upper_bytes = budget.physical_upper_bytes,
        .declared_loader_budget_bytes = parsed.value.reservation.loader_budget_bytes,
        .declared_materialization_host_bytes = parsed.value.reservation.materialization_host_bytes,
        .buffers_match_declared_materialization = budget.buffers_shared_bytes == parsed.value.reservation.materialization_host_bytes,
        .total_matches_declared_loader_budget = budget.physical_upper_bytes == parsed.value.reservation.loader_budget_bytes,
    });
}

pub fn writeBaselineReceipt(io: std.Io, path: []const u8, capture: [32]u8, build: [32]u8, baseline: installed.BaselinePreflightReceipt) !void {
    validateBaselineReceiptShape(baseline) catch return error.InvalidContextTransaction;
    if (!std.meta.eql(build, baseline.build_sha256) or !nonzero(capture) or !nonzero(build))
        return error.InvalidContextTransaction;
    try writeExclusiveJson(io, path, BaselineEnvelope{ .capture_input_manifest_sha256 = capture, .source_build_sha256 = build, .baseline = baseline });
}

pub fn writeFailureReceipt(io: std.Io, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, error_name: []const u8, receipt: installed.Receipt) !void {
    try writeFailureReceiptDir(io, std.Io.Dir.cwd(), path, external, capture, build, error_name, receipt);
}

pub fn markCommittedReceiptWriteFailure(receipt: *installed.Receipt) !void {
    if (!receipt.completed or receipt.terminal.state != .committed or
        !receipt.diagnostic_only or receipt.serving_authority or receipt.release_authority)
        return error.InvalidContextTransaction;
    receipt.completed = false;
    receipt.terminal.state = .committed_but_receipt_failed;
}

fn writeFailureReceiptDir(io: std.Io, dir: std.Io.Dir, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, error_name: []const u8, receipt: installed.Receipt) !void {
    if (error_name.len == 0 or receipt.completed or !receipt.diagnostic_only or receipt.serving_authority or receipt.release_authority or
        !nonzero(external) or !nonzero(capture) or !nonzero(build)) return error.InvalidContextTransaction;
    try writeExclusiveJsonDir(io, dir, path, InvocationFailureReceipt{ .error_name = error_name, .external_manifest_sha256 = external, .capture_input_manifest_sha256 = capture, .source_build_sha256 = build, .installed = receipt });
}

pub fn persistFailureOrReturnOriginal(io: std.Io, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, original: anyerror, receipt: installed.Receipt) anyerror!void {
    return persistFailureOrReturnOriginalDir(io, std.Io.Dir.cwd(), path, external, capture, build, original, receipt);
}

fn persistFailureOrReturnOriginalDir(io: std.Io, dir: std.Io.Dir, path: []const u8, external: [32]u8, capture: [32]u8, build: [32]u8, original: anyerror, receipt: installed.Receipt) anyerror!void {
    writeFailureReceiptDir(io, dir, path, external, capture, build, @errorName(original), receipt) catch |writer_err| {
        const external_hex = std.fmt.bytesToHex(external, .lower);
        std.debug.print(
            "context_stored_installed_failure_receipt: original={s} writer={s} state={s} reservation={d} load={d} session={d} publication_before={d} publication_after={d} baseline_bytes={d} stored_bytes={d} external_manifest={s}\n",
            .{
                @errorName(original),
                @errorName(writer_err),
                @tagName(receipt.terminal.state),
                receipt.terminal.reservation_identity,
                receipt.load_generation,
                receipt.session_identity,
                receipt.publication_epoch_before,
                receipt.publication_epoch_after,
                receipt.baseline_context_physical_bytes,
                receipt.terminal.stored_request_context_physical_bytes,
                &external_hex,
            },
        );
        return original;
    };
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

test "context installed external manifest rejects authority and incomplete all36 profile before file opens" {
    const minimal =
        \\{"schema_name":"antfly.context.stored-installed-external-manifest.v1","diagnostic_only":false,"serving_authority":false,"release_authority":false,"artifact_dir":"x","evidence_dir":"x","ledger_dir":"x","reservation_name":"x","model_identity":{"artifact":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"tokenizer":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"template":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"layout":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"runtime":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"algorithm_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"build_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"artifact_manifest_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"profile_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"source_descriptor_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"source_payload_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"prompt_tokens_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"native_peak_budget":1,"retained_limit":1,"reservation":{"reservation_identity":1,"algorithm_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"build_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"artifact_manifest_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"profile_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"coordinates":[],"source_descriptor_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"source_payload_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"loader_budget_bytes":1,"materialization_host_bytes":1,"artifact_count":0,"expected_baseline_context_bytes":1,"expected_owner_count":3},"artifacts":[],"evidence":[],"generator_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"corpus_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"ordered_rows_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"partition_sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"eligible_labels":[],"eligible_ranges":[],"eligible_blocks":[],"parts":[]}
    ;
    var parsed = try std.json.parseFromSlice(Manifest, std.testing.allocator, minimal, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectError(error.InvalidPayloadProvenance, validateManifest(parsed.value));
}

test "context installed external manifest matches parsed file specs by content" {
    const Fixture = struct {
        baseline_preflight: driver_mod.FileSpec,
        evidence: []const driver_mod.FileSpec,
    };
    const bytes =
        \\{"baseline_preflight":{"path":"baseline.json","size_bytes":123,"sha256":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]},"evidence":[{"path":"baseline.json","size_bytes":123,"sha256":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}]}
    ;
    var parsed = try std.json.parseFromSlice(Fixture, std.testing.allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
        .duplicate_field_behavior = .@"error",
    });
    defer parsed.deinit();
    const baseline = parsed.value.baseline_preflight;
    try std.testing.expect(baseline.path.ptr != parsed.value.evidence[0].path.ptr);
    try std.testing.expect(containsFileSpec(parsed.value.evidence, baseline));

    var changed = baseline;
    changed.path = "other.json";
    try std.testing.expect(!containsFileSpec(parsed.value.evidence, changed));
    changed = baseline;
    changed.size_bytes += 1;
    try std.testing.expect(!containsFileSpec(parsed.value.evidence, changed));
    changed = baseline;
    changed.sha256[0] +%= 1;
    try std.testing.expect(!containsFileSpec(parsed.value.evidence, changed));
}

test "context installed all36 coordinates map lossy ordinals across exact prefix and tail" {
    const exact_one = [_]u32{512};
    const exact_tail = [_]u32{1504};
    const retained = [_][1]u32{ .{352}, .{288}, .{288} };
    var owner_segments: [3][14]storage.Segment = undefined;
    var parts: [3][]const storage.Segment = undefined;
    var coordinates: [36]coordinator.StoredDiagnosticCoordinate = undefined;
    const layers = [_]u32{ 4, 9, 14 };
    for (&owner_segments, &parts, 0..) |*segments, *owner_parts, owner| {
        segments[0] = .{ .logical_count = 512, .retained = &exact_one };
        for (1..13) |segment| segments[segment] = .{ .logical_count = 512, .retained = &retained[owner] };
        segments[13] = .{ .logical_count = 1504, .retained = &exact_tail };
        owner_parts.* = segments;
        for (0..12) |span| coordinates[owner * 12 + span] = .{
            .owner_layer = layers[owner],
            .head = 0,
            .span = @intCast(span),
            .logical_start = @intCast((span + 1) * 512),
            .logical_count = 512,
            .retained_rows = retained[owner][0],
            .head_dim = 512,
        };
    }
    try validateCoordinateParts(&coordinates, &parts);
    const measured = try driver_mod.loaderBudgetForCoordinatesDiagnostic(&coordinates);
    if (std.heap.pageSize() == 16 * 1024)
        try std.testing.expectEqual(@as(usize, 23_396_352), measured.budget.buffers_shared_bytes);
    try std.testing.expectError(
        error.InvalidPayloadProvenance,
        validateLoaderBudgetValues(&coordinates, measured.budget.buffers_shared_bytes, measured.budget.buffers_shared_bytes),
    );
    try validateLoaderBudgetValues(&coordinates, measured.budget.buffers_shared_bytes, measured.budget.physical_upper_bytes);
    const layout = try driver_mod.layoutForCoordinatesDiagnostic(&coordinates, &parts);
    for (layout.destination_spans, 0..) |destination_span, index|
        try std.testing.expectEqual(@as(u32, @intCast((index % 12) + 1)), destination_span);
    coordinates[12].owner_layer = 4;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateCoordinateParts(&coordinates, &parts));
    for (coordinates[12..24]) |*coordinate| coordinate.owner_layer = 15;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateCoordinateParts(&coordinates, &parts));
    for (coordinates[12..24]) |*coordinate| coordinate.owner_layer = 9;
    coordinates[0].span = 1;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateCoordinateParts(&coordinates, &parts));
    coordinates[0].span = 0;
    coordinates[0].logical_start = 0;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateCoordinateParts(&coordinates, &parts));
}

test "context installed buffer-only budget rejects before nonexistent directories open" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var labels: [8160]u32 = @splat(0);
    const ranges = [_]replay.Range{.{ .start = 0, .end = @intCast(labels.len) }};
    var blocks: [12]replay.Range = undefined;
    for (&blocks, 0..) |*block, index| block.* = .{ .start = @intCast(index * 512), .end = @intCast((index + 1) * 512) };
    const exact = [_]u32{512};
    const tail = [_]u32{1504};
    const retained = [_][1]u32{ .{352}, .{288}, .{288} };
    const layers = [_]u32{ 4, 9, 14 };
    var owner_segments: [3][14]storage.Segment = undefined;
    var parts: [3][]const storage.Segment = undefined;
    var coordinates: [36]coordinator.StoredDiagnosticCoordinate = undefined;
    var artifacts: [36]driver_mod.ArtifactSpec = undefined;
    const missing = driver_mod.FileSpec{ .path = "missing.bin", .size_bytes = 512, .sha256 = @splat(20) };
    for (&owner_segments, &parts, 0..) |*segments, *owner_parts, owner| {
        segments[0] = .{ .logical_count = 512, .retained = &exact };
        for (1..13) |segment| segments[segment] = .{ .logical_count = 512, .retained = &retained[owner] };
        segments[13] = .{ .logical_count = 1504, .retained = &tail };
        owner_parts.* = segments;
        for (0..12) |span| {
            const index = owner * 12 + span;
            coordinates[index] = .{
                .owner_layer = layers[owner],
                .head = 0,
                .span = @intCast(span),
                .logical_start = @intCast((span + 1) * 512),
                .logical_count = 512,
                .retained_rows = retained[owner][0],
                .head_dim = 512,
            };
            artifacts[index] = .{ .coordinate = coordinates[index], .keys = missing, .values = missing, .bias = missing };
        }
    }
    const baseline = driver_mod.FileSpec{ .path = "missing-baseline.json", .size_bytes = 1, .sha256 = @splat(21) };
    const profile = try coordinator.storedDiagnosticProfileDigestDiagnostic(&coordinates);
    const geometry_budget = try driver_mod.loaderBudgetForCoordinatesDiagnostic(&coordinates);
    const identity = profiles.Identity{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    var manifest = Manifest{
        .schema_name = schema,
        .diagnostic_only = true,
        .serving_authority = false,
        .release_authority = false,
        .artifact_dir = "missing-artifact-dir",
        .evidence_dir = "missing-evidence-dir",
        .ledger_dir = "missing-ledger-dir",
        .reservation_name = "must-not-open",
        .model_identity = identity,
        .algorithm_sha256 = @splat(6),
        .build_sha256 = @splat(7),
        .artifact_manifest_sha256 = @splat(8),
        .profile_sha256 = profile,
        .source_descriptor_sha256 = @splat(9),
        .source_payload_sha256 = @splat(10),
        .prompt_tokens_sha256 = @splat(11),
        .prompt_tokens = @intCast(labels.len),
        .baseline_preflight = baseline,
        .native_peak_budget = 1024 * 1024 * 1024,
        .retained_limit = 352,
        .reservation = .{
            .reservation_identity = 12,
            .algorithm_sha256 = @splat(6),
            .build_sha256 = @splat(7),
            .artifact_manifest_sha256 = @splat(8),
            .profile_sha256 = profile,
            .coordinates = &coordinates,
            .source_descriptor_sha256 = @splat(9),
            .source_payload_sha256 = @splat(10),
            .loader_budget_bytes = geometry_budget.budget.buffers_shared_bytes,
            .materialization_host_bytes = geometry_budget.budget.buffers_shared_bytes,
            .artifact_count = 36,
            .expected_baseline_context_bytes = 1,
            .expected_owner_count = 3,
            .expected_total_owner_count = 15,
        },
        .artifacts = &artifacts,
        .evidence = &.{baseline},
        .generator_sha256 = @splat(13),
        .corpus_sha256 = @splat(14),
        .ordered_rows_sha256 = @splat(15),
        .partition_sha256 = @splat(16),
        .eligible_labels = &labels,
        .eligible_ranges = &ranges,
        .eligible_blocks = &blocks,
        .parts = &parts,
    };
    try validateManifest(manifest);
    try writeExclusiveJsonDir(io, tmp.dir, "manifest.json", manifest);
    const manifest_bytes = try tmp.dir.readFileAlloc(io, "manifest.json", std.testing.allocator, .limited(max_manifest_bytes));
    defer std.testing.allocator.free(manifest_bytes);
    var manifest_sha: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(manifest_bytes, &manifest_sha, .{});
    const manifest_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/manifest.json", .{tmp.sub_path});
    defer std.testing.allocator.free(manifest_path);
    try std.testing.expectError(error.InvalidPayloadProvenance, load(std.testing.allocator, io, manifest_path, manifest_sha, @splat(17)));

    manifest.reservation.loader_budget_bytes = geometry_budget.budget.physical_upper_bytes;
    try validateManifest(manifest);
    try validateExactLoaderBudget(manifest);
    try writeExclusiveJsonDir(io, tmp.dir, "valid-budget-manifest.json", manifest);
    const valid_bytes = try tmp.dir.readFileAlloc(io, "valid-budget-manifest.json", std.testing.allocator, .limited(max_manifest_bytes));
    defer std.testing.allocator.free(valid_bytes);
    var valid_sha: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(valid_bytes, &valid_sha, .{});
    const valid_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/valid-budget-manifest.json", .{tmp.sub_path});
    defer std.testing.allocator.free(valid_path);
    try std.testing.expectError(error.FileNotFound, load(std.testing.allocator, io, valid_path, valid_sha, @splat(17)));
}

test "context installed external manifest budget preflight writes exact native metadata only" {
    const enabled = std.c.getenv("ANTFLY_CONTEXT_STORED_BUDGET_PREFLIGHT") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const manifest_path = std.c.getenv("ANTFLY_CONTEXT_STORED_ALL36_MANIFEST") orelse return error.MissingReplayCaptureInput;
    const manifest_sha_text = std.c.getenv("ANTFLY_CONTEXT_STORED_ALL36_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
    const output_path = std.c.getenv("ANTFLY_CONTEXT_STORED_BUDGET_RECEIPT") orelse return error.MissingReplayCaptureInput;
    try writeLoaderBudgetPreflightReceipt(
        std.testing.allocator,
        std.testing.io,
        std.mem.span(manifest_path),
        try parseDigest32(std.mem.span(manifest_sha_text)),
        std.mem.span(output_path),
    );
}

test "context installed external manifest load-only opens and closes exact parsed authority without claims" {
    const enabled = std.c.getenv("ANTFLY_CONTEXT_STORED_LOAD_ONLY") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const manifest_path = std.c.getenv("ANTFLY_CONTEXT_STORED_ALL36_MANIFEST") orelse return error.MissingReplayCaptureInput;
    const manifest_sha_text = std.c.getenv("ANTFLY_CONTEXT_STORED_ALL36_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
    const capture_sha_text = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
    const manifest_sha = try parseDigest32(std.mem.span(manifest_sha_text));
    const capture_sha = try parseDigest32(std.mem.span(capture_sha_text));

    var ledger_path: ?[]u8 = null;
    defer if (ledger_path) |value| std.testing.allocator.free(value);
    var reservation_name: ?[]u8 = null;
    defer if (reservation_name) |value| std.testing.allocator.free(value);
    {
        var loaded = try load(std.testing.allocator, std.testing.io, std.mem.span(manifest_path), manifest_sha, capture_sha);
        defer loaded.deinit();
        ledger_path = try std.testing.allocator.dupe(u8, loaded.parsed.value.ledger_dir);
        reservation_name = try std.testing.allocator.dupe(u8, loaded.parsed.value.reservation_name);
        try std.testing.expect(loaded.driver.ledger == null);
        try std.testing.expect(loaded.driver.tuple_claim == null);
        try std.testing.expect(loaded.driver.content_claim == null);
        try std.testing.expect(!loaded.driver.materialized);
        try std.testing.expect(!loaded.driver.reservation_open);
        try std.testing.expect(!loaded.driver.terminal);
        try std.testing.expectError(error.FileNotFound, loaded.ledger_dir.access(std.testing.io, reservation_name.?, .{}));
    }
    var audit_ledger_dir = try std.Io.Dir.cwd().openDir(std.testing.io, ledger_path.?, .{ .follow_symlinks = false });
    defer audit_ledger_dir.close(std.testing.io);
    try std.testing.expectError(error.FileNotFound, audit_ledger_dir.access(std.testing.io, reservation_name.?, .{}));
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

test "context installed external manifest eligibility ownership survives downstream config and open failures" {
    try std.testing.expectError(error.InvalidPayloadProvenance, eligibleThenDriverFailureForTest(false));
    try std.testing.expectError(error.ExpectedDriverOpenFailure, eligibleThenDriverFailureForTest(true));
}

test "context baseline and incomplete failure envelopes are persisted exclusively" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity: profiles.Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    const baseline = installed.BaselinePreflightReceipt{ .load_generation = 1, .model_identity = identity, .build_sha256 = @splat(6), .prompt_tokens = 8160, .prompt_tokens_sha256 = @splat(7), .session_identity = 9, .source_descriptor_sha256 = @splat(8), .source_payload_sha256 = @splat(9), .native_peak_budget = 1024, .tracker_owner_identity = 10, .tracker_live_bytes = 200, .tracker_peak_bytes = 300, .baseline_context_physical_bytes = 100, .replay_control_bytes = 0, .global_owner_count = 3, .total_owner_count = 36 };
    const baseline_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/baseline.json", .{tmp.sub_path});
    defer std.testing.allocator.free(baseline_path);
    try writeBaselineReceipt(io, baseline_path, @splat(11), @splat(6), baseline);
    const baseline_bytes = try tmp.dir.readFileAlloc(io, "baseline.json", std.testing.allocator, .limited(64 * 1024));
    defer std.testing.allocator.free(baseline_bytes);
    var parsed_baseline = try std.json.parseFromSlice(BaselineEnvelope, std.testing.allocator, baseline_bytes, .{});
    defer parsed_baseline.deinit();
    try std.testing.expectEqual(@as(u32, 3), parsed_baseline.value.baseline.global_owner_count);
    try std.testing.expectEqual(@as(u32, 36), parsed_baseline.value.baseline.total_owner_count);

    const failure_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/failure.json", .{tmp.sub_path});
    defer std.testing.allocator.free(failure_path);
    const failed_receipt = installed.Receipt{ .terminal = .{ .state = .failed_before_commit } };
    try writeFailureReceipt(io, failure_path, @splat(10), @splat(11), @splat(6), "InvalidPayloadProvenance", failed_receipt);
    const failure_bytes = try tmp.dir.readFileAlloc(io, "failure.json", std.testing.allocator, .limited(64 * 1024));
    defer std.testing.allocator.free(failure_bytes);
    var parsed_failure = try std.json.parseFromSlice(InvocationFailureReceipt, std.testing.allocator, failure_bytes, .{});
    defer parsed_failure.deinit();
    try std.testing.expect(!parsed_failure.value.completed);
    try std.testing.expectEqual(.failed_before_commit, parsed_failure.value.installed.terminal.state);
}

test "context baseline preflight shape rejects impossible tracker and identity relations" {
    const identity: profiles.Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    const good = installed.BaselinePreflightReceipt{ .load_generation = 1, .model_identity = identity, .build_sha256 = @splat(6), .prompt_tokens = 8160, .prompt_tokens_sha256 = @splat(7), .session_identity = 9, .source_descriptor_sha256 = @splat(8), .source_payload_sha256 = @splat(9), .native_peak_budget = 1024, .tracker_owner_identity = 10, .tracker_live_bytes = 200, .tracker_peak_bytes = 300, .baseline_context_physical_bytes = 100, .replay_control_bytes = 0, .global_owner_count = 3, .total_owner_count = 36 };
    try validateBaselineReceiptShape(good);
    inline for (.{ "schema", "load", "session", "owner", "live", "peak_order", "peak_budget" }) |mutation| {
        var bad = good;
        if (comptime std.mem.eql(u8, mutation, "schema")) bad.schema_name = "antfly.context.stored-installed-baseline-preflight.invalid";
        if (comptime std.mem.eql(u8, mutation, "load")) bad.load_generation = 0;
        if (comptime std.mem.eql(u8, mutation, "session")) bad.session_identity = 0;
        if (comptime std.mem.eql(u8, mutation, "owner")) bad.tracker_owner_identity = 0;
        if (comptime std.mem.eql(u8, mutation, "live")) bad.tracker_live_bytes = 99;
        if (comptime std.mem.eql(u8, mutation, "peak_order")) bad.tracker_peak_bytes = 199;
        if (comptime std.mem.eql(u8, mutation, "peak_budget")) bad.tracker_peak_bytes = 1025;
        try std.testing.expectError(error.InvalidPayloadProvenance, validateBaselineReceiptShape(bad));
    }
}

test "context baseline envelope rejects valid receipt from another capture" {
    const envelope = BaselineEnvelope{ .capture_input_manifest_sha256 = @splat(12), .source_build_sha256 = @splat(6), .baseline = undefined };
    try validateBaselineCaptureBinding(@splat(12), envelope);
    try std.testing.expectError(error.InvalidPayloadProvenance, validateBaselineCaptureBinding(@splat(11), envelope));
}

test "context committed success receipt writer failure is classified without authority" {
    var receipt = installed.Receipt{ .completed = true, .terminal = .{ .state = .committed } };
    try markCommittedReceiptWriteFailure(&receipt);
    try std.testing.expect(!receipt.completed);
    try std.testing.expectEqual(.committed_but_receipt_failed, receipt.terminal.state);
    try std.testing.expect(receipt.diagnostic_only);
    try std.testing.expect(!receipt.serving_authority);
    try std.testing.expect(!receipt.release_authority);
}

test "context failure receipt writer failure preserves original diagnostic error" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const existing = try tmp.dir.createFile(io, "failure.json", .{ .exclusive = true });
    existing.close(io);
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/failure.json", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const original = error.InvalidPayloadProvenance;
    const receipt = installed.Receipt{ .terminal = .{ .state = .failed_before_commit } };
    try std.testing.expectError(error.InvalidPayloadProvenance, persistFailureOrReturnOriginal(io, path, @splat(1), @splat(2), @splat(3), original, receipt));
}
