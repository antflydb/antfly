// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Fail-closed determinism audit for replayable scenario adapters.
//!
//! Runtime behavior-affecting randomness has two admitted sources: immediate
//! structured choices recorded in the trace and entropy requested from the
//! world's borrowed std.Io. This module also provides the source gate that
//! rejects common host-only escape hatches in replayable adapters. A narrow
//! differential boundary requires a line-local, reviewed allowance with a
//! non-empty reason: `vopr-audit: allow(category) reason`.

const std = @import("std");
const choice = @import("choice.zig");
const trace = @import("trace.zig");

pub const Category = enum {
    host_entropy,
    delayed_private_prng,
    host_clock,
    native_thread_or_io,
    host_filesystem,
    native_library,
    unordered_iteration,
    unstable_identity,
};

pub const Finding = struct {
    category: Category,
    line: usize,
    column: usize,
    token: []const u8,
};

const Pattern = struct {
    category: Category,
    token: []const u8,
};

const patterns = [_]Pattern{
    .{ .category = .host_entropy, .token = "std.crypto.random" },
    .{ .category = .host_entropy, .token = "std.posix.getrandom" },
    .{ .category = .host_entropy, .token = "io_impl.io().random(" },
    .{ .category = .delayed_private_prng, .token = "std.Random.DefaultPrng" },
    .{ .category = .delayed_private_prng, .token = "std.Random.Xoshiro" },
    .{ .category = .host_clock, .token = "std.time.nanoTimestamp" },
    .{ .category = .host_clock, .token = "std.time.milliTimestamp" },
    .{ .category = .host_clock, .token = "std.time.timestamp" },
    .{ .category = .host_clock, .token = "platform_time" },
    .{ .category = .host_clock, .token = "std.posix.clock_gettime" },
    .{ .category = .native_thread_or_io, .token = "std.Thread" },
    .{ .category = .native_thread_or_io, .token = "std.Io.Threaded" },
    .{ .category = .host_filesystem, .token = "std.testing.tmpDir" },
    .{ .category = .host_filesystem, .token = "std.testing.io" },
    .{ .category = .host_filesystem, .token = "std.Options.debug_io" },
    .{ .category = .host_filesystem, .token = "std.posix.open" },
    .{ .category = .host_filesystem, .token = "std.posix.mkdir" },
    .{ .category = .host_filesystem, .token = "std.posix.fsync" },
    .{ .category = .native_library, .token = "std.DynLib" },
    .{ .category = .native_library, .token = "@cImport" },
    .{ .category = .native_library, .token = "@extern" },
    .{ .category = .unordered_iteration, .token = ".iterator()" },
    .{ .category = .unstable_identity, .token = "@intFromPtr" },
};

pub const Evidence = struct {
    structured_choices: u64,
    structured_choice_sites: u64,
    std_io_entropy_calls: u64,
};

/// Combines the trace's immediate-choice proof with the deterministic std.Io
/// entropy counter. Host entropy is intentionally not accepted as evidence;
/// replayable source manifests must pass `firstFinding` separately.
pub fn auditEvidence(history: *const trace.Trace, std_io_entropy_calls: u64) !Evidence {
    const audited = try choice.auditTrace(history);
    return .{
        .structured_choices = audited.choices,
        .structured_choice_sites = audited.distinct_sites,
        .std_io_entropy_calls = std_io_entropy_calls,
    };
}

pub fn firstFinding(source: []const u8) ?Finding {
    var lines = std.mem.splitScalar(u8, source, '\n');
    var line_number: usize = 0;
    var previous_line: []const u8 = "";
    while (lines.next()) |line| {
        line_number += 1;
        for (patterns) |pattern| {
            const column = std.mem.indexOf(u8, line, pattern.token) orelse continue;
            if (hasAllowance(line, pattern.category) or hasAllowance(previous_line, pattern.category)) continue;
            return .{
                .category = pattern.category,
                .line = line_number,
                .column = column + 1,
                .token = pattern.token,
            };
        }
        previous_line = line;
    }
    return null;
}

pub fn auditSource(source: []const u8) !void {
    if (firstFinding(source) != null) return error.UncontrolledDeterminismSource;
}

fn hasAllowance(line: []const u8, category: Category) bool {
    const marker = "vopr-audit: allow(";
    const start = std.mem.indexOf(u8, line, marker) orelse return false;
    const category_name = @tagName(category);
    const category_start = start + marker.len;
    if (!std.mem.startsWith(u8, line[category_start..], category_name)) return false;
    const close = category_start + category_name.len;
    if (close >= line.len or line[close] != ')') return false;
    // A suppression without a reviewable rationale is itself invalid.
    return std.mem.trim(u8, line[close + 1 ..], " \t/\r").len != 0;
}

test "determinism source audit rejects host entropy clocks identity and unordered iteration" {
    const cases = [_]struct { source: []const u8, category: Category }{
        .{ .source = "const x = std.crypto.random.int(u64);", .category = .host_entropy },
        .{ .source = "var p = std.Random.DefaultPrng.init(seed);", .category = .delayed_private_prng },
        .{ .source = "const now = std.time.nanoTimestamp();", .category = .host_clock },
        .{ .source = "var it = values.iterator();", .category = .unordered_iteration },
        .{ .source = "const id = @intFromPtr(value);", .category = .unstable_identity },
    };
    for (cases) |case| {
        const finding = firstFinding(case.source).?;
        try std.testing.expectEqual(case.category, finding.category);
        try std.testing.expectEqual(@as(usize, 1), finding.line);
    }
}

test "determinism source audit requires a reason for narrow differential allowances" {
    try std.testing.expectError(error.UncontrolledDeterminismSource, auditSource(
        "// vopr-audit: allow(host_filesystem)\nconst io = std.testing.io;",
    ));
    try auditSource(
        "// vopr-audit: allow(host_filesystem) physical backend is differential only\nconst io = std.testing.io;",
    );
    try auditSource(
        "const io = std.testing.io; // vopr-audit: allow(host_filesystem) fail-closed TLS construction never starts host I/O",
    );
}

test "determinism evidence accepts only immediate choices and borrowed IO entropy" {
    const ids = @import("id.zig");
    const observation = @import("observation.zig");
    var history = try trace.Trace.init(std.testing.allocator, .{ .scenario = "determinism-evidence", .scenario_version = 1 }, .{ .transition_budget = 1 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    try history.addChoice(.{
        .site_id = ids.stable("choice", "determinism-evidence.transition"),
        .site_name = "determinism-evidence.transition",
        .occurrence = 0,
        .enabled_ids = &.{7},
        .selected_id = 7,
    });
    try history.addTransition(.{ .index = 1, .id = 7, .name = "typed", .kind = .workload });
    try history.addObservation(.{ .index = 1, .digest = digest, .features = &.{} });
    history.summary = .{ .transitions = 1, .final_observation_digest = digest, .property_failures = 0 };
    const evidence = try auditEvidence(&history, 3);
    try std.testing.expectEqual(@as(u64, 1), evidence.structured_choices);
    try std.testing.expectEqual(@as(u64, 1), evidence.structured_choice_sites);
    try std.testing.expectEqual(@as(u64, 3), evidence.std_io_entropy_calls);
}
