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
const sim_fixture = @import("sim_fixture.zig");

pub const Action = union(enum) {
    append,
    append_batch: u8,
    concurrent_pair,
    reopen_and_verify_from: u64,
    truncate_and_verify_from: struct {
        up_to_lsn: u64,
        from_lsn: u64,
    },
    verify_from: u64,
};

pub const Mode = enum {
    replay,
    crash,
};

pub const CommitBackend = enum {
    sync,
    worker_thread,
    async_io,
    adaptive,
};

pub const CommitPhase = enum {
    before_data_sync,
    after_data_sync_before_meta,
    after_meta_write_before_meta_sync,
    fully_published,
};

pub const CrashOutcome = enum {
    previous,
    previous_or_committed,
    committed,
};

pub const Options = struct {
    no_sync: bool = false,
    commit_backend: CommitBackend = .adaptive,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    expected_visible_entries: ?usize = null,
    expected_last_lsn: ?u64 = null,
    expected_outcome: ?CrashOutcome = null,
};

pub const ReplayFixture = struct {
    mode: Mode = .replay,
    opts: Options = .{},
    actions: []Action = &.{},
    prelude_actions: []Action = &.{},
    crash_action: ?Action = null,
    phase: ?CommitPhase = null,
    label: ?[]u8 = null,
    case_label: ?[]u8 = null,
    origin_seed: ?u64 = null,
    expectation_note: ?[]u8 = null,

    pub fn deinit(self: *ReplayFixture, allocator: std.mem.Allocator) void {
        allocator.free(self.actions);
        allocator.free(self.prelude_actions);
        if (self.label) |label| allocator.free(label);
        if (self.case_label) |case_label| allocator.free(case_label);
        if (self.expectation_note) |note| allocator.free(note);
        self.* = undefined;
    }
};

pub fn parseAction(line: []const u8) !Action {
    var tokens: [8][]const u8 = undefined;
    var token_count: usize = 0;
    var token_iter = std.mem.tokenizeAny(u8, line, " \t");
    while (token_iter.next()) |token| {
        if (token_count >= tokens.len) return error.InvalidFixture;
        tokens[token_count] = token;
        token_count += 1;
    }
    const fields = tokens[0..token_count];
    if (fields.len == 0) return error.InvalidFixture;

    if (std.mem.eql(u8, fields[0], "append")) {
        if (fields.len != 1) return error.InvalidFixture;
        return .append;
    }
    if (std.mem.eql(u8, fields[0], "append_batch")) {
        if (fields.len != 2) return error.InvalidFixture;
        return .{ .append_batch = try std.fmt.parseUnsigned(u8, fields[1], 10) };
    }
    if (std.mem.eql(u8, fields[0], "concurrent_pair")) {
        if (fields.len != 1) return error.InvalidFixture;
        return .concurrent_pair;
    }
    if (std.mem.eql(u8, fields[0], "reopen_and_verify_from")) {
        if (fields.len != 2) return error.InvalidFixture;
        return .{ .reopen_and_verify_from = try parseU64(fields[1]) };
    }
    if (std.mem.eql(u8, fields[0], "truncate_and_verify_from")) {
        if (fields.len != 3) return error.InvalidFixture;
        return .{ .truncate_and_verify_from = .{
            .up_to_lsn = try parseU64(fields[1]),
            .from_lsn = try parseU64(fields[2]),
        } };
    }
    if (std.mem.eql(u8, fields[0], "verify_from")) {
        if (fields.len != 2) return error.InvalidFixture;
        return .{ .verify_from = try parseU64(fields[1]) };
    }

    return error.InvalidFixture;
}

pub fn renderAction(allocator: std.mem.Allocator, action: Action) ![]u8 {
    return switch (action) {
        .append => allocator.dupe(u8, "append"),
        .append_batch => |count| std.fmt.allocPrint(allocator, "append_batch {d}", .{count}),
        .concurrent_pair => allocator.dupe(u8, "concurrent_pair"),
        .reopen_and_verify_from => |from_lsn| std.fmt.allocPrint(allocator, "reopen_and_verify_from {d}", .{from_lsn}),
        .truncate_and_verify_from => |payload| std.fmt.allocPrint(allocator, "truncate_and_verify_from {d} {d}", .{ payload.up_to_lsn, payload.from_lsn }),
        .verify_from => |from_lsn| std.fmt.allocPrint(allocator, "verify_from {d}", .{from_lsn}),
    };
}

pub fn parseFixture(allocator: std.mem.Allocator, contents: []const u8) !ReplayFixture {
    var raw_fixture = try sim_fixture.parse(allocator, contents);
    defer raw_fixture.deinit(allocator);

    const mode = raw_fixture.mode orelse return error.InvalidFixture;
    const fixture_mode: Mode = if (std.mem.eql(u8, mode, "wal"))
        .replay
    else if (std.mem.eql(u8, mode, "wal_crash"))
        .crash
    else
        return error.InvalidFixture;

    var fixture = ReplayFixture{
        .mode = fixture_mode,
        .opts = .{},
        .actions = &.{},
        .prelude_actions = &.{},
    };
    var actions: std.ArrayListUnmanaged(Action) = .empty;
    errdefer actions.deinit(allocator);
    var prelude_actions: std.ArrayListUnmanaged(Action) = .empty;
    errdefer prelude_actions.deinit(allocator);

    if (raw_fixture.label) |label| fixture.label = try allocator.dupe(u8, label);
    if (raw_fixture.case_label) |case_label| fixture.case_label = try allocator.dupe(u8, case_label);
    if (raw_fixture.origin_seed) |seed| fixture.origin_seed = try parseU64(seed);
    if (raw_fixture.expectation) |note| fixture.expectation_note = try allocator.dupe(u8, note);
    if (raw_fixture.phase) |phase| fixture.phase = try parseCommitPhase(phase);
    if (raw_fixture.no_sync) |value| fixture.opts.no_sync = std.mem.eql(u8, value, "true");
    if (raw_fixture.commit_backend) |value| fixture.opts.commit_backend = try parseCommitBackend(value);
    if (sim_fixture.extraFieldValue(&raw_fixture, "group_commit_window_ns")) |value| {
        fixture.opts.group_commit_window_ns = try parseU64(value);
    }
    fixture.opts.group_commit_max_requests = (try sim_fixture.parseOptionalUnsignedExtraField(
        usize,
        &raw_fixture,
        "group_commit_max_requests",
    )) orelse fixture.opts.group_commit_max_requests;
    fixture.opts.expected_visible_entries = try sim_fixture.parseOptionalUnsignedExtraField(
        usize,
        &raw_fixture,
        "expected_visible_entries",
    );
    fixture.opts.expected_last_lsn = try sim_fixture.parseOptionalUnsignedExtraField(
        u64,
        &raw_fixture,
        "expected_last_lsn",
    );
    fixture.opts.expected_outcome = try sim_fixture.parseOptionalEnumTagExtraField(
        CrashOutcome,
        &raw_fixture,
        "expected_outcome",
    );

    switch (fixture.mode) {
        .replay => {
            for (raw_fixture.actions.items) |line| {
                try actions.append(allocator, try parseAction(line));
            }
        },
        .crash => {
            for (raw_fixture.actions.items) |line| {
                try prelude_actions.append(allocator, try parseAction(line));
            }
            if (raw_fixture.crash_action) |line| {
                fixture.crash_action = try parseAction(line);
            }
        },
    }

    fixture.actions = try actions.toOwnedSlice(allocator);
    fixture.prelude_actions = try prelude_actions.toOwnedSlice(allocator);
    return fixture;
}

pub fn renderReplayArtifact(
    allocator: std.mem.Allocator,
    opts_with_expectations: Options,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    expected_visible_entries: usize,
    expected_last_lsn: u64,
    actions: []const Action,
) ![]u8 {
    var opts = opts_with_expectations;
    opts.expected_visible_entries = expected_visible_entries;
    opts.expected_last_lsn = expected_last_lsn;
    var fixture = try initArtifactFixture(allocator, "wal", opts, case_label, seed, expectation_note);
    defer fixture.deinit(allocator);
    for (actions) |action| {
        try fixture.actions.append(allocator, try renderAction(allocator, action));
    }
    return sim_fixture.render(allocator, &fixture);
}

pub fn renderCrashArtifact(
    allocator: std.mem.Allocator,
    opts_with_expectations: Options,
    case_label: []const u8,
    seed: u64,
    phase: CommitPhase,
    expectation_note: []const u8,
    expected_outcome: CrashOutcome,
    prelude_actions: []const Action,
    crash_action: Action,
) ![]u8 {
    var opts = opts_with_expectations;
    opts.expected_outcome = expected_outcome;
    var fixture = try initArtifactFixture(allocator, "wal_crash", opts, case_label, seed, expectation_note);
    defer fixture.deinit(allocator);
    fixture.phase = try allocator.dupe(u8, @tagName(phase));
    for (prelude_actions) |action| {
        try fixture.actions.append(allocator, try renderAction(allocator, action));
    }
    fixture.crash_action = try renderAction(allocator, crash_action);
    return sim_fixture.render(allocator, &fixture);
}

fn initArtifactFixture(
    allocator: std.mem.Allocator,
    mode: []const u8,
    opts: Options,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
) !sim_fixture.Fixture {
    var fixture: sim_fixture.Fixture = .{
        .mode = try allocator.dupe(u8, mode),
        .label = try allocator.dupe(u8, case_label),
        .case_label = try allocator.dupe(u8, case_label),
        .origin_seed = try std.fmt.allocPrint(allocator, "0x{x}", .{seed}),
        .expectation = try allocator.dupe(u8, expectation_note),
        .no_sync = try allocator.dupe(u8, if (opts.no_sync) "true" else "false"),
        .commit_backend = try allocator.dupe(u8, @tagName(opts.commit_backend)),
    };
    errdefer fixture.deinit(allocator);

    try sim_fixture.appendOptionalUnsignedExtraField(
        allocator,
        &fixture,
        "group_commit_window_ns",
        if (opts.group_commit_window_ns == 0) null else opts.group_commit_window_ns,
    );
    try sim_fixture.appendOptionalUnsignedExtraField(
        allocator,
        &fixture,
        "group_commit_max_requests",
        if (opts.group_commit_max_requests == 64) null else opts.group_commit_max_requests,
    );
    try sim_fixture.appendOptionalUnsignedExtraField(
        allocator,
        &fixture,
        "expected_visible_entries",
        opts.expected_visible_entries,
    );
    try sim_fixture.appendOptionalUnsignedExtraField(
        allocator,
        &fixture,
        "expected_last_lsn",
        opts.expected_last_lsn,
    );
    try sim_fixture.appendOptionalEnumTagExtraField(allocator, &fixture, "expected_outcome", opts.expected_outcome);
    return fixture;
}

fn parseCommitBackend(value: []const u8) !CommitBackend {
    if (std.mem.eql(u8, value, "sync")) return .sync;
    if (std.mem.eql(u8, value, "worker_thread")) return .worker_thread;
    if (std.mem.eql(u8, value, "async_io")) return .async_io;
    if (std.mem.eql(u8, value, "adaptive")) return .adaptive;
    return error.InvalidFixture;
}

fn parseCommitPhase(value: []const u8) !CommitPhase {
    if (std.mem.eql(u8, value, "before_data_sync")) return .before_data_sync;
    if (std.mem.eql(u8, value, "after_data_sync_before_meta")) return .after_data_sync_before_meta;
    if (std.mem.eql(u8, value, "after_meta_write_before_meta_sync")) return .after_meta_write_before_meta_sync;
    if (std.mem.eql(u8, value, "fully_published")) return .fully_published;
    return error.InvalidFixture;
}

fn parseU64(value: []const u8) !u64 {
    if (std.mem.startsWith(u8, value, "0x")) return std.fmt.parseUnsigned(u64, value[2..], 16);
    return std.fmt.parseUnsigned(u64, value, 10);
}
