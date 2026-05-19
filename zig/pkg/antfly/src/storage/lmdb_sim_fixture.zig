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

pub const DifferentialAction = union(enum) {
    put_main: struct { key_index: u8, value_index: u16 },
    delete_main: struct { key_index: u8 },
    put_docs: struct { key_index: u8, value_index: u16 },
    delete_docs: struct { key_index: u8 },
    put_dup: struct { key_index: u8, value_index: u16 },
    delete_dup_value: struct { key_index: u8, value_index: u16 },
};

pub const ScheduledAction = union(enum) {
    direct: DifferentialAction,
    nested_commit: struct { parent: DifferentialAction, child: DifferentialAction },
    nested_abort: struct { parent: DifferentialAction, child: DifferentialAction },
    reader_then_direct: DifferentialAction,
};

pub const Mode = enum {
    differential,
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
    max_dbs: u32 = 4,
    write_map: bool = false,
    map_async: bool = false,
    fixed_map: bool = false,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    commit_backend: CommitBackend = .sync,
    expected_main_count: ?usize = null,
    expected_docs_count: ?usize = null,
    expected_dups_count: ?usize = null,
    expected_outcome: ?CrashOutcome = null,
};

pub const ReplayFixture = struct {
    mode: Mode = .differential,
    opts: Options = .{},
    actions: []ScheduledAction = &.{},
    prelude_actions: []DifferentialAction = &.{},
    crash_action: ?DifferentialAction = null,
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

pub fn parseFixture(allocator: std.mem.Allocator, contents: []const u8) !ReplayFixture {
    var raw_fixture = try sim_fixture.parse(allocator, contents);
    defer raw_fixture.deinit(allocator);

    var fixture = ReplayFixture{
        .mode = blk: {
            const mode = raw_fixture.mode orelse return error.InvalidFixture;
            if (std.mem.eql(u8, mode, "differential")) break :blk .differential;
            if (std.mem.eql(u8, mode, "crash")) break :blk .crash;
            return error.InvalidFixture;
        },
        .opts = .{},
        .actions = &.{},
        .prelude_actions = &.{},
    };
    var actions: std.ArrayListUnmanaged(ScheduledAction) = .empty;
    errdefer actions.deinit(allocator);
    var prelude_actions: std.ArrayListUnmanaged(DifferentialAction) = .empty;
    errdefer prelude_actions.deinit(allocator);

    if (raw_fixture.label) |label| fixture.label = try allocator.dupe(u8, label);
    if (raw_fixture.case_label) |case_label| fixture.case_label = try allocator.dupe(u8, case_label);
    if (raw_fixture.origin_seed) |seed| fixture.origin_seed = try parseSeed(seed);
    if (raw_fixture.expectation) |note| fixture.expectation_note = try allocator.dupe(u8, note);
    if (raw_fixture.phase) |phase| fixture.phase = try parseCommitPhase(phase);

    try applyFixtureOptions(&fixture.opts, &raw_fixture);

    switch (fixture.mode) {
        .differential => {
            for (raw_fixture.actions.items) |line| {
                try actions.append(allocator, try parseScheduledActionLine(line));
            }
        },
        .crash => {
            for (raw_fixture.actions.items) |line| {
                try prelude_actions.append(allocator, try parseDifferentialActionLine(line));
            }
            if (raw_fixture.crash_action) |line| {
                fixture.crash_action = try parseDifferentialActionLine(line);
            }
        },
    }

    fixture.actions = try actions.toOwnedSlice(allocator);
    fixture.prelude_actions = try prelude_actions.toOwnedSlice(allocator);
    return fixture;
}

pub fn renderDifferentialArtifact(
    allocator: std.mem.Allocator,
    opts: Options,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    actions: []const ScheduledAction,
) ![]u8 {
    var fixture = try initArtifactFixture(allocator, "differential", opts, case_label, seed, expectation_note);
    defer fixture.deinit(allocator);
    for (actions) |action| {
        try fixture.actions.append(allocator, try renderScheduledActionLine(allocator, action));
    }
    return sim_fixture.render(allocator, &fixture);
}

pub fn renderCrashArtifact(
    allocator: std.mem.Allocator,
    opts: Options,
    case_label: []const u8,
    seed: u64,
    phase: CommitPhase,
    expectation_note: []const u8,
    prelude_actions: []const DifferentialAction,
    crash_action: DifferentialAction,
) ![]u8 {
    var fixture = try initArtifactFixture(allocator, "crash", opts, case_label, seed, expectation_note);
    defer fixture.deinit(allocator);
    fixture.phase = try allocator.dupe(u8, @tagName(phase));
    for (prelude_actions) |action| {
        try fixture.actions.append(allocator, try renderDifferentialActionLine(allocator, action));
    }
    fixture.crash_action = try renderDifferentialActionLine(allocator, crash_action);
    return sim_fixture.render(allocator, &fixture);
}

fn applyFixtureOptions(opts: *Options, raw_fixture: *const sim_fixture.Fixture) !void {
    if (raw_fixture.max_dbs) |value| opts.max_dbs = try std.fmt.parseUnsigned(u32, value, 10);
    if (raw_fixture.write_map) |value| opts.write_map = try parseBool(value);
    if (raw_fixture.map_async) |value| opts.map_async = try parseBool(value);
    if (raw_fixture.fixed_map) |value| opts.fixed_map = try parseBool(value);
    if (raw_fixture.no_sync) |value| opts.no_sync = try parseBool(value);
    if (raw_fixture.no_meta_sync) |value| opts.no_meta_sync = try parseBool(value);
    if (raw_fixture.commit_backend) |value| opts.commit_backend = try parseCommitBackend(value);
    opts.expected_main_count = try sim_fixture.parseOptionalUnsignedExtraField(usize, raw_fixture, "expected_main_count");
    opts.expected_docs_count = try sim_fixture.parseOptionalUnsignedExtraField(usize, raw_fixture, "expected_docs_count");
    opts.expected_dups_count = try sim_fixture.parseOptionalUnsignedExtraField(usize, raw_fixture, "expected_dups_count");
    opts.expected_outcome = try sim_fixture.parseOptionalEnumTagExtraField(CrashOutcome, raw_fixture, "expected_outcome");
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
        .max_dbs = try std.fmt.allocPrint(allocator, "{d}", .{opts.max_dbs}),
        .write_map = try allocator.dupe(u8, boolWord(opts.write_map)),
        .map_async = try allocator.dupe(u8, boolWord(opts.map_async)),
        .fixed_map = try allocator.dupe(u8, boolWord(opts.fixed_map)),
        .no_sync = try allocator.dupe(u8, boolWord(opts.no_sync)),
        .no_meta_sync = try allocator.dupe(u8, boolWord(opts.no_meta_sync)),
        .commit_backend = try allocator.dupe(u8, @tagName(opts.commit_backend)),
    };
    errdefer fixture.deinit(allocator);

    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_main_count", opts.expected_main_count);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_docs_count", opts.expected_docs_count);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dups_count", opts.expected_dups_count);
    try sim_fixture.appendOptionalEnumTagExtraField(allocator, &fixture, "expected_outcome", opts.expected_outcome);
    return fixture;
}

fn parseScheduledActionLine(line: []const u8) !ScheduledAction {
    var tokens: [16][]const u8 = undefined;
    const fields = try fixtureLineFields(&tokens, line);
    return parseScheduledActionFields(fields);
}

fn parseDifferentialActionLine(line: []const u8) !DifferentialAction {
    var tokens: [16][]const u8 = undefined;
    const fields = try fixtureLineFields(&tokens, line);
    return parseDifferentialActionFields(fields);
}

fn fixtureLineFields(tokens: *[16][]const u8, line: []const u8) ![]const []const u8 {
    var token_count: usize = 0;
    var token_iter = std.mem.tokenizeAny(u8, line, " \t");
    while (token_iter.next()) |token| {
        if (token_count >= tokens.len) return error.InvalidFixture;
        tokens[token_count] = token;
        token_count += 1;
    }
    return tokens[0..token_count];
}

fn parseScheduledActionFields(fields: []const []const u8) !ScheduledAction {
    if (fields.len == 0) return error.InvalidFixture;

    if (std.mem.eql(u8, fields[0], "direct")) {
        return .{ .direct = try parseDifferentialActionFields(fields[1..]) };
    }
    if (std.mem.eql(u8, fields[0], "reader_then_direct")) {
        return .{ .reader_then_direct = try parseDifferentialActionFields(fields[1..]) };
    }
    if (std.mem.eql(u8, fields[0], "nested_commit")) {
        const split_at = try differentialActionFieldCount(fields[1..]);
        return .{ .nested_commit = .{
            .parent = try parseDifferentialActionFields(fields[1 .. 1 + split_at]),
            .child = try parseDifferentialActionFields(fields[1 + split_at ..]),
        } };
    }
    if (std.mem.eql(u8, fields[0], "nested_abort")) {
        const split_at = try differentialActionFieldCount(fields[1..]);
        return .{ .nested_abort = .{
            .parent = try parseDifferentialActionFields(fields[1 .. 1 + split_at]),
            .child = try parseDifferentialActionFields(fields[1 + split_at ..]),
        } };
    }

    return error.InvalidFixture;
}

fn differentialActionFieldCount(fields: []const []const u8) !usize {
    if (fields.len == 0) return error.InvalidFixture;
    if (std.mem.eql(u8, fields[0], "delete_main") or
        std.mem.eql(u8, fields[0], "delete_docs"))
    {
        if (fields.len < 2) return error.InvalidFixture;
        return 2;
    }
    if (std.mem.eql(u8, fields[0], "put_main") or
        std.mem.eql(u8, fields[0], "put_docs") or
        std.mem.eql(u8, fields[0], "put_dup") or
        std.mem.eql(u8, fields[0], "delete_dup_value"))
    {
        if (fields.len < 3) return error.InvalidFixture;
        return 3;
    }
    return error.InvalidFixture;
}

fn parseDifferentialActionFields(fields: []const []const u8) !DifferentialAction {
    if (fields.len == 0) return error.InvalidFixture;

    if (std.mem.eql(u8, fields[0], "put_main")) {
        if (fields.len != 3) return error.InvalidFixture;
        return .{ .put_main = .{
            .key_index = try std.fmt.parseUnsigned(u8, fields[1], 10),
            .value_index = try std.fmt.parseUnsigned(u16, fields[2], 10),
        } };
    }
    if (std.mem.eql(u8, fields[0], "delete_main")) {
        if (fields.len != 2) return error.InvalidFixture;
        return .{ .delete_main = .{ .key_index = try std.fmt.parseUnsigned(u8, fields[1], 10) } };
    }
    if (std.mem.eql(u8, fields[0], "put_docs")) {
        if (fields.len != 3) return error.InvalidFixture;
        return .{ .put_docs = .{
            .key_index = try std.fmt.parseUnsigned(u8, fields[1], 10),
            .value_index = try std.fmt.parseUnsigned(u16, fields[2], 10),
        } };
    }
    if (std.mem.eql(u8, fields[0], "delete_docs")) {
        if (fields.len != 2) return error.InvalidFixture;
        return .{ .delete_docs = .{ .key_index = try std.fmt.parseUnsigned(u8, fields[1], 10) } };
    }
    if (std.mem.eql(u8, fields[0], "put_dup")) {
        if (fields.len != 3) return error.InvalidFixture;
        return .{ .put_dup = .{
            .key_index = try std.fmt.parseUnsigned(u8, fields[1], 10),
            .value_index = try std.fmt.parseUnsigned(u16, fields[2], 10),
        } };
    }
    if (std.mem.eql(u8, fields[0], "delete_dup_value")) {
        if (fields.len != 3) return error.InvalidFixture;
        return .{ .delete_dup_value = .{
            .key_index = try std.fmt.parseUnsigned(u8, fields[1], 10),
            .value_index = try std.fmt.parseUnsigned(u16, fields[2], 10),
        } };
    }

    return error.InvalidFixture;
}

fn renderScheduledActionLine(allocator: std.mem.Allocator, action: ScheduledAction) ![]u8 {
    return switch (action) {
        .direct => |direct_action| blk: {
            const rendered = try renderDifferentialActionLine(allocator, direct_action);
            defer allocator.free(rendered);
            break :blk std.fmt.allocPrint(allocator, "direct {s}", .{rendered});
        },
        .reader_then_direct => |direct_action| blk: {
            const rendered = try renderDifferentialActionLine(allocator, direct_action);
            defer allocator.free(rendered);
            break :blk std.fmt.allocPrint(allocator, "reader_then_direct {s}", .{rendered});
        },
        .nested_commit => |nested| blk: {
            const parent = try renderDifferentialActionLine(allocator, nested.parent);
            defer allocator.free(parent);
            const child = try renderDifferentialActionLine(allocator, nested.child);
            defer allocator.free(child);
            break :blk std.fmt.allocPrint(allocator, "nested_commit {s} {s}", .{ parent, child });
        },
        .nested_abort => |nested| blk: {
            const parent = try renderDifferentialActionLine(allocator, nested.parent);
            defer allocator.free(parent);
            const child = try renderDifferentialActionLine(allocator, nested.child);
            defer allocator.free(child);
            break :blk std.fmt.allocPrint(allocator, "nested_abort {s} {s}", .{ parent, child });
        },
    };
}

fn renderDifferentialActionLine(allocator: std.mem.Allocator, action: DifferentialAction) ![]u8 {
    return switch (action) {
        .put_main => |payload| std.fmt.allocPrint(allocator, "put_main {d} {d}", .{ payload.key_index, payload.value_index }),
        .delete_main => |payload| std.fmt.allocPrint(allocator, "delete_main {d}", .{payload.key_index}),
        .put_docs => |payload| std.fmt.allocPrint(allocator, "put_docs {d} {d}", .{ payload.key_index, payload.value_index }),
        .delete_docs => |payload| std.fmt.allocPrint(allocator, "delete_docs {d}", .{payload.key_index}),
        .put_dup => |payload| std.fmt.allocPrint(allocator, "put_dup {d} {d}", .{ payload.key_index, payload.value_index }),
        .delete_dup_value => |payload| std.fmt.allocPrint(allocator, "delete_dup_value {d} {d}", .{ payload.key_index, payload.value_index }),
    };
}

fn boolWord(value: bool) []const u8 {
    return if (value) "true" else "false";
}

fn parseBool(value: []const u8) !bool {
    if (std.mem.eql(u8, value, "true")) return true;
    if (std.mem.eql(u8, value, "false")) return false;
    return error.InvalidFixture;
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

fn parseCrashOutcome(value: []const u8) !CrashOutcome {
    if (std.mem.eql(u8, value, "previous")) return .previous;
    if (std.mem.eql(u8, value, "previous_or_committed")) return .previous_or_committed;
    if (std.mem.eql(u8, value, "committed")) return .committed;
    return error.InvalidFixture;
}

fn parseSeed(value: []const u8) !u64 {
    if (std.mem.startsWith(u8, value, "0x")) return try std.fmt.parseUnsigned(u64, value[2..], 16);
    return try std.fmt.parseUnsigned(u64, value, 10);
}
