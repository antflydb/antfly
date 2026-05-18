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
const sim_fixture = @import("../../sim_fixture.zig");

pub const DocSpec = enum {
    left_alpha,
    left_gamma,
    right_beta,
    mixed_alpha_beta,
};

pub const Action = union(enum) {
    add_doc: DocSpec,
    reopen,
    split_handoff,
};

pub const Mode = enum {
    replay,
    crash,
};

pub const CrashOutcome = enum {
    committed,
};

pub const Options = struct {
    expected_source_doc_count: ?u32 = null,
    expected_dest_doc_count: ?u32 = null,
    expected_source_alpha_hits: ?u32 = null,
    expected_source_beta_hits: ?u32 = null,
    expected_source_gamma_hits: ?u32 = null,
    expected_dest_alpha_hits: ?u32 = null,
    expected_dest_beta_hits: ?u32 = null,
    expected_dest_gamma_hits: ?u32 = null,
    expected_outcome: ?CrashOutcome = null,
};

pub const ReplayFixture = struct {
    mode: Mode = .replay,
    opts: Options = .{},
    actions: []Action = &.{},
    prelude_actions: []Action = &.{},
    crash_action: ?Action = null,
    phase: ?[]u8 = null,
    label: ?[]u8 = null,
    case_label: ?[]u8 = null,
    origin_seed: ?u64 = null,
    expectation_note: ?[]u8 = null,

    pub fn deinit(self: *ReplayFixture, allocator: std.mem.Allocator) void {
        allocator.free(self.actions);
        allocator.free(self.prelude_actions);
        if (self.phase) |phase| allocator.free(phase);
        if (self.label) |label| allocator.free(label);
        if (self.case_label) |case_label| allocator.free(case_label);
        if (self.expectation_note) |note| allocator.free(note);
        self.* = undefined;
    }
};

pub fn parseFixture(allocator: std.mem.Allocator, contents: []const u8) !ReplayFixture {
    var raw_fixture = try sim_fixture.parse(allocator, contents);
    defer raw_fixture.deinit(allocator);

    const mode = raw_fixture.mode orelse return error.InvalidFixture;
    const fixture_mode: Mode = if (std.mem.eql(u8, mode, "index_manager"))
        .replay
    else if (std.mem.eql(u8, mode, "index_manager_crash"))
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

    if (raw_fixture.phase) |phase| fixture.phase = try allocator.dupe(u8, phase);
    if (raw_fixture.label) |label| fixture.label = try allocator.dupe(u8, label);
    if (raw_fixture.case_label) |case_label| fixture.case_label = try allocator.dupe(u8, case_label);
    if (raw_fixture.origin_seed) |seed| fixture.origin_seed = try parseU64(seed);
    if (raw_fixture.expectation) |note| fixture.expectation_note = try allocator.dupe(u8, note);

    fixture.opts.expected_source_doc_count = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_source_doc_count");
    fixture.opts.expected_dest_doc_count = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_dest_doc_count");
    fixture.opts.expected_source_alpha_hits = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_source_alpha_hits");
    fixture.opts.expected_source_beta_hits = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_source_beta_hits");
    fixture.opts.expected_source_gamma_hits = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_source_gamma_hits");
    fixture.opts.expected_dest_alpha_hits = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_dest_alpha_hits");
    fixture.opts.expected_dest_beta_hits = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_dest_beta_hits");
    fixture.opts.expected_dest_gamma_hits = try sim_fixture.parseOptionalUnsignedExtraField(u32, &raw_fixture, "expected_dest_gamma_hits");
    fixture.opts.expected_outcome = try sim_fixture.parseOptionalEnumTagExtraField(CrashOutcome, &raw_fixture, "expected_outcome");

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
    opts: Options,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    actions: []const Action,
) ![]u8 {
    var fixture = try initArtifactFixture(allocator, "index_manager", opts, case_label, seed, expectation_note);
    defer fixture.deinit(allocator);
    for (actions) |action| {
        try fixture.actions.append(allocator, try renderAction(allocator, action));
    }
    return sim_fixture.render(allocator, &fixture);
}

pub fn renderCrashArtifact(
    allocator: std.mem.Allocator,
    opts: Options,
    case_label: []const u8,
    seed: u64,
    phase: []const u8,
    expectation_note: []const u8,
    prelude_actions: []const Action,
    crash_action: Action,
) ![]u8 {
    var fixture = try initArtifactFixture(allocator, "index_manager_crash", opts, case_label, seed, expectation_note);
    defer fixture.deinit(allocator);
    fixture.phase = try allocator.dupe(u8, phase);
    for (prelude_actions) |action| {
        try fixture.actions.append(allocator, try renderAction(allocator, action));
    }
    fixture.crash_action = try renderAction(allocator, crash_action);
    return sim_fixture.render(allocator, &fixture);
}

pub fn parseAction(line: []const u8) !Action {
    var tokens: [4][]const u8 = undefined;
    var token_count: usize = 0;
    var token_iter = std.mem.tokenizeAny(u8, line, " \t");
    while (token_iter.next()) |token| {
        if (token_count >= tokens.len) return error.InvalidFixture;
        tokens[token_count] = token;
        token_count += 1;
    }
    const fields = tokens[0..token_count];
    if (fields.len == 0) return error.InvalidFixture;

    if (std.mem.eql(u8, fields[0], "reopen")) {
        if (fields.len != 1) return error.InvalidFixture;
        return .reopen;
    }
    if (std.mem.eql(u8, fields[0], "split_handoff")) {
        if (fields.len != 1) return error.InvalidFixture;
        return .split_handoff;
    }
    if (std.mem.eql(u8, fields[0], "add_doc")) {
        if (fields.len != 2) return error.InvalidFixture;
        return .{ .add_doc = std.meta.stringToEnum(DocSpec, fields[1]) orelse return error.InvalidFixture };
    }
    return error.InvalidFixture;
}

pub fn renderAction(allocator: std.mem.Allocator, action: Action) ![]u8 {
    return switch (action) {
        .reopen => allocator.dupe(u8, "reopen"),
        .split_handoff => allocator.dupe(u8, "split_handoff"),
        .add_doc => |spec| std.fmt.allocPrint(allocator, "add_doc {s}", .{@tagName(spec)}),
    };
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
    };
    errdefer fixture.deinit(allocator);

    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_doc_count", opts.expected_source_doc_count);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_doc_count", opts.expected_dest_doc_count);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_alpha_hits", opts.expected_source_alpha_hits);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_beta_hits", opts.expected_source_beta_hits);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_gamma_hits", opts.expected_source_gamma_hits);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_alpha_hits", opts.expected_dest_alpha_hits);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_beta_hits", opts.expected_dest_beta_hits);
    try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_gamma_hits", opts.expected_dest_gamma_hits);
    try sim_fixture.appendOptionalEnumTagExtraField(allocator, &fixture, "expected_outcome", opts.expected_outcome);
    return fixture;
}

fn parseU64(value: []const u8) !u64 {
    if (std.mem.startsWith(u8, value, "0x")) return std.fmt.parseUnsigned(u64, value[2..], 16);
    return std.fmt.parseUnsigned(u64, value, 10);
}
