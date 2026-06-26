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

const db_mod = @import("mod.zig");
const db_internal = @import("internal.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const platform_time = @import("../../platform/time.zig");
const sim_fixture = @import("../sim_fixture.zig");
const storage_sim = @import("../sim_runtime.zig");
const TestHelpers = @import("test_support.zig");
const types = @import("types.zig");
const zig_lmdb = @import("lmdb_engine");

const Allocator = std.mem.Allocator;
const DB = db_mod.DB;
const OpenOptions = db_mod.OpenOptions;

var split_replay_artifact_nonce: u64 = 0;

const db_split_sim_fixture = struct {
    pub const DocSpec = enum {
        left_alpha,
        left_gamma,
        right_beta,
        mixed_alpha_beta,
    };

    pub const Action = union(enum) {
        add_doc: DocSpec,
        reopen_source,
        reopen_dest,
        split_full,
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
    };

    pub const ReplayFixture = struct {
        opts: Options = .{},
        actions: []Action = &.{},
        label: ?[]u8 = null,
        case_label: ?[]u8 = null,
        origin_seed: ?u64 = null,
        expectation_note: ?[]u8 = null,

        pub fn deinit(self: *ReplayFixture, allocator: std.mem.Allocator) void {
            allocator.free(self.actions);
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
        if (!std.mem.eql(u8, mode, "db_split")) return error.InvalidFixture;

        var fixture = ReplayFixture{};
        var actions: std.ArrayListUnmanaged(Action) = .empty;
        errdefer actions.deinit(allocator);

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

        for (raw_fixture.actions.items) |line| {
            try actions.append(allocator, try parseAction(line));
        }
        fixture.actions = try actions.toOwnedSlice(allocator);
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
        var fixture: sim_fixture.Fixture = .{
            .mode = try allocator.dupe(u8, "db_split"),
            .label = try allocator.dupe(u8, case_label),
            .case_label = try allocator.dupe(u8, case_label),
            .origin_seed = try std.fmt.allocPrint(allocator, "0x{x}", .{seed}),
            .expectation = try allocator.dupe(u8, expectation_note),
        };
        defer fixture.deinit(allocator);

        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_doc_count", opts.expected_source_doc_count);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_doc_count", opts.expected_dest_doc_count);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_alpha_hits", opts.expected_source_alpha_hits);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_beta_hits", opts.expected_source_beta_hits);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_source_gamma_hits", opts.expected_source_gamma_hits);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_alpha_hits", opts.expected_dest_alpha_hits);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_beta_hits", opts.expected_dest_beta_hits);
        try sim_fixture.appendOptionalUnsignedExtraField(allocator, &fixture, "expected_dest_gamma_hits", opts.expected_dest_gamma_hits);

        for (actions) |action| {
            try fixture.actions.append(allocator, try renderAction(allocator, action));
        }
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

        if (std.mem.eql(u8, fields[0], "reopen_source")) {
            if (fields.len != 1) return error.InvalidFixture;
            return .reopen_source;
        }
        if (std.mem.eql(u8, fields[0], "reopen_dest")) {
            if (fields.len != 1) return error.InvalidFixture;
            return .reopen_dest;
        }
        if (std.mem.eql(u8, fields[0], "split_full")) {
            if (fields.len != 1) return error.InvalidFixture;
            return .split_full;
        }
        if (std.mem.eql(u8, fields[0], "add_doc")) {
            if (fields.len != 2) return error.InvalidFixture;
            return .{ .add_doc = std.meta.stringToEnum(DocSpec, fields[1]) orelse return error.InvalidFixture };
        }
        return error.InvalidFixture;
    }

    pub fn renderAction(allocator: std.mem.Allocator, action: Action) ![]u8 {
        return switch (action) {
            .reopen_source => allocator.dupe(u8, "reopen_source"),
            .reopen_dest => allocator.dupe(u8, "reopen_dest"),
            .split_full => allocator.dupe(u8, "split_full"),
            .add_doc => |spec| std.fmt.allocPrint(allocator, "add_doc {s}", .{@tagName(spec)}),
        };
    }

    fn parseU64(value: []const u8) !u64 {
        if (std.mem.startsWith(u8, value, "0x")) return std.fmt.parseUnsigned(u64, value[2..], 16);
        return std.fmt.parseUnsigned(u64, value, 10);
    }
};

fn ensureDirPath(path: []const u8) !void {
    if (path.len == 0) return;
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), path);
}

fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

const DbSplitSimAction = db_split_sim_fixture.Action;
const DbSplitSimDocSpec = db_split_sim_fixture.DocSpec;
const db_split_sim_index_name = "ft_v1";
const db_split_sim_split_key = "doc:m";

const DbSplitTerm = enum {
    alpha,
    beta,
    gamma,
};

const DbSplitSimSummary = struct {
    source_doc_count: u32 = 0,
    dest_doc_count: u32 = 0,
    source_alpha_hits: u32 = 0,
    source_beta_hits: u32 = 0,
    source_gamma_hits: u32 = 0,
    dest_alpha_hits: u32 = 0,
    dest_beta_hits: u32 = 0,
    dest_gamma_hits: u32 = 0,
};

const DbSplitExpectedDoc = struct {
    side: enum { source, dest },
    term: DbSplitTerm,
};

const DbSplitOwnedWrite = struct {
    key: []u8,
    value: []u8,
    term: DbSplitTerm,

    fn deinit(self: *DbSplitOwnedWrite, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.value);
        self.* = undefined;
    }
};

const DbSplitSimRuntime = struct {
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    open_options: OpenOptions,
    source_db: ?DB,
    dest_db: ?DB,
    split_complete: bool,

    fn init(alloc: Allocator, source_path: [*:0]const u8, dest_path: [*:0]const u8) !DbSplitSimRuntime {
        return try initWithOptions(alloc, source_path, dest_path, .{});
    }

    fn initWithOptions(
        alloc: Allocator,
        source_path: [*:0]const u8,
        dest_path: [*:0]const u8,
        open_options: OpenOptions,
    ) !DbSplitSimRuntime {
        var runtime = DbSplitSimRuntime{
            .alloc = alloc,
            .source_path = source_path,
            .dest_path = dest_path,
            .open_options = open_options,
            .source_db = try DB.open(alloc, std.mem.span(source_path), open_options),
            .dest_db = null,
            .split_complete = false,
        };
        errdefer if (runtime.source_db) |*db| db.close();

        try ensureDbSplitSimIndexDir(alloc, std.mem.span(source_path));
        try runtime.source_db.?.addIndex(.{
            .name = db_split_sim_index_name,
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });
        return runtime;
    }

    fn deinit(self: *DbSplitSimRuntime) void {
        if (self.dest_db) |*db| {
            db.close();
            self.dest_db = null;
        }
        if (self.source_db) |*db| {
            db.close();
            self.source_db = null;
        }
        self.* = undefined;
    }

    fn reopenSource(self: *DbSplitSimRuntime) !void {
        if (self.source_db) |*db| {
            db.close();
            self.source_db = null;
        }
        self.source_db = try DB.open(self.alloc, std.mem.span(self.source_path), self.open_options);
    }

    fn reopenDest(self: *DbSplitSimRuntime) !void {
        if (!self.split_complete) return error.InvalidFixture;
        if (self.dest_db) |*db| {
            db.close();
            self.dest_db = null;
        }
        self.dest_db = try DB.open(self.alloc, std.mem.span(self.dest_path), self.open_options);
    }

    fn splitFull(self: *DbSplitSimRuntime) !void {
        if (self.split_complete) return error.InvalidFixture;
        try self.source_db.?.split(self.source_db.?.getRange(), db_split_sim_split_key, "", std.mem.span(self.dest_path), false);
        try ensureDbSplitSimIndexDir(self.alloc, std.mem.span(self.dest_path));
        self.dest_db = try DB.open(self.alloc, std.mem.span(self.dest_path), self.open_options);
        self.split_complete = true;
    }

    fn applyAction(self: *DbSplitSimRuntime, action: DbSplitSimAction, step: usize) !void {
        switch (action) {
            .reopen_source => try self.reopenSource(),
            .reopen_dest => try self.reopenDest(),
            .split_full => try self.splitFull(),
            .add_doc => |spec| try self.applyWrite(spec, step),
        }
    }

    fn applyWrite(self: *DbSplitSimRuntime, spec: DbSplitSimDocSpec, step: usize) !void {
        const writes = try buildDbSplitWrites(self.alloc, spec, step);
        defer {
            for (writes) |*write| write.deinit(self.alloc);
            self.alloc.free(writes);
        }

        if (!self.split_complete) {
            try applyDbSplitWritesToDb(&self.source_db.?, writes);
            return;
        }

        var source_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer source_writes.deinit(self.alloc);
        var dest_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer dest_writes.deinit(self.alloc);

        for (writes) |write| {
            if (std.mem.order(u8, write.key, db_split_sim_split_key) == .lt) {
                try source_writes.append(self.alloc, .{ .key = write.key, .value = write.value });
            } else {
                try dest_writes.append(self.alloc, .{ .key = write.key, .value = write.value });
            }
        }

        if (source_writes.items.len != 0) try self.source_db.?.batch(.{
            .writes = source_writes.items,
            .sync_level = .full_text,
        });
        if (dest_writes.items.len != 0) try self.dest_db.?.batch(.{
            .writes = dest_writes.items,
            .sync_level = .full_text,
        });
    }

    fn summary(self: *DbSplitSimRuntime, alloc: Allocator) !DbSplitSimSummary {
        return try summarizeDbSplitDatabases(alloc, &self.source_db.?, if (self.dest_db) |*db| db else null);
    }
};

fn summarizeDbSplitDatabases(alloc: Allocator, source_db: *DB, dest_db: ?*DB) !DbSplitSimSummary {
    const source_text = source_db.core.textIndex(db_split_sim_index_name).?;
    const source_snapshot = source_text.snapshot();
    const dest_snapshot = if (dest_db) |db|
        db.core.textIndex(db_split_sim_index_name).?.snapshot()
    else
        null;

    return .{
        .source_doc_count = source_snapshot.global_doc_count,
        .dest_doc_count = if (dest_snapshot) |snapshot| snapshot.global_doc_count else 0,
        .source_alpha_hits = try source_snapshot.termDocFreq(alloc, "title", "alpha"),
        .source_beta_hits = try source_snapshot.termDocFreq(alloc, "title", "beta"),
        .source_gamma_hits = try source_snapshot.termDocFreq(alloc, "title", "gamma"),
        .dest_alpha_hits = if (dest_snapshot) |snapshot| try snapshot.termDocFreq(alloc, "title", "alpha") else 0,
        .dest_beta_hits = if (dest_snapshot) |snapshot| try snapshot.termDocFreq(alloc, "title", "beta") else 0,
        .dest_gamma_hits = if (dest_snapshot) |snapshot| try snapshot.termDocFreq(alloc, "title", "gamma") else 0,
    };
}

fn applyDbSplitWritesToDb(db: *DB, writes: []const DbSplitOwnedWrite) !void {
    var batch_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer batch_writes.deinit(std.testing.allocator);
    for (writes) |write| {
        try batch_writes.append(std.testing.allocator, .{
            .key = write.key,
            .value = write.value,
        });
    }
    try db.batch(.{
        .writes = batch_writes.items,
        .sync_level = .full_text,
    });
}

fn buildDbSplitWrites(alloc: Allocator, spec: DbSplitSimDocSpec, step: usize) ![]DbSplitOwnedWrite {
    var writes = std.ArrayListUnmanaged(DbSplitOwnedWrite).empty;
    errdefer {
        for (writes.items) |*write| write.deinit(alloc);
        writes.deinit(alloc);
    }

    switch (spec) {
        .left_alpha => try writes.append(alloc, try dbSplitWrite(alloc, "doc:b", step, "alpha")),
        .left_gamma => try writes.append(alloc, try dbSplitWrite(alloc, "doc:c", step, "gamma")),
        .right_beta => try writes.append(alloc, try dbSplitWrite(alloc, "doc:z", step, "beta")),
        .mixed_alpha_beta => {
            try writes.append(alloc, try dbSplitWrite(alloc, "doc:b", step, "alpha"));
            try writes.append(alloc, try dbSplitWrite(alloc, "doc:z", step, "beta"));
        },
    }

    return writes.toOwnedSlice(alloc);
}

fn dbSplitWrite(alloc: Allocator, prefix: []const u8, step: usize, term: []const u8) !DbSplitOwnedWrite {
    return .{
        .key = try std.fmt.allocPrint(alloc, "{s}:{d}", .{ prefix, step }),
        .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"{s}\"}}", .{term}),
        .term = std.meta.stringToEnum(DbSplitTerm, term) orelse unreachable,
    };
}

fn expectedDbSplitSummary(actions: []const DbSplitSimAction) !DbSplitSimSummary {
    var docs = std.StringHashMapUnmanaged(DbSplitExpectedDoc).empty;
    defer docs.deinit(std.testing.allocator);

    var split_complete = false;
    for (actions, 0..) |action, step| {
        switch (action) {
            .reopen_source, .reopen_dest => {},
            .split_full => {
                if (split_complete) return error.InvalidFixture;
                split_complete = true;
                var it = docs.iterator();
                while (it.next()) |entry| {
                    entry.value_ptr.side = if (std.mem.order(u8, entry.key_ptr.*, db_split_sim_split_key) == .lt) .source else .dest;
                }
            },
            .add_doc => |spec| {
                const writes = try buildDbSplitWrites(std.testing.allocator, spec, step);
                defer {
                    for (writes) |*write| write.deinit(std.testing.allocator);
                    std.testing.allocator.free(writes);
                }

                for (writes) |write| {
                    const gop = try docs.getOrPut(std.testing.allocator, write.key);
                    if (!gop.found_existing) {
                        gop.key_ptr.* = try std.testing.allocator.dupe(u8, write.key);
                    }
                    gop.value_ptr.* = .{
                        .side = if (split_complete and std.mem.order(u8, write.key, db_split_sim_split_key) != .lt) .dest else .source,
                        .term = write.term,
                    };
                }
            },
        }
    }

    var summary = DbSplitSimSummary{};
    var it = docs.iterator();
    while (it.next()) |entry| {
        switch (entry.value_ptr.side) {
            .source => {
                summary.source_doc_count += 1;
                switch (entry.value_ptr.term) {
                    .alpha => summary.source_alpha_hits += 1,
                    .beta => summary.source_beta_hits += 1,
                    .gamma => summary.source_gamma_hits += 1,
                }
            },
            .dest => {
                summary.dest_doc_count += 1;
                switch (entry.value_ptr.term) {
                    .alpha => summary.dest_alpha_hits += 1,
                    .beta => summary.dest_beta_hits += 1,
                    .gamma => summary.dest_gamma_hits += 1,
                }
            },
        }
    }

    var cleanup_it = docs.keyIterator();
    while (cleanup_it.next()) |key| std.testing.allocator.free(key.*);
    return summary;
}

fn expectDbSplitSummaryEqual(case_label: []const u8, expected: DbSplitSimSummary, actual: DbSplitSimSummary) !void {
    if (!std.meta.eql(expected, actual)) {
        std.debug.print(
            "db split summary mismatch {s}\nexpected={any}\nactual={any}\n",
            .{ case_label, expected, actual },
        );
    }
    try sim_fixture.expectFieldEqual(case_label, "source_doc_count", expected.source_doc_count, actual.source_doc_count);
    try sim_fixture.expectFieldEqual(case_label, "dest_doc_count", expected.dest_doc_count, actual.dest_doc_count);
    try sim_fixture.expectFieldEqual(case_label, "source_alpha_hits", expected.source_alpha_hits, actual.source_alpha_hits);
    try sim_fixture.expectFieldEqual(case_label, "source_beta_hits", expected.source_beta_hits, actual.source_beta_hits);
    try sim_fixture.expectFieldEqual(case_label, "source_gamma_hits", expected.source_gamma_hits, actual.source_gamma_hits);
    try sim_fixture.expectFieldEqual(case_label, "dest_alpha_hits", expected.dest_alpha_hits, actual.dest_alpha_hits);
    try sim_fixture.expectFieldEqual(case_label, "dest_beta_hits", expected.dest_beta_hits, actual.dest_beta_hits);
    try sim_fixture.expectFieldEqual(case_label, "dest_gamma_hits", expected.dest_gamma_hits, actual.dest_gamma_hits);
}

fn fixtureOptionsFromDbSplitSummary(summary: DbSplitSimSummary) db_split_sim_fixture.Options {
    return .{
        .expected_source_doc_count = summary.source_doc_count,
        .expected_dest_doc_count = summary.dest_doc_count,
        .expected_source_alpha_hits = summary.source_alpha_hits,
        .expected_source_beta_hits = summary.source_beta_hits,
        .expected_source_gamma_hits = summary.source_gamma_hits,
        .expected_dest_alpha_hits = summary.dest_alpha_hits,
        .expected_dest_beta_hits = summary.dest_beta_hits,
        .expected_dest_gamma_hits = summary.dest_gamma_hits,
    };
}

fn dbSplitReplayArtifactPath(buf: []u8, suffix: []const u8) []const u8 {
    const base = "/tmp/antfly-db-split-replay-";
    const ts = monotonicTimeNs();
    const nonce = @atomicRmw(u64, &split_replay_artifact_nonce, .Add, 1, .monotonic);
    return std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}.fixture", .{ base, ts, nonce, suffix }) catch unreachable;
}

fn ensureDbSplitSimIndexDir(alloc: Allocator, db_path: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ db_path, db_split_sim_index_name });
    defer alloc.free(path);
    try ensureDirPath(path);
}

fn writeDbSplitReplayArtifactFile(path: []const u8, contents: []const u8) !void {
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &file_buf);
    try writer.interface.writeAll(contents);
    try writer.end();
}

fn writeDbSplitReplayFixtureArtifact(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    summary: DbSplitSimSummary,
    actions: []const DbSplitSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = dbSplitReplayArtifactPath(&path_buf, case_label);
    const path = try alloc.dupe(u8, artifact_path);
    errdefer alloc.free(path);

    const normalized = try db_split_sim_fixture.renderReplayArtifact(
        alloc,
        fixtureOptionsFromDbSplitSummary(summary),
        case_label,
        seed,
        expectation_note,
        actions,
    );
    defer alloc.free(normalized);

    try writeDbSplitReplayArtifactFile(path, normalized);
    return path;
}

fn printDbSplitAction(action: DbSplitSimAction) !void {
    const line = try db_split_sim_fixture.renderAction(std.testing.allocator, action);
    defer std.testing.allocator.free(line);
    std.debug.print("    {s}\n", .{line});
}

fn replayDbSplitActionsAtPaths(
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    actions: []const DbSplitSimAction,
) !DbSplitSimSummary {
    return replayDbSplitActionsAtPathsWithOptions(alloc, source_path, dest_path, .{}, actions);
}

fn replayDbSplitActionsAtPathsWithOptions(
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    open_options: OpenOptions,
    actions: []const DbSplitSimAction,
) !DbSplitSimSummary {
    var runtime = try DbSplitSimRuntime.initWithOptions(alloc, source_path, dest_path, open_options);
    defer runtime.deinit();

    for (actions, 0..) |action, step| {
        try runtime.applyAction(action, step);
    }

    if (runtime.split_complete) {
        try runtime.reopenSource();
        try runtime.source_db.?.runUntilIdle();
        try runtime.reopenDest();
        try runtime.dest_db.?.runUntilIdle();
    } else {
        try runtime.reopenSource();
        try runtime.source_db.?.runUntilIdle();
    }
    return try runtime.summary(alloc);
}

fn summarizeDbSplitPathsWithOptions(
    alloc: Allocator,
    source_path: [*:0]const u8,
    dest_path: [*:0]const u8,
    open_options: OpenOptions,
) !DbSplitSimSummary {
    var source_db = try DB.open(alloc, std.mem.span(source_path), open_options);
    defer source_db.close();
    try source_db.runUntilIdle();
    var dest_db = try DB.open(alloc, std.mem.span(dest_path), open_options);
    defer dest_db.close();
    try dest_db.runUntilIdle();
    return try summarizeDbSplitDatabases(alloc, &source_db, &dest_db);
}

fn reportReducedDbSplitSchedule(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    actions: []const DbSplitSimAction,
) !void {
    const Replayer = struct {
        alloc: Allocator,
        case_label: []const u8,

        pub fn replay(self: @This(), candidate: []const DbSplitSimAction) !void {
            var source_path_buf: [256]u8 = undefined;
            var dest_path_buf: [256]u8 = undefined;
            const source_path = TestHelpers.tempPath(&source_path_buf);
            const dest_path = TestHelpers.tempPath(&dest_path_buf);
            defer TestHelpers.cleanupTempDir(source_path);
            defer TestHelpers.cleanupTempDir(dest_path);
            const actual = try replayDbSplitActionsAtPaths(self.alloc, source_path, dest_path, candidate);
            try expectDbSplitSummaryEqual(self.case_label, try expectedDbSplitSummary(candidate), actual);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(DbSplitSimAction, alloc, actions, Replayer{
        .alloc = alloc,
        .case_label = case_label,
    });
    defer alloc.free(reduced);

    const summary = try expectedDbSplitSummary(reduced);
    const artifact_path = writeDbSplitReplayFixtureArtifact(
        alloc,
        case_label,
        seed,
        "expected DB split replay to preserve source/destination text state across full split and reopen",
        summary,
        reduced,
    ) catch |err| blk: {
        std.debug.print("failed to write db split replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| alloc.free(path);

    std.debug.print("reduced failing db split schedule ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
    for (reduced) |action| try printDbSplitAction(action);
}

fn randomDbSplitWriteSpec(random: std.Random) DbSplitSimDocSpec {
    return @enumFromInt(random.uintLessThan(u8, 4));
}

fn buildDbSplitReplayActions(
    alloc: Allocator,
    seed: u64,
    steps: usize,
) ![]DbSplitSimAction {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var actions = std.ArrayListUnmanaged(DbSplitSimAction).empty;
    errdefer actions.deinit(alloc);

    var split_complete = false;
    var saw_write = false;
    for (0..steps) |_| {
        if (!split_complete and saw_write and random.uintLessThan(u8, 6) == 0) {
            try actions.append(alloc, .split_full);
            split_complete = true;
            continue;
        }
        if (split_complete and random.uintLessThan(u8, 4) == 0) {
            try actions.append(alloc, if (random.boolean()) .reopen_source else .reopen_dest);
            continue;
        }
        if (!split_complete and random.uintLessThan(u8, 4) == 0) {
            try actions.append(alloc, .reopen_source);
            continue;
        }
        try actions.append(alloc, .{ .add_doc = randomDbSplitWriteSpec(random) });
        saw_write = true;
    }

    if (!split_complete) try actions.append(alloc, .split_full);
    return try actions.toOwnedSlice(alloc);
}

fn dbSplitModeledOpenOptions(modeled_device: *storage_sim.ModeledDevice) OpenOptions {
    return .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .storage = modeled_device.storage(),
        .index_backends = .{
            .text_main_backend = .lsm,
            .text_lsm_storage = modeled_device.storage(),
            .dense_storage_backend = .lsm,
            .dense_lsm_storage = modeled_device.storage(),
            .graph_reverse_backend = .lsm,
            .graph_lsm_storage = modeled_device.storage(),
        },
    };
}

fn runDbSplitReplayCase(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    const actions = try buildDbSplitReplayActions(alloc, seed, steps);
    defer alloc.free(actions);

    var source_path_buf: [256]u8 = undefined;
    var dest_path_buf: [256]u8 = undefined;
    const source_path = TestHelpers.tempPath(&source_path_buf);
    const dest_path = TestHelpers.tempPath(&dest_path_buf);
    defer TestHelpers.cleanupTempDir(source_path);
    defer TestHelpers.cleanupTempDir(dest_path);

    const actual = replayDbSplitActionsAtPaths(alloc, source_path, dest_path, actions) catch |err| {
        reportReducedDbSplitSchedule(alloc, case_label, seed, actions) catch {};
        return err;
    };
    expectDbSplitSummaryEqual(case_label, try expectedDbSplitSummary(actions), actual) catch |err| {
        reportReducedDbSplitSchedule(alloc, case_label, seed, actions) catch {};
        return err;
    };
}

fn runModeledDbSplitReplayCase(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    const actions = try buildDbSplitReplayActions(alloc, seed, steps);
    defer alloc.free(actions);

    var modeled_device = storage_sim.ModeledDevice.init(alloc);
    defer modeled_device.deinit();
    const open_options = dbSplitModeledOpenOptions(&modeled_device);

    var source_path_buf: [256]u8 = undefined;
    var dest_path_buf: [256]u8 = undefined;
    const source_path = TestHelpers.tempPath(&source_path_buf);
    const dest_path = TestHelpers.tempPath(&dest_path_buf);
    defer TestHelpers.cleanupTempDir(source_path);
    defer TestHelpers.cleanupTempDir(dest_path);
    try ensureDirPath(std.mem.span(source_path));
    try ensureDirPath(std.mem.span(dest_path));

    const actual = try replayDbSplitActionsAtPathsWithOptions(
        alloc,
        source_path,
        dest_path,
        open_options,
        actions,
    );
    expectDbSplitSummaryEqual(case_label, try expectedDbSplitSummary(actions), actual) catch |err| {
        reportReducedDbSplitSchedule(alloc, case_label, seed, actions) catch {};
        return err;
    };

    try modeled_device.device().crash();
    const reopened = try summarizeDbSplitPathsWithOptions(
        alloc,
        source_path,
        dest_path,
        open_options,
    );
    try expectDbSplitSummaryEqual(case_label, actual, reopened);
}

fn runDbSplitReplayFixtures(alloc: Allocator) !void {
    const root_dir = "pkg/antfly/src/storage/db/db_sim_fixtures";
    var fixture_dir = try std.Io.Dir.cwd().openDir(std.testing.io, root_dir, .{ .iterate = true });
    defer fixture_dir.close(std.testing.io);

    var walker = try fixture_dir.walk(alloc);
    defer walker.deinit();

    var fixture_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (fixture_paths.items) |path| alloc.free(path);
        fixture_paths.deinit(alloc);
    }

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_paths.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_paths.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    for (fixture_paths.items) |fixture_rel_path| {
        const fixture_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, fixture_rel_path });
        defer alloc.free(fixture_path);

        const raw = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, fixture_path, alloc, .limited(64 * 1024));
        defer alloc.free(raw);

        var fixture = try db_split_sim_fixture.parseFixture(alloc, raw);
        defer fixture.deinit(alloc);

        const fixture_name = fixture.case_label orelse fixture.label orelse fixture_rel_path;
        var source_path_buf: [256]u8 = undefined;
        var dest_path_buf: [256]u8 = undefined;
        const source_path = TestHelpers.tempPath(&source_path_buf);
        const dest_path = TestHelpers.tempPath(&dest_path_buf);
        defer TestHelpers.cleanupTempDir(source_path);
        defer TestHelpers.cleanupTempDir(dest_path);

        const actual = try replayDbSplitActionsAtPaths(alloc, source_path, dest_path, fixture.actions);
        try expectDbSplitFixtureExpectation(fixture_name, fixture.opts, actual);
    }
}

fn runModeledDbSplitReplayFixtures(alloc: Allocator) !void {
    const root_dir = "pkg/antfly/src/storage/db/db_sim_fixtures/replay";
    var fixture_dir = try std.Io.Dir.cwd().openDir(std.testing.io, root_dir, .{ .iterate = true });
    defer fixture_dir.close(std.testing.io);

    var walker = try fixture_dir.walk(alloc);
    defer walker.deinit();

    var fixture_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (fixture_paths.items) |path| alloc.free(path);
        fixture_paths.deinit(alloc);
    }

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_paths.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_paths.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    for (fixture_paths.items) |fixture_rel_path| {
        const fixture_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, fixture_rel_path });
        defer alloc.free(fixture_path);

        const raw = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, fixture_path, alloc, .limited(64 * 1024));
        defer alloc.free(raw);

        var fixture = try db_split_sim_fixture.parseFixture(alloc, raw);
        defer fixture.deinit(alloc);

        var modeled_device = storage_sim.ModeledDevice.init(alloc);
        defer modeled_device.deinit();
        const open_options = dbSplitModeledOpenOptions(&modeled_device);

        var source_path_buf: [256]u8 = undefined;
        var dest_path_buf: [256]u8 = undefined;
        const source_path = TestHelpers.tempPath(&source_path_buf);
        const dest_path = TestHelpers.tempPath(&dest_path_buf);
        defer TestHelpers.cleanupTempDir(source_path);
        defer TestHelpers.cleanupTempDir(dest_path);
        try ensureDirPath(std.mem.span(source_path));
        try ensureDirPath(std.mem.span(dest_path));

        const actual = try replayDbSplitActionsAtPathsWithOptions(
            alloc,
            source_path,
            dest_path,
            open_options,
            fixture.actions,
        );
        try modeled_device.device().crash();
        const reopened = try summarizeDbSplitPathsWithOptions(
            alloc,
            source_path,
            dest_path,
            open_options,
        );

        const fixture_name = fixture.case_label orelse fixture.label orelse fixture_rel_path;
        try expectDbSplitFixtureExpectation(fixture_name, fixture.opts, actual);
        try expectDbSplitSummaryEqual(fixture_name, actual, reopened);
    }
}

fn expectDbSplitFixtureExpectation(
    fixture_name: []const u8,
    opts: db_split_sim_fixture.Options,
    actual: DbSplitSimSummary,
) !void {
    if (opts.expected_source_doc_count) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_doc_count", expected, actual.source_doc_count);
    if (opts.expected_dest_doc_count) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_doc_count", expected, actual.dest_doc_count);
    if (opts.expected_source_alpha_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_alpha_hits", expected, actual.source_alpha_hits);
    if (opts.expected_source_beta_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_beta_hits", expected, actual.source_beta_hits);
    if (opts.expected_source_gamma_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_source_gamma_hits", expected, actual.source_gamma_hits);
    if (opts.expected_dest_alpha_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_alpha_hits", expected, actual.dest_alpha_hits);
    if (opts.expected_dest_beta_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_beta_hits", expected, actual.dest_beta_hits);
    if (opts.expected_dest_gamma_hits) |expected| try sim_fixture.expectFieldEqual(fixture_name, "expected_dest_gamma_hits", expected, actual.dest_gamma_hits);
}

test "db split sim default workload stays green" {
    const alloc = std.testing.allocator;
    try runDbSplitReplayCase(alloc, "db-split-default", 0xA17F_E101, 8);
}

test "db split sim reopen-heavy workload stays green" {
    const alloc = std.testing.allocator;
    try runDbSplitReplayCase(alloc, "db-split-reopen-heavy", 0xA17F_E102, 10);
}

test "db split full keeps subsequent left-side source writes searchable" {
    const alloc = std.testing.allocator;

    var source_path_buf: [256]u8 = undefined;
    var dest_path_buf: [256]u8 = undefined;
    const source_path = TestHelpers.tempPath(&source_path_buf);
    const dest_path = TestHelpers.tempPath(&dest_path_buf);
    defer TestHelpers.cleanupTempDir(source_path);
    defer TestHelpers.cleanupTempDir(dest_path);

    var runtime = try DbSplitSimRuntime.init(alloc, source_path, dest_path);
    defer runtime.deinit();

    try runtime.applyAction(.{ .add_doc = .left_alpha }, 0);
    try runtime.applyAction(.split_full, 1);
    try runtime.applyAction(.{ .add_doc = .left_alpha }, 2);

    try runtime.reopenSource();
    try runtime.source_db.?.runUntilIdle();
    try runtime.reopenDest();
    try runtime.dest_db.?.runUntilIdle();

    const raw = try runtime.source_db.?.get(alloc, "doc:b:2");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);

    const source_range = runtime.source_db.?.getRange();
    const source_applied = try runtime.source_db.?.core.loadAppliedSequence(alloc, db_split_sim_index_name);
    const source_replay_target = runtime.source_db.?.core.nextDerivedSequence();
    const summary = try runtime.summary(alloc);
    try std.testing.expectEqualStrings("", source_range.start);
    try std.testing.expectEqualStrings(db_split_sim_split_key, source_range.end);
    try std.testing.expectEqual(@as(u64, 2), source_replay_target);
    try std.testing.expectEqual(@as(u64, 2), source_applied);
    try std.testing.expectEqual(@as(u32, 2), summary.source_doc_count);
    try std.testing.expectEqual(@as(u32, 2), summary.source_alpha_hits);
    try std.testing.expectEqual(@as(u32, 0), summary.dest_doc_count);
}

test "db split replay fixtures stay green" {
    try runDbSplitReplayFixtures(std.testing.allocator);
}

test "db split modeled replay fixtures stay green" {
    try runModeledDbSplitReplayFixtures(std.testing.allocator);
}

test "db split modeled sim workloads stay green" {
    const alloc = std.testing.allocator;
    try runModeledDbSplitReplayCase(alloc, "db-split-modeled-default", 0xA17F_E301, 8);
    try runModeledDbSplitReplayCase(alloc, "db-split-modeled-reopen-heavy", 0xA17F_E302, 10);
}
