// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
//! Offline differential witness runner. PostgreSQL, not this executable,
//! generates expectations. Resource refusals are mismatches, never skips.
const std = @import("std");
const regex = @import("antfly_sql_regex");
const Span = regex.Span;
const Profile = struct { postgres_major: u32, encoding: []const u8, collation: []const u8 };
const Case = struct {
    id: []const u8,
    pattern: []const u8,
    input: []const u8,
    options: []const u8 = "",
    start: usize = 0,
    captures: usize,
    matched: ?bool = null,
    sqlstate: ?[]const u8 = null,
    spans: []const Span = &.{},
};
const Actual = struct {
    matched: ?bool = null,
    captures: ?usize = null,
    sqlstate: ?[]const u8 = null,
    spans: []const Span = &.{},
};
fn state(err: anyerror) []const u8 {
    return switch (err) {
        error.SqlInvalidRegularExpression => "2201B",
        error.SqlInvalidArgument => "22023",
        // Distinct diagnostic labels deliberately cannot match SQLSTATEs.
        else => @errorName(err),
    };
}
fn observe(a: std.mem.Allocator, case: Case) !Actual {
    var budget: regex.Budget = .{};
    const flags = regex.Flags.parse(case.options, false) catch |err| return .{ .sqlstate = state(err) };
    var program = regex.Program.compile(a, case.pattern, flags.native, .{}, &budget) catch |err| return .{ .sqlstate = state(err) };
    defer program.deinit();
    var subject = try regex.Subject.init(a, case.input);
    defer subject.deinit();
    const spans = try a.alloc(Span, program.captures + 1);
    const matched = program.find(subject, case.start, spans, &budget) catch |err| return .{ .sqlstate = state(err) };
    return .{ .matched = matched, .captures = program.captures, .spans = spans };
}
fn equal(case: Case, actual: Actual) bool {
    if (case.sqlstate) |expected| return actual.sqlstate != null and std.mem.eql(u8, expected, actual.sqlstate.?);
    if (actual.sqlstate != null or actual.matched != case.matched or actual.captures != case.captures or actual.spans.len != case.spans.len) return false;
    for (actual.spans, case.spans) |found, expected| if (found.start != expected.start or found.end != expected.end) return false;
    return true;
}
pub fn main(init: std.process.Init) !void {
    const a = init.arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    if (args.len != 3) return error.ExpectedWitnessAndReportPaths;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, args[1], a, .limited(64 * 1024 * 1024));
    const Fixture = struct { format: u32, profile: Profile, entries: []const Case };
    const fixture = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const input = fixture.value;
    if (input.format != 2 or input.entries.len == 0 or input.entries.len > 100_000 or input.profile.postgres_major < 18 or !std.mem.eql(u8, input.profile.encoding, "UTF8") or !std.mem.eql(u8, input.profile.collation, "C")) return error.UnsupportedWitnessProfile;
    const Failure = struct { id: []const u8, actual: Actual };
    var failures: std.ArrayList(Failure) = .empty;
    var mismatch_count: usize = 0;
    for (input.entries) |case| {
        if (case.captures > 255 or (case.sqlstate == null and (case.matched == null or case.spans.len != case.captures + 1)) or (case.sqlstate != null and (case.matched != null or case.spans.len != 0))) return error.InvalidWitness;
        var arena = std.heap.ArenaAllocator.init(init.gpa);
        defer arena.deinit();
        const actual = try observe(arena.allocator(), case);
        if (equal(case, actual)) continue;
        mismatch_count += 1;
        if (failures.items.len == 50) continue;
        var owned = actual;
        owned.spans = try a.dupe(Span, actual.spans);
        try failures.append(a, .{ .id = case.id, .actual = owned });
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const output = try std.json.Stringify.valueAlloc(a, .{ .format = 1, .witness_sha256 = hex[0..], .profile = input.profile, .checked = input.entries.len, .mismatch_count = mismatch_count, .failures = failures.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[2], .data = output });
}
