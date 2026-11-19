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

//! Whole-generation one-way synchronization. Preparing a replacement and
//! activating it are separate: the destination owner commits the generation.
const std = @import("std");
const cli = @import("mod.zig");
const staging = @import("../../standalone/restore_staging_bridge.zig");

const Args = struct {
    help: bool = false,
    input: ?[]const u8 = null,
    output: ?[]const u8 = null,
    table: ?[]const u8 = null,
    tables: ?[]const u8 = null,
    definition: ?[]const u8 = null,
    max_input_bytes: u64 = 512 * 1024 * 1024,
    max_expanded_bytes: u64 = 1024 * 1024 * 1024,
};

pub fn run(a: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const opts = parse(args) catch return cli.argumentError("invalid sync arguments", .{});
    if (opts.help) {
        cli.output.notice("{s}", .{usage});
        return;
    }
    validate(opts) catch return cli.argumentError("sync requires --input, a new --out path, and --definition; use at most one table selector", .{});
    const definition = try cli.readFileAlloc(io, a, opts.definition.?, 1024 * 1024);
    defer a.free(definition);
    const report = try staging.syncDataJsonAlloc(a, opts.input.?, opts.output.?, opts.table orelse opts.tables orelse "", definition, opts.max_input_bytes, opts.max_expanded_bytes);
    defer a.free(report);
    try cli.output.writeJsonArtifact(a, io, report);
}

fn parse(args: *std.process.Args.Iterator) !Args {
    var out = Args{};
    var input_limit = false;
    var expanded_limit = false;
    while (args.next()) |arg| {
        if (cli.isHelpArg(arg)) {
            out.help = true;
        } else if (std.mem.eql(u8, arg, "--input") or std.mem.eql(u8, arg, "-i")) {
            try take(args, &out.input);
        } else if (std.mem.eql(u8, arg, "--out")) {
            try take(args, &out.output);
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            try take(args, &out.table);
        } else if (std.mem.eql(u8, arg, "--tables")) {
            try take(args, &out.tables);
        } else if (std.mem.eql(u8, arg, "--definition")) {
            try take(args, &out.definition);
        } else if (std.mem.eql(u8, arg, "--max-input-bytes")) {
            if (input_limit) return error.DuplicateArgument;
            input_limit = true;
            out.max_input_bytes = try std.fmt.parseUnsigned(u64, args.next() orelse return error.MissingArgument, 10);
        } else if (std.mem.eql(u8, arg, "--max-expanded-bytes")) {
            if (expanded_limit) return error.DuplicateArgument;
            expanded_limit = true;
            out.max_expanded_bytes = try std.fmt.parseUnsigned(u64, args.next() orelse return error.MissingArgument, 10);
        } else return error.UnknownArgument;
    }
    return out;
}

fn take(args: *std.process.Args.Iterator, value: *?[]const u8) !void {
    if (value.* != null) return error.DuplicateArgument;
    value.* = args.next() orelse return error.MissingArgument;
}

fn validate(opts: Args) !void {
    for ([_]?[]const u8{ opts.input, opts.output, opts.definition }) |value| {
        if (value == null or value.?.len == 0) return error.InvalidSyncArguments;
    }
    if (opts.max_input_bytes == 0 or opts.max_expanded_bytes == 0 or
        (opts.table != null and opts.tables != null)) return error.InvalidSyncArguments;
    if (opts.table) |name| if (name.len == 0) return error.InvalidSyncArguments;
    if (opts.tables) |names| if (names.len == 0) return error.InvalidSyncArguments;
}

pub const usage =
    \\usage: antfly sync --input <snapshot.aflite> --out <new-generation.aflite> --definition <trusted.json>
    \\                   [--table <name> | --tables <a,b>] [--max-input-bytes N] [--max-expanded-bytes N]
    \\
    \\  Stages a complete one-way replacement from documents and existing external dense vectors.
    \\  Omit table selection to include all source tables. Tables must be in default.public.
    \\  The caller supplies schema/index definitions; source jobs and authority are excluded.
    \\  --out must be new. The source and any existing output are never overwritten.
    \\  The destination owner validates and activates this whole file, preserving its identity and grants.
    \\  Activation replaces the complete selected data, including deletions and vector-only updates.
    \\  No network connection, incremental merge, or running-server mutation is performed.
    \\  Success emits a JSON report with operation=sync, mode=replace, hashes and inventory.
    \\
;

test "sync requires offline paths and a trusted definition with optional selection" {
    var argv = [_][*:0]const u8{ "--input", "source.aflite", "--out", "new.aflite", "--definition", "trusted.json", "--max-input-bytes", "1024" };
    var args = std.process.Args.Iterator.init(.{ .vector = &argv });
    var opts = try parse(&args);
    try validate(opts);
    try std.testing.expectEqual(@as(u64, 1024), opts.max_input_bytes);
    opts.tables = "records,notes";
    try validate(opts);
    opts.table = "records";
    try std.testing.expectError(error.InvalidSyncArguments, validate(opts));
    opts.tables = null;
    try validate(opts);
    opts.definition = null;
    try std.testing.expectError(error.InvalidSyncArguments, validate(opts));
    try std.testing.expectError(error.InvalidSyncArguments, validate(.{}));
    try std.testing.expectError(error.InvalidSyncArguments, validate(.{ .input = "a", .output = "b", .definition = "c", .max_expanded_bytes = 0 }));
}

test "sync rejects backup recovery and remote destination flags" {
    for ([_][*:0]const u8{ "--data-only", "--url", "--connection", "--backup-id", "--location", "--wait", "--mode", "--database", "--namespace" }) |flag| {
        var argv = [_][*:0]const u8{flag};
        var args = std.process.Args.Iterator.init(.{ .vector = &argv });
        try std.testing.expectError(error.UnknownArgument, parse(&args));
    }
    var duplicate = [_][*:0]const u8{ "--out", "a", "--out", "b" };
    var args = std.process.Args.Iterator.init(.{ .vector = &duplicate });
    try std.testing.expectError(error.DuplicateArgument, parse(&args));
}
