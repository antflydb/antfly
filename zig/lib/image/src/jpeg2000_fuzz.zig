// Copyright 2026 Antfly, Inc.
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

//! JPEG 2000 fuzz runner.
//!
//! This is a tiny standalone executable that walks a corpus directory and
//! feeds every `.j2k` / `.jp2` / `.bin` file through the public JPEG 2000
//! parser entry points. It is meant to be driven by an external fuzzer
//! (libFuzzer harness, afl-fuzz, honggfuzz, etc.) in "replay all seeds"
//! mode, or executed directly over a static corpus to smoke-test parsers
//! against malformed input.
//!
//! Usage:
//!     zig build image-jpeg2000-fuzz
//!     ./zig-out/bin/jpeg2000-fuzz <corpus-dir>
//!
//! For every file the runner invokes (in order):
//!   1. `box.parse` (JP2 container parser) when the file starts with the
//!      JP2 signature.
//!   2. `codestream.parseState` (main marker-segment parser).
//!   3. `decode.decodeU8Bytes` (end-to-end 8-bit decode).
//!
//! Each probe is expected to either succeed or return a well-typed error.
//! Any panic / segfault is a fuzz finding and will abort the process via
//! Zig's default panic handler (this runner intentionally installs no
//! custom handler so CI captures the native crash signal).
//!
//! Output is a per-file one-line summary to stdout:
//!     OK       <path>
//!     PARSE_ERR:<ErrorName>  <path>
//!     DECODE_ERR:<ErrorName> <path>
//! followed by a totals line.

const std = @import("std");
const antfly_image = @import("antfly_image");

const jpeg2000 = antfly_image.jpeg2000;

const max_fixture_bytes: usize = 64 * 1024 * 1024;

const Summary = struct {
    total: usize = 0,
    ok: usize = 0,
    parse_err: usize = 0,
    decode_err: usize = 0,
    read_err: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "jpeg2000-fuzz";
    const corpus_dir = args.next() orelse {
        std.debug.print(
            "usage: {s} <corpus-dir>\n" ++
                "  iterates .j2k/.jp2/.bin files under <corpus-dir> and\n" ++
                "  feeds each to box.parse, codestream.parseState, and decode.decodeU8Bytes.\n",
            .{argv0},
        );
        return error.MissingCorpusPath;
    };

    var summary = Summary{};
    try sweepCorpus(alloc, corpus_dir, &summary);
    printSummary(summary);
}

fn sweepCorpus(alloc: std.mem.Allocator, corpus_dir: []const u8, summary: *Summary) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var dir = try std.Io.Dir.cwd().openDir(io, corpus_dir, .{ .iterate = true });
    defer dir.close(io);

    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!isFuzzablePath(entry.path)) continue;

        summary.total += 1;
        probeOne(alloc, io, dir, entry.path, summary) catch |err| {
            // Only read/IO failures land here; all parser errors are handled
            // inside probeOne and counted. Surface IO problems on stderr but
            // keep sweeping the rest of the corpus.
            std.debug.print("READ_ERR:{s}\t{s}\n", .{ @errorName(err), entry.path });
            summary.read_err += 1;
        };
    }
}

fn probeOne(
    alloc: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
    summary: *Summary,
) !void {
    const bytes = try dir.readFileAlloc(io, path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bytes);

    // 1. JP2 container parse (only when the signature box is present).
    //    box.parse is non-allocating, so there is nothing to free.
    if (jpeg2000.box.hasSignature(bytes)) {
        _ = jpeg2000.box.parse(bytes) catch |err| {
            std.debug.print("PARSE_ERR:{s}\t{s}\n", .{ @errorName(err), path });
            summary.parse_err += 1;
            return;
        };
    }

    // 2. Codestream marker-segment parse.
    //    parseState can embed raw JPEG 2000 codestreams; for JP2-wrapped
    //    files codestream.parseState is still valid because the top-level
    //    codestream starts with SOC at the beginning of the file for
    //    well-formed inputs. Malformed inputs will return MissingSocMarker
    //    which is the expected well-typed error path and is counted as
    //    PARSE_ERR.
    if (jpeg2000.codestream.parseState(alloc, bytes)) |state| {
        var owned = state;
        owned.deinit(alloc);
    } else |err| {
        std.debug.print("PARSE_ERR:{s}\t{s}\n", .{ @errorName(err), path });
        summary.parse_err += 1;
        return;
    }

    // 3. Full U8 decode. Most fuzz inputs will fail here with a well-typed
    //    error; that is expected and counted as DECODE_ERR, not a crash.
    if (jpeg2000.decodeU8Bytes(alloc, bytes)) |decoded| {
        var owned = decoded;
        owned.deinit();
        std.debug.print("OK\t{s}\n", .{path});
        summary.ok += 1;
    } else |err| {
        std.debug.print("DECODE_ERR:{s}\t{s}\n", .{ @errorName(err), path });
        summary.decode_err += 1;
    }
}

fn isFuzzablePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".j2k") or
        std.mem.endsWith(u8, path, ".jp2") or
        std.mem.endsWith(u8, path, ".bin");
}

fn printSummary(summary: Summary) void {
    std.debug.print(
        "jpeg2000-fuzz totals: total={d} ok={d} parse_err={d} decode_err={d} read_err={d}\n",
        .{ summary.total, summary.ok, summary.parse_err, summary.decode_err, summary.read_err },
    );
}
