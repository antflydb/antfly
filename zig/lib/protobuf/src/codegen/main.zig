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

//! protoc-zig CLI entry point.
//!
//! Reads a binary `FileDescriptorSet` (produced once via
//! `protoc --descriptor_set_out=...`) and emits Zig source files under the
//! given output directory. Invoked from `build.zig` via the
//! `addProtoModule` helper.
//!
//! Usage:
//!   protoc-zig --desc path/to/foo.desc --output path/to/gen/
//!     [--skip-package google.protobuf]
//!     [--include-only-package antfly]

const std = @import("std");
const descriptor = @import("../descriptor.zig");
const resolve = @import("resolve.zig");
const emit = @import("emit.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_impl = std.heap.ArenaAllocator.init(gpa);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const argv = try init.minimal.args.toSlice(arena);

    var desc_path: ?[]const u8 = null;
    var output_dir: []const u8 = "generated";
    var skip_list = std.ArrayListUnmanaged([]const u8).empty;
    var include_list = std.ArrayListUnmanaged([]const u8).empty;
    var raw_packed_fields = std.ArrayListUnmanaged([]const u8).empty;
    var lazy_fields = std.ArrayListUnmanaged([]const u8).empty;

    // Default: always skip google.protobuf — well-known types aren't needed
    // for our code, and trying to emit them would drag in compilers, plugins,
    // etc. Caller can override with --include-google.
    var include_google = false;

    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const arg: []const u8 = argv[i];
        if (std.mem.eql(u8, arg, "--desc")) {
            i += 1;
            if (i >= argv.len) return fail("--desc requires an argument");
            desc_path = argv[i];
        } else if (std.mem.eql(u8, arg, "--output")) {
            i += 1;
            if (i >= argv.len) return fail("--output requires an argument");
            output_dir = argv[i];
        } else if (std.mem.eql(u8, arg, "--skip-package")) {
            i += 1;
            if (i >= argv.len) return fail("--skip-package requires an argument");
            try skip_list.append(arena, argv[i]);
        } else if (std.mem.eql(u8, arg, "--include-only-package")) {
            i += 1;
            if (i >= argv.len) return fail("--include-only-package requires an argument");
            try include_list.append(arena, argv[i]);
        } else if (std.mem.eql(u8, arg, "--raw-packed-field")) {
            i += 1;
            if (i >= argv.len) return fail("--raw-packed-field requires an argument");
            try raw_packed_fields.append(arena, argv[i]);
        } else if (std.mem.eql(u8, arg, "--lazy-field")) {
            i += 1;
            if (i >= argv.len) return fail("--lazy-field requires an argument");
            try lazy_fields.append(arena, argv[i]);
        } else if (std.mem.eql(u8, arg, "--include-google")) {
            include_google = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            std.debug.print("protoc-zig: unknown argument: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    const path = desc_path orelse {
        std.debug.print("protoc-zig: --desc is required\n", .{});
        printUsage();
        std.process.exit(1);
    };

    if (!include_google) {
        try skip_list.append(arena, "google.protobuf");
    }

    // Read the .desc file.
    const desc_bytes = std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .unlimited) catch |err| {
        std.debug.print("protoc-zig: error reading {s}: {}\n", .{ path, err });
        std.process.exit(1);
    };
    defer gpa.free(desc_bytes);

    // Decode FileDescriptorSet.
    var set = descriptor.FileDescriptorSet.decode(gpa, desc_bytes) catch |err| {
        std.debug.print("protoc-zig: error decoding descriptor set: {}\n", .{err});
        std.process.exit(1);
    };
    defer set.deinit(gpa);

    // Build symbol table (maps FQN → (package, path) for cross-file resolution).
    var table = resolve.SymbolTable.build(gpa, &set) catch |err| {
        std.debug.print("protoc-zig: error building symbol table: {}\n", .{err});
        std.process.exit(1);
    };
    defer table.deinit();

    // Generate Zig sources.
    var out = emit.generate(gpa, &set, &table, .{
        .skip_packages = skip_list.items,
        .include_only_packages = include_list.items,
        .raw_packed_fields = raw_packed_fields.items,
        .lazy_fields = lazy_fields.items,
    }) catch |err| {
        std.debug.print("protoc-zig: error generating: {}\n", .{err});
        std.process.exit(1);
    };
    defer out.deinit();

    // Ensure output directory exists.
    std.Io.Dir.cwd().createDirPath(io, output_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => {
            std.debug.print("protoc-zig: error creating {s}: {}\n", .{ output_dir, err });
            std.process.exit(1);
        },
    };

    // Write each generated file into the output directory.
    const dir = std.Io.Dir.cwd().openDir(io, output_dir, .{}) catch |err| {
        std.debug.print("protoc-zig: error opening {s}: {}\n", .{ output_dir, err });
        std.process.exit(1);
    };

    for (out.files.items) |f| {
        const file = dir.createFile(io, f.name, .{}) catch |err| {
            std.debug.print("protoc-zig: error creating {s}/{s}: {}\n", .{ output_dir, f.name, err });
            std.process.exit(1);
        };
        defer file.close(io);
        file.writePositionalAll(io, f.contents, 0) catch |err| {
            std.debug.print("protoc-zig: error writing {s}/{s}: {}\n", .{ output_dir, f.name, err });
            std.process.exit(1);
        };
    }
}

fn fail(msg: []const u8) !void {
    std.debug.print("protoc-zig: {s}\n", .{msg});
    printUsage();
    std.process.exit(1);
}

fn printUsage() void {
    const usage =
        \\Usage: protoc-zig [options]
        \\
        \\Options:
        \\  --desc <path>                 Path to a binary FileDescriptorSet (.desc) (required)
        \\  --output <dir>                Output directory (default: generated)
        \\  --skip-package <pkg>          Skip files whose package matches <pkg> or its descendants.
        \\                                Can be repeated. google.protobuf is skipped by default.
        \\  --include-only-package <pkg>  Only generate files whose package starts with <pkg>.
        \\                                Can be repeated. Overrides --skip-package.
        \\  --raw-packed-field <FQN>      Emit a repeated scalar field as raw packed bytes.
        \\                                Can be repeated. FQN format: package.Message.field.
        \\  --lazy-field <FQN>            Emit a repeated message field as raw element payloads.
        \\                                Can be repeated. FQN format: package.Message.field.
        \\  --include-google              Do NOT auto-skip google.protobuf.
        \\  --help                        Show this message.
        \\
    ;
    std.debug.print("{s}", .{usage});
}
