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

//! openapi-zig CLI entry point.
//!
//! Usage: openapi-zig --spec api.json --output generated/ --package my_api

const std = @import("std");
const openapi = @import("openapi");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_impl = std.heap.ArenaAllocator.init(gpa);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var spec_path: ?[]const u8 = null;
    var output_dir: []const u8 = "generated";
    var package_name: []const u8 = "api";
    const GenerateFlags = struct {
        types: bool = true,
        client: bool = true,
        server: bool = false,
        extractors: bool = false,

        fn parseToken(self: *@This(), token: []const u8) void {
            if (std.mem.eql(u8, token, "types")) self.types = true;
            if (std.mem.eql(u8, token, "client")) self.client = true;
            if (std.mem.eql(u8, token, "server")) self.server = true;
            if (std.mem.eql(u8, token, "extractors")) self.extractors = true;
        }
    };
    var gen = GenerateFlags{};
    var config_path: ?[]const u8 = null;
    var import_mapping = std.StringArrayHashMapUnmanaged([]const u8){};

    // Parse CLI args
    const argv = try init.minimal.args.toSlice(arena);
    var i: usize = 1; // skip argv[0]
    while (i < argv.len) : (i += 1) {
        const arg: []const u8 = argv[i];
        if (std.mem.eql(u8, arg, "--spec")) {
            i += 1;
            spec_path = if (i < argv.len) @as([]const u8, argv[i]) else null;
        } else if (std.mem.eql(u8, arg, "--output")) {
            i += 1;
            if (i < argv.len) output_dir = argv[i];
        } else if (std.mem.eql(u8, arg, "--package")) {
            i += 1;
            if (i < argv.len) package_name = argv[i];
        } else if (std.mem.eql(u8, arg, "--generate")) {
            i += 1;
            if (i < argv.len) {
                const what: []const u8 = argv[i];
                gen = .{};
                gen.types = false;
                gen.client = false;
                gen.server = false;
                gen.extractors = false;
                var iter = std.mem.splitScalar(u8, what, ',');
                while (iter.next()) |part| {
                    gen.parseToken(part);
                }
            }
        } else if (std.mem.eql(u8, arg, "--config")) {
            i += 1;
            config_path = if (i < argv.len) @as([]const u8, argv[i]) else null;
        } else if (std.mem.eql(u8, arg, "--import-mapping")) {
            i += 1;
            if (i < argv.len) {
                // Format: "path/to/spec.yaml=module_name"
                const mapping: []const u8 = argv[i];
                if (std.mem.indexOf(u8, mapping, "=")) |eq_pos| {
                    try import_mapping.put(arena, mapping[0..eq_pos], mapping[eq_pos + 1 ..]);
                }
            }
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            std.debug.print("Unknown argument: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    // Load config file if provided
    if (config_path) |cfg_path| {
        const cfg_bytes = std.Io.Dir.cwd().readFileAlloc(io, cfg_path, gpa, .unlimited) catch |err| {
            std.debug.print("Error reading config file '{s}': {}\n", .{ cfg_path, err });
            std.process.exit(1);
        };
        defer gpa.free(cfg_bytes);

        const parsed = std.json.parseFromSlice(std.json.Value, arena, cfg_bytes, .{}) catch |err| {
            std.debug.print("Error parsing config file '{s}': {}\n", .{ cfg_path, err });
            std.process.exit(1);
        };
        const root = parsed.value;

        if (root.object.get("spec")) |v| {
            if (v == .string) spec_path = v.string;
        }
        if (root.object.get("package")) |v| {
            if (v == .string) package_name = v.string;
        }
        if (root.object.get("output")) |v| {
            if (v == .string) output_dir = v.string;
        }
        if (root.object.get("generate")) |v| {
            if (v == .array) {
                gen = .{};
                gen.types = false;
                gen.client = false;
                gen.server = false;
                gen.extractors = false;
                for (v.array.items) |item| {
                    if (item == .string) {
                        gen.parseToken(item.string);
                    }
                }
            }
        }
        if (root.object.get("import_mapping")) |v| {
            if (v == .object) {
                for (v.object.keys(), v.object.values()) |key, val| {
                    if (val == .string) {
                        try import_mapping.put(arena, key, val.string);
                    }
                }
            }
        }
    }

    const path = spec_path orelse {
        std.debug.print("Error: --spec is required\n", .{});
        printUsage();
        std.process.exit(1);
    };

    // Read spec file
    const spec_bytes = std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .unlimited) catch |err| {
        std.debug.print("Error reading spec file '{s}': {}\n", .{ path, err });
        std.process.exit(1);
    };
    defer gpa.free(spec_bytes);

    // Parse
    var parser = openapi.Parser.init(arena);
    const doc = parser.parseDocument(spec_bytes) catch |err| {
        std.debug.print("Error parsing OpenAPI spec: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("Parsed: {s} v{s} ({d} schemas, {d} paths)\n", .{
        doc.info.title,
        doc.info.version,
        if (doc.components) |c| c.schemas.count() else 0,
        doc.paths.count(),
    });

    // Generate
    const result = try openapi.codegen.generate(arena, &doc, .{
        .package_name = package_name,
        .generate_types = gen.types,
        .generate_client = gen.client,
        .generate_server = gen.server,
        .generate_extractors = gen.extractors,
        .import_mapping = import_mapping,
    });

    // Write output files (ignore if directory already exists)
    std.Io.Dir.cwd().createDirPath(io, output_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => {
            std.debug.print("Error creating output directory '{s}': {}\n", .{ output_dir, err });
            std.process.exit(1);
        },
    };

    try writeFile(io, output_dir, "root.zig", result.root);

    if (result.types) |content| {
        try writeFile(io, output_dir, "types.zig", content);
    }
    if (result.client) |content| {
        try writeFile(io, output_dir, "client.zig", content);
    }
    if (result.server) |content| {
        try writeFile(io, output_dir, "server.zig", content);
    }

    std.debug.print("Generated {s} in {s}/\n", .{ package_name, output_dir });
}

fn writeFile(io: std.Io, dir_path: []const u8, file_name: []const u8, content: []const u8) !void {
    const dir = std.Io.Dir.cwd().openDir(io, dir_path, .{}) catch |err| {
        std.debug.print("Error opening directory '{s}': {}\n", .{ dir_path, err });
        return err;
    };
    const file = dir.createFile(io, file_name, .{}) catch |err| {
        std.debug.print("Error creating file '{s}/{s}': {}\n", .{ dir_path, file_name, err });
        return err;
    };
    defer file.close(io);
    file.writePositionalAll(io, content, 0) catch |err| {
        std.debug.print("Error writing file '{s}/{s}': {}\n", .{ dir_path, file_name, err });
        return err;
    };
    std.debug.print("  wrote {s}/{s} ({d} bytes)\n", .{ dir_path, file_name, content.len });
}

fn printUsage() void {
    const usage =
        \\Usage: openapi-zig [options]
        \\
        \\Options:
        \\  --spec <path>             Path to OpenAPI JSON spec (required)
        \\  --output <dir>            Output directory (default: generated)
        \\  --package <name>          Package name (default: api)
        \\  --generate <what>         Comma-separated: types,client,server,extractors (default: types,client)
        \\  --config <path>           Path to JSON config file
        \\  --import-mapping <spec=mod>  Map external $ref file path to Zig module name
        \\  --help                    Show this help
        \\
        \\Config file format (JSON):
        \\  {
        \\    "spec": "api.yaml",
        \\    "package": "metadata",
        \\    "output": "generated",
        \\    "generate": ["types", "server"],
        \\    "import_mapping": {
        \\      "../../lib/schema/openapi.yaml": "schema",
        \\      "../../lib/embeddings/openapi.yaml": "embeddings"
        \\    }
        \\  }
        \\
        \\Examples:
        \\  openapi-zig --spec api.json --output src/gen --package my_api
        \\  openapi-zig --config codegen.json
        \\  openapi-zig --spec api.json --import-mapping "../lib/types.yaml=shared_types"
        \\
    ;
    std.debug.print("{s}", .{usage});
}
