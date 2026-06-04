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
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

pub fn insert(allocator: std.mem.Allocator, _: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    var value_json: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--key")) {
            key = args.next();
        } else if (std.mem.eql(u8, arg, "--value")) {
            value_json = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const k = key orelse cli.fatal("--key is required", .{});
    const v = value_json orelse cli.fatal("--value is required", .{});

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, v, .{});
    defer parsed.deinit();

    var inserts: std.json.ArrayHashMap(std.json.Value) = .{};
    defer inserts.deinit(allocator);
    try inserts.map.put(allocator, k, parsed.value);

    var resp = try client.batch(tbl, .{
        .inserts = inserts,
        .sync_level = .full_index,
    });
    defer resp.deinit();
    std.debug.print("Insert successful.\n", .{});
}

pub fn delete(_: std.mem.Allocator, _: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var key: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--key")) {
            key = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const k = key orelse cli.fatal("--key is required", .{});

    const deletes = [_][]const u8{k};
    var resp = try client.batch(tbl, .{
        .deletes = &deletes,
        .sync_level = .full_index,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        if (data.value.deleted) |deleted| {
            std.debug.print("Delete successful. Deleted: {d}\n", .{deleted});
        } else {
            std.debug.print("Delete successful.\n", .{});
        }
    }
}

pub fn load(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var file_path: ?[]const u8 = null;
    var batch_size: usize = 1000;
    var max_batches: usize = std.math.maxInt(usize);
    var skip_lines: usize = 0;
    var sync_level: ?antfly_client.types.SyncLevel = .full_index;
    var id_field: ?[]const u8 = null;
    var skipped_invalid: usize = 0;
    var raw_lines_seen: usize = 0;
    var dry_run = false;
    var print_invalid = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        } else if (std.mem.eql(u8, arg, "--size")) {
            if (args.next()) |s| batch_size = std.fmt.parseInt(usize, s, 10) catch 1000;
        } else if (std.mem.eql(u8, arg, "--batches") or std.mem.eql(u8, arg, "--max-batches")) {
            if (args.next()) |s| max_batches = std.fmt.parseInt(usize, s, 10) catch std.math.maxInt(usize);
        } else if (std.mem.eql(u8, arg, "--skip")) {
            if (args.next()) |s| skip_lines = std.fmt.parseInt(usize, s, 10) catch 0;
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            if (args.next()) |s| sync_level = parseSyncLevel(s) orelse cli.fatal(
                "invalid --sync-level '{s}' (expected propose, write, full_text, enrichments, aknn, full_index)",
                .{s},
            );
        } else if (std.mem.eql(u8, arg, "--id-field")) {
            id_field = args.next();
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            dry_run = true;
        } else if (std.mem.eql(u8, arg, "--print-invalid")) {
            print_invalid = true;
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const path = file_path orelse cli.fatal("--file is required", .{});

    const file = std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only, .allow_directory = false }) catch |err| {
        cli.fatal("opening file {s}: {}", .{ path, err });
    };
    defer file.close(io);

    var total_loaded: usize = 0;
    var batch_count: usize = 0;
    var line_buf = std.ArrayListUnmanaged(u8).empty;
    defer line_buf.deinit(allocator);
    var line_offset: usize = 0;
    var read_buf: [8 * 1024 * 1024]u8 = undefined;
    var eof = false;

    while (batch_count < max_batches and !eof) {
        var inserts: std.json.ArrayHashMap(std.json.Value) = .{};
        defer inserts.deinit(allocator);
        var parsed_docs = std.ArrayListUnmanaged(std.json.Parsed(std.json.Value)).empty;
        defer {
            for (parsed_docs.items) |*doc| doc.deinit();
            parsed_docs.deinit(allocator);
        }
        var owned_ids = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_ids.items) |owned_id| allocator.free(owned_id);
            owned_ids.deinit(allocator);
        }
        var items_in_batch: usize = 0;

        while (items_in_batch < batch_size and !eof) {
            while (true) {
                if (std.mem.indexOfScalar(u8, line_buf.items[line_offset..], '\n') != null) break;

                const n_read = file.readStreaming(io, &.{&read_buf}) catch |err| switch (err) {
                    error.EndOfStream => 0,
                    else => cli.fatal("reading file {s}: {}", .{ path, err }),
                };
                if (n_read == 0) {
                    eof = true;
                    break;
                }
                try line_buf.appendSlice(allocator, read_buf[0..n_read]);
            }

            const relative_line_end = std.mem.indexOfScalar(u8, line_buf.items[line_offset..], '\n') orelse blk: {
                if (eof and line_offset < line_buf.items.len) break :blk line_buf.items.len - line_offset;
                break;
            };
            const line_start = line_offset;
            const line_end = line_offset + relative_line_end;
            const raw_line = line_buf.items[line_start..line_end];
            const line = std.mem.trimEnd(u8, raw_line, "\r");
            line_offset = @min(line_end + 1, line_buf.items.len);
            raw_lines_seen += 1;

            if (line.len == 0) continue;
            if (skip_lines > 0) {
                skip_lines -= 1;
                if (line_offset > 4 * 1024 * 1024 or line_offset == line_buf.items.len) {
                    line_buf.replaceRange(allocator, 0, line_offset, &.{}) catch @panic("OOM");
                    line_offset = 0;
                }
                continue;
            }

            var parsed_line = std.json.parseFromSlice(std.json.Value, allocator, line, .{ .allocate = .alloc_always }) catch |err| {
                skipped_invalid += 1;
                if (print_invalid) {
                    std.debug.print("Invalid JSON at raw line {d}: {}\n", .{ raw_lines_seen, err });
                }
                if (line_offset > 4 * 1024 * 1024 or line_offset == line_buf.items.len) {
                    line_buf.replaceRange(allocator, 0, line_offset, &.{}) catch @panic("OOM");
                    line_offset = 0;
                }
                continue;
            };
            errdefer parsed_line.deinit();

            const doc_id = if (id_field) |field| blk: {
                if (parsed_line.value.object.get(field)) |val| {
                    switch (val) {
                        .string => |s| break :blk s,
                        else => {},
                    }
                }
                const hash = std.hash.Wyhash.hash(0, line);
                const owned = std.fmt.allocPrint(allocator, "{x}", .{hash}) catch break :blk "unknown";
                try owned_ids.append(allocator, owned);
                break :blk owned;
            } else blk: {
                const hash = std.hash.Wyhash.hash(0, line);
                const owned = std.fmt.allocPrint(allocator, "{x}", .{hash}) catch break :blk "unknown";
                try owned_ids.append(allocator, owned);
                break :blk owned;
            };

            try inserts.map.put(allocator, doc_id, parsed_line.value);
            try parsed_docs.append(allocator, parsed_line);
            items_in_batch += 1;
            if (line_offset > 4 * 1024 * 1024 or line_offset == line_buf.items.len) {
                line_buf.replaceRange(allocator, 0, line_offset, &.{}) catch @panic("OOM");
                line_offset = 0;
            }
        }

        if (items_in_batch == 0) break;

        if (!dry_run) {
            var resp = try client.batch(tbl, .{
                .inserts = inserts,
                .sync_level = sync_level,
            });
            defer resp.deinit();
        }

        total_loaded += items_in_batch;
        batch_count += 1;
        std.debug.print("Batch {d}: loaded {d} items (total: {d}, raw lines seen: {d})\n", .{ batch_count, items_in_batch, total_loaded, raw_lines_seen });
    }

    std.debug.print("Bulk load command successful. Total loaded: {d}, raw lines seen: {d}", .{ total_loaded, raw_lines_seen });
    if (skipped_invalid > 0) std.debug.print(" (skipped invalid JSON lines: {d})", .{skipped_invalid});
    std.debug.print("\n", .{});
}

fn parseSyncLevel(value: []const u8) ?antfly_client.types.SyncLevel {
    if (std.mem.eql(u8, value, "propose")) return .propose;
    if (std.mem.eql(u8, value, "write")) return .write;
    if (std.mem.eql(u8, value, "full_text")) return .full_text;
    if (std.mem.eql(u8, value, "full-text")) return .full_text;
    if (std.mem.eql(u8, value, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, value, "aknn")) return .aknn;
    if (std.mem.eql(u8, value, "full_index")) return .full_index;
    if (std.mem.eql(u8, value, "full-index")) return .full_index;
    return null;
}
