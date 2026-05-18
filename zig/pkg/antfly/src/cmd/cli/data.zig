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
    var max_batches: usize = 100;
    var id_field: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        } else if (std.mem.eql(u8, arg, "--size")) {
            if (args.next()) |s| batch_size = std.fmt.parseInt(usize, s, 10) catch 1000;
        } else if (std.mem.eql(u8, arg, "--batches")) {
            if (args.next()) |s| max_batches = std.fmt.parseInt(usize, s, 10) catch 100;
        } else if (std.mem.eql(u8, arg, "--id-field")) {
            id_field = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const path = file_path orelse cli.fatal("--file is required", .{});

    const file_data = cli.readFileAlloc(io, allocator, path, 512 * 1024 * 1024) catch |err| {
        cli.fatal("reading file {s}: {}", .{ path, err });
    };
    defer allocator.free(file_data);

    var total_loaded: usize = 0;
    var batch_count: usize = 0;
    var line_start: usize = 0;

    while (batch_count < max_batches and line_start < file_data.len) {
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

        while (items_in_batch < batch_size and line_start < file_data.len) {
            const remaining = file_data[line_start..];
            const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse remaining.len;
            const line = remaining[0..line_end];
            line_start += line_end + 1;

            if (line.len == 0) continue;

            var parsed_line = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch continue;
            errdefer parsed_line.deinit();

            // Generate a document ID
            const doc_id = if (id_field) |field| blk: {
                if (parsed_line.value.object.get(field)) |val| {
                    switch (val) {
                        .string => |s| break :blk s,
                        else => break :blk "unknown",
                    }
                }
                break :blk "unknown";
            } else blk: {
                // Use hash of line as hex ID
                const hash = std.hash.Wyhash.hash(0, line);
                const owned = std.fmt.allocPrint(allocator, "{x}", .{hash}) catch break :blk "unknown";
                try owned_ids.append(allocator, owned);
                break :blk owned;
            };

            try inserts.map.put(allocator, doc_id, parsed_line.value);
            try parsed_docs.append(allocator, parsed_line);
            items_in_batch += 1;
        }

        if (items_in_batch == 0) break;

        var resp = try client.batch(tbl, .{
            .inserts = inserts,
        });
        defer resp.deinit();

        total_loaded += items_in_batch;
        batch_count += 1;
        std.debug.print("Batch {d}: loaded {d} items (total: {d})\n", .{ batch_count, items_in_batch, total_loaded });
    }

    std.debug.print("Bulk load command successful. Total loaded: {d}\n", .{total_loaded});
}
