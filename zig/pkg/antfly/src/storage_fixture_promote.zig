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
const fixture_format = @import("storage/sim_fixture.zig");

const DestinationSpec = struct {
    root_dir: []const u8,
    category: []const u8,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    _ = args.next() orelse return error.InvalidArguments;

    var force = false;
    var use_latest = false;
    var override_stem: ?[]const u8 = null;
    var source_path_arg: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--force")) {
            force = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--latest")) {
            use_latest = true;
            continue;
        }
        if (use_latest) {
            if (override_stem != null) {
                printUsage();
                return error.InvalidArguments;
            }
            override_stem = arg;
            continue;
        }
        if (source_path_arg == null) {
            source_path_arg = arg;
            continue;
        }
        if (override_stem != null) {
            printUsage();
            return error.InvalidArguments;
        }
        override_stem = arg;
    }

    const source_path = blk: {
        if (use_latest) {
            if (source_path_arg != null) {
                printUsage();
                return error.InvalidArguments;
            }
            break :blk try findLatestFixturePath(alloc, init.io);
        }
        break :blk try alloc.dupe(u8, source_path_arg orelse {
            printUsage();
            return error.InvalidArguments;
        });
    };
    defer alloc.free(source_path);

    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, source_path, alloc, .limited(64 * 1024));
    defer alloc.free(raw);

    var fixture = try fixture_format.parse(alloc, raw);
    defer fixture.deinit(alloc);

    const mode = fixture.mode orelse return error.InvalidFixture;
    const destination = try destinationSpec(mode);
    const stem = override_stem orelse fixture.case_label orelse fixture.label orelse return error.InvalidFixture;
    const normalized_stem = try fixture_format.normalizeStem(alloc, stem);
    defer alloc.free(normalized_stem);

    const dest_dir = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination.root_dir, destination.category });
    defer alloc.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(init.io, dest_dir);

    const dest_rel_path = try std.fmt.allocPrint(alloc, "{s}/{s}.fixture", .{ dest_dir, normalized_stem });
    defer alloc.free(dest_rel_path);

    const normalized = try fixture_format.render(alloc, &fixture);
    defer alloc.free(normalized);

    const cwd = std.Io.Dir.cwd();
    const existing = cwd.readFileAlloc(init.io, dest_rel_path, alloc, .limited(64 * 1024)) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (existing) |bytes| alloc.free(bytes);

    if (existing) |bytes| {
        if (std.mem.eql(u8, bytes, normalized)) {
            std.debug.print("fixture already up to date: {s}\n", .{dest_rel_path});
            return;
        }
        if (!force) {
            std.debug.print("destination exists and differs: {s}\nuse --force to overwrite\n", .{dest_rel_path});
            return error.PathAlreadyExists;
        }
    }

    var file = try cwd.createFile(init.io, dest_rel_path, .{});
    defer file.close(init.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(init.io, &file_buf);
    try writer.interface.writeAll(normalized);
    try writer.end();

    std.debug.print("promoted {s} -> {s}\n", .{ source_path, dest_rel_path });
}

fn destinationSpec(mode: []const u8) !DestinationSpec {
    if (std.mem.eql(u8, mode, "differential")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/lmdb_sim_fixtures",
            .category = "differential",
        };
    }
    if (std.mem.eql(u8, mode, "crash")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/lmdb_sim_fixtures",
            .category = "crash",
        };
    }
    if (std.mem.eql(u8, mode, "wal")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/wal_sim_fixtures",
            .category = "replay",
        };
    }
    if (std.mem.eql(u8, mode, "wal_crash")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/wal_sim_fixtures",
            .category = "crash",
        };
    }
    if (std.mem.eql(u8, mode, "persistent")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/persistent_sim_fixtures",
            .category = "replay",
        };
    }
    if (std.mem.eql(u8, mode, "persistent_crash")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/persistent_sim_fixtures",
            .category = "crash",
        };
    }
    if (std.mem.eql(u8, mode, "index_manager")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures",
            .category = "replay",
        };
    }
    if (std.mem.eql(u8, mode, "index_manager_crash")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures",
            .category = "crash",
        };
    }
    if (std.mem.eql(u8, mode, "db_split")) {
        return .{
            .root_dir = "pkg/antfly/src/storage/db/db_sim_fixtures",
            .category = "replay",
        };
    }
    return error.InvalidFixture;
}

fn findLatestFixturePath(allocator: std.mem.Allocator, io: anytype) ![]u8 {
    var tmp_dir = try std.Io.Dir.cwd().openDir(io, "/tmp", .{ .iterate = true });
    defer tmp_dir.close(io);

    var walker = try tmp_dir.walk(allocator);
    defer walker.deinit();

    var best_path: ?[]u8 = null;
    var best_timestamp: u64 = 0;
    errdefer if (best_path) |path| allocator.free(path);

    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const timestamp = parseFixtureArtifactTimestamp(entry.path) orelse continue;
        if (best_path != null and timestamp <= best_timestamp) continue;

        if (best_path) |path| allocator.free(path);
        best_path = try std.fmt.allocPrint(allocator, "/tmp/{s}", .{entry.path});
        best_timestamp = timestamp;
    }

    return best_path orelse error.FileNotFound;
}

fn parseFixtureArtifactTimestamp(path: []const u8) ?u64 {
    const prefixes = [_][]const u8{
        "antfly-lmdb-replay-",
        "antfly-wal-replay-",
        "antfly-persistent-replay-",
        "antfly-index-manager-replay-",
        "antfly-db-split-replay-",
    };

    for (prefixes) |prefix| {
        if (!std.mem.startsWith(u8, path, prefix)) continue;
        if (!std.mem.endsWith(u8, path, ".fixture")) return null;

        const trimmed = path[prefix.len .. path.len - ".fixture".len];
        var parts = std.mem.splitScalar(u8, trimmed, '-');
        _ = parts.next() orelse return null;
        const ts = parts.next() orelse return null;
        return std.fmt.parseUnsigned(u64, ts, 10) catch null;
    }

    return null;
}

fn printUsage() void {
    std.debug.print(
        "usage: storage_fixture_promote <source_fixture> [dest_stem] [--force]\n" ++
            "       storage_fixture_promote --latest [dest_stem] [--force]\n",
        .{},
    );
}
