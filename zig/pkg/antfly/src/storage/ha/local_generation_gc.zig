// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const fs_paths = @import("../../common/fs_paths.zig");

fn writeTestFile(path: []const u8, body: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(std.testing.io, parent);
    var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, body);
    try file.sync(std.testing.io);
}

fn expectPathExists(path: []const u8) !void {
    try std.Io.Dir.cwd().access(std.testing.io, path, .{});
}

fn expectPathMissing(path: []const u8) !void {
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
}

fn generationPath(alloc: std.mem.Allocator, root: []const u8, generation: []const u8) ![]u8 {
    return std.fs.path.join(alloc, &.{ root, "generations", generation });
}

fn prepareGeneration(alloc: std.mem.Allocator, root: []const u8, generation: []const u8) ![]u8 {
    const path = try generationPath(alloc, root, generation);
    errdefer alloc.free(path);
    const payload = try std.fs.path.join(alloc, &.{ path, "content/payload" });
    defer alloc.free(payload);
    try writeTestFile(payload, generation);
    return path;
}

test "storage.ha local generation gc retains newest and protected eligible generations only" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);

    const ineligible = try prepareGeneration(alloc, root, "gen-incomplete");
    defer alloc.free(ineligible);
    const target_scope = try prepareGeneration(alloc, root, "gen-target");
    defer alloc.free(target_scope);
    try markEligible(alloc, .{
        .root = root,
        .scope = .target_activation,
        .generation = "gen-target",
        .slot_name = "standby-a",
        .checkpoint_lsn = 90,
        .checkpoint_bytes = "durable-target-activation-receipt",
    });

    for (1..6) |index| {
        const generation = try std.fmt.allocPrint(alloc, "gen-{d}", .{index});
        defer alloc.free(generation);
        const path = try prepareGeneration(alloc, root, generation);
        defer alloc.free(path);
        const checkpoint = try std.fmt.allocPrint(alloc, "durable-publish-receipt-{d}", .{index});
        defer alloc.free(checkpoint);
        try markEligible(alloc, .{
            .root = root,
            .scope = .source_capture,
            .generation = generation,
            .slot_name = "standby-a",
            .checkpoint_lsn = index,
            .checkpoint_bytes = checkpoint,
        });
    }

    var result = try prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-5",
        .protected_generations = &.{"gen-1"},
        .retain_generations = 2,
    });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.deleted_generations);
    try std.testing.expectEqual(@as(usize, 3), result.retained_generations);

    for ([_][]const u8{ "gen-1", "gen-4", "gen-5", "gen-incomplete", "gen-target" }) |generation| {
        const path = try generationPath(alloc, root, generation);
        defer alloc.free(path);
        try expectPathExists(path);
    }
    for ([_][]const u8{ "gen-2", "gen-3" }) |generation| {
        const path = try generationPath(alloc, root, generation);
        defer alloc.free(path);
        try expectPathMissing(path);
    }

    var repeated = try prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-5",
        .protected_generations = &.{"gen-1"},
        .retain_generations = 2,
    });
    defer repeated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), repeated.deleted_generations);
}

test "storage.ha local generation gc resumes an interrupted tombstone deletion" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);

    for (1..3) |index| {
        const generation = try std.fmt.allocPrint(alloc, "gen-{d}", .{index});
        defer alloc.free(generation);
        const path = try prepareGeneration(alloc, root, generation);
        defer alloc.free(path);
        try markEligible(alloc, .{
            .root = root,
            .scope = .source_capture,
            .generation = generation,
            .slot_name = "standby-a",
            .checkpoint_lsn = index,
            .checkpoint_bytes = generation,
        });
    }

    try std.testing.expectError(error.InjectedLocalGCFailure, pruneWithOptions(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-2",
        .retain_generations = 1,
    }, .{ .fail_after_tombstones = 1 }));
    const old_path = try generationPath(alloc, root, "gen-1");
    defer alloc.free(old_path);
    try expectPathMissing(old_path);
    const tombstone = try generationPath(alloc, root, ".gc-gen-1");
    defer alloc.free(tombstone);
    try expectPathExists(tombstone);

    var recovered = try prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-2",
        .retain_generations = 1,
    });
    defer recovered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), recovered.resumed_tombstones);
    try expectPathMissing(tombstone);
}

test "storage.ha local generation gc fails closed without current durable eligibility" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try prepareGeneration(alloc, root, "gen-unpublished");
    defer alloc.free(path);

    try std.testing.expectError(error.CurrentGenerationNotEligible, prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-unpublished",
        .retain_generations = 1,
    }));
    try expectPathExists(path);
}
