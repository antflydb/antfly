// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! One-shot, idempotent migration of the pre-0.3 `<root>/ha/` hot-standby
//! layout to the canonical `<root>/standby/` layout described in
//! `DATA_DIR.md` and `HOT_STANDBY.md` ("Data directory layout" /
//! "Layout migration").
//!
//! The caller (`antfly standalone`) gathers every configured hot-standby
//! path and calls `migrateLegacyLayout` once, before any hot-standby store
//! is opened. For each configured path whose parent directory is named
//! `standby`, the grandparent directory is a candidate root:
//!
//!   (A) if `<root>/standby` does not exist and `<root>/ha` does, the whole
//!       directory is renamed `ha` -> `standby` in one atomic rename. This
//!       carries along anything the operator keeps alongside the
//!       hot-standby state files, such as `seed-captures/` and
//!       `standby-generations/<gen>/`.
//!   (B) inside `<root>/standby` (whether it just appeared or already
//!       existed), each legacy basename is renamed to its canonical name
//!       when the canonical name is absent and the legacy name is present.
//!
//! Nothing is ever deleted or overwritten: if both `<root>/ha` and
//! `<root>/standby` exist, step (A) is skipped for that root (the report
//! records the coexistence so the caller can warn) but step (B) still runs
//! inside `<root>/standby`. If both spellings of a file exist inside
//! `<root>/standby`, both are left in place and the report counts a
//! conflict. Paths whose parent is not named `standby` are left completely
//! alone, so a custom, non-standard layout is never touched. A crash between
//! (A) and (B) is recovered by the next call: (A) is a no-op once
//! `<root>/standby` exists, and (B) is per-file and idempotent.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// The canonical hot-standby state directory basename, relative to a data
/// directory root.
pub const canonical_dir = "standby";

/// The pre-0.3 hot-standby state directory basename, relative to a data
/// directory root.
pub const legacy_dir = "ha";

/// A legacy basename and the canonical basename it is renamed to, once it
/// lives directly under `canonical_dir`. Names absent from this list
/// (`primary.wal`, `slots`, `fence.wal`, `seed-captures`, and
/// `standby-generations`) are unchanged between the legacy and canonical
/// layouts and move only via the whole-directory rename in step (A).
const BasenameRename = struct {
    old_name: []const u8,
    new_name: []const u8,
};

pub const basename_renames = [_]BasenameRename{
    .{ .old_name = "standby.wal", .new_name = "log.wal" },
    .{ .old_name = "standby-progress.wal", .new_name = "progress.wal" },
};

/// What one `migrateLegacyLayout` call did. All counters are zero on a
/// no-op call, which is the expected steady state once every configured
/// root has been migrated.
pub const Report = struct {
    /// Number of roots where `<root>/ha` was renamed to `<root>/standby`.
    dirs_renamed: u32 = 0,
    /// Number of legacy basenames renamed to their canonical name inside an
    /// existing `<root>/standby` directory.
    files_renamed: u32 = 0,
    /// Number of legacy/canonical basename pairs found coexisting inside a
    /// `<root>/standby` directory; both were left in place.
    skipped_conflicts: u32 = 0,
    /// Number of roots where both `<root>/ha` and `<root>/standby` existed,
    /// so step (A) was skipped for that root. The caller should warn when
    /// this is nonzero: the legacy tree is inert but was not cleaned up.
    coexisting_roots: u32 = 0,

    pub fn changed(self: Report) bool {
        return self.dirs_renamed != 0 or self.files_renamed != 0;
    }
};

fn pathExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn joinAlloc(alloc: Allocator, a: []const u8, b: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ a, b });
}

/// Derives the migration root for one configured hot-standby path: the
/// parent of `path` must be named `canonical_dir`, and the root is that
/// parent's own parent. Returns `null` when `path` does not follow that
/// shape, which is the common case for a custom, non-standard layout.
fn rootForConfiguredPath(path: []const u8) ?[]const u8 {
    const parent = std.fs.path.dirname(path) orelse return null;
    if (!std.mem.eql(u8, std.fs.path.basename(parent), canonical_dir)) return null;
    return std.fs.path.dirname(parent);
}

/// Migrates one root's `legacy_dir` -> `canonical_dir` layout. Safe to call
/// repeatedly; see the module documentation for the exact algorithm.
fn migrateRoot(io: std.Io, alloc: Allocator, root: []const u8, report: *Report) !void {
    const legacy_path = try joinAlloc(alloc, root, legacy_dir);
    defer alloc.free(legacy_path);
    const canonical_path = try joinAlloc(alloc, root, canonical_dir);
    defer alloc.free(canonical_path);

    const canonical_exists = pathExists(io, canonical_path);
    const legacy_exists = pathExists(io, legacy_path);

    if (!canonical_exists and legacy_exists) {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), legacy_path, std.Io.Dir.cwd(), canonical_path, io);
        report.dirs_renamed += 1;
    } else if (canonical_exists and legacy_exists) {
        report.coexisting_roots += 1;
    }

    // Step (B) runs whenever `canonical_path` exists now, whether it already
    // existed or step (A) just created it by renaming the legacy directory.
    if (!pathExists(io, canonical_path)) return;

    for (basename_renames) |pair| {
        const old_file = try joinAlloc(alloc, canonical_path, pair.old_name);
        defer alloc.free(old_file);
        const new_file = try joinAlloc(alloc, canonical_path, pair.new_name);
        defer alloc.free(new_file);

        const old_exists = pathExists(io, old_file);
        const new_exists = pathExists(io, new_file);
        if (old_exists and new_exists) {
            report.skipped_conflicts += 1;
        } else if (old_exists and !new_exists) {
            try std.Io.Dir.rename(std.Io.Dir.cwd(), old_file, std.Io.Dir.cwd(), new_file, io);
            report.files_renamed += 1;
        }
    }
}

/// Migrates every root implied by `configured_paths` from the legacy
/// `ha/` hot-standby layout to the canonical `standby/` layout. Idempotent
/// and safe to call on every startup. Never creates a directory and never
/// deletes or overwrites an existing file. See the module documentation for
/// the full algorithm.
pub fn migrateLegacyLayout(io: std.Io, alloc: Allocator, configured_paths: []const []const u8) !Report {
    var roots: std.ArrayListUnmanaged([]const u8) = .empty;
    defer roots.deinit(alloc);

    for (configured_paths) |path| {
        const root = rootForConfiguredPath(path) orelse continue;
        var already_present = false;
        for (roots.items) |existing| {
            if (std.mem.eql(u8, existing, root)) {
                already_present = true;
                break;
            }
        }
        if (!already_present) try roots.append(alloc, root);
    }

    var report = Report{};
    for (roots.items) |root| {
        try migrateRoot(io, alloc, root, &report);
    }
    return report;
}

const testing = std.testing;

fn testRoot(tmp: *testing.TmpDir, alloc: Allocator) ![:0]u8 {
    return try tmp.dir.realPathFileAlloc(testing.io, ".", alloc);
}

fn writeTestFile(dir_path: []const u8, name: []const u8, body: []const u8) !void {
    const alloc = testing.allocator;
    const path = try std.fs.path.join(alloc, &.{ dir_path, name });
    defer alloc.free(path);
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(testing.io, parent);
    var file = try std.Io.Dir.cwd().createFile(testing.io, path, .{ .truncate = true });
    defer file.close(testing.io);
    try file.writeStreamingAll(testing.io, body);
}

fn expectFile(dir_path: []const u8, name: []const u8, body: []const u8) !void {
    const alloc = testing.allocator;
    const path = try std.fs.path.join(alloc, &.{ dir_path, name });
    defer alloc.free(path);
    const contents = try std.Io.Dir.cwd().readFileAlloc(testing.io, path, alloc, .limited(4096));
    defer alloc.free(contents);
    try testing.expectEqualStrings(body, contents);
}

fn expectMissing(dir_path: []const u8, name: []const u8) !void {
    const alloc = testing.allocator;
    const path = try std.fs.path.join(alloc, &.{ dir_path, name });
    defer alloc.free(path);
    try testing.expect(!pathExists(testing.io, path));
}

test "storage.hot_standby layout migrates a full legacy tree including operator sidecar directories" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(&tmp, alloc);
    defer alloc.free(root);

    const legacy = try std.fs.path.join(alloc, &.{ root, legacy_dir });
    defer alloc.free(legacy);
    try writeTestFile(legacy, "primary.wal", "primary");
    try writeTestFile(legacy, "slots", "slots");
    try writeTestFile(legacy, "standby.wal", "standby-log");
    try writeTestFile(legacy, "standby-progress.wal", "standby-progress");
    try writeTestFile(legacy, "fence.wal", "fence");
    try writeTestFile(legacy, "seed-captures/generations/gen-1/complete.json", "capture");
    try writeTestFile(legacy, "standby-generations/gen-1/receipt.json", "generation");

    const configured_primary_log = try std.fs.path.join(alloc, &.{ root, canonical_dir, "primary.wal" });
    defer alloc.free(configured_primary_log);
    const configured_fence = try std.fs.path.join(alloc, &.{ root, canonical_dir, "fence.wal" });
    defer alloc.free(configured_fence);

    var report = try migrateLegacyLayout(testing.io, alloc, &.{ configured_primary_log, configured_fence });
    try testing.expectEqual(@as(u32, 1), report.dirs_renamed);
    try testing.expectEqual(@as(u32, 2), report.files_renamed);
    try testing.expectEqual(@as(u32, 0), report.skipped_conflicts);
    try testing.expectEqual(@as(u32, 0), report.coexisting_roots);
    try testing.expect(report.changed());

    try expectMissing(root, legacy_dir);
    const canonical = try std.fs.path.join(alloc, &.{ root, canonical_dir });
    defer alloc.free(canonical);
    try expectFile(canonical, "primary.wal", "primary");
    try expectFile(canonical, "slots", "slots");
    try expectFile(canonical, "log.wal", "standby-log");
    try expectFile(canonical, "progress.wal", "standby-progress");
    try expectFile(canonical, "fence.wal", "fence");
    try expectFile(canonical, "seed-captures/generations/gen-1/complete.json", "capture");
    try expectFile(canonical, "standby-generations/gen-1/receipt.json", "generation");

    // Calling again is a no-op.
    report = try migrateLegacyLayout(testing.io, alloc, &.{ configured_primary_log, configured_fence });
    try testing.expectEqual(@as(u32, 0), report.dirs_renamed);
    try testing.expectEqual(@as(u32, 0), report.files_renamed);
    try testing.expectEqual(@as(u32, 0), report.skipped_conflicts);
    try testing.expectEqual(@as(u32, 0), report.coexisting_roots);
    try testing.expect(!report.changed());
}

test "storage.hot_standby layout finishes a partial migration left by an interrupted rename" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(&tmp, alloc);
    defer alloc.free(root);

    const canonical = try std.fs.path.join(alloc, &.{ root, canonical_dir });
    defer alloc.free(canonical);
    try writeTestFile(canonical, "primary.wal", "primary");
    try writeTestFile(canonical, "slots", "slots");
    try writeTestFile(canonical, "progress.wal", "standby-progress");
    // The directory rename already happened; only the file rename step is
    // left, simulating a crash between (A) and (B).
    try writeTestFile(canonical, "standby.wal", "standby-log");

    const configured_standby_log = try std.fs.path.join(alloc, &.{ canonical, "log.wal" });
    defer alloc.free(configured_standby_log);

    const report = try migrateLegacyLayout(testing.io, alloc, &.{configured_standby_log});
    try testing.expectEqual(@as(u32, 0), report.dirs_renamed);
    try testing.expectEqual(@as(u32, 1), report.files_renamed);
    try testing.expectEqual(@as(u32, 0), report.skipped_conflicts);
    try testing.expectEqual(@as(u32, 0), report.coexisting_roots);

    try expectFile(canonical, "log.wal", "standby-log");
    try expectMissing(canonical, "standby.wal");
}

test "storage.hot_standby layout leaves both trees when they coexist but still resolves file conflicts" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(&tmp, alloc);
    defer alloc.free(root);

    const legacy = try std.fs.path.join(alloc, &.{ root, legacy_dir });
    defer alloc.free(legacy);
    try writeTestFile(legacy, "primary.wal", "legacy-primary");
    try writeTestFile(legacy, "standby.wal", "legacy-standby-log");

    const canonical = try std.fs.path.join(alloc, &.{ root, canonical_dir });
    defer alloc.free(canonical);
    try writeTestFile(canonical, "primary.wal", "canonical-primary");
    // Both spellings already present under the canonical tree: never touched.
    try writeTestFile(canonical, "standby.wal", "canonical-standby-log");
    try writeTestFile(canonical, "log.wal", "canonical-log");

    const configured_primary_log = try std.fs.path.join(alloc, &.{ canonical, "primary.wal" });
    defer alloc.free(configured_primary_log);

    const report = try migrateLegacyLayout(testing.io, alloc, &.{configured_primary_log});
    try testing.expectEqual(@as(u32, 0), report.dirs_renamed);
    try testing.expectEqual(@as(u32, 0), report.files_renamed);
    try testing.expectEqual(@as(u32, 1), report.skipped_conflicts);
    try testing.expectEqual(@as(u32, 1), report.coexisting_roots);

    // Both trees remain, untouched beyond the already-satisfied canonical names.
    try expectFile(legacy, "primary.wal", "legacy-primary");
    try expectFile(legacy, "standby.wal", "legacy-standby-log");
    try expectFile(canonical, "primary.wal", "canonical-primary");
    try expectFile(canonical, "standby.wal", "canonical-standby-log");
    try expectFile(canonical, "log.wal", "canonical-log");
}

test "storage.hot_standby layout is a no-op on an already-canonical tree" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(&tmp, alloc);
    defer alloc.free(root);

    const canonical = try std.fs.path.join(alloc, &.{ root, canonical_dir });
    defer alloc.free(canonical);
    try writeTestFile(canonical, "primary.wal", "primary");
    try writeTestFile(canonical, "slots", "slots");
    try writeTestFile(canonical, "log.wal", "log");
    try writeTestFile(canonical, "progress.wal", "progress");
    try writeTestFile(canonical, "fence.wal", "fence");

    const configured_primary_log = try std.fs.path.join(alloc, &.{ canonical, "primary.wal" });
    defer alloc.free(configured_primary_log);

    const report = try migrateLegacyLayout(testing.io, alloc, &.{configured_primary_log});
    try testing.expectEqual(Report{}, report);
    try expectMissing(root, legacy_dir);
}

test "storage.hot_standby layout ignores a configured path outside a standby directory" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(&tmp, alloc);
    defer alloc.free(root);

    const legacy = try std.fs.path.join(alloc, &.{ root, legacy_dir });
    defer alloc.free(legacy);
    try writeTestFile(legacy, "primary.wal", "legacy-primary");

    const custom_path = try std.fs.path.join(alloc, &.{ root, "custom", "primary.wal" });
    defer alloc.free(custom_path);

    const report = try migrateLegacyLayout(testing.io, alloc, &.{custom_path});
    try testing.expectEqual(Report{}, report);
    // The unrelated legacy tree is left completely alone.
    try expectFile(legacy, "primary.wal", "legacy-primary");
    try expectMissing(root, canonical_dir);
}
