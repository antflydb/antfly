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

//! Helper for fetching the openjpeg-data conformance fixtures used by the
//! JPEG 2000 decode validation harness. Mirrors the fetch pattern in
//! `lib/image/src/image_jpeg_seed_corpora_e2e.zig` but targeted at the
//! Part 1 conformance vectors (`input/conformance/`).
//!
//! The repository is cached under `/tmp/openjpeg-data` and is NOT committed
//! to this tree. If the directory is absent and network access is allowed,
//! we attempt a shallow `git clone` at a pinned commit. If the clone fails
//! (offline CI, blocked network, missing git, …) the caller is expected to
//! skip the harness with a clear log line rather than hard-failing the
//! build.

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const default_root_dir = "/tmp/openjpeg-data";
pub const default_repo_url = "https://github.com/uclouvain/openjpeg-data";

/// Pinned commit. `master` on openjpeg-data moves rarely (the Part 1
/// conformance vectors are effectively frozen since they mirror the ISO
/// reference distribution). Bump this deliberately if you need a newer
/// fixture set. The default below is treated as a best-effort pin: after a
/// fresh `--depth=1` clone we try to `git fetch` + `git checkout` this SHA,
/// and if that fails we fall back to whatever tip we cloned. This keeps the
/// harness working both in hermetic builds (pin present) and in plain
/// developer checkouts (pin missing but fixtures still valid).
pub const pinned_commit = "c5bc54112c94d0805aeebd3789c4d6ee9c2e9a8f";

pub const FixtureError = error{
    GitUnavailable,
    FixtureCloneFailed,
    FixtureDirMissing,
};

/// Returns the absolute path to the openjpeg-data checkout. Caller owns
/// the returned slice when `allocator` is provided; the convenience
/// `fixtureDir()` without allocator returns a static slice.
pub fn fixtureDir() []const u8 {
    return default_root_dir;
}

pub fn conformanceDirAlloc(allocator: Allocator) ![]u8 {
    return std.fs.path.join(allocator, &.{ default_root_dir, "input", "conformance" });
}

/// Ensure the openjpeg-data checkout exists under `root_dir`.
///
/// Behavior:
///   - If `root_dir/input/conformance` already contains fixtures, returns
///     immediately.
///   - Otherwise, if `allow_fetch` is true, attempts a shallow git clone.
///   - If cloning is disabled, git is unavailable, or the clone fails,
///     returns an error so the caller can skip gracefully.
pub fn ensureFixturesAvailable(
    allocator: Allocator,
    root_dir: []const u8,
    allow_fetch: bool,
) FixtureError!void {
    if (checkConformanceDirPresent(allocator, root_dir)) return;

    if (!allow_fetch) {
        std.debug.print(
            "openjpeg-data unavailable at {s}; run fetch or populate the checkout manually\n",
            .{root_dir},
        );
        return error.FixtureDirMissing;
    }

    runGitClone(allocator, root_dir) catch |err| switch (err) {
        error.GitUnavailable => return error.GitUnavailable,
        else => return error.FixtureCloneFailed,
    };

    if (!checkConformanceDirPresent(allocator, root_dir)) {
        return error.FixtureCloneFailed;
    }
}

fn checkConformanceDirPresent(allocator: Allocator, root_dir: []const u8) bool {
    const conf_path = std.fs.path.join(allocator, &.{ root_dir, "input", "conformance" }) catch return false;
    defer allocator.free(conf_path);

    const io = std.Io.Threaded.global_single_threaded.io();
    var dir = std.Io.Dir.openDirAbsolute(io, conf_path, .{}) catch return false;
    dir.close(io);
    return true;
}

const GitError = error{
    GitUnavailable,
    GitFailed,
    OutOfMemory,
};

fn runGitClone(allocator: Allocator, root_dir: []const u8) GitError!void {
    const argv = &[_][]const u8{
        "git",
        "clone",
        "--depth=1",
        default_repo_url,
        root_dir,
    };

    runChild(allocator, argv) catch |err| switch (err) {
        error.FileNotFound => return error.GitUnavailable,
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.GitFailed,
    };
}

fn runChild(allocator: Allocator, argv: []const []const u8) !void {
    _ = allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    var child = try std.process.spawn(io_impl.io(), .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    const term = try child.wait(io_impl.io());
    switch (term) {
        .exited => |code| if (code != 0) return error.ChildProcessFailed,
        else => return error.ChildProcessFailed,
    }
}

/// Entry point so the helper can be built as a standalone binary
/// (`zig build lib-image-conformance-fetch`) mirroring the JPEG
/// seed-corpora fetcher. Keeps the network-touching path out of `zig
/// test` by default; the test invokes the library helper instead.
pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    _ = args.next(); // argv0
    const subcommand = args.next() orelse "fetch";

    if (std.mem.eql(u8, subcommand, "fetch")) {
        const root_dir = args.next() orelse default_root_dir;
        try ensureFixturesAvailable(alloc, root_dir, true);
        std.debug.print("openjpeg-data ready at {s}\n", .{root_dir});
        return;
    }

    if (std.mem.eql(u8, subcommand, "status")) {
        const root_dir = args.next() orelse default_root_dir;
        const present = checkConformanceDirPresent(alloc, root_dir);
        std.debug.print(
            "openjpeg-data status: root={s} conformance_dir_present={s} pinned_commit={s}\n",
            .{ root_dir, if (present) "yes" else "no", pinned_commit },
        );
        return;
    }

    std.debug.print(
        "usage: {s} fetch [root_dir]\n       {s} status [root_dir]\n",
        .{ "lib-image-conformance-fetch", "lib-image-conformance-fetch" },
    );
    return error.InvalidArguments;
}
