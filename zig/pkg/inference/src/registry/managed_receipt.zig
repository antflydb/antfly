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

const std = @import("std");

pub const in_progress_filename = ".antfly-download-in-progress";
pub const plan_filename = ".antfly-download-plan.json";
pub const complete_filename = ".antfly-download-complete.json";
pub const max_receipt_bytes = 16 * 1024 * 1024;
pub const max_artifact_count = 64 * 1024;

pub const ArtifactReceipt = struct {
    path: []const u8,
    size: u64,
    sha256: ?[]const u8 = null,
};

pub const DownloadSource = struct {
    owner: []const u8,
    name: []const u8,
    variant: []const u8,
};

pub const DownloadReceipt = struct {
    version: u32 = 1,
    source: ?DownloadSource = null,
    artifacts: []const ArtifactReceipt,
};

pub const ValidatedArtifact = struct {
    path: []const u8,
    canonical_path: []u8,
    size: u64,
    sha256: ?[]const u8,
};

pub const ValidatedReceipt = struct {
    allocator: std.mem.Allocator,
    parsed: std.json.Parsed(DownloadReceipt),
    artifacts: []ValidatedArtifact,
    path_index: std.StringHashMapUnmanaged(usize),

    pub fn find(self: *const ValidatedReceipt, path: []const u8) ?*const ValidatedArtifact {
        const index = self.path_index.get(path) orelse return null;
        return &self.artifacts[index];
    }

    pub fn deinit(self: *ValidatedReceipt) void {
        for (self.artifacts) |artifact| self.allocator.free(artifact.canonical_path);
        self.allocator.free(self.artifacts);
        self.path_index.deinit(self.allocator);
        self.parsed.deinit();
        self.* = undefined;
    }
};

/// Re-hash an artifact that has already passed receipt path and size
/// validation against a pinned SHA-256 identity.
///
/// This deliberately opens the canonical artifact path and hashes through the
/// file handle rather than trusting the digest declared in the receipt.  A
/// receipt describes a completed download; it does not prove that a local
/// artifact was not replaced afterwards with a same-size file.
pub fn verifyValidatedArtifactSha256(
    io: std.Io,
    artifact: *const ValidatedArtifact,
    expected_hex: []const u8,
) !void {
    if (expected_hex.len != 64) return error.ChecksumMismatch;

    var file = try std.Io.Dir.cwd().openFile(io, artifact.canonical_path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file or stat.size != artifact.size) return error.InvalidManagedDownload;

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buf: [64 * 1024]u8 = undefined;
    while (true) {
        const n = file.readStreaming(io, &.{buf[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        hasher.update(buf[0..n]);
    }

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual_hex = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual_hex, expected_hex)) return error.ChecksumMismatch;
}

const ReceiptSource = enum {
    completion,
    plan,
};

fn managedPath(allocator: std.mem.Allocator, dest_dir: []const u8, filename: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ dest_dir, filename });
}

fn pathPresent(dir: std.Io.Dir, io: std.Io, path: []const u8) !bool {
    dir.access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn publicationBlockedPaths(
    dir: std.Io.Dir,
    io: std.Io,
    in_progress_path: []const u8,
    plan_path: []const u8,
) !bool {
    return try pathPresent(dir, io, in_progress_path) or try pathPresent(dir, io, plan_path);
}

pub fn publicationBlocked(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) bool {
    const in_progress_path = managedPath(allocator, dest_dir, in_progress_filename) catch return true;
    defer allocator.free(in_progress_path);
    const plan_path = managedPath(allocator, dest_dir, plan_filename) catch return true;
    defer allocator.free(plan_path);
    return publicationBlockedPaths(std.Io.Dir.cwd(), io, in_progress_path, plan_path) catch true;
}

pub fn artifactPathIsSafe(path: []const u8) bool {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return false;
    if (std.mem.indexOfScalar(u8, path, '\\') != null or
        std.mem.indexOfScalar(u8, path, ':') != null or
        std.mem.indexOfScalar(u8, path, 0) != null)
    {
        return false;
    }
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| {
        if (part.len == 0 or
            std.mem.eql(u8, part, ".") or
            std.mem.eql(u8, part, ".."))
        {
            return false;
        }
    }
    return true;
}

fn pathIsWithinRoot(root: []const u8, path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, root) or path.len <= root.len) return false;
    if (std.fs.path.isSep(root[root.len - 1])) return true;
    return std.fs.path.isSep(path[root.len]);
}

fn realPathAlloc(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    const sentinel_path = try std.Io.Dir.cwd().realPathFileAlloc(io, path, allocator);
    defer allocator.free(sentinel_path);
    return try allocator.dupe(u8, sentinel_path);
}

/// Resolve the parent of a requested file without resolving the final path
/// component. This preserves the exact name used to address a symlink while
/// removing relative and symlinked ancestor ambiguity. The returned path is
/// owned by `allocator`.
pub fn resolveRequestedFilePath(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) ![]u8 {
    const parent = std.fs.path.dirname(path) orelse ".";
    const canonical_parent = try realPathAlloc(allocator, io, parent);
    defer allocator.free(canonical_parent);
    return std.fs.path.join(allocator, &.{ canonical_parent, std.fs.path.basename(path) });
}

/// Resolve a path to its canonical regular-file target. The returned path is
/// owned by `allocator`.
pub fn resolveRegularFilePath(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) ![]u8 {
    const canonical_path = try realPathAlloc(allocator, io, path);
    errdefer allocator.free(canonical_path);
    const stat = try std.Io.Dir.cwd().statFile(io, canonical_path, .{ .follow_symlinks = false });
    if (stat.kind != .file) return error.InvalidModelArtifactKind;
    return canonical_path;
}

fn resolveContainedArtifactFromCanonicalRoot(
    allocator: std.mem.Allocator,
    io: std.Io,
    canonical_root: []const u8,
    relative_path: []const u8,
) ![]u8 {
    if (!artifactPathIsSafe(relative_path)) return error.InvalidModelArtifactPath;
    const joined_path = try std.fs.path.join(allocator, &.{ canonical_root, relative_path });
    defer allocator.free(joined_path);
    const canonical_path = try realPathAlloc(allocator, io, joined_path);
    errdefer allocator.free(canonical_path);
    if (!pathIsWithinRoot(canonical_root, canonical_path)) return error.ModelArtifactOutsideRoot;
    return canonical_path;
}

/// Resolve an untrusted relative artifact path to a canonical regular file
/// contained by `root`. The returned path is owned by `allocator`.
pub fn resolveContainedArtifactPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    relative_path: []const u8,
) ![]u8 {
    const canonical_root = try realPathAlloc(allocator, io, root);
    defer allocator.free(canonical_root);
    const canonical_path = try resolveContainedArtifactFromCanonicalRoot(allocator, io, canonical_root, relative_path);
    errdefer allocator.free(canonical_path);
    const stat = try std.Io.Dir.cwd().statFile(io, canonical_path, .{ .follow_symlinks = false });
    if (stat.kind != .file) return error.InvalidModelArtifactKind;
    return canonical_path;
}

/// Load and validate a managed completion receipt.
///
/// `null` means the directory is unmanaged. A present but invalid, incomplete,
/// or concurrently changing publication returns `error.InvalidManagedDownload`
/// or `error.IncompleteManagedDownload`; operational filesystem errors remain
/// visible to callers. Artifact paths are canonicalized and proven to remain
/// under the canonical model root before they are returned.
fn loadValidatedSource(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
    source: ReceiptSource,
) !?ValidatedReceipt {
    const cwd = std.Io.Dir.cwd();
    const in_progress_path = try managedPath(allocator, dest_dir, in_progress_filename);
    defer allocator.free(in_progress_path);
    const plan_path = try managedPath(allocator, dest_dir, plan_filename);
    defer allocator.free(plan_path);
    switch (source) {
        .completion => if (try publicationBlockedPaths(cwd, io, in_progress_path, plan_path)) {
            return error.IncompleteManagedDownload;
        },
        .plan => if (!try pathPresent(cwd, io, in_progress_path)) {
            return error.IncompleteManagedDownload;
        },
    }

    const complete_path = try managedPath(allocator, dest_dir, complete_filename);
    defer allocator.free(complete_path);
    const receipt_path = switch (source) {
        .completion => complete_path,
        .plan => plan_path,
    };
    const receipt_json = cwd.readFileAlloc(
        io,
        receipt_path,
        allocator,
        .limited(max_receipt_bytes),
    ) catch |err| switch (err) {
        error.FileNotFound => {
            if (source == .plan or
                try publicationBlockedPaths(cwd, io, in_progress_path, plan_path))
            {
                return error.IncompleteManagedDownload;
            }
            return null;
        },
        else => return err,
    };
    defer allocator.free(receipt_json);

    var parsed = std.json.parseFromSlice(
        DownloadReceipt,
        allocator,
        receipt_json,
        .{
            .ignore_unknown_fields = true,
            // ValidatedReceipt outlives receipt_json; force every string into
            // the parsed arena instead of borrowing unescaped input slices.
            .allocate = .alloc_always,
        },
    ) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidManagedDownload,
    };
    errdefer parsed.deinit();
    if ((parsed.value.version != 1 and parsed.value.version != 2) or
        parsed.value.artifacts.len == 0 or
        parsed.value.artifacts.len > max_artifact_count)
    {
        return error.InvalidManagedDownload;
    }

    const canonical_root = realPathAlloc(allocator, io, dest_dir) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return error.InvalidManagedDownload,
        else => return err,
    };
    defer allocator.free(canonical_root);

    const artifacts = try allocator.alloc(ValidatedArtifact, parsed.value.artifacts.len);
    errdefer allocator.free(artifacts);
    var path_index: std.StringHashMapUnmanaged(usize) = .{};
    errdefer path_index.deinit(allocator);
    try path_index.ensureTotalCapacity(allocator, @intCast(parsed.value.artifacts.len));
    var initialized: usize = 0;
    errdefer for (artifacts[0..initialized]) |artifact| allocator.free(artifact.canonical_path);

    var has_supported_payload = false;
    for (parsed.value.artifacts, 0..) |artifact, index| {
        const entry = path_index.getOrPutAssumeCapacity(artifact.path);
        if (entry.found_existing) return error.InvalidManagedDownload;
        entry.value_ptr.* = index;
        const canonical_path = resolveContainedArtifactFromCanonicalRoot(
            allocator,
            io,
            canonical_root,
            artifact.path,
        ) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return error.InvalidManagedDownload,
            error.InvalidModelArtifactPath => return error.InvalidManagedDownload,
            else => return err,
        };
        errdefer allocator.free(canonical_path);
        const stat = try cwd.statFile(io, canonical_path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size != artifact.size) return error.InvalidManagedDownload;

        artifacts[index] = .{
            .path = artifact.path,
            .canonical_path = canonical_path,
            .size = artifact.size,
            .sha256 = artifact.sha256,
        };
        initialized += 1;
        if (std.mem.endsWith(u8, artifact.path, ".gguf") or
            std.mem.endsWith(u8, artifact.path, ".onnx") or
            std.mem.endsWith(u8, artifact.path, ".safetensors"))
        {
            has_supported_payload = true;
        }
    }
    if (!has_supported_payload) return error.InvalidManagedDownload;
    switch (source) {
        .completion => if (try publicationBlockedPaths(cwd, io, in_progress_path, plan_path)) {
            return error.IncompleteManagedDownload;
        },
        .plan => if (!try pathPresent(cwd, io, in_progress_path) or
            !try pathPresent(cwd, io, plan_path))
        {
            return error.IncompleteManagedDownload;
        },
    }

    return .{
        .allocator = allocator,
        .parsed = parsed,
        .artifacts = artifacts,
        .path_index = path_index,
    };
}

pub fn loadValidated(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) !?ValidatedReceipt {
    return loadValidatedSource(allocator, io, dest_dir, .completion);
}

/// Load the validated artifact plan of a private staging transaction. This is
/// intentionally separate from published discovery: callers must already own
/// the pull lock and know that `dest_dir` is not externally visible.
pub fn loadValidatedPlan(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) !ValidatedReceipt {
    return (try loadValidatedSource(allocator, io, dest_dir, .plan)) orelse
        error.IncompleteManagedDownload;
}

test "validated artifact checksum rehashes the opened file" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = "payload" });
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "payload.bin" });
    defer allocator.free(path);
    const artifact = ValidatedArtifact{
        .path = "payload.bin",
        .canonical_path = path,
        .size = 7,
        .sha256 = null,
    };
    try verifyValidatedArtifactSha256(
        io,
        &artifact,
        "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5",
    );
    try std.testing.expectError(
        error.ChecksumMismatch,
        verifyValidatedArtifactSha256(
            io,
            &artifact,
            "0000000000000000000000000000000000000000000000000000000000000000",
        ),
    );
}

test "artifact receipt paths reject ambiguous and platform-specific forms" {
    try std.testing.expect(artifactPathIsSafe("nested/model.gguf"));
    try std.testing.expect(!artifactPathIsSafe("../model.gguf"));
    try std.testing.expect(!artifactPathIsSafe("nested//model.gguf"));
    try std.testing.expect(!artifactPathIsSafe("nested\\model.gguf"));
    try std.testing.expect(!artifactPathIsSafe("C:model.gguf"));
    try std.testing.expect(!artifactPathIsSafe("nested/\x00model.gguf"));
}

test "validated managed receipt rejects artifacts escaping through symlinks" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "outside.gguf", .data = "outside" });
    try tmp.dir.symLink(io, "../outside.gguf", "model/model.gguf", .{});
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}",
    });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);

    try std.testing.expectError(error.ModelArtifactOutsideRoot, loadValidated(allocator, io, model_dir));
}

test "validated managed receipt accepts contained symlink artifacts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model/artifacts");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/artifacts/model.gguf", .data = "inside" });
    try tmp.dir.symLink(io, "artifacts/model.gguf", "model/model.gguf", .{});
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":6}]}",
    });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);

    var receipt = (try loadValidated(allocator, io, model_dir)) orelse return error.TestExpectedManagedReceipt;
    defer receipt.deinit();
    try std.testing.expectEqual(@as(usize, 1), receipt.artifacts.len);
    try std.testing.expect(std.mem.endsWith(u8, receipt.artifacts[0].canonical_path, "artifacts/model.gguf"));
}

test "validated staging plan is private from published receipt loading" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-in-progress",
        .data = "{\"version\":1,\"state\":\"in_progress\"}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-plan.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}",
    });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);

    try std.testing.expectError(error.IncompleteManagedDownload, loadValidated(allocator, io, model_dir));
    var plan = try loadValidatedPlan(allocator, io, model_dir);
    defer plan.deinit();
    try std.testing.expect(plan.find("model.gguf") != null);
}

test "validated receipts reject duplicate artifact paths" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7},{\"path\":\"model.gguf\",\"size\":7}]}",
    });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);

    try std.testing.expectError(error.InvalidManagedDownload, loadValidated(allocator, io, model_dir));
}

test "validated managed receipt cleans up every allocation failure" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model/nested");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{ .sub_path = "model/nested/mmproj-Q8_0.gguf", .data = "projector" });
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7},{\"path\":\"nested/mmproj-Q8_0.gguf\",\"size\":9}]}",
    });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);

    const Runner = struct {
        fn run(alloc: std.mem.Allocator, path: []const u8) !void {
            var receipt = (try loadValidated(alloc, std.testing.io, path)) orelse
                return error.TestExpectedManagedReceipt;
            defer receipt.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(allocator, Runner.run, .{model_dir});
}
