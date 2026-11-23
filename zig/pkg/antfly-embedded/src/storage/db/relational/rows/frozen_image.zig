// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Immutable initial COPY images use the ordinary native snapshot engine.
//! Lifetime belongs to the SQL generation and admitted reader frontier, never
//! a query-cache TTL. A binding is an identity, not SQL read authorization.
const std = @import("std");
const backup = @import("../../native_backup.zig");
const seal = @import("../../native_backup_seal.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

pub const Binding = @import("frozen_contract.zig").Binding;
pub const Handle = @import("frozen_contract.zig").Handle;
pub const Manifest = struct {
    version: u16 = 1,
    binding: Binding,
    sequence: u64,
    files: []const seal.File,
};
pub fn parentAlloc(a: A, db_path: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}.generation-images", .{db_path});
}
pub fn pathAlloc(a: A, db_path: []const u8, binding: Binding) ![]u8 {
    return std.fmt.allocPrint(a, "{s}.generation-images/{s}", .{ db_path, try binding.id() });
}
pub fn finish(a: A, io: std.Io, root: []const u8, binding: Binding, sequence: u64, cancellation: Cancellation) !void {
    try binding.validate();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const files = try seal.inventoryAlloc(scratch, io, root, cancellation);
    const manifest: Manifest = .{ .binding = binding, .sequence = sequence, .files = files };
    const bytes = try std.json.Stringify.valueAlloc(scratch, manifest, .{});
    if (bytes.len > backup.max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
    _ = try backup.writeFileDurable(io, try std.fs.path.join(scratch, &.{ root, "generation-image.json" }), bytes);
    try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, root);
}
pub fn validate(a: A, io: std.Io, root: []const u8, expected: Handle, cancellation: Cancellation) !void {
    _ = try inspect(a, io, root, expected.binding, expected.physical, cancellation);
}
pub fn readHandle(a: A, io: std.Io, root: []const u8, expected: Binding, cancellation: Cancellation) !Handle {
    return inspect(a, io, root, expected, null, cancellation);
}
fn inspect(a: A, io: std.Io, root: []const u8, expected: Binding, physical: ?[32]u8, cancellation: Cancellation) !Handle {
    try expected.validate();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const raw = backup.readFileAlloc(scratch, io, try std.fs.path.join(scratch, &.{ root, "generation-image.json" }), backup.max_manifest_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.RelationPopulationChanged,
        else => return err,
    };
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw, &checksum, .{});
    if (physical) |digest| if (!std.mem.eql(u8, &digest, &checksum)) return error.RelationPopulationChanged;
    const manifest = std.json.parseFromSliceLeaky(Manifest, scratch, raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.RelationPopulationChanged,
    };
    if (manifest.version != 1 or !std.meta.eql(manifest.binding, expected) or manifest.files.len > seal.max_files) return error.RelationPopulationChanged;
    for (manifest.files) |file| {
        try cancellation.check();
        if (std.fs.path.isAbsolute(file.path) or std.mem.indexOf(u8, file.path, "..") != null or std.mem.indexOfAny(u8, file.path, "\\\x00") != null or std.mem.eql(u8, file.path, "generation-image.json")) return error.RelationPopulationChanged;
        const stat = try backup.statRegularFile(io, try std.fs.path.join(scratch, &.{ root, file.path }));
        if (stat.inode != file.inode or stat.size != file.size or stat.mtime.toNanoseconds() != file.mtime_ns) return error.RelationPopulationChanged;
    }
    return .{ .binding = expected, .physical = checksum };
}
