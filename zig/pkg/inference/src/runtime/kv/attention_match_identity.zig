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

const std = @import("std");
const Hash = std.crypto.hash.sha2.Sha256;

fn bytes(hash: *Hash, value: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, value.len, .little);
    hash.update(&length);
    hash.update(value);
}
fn valueHash(hash: *Hash, value: anytype) !void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .bool => hash.update(&.{if (value) 1 else 0}),
        .int => {
            var buffer: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &buffer, value, .little);
            hash.update(&buffer);
        },
        .float => {
            if (!std.math.isFinite(value)) return error.InvalidKvIdentity;
            if (T == f32) try valueHash(hash, @as(u32, @bitCast(value))) else if (T == f64) try valueHash(hash, @as(u64, @bitCast(value))) else @compileError("unsupported identity float");
        },
        .@"enum" => bytes(hash, @tagName(value)),
        .array => {
            try valueHash(hash, @as(u64, value.len));
            for (value) |element| try valueHash(hash, element);
        },
        .pointer => |pointer| {
            if (pointer.size != .slice or pointer.child != u8) @compileError("identity supports byte slices only");
            bytes(hash, value);
        },
        .@"struct" => |info| {
            inline for (info.field_names) |field| {
                bytes(hash, field);
                try valueHash(hash, @field(value, field));
            }
        },
        else => @compileError("unsupported identity field"),
    }
}

/// Hash actual effective configuration, never raw struct padding or addresses.
/// All inputs must come from the loaded model and selected execution contract.
pub fn layout(config: anytype, precision: []const u8) ![32]u8 {
    if (!std.mem.eql(u8, precision, "f16") and !std.mem.eql(u8, precision, "f32")) return error.InvalidKvIdentity;
    var hash = Hash.init(.{});
    bytes(&hash, "antfly.gemma4.post-rope.bias-after-softcap.coverage-v1");
    bytes(&hash, precision);
    try valueHash(&hash, config);
    return hash.finalResult();
}

pub fn template(source: []const u8, bos: []const u8, eos: []const u8, unk: []const u8, pad: []const u8) [32]u8 {
    var hash = Hash.init(.{});
    bytes(&hash, "antfly.chat-template.v1");
    for ([_][]const u8{ source, bos, eos, unk, pad }) |part| bytes(&hash, part);
    return hash.finalResult();
}

pub const ArtifactDigest = struct { role: []const u8, size: u64, sha256: [32]u8 };
/// Caller provides a stable, ordered set of loader-verified content digests.
/// File locations and inode metadata are deliberately not portable identity.
pub fn artifacts(domain: []const u8, entries: []const ArtifactDigest) ![32]u8 {
    if (entries.len == 0) return error.InvalidKvIdentity;
    var hash = Hash.init(.{});
    bytes(&hash, domain);
    for (entries, 0..) |entry, i| {
        if (entry.role.len == 0 or entry.size == 0) return error.InvalidKvIdentity;
        if (i > 0 and !std.mem.lessThan(u8, entries[i - 1].role, entry.role)) return error.InvalidKvIdentity;
        bytes(&hash, entry.role);
        try valueHash(&hash, entry.size);
        hash.update(&entry.sha256);
    }
    return hash.finalResult();
}

test "portable identity binds effective layout precision template and unambiguous fields" {
    const Config = struct { dim: u32 = 512, rope: f32 = 10000, shared: bool = true, donors: [2]u32 = .{ 0, 0 }, name: []const u8 = "gemma" };
    var config: Config = .{};
    const first = try layout(config, "f16");
    try std.testing.expectEqualSlices(u8, &first, &try layout(config, "f16"));
    config.donors[1] = 1;
    try std.testing.expect(!std.mem.eql(u8, &first, &try layout(config, "f16")));
    config = .{};
    try std.testing.expect(!std.mem.eql(u8, &first, &try layout(config, "f32")));
    config.rope = std.math.nan(f32);
    try std.testing.expectError(error.InvalidKvIdentity, layout(config, "f16"));
    try std.testing.expect(!std.mem.eql(u8, &template("ab", "c", "", "", ""), &template("a", "bc", "", "", "")));
}

test "artifact identity requires unique ordered roles and exact content" {
    var entries = [_]ArtifactDigest{.{ .role = "weights", .size = 10, .sha256 = @splat(3) }};
    const first = try artifacts("weights-v1", &entries);
    entries[0].sha256[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &first, &try artifacts("weights-v1", &entries)));
    try std.testing.expectError(error.InvalidKvIdentity, artifacts("weights-v1", &.{ entries[0], entries[0] }));
}

pub const Control = struct {
    context: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |callback| try callback(self.context);
    }
};
pub const FileArtifact = struct {
    role: []const u8,
    path: []const u8,
    max_bytes: u64,
    /// Optional files must be listed even while absent, so their appearance
    /// during model construction invalidates the generation signature.
    optional: bool = false,
};

fn statHash(hash: *Hash, stat: std.Io.File.Stat) !void {
    try valueHash(hash, stat.inode);
    try valueHash(hash, stat.size);
    try valueHash(hash, @as(i128, stat.mtime.toNanoseconds()));
    try valueHash(hash, @as(i128, stat.ctime.toNanoseconds()));
}
fn statEqual(a: std.Io.File.Stat, b: std.Io.File.Stat) bool {
    return a.kind == b.kind and a.inode == b.inode and a.size == b.size and
        a.mtime.toNanoseconds() == b.mtime.toNanoseconds() and a.ctime.toNanoseconds() == b.ctime.toNanoseconds();
}
fn validateFiles(entries: []const FileArtifact) !void {
    if (entries.len == 0 or entries.len > 64) return error.InvalidKvIdentity;
    for (entries, 0..) |entry, i| {
        if (entry.role.len == 0 or entry.path.len == 0 or entry.max_bytes == 0 or
            (i > 0 and !std.mem.lessThan(u8, entries[i - 1].role, entry.role))) return error.InvalidKvIdentity;
    }
}
fn fileStat(dir: std.Io.Dir, io: std.Io, entry: FileArtifact) !?std.Io.File.Stat {
    const stat = @import("../../execution_control.zig").physicalWait(std.Io.Dir.statFile, .{ dir, io, entry.path, std.Io.Dir.StatFileOptions{} }) catch |err| {
        if (entry.optional and err == error.FileNotFound) return null;
        return err;
    };
    if (stat.kind != .file or stat.size > entry.max_bytes) return error.KvIdentityArtifactLimit;
    return stat;
}

/// Local generation marker, not a portable identity. Capture before any of
/// these assets are loaded; compare after loading and before publishing digests.
pub fn fileGeneration(dir: std.Io.Dir, io: std.Io, entries: []const FileArtifact) ![32]u8 {
    try validateFiles(entries);
    var hash = Hash.init(.{});
    for (entries) |entry| {
        bytes(&hash, entry.role);
        bytes(&hash, entry.path);
        if (try fileStat(dir, io, entry)) |stat| {
            hash.update(&.{1});
            try statHash(&hash, stat);
        } else hash.update(&.{0});
    }
    return hash.finalResult();
}

/// Derive identity from actual admitted file contents, never receipts or a
/// writable digest memo. The caller supplies the marker captured before loading
/// these exact assets. Paths/inodes participate only in generation checks.
/// Memory is bounded to 64 KiB, with cancellation between reads and publication.
pub fn hashFiles(dir: std.Io.Dir, io: std.Io, entries: []const FileArtifact, expected_generation: [32]u8, domain: []const u8, control: Control) ![32]u8 {
    try control.check();
    if (!std.mem.eql(u8, &expected_generation, &try fileGeneration(dir, io, entries))) return error.ModelArtifactsChanging;
    var aggregate = Hash.init(.{});
    bytes(&aggregate, domain);
    var buffer: [64 * 1024]u8 = undefined;
    for (entries) |entry| {
        try control.check();
        bytes(&aggregate, entry.role);
        if (try fileStat(dir, io, entry)) |before| {
            aggregate.update(&.{1});
            const Discard = struct {
                fn close(file: std.Io.File, args: anytype) void {
                    file.close(args[1]);
                }
            };
            var file = try @import("../../execution_control.zig").ownedPhysicalWait(std.Io.Dir.openFile, .{ dir, io, entry.path, std.Io.Dir.OpenFileOptions{} }, Discard.close);
            defer file.close(io);
            if (!statEqual(before, try @import("../../execution_control.zig").physicalWait(std.Io.File.stat, .{ file, io }))) return error.ModelArtifactsChanging;
            var hash = Hash.init(.{});
            var size: u64 = 0;
            while (true) {
                try control.check();
                const count = try @import("../../util/admitted_file.zig").read(file, io, &buffer);
                if (count == 0) break;
                size = try std.math.add(u64, size, count);
                if (size > before.size or size > entry.max_bytes) return error.ModelArtifactsChanging;
                hash.update(buffer[0..count]);
            }
            if (size != before.size or !statEqual(before, try @import("../../execution_control.zig").physicalWait(std.Io.File.stat, .{ file, io }))) return error.ModelArtifactsChanging;
            try valueHash(&aggregate, size);
            aggregate.update(&hash.finalResult());
        } else aggregate.update(&.{0});
    }
    try control.check();
    if (!std.mem.eql(u8, &expected_generation, &try fileGeneration(dir, io, entries))) return error.ModelArtifactsChanging;
    return aggregate.finalResult();
}

test "file identity is portable across locations and rejects changed loaded generations" {
    const io = std.testing.io;
    var left = std.testing.tmpDir(.{});
    defer left.cleanup();
    var right = std.testing.tmpDir(.{});
    defer right.cleanup();
    const contents = "actual weights and tokenizer bytes";
    try left.dir.writeFile(io, .{ .sub_path = "left.gguf", .data = contents });
    try right.dir.writeFile(io, .{ .sub_path = "copied.gguf", .data = contents });
    const entries = [_]FileArtifact{ .{ .role = "added-tokens", .path = "added.json", .max_bytes = 1024, .optional = true }, .{ .role = "weights", .path = "left.gguf", .max_bytes = 1024 } };
    var copies = entries;
    copies[1].path = "copied.gguf";
    const generation = try fileGeneration(left.dir, io, &entries);
    const digest = try hashFiles(left.dir, io, &entries, generation, "weights-v1", .{});
    const copy = try hashFiles(right.dir, io, &copies, try fileGeneration(right.dir, io, &copies), "weights-v1", .{});
    try std.testing.expectEqualSlices(u8, &digest, &copy);
    // A forged on-disk memo is deliberately outside the identity mechanism.
    try left.dir.writeFile(io, .{ .sub_path = "left.gguf.antfly-sha256-cache", .data = "forged digest" });
    try std.testing.expectEqualSlices(u8, &digest, &try hashFiles(left.dir, io, &entries, generation, "weights-v1", .{}));
    try left.dir.writeFile(io, .{ .sub_path = "added.json", .data = "[]" });
    try std.testing.expectError(error.ModelArtifactsChanging, hashFiles(left.dir, io, &entries, generation, "weights-v1", .{}));
    const added_generation = try fileGeneration(left.dir, io, &entries);
    const with_added = try hashFiles(left.dir, io, &entries, added_generation, "weights-v1", .{});
    try std.testing.expect(!std.mem.eql(u8, &digest, &with_added));
    try left.dir.writeFile(io, .{ .sub_path = "left.gguf", .data = "different weights, same generation rejected" });
    try std.testing.expectError(error.ModelArtifactsChanging, hashFiles(left.dir, io, &entries, added_generation, "weights-v1", .{}));
}

test "file identity bounds assets and checks cancellation and mutation during streaming" {
    const io = std.testing.io;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const data = try std.testing.allocator.alloc(u8, 192 * 1024);
    defer std.testing.allocator.free(data);
    @memset(data, 7);
    try directory.dir.writeFile(io, .{ .sub_path = "model", .data = data });
    const entries = [_]FileArtifact{.{ .role = "weights", .path = "model", .max_bytes = 256 * 1024 }};
    const generation = try fileGeneration(directory.dir, io, &entries);
    const Mutator = struct {
        dir: std.Io.Dir,
        calls: usize = 0,
        cancel: bool,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls != 4) return;
            if (self.cancel) return error.Cancelled;
            try self.dir.writeFile(std.testing.io, .{ .sub_path = "model", .data = "modified during read" });
        }
    };
    var callback: Mutator = .{ .dir = directory.dir, .cancel = true };
    try std.testing.expectError(error.Cancelled, hashFiles(directory.dir, io, &entries, generation, "weights-v1", .{ .context = &callback, .check_fn = Mutator.check }));
    callback.cancel = false;
    callback.calls = 0;
    try std.testing.expectError(error.ModelArtifactsChanging, hashFiles(directory.dir, io, &entries, generation, "weights-v1", .{ .context = &callback, .check_fn = Mutator.check }));
    var limited = entries;
    limited[0].max_bytes = 1;
    try std.testing.expectError(error.KvIdentityArtifactLimit, fileGeneration(directory.dir, io, &limited));
    try std.testing.expectError(error.InvalidKvIdentity, fileGeneration(directory.dir, io, &.{ entries[0], entries[0] }));
}

pub const TokenizerContract = struct {
    route: enum { huggingface, sentencepiece },
    preserve_inline_specials: bool,
};
pub fn tokenizer(bundle_digest: [32]u8, route: @FieldType(TokenizerContract, "route"), preserve_inline_specials: bool) [32]u8 {
    var hash = Hash.init(.{});
    bytes(&hash, "antfly.gemma4.tokenizer-runtime-v1");
    hash.update(&bundle_digest);
    bytes(&hash, @tagName(route));
    hash.update(&.{if (preserve_inline_specials) 1 else 0});
    return hash.finalResult();
}

test "tokenizer identity binds implementation and compatibility contract" {
    const bundle: [32]u8 = @splat(3);
    const hf = tokenizer(bundle, .huggingface, false);
    const sp = tokenizer(bundle, .sentencepiece, false);
    try std.testing.expect(!std.mem.eql(u8, &hf, &sp));
    try std.testing.expect(!std.mem.eql(u8, &sp, &tokenizer(bundle, .sentencepiece, true)));
}
