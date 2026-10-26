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

//! Test-only trust boundary for context coverage qualification. Authoritative
//! document and build facts are derived from opened bytes. JSONL commands may
//! repeat those facts, but can only select an already sealed entry.
const std = @import("std");
const coverage = @import("context_capture_coverage.zig");
const release_build = @import("context_release_build_binding.zig");

const max_metadata_bytes = 16 * 1024 * 1024;

pub const CommandBinding = struct {
    case_id: []const u8,
    document_sha256: []const u8,
    question_sha256: []const u8,
    request_content_sha256: []const u8,
    bucket: coverage.Bucket,
    binary_receipt_id: []const u8,
    source_receipt_id: []const u8,
};

const Manifest = struct {
    schema_version: u32,
    session_id: []const u8,
    requests: []const struct {
        id: []const u8,
        document: struct { path: []const u8, sha256: []const u8, bucket: coverage.Bucket },
        question: struct { path: []const u8, sha256: []const u8 },
    },
};

pub const Selected = struct {
    document_bytes: []u8,
    question_bytes: []u8,
    document: coverage.Document,
    question_sha256: [32]u8,
    request_content_sha256: [32]u8,
    provenance: coverage.Provenance,
};

pub const Trust = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    campaign_dir: std.Io.Dir,
    parsed_manifest: std.json.Parsed(Manifest),
    binary_sha256: [32]u8,
    source_receipt_sha256: [32]u8,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        campaign_dir: std.Io.Dir,
        manifest_file: []const u8,
        source_receipt_file: []const u8,
        expected_session_id: []const u8,
        expected_manifest_sha256: [32]u8,
    ) !Trust {
        const manifest_bytes = try readCampaignFile(allocator, io, campaign_dir, manifest_file);
        defer allocator.free(manifest_bytes);
        var manifest_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(manifest_bytes, &manifest_sha256, .{});
        if (!std.mem.eql(u8, &manifest_sha256, &expected_manifest_sha256))
            return error.CoverageManifestDigestMismatch;
        var parsed_manifest = try std.json.parseFromSlice(Manifest, allocator, manifest_bytes, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = false,
        });
        errdefer parsed_manifest.deinit();
        if (parsed_manifest.value.schema_version != 2 or
            !std.mem.eql(u8, parsed_manifest.value.session_id, expected_session_id) or
            parsed_manifest.value.requests.len == 0)
            return error.InvalidCoverageManifest;

        for (parsed_manifest.value.requests, 0..) |entry, index| {
            if (entry.id.len == 0 or entry.id.len > 1024 or
                !validRelative(entry.document.path) or !validRelative(entry.question.path) or
                std.mem.eql(u8, entry.document.path, entry.question.path))
                return error.InvalidCoverageManifest;
            _ = try digest32(entry.document.sha256);
            _ = try digest32(entry.question.sha256);
            for (parsed_manifest.value.requests[0..index]) |prior|
                if (std.mem.eql(u8, prior.id, entry.id)) return error.InvalidCoverageManifest;
        }

        const receipt_bytes = try readCampaignFile(allocator, io, campaign_dir, source_receipt_file);
        defer allocator.free(receipt_bytes);
        var source_receipt_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(receipt_bytes, &source_receipt_sha256, .{});
        const build = release_build.parse(allocator, receipt_bytes) catch
            return error.InvalidCoverageBuildReceipt;

        const executable_path = try std.process.executablePathAlloc(io, allocator);
        defer allocator.free(executable_path);
        const executable = try std.Io.Dir.cwd().openFile(io, executable_path, .{
            .mode = .read_only,
            .allow_directory = false,
            .follow_symlinks = false,
        });
        defer executable.close(io);
        const stat = try executable.stat(io);
        if (stat.kind != .file) return error.InvalidCoverageBuildReceipt;
        const binary_sha256 = try hashOpened(io, executable, stat.size);
        if (!std.mem.eql(u8, &binary_sha256, &build.binary_sha256))
            return error.InvalidCoverageBuildReceipt;

        return .{
            .allocator = allocator,
            .io = io,
            .campaign_dir = campaign_dir,
            .parsed_manifest = parsed_manifest,
            .binary_sha256 = binary_sha256,
            .source_receipt_sha256 = source_receipt_sha256,
        };
    }

    pub fn deinit(self: *Trust) void {
        self.parsed_manifest.deinit();
        self.* = undefined;
    }

    pub fn select(self: *const Trust, binding: CommandBinding) !Selected {
        const binary_receipt_id = try digest32(binding.binary_receipt_id);
        const source_receipt_id = try digest32(binding.source_receipt_id);
        if (!std.mem.eql(u8, &binary_receipt_id, &self.binary_sha256) or
            !std.mem.eql(u8, &source_receipt_id, &self.source_receipt_sha256))
            return error.CoverageBuildBindingMismatch;

        const entry = for (self.parsed_manifest.value.requests) |candidate| {
            if (std.mem.eql(u8, candidate.id, binding.case_id)) break candidate;
        } else return error.UnknownCoverageDocument;
        const sealed_document_sha256 = try digest32(entry.document.sha256);
        const repeated_document_sha256 = try digest32(binding.document_sha256);
        const sealed_question_sha256 = try digest32(entry.question.sha256);
        const repeated_question_sha256 = try digest32(binding.question_sha256);
        if (entry.document.bucket != binding.bucket or
            !std.mem.eql(u8, &sealed_document_sha256, &repeated_document_sha256) or
            !std.mem.eql(u8, &sealed_question_sha256, &repeated_question_sha256))
            return error.CoverageDocumentBindingMismatch;

        // The final component is opened once with no-follow. Type, size, read,
        // and hash all refer to this handle, closing the stat/reopen race.
        const document_bytes = try readCampaignFile(self.allocator, self.io, self.campaign_dir, entry.document.path);
        errdefer self.allocator.free(document_bytes);
        const question_bytes = try readCampaignFile(self.allocator, self.io, self.campaign_dir, entry.question.path);
        errdefer self.allocator.free(question_bytes);
        if ((std.math.add(usize, document_bytes.len, question_bytes.len) catch return error.InvalidCoveragePath) > max_metadata_bytes)
            return error.InvalidCoveragePath;
        if (document_bytes.len == 0 or question_bytes.len == 0) return error.EmptyCoverageDocument;
        var actual_document_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(document_bytes, &actual_document_sha256, .{});
        var actual_question_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(question_bytes, &actual_question_sha256, .{});
        if (!std.mem.eql(u8, &actual_document_sha256, &sealed_document_sha256) or
            !std.mem.eql(u8, &actual_question_sha256, &sealed_question_sha256))
            return error.CoverageDocumentDigestMismatch;
        const request_content_sha256 = structuredRequestDigest(document_bytes, question_bytes);
        const repeated_request_content_sha256 = try digest32(binding.request_content_sha256);
        if (!std.mem.eql(u8, &request_content_sha256, &repeated_request_content_sha256))
            return error.CoverageRequestContentMismatch;
        return .{
            .document_bytes = document_bytes,
            .question_bytes = question_bytes,
            .document = .{ .id = entry.id, .sha256 = actual_document_sha256, .bucket = entry.document.bucket },
            .question_sha256 = actual_question_sha256,
            .request_content_sha256 = request_content_sha256,
            .provenance = .{
                .binary_sha256 = self.binary_sha256,
                .source_build_sha256 = self.source_receipt_sha256,
            },
        };
    }
};

pub fn structuredRequestDigest(document: []const u8, question: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.context-serving.structured-request.v1\x00");
    hash.update("user\x00");
    hash.update(&.{2});
    for ([_]struct { bytes: []const u8, selected: u8 }{
        .{ .bytes = document, .selected = 1 },
        .{ .bytes = question, .selected = 0 },
    }) |part| {
        hash.update(&.{ 1, part.selected });
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, @intCast(part.bytes.len), .little);
        hash.update(&length);
        hash.update(part.bytes);
    }
    var target: [8]u8 = undefined;
    std.mem.writeInt(u32, target[0..4], 0, .little);
    std.mem.writeInt(u32, target[4..8], 0, .little);
    hash.update(&target);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

test "context structured request digest golden binds ordered attributed parts" {
    const expected = try digest32("581cbb0a43f6f1b0ce63bf8c4fba8b33393fd4a1e15ddc55ce4bfee984fc3f9f");
    try std.testing.expectEqual(expected, structuredRequestDigest("DOC", "Q"));
    try std.testing.expect(!std.mem.eql(u8, &expected, &structuredRequestDigest("Q", "DOC")));
}

fn digest32(text: []const u8) ![32]u8 {
    if (text.len != 64) return error.InvalidCoverageDigest;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, text) catch return error.InvalidCoverageDigest;
    return result;
}

fn validRelative(path: []const u8) bool {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return false;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part|
        if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}

fn openCampaignFile(io: std.Io, root: std.Io.Dir, path: []const u8) !std.Io.File {
    if (!validRelative(path)) return error.InvalidCoveragePath;
    var current = root;
    var current_owned = false;
    defer if (current_owned) current.close(io);
    var parts = std.mem.splitScalar(u8, path, '/');
    var part = parts.next() orelse return error.InvalidCoveragePath;
    while (parts.next()) |next| {
        const child = try current.openDir(io, part, .{ .follow_symlinks = false });
        if (current_owned) current.close(io);
        current = child;
        current_owned = true;
        part = next;
    }
    return current.openFile(io, part, .{
        .mode = .read_only,
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    });
}

fn readCampaignFile(allocator: std.mem.Allocator, io: std.Io, root: std.Io.Dir, path: []const u8) ![]u8 {
    const file = try openCampaignFile(io, root, path);
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file or stat.size > max_metadata_bytes) return error.InvalidCoveragePath;
    const size = std.math.cast(usize, stat.size) orelse return error.InvalidCoveragePath;
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    if (try file.readPositionalAll(io, bytes, 0) != size) return error.InvalidCoveragePath;
    return bytes;
}

fn hashOpened(io: std.Io, file: std.Io.File, size: u64) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < size) {
        const remaining = std.math.cast(usize, @min(size - offset, buffer.len)) orelse unreachable;
        const read = try file.readPositionalAll(io, buffer[0..remaining], offset);
        if (read == 0) return error.InvalidCoverageBuildReceipt;
        hash.update(buffer[0..read]);
        offset += read;
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
