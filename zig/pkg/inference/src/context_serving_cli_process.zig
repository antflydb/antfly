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

//! Qualification-only owner for the genuine `antfly inference generate` route.
//! The child parses production CLI flags and sends its request to the already
//! loaded persistent qualification server. It never loads a second model and
//! cannot be substituted by the in-process native helper.
const std = @import("std");

pub const max_output_bytes = 1024 * 1024;
pub const timeout_seconds = 300;

/// The production streaming CLI writes each public SSE content delta verbatim
/// and exactly one trailing newline after the stream completes. Reasoning
/// deltas, token ids, stderr, and timing output are not part of stdout.
pub fn validatePublicStdout(stdout: []const u8, expected_bytes: u32, expected_sha256: [32]u8) !void {
    const content_len: usize = @intCast(expected_bytes);
    if (stdout.len != try std.math.add(usize, content_len, 1) or stdout[content_len] != '\n')
        return error.CliPublicOutputMismatch;
    var actual: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(stdout[0..content_len], &actual, .{});
    if (!std.mem.eql(u8, &actual, &expected_sha256))
        return error.CliPublicOutputMismatch;
}

pub const Request = struct {
    model: []const u8,
    document_file: []const u8,
    document_sha256: []const u8,
    question_file: []const u8,
    question_sha256: []const u8,
    max_context_tokens: u32,
    max_kv_tokens: u32,
    keep_prefix_tokens: u32,
    keep_recent_tokens: u32,
    reserved_output_tokens: u32,
    stream: bool = false,
};

pub const Receipt = struct {
    allocator: std.mem.Allocator,
    binary_sha256: [32]u8,
    argv_sha256: [32]u8,
    argv_preimage: []u8,
    request_sha256: [32]u8,
    stdout_sha256: [32]u8,
    stderr_sha256: [32]u8,
    elapsed_ns: u64,
    exit_code: u8,
    stdout: []u8,
    stderr: []u8,

    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.argv_preimage);
        self.allocator.free(self.stdout);
        self.allocator.free(self.stderr);
        self.* = undefined;
    }
};

pub const Runner = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    campaign_dir: std.Io.Dir,
    binary: []const u8,
    expected_binary_sha256: [32]u8,

    fn digestFile(self: *const Runner, path: []const u8) ![32]u8 {
        const file = try std.Io.Dir.cwd().openFile(self.io, path, .{
            .mode = .read_only,
            .allow_directory = false,
            .follow_symlinks = false,
        });
        defer file.close(self.io);
        const stat = try file.stat(self.io);
        if (stat.kind != .file or stat.size == 0) return error.InvalidCliQualificationBinary;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var bytes: [64 * 1024]u8 = undefined;
        var offset: u64 = 0;
        while (offset < stat.size) {
            const remaining: usize = @intCast(@min(stat.size - offset, bytes.len));
            const read = try file.readPositionalAll(self.io, bytes[0..remaining], offset);
            if (read == 0) return error.InvalidCliQualificationBinary;
            hash.update(bytes[0..read]);
            offset += read;
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    const argv_domain = "antfly.context-serving.cli-argv.v1\x00";
    const max_argv_bytes = 64 * 1024;
    const max_argv_items = 64;

    /// Exact bytes hashed for the argv receipt. The same slices are passed to
    /// std.process.run; the parent independently parses this bounded artifact
    /// and reconstructs the one permitted production CLI invocation.
    fn argvPreimage(allocator: std.mem.Allocator, argv: []const []const u8) ![]u8 {
        if (argv.len == 0 or argv.len > max_argv_items) return error.InvalidCliQualificationRequest;
        var total: usize = argv_domain.len;
        for (argv) |arg| {
            if (arg.len == 0 or std.mem.indexOfScalar(u8, arg, 0) != null)
                return error.InvalidCliQualificationRequest;
            total = try std.math.add(usize, total, try std.math.add(usize, 8, arg.len));
        }
        if (total > max_argv_bytes) return error.InvalidCliQualificationRequest;
        const result = try allocator.alloc(u8, total);
        errdefer allocator.free(result);
        @memcpy(result[0..argv_domain.len], argv_domain);
        var cursor: usize = argv_domain.len;
        for (argv) |arg| {
            std.mem.writeInt(u64, result[cursor..][0..8], @intCast(arg.len), .little);
            cursor += 8;
            @memcpy(result[cursor..][0..arg.len], arg);
            cursor += arg.len;
        }
        if (cursor != result.len) return error.InvalidCliQualificationRequest;
        return result;
    }

    pub fn requestDigest(request: Request) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context-serving.cli-request.v1\x00");
        var size_bytes: [8]u8 = undefined;
        for ([_][]const u8{ request.model, request.document_file, request.document_sha256, request.question_file, request.question_sha256 }) |value| {
            std.mem.writeInt(u64, &size_bytes, @intCast(value.len), .little);
            hash.update(&size_bytes);
            hash.update(value);
        }
        var value_bytes: [4]u8 = undefined;
        for ([_]u32{ request.max_context_tokens, request.max_kv_tokens, request.keep_prefix_tokens, request.keep_recent_tokens, request.reserved_output_tokens }) |value| {
            std.mem.writeInt(u32, &value_bytes, value, .little);
            hash.update(&value_bytes);
        }
        hash.update(&[_]u8{@intFromBool(request.stream)});
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn run(self: *const Runner, server_url: []const u8, request: Request) !Receipt {
        if (!std.mem.eql(u8, std.fs.path.basename(self.binary), "antfly") or
            server_url.len == 0 or request.model.len == 0 or request.document_file.len == 0 or
            request.question_file.len == 0 or request.document_sha256.len != 64 or
            request.question_sha256.len != 64 or request.max_context_tokens == 0 or
            request.max_kv_tokens == 0 or request.reserved_output_tokens == 0)
            return error.InvalidCliQualificationRequest;
        const actual_binary_sha256 = try self.digestFile(self.binary);
        if (!std.mem.eql(u8, &actual_binary_sha256, &self.expected_binary_sha256))
            return error.CliQualificationBinaryChanged;

        var max_context: [16]u8 = undefined;
        var max_kv: [16]u8 = undefined;
        var keep_prefix: [16]u8 = undefined;
        var keep_recent: [16]u8 = undefined;
        var max_tokens: [16]u8 = undefined;
        const max_context_text = try std.fmt.bufPrint(&max_context, "{d}", .{request.max_context_tokens});
        const max_kv_text = try std.fmt.bufPrint(&max_kv, "{d}", .{request.max_kv_tokens});
        const keep_prefix_text = try std.fmt.bufPrint(&keep_prefix, "{d}", .{request.keep_prefix_tokens});
        const keep_recent_text = try std.fmt.bufPrint(&keep_recent, "{d}", .{request.keep_recent_tokens});
        const max_tokens_text = try std.fmt.bufPrint(&max_tokens, "{d}", .{request.reserved_output_tokens});

        var argv = std.ArrayList([]const u8).empty;
        defer argv.deinit(self.allocator);
        try argv.append(self.allocator, self.binary);
        try argv.append(self.allocator, "inference");
        try argv.appendSlice(self.allocator, &.{
            "generate",                  request.model,             "-",
            "--server",                  server_url,                "--require-server",
            "--backend",                 "metal",                   "--max-tokens",
            max_tokens_text,             "--temperature",           "0",
            "--top-p",                   "0",                       "--top-k",
            "1",                         "--ignore-eos",            "--disable-thinking",
            "--context-mode",            "attention_matching",      "--context-window",
            max_context_text,            "--max-kv-tokens",         max_kv_text,
            "--keep-prefix-tokens",      keep_prefix_text,          "--keep-recent-tokens",
            keep_recent_text,            "--compact-part",          "0:0",
            "--context-document-file",   request.document_file,     "--context-document-sha256",
            request.document_sha256,     "--context-question-file", request.question_file,
            "--context-question-sha256", request.question_sha256,
        });
        if (request.stream) try argv.append(self.allocator, "--stream");
        const argv_preimage = try argvPreimage(self.allocator, argv.items);
        errdefer self.allocator.free(argv_preimage);
        var argv_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(argv_preimage, &argv_sha256, .{});
        const request_sha256 = requestDigest(request);
        const started = std.Io.Timestamp.now(self.io, .awake);
        const result = try std.process.run(self.allocator, self.io, .{
            .argv = argv.items,
            .cwd = .{ .dir = self.campaign_dir },
            .stdout_limit = .limited(max_output_bytes),
            .stderr_limit = .limited(max_output_bytes),
            .timeout = .{ .duration = .{ .raw = .fromSeconds(timeout_seconds), .clock = .awake } },
        });
        errdefer self.allocator.free(result.stdout);
        errdefer self.allocator.free(result.stderr);
        const finished = std.Io.Timestamp.now(self.io, .awake);
        const elapsed = std.Io.Timestamp.durationTo(started, finished).nanoseconds;
        if (elapsed <= 0) return error.InvalidCliQualificationTiming;
        const exit_code = switch (result.term) {
            .exited => |code| std.math.cast(u8, code) orelse return error.CliQualificationFailed,
            else => return error.CliQualificationFailed,
        };
        if (exit_code != 0) return error.CliQualificationFailed;
        var stdout_sha256: [32]u8 = undefined;
        var stderr_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(result.stdout, &stdout_sha256, .{});
        std.crypto.hash.sha2.Sha256.hash(result.stderr, &stderr_sha256, .{});
        return .{
            .allocator = self.allocator,
            .binary_sha256 = actual_binary_sha256,
            .argv_sha256 = argv_sha256,
            .argv_preimage = argv_preimage,
            .request_sha256 = request_sha256,
            .stdout_sha256 = stdout_sha256,
            .stderr_sha256 = stderr_sha256,
            .elapsed_ns = @intCast(elapsed),
            .exit_code = exit_code,
            .stdout = result.stdout,
            .stderr = result.stderr,
        };
    }
};

test "context serving CLI argv evidence binds exact executed arguments" {
    const argv = [_][]const u8{ "/qualified/antfly", "inference", "generate", "gemma4-e2b", "-", "--server", "http://127.0.0.1:43123", "--stream" };
    const preimage = try Runner.argvPreimage(std.testing.allocator, &argv);
    defer std.testing.allocator.free(preimage);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(preimage, &digest, .{});
    try std.testing.expectEqualStrings(
        "c9b045a10fa39207bf5ea77ca6ff4e77d0ff9546c740f892db35713df3ecf422",
        &std.fmt.bytesToHex(digest, .lower),
    );
    try std.testing.expectError(error.InvalidCliQualificationRequest, Runner.argvPreimage(std.testing.allocator, &[_][]const u8{""}));
}

test "context serving CLI request identity matches parent contract" {
    const actual = Runner.requestDigest(.{
        .model = "gemma4-e2b",
        .document_file = "document.txt",
        .document_sha256 = "1111111111111111111111111111111111111111111111111111111111111111",
        .question_file = "question.txt",
        .question_sha256 = "2222222222222222222222222222222222222222222222222222222222222222",
        .max_context_tokens = 8192,
        .max_kv_tokens = 6144,
        .keep_prefix_tokens = 128,
        .keep_recent_tokens = 256,
        .reserved_output_tokens = 32,
        .stream = true,
    });
    try std.testing.expectEqualStrings(
        "22cbd1044f5b33809b648c00a711e9c06264e4b0dd1136d58a3b7c525e9381b6",
        &std.fmt.bytesToHex(actual, .lower),
    );
}

test "context serving CLI public output accepts only exact content plus one newline" {
    const public = "answer\ninside";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(public, &digest, .{});
    try validatePublicStdout("answer\ninside\n", public.len, digest);
    try std.testing.expectError(error.CliPublicOutputMismatch, validatePublicStdout("answer\ninside", public.len, digest));
    try std.testing.expectError(error.CliPublicOutputMismatch, validatePublicStdout("answer\ninside\r\n", public.len, digest));
    try std.testing.expectError(error.CliPublicOutputMismatch, validatePublicStdout("answer\ninside\n\n", public.len, digest));
    try std.testing.expectError(error.CliPublicOutputMismatch, validatePublicStdout("reasoninganswer\ninside\n", public.len, digest));
    var forged = digest;
    forged[0] ^= 1;
    try std.testing.expectError(error.CliPublicOutputMismatch, validatePublicStdout("answer\ninside\n", public.len, forged));
}
