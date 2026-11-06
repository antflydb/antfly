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
const platform = @import("antfly_platform");

/// A load-only artifact's compiler footprint, bound to its bytes and provider.
/// HLO remains exportable/inspectable offline, but execution must not compile
/// unknown native code inside a supposedly bounded inference invocation.
pub const PjrtMemoryPlan = struct {
    version: u32 = 1,
    platform: enum { cpu, cuda },
    artifact_sha256: [32]u8,
    plugin_sha256: [32]u8,
    memory: PjrtMemoryStats,
};

pub const PjrtMemoryStats = struct {
    code: usize = 0,
    arguments: usize = 0,
    outputs: usize = 0,
    temporary: usize = 0,
    host_code: usize = 0,
    host_arguments: usize = 0,
    host_outputs: usize = 0,
    host_temporary: usize = 0,
    peak: usize = 0,
    total: usize = 0,

    pub fn fromCompiler(stats: anytype) @This() {
        var result: @This() = .{};
        inline for (@typeInfo(@This()).@"struct".field_names) |name| @field(result, name) = @field(stats, name);
        return result;
    }
};

pub const Manifest = struct {
    version: u32 = 1,
    kind: []const u8,
    artifact_role: []const u8 = artifact_role_prefill,
    backend: []const u8,
    model_dir: []const u8,
    artifact_path: []const u8,
    source_path: []const u8 = "",
    partition_signature: []const u8 = "",
    prompt_tokens: usize,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
    raw_prompt: bool,
    chat_template_applied: bool,
    attention_bindings: []AttentionBindingMeta = &.{},
    pjrt_parameter_mode: []const u8 = pjrt_parameter_mode_embedded,
    pjrt_input_bindings: []PjrtInputBindingMeta = &.{},
    pjrt_output_node_ids: []u32 = &.{},
    pjrt_output_bindings: []PjrtOutputBindingMeta = &.{},
    pjrt_input_shapes: [][]i64 = &.{},
    pjrt_output_shapes: [][]i64 = &.{},
    pjrt_memory_plan: ?PjrtMemoryPlan = null,
};

pub const PackageArtifactEntry = struct {
    manifest_path: []const u8,
    artifact_path: []const u8,
    artifact_role: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
};

pub const PackageManifest = struct {
    version: u32 = 1,
    backend: []const u8,
    model_dir: []const u8,
    kind: []const u8,
    pjrt_parameter_mode: []const u8 = pjrt_parameter_mode_embedded,
    artifacts: []const PackageArtifactEntry = &.{},
};

pub const AttentionBindingMeta = struct {
    node_id: u32,
    k_node_id: u32,
    v_node_id: u32,
    layer_index: u32,
    skip_kv_write: bool,
};

pub const PjrtValueBindingMeta = struct {
    kind: []const u8,
    node_id: u32,
    name: []const u8 = "",
    layer_index: ?u32 = null,
};

pub const PjrtInputBindingMeta = PjrtValueBindingMeta;
pub const PjrtOutputBindingMeta = PjrtValueBindingMeta;

pub const pjrt_binding_graph_node = "graph_node";
pub const pjrt_binding_embedding_ids = "embedding_ids";
pub const pjrt_binding_input_ids = "input_ids";
pub const pjrt_binding_past_key = "past_key";
pub const pjrt_binding_past_value = "past_value";
pub const pjrt_binding_present_key = "present_key";
pub const pjrt_binding_present_value = "present_value";
pub const pjrt_parameter_mode_embedded = "embedded";
pub const pjrt_parameter_mode_inputs = "inputs";

pub fn pjrtBindingIsInputIds(binding: PjrtValueBindingMeta) bool {
    return std.mem.eql(u8, binding.kind, pjrt_binding_embedding_ids) or
        std.mem.eql(u8, binding.kind, pjrt_binding_input_ids);
}

pub fn pjrtBindingIsKvCache(binding: PjrtValueBindingMeta) bool {
    return std.mem.eql(u8, binding.kind, pjrt_binding_past_key) or
        std.mem.eql(u8, binding.kind, pjrt_binding_past_value) or
        std.mem.eql(u8, binding.kind, pjrt_binding_present_key) or
        std.mem.eql(u8, binding.kind, pjrt_binding_present_value);
}

pub const MatchRequest = struct {
    require_pjrt_memory_plan: bool = false,
    backend: []const u8,
    model_dir: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
    kind: ?[]const u8 = null,
    artifact_role: ?[]const u8 = null,
    partition_signature: ?[]const u8 = null,
    pjrt_parameter_mode: ?[]const u8 = null,
};

pub const PackageEntryMatchRequest = struct {
    artifact_role: ?[]const u8 = null,
    seq_len: ?usize = null,
    query_seq_len: ?usize = null,
    attention_mode: ?[]const u8 = null,
};

pub const artifact_role_prefill = "prefill";
pub const artifact_role_decode = "decode";
pub const artifact_role_debug = "debug";

pub const LocatedArtifact = struct {
    manifest_path: []u8,
    artifact_path: []u8,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.manifest_path);
        allocator.free(self.artifact_path);
        self.* = undefined;
    }
};

pub fn defaultArtifactRoot(allocator: std.mem.Allocator) ![]u8 {
    const home = platform.env.getenv("HOME") orelse return allocator.dupe(u8, "./artifacts");
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "artifacts" });
}

pub fn defaultArtifactDirForModel(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: []const u8,
) ![]u8 {
    const root = try defaultArtifactRoot(allocator);
    defer allocator.free(root);
    const namespace = try modelArtifactNamespace(allocator, model_dir);
    defer namespace.deinit(allocator);
    return std.fs.path.join(allocator, &.{ root, namespace.owner, namespace.model, backend });
}

fn sanitizeArtifactPathComponent(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, value.len);
    errdefer allocator.free(out);
    for (value, 0..) |ch, i| {
        out[i] = switch (ch) {
            'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.' => ch,
            else => '_',
        };
    }
    return out;
}

const ModelArtifactNamespace = struct {
    owner: []u8,
    model: []u8,

    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        allocator.free(self.owner);
        allocator.free(self.model);
    }
};

fn modelArtifactNamespace(allocator: std.mem.Allocator, model_dir: []const u8) !ModelArtifactNamespace {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(allocator);

    var iter = std.mem.tokenizeScalar(u8, model_dir, std.fs.path.sep);
    while (iter.next()) |part| {
        try parts.append(allocator, part);
    }
    if (parts.items.len == 0) return error.BadPathName;

    const model_part = parts.items[parts.items.len - 1];
    const owner_part = if (parts.items.len >= 2) parts.items[parts.items.len - 2] else model_part;
    return .{
        .owner = try sanitizeArtifactPathComponent(allocator, owner_part),
        .model = try sanitizeArtifactPathComponent(allocator, model_part),
    };
}

pub fn packageManifestPath(
    allocator: std.mem.Allocator,
    artifact_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    kind: []const u8,
    parameter_mode: ?[]const u8,
) ![]u8 {
    const model_key = try sanitizeArtifactPathComponent(allocator, std.fs.path.basename(model_dir));
    defer allocator.free(model_key);
    const backend_key = try sanitizeArtifactPathComponent(allocator, backend);
    defer allocator.free(backend_key);
    const kind_key = try sanitizeArtifactPathComponent(allocator, kind);
    defer allocator.free(kind_key);
    const filename = if (parameter_mode) |mode| blk: {
        const mode_key = try sanitizeArtifactPathComponent(allocator, mode);
        defer allocator.free(mode_key);
        break :blk try std.fmt.allocPrint(
            allocator,
            "{s}.{s}.{s}.{s}.antfly-inference-package.json",
            .{ model_key, backend_key, kind_key, mode_key },
        );
    } else try std.fmt.allocPrint(
        allocator,
        "{s}.{s}.{s}.antfly-inference-package.json",
        .{ model_key, backend_key, kind_key },
    );
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ artifact_dir, filename });
}

pub fn artifactManifestPath(allocator: std.mem.Allocator, artifact_path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.inference.json", .{artifact_path});
}

pub fn isPackageManifestPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".antfly-inference-package.json");
}

pub fn resolveManifestPath(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, path, ".inference.json")) return allocator.dupe(u8, path);
    return artifactManifestPath(allocator, path);
}

pub fn writeManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
    manifest: Manifest,
) !void {
    const manifest_json = try std.json.Stringify.valueAlloc(allocator, manifest, .{ .whitespace = .indent_2 });
    defer allocator.free(manifest_json);
    try writeFileAtPath(io, manifest_path, manifest_json);
}

pub fn readManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
) !std.json.Parsed(Manifest) {
    const raw = try readFileAllocAtPath(io, manifest_path, allocator);
    defer allocator.free(raw);
    return std.json.parseFromSlice(Manifest, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

pub fn writePackageManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
    manifest: PackageManifest,
) !void {
    const manifest_json = try std.json.Stringify.valueAlloc(allocator, manifest, .{ .whitespace = .indent_2 });
    defer allocator.free(manifest_json);
    try writeFileAtPath(io, manifest_path, manifest_json);
}

pub fn readPackageManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
) !std.json.Parsed(PackageManifest) {
    const raw = try readFileAllocAtPath(io, manifest_path, allocator);
    defer allocator.free(raw);
    return std.json.parseFromSlice(PackageManifest, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn writeFileAtPath(io: std.Io, path: []const u8, data: []const u8) !void {
    if (!std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = path,
            .data = data,
        });
        return;
    }

    const parent = std.fs.path.dirname(path) orelse return error.BadPathName;
    const base = std.fs.path.basename(path);
    var dir = try std.Io.Dir.openDirAbsolute(io, parent, .{});
    defer dir.close(io);
    try dir.writeFile(io, .{
        .sub_path = base,
        .data = data,
    });
}

fn readFileAllocAtPath(io: std.Io, path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const info = try file.stat(io);
    const size = std.math.cast(usize, info.size) orelse return error.ResourceLimitExceeded;
    // A single exact allocation avoids geometric growth peaks, and an opened
    // descriptor's size bounds memory even if the path is replaced or grows.
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    if (try file.readPositionalAll(io, bytes, 0) != size) return error.InvalidArtifact;
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, info.size) != 0) return error.InvalidArtifact;
    return bytes;
}

pub fn validatePackageManifest(
    package: PackageManifest,
    backend: []const u8,
    model_dir: []const u8,
    kind: []const u8,
    parameter_mode: ?[]const u8,
) !void {
    if (!std.mem.eql(u8, package.backend, backend) or
        !std.mem.eql(u8, package.model_dir, model_dir) or
        !std.mem.eql(u8, package.kind, kind))
    {
        return error.InvalidArtifact;
    }
    if (parameter_mode) |mode| {
        if (!std.mem.eql(u8, package.pjrt_parameter_mode, mode)) return error.InvalidArtifact;
    }
}

pub fn packageEntryMatches(entry: PackageArtifactEntry, req: PackageEntryMatchRequest) bool {
    return (req.artifact_role == null or std.mem.eql(u8, entry.artifact_role, req.artifact_role.?)) and
        (req.seq_len == null or entry.seq_len == req.seq_len.?) and
        (req.query_seq_len == null or entry.query_seq_len == req.query_seq_len.?) and
        (req.attention_mode == null or std.mem.eql(u8, entry.attention_mode, req.attention_mode.?));
}

pub fn findUniqueMatchingPackageEntry(
    package: PackageManifest,
    req: PackageEntryMatchRequest,
) !?PackageArtifactEntry {
    var found: ?PackageArtifactEntry = null;
    for (package.artifacts) |entry| {
        if (!packageEntryMatches(entry, req)) continue;
        if (found != null) return error.AmbiguousCompiledArtifact;
        found = entry;
    }
    return found;
}

pub fn matchesRequest(manifest: Manifest, req: MatchRequest) bool {
    return (!req.require_pjrt_memory_plan or hasManagedPjrtPlan(manifest)) and std.mem.eql(u8, manifest.backend, req.backend) and
        (req.kind == null or std.mem.eql(u8, manifest.kind, req.kind.?)) and
        (req.artifact_role == null or std.mem.eql(u8, manifest.artifact_role, req.artifact_role.?)) and
        (req.partition_signature == null or std.mem.eql(u8, manifest.partition_signature, req.partition_signature.?)) and
        (req.pjrt_parameter_mode == null or std.mem.eql(u8, manifest.pjrt_parameter_mode, req.pjrt_parameter_mode.?)) and
        std.mem.eql(u8, manifest.model_dir, req.model_dir) and
        manifest.seq_len == req.seq_len and
        manifest.query_seq_len == req.query_seq_len and
        std.mem.eql(u8, manifest.attention_mode, req.attention_mode);
}

pub fn findMatchingArtifactPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    req: MatchRequest,
) !?LocatedArtifact {
    return findMatchingArtifactPathMode(allocator, io, search_dir, req, false);
}

pub fn findUniqueMatchingArtifactPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    req: MatchRequest,
) !?LocatedArtifact {
    return findMatchingArtifactPathMode(allocator, io, search_dir, req, true);
}

fn findMatchingArtifactPathMode(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    req: MatchRequest,
    require_unique: bool,
) !?LocatedArtifact {
    var dir = if (std.fs.path.isAbsolute(search_dir))
        try std.Io.Dir.openDirAbsolute(io, search_dir, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, search_dir, .{ .iterate = true });
    defer dir.close(io);

    var found: ?LocatedArtifact = null;
    errdefer if (found) |*located| located.deinit(allocator);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".inference.json")) continue;

        const manifest_path = try std.fs.path.join(allocator, &.{ search_dir, entry.name });
        errdefer allocator.free(manifest_path);

        var parsed = readManifest(allocator, io, manifest_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => {
                allocator.free(manifest_path);
                continue;
            },
            else => return err,
        };
        defer parsed.deinit();

        if (!matchesRequest(parsed.value, req)) {
            allocator.free(manifest_path);
            continue;
        }

        if (require_unique and found != null) {
            return error.AmbiguousCompiledArtifact;
        }

        found = .{
            .manifest_path = manifest_path,
            .artifact_path = try allocator.dupe(u8, parsed.value.artifact_path),
        };

        if (!require_unique) return found;
    }

    return found;
}

test "artifactManifestPath appends inference sidecar suffix" {
    const path = try artifactManifestPath(std.testing.allocator, "/tmp/model.mlpackage");
    defer std.testing.allocator.free(path);
    try std.testing.expectEqualStrings("/tmp/model.mlpackage.inference.json", path);
}

test "packageManifestPath includes model, backend, kind, and parameter mode" {
    const path = try packageManifestPath(std.testing.allocator, "/tmp/artifacts", "xla", "/tmp/model/gpt2", "pjrt_executable", "inputs");
    defer std.testing.allocator.free(path);
    try std.testing.expectEqualStrings("/tmp/artifacts/gpt2.xla.pjrt_executable.inputs.antfly-inference-package.json", path);
}

test "modelArtifactNamespace keeps owner and model" {
    const namespace = try modelArtifactNamespace(std.testing.allocator, "/Users/test/.antfly/inference/models/openai-community/gpt2");
    defer namespace.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("openai-community", namespace.owner);
    try std.testing.expectEqualStrings("gpt2", namespace.model);
}

test "modelArtifactNamespace falls back to basename for single-component paths" {
    const namespace = try modelArtifactNamespace(std.testing.allocator, "gpt2");
    defer namespace.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("gpt2", namespace.owner);
    try std.testing.expectEqualStrings("gpt2", namespace.model);
}

test "resolveManifestPath accepts artifact or sidecar path" {
    const direct = try resolveManifestPath(std.testing.allocator, "/tmp/model.exec");
    defer std.testing.allocator.free(direct);
    try std.testing.expectEqualStrings("/tmp/model.exec.inference.json", direct);

    const sidecar = try resolveManifestPath(std.testing.allocator, "/tmp/model.exec.inference.json");
    defer std.testing.allocator.free(sidecar);
    try std.testing.expectEqualStrings("/tmp/model.exec.inference.json", sidecar);
}

test "isPackageManifestPath detects package sidecars" {
    try std.testing.expect(isPackageManifestPath("/tmp/model.xla.antfly-inference-package.json"));
    try std.testing.expect(!isPackageManifestPath("/tmp/model.exec.inference.json"));
}

test "matchesRequest matches exact artifact shape and backend" {
    const manifest: Manifest = .{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.exec",
        .partition_signature = "abc123",
        .prompt_tokens = 128,
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = false,
        .chat_template_applied = true,
    };
    try std.testing.expect(matchesRequest(manifest, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
    try std.testing.expect(!matchesRequest(manifest, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 128,
        .attention_mode = "paged_prefill",
    }));
    try std.testing.expect(matchesRequest(manifest, .{
        .backend = "xla",
        .kind = "pjrt_executable",
        .artifact_role = artifact_role_prefill,
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
    try std.testing.expect(!matchesRequest(manifest, .{
        .backend = "xla",
        .kind = "pjrt_debug_executable",
        .artifact_role = artifact_role_decode,
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
    try std.testing.expect(matchesRequest(manifest, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .partition_signature = "abc123",
    }));
    try std.testing.expect(!matchesRequest(manifest, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .partition_signature = "def456",
    }));
}

test "PJRT binding helpers classify semantic input and KV bindings" {
    try std.testing.expect(pjrtBindingIsInputIds(.{
        .kind = pjrt_binding_input_ids,
        .node_id = 1,
        .name = "input_ids",
    }));
    try std.testing.expect(pjrtBindingIsInputIds(.{
        .kind = pjrt_binding_embedding_ids,
        .node_id = 1,
    }));
    try std.testing.expect(pjrtBindingIsKvCache(.{
        .kind = pjrt_binding_past_key,
        .node_id = 2,
        .name = "past_key_values.0.key",
        .layer_index = 0,
    }));
    try std.testing.expect(!pjrtBindingIsKvCache(.{
        .kind = pjrt_binding_graph_node,
        .node_id = 3,
    }));
}

test "defaultArtifactDirForModel uses ~/.antfly/inference/artifacts layout when HOME is set" {
    if (platform.env.getenv("HOME")) |home| {
        const path = try defaultArtifactDirForModel(std.testing.allocator, "/tmp/ggml-org/gemma-4-e2b-it-gguf", "xla");
        defer std.testing.allocator.free(path);
        const expected_prefix = try std.fs.path.join(std.testing.allocator, &.{ home, ".antfly", "inference", "artifacts" });
        defer std.testing.allocator.free(expected_prefix);
        try std.testing.expect(std.mem.startsWith(u8, path, expected_prefix));
        try std.testing.expect(std.mem.endsWith(u8, path, "ggml-org/gemma-4-e2b-it-gguf/xla"));
    }
}

test "writeManifest and readManifest roundtrip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "artifact.inference.json" });
    defer allocator.free(manifest_path);

    const manifest: Manifest = .{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.exec",
        .prompt_tokens = 64,
        .seq_len = 64,
        .query_seq_len = 64,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_memory_plan = .{ .platform = .cuda, .artifact_sha256 = @splat(17), .plugin_sha256 = @splat(42), .memory = .{ .code = 1024, .temporary = 2048 } },
    };
    try writeManifest(allocator, io, manifest_path, manifest);

    var parsed = try readManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("pjrt_executable", parsed.value.kind);
    try std.testing.expectEqualStrings("xla", parsed.value.backend);
    try std.testing.expectEqual(@as(usize, 64), parsed.value.seq_len);
    try std.testing.expect(parsed.value.raw_prompt);
    try std.testing.expect(!parsed.value.chat_template_applied);
    try std.testing.expectEqualDeep(manifest.pjrt_memory_plan.?, parsed.value.pjrt_memory_plan.?);
}

test "writePackageManifest and readPackageManifest roundtrip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "artifact.antfly-inference-package.json" });
    defer allocator.free(manifest_path);

    const manifest: PackageManifest = .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = "/tmp/model.decode.s3.exec.inference.json",
                .artifact_path = "/tmp/model.decode.s3.exec",
                .artifact_role = artifact_role_decode,
                .seq_len = 3,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    };
    try writePackageManifest(allocator, io, manifest_path, manifest);

    var parsed = try readPackageManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("xla", parsed.value.backend);
    try std.testing.expectEqualStrings("pjrt_executable", parsed.value.kind);
    try std.testing.expectEqualStrings(pjrt_parameter_mode_inputs, parsed.value.pjrt_parameter_mode);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.artifacts.len);
    try std.testing.expectEqualStrings("/tmp/model.decode.s3.exec", parsed.value.artifacts[1].artifact_path);
}

test "writeManifest and readManifest support absolute paths" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.resolve(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "absolute-artifact.inference.json" });
    defer allocator.free(manifest_path);

    const manifest: Manifest = .{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.exec",
        .prompt_tokens = 8,
        .seq_len = 8,
        .query_seq_len = 8,
        .attention_mode = "paged_prefill",
        .raw_prompt = false,
        .chat_template_applied = true,
    };
    try writeManifest(allocator, io, manifest_path, manifest);

    var parsed = try readManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("pjrt_executable", parsed.value.kind);
    try std.testing.expectEqualStrings("/tmp/model.exec", parsed.value.artifact_path);
}

test "writePackageManifest and readPackageManifest support absolute paths" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.resolve(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "absolute-package.antfly-inference-package.json" });
    defer allocator.free(manifest_path);

    const manifest: PackageManifest = .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = pjrt_parameter_mode_embedded,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
        },
    };
    try writePackageManifest(allocator, io, manifest_path, manifest);

    var parsed = try readPackageManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("xla", parsed.value.backend);
    try std.testing.expectEqualStrings("pjrt_executable", parsed.value.kind);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.artifacts.len);
}

test "findMatchingArtifactPath returns the matching sidecar artifact" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    const match_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "good.exec.inference.json" });
    defer allocator.free(match_manifest_path);
    try writeManifest(allocator, io, match_manifest_path, .{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.good.exec",
        .prompt_tokens = 64,
        .seq_len = 64,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = false,
        .chat_template_applied = true,
    });

    const non_match_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "other.exec.inference.json" });
    defer allocator.free(non_match_manifest_path);
    try writeManifest(allocator, io, non_match_manifest_path, .{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.prefill.exec",
        .prompt_tokens = 64,
        .seq_len = 64,
        .query_seq_len = 64,
        .attention_mode = "paged_prefill",
        .raw_prompt = false,
        .chat_template_applied = true,
    });

    var found = (try findMatchingArtifactPath(allocator, io, base_dir, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .seq_len = 64,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    })).?;
    defer found.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/model.good.exec", found.artifact_path);
    try std.testing.expect(std.mem.endsWith(u8, found.manifest_path, "good.exec.inference.json"));
}

test "findUniqueMatchingArtifactPath rejects ambiguous matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    inline for (.{ "a", "b" }) |name| {
        const manifest_path = try std.fs.path.join(allocator, &.{ base_dir, name ++ ".exec.inference.json" });
        defer allocator.free(manifest_path);
        try writeManifest(allocator, io, manifest_path, .{
            .kind = "pjrt_executable",
            .backend = "xla",
            .model_dir = "/tmp/model",
            .artifact_path = "/tmp/model." ++ name ++ ".exec",
            .prompt_tokens = 64,
            .seq_len = 64,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
            .raw_prompt = false,
            .chat_template_applied = true,
        });
    }

    try std.testing.expectError(error.AmbiguousCompiledArtifact, findUniqueMatchingArtifactPath(allocator, io, base_dir, .{
        .backend = "xla",
        .kind = "pjrt_executable",
        .artifact_role = artifact_role_prefill,
        .model_dir = "/tmp/model",
        .seq_len = 64,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
}

pub fn hasManagedPjrtPlan(manifest: Manifest) bool {
    const plan = manifest.pjrt_memory_plan orelse return false;
    return plan.version == 1 and (std.mem.eql(u8, manifest.kind, "pjrt_executable") or std.mem.eql(u8, manifest.kind, "pjrt_partition_executable"));
}

/// Select only executable packages this loader can actually execute. Old HLO
/// and unprofiled caches must not hide a usable standalone artifact.
pub fn findManagedPjrtPackage(allocator: std.mem.Allocator, io: std.Io, directory: []const u8, backend: []const u8, model_dir: []const u8, seq_len: usize, query_seq_len: usize, attention_mode: []const u8) !?[]u8 {
    if (!std.mem.eql(u8, backend, "xla")) return null;
    for ([_][]const u8{ pjrt_parameter_mode_embedded, pjrt_parameter_mode_inputs }) |mode| {
        const path = try packageManifestPath(allocator, directory, backend, model_dir, "pjrt_executable", mode);
        defer allocator.free(path);
        var package = readPackageManifest(allocator, io, path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer package.deinit();
        try validatePackageManifest(package.value, backend, model_dir, "pjrt_executable", mode);
        const entry = (try findUniqueMatchingPackageEntry(package.value, .{ .artifact_role = artifact_role_prefill, .seq_len = seq_len, .query_seq_len = query_seq_len, .attention_mode = attention_mode })) orelse continue;
        var prefill = readManifest(allocator, io, entry.manifest_path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer prefill.deinit();
        if (!matchesRequest(prefill.value, .{ .backend = backend, .model_dir = model_dir, .kind = "pjrt_executable", .artifact_role = artifact_role_prefill, .seq_len = seq_len, .query_seq_len = query_seq_len, .attention_mode = attention_mode, .pjrt_parameter_mode = mode, .require_pjrt_memory_plan = true })) continue;
        return try allocator.dupe(u8, path);
    }
    return null;
}

/// Managed standalone lookup uses the same parameter-mode preference as
/// package lookup. Duplicate artifacts within one mode remain an error.
pub fn findManagedPjrtPrefill(allocator: std.mem.Allocator, io: std.Io, directory: []const u8, backend: []const u8, model_dir: []const u8, seq_len: usize, query_seq_len: usize, attention_mode: []const u8) !?LocatedArtifact {
    if (!std.mem.eql(u8, backend, "xla")) return null;
    for ([_][]const u8{ pjrt_parameter_mode_embedded, pjrt_parameter_mode_inputs }) |mode| {
        if (try findUniqueMatchingArtifactPath(allocator, io, directory, .{ .backend = backend, .model_dir = model_dir, .kind = "pjrt_executable", .artifact_role = artifact_role_prefill, .seq_len = seq_len, .query_seq_len = query_seq_len, .attention_mode = attention_mode, .pjrt_parameter_mode = mode, .require_pjrt_memory_plan = true })) |found| return found;
    }
    return null;
}

test "managed PJRT standalone lookup prefers embedded then inputs and rejects duplicates within a mode" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var artifact = Manifest{ .kind = "pjrt_executable", .backend = "xla", .model_dir = root, .artifact_path = "model.exec", .prompt_tokens = 1, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .raw_prompt = true, .chat_template_applied = false, .pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{} } };
    const embedded = try std.fs.path.join(a, &.{ root, "embedded.inference.json" });
    defer a.free(embedded);
    const inputs = try std.fs.path.join(a, &.{ root, "inputs.inference.json" });
    defer a.free(inputs);
    const duplicate = try std.fs.path.join(a, &.{ root, "duplicate.inference.json" });
    defer a.free(duplicate);
    try writeManifest(a, io, embedded, artifact);
    artifact.pjrt_parameter_mode = pjrt_parameter_mode_inputs;
    try writeManifest(a, io, inputs, artifact);
    {
        var found = (try findManagedPjrtPrefill(a, io, root, "xla", root, 1, 1, "paged_prefill")).?;
        defer found.deinit(a);
        try std.testing.expectEqualStrings(embedded, found.manifest_path);
    }
    // A legacy embedded artifact must not hide a usable profiled inputs file.
    artifact.pjrt_parameter_mode = pjrt_parameter_mode_embedded;
    artifact.pjrt_memory_plan = null;
    try writeManifest(a, io, embedded, artifact);
    {
        var found = (try findManagedPjrtPrefill(a, io, root, "xla", root, 1, 1, "paged_prefill")).?;
        defer found.deinit(a);
        try std.testing.expectEqualStrings(inputs, found.manifest_path);
    }
    var profiled = try readManifest(a, io, inputs);
    defer profiled.deinit();
    try writeManifest(a, io, duplicate, profiled.value);
    try std.testing.expectError(error.AmbiguousCompiledArtifact, findManagedPjrtPrefill(a, io, root, "xla", root, 1, 1, "paged_prefill"));
}
