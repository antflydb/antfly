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
const platform = @import("antfly_platform");

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
    onnx_input_node_ids: []u32 = &.{},
    onnx_input_names: [][]u8 = &.{},
    onnx_output_node_ids: []u32 = &.{},
    onnx_output_names: [][]u8 = &.{},
    pjrt_parameter_mode: []const u8 = pjrt_parameter_mode_embedded,
    pjrt_input_bindings: []PjrtInputBindingMeta = &.{},
    pjrt_output_node_ids: []u32 = &.{},
    pjrt_output_bindings: []PjrtOutputBindingMeta = &.{},
    pjrt_input_shapes: [][]i64 = &.{},
    pjrt_output_shapes: [][]i64 = &.{},
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
    return std.fs.path.join(allocator, &.{ home, ".termite", "artifacts" });
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

    fn deinit(self: @This(), allocator: std.mem.Allocator) void {
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
            "{s}.{s}.{s}.{s}.termite-package.json",
            .{ model_key, backend_key, kind_key, mode_key },
        );
    } else try std.fmt.allocPrint(
        allocator,
        "{s}.{s}.{s}.termite-package.json",
        .{ model_key, backend_key, kind_key },
    );
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ artifact_dir, filename });
}

pub fn artifactManifestPath(allocator: std.mem.Allocator, artifact_path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.termite.json", .{artifact_path});
}

pub fn isPackageManifestPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".termite-package.json");
}

pub fn resolveManifestPath(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, path, ".termite.json")) return allocator.dupe(u8, path);
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
    if (!std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    }

    const parent = std.fs.path.dirname(path) orelse return error.BadPathName;
    const base = std.fs.path.basename(path);
    var dir = try std.Io.Dir.openDirAbsolute(io, parent, .{});
    defer dir.close(io);
    return dir.readFileAlloc(io, base, allocator, .limited(std.math.maxInt(usize)));
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
    return std.mem.eql(u8, manifest.backend, req.backend) and
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
        if (!std.mem.endsWith(u8, entry.name, ".termite.json")) continue;

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

test "artifactManifestPath appends termite sidecar suffix" {
    const path = try artifactManifestPath(std.testing.allocator, "/tmp/model.mlpackage");
    defer std.testing.allocator.free(path);
    try std.testing.expectEqualStrings("/tmp/model.mlpackage.termite.json", path);
}

test "packageManifestPath includes model, backend, kind, and parameter mode" {
    const path = try packageManifestPath(std.testing.allocator, "/tmp/artifacts", "xla", "/tmp/model/gpt2", "pjrt_executable", "inputs");
    defer std.testing.allocator.free(path);
    try std.testing.expectEqualStrings("/tmp/artifacts/gpt2.xla.pjrt_executable.inputs.termite-package.json", path);
}

test "modelArtifactNamespace keeps owner and model" {
    const namespace = try modelArtifactNamespace(std.testing.allocator, "/Users/test/.termite/models/openai-community/gpt2");
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
    const direct = try resolveManifestPath(std.testing.allocator, "/tmp/model.onnx");
    defer std.testing.allocator.free(direct);
    try std.testing.expectEqualStrings("/tmp/model.onnx.termite.json", direct);

    const sidecar = try resolveManifestPath(std.testing.allocator, "/tmp/model.onnx.termite.json");
    defer std.testing.allocator.free(sidecar);
    try std.testing.expectEqualStrings("/tmp/model.onnx.termite.json", sidecar);
}

test "isPackageManifestPath detects package sidecars" {
    try std.testing.expect(isPackageManifestPath("/tmp/model.xla.termite-package.json"));
    try std.testing.expect(!isPackageManifestPath("/tmp/model.onnx.termite.json"));
}

test "matchesRequest matches exact artifact shape and backend" {
    const manifest: Manifest = .{
        .kind = "onnx_graph",
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.onnx",
        .partition_signature = "abc123",
        .prompt_tokens = 128,
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = false,
        .chat_template_applied = true,
    };
    try std.testing.expect(matchesRequest(manifest, .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
    try std.testing.expect(!matchesRequest(manifest, .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 128,
        .attention_mode = "paged_prefill",
    }));
    try std.testing.expect(matchesRequest(manifest, .{
        .backend = "onnx",
        .kind = "onnx_graph",
        .artifact_role = artifact_role_prefill,
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
    try std.testing.expect(!matchesRequest(manifest, .{
        .backend = "onnx",
        .kind = "onnx_debug_graph",
        .artifact_role = artifact_role_decode,
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
    try std.testing.expect(matchesRequest(manifest, .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .seq_len = 128,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .partition_signature = "abc123",
    }));
    try std.testing.expect(!matchesRequest(manifest, .{
        .backend = "onnx",
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

test "defaultArtifactDirForModel uses ~/.termite/artifacts layout when HOME is set" {
    if (platform.env.getenv("HOME")) |home| {
        const path = try defaultArtifactDirForModel(std.testing.allocator, "/tmp/ggml-org/gemma-4-e2b-it-gguf", "onnx");
        defer std.testing.allocator.free(path);
        const expected_prefix = try std.fs.path.join(std.testing.allocator, &.{ home, ".termite", "artifacts" });
        defer std.testing.allocator.free(expected_prefix);
        try std.testing.expect(std.mem.startsWith(u8, path, expected_prefix));
        try std.testing.expect(std.mem.endsWith(u8, path, "ggml-org/gemma-4-e2b-it-gguf/onnx"));
    }
}

test "writeManifest and readManifest roundtrip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "artifact.termite.json" });
    defer allocator.free(manifest_path);

    const manifest: Manifest = .{
        .kind = "onnx_graph",
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.onnx",
        .prompt_tokens = 64,
        .seq_len = 64,
        .query_seq_len = 64,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
    };
    try writeManifest(allocator, io, manifest_path, manifest);

    var parsed = try readManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("onnx_graph", parsed.value.kind);
    try std.testing.expectEqualStrings("onnx", parsed.value.backend);
    try std.testing.expectEqual(@as(usize, 64), parsed.value.seq_len);
    try std.testing.expect(parsed.value.raw_prompt);
    try std.testing.expect(!parsed.value.chat_template_applied);
}

test "writePackageManifest and readPackageManifest roundtrip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "artifact.termite-package.json" });
    defer allocator.free(manifest_path);

    const manifest: PackageManifest = .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.exec.termite.json",
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = "/tmp/model.decode.s3.exec.termite.json",
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

    const manifest_path = try std.fs.path.resolve(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "absolute-artifact.termite.json" });
    defer allocator.free(manifest_path);

    const manifest: Manifest = .{
        .kind = "onnx_graph",
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.onnx",
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
    try std.testing.expectEqualStrings("onnx_graph", parsed.value.kind);
    try std.testing.expectEqualStrings("/tmp/model.onnx", parsed.value.artifact_path);
}

test "writePackageManifest and readPackageManifest support absolute paths" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_path = try std.fs.path.resolve(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "absolute-package.termite-package.json" });
    defer allocator.free(manifest_path);

    const manifest: PackageManifest = .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = pjrt_parameter_mode_embedded,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.exec.termite.json",
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

    const match_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "good.onnx.termite.json" });
    defer allocator.free(match_manifest_path);
    try writeManifest(allocator, io, match_manifest_path, .{
        .kind = "onnx_graph",
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.good.onnx",
        .prompt_tokens = 64,
        .seq_len = 64,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = false,
        .chat_template_applied = true,
    });

    const non_match_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "other.onnx.termite.json" });
    defer allocator.free(non_match_manifest_path);
    try writeManifest(allocator, io, non_match_manifest_path, .{
        .kind = "onnx_graph",
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.prefill.onnx",
        .prompt_tokens = 64,
        .seq_len = 64,
        .query_seq_len = 64,
        .attention_mode = "paged_prefill",
        .raw_prompt = false,
        .chat_template_applied = true,
    });

    var found = (try findMatchingArtifactPath(allocator, io, base_dir, .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .seq_len = 64,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    })).?;
    defer found.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/model.good.onnx", found.artifact_path);
    try std.testing.expect(std.mem.endsWith(u8, found.manifest_path, "good.onnx.termite.json"));
}

test "findUniqueMatchingArtifactPath rejects ambiguous matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    inline for (.{ "a", "b" }) |name| {
        const manifest_path = try std.fs.path.join(allocator, &.{ base_dir, name ++ ".onnx.termite.json" });
        defer allocator.free(manifest_path);
        try writeManifest(allocator, io, manifest_path, .{
            .kind = "onnx_graph",
            .backend = "onnx",
            .model_dir = "/tmp/model",
            .artifact_path = "/tmp/model." ++ name ++ ".onnx",
            .prompt_tokens = 64,
            .seq_len = 64,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
            .raw_prompt = false,
            .chat_template_applied = true,
        });
    }

    try std.testing.expectError(error.AmbiguousCompiledArtifact, findUniqueMatchingArtifactPath(allocator, io, base_dir, .{
        .backend = "onnx",
        .kind = "onnx_graph",
        .artifact_role = artifact_role_prefill,
        .model_dir = "/tmp/model",
        .seq_len = 64,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    }));
}
