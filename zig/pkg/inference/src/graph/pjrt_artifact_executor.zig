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
const ml = @import("ml");
const pjrt_lib = @import("pjrt");

const compiled_artifact = @import("../compiled_artifact.zig");
const ops = @import("../ops/ops.zig");
const pjrt_compiler = @import("pjrt_compiler.zig");
const pjrt_executor = @import("pjrt_executor.zig");

pub const ArtifactLoadMode = enum {
    /// Termite stores HLO bytes and compiles them through the configured PJRT
    /// plugin when the artifact is loaded.
    hlo_compile_on_load,
    /// Future mode: Termite stores/loads a plugin-native executable artifact
    /// without recompiling HLO on load.
    load_only_executable,
};

pub fn artifactLoadMode(client: *const pjrt_lib.pjrt.Client, kind: []const u8) !ArtifactLoadMode {
    if (std.mem.eql(u8, kind, "pjrt_hlo") or std.mem.eql(u8, kind, "pjrt_partition_hlo")) return .hlo_compile_on_load;
    if (!std.mem.eql(u8, kind, "pjrt_executable") and
        !std.mem.eql(u8, kind, "pjrt_partition_executable")) return error.UnsupportedArtifactKind;
    const support = client.executableArtifactSupport();
    return if (support.loadOnlyExecutableArtifacts())
        .load_only_executable
    else
        error.UnsupportedArtifactKind;
}

pub fn createModelExecutorFromArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*pjrt_executor.PjrtModelExecutor {
    return createModelExecutorFromArtifacts(allocator, io, artifact, &.{}, graph, host_backend, client);
}

pub fn createModelExecutorFromManifestPaths(
    allocator: std.mem.Allocator,
    io: std.Io,
    prefill_manifest_path: []const u8,
    decode_manifest_paths: []const []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*pjrt_executor.PjrtModelExecutor {
    var prefill_manifest = try compiled_artifact.readManifest(allocator, io, prefill_manifest_path);
    defer prefill_manifest.deinit();

    const decode_manifests = try allocator.alloc(std.json.Parsed(compiled_artifact.Manifest), decode_manifest_paths.len);
    var decode_manifests_initialized: usize = 0;
    defer {
        for (decode_manifests[0..decode_manifests_initialized]) |*manifest| manifest.deinit();
        allocator.free(decode_manifests);
    }
    const decode_artifacts = try allocator.alloc(compiled_artifact.Manifest, decode_manifest_paths.len);
    defer allocator.free(decode_artifacts);
    for (decode_manifest_paths, 0..) |path, i| {
        decode_manifests[i] = try compiled_artifact.readManifest(allocator, io, path);
        decode_manifests_initialized += 1;
        decode_artifacts[i] = decode_manifests[i].value;
    }

    return createModelExecutorFromArtifacts(
        allocator,
        io,
        prefill_manifest.value,
        decode_artifacts,
        graph,
        host_backend,
        client,
    );
}

pub fn createModelExecutorFromPackageManifestPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_manifest_path: []const u8,
    prefill_seq_len: usize,
    prefill_query_seq_len: usize,
    prefill_attention_mode: []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*pjrt_executor.PjrtModelExecutor {
    var package = try compiled_artifact.readPackageManifest(allocator, io, package_manifest_path);
    defer package.deinit();

    if (!std.mem.eql(u8, package.value.kind, "pjrt_hlo") and
        !std.mem.eql(u8, package.value.kind, "pjrt_executable"))
    {
        return error.UnsupportedArtifactKind;
    }
    try compiled_artifact.validatePackageManifest(
        package.value,
        "xla",
        package.value.model_dir,
        package.value.kind,
        package.value.pjrt_parameter_mode,
    );

    const prefill_entry = try compiled_artifact.findUniqueMatchingPackageEntry(package.value, .{
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .seq_len = prefill_seq_len,
        .query_seq_len = prefill_query_seq_len,
        .attention_mode = prefill_attention_mode,
    });
    const prefill_manifest_path = if (prefill_entry) |entry| entry.manifest_path else return error.ArtifactShapeMismatch;

    const decode_manifest_paths = try findPackageDecodeManifestPaths(
        allocator,
        io,
        package.value,
        prefill_manifest_path,
        prefill_seq_len,
    );
    defer allocator.free(decode_manifest_paths);

    return createModelExecutorFromManifestPaths(
        allocator,
        io,
        prefill_manifest_path,
        decode_manifest_paths,
        graph,
        host_backend,
        client,
    );
}

fn createModelExecutorFromArtifacts(
    allocator: std.mem.Allocator,
    io: std.Io,
    prefill_artifact: compiled_artifact.Manifest,
    decode_artifacts: []const compiled_artifact.Manifest,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*pjrt_executor.PjrtModelExecutor {
    if (prefill_artifact.pjrt_input_bindings.len == 0 or prefill_artifact.pjrt_output_node_ids.len == 0) {
        return error.MissingArtifactMetadata;
    }
    if (prefill_artifact.pjrt_input_shapes.len != prefill_artifact.pjrt_input_bindings.len or
        prefill_artifact.pjrt_output_shapes.len != prefill_artifact.pjrt_output_node_ids.len)
    {
        return error.MissingArtifactMetadata;
    }
    for (decode_artifacts) |artifact| {
        if (artifact.pjrt_input_bindings.len == 0 or artifact.pjrt_output_node_ids.len == 0) return error.MissingArtifactMetadata;
        if (artifact.pjrt_input_shapes.len != artifact.pjrt_input_bindings.len or
            artifact.pjrt_output_shapes.len != artifact.pjrt_output_node_ids.len)
        {
            return error.MissingArtifactMetadata;
        }
    }
    const load_mode = try artifactLoadMode(client, prefill_artifact.kind);
    switch (load_mode) {
        .hlo_compile_on_load => std.log.info(
            "PJRT artifact load mode: HLO compile-on-load; plugin-native executable artifacts use a separate manifest kind",
            .{},
        ),
        .load_only_executable => std.log.info("PJRT artifact load mode: plugin-native executable load-only", .{}),
    }

    const prefill_input_bindings = try buildInputBindings(allocator, prefill_artifact.pjrt_input_bindings);
    defer allocator.free(prefill_input_bindings);
    const prefill_semantic_input_bindings = try buildSemanticInputBindings(allocator, prefill_artifact.pjrt_input_bindings);
    defer freeSemanticInputBindings(allocator, prefill_semantic_input_bindings);
    const prefill_semantic_output_bindings = try buildSemanticOutputBindings(allocator, prefill_artifact.pjrt_output_node_ids, prefill_artifact.pjrt_output_bindings);
    defer freeSemanticOutputBindings(allocator, prefill_semantic_output_bindings);
    const prefill_output_node_ids = try cloneOutputNodeIds(allocator, prefill_artifact.pjrt_output_node_ids);
    defer allocator.free(prefill_output_node_ids);

    const prefill_artifact_bytes = try std.Io.Dir.cwd().readFileAlloc(io, prefill_artifact.artifact_path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(prefill_artifact_bytes);
    if (prefill_artifact_bytes.len == 0) return error.InvalidArtifact;

    const decode_packages = try allocator.alloc(pjrt_executor.DecodePackageSpec, decode_artifacts.len);
    var decode_packages_initialized: usize = 0;
    defer {
        for (decode_packages[0..decode_packages_initialized]) |pkg| {
            allocator.free(@constCast(pkg.artifact_bytes));
            allocator.free(@constCast(pkg.input_bindings));
            freeSemanticInputBindings(allocator, @constCast(pkg.semantic_input_bindings));
            freeSemanticOutputBindings(allocator, @constCast(pkg.semantic_output_bindings));
            allocator.free(@constCast(pkg.output_node_ids));
        }
        allocator.free(decode_packages);
    }
    for (decode_artifacts, 0..) |artifact, i| {
        if (!std.mem.eql(u8, artifact.kind, prefill_artifact.kind)) return error.UnsupportedArtifactKind;
        decode_packages[i] = blk: {
            const bytes = try std.Io.Dir.cwd().readFileAlloc(io, artifact.artifact_path, allocator, .limited(std.math.maxInt(usize)));
            errdefer allocator.free(bytes);
            if (bytes.len == 0) return error.InvalidArtifact;

            const input_bindings = try buildInputBindings(allocator, artifact.pjrt_input_bindings);
            errdefer allocator.free(input_bindings);
            const semantic_input_bindings = try buildSemanticInputBindings(allocator, artifact.pjrt_input_bindings);
            errdefer freeSemanticInputBindings(allocator, semantic_input_bindings);
            const semantic_output_bindings = try buildSemanticOutputBindings(allocator, artifact.pjrt_output_node_ids, artifact.pjrt_output_bindings);
            errdefer freeSemanticOutputBindings(allocator, semantic_output_bindings);
            const output_node_ids = try cloneOutputNodeIds(allocator, artifact.pjrt_output_node_ids);
            errdefer allocator.free(output_node_ids);

            break :blk .{
                .seq_len = artifact.seq_len,
                .artifact_bytes = bytes,
                .input_bindings = input_bindings,
                .output_node_ids = output_node_ids,
                .input_shapes = artifact.pjrt_input_shapes,
                .output_shapes = artifact.pjrt_output_shapes,
                .semantic_input_bindings = semantic_input_bindings,
                .semantic_output_bindings = semantic_output_bindings,
            };
        };
        decode_packages_initialized += 1;
    }

    return switch (load_mode) {
        .hlo_compile_on_load => pjrt_executor.createModelExecutorFromHloPackages(
            allocator,
            graph,
            prefill_artifact_bytes,
            prefill_input_bindings,
            prefill_output_node_ids,
            prefill_artifact.pjrt_input_shapes,
            prefill_artifact.pjrt_output_shapes,
            prefill_semantic_input_bindings,
            prefill_semantic_output_bindings,
            decode_packages[0..decode_packages_initialized],
            host_backend,
            client,
        ),
        .load_only_executable => pjrt_executor.createModelExecutorFromExecutablePackages(
            allocator,
            graph,
            prefill_artifact_bytes,
            prefill_input_bindings,
            prefill_output_node_ids,
            prefill_artifact.pjrt_input_shapes,
            prefill_artifact.pjrt_output_shapes,
            prefill_semantic_input_bindings,
            prefill_semantic_output_bindings,
            decode_packages[0..decode_packages_initialized],
            host_backend,
            client,
        ),
    };
}

fn findPackageDecodeManifestPaths(
    allocator: std.mem.Allocator,
    io: std.Io,
    package: compiled_artifact.PackageManifest,
    prefill_manifest_path: []const u8,
    prefill_seq_len: usize,
) ![]const []const u8 {
    var matches = std.ArrayListUnmanaged(compiled_artifact.PackageArtifactEntry).empty;
    defer matches.deinit(allocator);
    for (package.artifacts) |entry| {
        if (!compiled_artifact.packageEntryMatches(entry, .{
            .artifact_role = compiled_artifact.artifact_role_decode,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
        })) continue;
        if (entry.seq_len <= prefill_seq_len) continue;
        try matches.append(allocator, entry);
    }

    std.mem.sort(compiled_artifact.PackageArtifactEntry, matches.items, {}, struct {
        fn lessThan(_: void, a: compiled_artifact.PackageArtifactEntry, b: compiled_artifact.PackageArtifactEntry) bool {
            return a.seq_len < b.seq_len;
        }
    }.lessThan);
    if (matches.items.len > 1) {
        for (matches.items[1..], matches.items[0 .. matches.items.len - 1]) |curr, prev| {
            if (curr.seq_len == prev.seq_len) return error.AmbiguousCompiledArtifact;
        }
    }
    if (matches.items.len == 0 or matches.items[0].seq_len != prefill_seq_len + 1) return &.{};

    var contiguous_len: usize = 1;
    while (contiguous_len < matches.items.len) : (contiguous_len += 1) {
        const prev = matches.items[contiguous_len - 1];
        const curr = matches.items[contiguous_len];
        if (curr.seq_len != prev.seq_len + 1) break;
        if (!try packageKvShapesCompatible(io, allocator, prev.manifest_path, curr.manifest_path)) break;
    }
    if (!try packageKvShapesCompatible(io, allocator, prefill_manifest_path, matches.items[0].manifest_path)) return &.{};

    const manifest_paths = try allocator.alloc([]const u8, contiguous_len);
    for (matches.items[0..contiguous_len], 0..) |entry, i| manifest_paths[i] = entry.manifest_path;
    return manifest_paths;
}

fn packageKvShapesCompatible(
    io: std.Io,
    allocator: std.mem.Allocator,
    source_manifest_path: []const u8,
    target_manifest_path: []const u8,
) !bool {
    var source_parsed = try compiled_artifact.readManifest(allocator, io, source_manifest_path);
    defer source_parsed.deinit();
    var target_parsed = try compiled_artifact.readManifest(allocator, io, target_manifest_path);
    defer target_parsed.deinit();

    const source = source_parsed.value;
    const target = target_parsed.value;
    if (source.pjrt_output_shapes.len != source.pjrt_output_node_ids.len or
        target.pjrt_input_shapes.len != target.pjrt_input_bindings.len)
    {
        return false;
    }

    for (target.pjrt_input_bindings, 0..) |input_binding, input_index| {
        if (!pjrtBindingIsPastKv(input_binding.kind)) continue;
        const output_index = findMatchingPresentOutputIndex(source, input_binding) orelse return false;
        if (!std.mem.eql(i64, source.pjrt_output_shapes[output_index], target.pjrt_input_shapes[input_index])) return false;
    }
    return true;
}

fn pjrtBindingIsPastKv(kind: []const u8) bool {
    return std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_past_key) or
        std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_past_value);
}

fn pjrtBindingIsPresentKv(kind: []const u8) bool {
    return std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_present_key) or
        std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_present_value);
}

fn findMatchingPresentOutputIndex(
    source: compiled_artifact.Manifest,
    input_binding: compiled_artifact.PjrtInputBindingMeta,
) ?usize {
    for (source.pjrt_output_bindings, 0..) |output_binding, output_index| {
        if (!pjrtBindingIsPresentKv(output_binding.kind)) continue;
        if (output_binding.layer_index != input_binding.layer_index) continue;
        if (std.mem.eql(u8, input_binding.kind, compiled_artifact.pjrt_binding_past_key) and
            std.mem.eql(u8, output_binding.kind, compiled_artifact.pjrt_binding_present_key)) return output_index;
        if (std.mem.eql(u8, input_binding.kind, compiled_artifact.pjrt_binding_past_value) and
            std.mem.eql(u8, output_binding.kind, compiled_artifact.pjrt_binding_present_value)) return output_index;
    }
    return null;
}

fn buildInputBindings(
    allocator: std.mem.Allocator,
    bindings: []const compiled_artifact.PjrtInputBindingMeta,
) ![]pjrt_compiler.InputBinding {
    const out = try allocator.alloc(pjrt_compiler.InputBinding, bindings.len);
    errdefer allocator.free(out);
    for (bindings, 0..) |binding, i| {
        const node_id: ml.graph.NodeId = @intCast(binding.node_id);
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_graph_node)) {
            out[i] = .{ .graph_node = node_id };
        } else if (compiled_artifact.pjrtBindingIsInputIds(binding)) {
            out[i] = .{ .embedding_ids = node_id };
        } else if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_key) or
            std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_value))
        {
            out[i] = .{ .semantic_past_graph_node = node_id };
        } else if (compiled_artifact.pjrtBindingIsKvCache(binding)) {
            return error.UnsupportedArtifactInputs;
        } else {
            return error.MissingArtifactMetadata;
        }
    }
    return out;
}

fn buildSemanticInputBindings(
    allocator: std.mem.Allocator,
    bindings: []const compiled_artifact.PjrtInputBindingMeta,
) ![]pjrt_executor.PjrtSemanticInputBinding {
    var out = std.ArrayListUnmanaged(pjrt_executor.PjrtSemanticInputBinding).empty;
    errdefer {
        for (out.items) |*binding| allocator.free(binding.name);
        out.deinit(allocator);
    }
    for (bindings, 0..) |binding, input_index| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_graph_node) or
            compiled_artifact.pjrtBindingIsInputIds(binding))
            continue;

        const kind: pjrt_executor.PjrtRetainedBufferKind = if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_key))
            .key
        else if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_value))
            .value
        else if (compiled_artifact.pjrtBindingIsKvCache(binding))
            return error.UnsupportedArtifactInputs
        else
            return error.MissingArtifactMetadata;
        if (binding.name.len == 0) return error.MissingArtifactMetadata;
        try out.append(allocator, .{
            .input_index = input_index,
            .name = try allocator.dupe(u8, binding.name),
            .layer_index = binding.layer_index,
            .kind = kind,
        });
    }
    return out.toOwnedSlice(allocator);
}

fn buildSemanticOutputBindings(
    allocator: std.mem.Allocator,
    output_node_ids: []const u32,
    bindings: []const compiled_artifact.PjrtOutputBindingMeta,
) ![]pjrt_executor.PjrtSemanticOutputBinding {
    var out = std.ArrayListUnmanaged(pjrt_executor.PjrtSemanticOutputBinding).empty;
    errdefer {
        for (out.items) |*binding| allocator.free(binding.name);
        out.deinit(allocator);
    }
    for (bindings) |binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_graph_node)) continue;
        const kind: pjrt_executor.PjrtRetainedBufferKind = if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_present_key))
            .key
        else if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_present_value))
            .value
        else if (compiled_artifact.pjrtBindingIsKvCache(binding))
            return error.UnsupportedArtifactInputs
        else
            return error.MissingArtifactMetadata;
        if (binding.name.len == 0) return error.MissingArtifactMetadata;
        const output_index = findOutputBindingIndex(output_node_ids, binding.node_id) orelse return error.MissingArtifactMetadata;
        try out.append(allocator, .{
            .output_index = output_index,
            .name = try allocator.dupe(u8, binding.name),
            .layer_index = binding.layer_index,
            .kind = kind,
        });
    }
    return out.toOwnedSlice(allocator);
}

fn freeSemanticOutputBindings(
    allocator: std.mem.Allocator,
    bindings: []pjrt_executor.PjrtSemanticOutputBinding,
) void {
    for (bindings) |*binding| allocator.free(binding.name);
    allocator.free(bindings);
}

fn freeSemanticInputBindings(
    allocator: std.mem.Allocator,
    bindings: []pjrt_executor.PjrtSemanticInputBinding,
) void {
    for (bindings) |*binding| allocator.free(binding.name);
    allocator.free(bindings);
}

fn findOutputBindingIndex(output_node_ids: []const u32, node_id: u32) ?usize {
    for (output_node_ids, 0..) |output_node_id, i| {
        if (output_node_id == node_id) return i;
    }
    return null;
}

fn cloneOutputNodeIds(
    allocator: std.mem.Allocator,
    node_ids: []const u32,
) ![]ml.graph.NodeId {
    const out = try allocator.alloc(ml.graph.NodeId, node_ids.len);
    errdefer allocator.free(out);
    for (node_ids, 0..) |node_id, i| out[i] = @intCast(node_id);
    return out;
}

test "PJRT artifact load mode is HLO compile-on-load until executable persistence is bound" {
    const fake_client: pjrt_lib.pjrt.Client = undefined;
    try std.testing.expectEqual(ArtifactLoadMode.hlo_compile_on_load, try artifactLoadMode(&fake_client, "pjrt_hlo"));
    try std.testing.expectEqual(ArtifactLoadMode.hlo_compile_on_load, try artifactLoadMode(&fake_client, "pjrt_partition_hlo"));
}

test "PJRT artifact input bindings accept semantic input_ids" {
    const bindings = [_]compiled_artifact.PjrtInputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 7, .name = "input_ids" },
    };
    const parsed = try buildInputBindings(std.testing.allocator, &bindings);
    defer std.testing.allocator.free(parsed);
    try std.testing.expectEqual(@as(usize, 1), parsed.len);
    switch (parsed[0]) {
        .embedding_ids => |node_id| try std.testing.expectEqual(@as(ml.graph.NodeId, 7), node_id),
        else => return error.TestUnexpectedBindingKind,
    }
}

test "PJRT artifact input bindings split semantic past KV from graph placeholders" {
    const bindings = [_]compiled_artifact.PjrtInputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 9, .name = "past_key_values.0.key", .layer_index = 0 },
    };
    const input_bindings = try buildInputBindings(std.testing.allocator, &bindings);
    defer std.testing.allocator.free(input_bindings);
    switch (input_bindings[0]) {
        .semantic_past_graph_node => |node_id| try std.testing.expectEqual(@as(ml.graph.NodeId, 9), node_id),
        else => return error.TestUnexpectedBindingKind,
    }

    const semantic = try buildSemanticInputBindings(std.testing.allocator, &bindings);
    defer freeSemanticInputBindings(std.testing.allocator, semantic);
    try std.testing.expectEqual(@as(usize, 1), semantic.len);
    try std.testing.expectEqual(@as(usize, 0), semantic[0].input_index);
    try std.testing.expectEqual(pjrt_executor.PjrtRetainedBufferKind.key, semantic[0].kind);
    try std.testing.expectEqualStrings("past_key_values.0.key", semantic[0].name);
}

test "PJRT artifact output bindings reject past KV outputs" {
    const bindings = [_]compiled_artifact.PjrtOutputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_past_value, .node_id = 10, .name = "past_key_values.0.value", .layer_index = 0 },
    };
    try std.testing.expectError(error.UnsupportedArtifactInputs, buildSemanticOutputBindings(std.testing.allocator, &.{10}, &bindings));
}

test "PJRT artifact output bindings build semantic present bindings" {
    const bindings = [_]compiled_artifact.PjrtOutputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 10, .name = "present.0.value", .layer_index = 0 },
    };
    const parsed = try buildSemanticOutputBindings(std.testing.allocator, &.{ 4, 10 }, &bindings);
    defer freeSemanticOutputBindings(std.testing.allocator, parsed);
    try std.testing.expectEqual(@as(usize, 1), parsed.len);
    try std.testing.expectEqual(@as(usize, 1), parsed[0].output_index);
    try std.testing.expectEqual(pjrt_executor.PjrtRetainedBufferKind.value, parsed[0].kind);
    try std.testing.expectEqualStrings("present.0.value", parsed[0].name);
}

test "PJRT package selectors resolve prefill and contiguous decode chain" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "prefill.termite.json" });
    defer allocator.free(prefill_manifest_path);
    const decode3_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode.s3.termite.json" });
    defer allocator.free(decode3_manifest_path);
    const decode4_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode.s4.termite.json" });
    defer allocator.free(decode4_manifest_path);

    const kv_shape = &[_]i64{ 2, 768 };
    try compiled_artifact.writeManifest(allocator, io, prefill_manifest_path, .{
        .kind = "pjrt_executable",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/prefill.exec",
        .prompt_tokens = 2,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_output_node_ids = @constCast(&[_]u32{ 10, 11 }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 10, .name = "present.0.key", .layer_index = 0 },
            .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 11, .name = "present.0.value", .layer_index = 0 },
        }),
        .pjrt_output_shapes = @constCast(&[_][]i64{ @constCast(kv_shape), @constCast(kv_shape) }),
    });
    try compiled_artifact.writeManifest(allocator, io, decode3_manifest_path, .{
        .kind = "pjrt_executable",
        .artifact_role = compiled_artifact.artifact_role_decode,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/decode.s3.exec",
        .prompt_tokens = 2,
        .seq_len = 3,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_input_bindings = @constCast(&[_]compiled_artifact.PjrtInputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 10, .name = "past_key_values.0.key", .layer_index = 0 },
            .{ .kind = compiled_artifact.pjrt_binding_past_value, .node_id = 11, .name = "past_key_values.0.value", .layer_index = 0 },
        }),
        .pjrt_output_node_ids = @constCast(&[_]u32{ 12, 13 }),
        .pjrt_input_shapes = @constCast(&[_][]i64{ @constCast(kv_shape), @constCast(kv_shape) }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 12, .name = "present.0.key", .layer_index = 0 },
            .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 13, .name = "present.0.value", .layer_index = 0 },
        }),
        .pjrt_output_shapes = @constCast(&[_][]i64{ @constCast(kv_shape), @constCast(kv_shape) }),
    });
    try compiled_artifact.writeManifest(allocator, io, decode4_manifest_path, .{
        .kind = "pjrt_executable",
        .artifact_role = compiled_artifact.artifact_role_decode,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/decode.s4.exec",
        .prompt_tokens = 2,
        .seq_len = 4,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_input_bindings = @constCast(&[_]compiled_artifact.PjrtInputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 12, .name = "past_key_values.0.key", .layer_index = 0 },
            .{ .kind = compiled_artifact.pjrt_binding_past_value, .node_id = 13, .name = "past_key_values.0.value", .layer_index = 0 },
        }),
        .pjrt_output_node_ids = @constCast(&[_]u32{ 14, 15 }),
        .pjrt_input_shapes = @constCast(&[_][]i64{ @constCast(kv_shape), @constCast(kv_shape) }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 14, .name = "present.0.key", .layer_index = 0 },
            .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 15, .name = "present.0.value", .layer_index = 0 },
        }),
        .pjrt_output_shapes = @constCast(&[_][]i64{ @constCast(kv_shape), @constCast(kv_shape) }),
    });

    const package: compiled_artifact.PackageManifest = .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = prefill_manifest_path,
                .artifact_path = "/tmp/prefill.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = decode3_manifest_path,
                .artifact_path = "/tmp/decode.s3.exec",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 3,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
            .{
                .manifest_path = decode4_manifest_path,
                .artifact_path = "/tmp/decode.s4.exec",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 4,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    };

    const prefill_entry = (try compiled_artifact.findUniqueMatchingPackageEntry(package, .{
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
    })).?;
    try std.testing.expectEqualStrings(prefill_manifest_path, prefill_entry.manifest_path);
    const decode_paths = try findPackageDecodeManifestPaths(allocator, io, package, prefill_manifest_path, 2);
    defer allocator.free(decode_paths);
    try std.testing.expectEqual(@as(usize, 2), decode_paths.len);
    try std.testing.expectEqualStrings(decode3_manifest_path, decode_paths[0]);
    try std.testing.expectEqualStrings(decode4_manifest_path, decode_paths[1]);
}
