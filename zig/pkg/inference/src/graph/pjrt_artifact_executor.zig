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
const admission_mod = @import("pjrt_admission.zig");
const ml = @import("ml");
const pjrt_lib = @import("pjrt");

const compiled_artifact = @import("../compiled_artifact.zig");
const ops = @import("../ops/ops.zig");
const pjrt_compiler = @import("pjrt_compiler.zig");
const pjrt_executor = @import("pjrt_executor.zig");

pub const ArtifactLoadMode = enum {
    /// Antfly inference stores HLO bytes and compiles them through the configured PJRT
    /// plugin when the artifact is loaded.
    hlo_compile_on_load,
    /// Antfly inference stores/loads a plugin-native executable artifact
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
    return createModelExecutorFromArtifacts(allocator, io, artifact, &.{}, graph, host_backend, client, null);
}

/// The caller retains a verified, admitted buffer through executable loading.
/// Avoid a second full-file allocation hidden inside the executor factory.
pub fn createModelExecutorFromArtifactBytes(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    bytes: []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*pjrt_executor.PjrtModelExecutor {
    return createModelExecutorFromArtifacts(allocator, io, artifact, &.{}, graph, host_backend, client, bytes);
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
        null,
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
        null,
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

/// The explicit runner owns the already admitted client and verified bytes.
/// Metadata and runtime slots use the same bounded ownership as managed loads.
pub fn createModelExecutorFromAdmittedArtifactBytes(allocator: std.mem.Allocator, io: std.Io, artifact: compiled_artifact.Manifest, bytes: []const u8, graph: *const ml.graph.Graph, host_backend: *const ops.ComputeBackend, client: *pjrt_lib.pjrt.Client, policy: *const admission_mod.Context) !*pjrt_executor.PjrtModelExecutor {
    const metadata = try @import("pjrt_metadata.zig").Budget.create(allocator, policy);
    errdefer metadata.destroy();
    const executor = createModelExecutorFromArtifactBytes(metadata.allocator(), io, artifact, bytes, graph, host_backend, client) catch |err| return metadata.translate(err);
    errdefer pjrt_executor.PjrtModelExecutor.deinit(executor);
    executor.retained_capacity = retainedCapacity(metadata.allocator(), executor.semantic_output_bindings, executor.decode_variants) catch |err| return metadata.translate(err);
    executor.prepareRuntimeSlots(1) catch |err| return metadata.translate(err);
    executor.metadata_budget = metadata;
    metadata.finishConstruction();
    return executor;
}

pub fn createAdmittedModelExecutorFromPackageManifestPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_manifest_path: []const u8,
    prefill_seq_len: usize,
    prefill_query_seq_len: usize,
    prefill_attention_mode: []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    policy: *const admission_mod.Context,
    plugin: [:0]const u8,
) !*pjrt_executor.PjrtModelExecutor {
    const budget = try @import("pjrt_metadata.zig").Budget.create(allocator, policy);
    defer budget.destroy();
    return createAdmittedModelExecutorFromPackageManifestPathWithMetadata(allocator, io, package_manifest_path, prefill_seq_len, prefill_query_seq_len, prefill_attention_mode, graph, host_backend, policy, plugin, budget.allocator()) catch |err| return budget.translate(err);
}

fn createAdmittedModelExecutorFromPackageManifestPathWithMetadata(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_manifest_path: []const u8,
    prefill_seq_len: usize,
    prefill_query_seq_len: usize,
    prefill_attention_mode: []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    policy: *const admission_mod.Context,
    plugin: [:0]const u8,
    metadata: std.mem.Allocator,
) !*pjrt_executor.PjrtModelExecutor {
    var package = try compiled_artifact.readPackageManifest(metadata, io, package_manifest_path);
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

    var prefill_manifest = try compiled_artifact.readManifest(metadata, io, prefill_manifest_path);
    defer prefill_manifest.deinit();
    if (!compiled_artifact.matchesRequest(prefill_manifest.value, .{ .backend = "xla", .model_dir = package.value.model_dir, .kind = "pjrt_executable", .artifact_role = compiled_artifact.artifact_role_prefill, .seq_len = prefill_seq_len, .query_seq_len = prefill_query_seq_len, .attention_mode = prefill_attention_mode, .pjrt_parameter_mode = package.value.pjrt_parameter_mode, .require_pjrt_memory_plan = true })) return error.ArtifactShapeMismatch;

    const decode_manifest_paths = try findPackageDecodeManifestPaths(
        metadata,
        io,
        package.value,
        prefill_manifest_path,
        prefill_seq_len,
        policy.decode_steps_limit,
    );
    defer metadata.free(decode_manifest_paths);

    @import("pjrt_metadata.zig").Budget.releaseUnusedCapacity(metadata);
    return createAdmittedModelExecutorFromManifestPaths(
        allocator,
        io,
        prefill_manifest_path,
        decode_manifest_paths,
        graph,
        host_backend,
        policy,
        plugin,
    );
}

pub fn createAdmittedModelExecutorFromManifestPaths(
    allocator: std.mem.Allocator,
    io: std.Io,
    prefill_path: []const u8,
    decode_paths: []const []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    policy: *const admission_mod.Context,
    plugin: [:0]const u8,
) !*pjrt_executor.PjrtModelExecutor {
    const budget = try @import("pjrt_metadata.zig").Budget.create(allocator, policy);
    defer budget.destroy();
    return createAdmittedModelExecutorFromManifestPathsWithMetadata(allocator, io, prefill_path, decode_paths, graph, host_backend, policy, plugin, budget.allocator()) catch |err| return budget.translate(err);
}

fn createAdmittedModelExecutorFromManifestPathsWithMetadata(
    allocator: std.mem.Allocator,
    io: std.Io,
    prefill_path: []const u8,
    decode_paths: []const []const u8,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    policy: *const admission_mod.Context,
    plugin: [:0]const u8,
    metadata: std.mem.Allocator,
) !*pjrt_executor.PjrtModelExecutor {
    const selected_decode_paths = decode_paths[0..@min(decode_paths.len, policy.decode_steps_limit orelse decode_paths.len)];
    if (policy.decode_steps_limit) |required| {
        if (selected_decode_paths.len != required) return error.PjrtLoadOnlyArtifactsRequired;
    }
    const manifests = try metadata.alloc(std.json.Parsed(compiled_artifact.Manifest), selected_decode_paths.len + 1);
    defer metadata.free(manifests);
    var initialized: usize = 0;
    defer for (manifests[0..initialized]) |*parsed| parsed.deinit();
    manifests[0] = try compiled_artifact.readManifest(metadata, io, prefill_path);
    initialized += 1;
    for (selected_decode_paths, 1..) |path, i| {
        manifests[i] = try compiled_artifact.readManifest(metadata, io, path);
        initialized += 1;
    }
    const artifacts = try metadata.alloc(compiled_artifact.Manifest, manifests.len);
    defer metadata.free(artifacts);
    for (manifests, artifacts) |parsed, *artifact| artifact.* = parsed.value;
    @import("pjrt_metadata.zig").Budget.releaseUnusedCapacity(metadata);
    return createAdmittedModelExecutorFromManifests(allocator, artifacts, graph, host_backend, policy, plugin);
}

/// The selector retains these parsed manifests through construction, avoiding
/// a second read/parse of every selected decode sidecar.
pub fn createAdmittedModelExecutorFromManifests(
    allocator: std.mem.Allocator,
    artifacts: []const compiled_artifact.Manifest,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    policy: *const admission_mod.Context,
    plugin: [:0]const u8,
) !*pjrt_executor.PjrtModelExecutor {
    const metadata = try @import("pjrt_metadata.zig").Budget.create(allocator, policy);
    errdefer metadata.destroy();
    const executor = createAdmittedModelExecutorWithAllocator(metadata.allocator(), artifacts, graph, host_backend, policy, plugin) catch |err| return metadata.translate(err);
    errdefer pjrt_executor.PjrtModelExecutor.deinit(executor);
    executor.prepareRuntimeSlots(policy.runtime_capacity) catch |err| return metadata.translate(err);
    executor.metadata_budget = metadata;
    // Retained executor metadata keeps its individual residency; unconsumed
    // construction headroom must not remain pinned for the executor's lifetime.
    metadata.finishConstruction();
    return executor;
}

fn createAdmittedModelExecutorWithAllocator(
    allocator: std.mem.Allocator,
    artifacts: []const compiled_artifact.Manifest,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    policy: *const admission_mod.Context,
    plugin: [:0]const u8,
) !*pjrt_executor.PjrtModelExecutor {
    if (artifacts.len == 0) return error.MissingArtifactMetadata;
    if (policy.decode_steps_limit) |required| if (artifacts.len - 1 != required) return error.PjrtLoadOnlyArtifactsRequired;
    for (artifacts[1..], 1..) |artifact, i| {
        if (artifact.seq_len != (std.math.add(usize, artifacts[0].seq_len, i) catch return error.ArtifactShapeMismatch) or !std.mem.eql(u8, artifact.kind, artifacts[0].kind) or !std.mem.eql(u8, artifact.artifact_role, compiled_artifact.artifact_role_decode) or artifact.query_seq_len != 1 or !std.mem.eql(u8, artifact.attention_mode, "paged_decode") or !std.mem.eql(u8, artifact.model_dir, artifacts[0].model_dir) or !std.mem.eql(u8, artifact.backend, artifacts[0].backend) or !std.mem.eql(u8, artifact.pjrt_parameter_mode, artifacts[0].pjrt_parameter_mode)) return error.ArtifactShapeMismatch;
        if (!@import("pjrt_artifact_selection.zig").kvShapesCompatible(artifacts[i - 1], artifact)) return error.ArtifactShapeMismatch;
    }
    const graph_allocator = @import("antfly_platform").allocator.reclaimingScratch(allocator);
    const owner = try admission_mod.Package.init(policy, artifacts, plugin, 0);
    errdefer owner.deinit();
    const owned_graph = try graph_allocator.create(ml.graph.Graph);
    errdefer graph_allocator.destroy(owned_graph);
    owned_graph.* = ml.graph.Graph.init(graph_allocator);
    errdefer owned_graph.deinit();
    // All copies, including shapes and binding names, use the retained
    // admission allocator; transient constructor copies are charged at peak.
    owned_graph.nodes = .fromOwnedSlice(try graph_allocator.dupe(ml.graph.Node, graph.nodes.items));
    const constants = try graph_allocator.alignedAlloc(u8, .@"8", graph.constant_pool.items.len);
    @memcpy(constants, graph.constant_pool.items);
    owned_graph.constant_pool = .fromOwnedSlice(constants);
    owned_graph.string_table = .fromOwnedSlice(try graph_allocator.dupe(u8, graph.string_table.items));
    owned_graph.outputs = .fromOwnedSlice(try graph_allocator.dupe(ml.graph.NodeId, graph.outputs.items));
    owned_graph.parameters = .fromOwnedSlice(try graph_allocator.dupe(ml.graph.NodeId, graph.parameters.items));
    const prefill = try loadAdmittedVariant(graph_allocator, owned_graph, host_backend, owner, 0, artifacts[0]);
    errdefer freeAdmittedVariant(graph_allocator, prefill);
    const variants = try graph_allocator.alloc(pjrt_executor.PjrtModelExecutor.DecodeVariant, artifacts.len - 1);
    var loaded: usize = 0;
    errdefer {
        for (variants[0..loaded]) |variant| freeAdmittedVariant(graph_allocator, variant);
        graph_allocator.free(variants);
    }
    for (artifacts[1..], 0..) |artifact, i| {
        variants[i] = try loadAdmittedVariant(graph_allocator, owned_graph, host_backend, owner, i + 1, artifact);
        loaded += 1;
    }
    const retained_capacity = try retainedCapacity(graph_allocator, prefill.semantic_output_bindings, variants);
    try owner.loaded();
    const executor = try graph_allocator.create(pjrt_executor.PjrtModelExecutor);
    executor.* = .{
        .allocator = graph_allocator,
        .graph = owned_graph,
        .owned_graph = owned_graph,
        .executor = prefill.executor,
        .host_backend = host_backend,
        .artifact_owner = owner,
        .retained_capacity = retained_capacity,
        .input_materialization = prefill.input_materialization,
        .semantic_input_bindings = prefill.semantic_input_bindings,
        .semantic_output_bindings = prefill.semantic_output_bindings,
        .decode_variants = variants,
    };
    executor.prepareOutputMappings();
    return executor;
}

/// Only one phase's outputs survive a successful execution. Size the table
/// for the largest phase, preserving distinct layerless identities per phase.
fn retainedCapacity(a: std.mem.Allocator, prefill: []const pjrt_executor.PjrtSemanticOutputBinding, variants: []const pjrt_executor.PjrtModelExecutor.DecodeVariant) !usize {
    const Identity = @import("pjrt_kv_identity.zig").Identity;
    var keys: std.HashMapUnmanaged(Identity, void, Identity.Context, 80) = .empty;
    defer keys.deinit(a);
    for (prefill) |binding| try keys.put(a, binding.identity(), {});
    var capacity = keys.count();
    for (variants) |variant| {
        keys.clearRetainingCapacity();
        for (variant.semantic_output_bindings) |binding| try keys.put(a, binding.identity(), {});
        capacity = @max(capacity, keys.count());
    }
    return capacity;
}

fn freeAdmittedVariant(allocator: std.mem.Allocator, variant: pjrt_executor.PjrtModelExecutor.DecodeVariant) void {
    variant.executor.partitionExecutor().deinitExecutor();
    freeSemanticInputBindings(allocator, variant.semantic_input_bindings);
    freeSemanticOutputBindings(allocator, variant.semantic_output_bindings);
}

fn loadAdmittedVariant(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, host_backend: *const ops.ComputeBackend, owner: *admission_mod.Package, index: usize, artifact: compiled_artifact.Manifest) !pjrt_executor.PjrtModelExecutor.DecodeVariant {
    const bytes = try owner.read(index, artifact);
    defer owner.allocator.free(bytes);
    const inputs = try buildInputBindings(allocator, artifact.pjrt_input_bindings);
    defer allocator.free(inputs);
    const outputs = try cloneOutputNodeIds(allocator, artifact.pjrt_output_node_ids);
    defer allocator.free(outputs);
    const semantic_inputs = try buildSemanticInputBindings(allocator, artifact.pjrt_input_bindings);
    errdefer freeSemanticInputBindings(allocator, semantic_inputs);
    const semantic_outputs = try buildSemanticOutputBindings(allocator, artifact.pjrt_output_node_ids, artifact.pjrt_output_bindings);
    errdefer freeSemanticOutputBindings(allocator, semantic_outputs);
    const executable = try pjrt_executor.createExecutorFromSerializedExecutableWithShapes(allocator, graph, bytes, inputs, outputs, artifact.pjrt_input_shapes, artifact.pjrt_output_shapes, host_backend, &owner.client);
    errdefer executable.partitionExecutor().deinitExecutor();
    try admission_mod.ArtifactDeviceAdmission.verifyLoaded(artifact, try executable.executable.compiledMemoryStats());
    return .{ .seq_len = artifact.seq_len, .executor = executable, .input_materialization = pjrt_executor.detectInputMaterialization(inputs, semantic_inputs, semantic_outputs), .semantic_input_bindings = semantic_inputs, .semantic_output_bindings = semantic_outputs };
}

fn createModelExecutorFromArtifacts(
    allocator: std.mem.Allocator,
    io: std.Io,
    prefill_artifact: compiled_artifact.Manifest,
    decode_artifacts: []const compiled_artifact.Manifest,
    graph: *const ml.graph.Graph,
    host_backend: *const ops.ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
    provided_prefill_bytes: ?[]const u8,
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

    const prefill_artifact_bytes = provided_prefill_bytes orelse try std.Io.Dir.cwd().readFileAlloc(io, prefill_artifact.artifact_path, allocator, .limited(std.math.maxInt(usize)));
    defer if (provided_prefill_bytes == null) allocator.free(prefill_artifact_bytes);
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
    max_decode_steps: ?usize,
) ![]const []const u8 {
    if (max_decode_steps == 0) return &.{};
    var matches = std.ArrayListUnmanaged(compiled_artifact.PackageArtifactEntry).empty;
    defer matches.deinit(allocator);
    for (package.artifacts) |entry| {
        if (!compiled_artifact.packageEntryMatches(entry, .{
            .artifact_role = compiled_artifact.artifact_role_decode,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
        })) continue;
        if (entry.seq_len <= prefill_seq_len) continue;
        if (max_decode_steps) |limit| if (entry.seq_len > prefill_seq_len +| limit) continue;
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

    return @import("pjrt_artifact_selection.zig").kvShapesCompatible(source_parsed.value, target_parsed.value);
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
        try out.ensureUnusedCapacity(allocator, 1);
        const name = try allocator.dupe(u8, binding.name);
        out.appendAssumeCapacity(.{
            .input_index = input_index,
            .name = name,
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
        for (out.items) |existing| {
            if (existing.output_index == output_index or existing.identity().eql(binding.kvIdentity().?)) return error.MissingArtifactMetadata;
        }
        try out.ensureUnusedCapacity(allocator, 1);
        const name = try allocator.dupe(u8, binding.name);
        out.appendAssumeCapacity(.{
            .output_index = output_index,
            .name = name,
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

    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "prefill.inference.json" });
    defer allocator.free(prefill_manifest_path);
    const decode3_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode.s3.inference.json" });
    defer allocator.free(decode3_manifest_path);
    const decode4_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode.s4.inference.json" });
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
    const decode_paths = try findPackageDecodeManifestPaths(allocator, io, package, prefill_manifest_path, 2, null);
    defer allocator.free(decode_paths);
    try std.testing.expectEqual(@as(usize, 2), decode_paths.len);
    try std.testing.expectEqualStrings(decode3_manifest_path, decode_paths[0]);
    try std.testing.expectEqualStrings(decode4_manifest_path, decode_paths[1]);
}

test "managed PJRT semantic binding builders refund names at every allocation failure" {
    const Scenario = struct {
        fn inputs(a: std.mem.Allocator) !void {
            const bindings = [_]compiled_artifact.PjrtInputBindingMeta{
                .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 1, .layer_index = 0, .name = "past.0.key" },
                .{ .kind = compiled_artifact.pjrt_binding_past_value, .node_id = 2, .layer_index = 0, .name = "past.0.value" },
            };
            const result = try buildSemanticInputBindings(a, &bindings);
            defer freeSemanticInputBindings(a, result);
            try std.testing.expectEqual(@as(usize, 2), result.len);
        }
        fn outputs(a: std.mem.Allocator) !void {
            const bindings = [_]compiled_artifact.PjrtOutputBindingMeta{
                .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 1, .layer_index = 0, .name = "present.0.key" },
                .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 2, .layer_index = 0, .name = "present.0.value" },
            };
            const result = try buildSemanticOutputBindings(a, &.{ 1, 2 }, &bindings);
            defer freeSemanticOutputBindings(a, result);
            try std.testing.expectEqual(@as(usize, 2), result.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.inputs, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.outputs, .{});
}

test "managed PJRT rejects aliased native output ownership" {
    const bindings = [_]compiled_artifact.PjrtOutputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 1, .name = "key" },
        .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 1, .name = "value" },
    };
    try std.testing.expectError(error.MissingArtifactMetadata, buildSemanticOutputBindings(std.testing.allocator, &.{1}, &bindings));
}

test "managed PJRT capacity keeps layerless named states distinct" {
    const bindings = [_]pjrt_executor.PjrtSemanticOutputBinding{
        .{ .output_index = 0, .name = "a", .layer_index = null, .kind = .key },
        .{ .output_index = 1, .name = "b", .layer_index = null, .kind = .key },
        .{ .output_index = 2, .name = "prefill", .layer_index = 0, .kind = .key },
        .{ .output_index = 3, .name = "decode", .layer_index = 0, .kind = .key },
    };
    try std.testing.expectEqual(@as(usize, 3), try retainedCapacity(std.testing.allocator, &bindings, &.{}));
}

test "managed PJRT rejects duplicate logical output state" {
    const bindings = [_]compiled_artifact.PjrtOutputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 1, .layer_index = 0, .name = "one" },
        .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 2, .layer_index = 0, .name = "two" },
    };
    try std.testing.expectError(error.MissingArtifactMetadata, buildSemanticOutputBindings(std.testing.allocator, &.{ 1, 2 }, &bindings));
}
