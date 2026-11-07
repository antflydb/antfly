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
const artifact = @import("../compiled_artifact.zig");

pub const Request = struct {
    model_dir: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
    decode_steps: ?usize,
};

/// Keep parsed metadata through executor construction. Package inspection and
/// standalone fallback share one catalog within the current parameter mode.
pub const Selection = struct {
    catalog: Catalog,
    chain: []usize,
    budget: ?*@import("pjrt_metadata.zig").Budget = null,
    manifests: ?[]artifact.Manifest = null,

    pub fn values(self: *const @This(), allocator: std.mem.Allocator) ![]artifact.Manifest {
        const result = try allocator.alloc(artifact.Manifest, self.chain.len);
        for (self.chain, result) |index, *value| value.* = self.catalog.entries.items[index].parsed.value;
        return result;
    }

    pub fn deinit(self: *@This()) void {
        if (self.manifests) |manifests| self.catalog.allocator.free(manifests);
        self.catalog.allocator.free(self.chain);
        self.catalog.deinit();
        if (self.budget) |budget| budget.destroy();
    }
};

const Catalog = struct {
    const Entry = struct { path: []u8, parsed: std.json.Parsed(artifact.Manifest), available: bool, standalone: bool = false };
    allocator: std.mem.Allocator,
    io: std.Io,
    parameter_mode: ?[]const u8 = null,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    by_path: std.StringHashMapUnmanaged(usize) = .empty,

    fn deinit(self: *@This()) void {
        self.by_path.deinit(self.allocator);
        for (self.entries.items) |*entry| {
            entry.parsed.deinit();
            self.allocator.free(entry.path);
        }
        self.entries.deinit(self.allocator);
    }

    fn read(self: *@This(), path: []const u8, filter: ?Request) !?usize {
        if (self.by_path.get(path)) |index| return index;
        const parsed = (if (filter) |request| artifact.readManifestIfInspected(self.allocator, self.io, path, ModeFilter{ .request = request, .mode = self.parameter_mode }, relevantMode) else artifact.readManifestIf(self.allocator, self.io, path, {}, struct {
            fn matches(_: artifact.Manifest, _: void) bool {
                return true;
            }
        }.matches)) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        var retained = parsed orelse return null;
        errdefer retained.deinit();
        const stat = std.Io.Dir.cwd().statFile(self.io, retained.value.artifact_path, .{}) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const available = if (stat) |info| info.kind == .file and info.size > 0 else false;
        const owned = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned);
        try self.entries.ensureUnusedCapacity(self.allocator, 1);
        try self.by_path.ensureUnusedCapacity(self.allocator, 1);
        const index = self.entries.items.len;
        self.entries.appendAssumeCapacity(.{ .path = owned, .parsed = retained, .available = available });
        self.by_path.putAssumeCapacity(owned, index);
        return index;
    }

    fn scan(self: *@This(), directory: []const u8, request: Request) !void {
        var dir = (if (std.fs.path.isAbsolute(directory))
            std.Io.Dir.openDirAbsolute(self.io, directory, .{ .iterate = true })
        else
            std.Io.Dir.cwd().openDir(self.io, directory, .{ .iterate = true })) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(self.io);
        var iter = dir.iterate();
        while (try iter.next(self.io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".inference.json")) continue;
            const path = try std.fs.path.join(self.allocator, &.{ directory, entry.name });
            defer self.allocator.free(path);
            const index = self.read(path, request) catch |err| switch (err) {
                error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => continue,
                else => return err,
            };
            if (index) |i| self.entries.items[i].standalone = true;
        }
    }
};

const ModeFilter = struct { request: Request, mode: ?[]const u8 };

fn relevantMode(value: artifact.Manifest, filter: ModeFilter) bool {
    if (filter.mode) |mode| if (!std.mem.eql(u8, value.pjrt_parameter_mode, mode)) return false;
    return relevant(value, filter.request);
}

fn relevant(value: artifact.Manifest, request: Request) bool {
    return matchesPrefill(value, request, artifact.pjrt_parameter_mode_embedded) or
        matchesPrefill(value, request, artifact.pjrt_parameter_mode_inputs) or
        (artifact.hasManagedPjrtPlan(value) and std.mem.eql(u8, value.backend, "xla") and std.mem.eql(u8, value.kind, "pjrt_executable") and std.mem.eql(u8, value.model_dir, request.model_dir) and std.mem.eql(u8, value.artifact_role, artifact.artifact_role_decode) and value.query_seq_len == 1 and std.mem.eql(u8, value.attention_mode, "paged_decode") and inRange(value.seq_len, request));
}

fn matchesPrefill(value: artifact.Manifest, request: Request, mode: []const u8) bool {
    return artifact.matchesRequest(value, .{ .backend = "xla", .model_dir = request.model_dir, .kind = "pjrt_executable", .artifact_role = artifact.artifact_role_prefill, .seq_len = request.seq_len, .query_seq_len = request.query_seq_len, .attention_mode = request.attention_mode, .pjrt_parameter_mode = mode, .require_pjrt_memory_plan = true });
}

pub fn kvShapesCompatible(source: artifact.Manifest, target: artifact.Manifest) bool {
    if (source.pjrt_output_shapes.len != source.pjrt_output_node_ids.len or target.pjrt_input_shapes.len != target.pjrt_input_bindings.len) return false;
    for (target.pjrt_input_bindings, target.pjrt_input_shapes) |input, shape| {
        const present_kind = if (std.mem.eql(u8, input.kind, artifact.pjrt_binding_past_key)) artifact.pjrt_binding_present_key else if (std.mem.eql(u8, input.kind, artifact.pjrt_binding_past_value)) artifact.pjrt_binding_present_value else continue;
        var found = false;
        for (source.pjrt_output_bindings) |output| {
            if (!std.mem.eql(u8, output.kind, present_kind) or !output.kvIdentity().?.eql(input.kvIdentity().?)) continue;
            if (found) return false; // Two outputs cannot own the same state.
            for (source.pjrt_output_node_ids, source.pjrt_output_shapes) |node, output_shape| {
                if (node != output.node_id) continue;
                if (!std.mem.eql(i64, output_shape, shape)) return false;
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn matchesDecode(value: artifact.Manifest, prefill: artifact.Manifest) bool {
    if (!artifact.hasManagedPjrtPlan(value) or !artifact.hasManagedPjrtPlan(prefill)) return false;
    return artifact.matchesRequest(value, .{ .backend = prefill.backend, .model_dir = prefill.model_dir, .kind = prefill.kind, .artifact_role = artifact.artifact_role_decode, .seq_len = value.seq_len, .query_seq_len = 1, .attention_mode = "paged_decode", .pjrt_parameter_mode = prefill.pjrt_parameter_mode, .require_pjrt_memory_plan = true }) and value.pjrt_memory_plan.?.platform == prefill.pjrt_memory_plan.?.platform and std.mem.eql(u8, &value.pjrt_memory_plan.?.plugin_sha256, &prefill.pjrt_memory_plan.?.plugin_sha256);
}

fn inRange(seq_len: usize, request: Request) bool {
    return seq_len > request.seq_len and (request.decode_steps == null or seq_len - request.seq_len <= request.decode_steps.?);
}

fn completeChain(catalog: *Catalog, prefill: usize, decodes: []usize, required: ?usize) !?[]usize {
    std.mem.sort(usize, decodes, catalog, struct {
        fn lessThan(c: *Catalog, a: usize, b: usize) bool {
            return c.entries.items[a].parsed.value.seq_len < c.entries.items[b].parsed.value.seq_len;
        }
    }.lessThan);
    for (decodes, 0..) |index, i| {
        if (i > 0 and catalog.entries.items[index].parsed.value.seq_len == catalog.entries.items[decodes[i - 1]].parsed.value.seq_len) return error.AmbiguousCompiledArtifact;
    }
    var count: usize = 0;
    var previous = catalog.entries.items[prefill].parsed.value;
    for (decodes) |index| {
        const next = catalog.entries.items[index].parsed.value;
        if (next.seq_len != (std.math.add(usize, previous.seq_len, 1) catch break) or !kvShapesCompatible(previous, next)) break;
        count += 1;
        previous = next;
    }
    if (required) |steps| if (count != steps) return null;
    const chain = try catalog.allocator.alloc(usize, count + 1);
    chain[0] = prefill;
    @memcpy(chain[1..], decodes[0..count]);
    return chain;
}

fn packageChain(catalog: *Catalog, directory: []const u8, request: Request, mode: []const u8) !?[]usize {
    const path = try artifact.packageManifestPath(catalog.allocator, directory, "xla", request.model_dir, "pjrt_executable", mode);
    defer catalog.allocator.free(path);
    var package = artifact.readPackageManifest(catalog.allocator, catalog.io, path) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer package.deinit();
    try artifact.validatePackageManifest(package.value, "xla", request.model_dir, "pjrt_executable", mode);
    const entry = (try artifact.findUniqueMatchingPackageEntry(package.value, .{ .artifact_role = artifact.artifact_role_prefill, .seq_len = request.seq_len, .query_seq_len = request.query_seq_len, .attention_mode = request.attention_mode })) orelse return null;
    const prefill = (try catalog.read(entry.manifest_path, request)) orelse return null;
    const value = catalog.entries.items[prefill].parsed.value;
    if (!catalog.entries.items[prefill].available or !matchesPrefill(value, request, mode)) return null;
    var decodes: std.ArrayListUnmanaged(usize) = .empty;
    defer decodes.deinit(catalog.allocator);
    for (package.value.artifacts) |decode| {
        if (!artifact.packageEntryMatches(decode, .{ .artifact_role = artifact.artifact_role_decode, .query_seq_len = 1, .attention_mode = "paged_decode" }) or !inRange(decode.seq_len, request)) continue;
        const index = (try catalog.read(decode.manifest_path, request)) orelse return null;
        const parsed = catalog.entries.items[index].parsed.value;
        if (!catalog.entries.items[index].available or parsed.seq_len != decode.seq_len or !matchesDecode(parsed, value)) return null;
        try decodes.append(catalog.allocator, index);
    }
    return completeChain(catalog, prefill, decodes.items, request.decode_steps);
}

fn standaloneChain(catalog: *Catalog, request: Request, mode: []const u8) !?[]usize {
    var selected: ?[]usize = null;
    errdefer if (selected) |chain| catalog.allocator.free(chain);
    for (catalog.entries.items, 0..) |prefill, first| {
        if (!prefill.standalone or !prefill.available or !matchesPrefill(prefill.parsed.value, request, mode)) continue;
        var decodes: std.ArrayListUnmanaged(usize) = .empty;
        defer decodes.deinit(catalog.allocator);
        for (catalog.entries.items, 0..) |entry, index| {
            if (entry.standalone and entry.available and inRange(entry.parsed.value.seq_len, request) and matchesDecode(entry.parsed.value, prefill.parsed.value)) try decodes.append(catalog.allocator, index);
        }
        const chain = (try completeChain(catalog, first, decodes.items, request.decode_steps)) orelse continue;
        if (selected != null) {
            catalog.allocator.free(chain);
            return error.AmbiguousCompiledArtifact;
        }
        selected = chain;
    }
    return selected;
}

/// Prefer embedded parameters only among complete compatible execution chains.
/// Missing coverage can select another candidate; identity/deserialization
/// failures from the selected chain remain errors in the executor loader.
pub fn findAdmitted(allocator: std.mem.Allocator, io: std.Io, directory: []const u8, request: Request, context: *const @import("pjrt_admission.zig").Context) !?Selection {
    const budget = try @import("pjrt_metadata.zig").Budget.create(allocator, context);
    var transferred = false;
    defer if (!transferred) budget.destroy();
    var selection = (find(budget.allocator(), io, directory, request) catch |err| return budget.translate(err)) orelse return null;
    errdefer selection.deinit();
    selection.manifests = selection.values(budget.allocator()) catch |err| return budget.translate(err);
    selection.budget = budget;
    budget.finishConstruction();
    transferred = true;
    return selection;
}

fn find(allocator: std.mem.Allocator, io: std.Io, directory: []const u8, request: Request) !?Selection {
    for ([_][]const u8{ artifact.pjrt_parameter_mode_embedded, artifact.pjrt_parameter_mode_inputs }) |mode| {
        // Inspect one priority at a time. Failed/incomplete higher-priority
        // metadata is released before attempting the fallback, while package
        // and standalone lookup still share parsed entries within this mode.
        var catalog = Catalog{ .allocator = allocator, .io = io, .parameter_mode = mode };
        var owned = true;
        defer if (owned) catalog.deinit();
        if (try packageChain(&catalog, directory, request, mode)) |chain| {
            owned = false;
            return .{ .catalog = catalog, .chain = chain };
        }
        try catalog.scan(directory, request);
        if (try standaloneChain(&catalog, request, mode)) |chain| {
            owned = false;
            return .{ .catalog = catalog, .chain = chain };
        }
    }
    return null;
}

fn selectedModeScenario(a: std.mem.Allocator, directory: []const u8, steps: usize, expected_mode: []const u8) !void {
    var selection = (try find(a, std.testing.io, directory, .{ .model_dir = directory, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = steps })) orelse return error.TestUnexpectedResult;
    defer selection.deinit();
    const values_ = try selection.values(a);
    defer a.free(values_);
    try std.testing.expectEqual(steps + 1, values_.len);
    try std.testing.expectEqualStrings(expected_mode, values_[0].pjrt_parameter_mode);
}

test "managed PJRT chain selection prefers modes only among complete compatible candidates" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const embedded = try std.fs.path.join(a, &.{ root, "embedded.inference.json" });
    defer a.free(embedded);
    const inputs = try std.fs.path.join(a, &.{ root, "inputs.inference.json" });
    defer a.free(inputs);
    const decode = try std.fs.path.join(a, &.{ root, "decode.inference.json" });
    defer a.free(decode);
    const executable = try std.fs.path.join(a, &.{ root, "model.exec" });
    defer a.free(executable);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.exec", .data = "not-read-by-selector" });
    var prefill = artifact.Manifest{ .backend = "xla", .kind = "pjrt_executable", .model_dir = root, .artifact_path = executable, .prompt_tokens = 1, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .raw_prompt = true, .chat_template_applied = false, .pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{} } };
    try artifact.writeManifest(a, io, embedded, prefill);
    prefill.pjrt_parameter_mode = artifact.pjrt_parameter_mode_inputs;
    try artifact.writeManifest(a, io, inputs, prefill);
    var decoded = prefill;
    decoded.artifact_role = artifact.artifact_role_decode;
    decoded.attention_mode = "paged_decode";
    decoded.seq_len = 2;
    try artifact.writeManifest(a, io, decode, decoded);
    const embedded_package = try artifact.packageManifestPath(a, root, "xla", root, "pjrt_executable", artifact.pjrt_parameter_mode_embedded);
    defer a.free(embedded_package);
    const inputs_package = try artifact.packageManifestPath(a, root, "xla", root, "pjrt_executable", artifact.pjrt_parameter_mode_inputs);
    defer a.free(inputs_package);
    const prefill_entry = artifact.PackageArtifactEntry{ .manifest_path = embedded, .artifact_path = prefill.artifact_path, .artifact_role = "prefill", .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill" };
    var inputs_entry = prefill_entry;
    inputs_entry.manifest_path = inputs;
    const decode_entry = artifact.PackageArtifactEntry{ .manifest_path = decode, .artifact_path = decoded.artifact_path, .artifact_role = "decode", .seq_len = 2, .query_seq_len = 1, .attention_mode = "paged_decode" };
    try artifact.writePackageManifest(a, io, embedded_package, .{ .backend = "xla", .model_dir = root, .kind = "pjrt_executable", .artifacts = &.{prefill_entry} });
    var package = artifact.PackageManifest{ .backend = "xla", .model_dir = root, .kind = "pjrt_executable", .pjrt_parameter_mode = artifact.pjrt_parameter_mode_inputs, .artifacts = &.{ inputs_entry, decode_entry } };
    try artifact.writePackageManifest(a, io, inputs_package, package);
    const failures = @import("antfly_platform").allocator;
    try failures.checkAllAllocationFailures(a, selectedModeScenario, .{ root, @as(usize, 1), artifact.pjrt_parameter_mode_inputs });
    try selectedModeScenario(a, root, 0, artifact.pjrt_parameter_mode_embedded);
    try admittedMetadataScenario(root, embedded, embedded_package);

    // Missing executable bytes make a candidate unavailable, even if its
    // sidecar still exists. Selection does not hash/read the bytes themselves.
    var unavailable = prefill;
    unavailable.pjrt_parameter_mode = artifact.pjrt_parameter_mode_embedded;
    const missing = try std.fs.path.join(a, &.{ root, "missing.exec" });
    defer a.free(missing);
    unavailable.artifact_path = missing;
    try artifact.writeManifest(a, io, embedded, unavailable);
    try selectedModeScenario(a, root, 0, artifact.pjrt_parameter_mode_inputs);
    unavailable.artifact_path = executable;
    try artifact.writeManifest(a, io, embedded, unavailable);

    // An incomplete package must not hide a complete standalone chain either.
    package.artifacts = &.{inputs_entry};
    try artifact.writePackageManifest(a, io, inputs_package, package);
    try selectedModeScenario(a, root, 1, artifact.pjrt_parameter_mode_inputs);
    const duplicate = try std.fs.path.join(a, &.{ root, "duplicate.inference.json" });
    defer a.free(duplicate);
    // Two incomplete embedded prefills still cannot mask complete inputs.
    try artifact.writeManifest(a, io, duplicate, unavailable);
    try selectedModeScenario(a, root, 1, artifact.pjrt_parameter_mode_inputs);
    // Ambiguity among complete chains remains an error.
    try artifact.writeManifest(a, io, duplicate, prefill);
    try std.testing.expectError(error.AmbiguousCompiledArtifact, find(a, io, root, .{ .model_dir = root, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 1 }));
    try tmp.dir.deleteFile(io, "duplicate.inference.json");
    // Missing files, unsupported profiles, gaps, and provider mismatches never
    // qualify as coverage; no executable bytes or provider are opened here.
    decoded.seq_len = 3;
    try artifact.writeManifest(a, io, decode, decoded);
    try std.testing.expectEqual(null, try find(a, io, root, .{ .model_dir = root, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 1 }));
    decoded.seq_len = 2;
    decoded.pjrt_memory_plan.?.plugin_sha256[0] = 1;
    try artifact.writeManifest(a, io, decode, decoded);
    try std.testing.expectEqual(null, try find(a, io, root, .{ .model_dir = root, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 1 }));
    decoded.pjrt_memory_plan = null;
    try artifact.writeManifest(a, io, decode, decoded);
    try std.testing.expectEqual(null, try find(a, io, root, .{ .model_dir = root, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 1 }));
    try tmp.dir.deleteFile(io, "decode.inference.json");
    try std.testing.expectEqual(null, try find(a, io, root, .{ .model_dir = root, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 1 }));
}

test "managed PJRT chain KV compatibility uses node identities for sparse semantic bindings" {
    var logits = [_]i64{2};
    var kv = [_]i64{1};
    var outputs = [_][]i64{ &logits, &kv };
    var inputs = [_][]i64{&kv};
    var nodes = [_]u32{ 7, 19 };
    var present = [_]artifact.PjrtOutputBindingMeta{.{ .kind = artifact.pjrt_binding_present_key, .node_id = 19, .layer_index = 0 }};
    var past = [_]artifact.PjrtInputBindingMeta{.{ .kind = artifact.pjrt_binding_past_key, .node_id = 2, .layer_index = 0 }};
    var source = artifact.Manifest{ .backend = "xla", .kind = "pjrt_executable", .model_dir = "model", .artifact_path = "unused", .prompt_tokens = 1, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .raw_prompt = true, .chat_template_applied = false, .pjrt_output_shapes = &outputs, .pjrt_output_node_ids = &nodes, .pjrt_output_bindings = &present };
    var target = source;
    target.pjrt_input_shapes = &inputs;
    target.pjrt_input_bindings = &past;
    try std.testing.expect(kvShapesCompatible(source, target));
    present[0].node_id = 7;
    try std.testing.expect(!kvShapesCompatible(source, target));
    present[0].node_id = 19;
    past[0].layer_index = 1;
    try std.testing.expect(!kvShapesCompatible(source, target));
    past[0].layer_index = 0;
    source.pjrt_output_shapes = outputs[0..1];
    try std.testing.expect(!kvShapesCompatible(source, target));
}

fn admittedSelectionFailureScenario(a: std.mem.Allocator, directory: []const u8, context: *const @import("pjrt_admission.zig").Context) !void {
    const before = context.manager.currentAdmissionAmounts();
    defer std.debug.assert(std.meta.eql(before, context.manager.currentAdmissionAmounts()));
    var selection = (try findAdmitted(a, std.testing.io, directory, .{ .model_dir = directory, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 0 }, context)) orelse return error.TestUnexpectedResult;
    defer selection.deinit();
    try std.testing.expect(selection.manifests.?.len == 1);
    try std.testing.expect(context.manager.currentAdmissionAmounts().host_weight_bytes > before.host_weight_bytes);
}

fn admittedMetadataScenario(directory: []const u8, sidecar: []const u8, package: []const u8) !void {
    const a = std.testing.allocator;
    const manager_mod = @import("../server/model_manager.zig");
    var manager = manager_mod.ModelManager.init(a, @import("../backends/backends.zig").SessionManager.init(a));
    defer manager.deinit();
    const policy = manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 1024 * 1024, .combined_limit_bytes = 1024 * 1024 } };
    try policy.configure(&manager);
    const context = @import("pjrt_admission.zig").Context{ .allocator = a, .manager = &manager, .policy = policy };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, admittedSelectionFailureScenario, .{ directory, &context });
    {
        var selection = (try findAdmitted(a, std.testing.io, directory, .{ .model_dir = directory, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 0 }, &context)) orelse return error.TestUnexpectedResult;
        defer selection.deinit();
        const retained = manager.currentAdmissionAmounts().host_weight_bytes;
        try std.testing.expect(retained > 0);
        var competing = try manager.acquireRunResourceAmounts(.cpu, @import("pjrt_admission.zig").ArtifactDeviceAdmission.limitsFor(&context, true), .{ .host_weight_bytes = 1024 * 1024 - retained });
        defer competing.release();
        // Each selection fits alone; outstanding ownership makes the second
        // temporarily unavailable rather than an impossible-request error.
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, admittedSelectionFailureScenario(a, directory, &context));
    }
    // The raw file read is admitted before the backing allocator sees it.
    // Test both the package and the selected executable's sidecar.
    for ([_][]const u8{ package, sidecar }) |path| {
        const io = std.testing.io;
        const original = try std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(1024 * 1024));
        defer a.free(original);
        const padded = try a.alloc(u8, 2 * 1024 * 1024);
        defer a.free(padded);
        @memset(padded, ' ');
        @memcpy(padded[0..original.len], original);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = padded });
        defer std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = original }) catch unreachable;
        var quota = @import("antfly_platform").allocator.reclamation.Quota.init(a, 1024 * 1024);
        defer quota.deinit();
        try std.testing.expectError(error.ResourceLimitExceeded, admittedSelectionFailureScenario(quota.allocator(), directory, &context));
        try std.testing.expect(quota.largest_rejected_growth == 0);
    }
    // A reclaiming allocator ensures JSON arenas and temporary reads refund
    // real memory even when the caller supplies an arena.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try admittedSelectionFailureScenario(arena.allocator(), directory, &context);
}

test "managed PJRT streaming inspection skips unrelated large fields before full allocation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const path = try std.fs.path.join(a, &.{ directory, "large.inference.json" });
    defer a.free(path);
    const large = try a.alloc(u8, 2 * 1024 * 1024);
    defer a.free(large);
    @memset(large, 'k');
    const manager_mod = @import("../server/model_manager.zig");
    var manager = manager_mod.ModelManager.init(a, @import("../backends/backends.zig").SessionManager.init(a));
    defer manager.deinit();
    const policy = manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 128 * 1024 } };
    try policy.configure(&manager);
    const context = @import("pjrt_admission.zig").Context{ .allocator = a, .manager = &manager, .policy = policy };
    const request = Request{ .model_dir = directory, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 0 };
    for ([_][]const u8{ "pjrt_hlo", "pjrt_executable" }) |kind| {
        const bytes = try std.json.Stringify.valueAlloc(a, .{ .kind = kind, .backend = "xla", .model_dir = directory, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .artifact_path = "unused", .prompt_tokens = 1, .raw_prompt = true, .chat_template_applied = false, .ignored = large, .pjrt_memory_plan = artifact.PjrtMemoryPlan{ .platform = .cpu, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{} } }, .{});
        defer a.free(bytes);
        try tmp.dir.writeFile(io, .{ .sub_path = "large.inference.json", .data = bytes });
        var quota = @import("antfly_platform").allocator.reclamation.Quota.init(a, 128 * 1024);
        defer quota.deinit();
        const budget = try @import("pjrt_metadata.zig").Budget.create(quota.allocator(), &context);
        defer budget.destroy();
        var catalog = Catalog{ .allocator = budget.allocator(), .io = io };
        defer catalog.deinit();
        if (std.mem.eql(u8, kind, "pjrt_hlo")) {
            try std.testing.expectEqual(null, try catalog.read(path, request));
            try std.testing.expect(quota.peak_bytes < 64 * 1024);
        } else {
            try std.testing.expectError(error.OutOfMemory, catalog.read(path, request));
            try std.testing.expectEqual(error.ResourceLimitExceeded, budget.translate(error.OutOfMemory));
            try std.testing.expectEqual(@as(usize, 0), quota.largest_rejected_growth);
        }
    }
}

test "managed PJRT layerless chain compatibility uses the runtime identity" {
    var small = [_]i64{1};
    var large = [_]i64{2};
    var outputs = [_][]i64{ &small, &large };
    var inputs = [_][]i64{&large};
    var nodes = [_]u32{ 1, 2 };
    var present = [_]artifact.PjrtOutputBindingMeta{
        .{ .kind = artifact.pjrt_binding_present_key, .node_id = 1, .name = "state.a" },
        .{ .kind = artifact.pjrt_binding_present_key, .node_id = 2, .name = "state.b" },
    };
    var past = [_]artifact.PjrtInputBindingMeta{.{ .kind = artifact.pjrt_binding_past_key, .node_id = 3, .name = "state.b" }};
    var source = artifact.Manifest{ .backend = "xla", .kind = "pjrt_executable", .model_dir = "model", .artifact_path = "unused", .prompt_tokens = 1, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .raw_prompt = true, .chat_template_applied = false, .pjrt_output_shapes = &outputs, .pjrt_output_node_ids = &nodes, .pjrt_output_bindings = &present };
    var target = source;
    target.pjrt_input_shapes = &inputs;
    target.pjrt_input_bindings = &past;
    // The first output has a different identity and shape; skip it.
    try std.testing.expect(kvShapesCompatible(source, target));
    past[0].name = "state.missing";
    try std.testing.expect(!kvShapesCompatible(source, target));
    past[0].name = "state.a";
    try std.testing.expect(!kvShapesCompatible(source, target));
    past[0].name = "state.b";
    source.pjrt_output_bindings = present[0..1];
    try std.testing.expect(!kvShapesCompatible(source, target));
    source.pjrt_output_bindings = &present;
    // Layered phase aliases remain compatible regardless of name.
    past[0].layer_index = 7;
    present[1].layer_index = 7;
    past[0].name = "past.alias";
    try std.testing.expect(kvShapesCompatible(source, target));
    // Ambiguous sources cannot share one logical state.
    present[0].layer_index = 7;
    try std.testing.expect(!kvShapesCompatible(source, target));
}

test "managed PJRT mode priority skips unselected metadata and releases fallback catalogs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const exe = try std.fs.path.join(a, &.{ root, "model.exec" });
    defer a.free(exe);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.exec", .data = "unused-by-selection" });
    const embedded = try std.fs.path.join(a, &.{ root, "embedded.inference.json" });
    defer a.free(embedded);
    const manifest = artifact.Manifest{ .backend = "xla", .kind = "pjrt_executable", .model_dir = root, .artifact_path = exe, .prompt_tokens = 1, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .raw_prompt = true, .chat_template_applied = false, .pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{} } };
    try artifact.writeManifest(a, io, embedded, manifest);
    const manager_mod = @import("../server/model_manager.zig");
    var manager = manager_mod.ModelManager.init(a, @import("../backends/backends.zig").SessionManager.init(a));
    defer manager.deinit();
    const policy = manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 128 * 1024 } };
    try policy.configure(&manager);
    const context = @import("pjrt_admission.zig").Context{ .allocator = a, .manager = &manager, .policy = policy };
    const request = Request{ .model_dir = root, .seq_len = 1, .query_seq_len = 1, .attention_mode = "paged_prefill", .decode_steps = 0 };
    {
        var selected = (try findAdmitted(a, io, root, request, &context)).?;
        defer selected.deinit();
        try std.testing.expectEqualStrings(artifact.pjrt_parameter_mode_embedded, selected.manifests.?[0].pjrt_parameter_mode);
    }
    const padding = try a.alloc(u8, 256 * 1024);
    defer a.free(padding);
    @memset(padding, ' ');
    var alternative = manifest;
    alternative.pjrt_parameter_mode = artifact.pjrt_parameter_mode_inputs;
    const json = try std.json.Stringify.valueAlloc(a, alternative, .{});
    defer a.free(json);
    @memcpy(padding[0..json.len], json);
    try tmp.dir.writeFile(io, .{ .sub_path = "inputs.inference.json", .data = padding });
    {
        var quota = @import("antfly_platform").allocator.reclamation.Quota.init(a, 128 * 1024);
        defer quota.deinit();
        var selected = (try findAdmitted(quota.allocator(), io, root, request, &context)).?;
        defer selected.deinit();
        try std.testing.expectEqualStrings(artifact.pjrt_parameter_mode_embedded, selected.manifests.?[0].pjrt_parameter_mode);
        try std.testing.expect(quota.peak_bytes < 64 * 1024);
        try std.testing.expectEqual(@as(usize, 0), quota.largest_rejected_growth);
    }

    var shapes: [4000][]i64 = @splat(&.{});
    alternative.pjrt_input_shapes = &shapes;
    const compact = try std.json.Stringify.valueAlloc(a, alternative, .{});
    defer a.free(compact);
    try std.testing.expect(compact.len < 16 * 1024);
    try tmp.dir.writeFile(io, .{ .sub_path = "inputs.inference.json", .data = compact });
    {
        var quota = @import("antfly_platform").allocator.reclamation.Quota.init(a, 128 * 1024);
        defer quota.deinit();
        var selected = (try findAdmitted(quota.allocator(), io, root, request, &context)).?;
        defer selected.deinit();
        try std.testing.expectEqualStrings(artifact.pjrt_parameter_mode_embedded, selected.manifests.?[0].pjrt_parameter_mode);
        try std.testing.expect(quota.peak_bytes < 64 * 1024);
    }
    {
        // Explicit-runner discovery shares the same streaming mode filter.
        var quota = @import("antfly_platform").allocator.reclamation.Quota.init(a, 128 * 1024);
        defer quota.deinit();
        var located = (try artifact.findUniqueMatchingArtifactPath(quota.allocator(), io, root, .{
            .backend = "xla",
            .model_dir = root,
            .kind = "pjrt_executable",
            .artifact_role = artifact.artifact_role_prefill,
            .seq_len = 1,
            .query_seq_len = 1,
            .attention_mode = "paged_prefill",
            .pjrt_parameter_mode = artifact.pjrt_parameter_mode_embedded,
            .require_pjrt_memory_plan = true,
        })).?;
        defer located.deinit(quota.allocator());
        try std.testing.expectEqualStrings(exe, located.artifact_path);
        try std.testing.expect(quota.peak_bytes < 64 * 1024);
        try std.testing.expectEqual(@as(usize, 0), quota.largest_rejected_growth);
    }
    alternative.pjrt_input_shapes = &.{};

    // Both modes fit separately. The incomplete embedded arena must be freed
    // before parsing the inputs fallback, rather than accumulating catalogs.
    const source = try a.alloc(u8, 40 * 1024);
    defer a.free(source);
    @memset(source, 's');
    var large_embedded = manifest;
    large_embedded.source_path = source;
    try artifact.writeManifest(a, io, embedded, large_embedded);
    alternative.source_path = source;
    const inputs_path = try std.fs.path.join(a, &.{ root, "inputs.inference.json" });
    defer a.free(inputs_path);
    try artifact.writeManifest(a, io, inputs_path, alternative);
    var decode = alternative;
    decode.source_path = "";
    decode.seq_len = 2;
    decode.artifact_role = artifact.artifact_role_decode;
    decode.attention_mode = "paged_decode";
    const decode_path = try std.fs.path.join(a, &.{ root, "decode.inference.json" });
    defer a.free(decode_path);
    try artifact.writeManifest(a, io, decode_path, decode);
    var fallback_manager = manager_mod.ModelManager.init(a, @import("../backends/backends.zig").SessionManager.init(a));
    defer fallback_manager.deinit();
    const fallback_policy = manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 192 * 1024 } };
    try fallback_policy.configure(&fallback_manager);
    const fallback_context = @import("pjrt_admission.zig").Context{ .allocator = a, .manager = &fallback_manager, .policy = fallback_policy };
    var fallback_request = request;
    fallback_request.decode_steps = 1;
    {
        var quota = @import("antfly_platform").allocator.reclamation.Quota.init(a, 192 * 1024);
        defer quota.deinit();
        var selected = (try findAdmitted(quota.allocator(), io, root, fallback_request, &fallback_context)).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 2), selected.manifests.?.len);
        try std.testing.expectEqualStrings(artifact.pjrt_parameter_mode_inputs, selected.manifests.?[0].pjrt_parameter_mode);
        try std.testing.expectEqual(@as(usize, 0), quota.largest_rejected_growth);
    }
    // Priority filtering must preserve ambiguity checks within the selected mode.
    alternative.source_path = "";
    const duplicate = try std.fs.path.join(a, &.{ root, "duplicate.inference.json" });
    defer a.free(duplicate);
    try artifact.writeManifest(a, io, duplicate, alternative);
    try std.testing.expectError(error.AmbiguousCompiledArtifact, findAdmitted(a, io, root, fallback_request, &fallback_context));
}
