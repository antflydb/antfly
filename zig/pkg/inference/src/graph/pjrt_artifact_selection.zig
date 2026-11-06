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
/// standalone fallback share one catalog, so each sidecar is parsed once.
pub const Selection = struct {
    catalog: Catalog,
    chain: []usize,

    pub fn values(self: *const @This(), allocator: std.mem.Allocator) ![]artifact.Manifest {
        const result = try allocator.alloc(artifact.Manifest, self.chain.len);
        for (self.chain, result) |index, *value| value.* = self.catalog.entries.items[index].parsed.value;
        return result;
    }

    pub fn deinit(self: *@This()) void {
        self.catalog.allocator.free(self.chain);
        self.catalog.deinit();
    }
};

const Catalog = struct {
    const Entry = struct { path: []u8, parsed: std.json.Parsed(artifact.Manifest), available: bool, standalone: bool = false };
    allocator: std.mem.Allocator,
    io: std.Io,
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
        var parsed = artifact.readManifest(self.allocator, self.io, path) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        errdefer parsed.deinit();
        if (filter) |request| {
            const value = parsed.value;
            const relevant_prefill = matchesPrefill(value, request, artifact.pjrt_parameter_mode_embedded) or matchesPrefill(value, request, artifact.pjrt_parameter_mode_inputs);
            const relevant_decode = artifact.hasManagedPjrtPlan(value) and std.mem.eql(u8, value.backend, "xla") and std.mem.eql(u8, value.kind, "pjrt_executable") and std.mem.eql(u8, value.model_dir, request.model_dir) and std.mem.eql(u8, value.artifact_role, artifact.artifact_role_decode) and value.query_seq_len == 1 and std.mem.eql(u8, value.attention_mode, "paged_decode") and inRange(value.seq_len, request);
            if (!relevant_prefill and !relevant_decode) {
                parsed.deinit();
                return null;
            }
        }
        const stat = std.Io.Dir.cwd().statFile(self.io, parsed.value.artifact_path, .{}) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        const available = if (stat) |info| info.kind == .file and info.size > 0 else false;
        const owned = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned);
        try self.entries.ensureUnusedCapacity(self.allocator, 1);
        try self.by_path.ensureUnusedCapacity(self.allocator, 1);
        const index = self.entries.items.len;
        self.entries.appendAssumeCapacity(.{ .path = owned, .parsed = parsed, .available = available });
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

fn matchesPrefill(value: artifact.Manifest, request: Request, mode: []const u8) bool {
    return artifact.matchesRequest(value, .{ .backend = "xla", .model_dir = request.model_dir, .kind = "pjrt_executable", .artifact_role = artifact.artifact_role_prefill, .seq_len = request.seq_len, .query_seq_len = request.query_seq_len, .attention_mode = request.attention_mode, .pjrt_parameter_mode = mode, .require_pjrt_memory_plan = true });
}

pub fn kvShapesCompatible(source: artifact.Manifest, target: artifact.Manifest) bool {
    if (source.pjrt_output_shapes.len != source.pjrt_output_node_ids.len or target.pjrt_input_shapes.len != target.pjrt_input_bindings.len) return false;
    for (target.pjrt_input_bindings, target.pjrt_input_shapes) |input, shape| {
        const present_kind = if (std.mem.eql(u8, input.kind, artifact.pjrt_binding_past_key)) artifact.pjrt_binding_present_key else if (std.mem.eql(u8, input.kind, artifact.pjrt_binding_past_value)) artifact.pjrt_binding_present_value else continue;
        var found = false;
        for (source.pjrt_output_bindings) |output| {
            if (output.layer_index != input.layer_index or !std.mem.eql(u8, output.kind, present_kind)) continue;
            for (source.pjrt_output_node_ids, source.pjrt_output_shapes) |node, output_shape| {
                if (node != output.node_id) continue;
                if (!std.mem.eql(i64, output_shape, shape)) return false;
                found = true;
                break;
            }
            if (found) break;
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
    const prefill = (try catalog.read(entry.manifest_path, null)) orelse return null;
    const value = catalog.entries.items[prefill].parsed.value;
    if (!catalog.entries.items[prefill].available or !matchesPrefill(value, request, mode)) return null;
    var decodes: std.ArrayListUnmanaged(usize) = .empty;
    defer decodes.deinit(catalog.allocator);
    for (package.value.artifacts) |decode| {
        if (!artifact.packageEntryMatches(decode, .{ .artifact_role = artifact.artifact_role_decode, .query_seq_len = 1, .attention_mode = "paged_decode" }) or !inRange(decode.seq_len, request)) continue;
        const index = (try catalog.read(decode.manifest_path, null)) orelse return null;
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
pub fn find(allocator: std.mem.Allocator, io: std.Io, directory: []const u8, request: Request) !?Selection {
    var catalog = Catalog{ .allocator = allocator, .io = io };
    var owned = true;
    defer if (owned) catalog.deinit();
    var scanned = false;
    for ([_][]const u8{ artifact.pjrt_parameter_mode_embedded, artifact.pjrt_parameter_mode_inputs }) |mode| {
        if (try packageChain(&catalog, directory, request, mode)) |chain| {
            owned = false;
            return .{ .catalog = catalog, .chain = chain };
        }
        if (!scanned) {
            try catalog.scan(directory, request);
            scanned = true;
        }
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
