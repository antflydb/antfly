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

//! Model-owned immutable encoder/decoder generation. Serving supplies admitted
//! component loading and artifact-generation checks, then publishes this owner.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const runtime = @import("../runtime/root.zig");
const manifest_mod = @import("manifest.zig");
const managed_receipt = @import("../registry/managed_receipt.zig");
const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
const encoder_decoder = @import("../pipelines/encoder_decoder.zig");
const tokenizer_mod = @import("inference_tokenizer");
const ManagedHfTokenizer = @import("managed_tokenizer.zig").Managed;
const ManagedSession = @import("session_loader.zig").ManagedSession;
const ComponentLoader = @import("component_loader.zig").ComponentLoader;
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const RuntimeId = u128;
const ComponentPlanKey = [32]u8;

pub const Request = struct {
    model_dir: []const u8,
    component_paths: []const []const u8,
    preferred_backends: []const backends.BackendType,
    asset_generation: ComponentPlanKey,
    runtime_id: RuntimeId,
    model_type: manifest_mod.ModelType,
    kind: CompositeKind,
};
pub const Host = struct {
    context: *anyopaque,
    io: std.Io,
    load_tokenizer_fn: *const fn (*anyopaque, []const u8) anyerror!ManagedHfTokenizer,
    plan_components_fn: *const fn (*anyopaque, []const u8, []const backends.BackendType, []const []const u8) anyerror!ComponentLoader,
    identity_fn: *const fn (*anyopaque, Request) anyerror!ComponentPlanKey,
};

pub const CompositeKind = enum { whisper_metadata, whisper, seq2seq };

/// One immutable publication pinned across tokenization, encode and decode.
/// Component sessions keep their resident admission and backend ownership.
pub const CompositeAssets = struct {
    runtime_id: RuntimeId = 0,
    allocator: std.mem.Allocator = std.heap.page_allocator,
    model_dir: []u8 = &.{},
    model_dir_owned: bool = false,
    preferred_backends: []backends.BackendType = &.{},
    preferred_backends_owned: bool = false,
    startup_managed: bool = false,
    model_type: manifest_mod.ModelType = .transcriber,
    encoder_gate: std.atomic.Mutex = .unlocked,
    decoder_gate: std.atomic.Mutex = .unlocked,
    managed_tokenizer: ManagedHfTokenizer,
    prompt_cache: ?whisper_prompt.PromptCache,
    encoder: ?ManagedSession = null,
    decoder: ?ManagedSession = null,
    kind: CompositeKind = .whisper_metadata,
    decoder_execution: enum { full_prefix, merged_kv } = .full_prefix,
    considered_merged_decoder: bool = false,
    decoder_config: encoder_decoder.DecoderConfig,
    generation: ComponentPlanKey,
    active_handles: usize = 0,
    last_used_ns: u64 = 0,

    pub fn tokenizer(self: *const CompositeAssets) tokenizer_mod.Tokenizer {
        return self.managed_tokenizer.tokenizer.tokenizer();
    }

    pub fn reclaimableAdmissions(self: *const CompositeAssets) [3]?runtime.tier.memory.AdmissionLease {
        return .{
            self.managed_tokenizer.reclaimableAdmission(),
            if (self.encoder) |managed| managed.resource_lease else null,
            if (self.decoder) |managed| managed.resource_lease else null,
        };
    }

    pub fn deinit(self: *CompositeAssets) void {
        var encoder_scope = if (self.encoder) |managed| managed.session.beginClose() else backends.Session.CloseScope{};
        defer encoder_scope.deinit();
        var decoder_scope = if (self.decoder) |managed| managed.session.beginClose() else backends.Session.CloseScope{};
        defer decoder_scope.deinit();
        if (self.decoder) |*managed| managed.deinit();
        if (self.encoder) |*managed| managed.deinit();
        if (self.prompt_cache) |*cache| cache.deinit();
        self.managed_tokenizer.deinit();
        if (self.preferred_backends_owned) self.allocator.free(self.preferred_backends);
        if (self.model_dir_owned) self.allocator.free(self.model_dir);
        self.* = undefined;
    }
};

const ResolvedWhisperSidecars = struct {
    allocator: std.mem.Allocator,
    tokenizer_path: []u8,
    config_path: []u8,
    generation_config_path: ?[]u8,

    pub fn deinit(self: *ResolvedWhisperSidecars) void {
        self.allocator.free(self.tokenizer_path);
        self.allocator.free(self.config_path);
        if (self.generation_config_path) |path| self.allocator.free(path);
        self.* = undefined;
    }
};

fn resolveWhisperSidecars(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_dir: []const u8,
) !ResolvedWhisperSidecars {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const tokenizer_path = manifest.tokenizer_json_path orelse
        return error.TokenizerNotFound;
    const config_path = manifest.config_path orelse
        return error.InvalidWhisperDecoderConfig;

    const owned_tokenizer_path = try allocator.dupe(u8, tokenizer_path);
    errdefer allocator.free(owned_tokenizer_path);
    const owned_config_path = try allocator.dupe(u8, config_path);
    errdefer allocator.free(owned_config_path);

    var receipt = try managed_receipt.loadValidated(
        allocator,
        io,
        model_dir,
    );
    defer if (receipt) |*validated| validated.deinit();
    const generation_config_path: ?[]u8 = if (receipt) |*validated|
        if (validated.find("generation_config.json")) |artifact|
            try allocator.dupe(u8, artifact.canonical_path)
        else
            null
    else
        managed_receipt.resolveContainedArtifactPath(
            allocator,
            io,
            model_dir,
            "generation_config.json",
        ) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };

    return .{
        .allocator = allocator,
        .tokenizer_path = owned_tokenizer_path,
        .config_path = owned_config_path,
        .generation_config_path = generation_config_path,
    };
}
pub fn load(allocator: std.mem.Allocator, host: Host, request: Request, control: ?InferenceExecutionControl) !*CompositeAssets {
    const model_dir = request.model_dir;
    const component_paths = request.component_paths;
    const preferred_backends = request.preferred_backends;
    const asset_generation = request.asset_generation;
    const runtime_id = request.runtime_id;
    const model_type = request.model_type;
    const kind = request.kind;

    if (control) |active| try active.check();
    var sidecars = try resolveWhisperSidecars(allocator, host.io, model_dir);
    defer sidecars.deinit();

    var managed_tokenizer = try host.load_tokenizer_fn(host.context, sidecars.tokenizer_path);
    errdefer managed_tokenizer.deinit();
    var prompt_cache: ?whisper_prompt.PromptCache = if (kind == .seq2seq) null else try whisper_prompt.PromptCache.initFromPaths(
        allocator,
        sidecars.generation_config_path,
        sidecars.config_path,
        managed_tokenizer.tokenizer.tokenizer(),
    );
    errdefer if (prompt_cache) |*cache| cache.deinit();
    const decoder_config = try encoder_decoder.loadDecoderConfigFile(
        allocator,
        sidecars.config_path,
    );
    var encoder: ?ManagedSession = null;
    errdefer if (encoder) |*managed| managed.deinit();
    var decoder: ?ManagedSession = null;
    errdefer if (decoder) |*managed| managed.deinit();
    if (kind != .whisper_metadata) {
        if (component_paths.len != 2 and component_paths.len != 3) return error.InvalidModelLayout;
        var metadata = std.heap.ArenaAllocator.init(allocator);
        defer metadata.deinit();
        const merged = if (component_paths.len == 3) blk: {
            const signature = @import("../backends/imported_onnx_session.zig").inspectSignature(metadata.allocator(), component_paths[2]) catch |err| switch (err) {
                error.IncompatibleModel, error.UnsupportedDType => break :blk false,
                else => return err,
            };
            break :blk @import("../pipelines/seq2seq_decode.zig").qualifiedSignature(signature.inputs, signature.outputs);
        } else false;
        if (merged) {
            const selected = [_][]const u8{ component_paths[0], component_paths[2] };
            // Optional graph support must not constrain fallback placement.
            const loaded = blk: {
                var loader = host.plan_components_fn(host.context, model_dir, preferred_backends, &selected) catch |err| break :blk err;
                encoder = (if (control) |active| loader.loadWithControl(selected[0], active) else loader.load(selected[0])) catch |err| break :blk err;
                var strict = loader.restrictToBackend(encoder.?.session.backend()) catch |err| break :blk err;
                decoder = strict.loadWithImportedOnnxContextAndControl(selected[1], backends.imported_onnx_session.sharedBackendContext(encoder.?.session), control) catch |err| break :blk err;
                if (!@import("../pipelines/seq2seq_decode.zig").qualified(decoder.?.session)) break :blk error.IncompatibleModel;
                break :blk @as(anyerror!void, {});
            };
            if (loaded) |_| {} else |err| {
                switch (err) {
                    error.IncompatibleModel, error.UnsupportedArchitecture, error.UnsupportedOperation, error.UnsupportedOperator, error.NoCompatibleBackend, error.NoBackendAvailable, error.UnsupportedBackend => {},
                    else => return err,
                }
                if (decoder) |*candidate| candidate.deinit();
                decoder = null;
                if (encoder) |*candidate| candidate.deinit();
                encoder = null;
            }
        }
        if (decoder == null) {
            var loader = try host.plan_components_fn(host.context, model_dir, preferred_backends, component_paths[0..2]);
            encoder = if (control) |active| try loader.loadWithControl(component_paths[0], active) else try loader.load(component_paths[0]);
            var strict = try loader.restrictToBackend(encoder.?.session.backend());
            decoder = try strict.loadWithImportedOnnxContextAndControl(component_paths[1], backends.imported_onnx_session.sharedBackendContext(encoder.?.session), control);
        }
    }
    const verified_generation = try host.identity_fn(host.context, request);
    if (!std.mem.eql(u8, asset_generation[0..], verified_generation[0..]))
        return error.ModelArtifactsChanging;

    const assets = try allocator.create(CompositeAssets);
    errdefer allocator.destroy(assets);
    const owned_model_dir = try allocator.dupe(u8, model_dir);
    errdefer allocator.free(owned_model_dir);
    const owned_preferred_backends = try allocator.dupe(backends.BackendType, preferred_backends);
    errdefer allocator.free(owned_preferred_backends);
    assets.* = .{
        .runtime_id = runtime_id,
        .allocator = allocator,
        .model_dir = owned_model_dir,
        .model_dir_owned = true,
        .preferred_backends = owned_preferred_backends,
        .preferred_backends_owned = true,
        .model_type = model_type,
        .managed_tokenizer = managed_tokenizer.take(),
        .prompt_cache = prompt_cache,
        .encoder = encoder,
        .decoder = decoder,
        .kind = kind,
        .decoder_execution = if (decoder != null and @import("../pipelines/seq2seq_decode.zig").qualified(decoder.?.session)) .merged_kv else .full_prefix,
        .considered_merged_decoder = component_paths.len == 3,
        .decoder_config = decoder_config,
        .generation = asset_generation,
    };
    if (assets.encoder) |*managed| {
        managed.session.execution_gate = &assets.encoder_gate;
        const info = managed.session.outputInfo();
        if (info.len == 1 and info[0].shape.len == 3 and info[0].dtype == .f32) {
            // Exported hidden width is authoritative; the validated model
            // configuration supplies the bound when the export is symbolic.
            const width = if (info[0].shape[2] > 0) @as(usize, @intCast(info[0].shape[2])) else decoder_config.hidden_size orelse 0;
            if (width > 0) managed.session.output_geometry = .{ .input_name = if (kind == .whisper) "input_features" else "input_ids", .sequence_axis = if (kind == .whisper) 2 else 1, .sequence_divisor = if (kind == .whisper) 2 else 1, .width = width };
        }
    }
    if (assets.decoder) |*managed| {
        managed.session.execution_gate = &assets.decoder_gate;
        const info = managed.session.outputInfo();
        if (info.len == 1 and info[0].shape.len == 3 and info[0].dtype == .f32)
            managed.session.output_geometry = .{ .input_name = "input_ids", .width = if (info[0].shape[2] > 0) @intCast(info[0].shape[2]) else decoder_config.vocab_size };
    }
    prompt_cache = undefined;
    return assets;
}

test "model composite construction releases admitted peers on every allocation failure" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try temp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"t5\",\"d_model\":2,\"vocab_size\":4}" });
    const tokenizer_json = "{\"model\":{\"type\":\"WordPiece\",\"unk_token\":\"[UNK]\",\"vocab\":{\"[UNK]\":0,\"hello\":1}}}";
    try temp.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data = tokenizer_json });
    const directory = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..] });
    defer a.free(directory);
    const Probe = struct {
        allocator: std.mem.Allocator,
        admission: runtime.tier.memory.AdmissionController = .{},
        opened: usize = 0,
        closed: usize = 0,
        identity_changed: bool = false,
        cancel_decoder: bool = false,
        const key = @as([32]u8, @splat(7));
        fn ptr(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn close(raw: *anyopaque) void {
            ptr(raw).closed += 1;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "hidden", .dtype = .f32, .shape = &.{ 1, 2, 2 } }};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn loadSession(raw: *anyopaque, _: []const u8, preferred: []const backends.BackendType, _: ?*backends.imported_onnx_session.SharedBackendContext, control: ?InferenceExecutionControl) !ManagedSession {
            const self = ptr(raw);
            if (control) |active| try active.check();
            try std.testing.expectEqualSlices(backends.BackendType, &.{.native}, preferred);
            if (self.cancel_decoder and self.opened != 0) return error.Cancelled;
            const lease = try self.admission.tryAcquire(.cpu, .{}, .{ .host_weight_bytes = 16 }, false);
            self.opened += 1;
            return .{ .resource_lease = lease, .session = .{ .ptr = self, .vtable = &.{ .run = undefined, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close } } };
        }
        fn tokenizer(_: *anyopaque, _: []const u8) !ManagedHfTokenizer {
            // Tokenizer parsing has separate exhaustive allocation coverage.
            // Keep its global initialization outside the composite sweep.
            return .{ .tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(std.testing.allocator, tokenizer_json) };
        }
        fn plan(raw: *anyopaque, _: []const u8, _: []const backends.BackendType, paths: []const []const u8) !ComponentLoader {
            const self = ptr(raw);
            var result = ComponentLoader{ .services = .{ .allocator = self.allocator, .context = self, .load_session_fn = loadSession, .load_tokenizer_fn = tokenizer } };
            result.allowed_backends[0] = .native;
            result.allowed_backend_count = 1;
            for (paths) |path| try result.addComponentPath(path);
            return result;
        }
        fn identity(raw: *anyopaque, _: Request) !ComponentPlanKey {
            var result = key;
            if (ptr(raw).identity_changed) result[0] ^= 1;
            return result;
        }
        fn host(self: *@This()) Host {
            return .{ .context = self, .io = std.testing.io, .load_tokenizer_fn = tokenizer, .plan_components_fn = plan, .identity_fn = identity };
        }
        fn request(root: []const u8) Request {
            return .{ .model_dir = root, .component_paths = &.{ "encoder.onnx", "decoder.onnx" }, .preferred_backends = &.{.native}, .asset_generation = key, .runtime_id = 123, .model_type = .rewriter, .kind = .seq2seq };
        }
        fn run(allocator: std.mem.Allocator, root: []const u8) !void {
            var self = @This(){ .allocator = allocator };
            defer std.testing.expectEqual(self.opened, self.closed) catch @panic("composite session leaked");
            defer std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, self.admission.snapshot()) catch @panic("composite admission leaked");
            const result = try load(allocator, self.host(), request(root), null);
            defer allocator.destroy(result);
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), self.opened);
            try std.testing.expectEqual(@as(usize, 32), self.admission.snapshot().host_weight_bytes);
            try std.testing.expect(result.reclaimableAdmissions()[1] != null and result.reclaimableAdmissions()[2] != null);
            try std.testing.expectEqual(@as(u128, 123), result.runtime_id);
            try std.testing.expectEqual(&result.encoder_gate, result.encoder.?.session.execution_gate.?);
            try std.testing.expectEqual(&result.decoder_gate, result.decoder.?.session.execution_gate.?);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Probe.run, .{directory});
    var changed = Probe{ .allocator = a, .identity_changed = true };
    try std.testing.expectError(error.ModelArtifactsChanging, load(a, changed.host(), Probe.request(directory), null));
    try std.testing.expectEqual(@as(usize, 2), changed.closed);
    var cancelled = Probe{ .allocator = a, .cancel_decoder = true };
    try std.testing.expectError(error.Cancelled, load(a, cancelled.host(), Probe.request(directory), null));
    try std.testing.expectEqual(@as(usize, 1), cancelled.closed);
}
