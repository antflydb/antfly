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

//! Provider execution for constrained generators, shared by decisions and
//! asset producers. Provider/model capability facts and credentials stay here.
const std = @import("std");
const generating = @import("mod.zig");
const constrained = @import("antfly_structured");
const work = @import("antfly_inference_work");
const token_evidence = @import("antfly_structured").token_evidence;
const managed = @import("../inference/managed_embedder.zig");
const remote = @import("antfly_inference_remote_capabilities");
const secrets = @import("../common/secrets.zig");
const httpx = @import("httpx");
const Json = std.json.Value;
const Allocator = std.mem.Allocator;

/// Validate adapter-specific media constraints before any batch is dispatched.
pub fn validateEvidence(cfg: generating.GeneratorConfig, input: @import("../functions/decision_input.zig").Input) !void {
    try input.validate();
    if (input == .text) return;
    for (input.messages) |message| {
        const content = message.object.get("content").?;
        if (content != .array) continue;
        for (content.array.items) |part| {
            if (!std.mem.eql(u8, part.object.get("type").?.string, "input_image")) continue;
            const url = part.object.get("image_url").?.string;
            if (cfg.provider != .openai) {
                if (!std.mem.startsWith(u8, url, "data:")) return error.UnsupportedInferenceFeature;
                if (part.object.get("detail")) |detail| if (!std.mem.eql(u8, detail.string, "auto")) return error.UnsupportedInferenceFeature;
            }
        }
    }
}

pub const Runtime = struct {
    http: *httpx.Client,
    options: generating.BackendFactory.Options = .{},
    cache: *remote.Cache,
    limits: constrained.Limits = .{},
    capability_snapshot: ?constrained.Capabilities = null,
    http_control: ?httpx.socket.WriteControl = null,
    require_execution_lease: bool = false,
    semantic_validation: ?struct {
        ptr: *anyopaque,
        validate: *const fn (*anyopaque, Allocator, usize, Json, ?constrained.TokenEvidence) anyerror!void,
    } = null,

    pub fn capabilities(self: *Runtime, a: Allocator, cfg: *generating.GeneratorConfig) !constrained.Capabilities {
        const caps = try self.resolveCapabilities(a, cfg);
        self.capability_snapshot = caps;
        self.require_execution_lease = self.require_execution_lease or cfg.capability_token != null;
        return caps;
    }

    const RemoteHeaders = struct {
        entries: [2][2][]const u8 = undefined,
        count: usize = 0,
        authorization: ?[]u8 = null,
        fn deinit(self: RemoteHeaders, a: Allocator) void {
            if (self.authorization) |value| a.free(value);
        }
    };
    fn remoteHeaders(self: *Runtime, a: Allocator, cfg: generating.GeneratorConfig) !RemoteHeaders {
        var source = try secrets.SecretValue.initConfigOrEnv(a, cfg.api_key orelse (if (cfg.url.len == 0) self.options.inference_api_key else null), "ANTFLY_INFERENCE_API_KEY");
        defer source.deinit(a);
        const key = try source.resolveOwned(a, self.options.secret_store);
        defer if (key) |value| a.free(value);
        var headers = RemoteHeaders{};
        errdefer headers.deinit(a);
        if (key) |value| {
            headers.authorization = try std.fmt.allocPrint(a, "Bearer {s}", .{value});
            headers.entries[0] = .{ "Authorization", headers.authorization.? };
            headers.count = 1;
        }
        headers.count = try self.options.execution.routing.appendHeaders(&headers.entries, headers.count);
        return headers;
    }
    fn checkCancellation(ptr: *const anyopaque) !void {
        const self: *const Runtime = @ptrCast(@alignCast(ptr));
        try self.options.execution.cancellation.check();
        if (self.options.request_context) |context| try context.check();
    }
    fn cancellationRequested(ptr: *const anyopaque) bool {
        checkCancellation(ptr) catch return true;
        return false;
    }
    fn waitContext(self: *Runtime) remote.WaitContext {
        var context = self.options.execution.waitContext();
        context.execution_wait = self.http_control;
        if (self.options.request_context) |request| if (request.deadline_ns) |deadline| {
            context.deadline_ns = if (context.deadline_ns) |existing| @min(existing, deadline) else deadline;
        };
        context.cancellation = .{ .ptr = self, .is_cancelled_fn = cancellationRequested, .check_fn = checkCancellation };
        return context;
    }
    fn resolveCapabilities(self: *Runtime, a: Allocator, cfg: *generating.GeneratorConfig) !constrained.Capabilities {
        const execution = @import("antfly_inference_execution_context");
        var context = self.options.request_context orelse self.options.execution.requestContext(self.http.io);
        context.cancellation = .{ .ptr = self, .check_fn = checkCancellation, .is_cancelled_fn = cancellationRequested };
        var wait: execution.HttpWait = .{ .context = context };
        const previous = self.http_control;
        self.http_control = wait.control();
        defer self.http_control = previous;
        const result = resolveCapabilitiesInvoke(self, a, cfg) catch |err| {
            try wait.ready();
            return err;
        };
        try wait.ready();
        return result;
    }

    fn resolveCapabilitiesInvoke(self: *Runtime, a: Allocator, cfg: *generating.GeneratorConfig) !constrained.Capabilities {
        try cfg.validate();
        if (cfg.provider != .antfly) return hostedCapabilities(cfg.*);
        var native: ?work.InferenceCapabilities = null;
        if (cfg.url.len == 0) {
            const local = self.options.antfly_provider orelse return error.UnsupportedConstrainedOutput;
            if (local.generate_json == null) return error.UnsupportedConstrainedOutput;
            if (local.model_capabilities_with_context) |resolve| {
                const context = self.options.request_context orelse self.options.execution.requestContext(self.http.io);
                native = try managed.AntflyProviderBoundary.call("model_capabilities_with_context", local.boundary_dispatch, resolve, .{ local.ptr, a, cfg.model, work.Task.generate, context });
            } else if (local.model_capabilities) |resolve| {
                native = try managed.AntflyProviderBoundary.call("model_capabilities", local.boundary_dispatch, resolve, .{ local.ptr, a, cfg.model, work.Task.generate });
            }
        } else {
            const headers = try self.remoteHeaders(a, cfg.*);
            defer headers.deinit(a);
            // Constrained batches are scalar /generate requests, so their lease
            // must be scoped to that concrete operation as well.
            const lease = try self.cache.getOrDiscoverLeaseWithContext(self.http, cfg.url, cfg.model, .generate, headers.entries[0..headers.count], self.waitContext());
            native = lease.capabilities;
            cfg.capability_token = if (lease.routing_token) |token| try a.dupe(u8, token.slice()) else null;
            cfg.capability_revision = if (lease.descriptor_revision) |revision| try a.dupe(u8, revision.slice()) else null;
        }
        const caps = native orelse return error.UnsupportedConstrainedOutput;
        return .{ .limits = constrained.Limits.fromNativeBatch(caps.batch), .images = caps.input_modalities.image, .grammar = caps.generation_grammar_v1, .token_logprobs = caps.generation_token_logprobs_v1, .model_revision = if (caps.model_revision) |revision| try std.fmt.allocPrint(a, "{x}", .{&revision}) else null };
    }

    /// Refresh routing credentials before dispatch, while keeping the model
    /// contract used for packing, probability evidence and response validation.
    fn bindExecutionLease(self: *Runtime, a: Allocator, cfg: *generating.GeneratorConfig) !void {
        if (cfg.provider != .antfly or cfg.url.len == 0) return;
        const renewed = try self.resolveCapabilities(a, cfg);
        if (self.require_execution_lease and cfg.capability_token == null) return error.InferenceCapabilitiesStale;
        self.require_execution_lease = self.require_execution_lease or cfg.capability_token != null;
        if (self.capability_snapshot) |snapshot| {
            if (!snapshot.sameModelContract(renewed)) return error.InferenceCapabilitiesStale;
        } else self.capability_snapshot = renewed;
    }
    fn invalidateExecutionLease(self: *Runtime, a: Allocator, cfg: generating.GeneratorConfig) !void {
        const headers = try self.remoteHeaders(a, cfg);
        defer headers.deinit(a);
        try self.cache.invalidateLease(cfg.url, cfg.model, .generate, headers.entries[0..headers.count], try remote.LeaseIdentity.init(cfg.capability_token, cfg.capability_revision));
    }

    pub fn execute(self: *Runtime, a: Allocator, cfg: generating.GeneratorConfig, mode: constrained.Output, schema: Json, instructions: []const u8, batch: constrained.Batch, inputs: []const []const u8) !constrained.Result {
        var backend = Backend{ .runtime = self, .cfg = cfg };
        return constrained.execute(a, .{ .ptr = &backend, .invoke = Backend.invoke, .checkpoint = Backend.checkpoint, .validate_item = Backend.validateItem }, mode, schema, instructions, batch, inputs);
    }

    pub fn executeEvidence(self: *Runtime, a: Allocator, cfg: generating.GeneratorConfig, mode: constrained.Output, schema: Json, instructions: []const u8, batch: constrained.Batch, inputs: []const Json) !constrained.Result {
        var backend = Backend{ .runtime = self, .cfg = cfg };
        return constrained.executeEvidence(a, .{ .limits = self.limits, .ptr = &backend, .invoke = Backend.invoke, .checkpoint = Backend.checkpoint, .validate_item = Backend.validateItem }, mode, schema, instructions, batch, inputs);
    }

    const Backend = struct {
        runtime: *Runtime,
        cfg: generating.GeneratorConfig,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.runtime.options.request_context) |context| try context.check();
        }
        fn validateItem(ptr: *anyopaque, a: Allocator, id: usize, value: Json, proof: ?constrained.TokenEvidence) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.runtime.semantic_validation) |validation| try validation.validate(validation.ptr, a, id, value, proof);
        }
        fn recordUsage(ptr: *anyopaque, input: u64, output: u64) void {
            const usage: *constrained.Usage = @ptrCast(@alignCast(ptr));
            usage.record(input, output);
        }
        fn invoke(ptr: *anyopaque, a: Allocator, request: constrained.Invocation) !constrained.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try checkpoint(ptr);
            var cfg = self.cfg;
            try self.runtime.bindExecutionLease(a, &cfg);
            const wire_schema = try constrained.providerSchema(a, cfg, request.mode, request.schema);
            // Invocation-owned options must never mutate the registered spec.
            cfg.tools_json = null;
            cfg.tool_choice_json = null;
            cfg.response_format_json = null;
            if (request.mode == .tool_call) {
                cfg.tools_json = try std.json.Stringify.valueAlloc(a, .{.{ .type = "function", .function = .{
                    .name = "antfly_output",
                    .description = "Return the requested typed results.",
                    .strict = true,
                    .parameters = wire_schema,
                } }}, .{});
                cfg.tool_choice_json = "{\"type\":\"function\",\"function\":{\"name\":\"antfly_output\"}}";
            } else {
                cfg.response_format_json = try std.json.Stringify.valueAlloc(a, .{ .type = "json_schema", .json_schema = .{
                    .name = "antfly_output",
                    .strict = true,
                    .schema = wire_schema,
                } }, .{});
            }
            const schema_text = try std.json.Stringify.valueAlloc(a, request.schema, .{});
            const system = try std.fmt.allocPrint(a, "Return one structured result per input item. Preserve the item IDs exactly. The JSON schema is: {s}", .{schema_text});
            const messages = try constrained.evidenceMessages(a, request, system, cfg.provider == .openai);
            var refreshed = false;
            var result = while (true) {
                const generated = generating.executeChainWithOptions(a, self.runtime.http, &.{.{ .generator = cfg }}, .{
                    .antfly_provider = self.runtime.options.antfly_provider,
                    .provider_runtime = self.runtime.options.provider_runtime,
                    .secret_store = self.runtime.options.secret_store,
                    .inference_api_key = self.runtime.options.inference_api_key,
                    .max_response_bytes = self.runtime.options.max_response_bytes,
                    .source_table = self.runtime.options.source_table,
                    .execution = self.runtime.options.execution,
                    .request_context = self.runtime.options.request_context,
                    .limits = self.runtime.options.limits,
                    .usage_observer = if (request.usage) |usage| .{ .ptr = usage, .record_fn = recordUsage } else null,
                }, messages) catch |err| {
                    if (err == error.InferenceCapabilitiesStale and cfg.provider == .antfly and cfg.url.len > 0) {
                        // An authoritative rejection happened before inference.
                        // Revoke even on the final attempt; never resurrect its
                        // stale tuple if rediscovery encounters an outage.
                        try self.runtime.invalidateExecutionLease(a, cfg);
                        if (refreshed) return err;
                        try checkpoint(ptr);
                        try self.runtime.bindExecutionLease(a, &cfg);
                        refreshed = true;
                        continue;
                    }
                    switch (err) {
                        error.InvalidStructuredOutput, error.InvalidGeneratedToolArguments => return .{ .value = try std.json.parseFromSliceLeaky(Json, a, "{\"items\":[]}", .{}) },
                        else => return err,
                    }
                };
                break generated;
            };
            defer result.deinit();
            const raw = if (request.mode == .tool_call) blk: {
                if (result.tool_calls.len != 1 or result.tool_calls_synthesized or !std.mem.eql(u8, result.tool_calls[0].name, "antfly_output"))
                    return emptyReply(a, result.usage);
                break :blk result.tool_calls[0].arguments;
            } else result.content;
            const value = constrained.parseReply(a, raw) catch |err| {
                if (err == error.OutOfMemory) return err;
                return emptyReply(a, result.usage);
            };
            return .{ .value = value, .input_tokens = if (result.usage) |usage| usage.input_tokens else 0, .output_tokens = if (result.usage) |usage| usage.output_tokens else 0, .token_evidence = if (result.token_logprobs_json) |raw_evidence| blk: {
                const parsed_evidence = std.json.parseFromSliceLeaky(Json, a, raw_evidence, .{ .allocate = .alloc_always }) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    return emptyReply(a, result.usage);
                };
                const index = token_evidence.Index.init(a, raw, parsed_evidence) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    return emptyReply(a, result.usage);
                };
                break :blk index.evidence();
            } else null };
        }
    };
};
fn emptyReply(a: Allocator, usage: anytype) !constrained.Reply {
    return .{ .value = try std.json.parseFromSliceLeaky(Json, a, "{\"items\":[]}", .{}), .input_tokens = if (usage) |value| value.input_tokens else 0, .output_tokens = if (usage) |value| value.output_tokens else 0 };
}

pub const hostedCapabilities = constrained.hostedCapabilities;
