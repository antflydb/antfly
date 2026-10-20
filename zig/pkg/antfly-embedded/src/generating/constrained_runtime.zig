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

pub const Runtime = struct {
    http: *httpx.Client,
    options: generating.BackendFactory.Options = .{},
    cache: *remote.Cache,
    semantic_validation: ?struct {
        ptr: *anyopaque,
        validate: *const fn (*anyopaque, Allocator, usize, Json, ?constrained.TokenEvidence) anyerror!void,
    } = null,

    pub fn capabilities(self: *Runtime, a: Allocator, cfg: *generating.GeneratorConfig) !constrained.Capabilities {
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
            var source = try secrets.SecretValue.initConfigOrEnv(a, cfg.api_key, "ANTFLY_INFERENCE_API_KEY");
            defer source.deinit(a);
            const key = try source.resolveOwned(a, self.options.secret_store);
            defer if (key) |value| a.free(value);
            var headers: [2][2][]const u8 = undefined;
            var count: usize = 0;
            var auth: ?[]u8 = null;
            defer if (auth) |value| a.free(value);
            if (key) |value| {
                auth = try std.fmt.allocPrint(a, "Bearer {s}", .{value});
                headers[count] = .{ "Authorization", auth.? };
                count += 1;
            }
            count = try self.options.execution.routing.appendHeaders(&headers, count);
            const lease = try self.cache.getOrDiscoverLeaseWithContext(self.http, cfg.url, cfg.model, .generate_batch, headers[0..count], self.options.execution.waitContext());
            native = lease.capabilities;
            if (lease.routing_token) |token| cfg.capability_token = try a.dupe(u8, token.slice());
            if (lease.descriptor_revision) |revision| cfg.capability_revision = try a.dupe(u8, revision.slice());
        }
        const caps = native orelse return error.UnsupportedConstrainedOutput;
        return .{ .grammar = caps.generation_grammar_v1, .token_logprobs = caps.generation_token_logprobs_v1, .model_revision = if (caps.model_revision) |revision| try std.fmt.allocPrint(a, "{x}", .{&revision}) else null };
    }

    pub fn execute(self: *Runtime, a: Allocator, cfg: generating.GeneratorConfig, mode: constrained.Output, schema: Json, instructions: []const u8, batch: constrained.Batch, inputs: []const []const u8) !constrained.Result {
        var backend = Backend{ .runtime = self, .cfg = cfg };
        return constrained.execute(a, .{ .ptr = &backend, .invoke = Backend.invoke, .checkpoint = Backend.checkpoint, .validate_item = Backend.validateItem }, mode, schema, instructions, batch, inputs);
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
        fn invoke(ptr: *anyopaque, a: Allocator, request: constrained.Invocation) !constrained.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try checkpoint(ptr);
            var cfg = self.cfg;
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
            const body = try std.json.Stringify.valueAlloc(a, .{ .instructions = request.instructions, .items = request.items, .validation_error = request.validation_error }, .{ .emit_null_optional_fields = false });
            const schema_text = try std.json.Stringify.valueAlloc(a, request.schema, .{});
            const system = try std.fmt.allocPrint(a, "Return one structured result per input item. Preserve the item IDs exactly. The JSON schema is: {s}", .{schema_text});
            var result = generating.executeChainWithOptions(a, self.runtime.http, &.{.{ .generator = cfg }}, .{
                .antfly_provider = self.runtime.options.antfly_provider,
                .secret_store = self.runtime.options.secret_store,
                .inference_api_key = self.runtime.options.inference_api_key,
                .max_response_bytes = self.runtime.options.max_response_bytes,
                .source_table = self.runtime.options.source_table,
                .execution = self.runtime.options.execution,
                .request_context = self.runtime.options.request_context,
                .limits = self.runtime.options.limits,
            }, &.{ .{ .role = .system, .content = .{ .text = system } }, .{ .role = .user, .content = .{ .text = body } } }) catch |err| switch (err) {
                error.InvalidStructuredOutput, error.InvalidGeneratedToolArguments => return .{ .value = try std.json.parseFromSliceLeaky(Json, a, "{\"items\":[]}", .{}) },
                else => return err,
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
