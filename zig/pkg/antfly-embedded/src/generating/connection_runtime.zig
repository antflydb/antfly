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
const httpx = @import("httpx");
const generating = @import("antfly_generating");
const inference_config = @import("../common/config.zig");
const telemetry = @import("../common/inference_telemetry.zig");
const secrets = @import("../common/secrets.zig");

const Allocator = std.mem.Allocator;

pub const Task = enum {
    generate,
    extract,
    decide,
    transcribe,
    read,

    fn capability(self: Task) []const u8 {
        return switch (self) {
            .generate => "models.generate",
            .extract => "models.extract",
            .decide => "models.decide",
            .transcribe => "models.transcribe",
            .read => "models.read",
        };
    }
};

pub const ResolvedInferenceConnection = struct {
    reference: inference_config.InferenceModelReference,
    config: inference_config.Config.InferenceConnectionConfig,
};

var request_sequence: std.atomic.Value(u64) = .init(0);

pub fn resolveGenerator(
    available: ?*const inference_config.Config.ConnectionsConfig,
    arena: Allocator,
    generator: *generating.GeneratorConfig,
    task: Task,
) !?ResolvedInferenceConnection {
    const reference = (try inference_config.InferenceModelReference.parse(generator.model)) orelse return null;
    const connections = available orelse return error.ConnectionNotFound;
    const connection = try inference_config.resolveInferenceConnection(connections, reference.connection, task.capability());
    switch (generator.provider) {
        .openai, .anthropic, .cloudflare, .gemini, .vertex, .bedrock => {},
        .openrouter, .ollama => if (task != .generate) return error.UnsupportedInferenceFeature,
        .antfly => {
            if (task != .generate) return error.UnsupportedInferenceFeature;
            if (connection.url == null) return error.ConnectionURLMissing;
        },
        else => return error.UnsupportedInferenceFeature,
    }
    if (!std.mem.eql(u8, connection.provider, @tagName(generator.provider))) return error.ModelTaskMismatch;
    if (generator.url.len != 0 or generator.api_key != null or generator.workspace_id != null or
        generator.project_id != null or generator.location != null or generator.region != null or generator.credentials_path != null or
        generator.account_id != null or generator.gateway_id != null or generator.connection_id != null)
        return error.InvalidGeneratedExtractorConfig;
    // The provider factories can otherwise pick up a process-wide
    // credential. A private or alternate origin needs an explicit
    // credential reference from the operator's connection.
    switch (generator.provider) {
        .openai, .anthropic, .openrouter, .gemini, .cloudflare => if (connection.api_key == null) {
            if (connection.url) |url| {
                const standard = generating.defaultUrl(generator.provider);
                if (!std.mem.eql(u8, std.mem.trimEnd(u8, url, "/"), standard)) return error.MissingGeneratorCredentials;
            }
        },
        else => {},
    }
    generator.model = reference.model;
    generator.region = connection.region;
    generator.url = connection.url orelse switch (generator.provider) {
        .cloudflare => try generating.cloudflare.baseUrl(arena, connection.account_id, null),
        .bedrock => try @import("antfly_inference_provider_defaults").bedrockRuntimeEndpointAlloc(arena, "", connection.region orelse @import("antfly_platform").env.getenv("AWS_REGION") orelse @import("antfly_platform").env.getenv("AWS_DEFAULT_REGION") orelse ""),
        else => generating.defaultUrl(generator.provider),
    };
    generator.api_key = connection.api_key;
    generator.workspace_id = connection.workspace_id;
    generator.account_id = connection.account_id;
    generator.gateway_id = connection.gateway_id;
    generator.project_id = connection.project_id;
    generator.location = connection.location;
    generator.credentials_path = connection.credentials_path;
    return .{ .reference = reference, .config = connection };
}

pub const ClientOptions = struct {
    principal: ?[]const u8 = null,
    metadata_json: []const u8 = "",
    workload: ?inference_config.InferenceAttributionIdentity.Workload = null,
    secret_store: ?*secrets.FileStore = null,
    max_response_bytes: usize = 0,
};

pub fn client(arena: Allocator, alloc: Allocator, io: std.Io, resolved: ResolvedInferenceConnection, effective_url: []const u8, task: []const u8, options: ClientOptions) !httpx.Client {
    const configured = resolved.config;
    const scope = if (configured.attribution != null and options.principal != null)
        try inference_config.inferenceScopeFromAuthenticatedMetadata(arena, options.metadata_json)
    else
        null;
    const identity: ?inference_config.InferenceAttributionIdentity = if (options.principal != null)
        inference_config.authenticatedInferenceAttribution(configured.attribution, options.principal, scope)
    else if (options.workload) |workload|
        .{ .workload = workload }
    else
        null;
    const attempts = try arena.create(telemetry.ProviderAttempts);
    attempts.* = .{
        .request_id = try std.fmt.allocPrint(arena, "internal-{x}-{x}", .{ @import("antfly_platform").time.monotonicNs(), request_sequence.fetchAdd(1, .monotonic) }),
        .connection = resolved.reference.connection,
        .task = task,
        .model = resolved.reference.model,
        .billing_principal = if (configured.attribution != null) options.principal else null,
        .billing_scope = inference_config.InferenceAttributionIdentity.billingScope(identity),
        .billing_workload = if (configured.attribution != null and options.principal == null)
            if (options.workload) |workload| workload.physical_table else null
        else
            null,
    };
    var transport: httpx.ClientConfig = .{ .keep_alive = false, .attempt_result_observer = attempts.observer() };
    if (options.max_response_bytes != 0) transport.max_response_size = options.max_response_bytes;
    return httpx.Client.initWithConfig(alloc, io, try configured.clientConfigForIdentityWithDefaultURL(arena, options.secret_store, transport, identity, effective_url));
}
