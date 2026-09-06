// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const bridge = @import("inference_bridge.zig");
const http = @import("../runtime_http_abi.zig");

pub const version: u32 = 3;
// Only options are JSON metadata. The application payload is carried raw.
pub const Request = struct { operation: Operation, options: []const u8 = "" };
pub const ResourceRequest = struct { operation: Operation, data: []const u8 };
pub const Envelope = struct { operation: Operation, options: []const u8 = "", data: []const u8 };
pub const Operation = enum { initialize, configure, provider, http, reserve, retain, release, prompt_cache, tokenizer_cache };
pub const Reply = struct { status: bridge.Status = .ok, options: []const u8 = "" };
pub const Event = struct {
    kind: enum { progress, stream_start, stream_write, stream_close },
    status: u16 = 200,
    phase: u8 = 0,
    completed: u64 = 0,
    total: u64 = 0,
    model: []const u8 = "",
    backend: []const u8 = "",
};

pub const WarmModel = struct {
    kind: []const u8,
    name: []const u8,
    backend: ?[]const u8,
    format: ?[]const u8,
    quantization: ?[]const u8,
    residency_mode: bridge.A4bResidencyMode,
    memory_budget_mb: u32,
};

pub const Create = struct {
    protocol_version: u32 = version,
    bridge_version: u32 = bridge.abi_version,
    data_dir: []const u8,
    models_dir: ?[]const u8,
    ml_dir: ?[]const u8,
    host_limit_bytes: usize,
    backend_limit_bytes: usize,
    combined_limit_bytes: usize,
    kv_limit_bytes: usize,
    scratch_limit_bytes: usize,
    process_memory_limit_bytes: usize,
    process_memory_limit_provenance: bridge.ProcessMemoryLimitProvenance,
    preload: []const WarmModel,
    keep_alive: ?[]const u8,
    max_loaded_models: i64,
    has_max_loaded_models: u8,
    content_security_json: ?[]const u8,
    s3_credentials_json: ?[]const u8,
    runtime_config_json: []const u8,

    pub fn fromContext(arena: std.mem.Allocator, context: *const bridge.CreateContext) !Create {
        const models = try arena.alloc(WarmModel, context.preload_len);
        const source = if (context.preload_ptr) |ptr| ptr[0..context.preload_len] else &.{};
        for (source, models) |model, *out| out.* = .{
            .kind = model.kind.slice(),
            .name = model.name.slice(),
            .backend = model.backend.slice(),
            .format = model.format.slice(),
            .quantization = model.quantization.slice(),
            .residency_mode = model.residency_mode,
            .memory_budget_mb = model.memory_budget_mb,
        };
        return .{
            .data_dir = context.data_dir_ptr[0..context.data_dir_len],
            .models_dir = context.models_dir.slice(),
            .ml_dir = context.ml_dir.slice(),
            .host_limit_bytes = context.host_limit_bytes,
            .backend_limit_bytes = context.backend_limit_bytes,
            .combined_limit_bytes = context.combined_limit_bytes,
            .kv_limit_bytes = context.kv_limit_bytes,
            .scratch_limit_bytes = context.scratch_limit_bytes,
            .process_memory_limit_bytes = context.process_memory_limit_bytes,
            .process_memory_limit_provenance = context.process_memory_limit_provenance,
            .preload = models,
            .keep_alive = context.keep_alive.slice(),
            .max_loaded_models = context.max_loaded_models,
            .has_max_loaded_models = context.has_max_loaded_models,
            .content_security_json = context.content_security_json.slice(),
            .s3_credentials_json = context.s3_credentials_json.slice(),
            .runtime_config_json = context.runtime_config_json.slice(),
        };
    }

    pub fn toContext(self: Create, arena: std.mem.Allocator, io: *const std.Io, out: *?*anyopaque) !bridge.CreateContext {
        if (self.protocol_version != version or self.bridge_version != bridge.abi_version) return error.UnsupportedVersion;
        const models = try arena.alloc(bridge.WarmModel, self.preload.len);
        for (self.preload, models) |model, *target| target.* = .{
            .kind = .init(model.kind),
            .name = .init(model.name),
            .backend = .init(model.backend),
            .format = .init(model.format),
            .quantization = .init(model.quantization),
            .residency_mode = model.residency_mode,
            .memory_budget_mb = model.memory_budget_mb,
        };
        return .{
            .abi_version = bridge.abi_version,
            .data_dir_ptr = self.data_dir.ptr,
            .data_dir_len = self.data_dir.len,
            .models_dir = .init(self.models_dir),
            .ml_dir = .init(self.ml_dir),
            .host_limit_bytes = self.host_limit_bytes,
            .backend_limit_bytes = self.backend_limit_bytes,
            .combined_limit_bytes = self.combined_limit_bytes,
            .kv_limit_bytes = self.kv_limit_bytes,
            .scratch_limit_bytes = self.scratch_limit_bytes,
            .process_memory_limit_bytes = self.process_memory_limit_bytes,
            .process_memory_limit_provenance = self.process_memory_limit_provenance,
            .preload_ptr = if (models.len == 0) null else models.ptr,
            .preload_len = models.len,
            .keep_alive = .init(self.keep_alive),
            .max_loaded_models = self.max_loaded_models,
            .has_max_loaded_models = self.has_max_loaded_models,
            .content_security_json = .init(self.content_security_json),
            .s3_credentials_json = .init(self.s3_credentials_json),
            .runtime_config_json = .init(self.runtime_config_json),
            .executor = .init(io),
            .out_handle = out,
        };
    }
};

pub const Provider = struct { operation: c_int, deadline_ns: ?u64 };
pub const Header = struct { name: []const u8, value: []const u8 };
pub const Http = struct {
    route: []const u8,
    method: http.HttpMethod,
    path: []const u8,
    query: ?[]const u8,
    headers: []const Header,
    params: []const Header,
    has_body: bool,
};
pub const HttpResponse = struct { status: u16, headers: []const Header };
pub const Reservation = struct { lease: usize = 0, amounts: bridge.AdmissionAmounts };
pub const Observation = struct { key: usize, previous: u64, next: u64 };

test "inference worker logical body limit matches the public HTTP contract" {
    try std.testing.expectEqual(@import("../api/public_limits.zig").max_request_body_bytes, @import("inference_worker_rpc.zig").max_body_bytes);
}
