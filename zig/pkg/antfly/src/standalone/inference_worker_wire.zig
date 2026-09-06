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

pub const version: u32 = 1;
pub const Envelope = struct { operation: Operation, data: []const u8 };
pub const Operation = enum { initialize, configure, provider, http, reserve, retain, release, prompt_cache, tokenizer_cache };
pub const Reply = struct { status: bridge.Status = .ok, data: []const u8 = "" };
pub const Event = struct {
    kind: enum { progress, stream_start, stream_write, stream_close },
    status: u16 = 200,
    data: []const u8 = "",
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

pub const Provider = struct { operation: c_int, request_json: []const u8, deadline_ns: ?u64 };
pub const Header = struct { name: []const u8, value: []const u8 };
pub const Http = struct {
    route: []const u8,
    method: http.HttpMethod,
    path: []const u8,
    query: ?[]const u8,
    headers: []const Header,
    params: []const Header,
    body_b64: ?[]const u8,
};
pub const HttpResponse = struct { status: u16, headers: []const Header, body_b64: []const u8 };
pub const Reservation = struct { lease: usize = 0, amounts: bridge.AdmissionAmounts };
pub const Observation = struct { key: usize, previous: u64, next: u64 };

pub fn encodeBytes(alloc: std.mem.Allocator, value: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(value.len));
    _ = std.base64.standard.Encoder.encode(out, value);
    return out;
}

pub fn decodeBytes(alloc: std.mem.Allocator, value: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(value));
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, value);
    return out;
}

test "inference worker wire preserves binary HTTP and SSE bytes" {
    const alloc = std.testing.allocator;
    const original = "\x00\xff\xfe\r\n\"multimodal\"";
    const encoded = try encodeBytes(alloc, original);
    defer alloc.free(encoded);
    const json = try std.json.Stringify.valueAlloc(alloc, Event{ .kind = .stream_write, .data = encoded }, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(Event, alloc, json, .{});
    defer parsed.deinit();
    const decoded = try decodeBytes(alloc, parsed.value.data);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(original, decoded);
    try std.testing.expectError(error.InvalidCharacter, decodeBytes(alloc, "!!!!"));
}
