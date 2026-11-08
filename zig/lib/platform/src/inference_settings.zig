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

/// Public execution settings shared by the DB and both inference executables.
/// Backend scheduling methods live in inference; this type owns the wire shape.
pub const GenerationBatching = struct {
    mode: enum { off, auto, on } = .off,
    max_step_items: usize = 2,
    max_step_query_tokens: usize = 512,
    max_decode_wait_us: u32 = 1000,
    max_idle_prefill_chunk_size: usize = 2048,
};

/// Interleave independently owned requests. Row batching is a separate opt-in.
pub const CooperativeExecution = struct {
    mode: enum { off, on } = .off,
    max_active_requests: usize = 2,
    short_context_burst: usize = 8,
    decode_handoff_wait_us: u32 = 5000,
};

pub const MemorillaProfile = struct {
    name: []const u8,
    encoder: []const u8,
    decoder: []const u8,
    memory_module_path: []const u8,
    max_documents: usize = 64,
    max_new_tokens: usize = 128,
};

/// Named assets are operator policy; requests select a name and never supply
/// filesystem paths or substitute a component of the admitted composition.
pub const Memorilla = struct {
    enabled: bool = false,
    profiles: []const MemorillaProfile = &.{},

    pub fn validate(self: Memorilla) !void {
        if (self.profiles.len > 16 or (self.enabled and self.profiles.len == 0)) return error.InvalidMemorillaConfig;
        for (self.profiles, 0..) |profile, i| {
            if (profile.name.len == 0 or profile.name.len > 128 or profile.encoder.len == 0 or
                profile.encoder.len > 4096 or profile.decoder.len == 0 or profile.decoder.len > 4096 or
                profile.memory_module_path.len == 0 or profile.memory_module_path.len > 4096 or
                profile.max_documents == 0 or profile.max_documents > 64 or profile.max_new_tokens == 0 or profile.max_new_tokens > 128)
                return error.InvalidMemorillaConfig;
            inline for (.{ "name", "encoder", "decoder", "memory_module_path" }) |field| {
                const text = @field(profile, field);
                _ = std.unicode.Utf8View.init(text) catch return error.InvalidMemorillaConfig;
                if (std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidMemorillaConfig;
            }
            for (self.profiles[0..i]) |previous| if (std.mem.eql(u8, previous.name, profile.name)) return error.DuplicateMemorillaProfile;
        }
    }

    pub fn find(self: Memorilla, name: []const u8) !MemorillaProfile {
        if (!self.enabled) return error.MemorillaDisabled;
        for (self.profiles) |profile| if (std.mem.eql(u8, profile.name, name)) return profile;
        return error.MemorillaProfileNotFound;
    }

    pub fn clone(self: Memorilla, a: std.mem.Allocator) !Memorilla {
        try self.validate();
        const profiles = try a.alloc(MemorillaProfile, self.profiles.len);
        var initialized: usize = 0;
        errdefer {
            for (profiles[0..initialized]) |profile| freeProfile(a, profile);
            a.free(profiles);
        }
        for (self.profiles, profiles) |source, *dest| {
            const name = try a.dupe(u8, source.name);
            errdefer a.free(name);
            const encoder = try a.dupe(u8, source.encoder);
            errdefer a.free(encoder);
            const decoder = try a.dupe(u8, source.decoder);
            errdefer a.free(decoder);
            const path = try a.dupe(u8, source.memory_module_path);
            dest.* = .{ .name = name, .encoder = encoder, .decoder = decoder, .memory_module_path = path, .max_documents = source.max_documents, .max_new_tokens = source.max_new_tokens };
            initialized += 1;
        }
        return .{ .enabled = self.enabled, .profiles = profiles };
    }

    fn freeProfile(a: std.mem.Allocator, profile: MemorillaProfile) void {
        a.free(profile.name);
        a.free(profile.encoder);
        a.free(profile.decoder);
        a.free(profile.memory_module_path);
    }

    /// Only call on an owned clone; Node configuration borrows its source.
    pub fn deinit(self: *Memorilla, a: std.mem.Allocator) void {
        for (self.profiles) |profile| freeProfile(a, profile);
        if (self.profiles.len > 0) a.free(self.profiles);
        self.* = .{};
    }
};

test "Memorilla settings bind named compositions and reject ambiguous or oversized profiles" {
    const a = std.testing.allocator;
    const profile = MemorillaProfile{ .name = "stage3", .encoder = "encoder", .decoder = "decoder", .memory_module_path = "/models/stage3" };
    const config = Memorilla{ .enabled = true, .profiles = &.{profile} };
    try config.validate();
    var owned = try config.clone(a);
    defer owned.deinit(a);
    try std.testing.expectEqualStrings("encoder", (try owned.find("stage3")).encoder);
    try std.testing.expectError(error.MemorillaDisabled, (Memorilla{}).find("stage3"));
    try std.testing.expectError(error.MemorillaProfileNotFound, config.find("missing"));
    try std.testing.expectError(error.DuplicateMemorillaProfile, (Memorilla{ .enabled = true, .profiles = &.{ profile, profile } }).validate());
    var oversized = profile;
    oversized.max_documents = 65;
    try std.testing.expectError(error.InvalidMemorillaConfig, (Memorilla{ .profiles = &.{oversized} }).validate());
}

test "Memorilla owned profile copies unwind every allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const config = Memorilla{ .enabled = true, .profiles = &.{
                .{ .name = "first", .encoder = "encoder-a", .decoder = "decoder-a", .memory_module_path = "/models/first" },
                .{ .name = "second", .encoder = "encoder-b", .decoder = "decoder-b", .memory_module_path = "/models/second" },
            } };
            var owned = try config.clone(a);
            defer owned.deinit(a);
            try std.testing.expect(owned.profiles.ptr != config.profiles.ptr);
            try std.testing.expect(owned.profiles[1].encoder.ptr != config.profiles[1].encoder.ptr);
            try std.testing.expectEqualStrings("decoder-b", (try owned.find("second")).decoder);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

pub fn parseKeepAliveMs(raw: []const u8) !u64 {
    if (std.mem.eql(u8, raw, "0")) return 0;
    if (raw.len == 0) return error.InvalidInferenceModelCacheConfig;
    var i: usize = 0;
    var total_ns: u64 = 0;
    while (i < raw.len) {
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        if (i == start) return error.InvalidInferenceModelCacheConfig;
        const value = std.fmt.parseUnsigned(u64, raw[start..i], 10) catch
            return error.InvalidInferenceModelCacheConfig;
        const unit_ns: u64 = if (std.mem.startsWith(u8, raw[i..], "ms")) blk: {
            i += 2;
            break :blk std.time.ns_per_ms;
        } else if (i < raw.len and raw[i] == 's') blk: {
            i += 1;
            break :blk std.time.ns_per_s;
        } else if (i < raw.len and raw[i] == 'm') blk: {
            i += 1;
            break :blk std.time.ns_per_min;
        } else if (i < raw.len and raw[i] == 'h') blk: {
            i += 1;
            break :blk std.time.ns_per_hour;
        } else return error.InvalidInferenceModelCacheConfig;
        const part_ns = std.math.mul(u64, value, unit_ns) catch
            return error.InvalidInferenceModelCacheConfig;
        total_ns = std.math.add(u64, total_ns, part_ns) catch
            return error.InvalidInferenceModelCacheConfig;
    }
    if (total_ns == 0) return 0;
    return @max(@as(u64, 1), total_ns / std.time.ns_per_ms);
}

test "canonical inference lifetime uses duration strings" {
    try std.testing.expectEqual(@as(u64, 0), try parseKeepAliveMs("0"));
    try std.testing.expectEqual(@as(u64, 90000), try parseKeepAliveMs("1m30s"));
    try std.testing.expectError(error.InvalidInferenceModelCacheConfig, parseKeepAliveMs("5000"));
}

/// Replacement paths for config keys removed by the canonical resource contract.
pub fn replacementPath(key: []const u8, nested: bool) ?[]const u8 {
    if (std.mem.eql(u8, key, "keep_alive_ms")) return "inference.keep_alive (duration string, e.g. 5s)";
    if (std.mem.eql(u8, key, "admission")) return "resources.<workload>.admission";
    if (std.mem.eql(u8, key, "request_limits")) return "resources.inference.request_limits";
    if (std.mem.eql(u8, key, "memory")) return "resources.inference.memory (byte units)";
    if (std.mem.eql(u8, key, "max_concurrent_requests")) return "resources.inference.admission.max_concurrent_requests";
    if (nested) return null;
    if (std.mem.eql(u8, key, "graph_execution")) return "resources.query.request_limits.graph";
    if (std.mem.eql(u8, key, "process_memory_budget_mb")) return "resources.memory.max_bytes (byte units)";
    inline for (.{ "models_dir", "ml_dir", "preload", "max_loaded_models", "keep_alive", "prompt_cache", "kernel_jit", "content_security", "s3_credentials", "pool_size", "generation_batching", "cooperative_execution", "http_max_body_mb", "memorilla" }) |model_key| {
        if (std.mem.eql(u8, key, model_key)) return "inference." ++ model_key;
    }
    return null;
}

pub fn validateCanonicalLocations(root: std.json.ObjectMap) !void {
    var fields = root.iterator();
    while (fields.next()) |field| {
        if (replacementPath(field.key_ptr.*, false) != null) return error.InvalidInferenceConfig;
    }
    if (root.get("inference")) |value| {
        if (value != .object) return error.InvalidInferenceConfig;
        var nested = value.object.iterator();
        while (nested.next()) |field| {
            if (replacementPath(field.key_ptr.*, true) != null) return error.InvalidInferenceConfig;
        }
    }
}

pub fn diagnoseInvalidConfig(allocator: std.mem.Allocator, raw: []const u8) void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, raw, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;
    var fields = parsed.value.object.iterator();
    while (fields.next()) |field| {
        if (replacementPath(field.key_ptr.*, false)) |replacement|
            std.log.err("unsupported config field '{s}'; use '{s}'", .{ field.key_ptr.*, replacement });
    }
    if (parsed.value.object.get("inference")) |value| {
        if (value != .object) return;
        var nested = value.object.iterator();
        while (nested.next()) |field| {
            if (replacementPath(field.key_ptr.*, true)) |replacement|
                std.log.err("unsupported config field 'inference.{s}'; use '{s}'", .{ field.key_ptr.*, replacement });
        }
    }
}

test "removed inference config fields have actionable replacements" {
    try std.testing.expectEqualStrings("resources.query.request_limits.graph", replacementPath("graph_execution", false).?);
    try std.testing.expectEqualStrings("inference.models_dir", replacementPath("models_dir", false).?);
    try std.testing.expect(replacementPath("keep_alive_ms", true) != null);
    try std.testing.expect(replacementPath("models_dir", true) == null);
}
