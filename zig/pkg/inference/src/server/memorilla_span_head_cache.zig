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

//! One verified span head (`span-head.safetensors` of a hybrid span-window
//! bundle) per loaded decoder, so a request no longer re-reads, re-hashes and
//! re-parses 130 MB. The same discipline as `memorilla_reader_cache`: the
//! sha256 pin in `manifest.json` is checked on every miss; a hit is fenced by
//! the stat identity of the bundle directory, `manifest.json` and the weights
//! (inode, size, mtime, ctime), so replacement or mutation retires the entry.
//! The weights stay admitted (host) for the entry's lifetime; no manager
//! admission or eviction happens under the cache mutex.
const std = @import("std");
const span = @import("../pipelines/memorilla_span.zig");
const encoder_lora = @import("../models/memorilla_encoder_lora.zig");
const bundle = @import("../models/memorilla.zig");
const reader_cache = @import("memorilla_reader_cache.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;

pub const identity_files = [_][]const u8{ "manifest.json", span.weights_file_name };

/// Head geometry the cached entry was parsed for (a protocol with another
/// geometry is a different head).
const Geometry = struct {
    width: usize,
    hidden: usize,
    layers: usize,
    heads: usize,
    max_positions: usize,
    max_documents: usize,

    fn of(protocol: span.Protocol) Geometry {
        return .{ .width = protocol.width, .hidden = protocol.hidden, .layers = protocol.layers, .heads = protocol.heads, .max_positions = protocol.max_positions, .max_documents = protocol.max_documents };
    }
};

pub const Cache = struct {
    const Entry = struct { head: span.Head, identity: reader_cache.Identity, geometry: Geometry, lease: memory.AdmissionLease };

    mutex: std.atomic.Mutex = .unlocked,
    entry: ?Entry = null,

    /// Holds the cache mutex: the head is read-only while borrowed.
    pub const Handle = struct {
        cache: *Cache,

        pub fn head(self: *const Handle) *const span.Head {
            return &self.cache.entry.?.head;
        }

        pub fn deinit(self: *Handle) void {
            self.cache.mutex.unlock();
            self.* = undefined;
        }
    };

    fn retire(self: *Cache) void {
        if (self.entry) |*entry| {
            entry.head.deinit();
            entry.lease.release();
            self.entry = null;
        }
    }

    /// Called only by the decoder's final owner, after all borrows drain.
    pub fn deinit(self: *Cache) void {
        self.retire();
        self.* = undefined;
    }

    /// Under the mutex: the observed identity, with a stale entry retired.
    /// True when the entry is a hit.
    fn inspect(self: *Cache, a: A, io: std.Io, directory: []const u8, geometry: Geometry, control: Control, observed: *reader_cache.Identity) !bool {
        observed.* = reader_cache.identity(a, io, directory, &identity_files, control) catch |err| {
            self.retire();
            return err;
        };
        if (self.entry) |*entry| {
            if (std.meta.eql(entry.identity, observed.*) and std.meta.eql(entry.geometry, geometry)) return true;
            self.retire();
        }
        return false;
    }

    /// Borrow the bundle's span head for `protocol`, loading and verifying it
    /// on a miss into `allocator` (the decoder's long-lived allocator).
    pub fn acquire(self: *Cache, allocator: A, a: A, io: std.Io, manager: anytype, directory: []const u8, protocol: span.Protocol, limits: memory.Limits, control: Control) !Handle {
        const geometry = Geometry.of(protocol);
        var first: reader_cache.Identity = undefined;
        try control.lock(&self.mutex);
        const hit = self.inspect(a, io, directory, geometry, control, &first) catch |err| {
            self.mutex.unlock();
            return err;
        };
        if (hit) return .{ .cache = self };
        self.mutex.unlock();
        var lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_weight_bytes = span.Head.weightBytes(protocol) });
        var transferred = false;
        defer if (!transferred) lease.release();
        try control.lock(&self.mutex);
        errdefer self.mutex.unlock();
        var before: reader_cache.Identity = undefined;
        // A racing miss may have loaded the same head: reuse it and refund.
        if (try self.inspect(a, io, directory, geometry, control, &before)) return .{ .cache = self };
        if (!std.meta.eql(first, before)) return error.ModelArtifactsChanging;
        var loaded = try span.Head.loadBundle(allocator, directory, protocol);
        errdefer loaded.deinit();
        const after = try reader_cache.identity(a, io, directory, &identity_files, control);
        if (!std.meta.eql(before, after)) return error.ModelArtifactsChanging;
        self.entry = .{ .head = loaded, .identity = after, .geometry = geometry, .lease = lease };
        transferred = true;
        return .{ .cache = self };
    }
};

/// The bundle's run-time encoder LoRA (`encoder-lora.safetensors`, pinned with
/// `candidate.json` and `protocol.json`), cached per encoder with the same
/// fence: full verification on a miss, stat identity on a hit.
pub const adapter_identity_files = [_][]const u8{ "manifest.json", encoder_lora.file_name, bundle.research_candidate_file, bundle.research_protocol_file };

pub const AdapterCache = struct {
    const Entry = struct { adapter: encoder_lora.Adapter, identity: reader_cache.Identity, geometry: encoder_lora.Geometry, lease: memory.AdmissionLease };

    mutex: std.atomic.Mutex = .unlocked,
    entry: ?Entry = null,

    /// Holds the cache mutex: the adapter is read-only while borrowed.
    pub const Handle = struct {
        cache: *AdapterCache,

        pub fn adapter(self: *const Handle) *const encoder_lora.Adapter {
            return &self.cache.entry.?.adapter;
        }

        pub fn deinit(self: *Handle) void {
            self.cache.mutex.unlock();
            self.* = undefined;
        }
    };

    fn retire(self: *AdapterCache) void {
        if (self.entry) |*entry| {
            entry.adapter.deinit();
            entry.lease.release();
            self.entry = null;
        }
    }

    pub fn deinit(self: *AdapterCache) void {
        self.retire();
        self.* = undefined;
    }

    fn observe(self: *AdapterCache, a: A, io: std.Io, directory: []const u8, geometry: encoder_lora.Geometry, control: Control, observed: *reader_cache.Identity) !bool {
        observed.* = reader_cache.identity(a, io, directory, &adapter_identity_files, control) catch |err| {
            self.retire();
            return err;
        };
        if (self.entry) |*entry| {
            if (std.meta.eql(entry.identity, observed.*) and std.meta.eql(entry.geometry, geometry)) return true;
            self.retire();
        }
        return false;
    }

    /// Borrow the bundle's adapter, loading and verifying it on a miss into
    /// `allocator` (the encoder's long-lived allocator). The load and the
    /// admission happen outside the mutex.
    pub fn acquire(self: *AdapterCache, allocator: A, a: A, io: std.Io, manager: anytype, directory: []const u8, geometry: encoder_lora.Geometry, limits: memory.Limits, control: Control) !Handle {
        var first: reader_cache.Identity = undefined;
        try control.lock(&self.mutex);
        const hit = self.observe(a, io, directory, geometry, control, &first) catch |err| {
            self.mutex.unlock();
            return err;
        };
        if (hit) return .{ .cache = self };
        self.mutex.unlock();
        var loaded = try encoder_lora.loadBundle(allocator, directory, geometry);
        var owns_loaded = true;
        defer if (owns_loaded) loaded.deinit();
        const after = try reader_cache.identity(a, io, directory, &adapter_identity_files, control);
        if (!std.meta.eql(first, after)) return error.ModelArtifactsChanging;
        var lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_weight_bytes = loaded.bytes() });
        var owns_lease = true;
        defer if (owns_lease) lease.release();
        try control.lock(&self.mutex);
        errdefer self.mutex.unlock();
        var now: reader_cache.Identity = undefined;
        // A racing miss may have installed the same adapter: reuse it.
        if (try self.observe(a, io, directory, geometry, control, &now)) return .{ .cache = self };
        if (!std.meta.eql(now, after)) return error.ModelArtifactsChanging;
        self.entry = .{ .adapter = loaded, .identity = now, .geometry = geometry, .lease = lease };
        owns_loaded = false;
        owns_lease = false;
        return .{ .cache = self };
    }
};

const TestManager = struct {
    controller: memory.AdmissionController = .{},
    cache_mutex: *std.atomic.Mutex,
    admissions: usize = 0,

    pub fn acquireRunResourceAmounts(self: *@This(), class: memory.BackendClass, limits: memory.Limits, amounts: memory.AdmissionAmounts) !memory.AdmissionLease {
        // Admission may evict models: never under the cache mutex.
        if (!self.cache_mutex.tryLock()) return error.TestAdmissionUnderCacheLock;
        self.cache_mutex.unlock();
        self.admissions += 1;
        return self.controller.tryAcquire(class, limits, amounts, false);
    }
};

/// A complete tiny span head (width 6, hidden 4, one layer) as safetensors
/// bytes; every value is `fill`. `pad` header spaces change the file size, so
/// a rewrite is visible to the stat fence even within one timestamp tick.
fn tinyHeadBytes(a: A, fill: f32, pad: usize) ![]u8 {
    const Spec = struct { name: []const u8, shape: []const usize };
    const specs = [_]Spec{
        .{ .name = "project.0.weight", .shape = &.{ 4, 6 } },                .{ .name = "project.0.bias", .shape = &.{4} },
        .{ .name = "project.1.weight", .shape = &.{4} },                     .{ .name = "project.1.bias", .shape = &.{4} },
        .{ .name = "segment.weight", .shape = &.{ 2, 4 } },                  .{ .name = "position.weight", .shape = &.{ 4, 4 } },
        .{ .name = "null", .shape = &.{ 1, 1, 4 } },                         .{ .name = "encoder.layers.0.self_attn.in_proj_weight", .shape = &.{ 12, 4 } },
        .{ .name = "encoder.layers.0.self_attn.in_proj_bias", .shape = &.{12} }, .{ .name = "encoder.layers.0.self_attn.out_proj.weight", .shape = &.{ 4, 4 } },
        .{ .name = "encoder.layers.0.self_attn.out_proj.bias", .shape = &.{4} }, .{ .name = "encoder.layers.0.linear1.weight", .shape = &.{ 16, 4 } },
        .{ .name = "encoder.layers.0.linear1.bias", .shape = &.{16} },       .{ .name = "encoder.layers.0.linear2.weight", .shape = &.{ 4, 16 } },
        .{ .name = "encoder.layers.0.linear2.bias", .shape = &.{4} },        .{ .name = "encoder.layers.0.norm1.weight", .shape = &.{4} },
        .{ .name = "encoder.layers.0.norm1.bias", .shape = &.{4} },          .{ .name = "encoder.layers.0.norm2.weight", .shape = &.{4} },
        .{ .name = "encoder.layers.0.norm2.bias", .shape = &.{4} },          .{ .name = "norm.weight", .shape = &.{4} },
        .{ .name = "norm.bias", .shape = &.{4} },                            .{ .name = "out.weight", .shape = &.{ 2, 4 } },
        .{ .name = "out.bias", .shape = &.{2} },
    };
    var header: std.ArrayListUnmanaged(u8) = .empty;
    defer header.deinit(a);
    try header.append(a, '{');
    var offset: usize = 0;
    for (specs, 0..) |spec, i| {
        var count: usize = 1;
        for (spec.shape) |d| count *= d;
        if (i != 0) try header.append(a, ',');
        try header.print(a, "\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{spec.name});
        for (spec.shape, 0..) |d, j| try header.print(a, "{s}{d}", .{ if (j == 0) "" else ",", d });
        try header.print(a, "],\"data_offsets\":[{d},{d}]}}", .{ offset, offset + 4 * count });
        offset += 4 * count;
    }
    try header.append(a, '}');
    try header.appendNTimes(a, ' ', pad);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, header.items.len, .little);
    try out.appendSlice(a, &size);
    try out.appendSlice(a, header.items);
    for (0..offset / 4) |_| try out.appendSlice(a, std.mem.asBytes(&fill));
    return out.toOwnedSlice(a);
}

fn writeTinyBundle(a: A, io: std.Io, dir: std.Io.Dir, fill: f32, pad: usize, pin: ?[]const u8) !void {
    const bytes = try tinyHeadBytes(a, fill, pad);
    defer a.free(bytes);
    try dir.writeFile(io, .{ .sub_path = "b/" ++ span.weights_file_name, .data = bytes });
    const sha = bundle.digest(pin orelse bytes);
    const manifest = try std.fmt.allocPrint(a, "{{\"files\":{{\"{s}\":{{\"size_bytes\":{d},\"sha256\":\"{s}\"}}}}}}", .{ span.weights_file_name, bytes.len, &sha });
    defer a.free(manifest);
    try dir.writeFile(io, .{ .sub_path = "b/manifest.json", .data = manifest });
}

test "Memorilla span head cache verifies on a miss, hits without admission and retires on drift" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try temp.dir.createDir(io, "b", .default_dir);
    try writeTinyBundle(a, io, temp.dir, 0.25, 0, null);
    const directory = try temp.dir.realPathFileAlloc(io, "b", a);
    defer a.free(directory);
    const protocol = span.Protocol{ .window_tokens = 4, .left_fraction = 0.5, .max_span_tokens = 3, .gate_threshold = 0, .gate_policy = .always, .width = 6, .hidden = 4, .layers = 1, .heads = 2, .max_positions = 3, .max_documents = 2 };
    const limits = memory.Limits{ .host_limit_bytes = 1 << 30, .backend_limit_bytes = 1 << 30, .combined_limit_bytes = 1 << 30 };
    var cache = Cache{};
    var manager = TestManager{ .cache_mutex = &cache.mutex };
    defer manager.controller.deinit();
    defer cache.deinit();

    var cold = try cache.acquire(a, a, io, &manager, directory, protocol, limits, .{});
    try std.testing.expectEqual(@as(f32, 0.25), cold.head().project_w[0]);
    const sha = cold.head().sha256;
    cold.deinit();
    try std.testing.expectEqual(span.Head.weightBytes(protocol), manager.controller.snapshot().host_weight_bytes);
    var hit = try cache.acquire(a, a, io, &manager, directory, protocol, limits, .{});
    try std.testing.expectEqualSlices(u8, &sha, &hit.head().sha256);
    hit.deinit();
    try std.testing.expectEqual(@as(usize, 1), manager.admissions);

    // Another geometry is another head: retire, re-admit, re-verify.
    var wider = protocol;
    wider.max_documents = 3;
    try std.testing.expectError(error.InvalidSpanHeadTensor, cache.acquire(a, a, io, &manager, directory, wider, limits, .{}));
    try std.testing.expectEqual(@as(usize, 0), manager.controller.snapshot().host_weight_bytes);
    var again = try cache.acquire(a, a, io, &manager, directory, protocol, limits, .{});
    again.deinit();

    // Replaced weights that the manifest no longer pins fail closed; the
    // stale entry is retired and its admission refunded.
    const old = try tinyHeadBytes(a, 0.25, 8);
    defer a.free(old);
    try writeTinyBundle(a, io, temp.dir, 0.5, 8, old);
    try std.testing.expectError(error.MemorillaArtifactMismatch, cache.acquire(a, a, io, &manager, directory, protocol, limits, .{}));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(usize, 0), manager.controller.snapshot().host_weight_bytes);

    // A consistent replacement loads fresh weights.
    try writeTinyBundle(a, io, temp.dir, 0.5, 16, null);
    var fresh = try cache.acquire(a, a, io, &manager, directory, protocol, limits, .{});
    try std.testing.expectEqual(@as(f32, 0.5), fresh.head().project_w[0]);
    fresh.deinit();
}
