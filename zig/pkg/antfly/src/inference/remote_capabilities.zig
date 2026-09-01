// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

//! Capability discovery for a remote Antfly inference service. The public
//! model catalog is the source of truth for resolved model modalities and
//! manifest capability flags; transport/resource ceilings are the stable
//! Antfly HTTP contract and remain conservative when a backend does not
//! advertise native batching.

const std = @import("std");
const httpx = @import("httpx");
const work = @import("work.zig");

const capability_cache_ttl_ns: u64 = 30 * std.time.ns_per_s;
const capability_cache_stale_ns: u64 = 5 * 60 * std.time.ns_per_s;
const capability_cache_max_entries: usize = 64;

fn monotonicNowNs(io: std.Io) u64 {
    const raw = std.Io.Timestamp.now(io, .awake).toNanoseconds();
    return if (raw <= 0) 0 else @intCast(raw);
}

const CachedCapability = struct {
    value: ?work.InferenceCapabilities,
    expires_at_ns: u64,
    stale_until_ns: u64,
};

const CapabilityFlight = struct {
    key: []u8,
    refs: usize = 1,
    done: bool = false,
    value: ?work.InferenceCapabilities = null,
    err: ?anyerror = null,
    ready: std.Io.Event = .unset,
};

/// Runtime-owned, task/model/auth-keyed capability snapshots. Catalog lookups
/// are single-flight, fresh values are reused across planner and executor
/// boundaries, and a previously validated snapshot survives a short catalog
/// outage. Authentication material is represented only by a one-way digest in
/// cache keys.
pub const Cache = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    entries: std.StringHashMapUnmanaged(CachedCapability) = .empty,
    flights: std.StringHashMapUnmanaged(*CapabilityFlight) = .empty,

    pub fn init(alloc: std.mem.Allocator, io: std.Io) Cache {
        return .{ .alloc = alloc, .io = io };
    }

    pub fn deinit(self: *Cache) void {
        self.mutex.lockUncancelable(self.io);
        std.debug.assert(self.flights.count() == 0);
        var entries = self.entries.iterator();
        while (entries.next()) |entry| self.alloc.free(@constCast(entry.key_ptr.*));
        self.entries.deinit(self.alloc);
        self.flights.deinit(self.alloc);
        self.mutex.unlock(self.io);
        self.* = undefined;
    }

    pub fn getOrDiscover(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        task: work.Task,
        headers: []const [2][]const u8,
    ) !?work.InferenceCapabilities {
        const key = try capabilityCacheKeyAlloc(self.alloc, inference_url, model, task, headers);
        defer self.alloc.free(key);
        const now_ns = monotonicNowNs(self.io);

        self.mutex.lockUncancelable(self.io);
        if (self.entries.get(key)) |entry| {
            if (now_ns < entry.expires_at_ns) {
                self.mutex.unlock(self.io);
                return entry.value;
            }
        }
        if (self.flights.get(key)) |flight| {
            flight.refs += 1;
            self.mutex.unlock(self.io);
            flight.ready.waitUncancelable(self.io);
            const value = flight.value;
            const flight_err = flight.err;
            self.mutex.lockUncancelable(self.io);
            self.releaseFlightLocked(flight);
            self.mutex.unlock(self.io);
            if (flight_err) |err| return err;
            return value;
        }

        const flight = self.alloc.create(CapabilityFlight) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        flight.* = .{ .key = self.alloc.dupe(u8, key) catch |err| {
            self.alloc.destroy(flight);
            self.mutex.unlock(self.io);
            return err;
        } };
        self.flights.put(self.alloc, flight.key, flight) catch |err| {
            self.alloc.free(flight.key);
            self.alloc.destroy(flight);
            self.mutex.unlock(self.io);
            return err;
        };
        self.mutex.unlock(self.io);

        const discovered = discover(self.alloc, http, inference_url, model, task, headers);
        if (discovered) |value| {
            self.mutex.lockUncancelable(self.io);
            self.admitLocked(key, value, monotonicNowNs(self.io)) catch {};
            flight.value = value;
            flight.done = true;
            flight.ready.set(self.io);
            self.releaseFlightLocked(flight);
            self.mutex.unlock(self.io);
            return value;
        } else |err| {
            self.mutex.lockUncancelable(self.io);
            const stale = self.entries.get(key);
            if (stale != null and monotonicNowNs(self.io) < stale.?.stale_until_ns) {
                flight.value = stale.?.value;
            } else {
                flight.err = err;
            }
            flight.done = true;
            flight.ready.set(self.io);
            const value = flight.value;
            const flight_err = flight.err;
            self.releaseFlightLocked(flight);
            self.mutex.unlock(self.io);
            if (flight_err) |flight_error| return flight_error;
            return value;
        }
    }

    fn admitLocked(self: *Cache, key: []const u8, value: ?work.InferenceCapabilities, now_ns: u64) !void {
        const cached = CachedCapability{
            .value = value,
            .expires_at_ns = now_ns +| capability_cache_ttl_ns,
            .stale_until_ns = now_ns +| capability_cache_stale_ns,
        };
        if (self.entries.getPtr(key)) |entry| {
            entry.* = cached;
            return;
        }
        if (self.entries.count() >= capability_cache_max_entries) self.evictOldestLocked();
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        try self.entries.put(self.alloc, owned_key, cached);
    }

    fn evictOldestLocked(self: *Cache) void {
        var oldest_key: ?[]const u8 = null;
        var oldest_expiry: u64 = std.math.maxInt(u64);
        var entries = self.entries.iterator();
        while (entries.next()) |entry| {
            if (entry.value_ptr.expires_at_ns < oldest_expiry) {
                oldest_expiry = entry.value_ptr.expires_at_ns;
                oldest_key = entry.key_ptr.*;
            }
        }
        if (oldest_key) |key| {
            const removed = self.entries.fetchRemove(key) orelse return;
            self.alloc.free(@constCast(removed.key));
        }
    }

    fn releaseFlightLocked(self: *Cache, flight: *CapabilityFlight) void {
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) return;
        std.debug.assert(flight.done);
        _ = self.flights.remove(flight.key);
        self.alloc.free(flight.key);
        self.alloc.destroy(flight);
    }
};

fn capabilityCacheKeyAlloc(
    alloc: std.mem.Allocator,
    inference_url: []const u8,
    model: []const u8,
    task: work.Task,
    headers: []const [2][]const u8,
) ![]u8 {
    var auth_hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for (headers) |header| {
        auth_hasher.update(header[0]);
        auth_hasher.update(&.{0});
        auth_hasher.update(header[1]);
        auth_hasher.update(&.{0xff});
    }
    var auth_digest: [32]u8 = undefined;
    auth_hasher.final(&auth_digest);
    const auth_hex = std.fmt.bytesToHex(auth_digest, .lower);
    return try std.fmt.allocPrint(alloc, "{s}\x1f{s}\x1f{s}\x1f{s}", .{
        inference_url,
        model,
        @tagName(task),
        &auth_hex,
    });
}

fn trimOperationSuffix(value: []const u8) []const u8 {
    var out = std.mem.trimEnd(u8, value, "/");
    for ([_][]const u8{ "/read", "/generate", "/generate/batch", "/embeddings" }) |suffix| {
        if (std.mem.endsWith(u8, out, suffix)) {
            out = out[0 .. out.len - suffix.len];
            break;
        }
    }
    return std.mem.trimEnd(u8, out, "/");
}

pub fn modelsUrlAlloc(alloc: std.mem.Allocator, inference_url: []const u8) ![]u8 {
    const base = trimOperationSuffix(inference_url);
    if (base.len == 0) return error.InvalidAntflyInferenceBaseUrl;
    if (std.mem.endsWith(u8, base, "/ai/v1")) return try std.fmt.allocPrint(alloc, "{s}/models", .{base});

    const scheme = std.mem.indexOf(u8, base, "://");
    const host_start = if (scheme) |index| index + 3 else 0;
    if (std.mem.indexOfPos(u8, base, host_start, "/") == null)
        return try std.fmt.allocPrint(alloc, "{s}/ai/v1/models", .{base});
    return error.InvalidAntflyInferenceBaseUrl;
}

fn taskCatalogName(task: work.Task) []const u8 {
    return switch (task) {
        .read => "readers",
        .generate => "generators",
        .embed => "embedders",
    };
}

fn outputForTask(task: work.Task) work.OutputKind {
    return switch (task) {
        .read => .read_result,
        .generate => .generated_text,
        .embed => .embedding,
    };
}

fn hasString(items: std.json.Array, expected: []const u8) bool {
    for (items.items) |item| if (item == .string and std.mem.eql(u8, item.string, expected)) return true;
    return false;
}

fn nativeBatchFor(task: work.Task, capabilities: ?std.json.Array) bool {
    if (task == .embed) return true;
    const values = capabilities orelse return false;
    return hasString(values, switch (task) {
        .read => "native_batch_read",
        .generate => "native_batch_generate_multimodal",
        .embed => unreachable,
    });
}

pub fn parseModelCapabilities(
    alloc: std.mem.Allocator,
    payload: []const u8,
    model: []const u8,
    task: work.Task,
) !?work.InferenceCapabilities {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidInferenceCapabilities;
    const catalog = parsed.value.object.get(taskCatalogName(task)) orelse return null;
    if (catalog != .object) return error.InvalidInferenceCapabilities;
    const info = catalog.object.get(model) orelse return null;
    if (info != .object) return error.InvalidInferenceCapabilities;

    var modalities = work.Modalities{};
    if (info.object.get("inputs")) |inputs| {
        if (inputs != .array) return error.InvalidInferenceCapabilities;
        modalities.text = hasString(inputs.array, "text");
        modalities.image = hasString(inputs.array, "image");
        modalities.audio = hasString(inputs.array, "audio");
        modalities.document = hasString(inputs.array, "document") or hasString(inputs.array, "pdf");
    }
    if (@as(u8, @bitCast(modalities)) == 0) return null;

    const capability_values: ?std.json.Array = if (info.object.get("capabilities")) |values| blk: {
        if (values != .array) return error.InvalidInferenceCapabilities;
        break :blk values.array;
    } else null;
    const max_items: usize = switch (task) {
        .read, .embed => 64,
        .generate => 128,
    };
    var result = work.InferenceCapabilities{
        .task = task,
        .input_modalities = modalities,
        .accepted_mime_types = .{
            .text_plain = modalities.text,
            .application_pdf = modalities.document,
            .image_png = modalities.image,
            .image_jpeg = modalities.image,
            .image_webp = modalities.image,
            .audio_wav = modalities.audio,
            .audio_mpeg = modalities.audio,
        },
        .input_granularity = if (modalities.document)
            .document
        else if (modalities.image)
            .page
        else
            .chunk,
        .batch = .{
            .mode = if (nativeBatchFor(task, capability_values)) .native else .serial_compatibility,
            .preferred_items = @min(@as(usize, 8), max_items),
            .max_items = max_items,
            .max_encoded_bytes = 64 * 1024 * 1024,
            .max_decoded_pixels = 50_000_000,
            .max_media_parts_per_item = if (task == .generate) 8 else 1,
            .per_item_failures = task == .generate,
        },
        .output = outputForTask(task),
        .result_cardinality = .one_per_item,
        .prompt_policy = .explicit,
        .borrowed_attachments = false,
    };
    if (capability_values) |values| {
        for (values.items) |value| {
            if (value == .string) try result.batch.applyManifestCapability(value.string);
        }
    }
    result.batch.preferred_items = @min(result.batch.preferred_items, result.batch.max_items);
    try result.validate();
    return result;
}

pub fn discover(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    task: work.Task,
    headers: []const [2][]const u8,
) !?work.InferenceCapabilities {
    const url = try modelsUrlAlloc(alloc, inference_url);
    defer alloc.free(url);
    var response = try http.get(url, .{ .headers = headers });
    defer response.deinit();
    if (!response.ok()) return error.RemoteCapabilityDiscoveryFailed;
    return try parseModelCapabilities(alloc, response.body orelse return error.RemoteCapabilityDiscoveryFailed, model, task);
}

test "remote Antfly capabilities resolve model modalities and batch mode" {
    const payload =
        \\{"readers":{"florence":{"inputs":["text","image"],"capabilities":["native_batch_read","inference.batch.max_items=12"]}},"embedders":{"clipclap":{"inputs":["text","image","audio"]}}}
    ;
    const reader = (try parseModelCapabilities(std.testing.allocator, payload, "florence", .read)).?;
    try std.testing.expect(reader.supports(.{ .image = true }));
    try std.testing.expectEqual(work.BatchMode.native, reader.batch.mode);
    try std.testing.expectEqual(@as(usize, 12), reader.batch.max_items);
    const embedder = (try parseModelCapabilities(std.testing.allocator, payload, "clipclap", .embed)).?;
    try std.testing.expect(embedder.supports(.{ .image = true, .audio = true }));
    try std.testing.expectEqual(work.BatchMode.native, embedder.batch.mode);
    try std.testing.expect((try parseModelCapabilities(std.testing.allocator, payload, "missing", .embed)) == null);
}

test "remote Antfly model catalog URL normalizes service and operation URLs" {
    const root = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082");
    defer std.testing.allocator.free(root);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", root);
    const read = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082/ai/v1/read");
    defer std.testing.allocator.free(read);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", read);
}
