// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

//! Capability discovery for a remote Antfly inference service. The public
//! model catalog is the source of truth for resolved model modalities,
//! execution behavior, and live transport/resource ceilings. Legacy catalogs
//! fail closed instead of making limits up in the client.

const std = @import("std");
const platform_time = @import("antfly_platform").time;
const httpx = @import("httpx");
const work = @import("work.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;

const capability_cache_ttl_ns: u64 = 30 * std.time.ns_per_s;
const capability_cache_stale_ns: u64 = 5 * 60 * std.time.ns_per_s;
const capability_cache_max_entries: usize = 64;
const capability_discovery_timeout_ns: u64 = 30 * std.time.ns_per_s;
const capability_wait_poll_ns: u64 = 25 * std.time.ns_per_ms;

fn monotonicNowNs(io: std.Io) u64 {
    _ = io;
    // Callers pass absolute deadlines produced by antfly_platform.time. Keep
    // every comparison in that clock domain; std.Io's `.awake` clock maps to
    // CLOCK_UPTIME_RAW on Darwin and cannot be compared with CLOCK_MONOTONIC.
    return platform_time.monotonicNs();
}

const CachedCapability = struct {
    value: ?work.InferenceCapabilities,
    expires_at_ns: u64,
    stale_until_ns: u64,
};

const FailedDiscoveryCompletion = struct {
    value: ?work.InferenceCapabilities = null,
    err: ?anyerror = null,
};

fn classifyFailedDiscovery(
    stale: ?CachedCapability,
    now_ns: u64,
    owner_context_error: ?anyerror,
    discovery_error: anyerror,
) FailedDiscoveryCompletion {
    // Request cancellation/expiry is control flow owned by the discovering
    // caller. It must retire the flight even when a valid stale entry exists,
    // allowing independent waiters to retry with their own contexts.
    if (owner_context_error != null)
        return .{ .err = error.CapabilityDiscoveryOwnerAbandoned };
    if (stale) |entry| {
        if (now_ns < entry.stale_until_ns) return .{ .value = entry.value };
    }
    return .{ .err = discovery_error };
}

const CapabilityFlight = struct {
    key: []u8,
    refs: usize = 1,
    done: bool = false,
    value: ?work.InferenceCapabilities = null,
    err: ?anyerror = null,
    ready: std.Io.Event = .unset,
    refs_changed: std.Io.Event = .unset,
};

pub const WaitContext = struct {
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,

    fn check(self: @This(), now_ns: u64) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (now_ns >= deadline) return error.Timeout;
    }
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
    closing: bool = false,

    pub fn init(alloc: std.mem.Allocator, io: std.Io) Cache {
        return .{ .alloc = alloc, .io = io };
    }

    pub fn deinit(self: *Cache) void {
        self.mutex.lockUncancelable(self.io);
        self.closing = true;
        while (self.flights.count() != 0) {
            var flights = self.flights.iterator();
            const flight = flights.next().?.value_ptr.*;
            flight.refs += 1;
            while (flight.refs != 1) {
                flight.refs_changed.reset();
                self.mutex.unlock(self.io);
                flight.refs_changed.waitUncancelable(self.io);
                self.mutex.lockUncancelable(self.io);
            }
            self.releaseFlightLocked(flight);
        }
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
        return try self.getOrDiscoverWithContext(http, inference_url, model, task, headers, .{
            .deadline_ns = monotonicNowNs(self.io) +| capability_discovery_timeout_ns,
        });
    }

    pub fn getOrDiscoverWithContext(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        task: work.Task,
        headers: []const [2][]const u8,
        wait_context: WaitContext,
    ) !?work.InferenceCapabilities {
        const key = try capabilityCacheKeyAlloc(self.alloc, inference_url, model, task, headers);
        defer self.alloc.free(key);
        while (true) {
            try wait_context.check(monotonicNowNs(self.io));
            const now_ns = monotonicNowNs(self.io);

            self.mutex.lockUncancelable(self.io);
            if (self.closing) {
                self.mutex.unlock(self.io);
                return error.CapabilityCacheClosed;
            }
            if (self.entries.get(key)) |entry| {
                if (now_ns < entry.expires_at_ns) {
                    self.mutex.unlock(self.io);
                    return entry.value;
                }
            }
            if (self.flights.get(key)) |flight| {
                if (flight.done and (if (flight.err) |err|
                    err == error.CapabilityDiscoveryOwnerAbandoned
                else
                    false))
                {
                    // Keep the retired flight tracked for teardown until its
                    // original waiters release it, but never let a retry add a
                    // new reference and prolong that retirement.
                    self.mutex.unlock(self.io);
                    try wait_context.check(monotonicNowNs(self.io));
                    self.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch |err| switch (err) {
                        error.Canceled => return error.Canceled,
                    };
                    continue;
                }
                flight.refs += 1;
                self.mutex.unlock(self.io);
                waitForFlight(self.io, flight, wait_context) catch |err| {
                    self.mutex.lockUncancelable(self.io);
                    self.releaseFlightLocked(flight);
                    self.mutex.unlock(self.io);
                    return err;
                };
                self.mutex.lockUncancelable(self.io);
                const value = flight.value;
                const flight_err = flight.err;
                self.releaseFlightLocked(flight);
                self.mutex.unlock(self.io);
                if (flight_err) |err| {
                    if (err == error.CapabilityDiscoveryOwnerAbandoned) continue;
                    return err;
                }
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

            const discovered = discoverWithContext(self.alloc, self.io, http, inference_url, model, task, headers, wait_context);
            if (discovered) |value| {
                const owner_context_error: ?anyerror = blk: {
                    wait_context.check(monotonicNowNs(self.io)) catch |err| break :blk err;
                    break :blk null;
                };
                self.mutex.lockUncancelable(self.io);
                self.admitLocked(key, value, monotonicNowNs(self.io)) catch {};
                flight.value = value;
                flight.done = true;
                flight.ready.set(self.io);
                self.releaseFlightLocked(flight);
                self.mutex.unlock(self.io);
                if (owner_context_error) |err| return err;
                return value;
            } else |err| {
                const owner_context_error: ?anyerror = blk: {
                    wait_context.check(monotonicNowNs(self.io)) catch |context_err| break :blk context_err;
                    break :blk null;
                };
                self.mutex.lockUncancelable(self.io);
                const completion = classifyFailedDiscovery(
                    self.entries.get(key),
                    monotonicNowNs(self.io),
                    owner_context_error,
                    err,
                );
                flight.value = completion.value;
                flight.err = completion.err;
                flight.done = true;
                flight.ready.set(self.io);
                const value = flight.value;
                const flight_err = flight.err;
                self.releaseFlightLocked(flight);
                self.mutex.unlock(self.io);
                if (owner_context_error) |context_err| return context_err;
                if (flight_err) |flight_error| return flight_error;
                return value;
            }
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
        flight.refs_changed.set(self.io);
        if (flight.refs != 0) return;
        std.debug.assert(flight.done);
        _ = self.flights.remove(flight.key);
        self.alloc.free(flight.key);
        self.alloc.destroy(flight);
    }
};

fn waitForFlight(io: std.Io, flight: *CapabilityFlight, context: WaitContext) !void {
    while (!flight.ready.isSet()) {
        const now_ns = monotonicNowNs(io);
        try context.check(now_ns);
        const wait_ns = if (context.deadline_ns) |deadline|
            @min(capability_wait_poll_ns, deadline - now_ns)
        else
            capability_wait_poll_ns;
        flight.ready.waitTimeout(io, .{ .duration = .{
            .raw = std.Io.Duration.fromNanoseconds(@intCast(wait_ns)),
            .clock = .awake,
        } }) catch |err| switch (err) {
            error.Timeout => continue,
            error.Canceled => return error.Canceled,
        };
    }
    try context.check(monotonicNowNs(io));
}

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

fn jsonCapabilityUsize(object: std.json.ObjectMap, name: []const u8) !usize {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value != .integer or value.integer < 0) return error.InvalidInferenceCapabilities;
    return std.math.cast(usize, value.integer) orelse error.InvalidInferenceCapabilities;
}

fn parseResolvedBatchCapabilities(value: std.json.Value) !work.BatchCapabilities {
    if (value != .object) return error.InvalidInferenceCapabilities;
    const mode_value = value.object.get("mode") orelse return error.InvalidInferenceCapabilities;
    if (mode_value != .string) return error.InvalidInferenceCapabilities;
    const mode: work.BatchMode = if (std.mem.eql(u8, mode_value.string, "native"))
        .native
    else if (std.mem.eql(u8, mode_value.string, "serial_compatibility"))
        .serial_compatibility
    else if (std.mem.eql(u8, mode_value.string, "none"))
        .none
    else
        return error.InvalidInferenceCapabilities;
    const per_item_value = value.object.get("per_item_failures") orelse return error.InvalidInferenceCapabilities;
    if (per_item_value != .bool) return error.InvalidInferenceCapabilities;
    return .{
        .mode = mode,
        .preferred_items = try jsonCapabilityUsize(value.object, "preferred_items"),
        .max_items = try jsonCapabilityUsize(value.object, "max_items"),
        .max_encoded_bytes = try jsonCapabilityUsize(value.object, "max_encoded_bytes"),
        .max_decoded_pixels = try jsonCapabilityUsize(value.object, "max_decoded_pixels"),
        .max_media_parts_per_item = try jsonCapabilityUsize(value.object, "max_media_parts_per_item"),
        .per_item_failures = per_item_value.bool,
    };
}

fn parseResolvedCapabilities(info: std.json.ObjectMap, task: work.Task) !?work.BatchCapabilities {
    const value = info.get("inference_capabilities") orelse return null;
    if (value != .object) return error.InvalidInferenceCapabilities;
    const task_value = value.object.get("task") orelse return error.InvalidInferenceCapabilities;
    if (task_value != .string or !std.mem.eql(u8, task_value.string, @tagName(task)))
        return error.InvalidInferenceCapabilities;
    const batch_value = value.object.get("batch") orelse return error.InvalidInferenceCapabilities;
    const batch = try parseResolvedBatchCapabilities(batch_value);
    try batch.validate();
    return batch;
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
    const resolved_batch = try parseResolvedCapabilities(info.object, task);
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
        // Older catalogs cannot prove live executor or request-limit facts.
        // Preserve compatibility safely as a singleton until the server
        // publishes the resolved descriptor below.
        .batch = resolved_batch orelse .{
            .mode = .none,
            .preferred_items = 1,
            .max_items = 1,
            .max_encoded_bytes = 0,
            .max_decoded_pixels = 0,
            .max_media_parts_per_item = if (modalities.image or modalities.audio) 1 else 0,
            .per_item_failures = false,
        },
        .output = outputForTask(task),
        .result_cardinality = .one_per_item,
        .prompt_policy = .explicit,
        .borrowed_attachments = false,
    };
    if (resolved_batch == null) if (capability_values) |values| {
        for (values.items) |value| {
            if (value == .string) try result.batch.applyManifestCapability(value.string);
        }
    };
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

fn discoverWithContext(
    alloc: std.mem.Allocator,
    io: std.Io,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    task: work.Task,
    headers: []const [2][]const u8,
    context: WaitContext,
) !?work.InferenceCapabilities {
    const now_ns = monotonicNowNs(io);
    try context.check(now_ns);
    const effective_deadline = @min(
        context.deadline_ns orelse now_ns +| capability_discovery_timeout_ns,
        now_ns +| capability_discovery_timeout_ns,
    );
    if (now_ns >= effective_deadline) return error.Timeout;
    const remaining_ns = effective_deadline - now_ns;
    const timeout_ms: u64 = @max(1, @min(
        @as(u64, 30_000),
        (remaining_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms,
    ));
    const url = try modelsUrlAlloc(alloc, inference_url);
    defer alloc.free(url);
    var response = try http.get(url, .{
        .headers = headers,
        .timeout_ms = timeout_ms,
        .cancellation = httpx.CancellationToken.fromCallback(
            context.cancellation.ptr,
            context.cancellation.is_cancelled_fn,
        ),
    });
    defer response.deinit();
    if (!response.ok()) return error.RemoteCapabilityDiscoveryFailed;
    return try parseModelCapabilities(alloc, response.body orelse return error.RemoteCapabilityDiscoveryFailed, model, task);
}

test "remote Antfly capabilities resolve model modalities and batch mode" {
    const payload =
        \\{"readers":{"florence":{"inputs":["text","image"],"inference_capabilities":{"task":"read","batch":{"mode":"native","preferred_items":8,"max_items":12,"max_encoded_bytes":33554432,"max_decoded_pixels":0,"max_media_parts_per_item":1,"per_item_failures":false}}}},"embedders":{"clipclap":{"inputs":["text","image","audio"],"inference_capabilities":{"task":"embed","batch":{"mode":"native","preferred_items":8,"max_items":64,"max_encoded_bytes":33554432,"max_decoded_pixels":0,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
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

test "remote Antfly capabilities fail closed for legacy execution claims" {
    const payload =
        \\{"generators":{"gemma4":{"inputs":["text","image"],"capabilities":["native_batch_generate_multimodal"]}}}
    ;
    const generator = (try parseModelCapabilities(std.testing.allocator, payload, "gemma4", .generate)).?;
    try std.testing.expectEqual(work.BatchMode.none, generator.batch.mode);
    try std.testing.expectEqual(@as(usize, 1), generator.batch.max_items);
}

test "remote Antfly model catalog URL normalizes service and operation URLs" {
    const root = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082");
    defer std.testing.allocator.free(root);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", root);
    const read = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082/ai/v1/read");
    defer std.testing.allocator.free(read);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", read);
}

test "remote Antfly capability single-flight wait observes deadline and cancellation" {
    const io = std.Options.debug_io;
    var flight = CapabilityFlight{ .key = @constCast("test") };
    try std.testing.expectError(error.Timeout, waitForFlight(io, &flight, .{ .deadline_ns = 0 }));

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, waitForFlight(io, &flight, .{
        .cancellation = CancellationToken.fromAtomic(&canceled),
    }));
}

test "remote Antfly completed capability flight remains tracked until all waiters release" {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc, std.Options.debug_io);
    defer cache.deinit();

    const flight = try alloc.create(CapabilityFlight);
    flight.* = .{
        .key = try alloc.dupe(u8, "endpoint:model:task"),
        .refs = 2,
        .done = true,
    };
    try cache.flights.put(alloc, flight.key, flight);

    cache.releaseFlightLocked(flight);
    try std.testing.expectEqual(@as(usize, 1), cache.flights.count());
    try std.testing.expectEqual(@as(usize, 1), flight.refs);

    cache.releaseFlightLocked(flight);
    try std.testing.expectEqual(@as(usize, 0), cache.flights.count());
}

test "canceled capability owner cannot publish stale success to waiters" {
    const stale = CachedCapability{
        .value = null,
        .expires_at_ns = 0,
        .stale_until_ns = 10_000,
    };
    const completion = classifyFailedDiscovery(
        stale,
        1,
        error.Canceled,
        error.RemoteCapabilityDiscoveryFailed,
    );
    try std.testing.expect(completion.value == null);
    try std.testing.expectEqual(error.CapabilityDiscoveryOwnerAbandoned, completion.err.?);
}
