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

fn primaryOperationSuffix(task: work.Task) []const u8 {
    return switch (task) {
        .read => "/read",
        .generate => "/generate",
        .embed => "/embed",
        .rerank => "/rerank",
        .chunk => "/chunk",
        .extract => "/extract",
        .rewrite => "/rewrite",
        .classify => "/classify",
        .transcribe => "/transcribe",
    };
}

fn trimOperationSuffix(value: []const u8) []const u8 {
    var out = std.mem.trimEnd(u8, value, "/");
    // Compatibility aliases are checked first because some extend a primary
    // route (for example generate/batch extends generate). Primary routes are
    // derived exhaustively from Task so adding a family cannot silently omit
    // catalog URL normalization.
    for ([_][]const u8{
        "/chat/completions",
        "/generate/batch",
        "/rerank_multimodal",
        "/embeddings",
    }) |suffix| {
        if (std.mem.endsWith(u8, out, suffix)) {
            return std.mem.trimEnd(u8, out[0 .. out.len - suffix.len], "/");
        }
    }
    for (std.enums.values(work.Task)) |task| {
        const suffix = primaryOperationSuffix(task);
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

fn percentEncodeQueryValueAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (raw) |byte| {
        if ((byte >= 'A' and byte <= 'Z') or
            (byte >= 'a' and byte <= 'z') or
            (byte >= '0' and byte <= '9') or
            byte == '-' or byte == '.' or byte == '_' or byte == '~')
        {
            try out.append(alloc, byte);
        } else {
            var buf: [3]u8 = undefined;
            try out.appendSlice(alloc, try std.fmt.bufPrint(&buf, "%{X:0>2}", .{byte}));
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn scopedModelsUrlAlloc(
    alloc: std.mem.Allocator,
    inference_url: []const u8,
    model: []const u8,
    task: work.Task,
) ![]u8 {
    const base = try modelsUrlAlloc(alloc, inference_url);
    defer alloc.free(base);
    const encoded_model = try percentEncodeQueryValueAlloc(alloc, model);
    defer alloc.free(encoded_model);
    return try std.fmt.allocPrint(alloc, "{s}?model={s}&task={s}", .{ base, encoded_model, @tagName(task) });
}

fn taskCatalogName(task: work.Task) []const u8 {
    return switch (task) {
        .read => "readers",
        .generate => "generators",
        .embed => "embedders",
        .rerank => "rerankers",
        .chunk => "chunkers",
        .extract => "extractors",
        .rewrite => "rewriters",
        .classify => "classifiers",
        .transcribe => "transcribers",
    };
}

fn outputForTask(task: work.Task) work.OutputKind {
    return switch (task) {
        .read => .read_result,
        .generate => .generated_text,
        .embed => .embedding,
        .rerank => .ranked_items,
        .chunk => .chunks,
        .extract => .extraction,
        .rewrite => .rewritten_text,
        .classify => .classification,
        .transcribe => .transcription,
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

fn jsonOptionalCapabilityUsize(object: std.json.ObjectMap, name: []const u8, zero_is_unknown: bool) !?usize {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value == .null) return null;
    if (value != .integer or value.integer < 0) return error.InvalidInferenceCapabilities;
    const number = std.math.cast(usize, value.integer) orelse return error.InvalidInferenceCapabilities;
    return if (zero_is_unknown and number == 0) null else number;
}

fn parseResolvedBatchCapabilities(value: std.json.Value, version: usize) !work.BatchCapabilities {
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
        .max_encoded_media_bytes = if (version >= 2)
            try jsonOptionalCapabilityUsize(value.object, "max_encoded_media_bytes", false)
        else
            try jsonOptionalCapabilityUsize(value.object, "max_encoded_bytes", true),
        .max_decoded_pixels = try jsonOptionalCapabilityUsize(value.object, "max_decoded_pixels", version < 2),
        .max_media_parts_per_item = try jsonCapabilityUsize(value.object, "max_media_parts_per_item"),
        .per_item_failures = per_item_value.bool,
    };
}

const ExactWireCapabilities = struct {
    modalities: work.Modalities,
    mime_types: work.MimeTypes,
    input_granularity: work.InputGranularity,
    output: work.OutputKind,
    result_cardinality: work.ResultCardinality,
    prompt_policy: work.PromptPolicy,
    borrowed_attachments: bool,
};

const ResolvedWireCapabilities = struct {
    batch: work.BatchCapabilities,
    exact: ?ExactWireCapabilities,
};

fn parseResolvedCapabilities(info: std.json.ObjectMap, task: work.Task) !?ResolvedWireCapabilities {
    const value = info.get("inference_capabilities") orelse return null;
    if (value != .object) return error.InvalidInferenceCapabilities;
    const version: usize = if (value.object.get("version")) |version_value| blk: {
        if (version_value != .integer or version_value.integer < 1) return error.InvalidInferenceCapabilities;
        break :blk std.math.cast(usize, version_value.integer) orelse return error.InvalidInferenceCapabilities;
    } else 1;
    if (version > 3) return error.UnsupportedInferenceCapabilitiesVersion;
    const task_value = value.object.get("task") orelse return error.InvalidInferenceCapabilities;
    if (task_value != .string or !std.mem.eql(u8, task_value.string, @tagName(task)))
        return error.InvalidInferenceCapabilities;
    const batch_value = value.object.get("batch") orelse return error.InvalidInferenceCapabilities;
    const batch = try parseResolvedBatchCapabilities(batch_value, version);
    try batch.validate();
    return .{
        .batch = batch,
        .exact = if (version >= 3) try parseExactWireCapabilities(value.object) else null,
    };
}

fn requiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value != .string) return error.InvalidInferenceCapabilities;
    return value.string;
}

fn requiredStringArray(object: std.json.ObjectMap, name: []const u8) !std.json.Array {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value != .array) return error.InvalidInferenceCapabilities;
    for (value.array.items) |item| if (item != .string) return error.InvalidInferenceCapabilities;
    return value.array;
}

fn validateStringSet(items: std.json.Array, allowed: []const []const u8) !void {
    if (items.items.len == 0) return error.InvalidInferenceCapabilities;
    for (items.items, 0..) |item, index| {
        var known = false;
        for (allowed) |candidate| {
            if (std.mem.eql(u8, item.string, candidate)) {
                known = true;
                break;
            }
        }
        if (!known) return error.InvalidInferenceCapabilities;
        for (items.items[0..index]) |prior| {
            if (std.mem.eql(u8, prior.string, item.string)) return error.InvalidInferenceCapabilities;
        }
    }
}

fn resultCardinalityForTask(task: work.Task) work.ResultCardinality {
    return switch (task) {
        .rerank, .chunk, .transcribe => .one_per_request,
        else => .one_per_item,
    };
}

fn promptPolicyForTask(task: work.Task) work.PromptPolicy {
    return switch (task) {
        .extract => .structured_schema,
        .chunk, .transcribe => .model_default,
        else => .explicit,
    };
}

fn parseExactWireCapabilities(object: std.json.ObjectMap) !ExactWireCapabilities {
    const modality_values = try requiredStringArray(object, "input_modalities");
    const mime_values = try requiredStringArray(object, "accepted_mime_types");
    try validateStringSet(modality_values, &.{ "text", "image", "audio", "document" });
    try validateStringSet(mime_values, &.{
        "text/plain",
        "application/json",
        "application/pdf",
        "image/png",
        "image/jpeg",
        "image/webp",
        "audio/wav",
        "audio/mpeg",
    });
    const input_granularity: work.InputGranularity = std.meta.stringToEnum(
        work.InputGranularity,
        try requiredString(object, "input_granularity"),
    ) orelse return error.InvalidInferenceCapabilities;
    const output: work.OutputKind = std.meta.stringToEnum(
        work.OutputKind,
        try requiredString(object, "output"),
    ) orelse return error.InvalidInferenceCapabilities;
    const result_cardinality: work.ResultCardinality = std.meta.stringToEnum(
        work.ResultCardinality,
        try requiredString(object, "result_cardinality"),
    ) orelse return error.InvalidInferenceCapabilities;
    const prompt_policy: work.PromptPolicy = std.meta.stringToEnum(
        work.PromptPolicy,
        try requiredString(object, "prompt_policy"),
    ) orelse return error.InvalidInferenceCapabilities;
    const borrowed_value = object.get("borrowed_attachments") orelse return error.InvalidInferenceCapabilities;
    if (borrowed_value != .bool) return error.InvalidInferenceCapabilities;
    return .{
        .modalities = .{
            .text = hasString(modality_values, "text"),
            .image = hasString(modality_values, "image"),
            .audio = hasString(modality_values, "audio"),
            .document = hasString(modality_values, "document"),
        },
        .mime_types = .{
            .text_plain = hasString(mime_values, "text/plain"),
            .application_json = hasString(mime_values, "application/json"),
            .application_pdf = hasString(mime_values, "application/pdf"),
            .image_png = hasString(mime_values, "image/png"),
            .image_jpeg = hasString(mime_values, "image/jpeg"),
            .image_webp = hasString(mime_values, "image/webp"),
            .audio_wav = hasString(mime_values, "audio/wav"),
            .audio_mpeg = hasString(mime_values, "audio/mpeg"),
        },
        .input_granularity = input_granularity,
        .output = output,
        .result_cardinality = result_cardinality,
        .prompt_policy = prompt_policy,
        .borrowed_attachments = borrowed_value.bool,
    };
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
    const capability_values: ?std.json.Array = if (info.object.get("capabilities")) |values| blk: {
        if (values != .array) return error.InvalidInferenceCapabilities;
        break :blk values.array;
    } else null;
    const resolved = try parseResolvedCapabilities(info.object, task);
    if (resolved) |wire| {
        if (wire.exact) |exact| modalities = exact.modalities;
    }
    if (@as(u8, @bitCast(modalities)) == 0) return null;
    const exact = if (resolved) |wire| wire.exact else null;
    var result = work.InferenceCapabilities{
        .task = task,
        .input_modalities = modalities,
        .accepted_mime_types = if (exact) |value| value.mime_types else .{
            .text_plain = modalities.text,
            .application_pdf = modalities.document,
            .image_png = modalities.image,
            .image_jpeg = modalities.image,
            .image_webp = modalities.image,
            .audio_wav = modalities.audio,
            .audio_mpeg = modalities.audio,
        },
        .input_granularity = if (exact) |value| value.input_granularity else if (modalities.document)
            .document
        else if (modalities.image)
            .page
        else if (modalities.text and (task == .read or task == .generate or task == .embed))
            .chunk
        else
            .item,
        // Older catalogs cannot prove live executor or request-limit facts.
        // Preserve compatibility safely as a singleton until the server
        // publishes the resolved descriptor below.
        .batch = if (resolved) |wire| wire.batch else .{
            .mode = .none,
            .preferred_items = 1,
            .max_items = 1,
            .max_encoded_media_bytes = null,
            .max_decoded_pixels = null,
            .max_media_parts_per_item = if (modalities.image or modalities.audio or modalities.document) 1 else 0,
            .per_item_failures = false,
        },
        .output = if (exact) |value| value.output else outputForTask(task),
        .result_cardinality = if (exact) |value| value.result_cardinality else resultCardinalityForTask(task),
        .prompt_policy = if (exact) |value| value.prompt_policy else promptPolicyForTask(task),
        // Discovery is performed across an HTTP boundary. A remote catalog
        // cannot turn that concrete route into a borrowed-memory invocation,
        // even if a misconfigured upstream publishes its local ABI fact.
        .borrowed_attachments = false,
    };
    if (resolved == null) if (capability_values) |values| {
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
    const url = try scopedModelsUrlAlloc(alloc, inference_url, model, task);
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
    const url = try scopedModelsUrlAlloc(alloc, inference_url, model, task);
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

test "remote Antfly capability parser supports every model family" {
    const cases = [_]struct {
        task: work.Task,
        category: []const u8,
        output: work.OutputKind,
        input: []const u8,
    }{
        .{ .task = .read, .category = "readers", .output = .read_result, .input = "image" },
        .{ .task = .generate, .category = "generators", .output = .generated_text, .input = "text" },
        .{ .task = .embed, .category = "embedders", .output = .embedding, .input = "text" },
        .{ .task = .rerank, .category = "rerankers", .output = .ranked_items, .input = "text" },
        .{ .task = .chunk, .category = "chunkers", .output = .chunks, .input = "text" },
        .{ .task = .extract, .category = "extractors", .output = .extraction, .input = "text" },
        .{ .task = .rewrite, .category = "rewriters", .output = .rewritten_text, .input = "text" },
        .{ .task = .classify, .category = "classifiers", .output = .classification, .input = "text" },
        .{ .task = .transcribe, .category = "transcribers", .output = .transcription, .input = "audio" },
    };
    for (cases) |case| {
        const payload = try std.fmt.allocPrint(
            std.testing.allocator,
            "{{\"{s}\":{{\"model\":{{\"inputs\":[\"{s}\"],\"inference_capabilities\":{{\"task\":\"{s}\",\"batch\":{{\"mode\":\"none\",\"preferred_items\":1,\"max_items\":1,\"max_encoded_bytes\":null,\"max_decoded_pixels\":null,\"max_media_parts_per_item\":0,\"per_item_failures\":false}}}}}}}}}}",
            .{ case.category, case.input, @tagName(case.task) },
        );
        defer std.testing.allocator.free(payload);
        const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "model", case.task)).?;
        try std.testing.expectEqual(case.output, capabilities.output);
        try std.testing.expectEqual(resultCardinalityForTask(case.task), capabilities.result_cardinality);
        try std.testing.expectEqual(promptPolicyForTask(case.task), capabilities.prompt_policy);
        if (case.task == .read or case.task == .generate or case.task == .embed) {
            try std.testing.expect(capabilities.input_granularity != .item);
        } else {
            try std.testing.expectEqual(work.InputGranularity.item, capabilities.input_granularity);
        }
    }
}

test "remote Antfly model catalog URL normalizes service and operation URLs" {
    const root = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082");
    defer std.testing.allocator.free(root);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", root);
    const read = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082/ai/v1/read");
    defer std.testing.allocator.free(read);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", read);

    for ([_][]const u8{
        "embed",
        "embeddings",
        "chunk",
        "rerank",
        "rerank_multimodal",
        "extract",
        "generate",
        "generate/batch",
        "chat/completions",
        "rewrite",
        "classify",
        "transcribe",
    }) |operation| {
        const operation_url = try std.fmt.allocPrint(std.testing.allocator, "http://localhost:8082/ai/v1/{s}", .{operation});
        defer std.testing.allocator.free(operation_url);
        const catalog_url = try modelsUrlAlloc(std.testing.allocator, operation_url);
        defer std.testing.allocator.free(catalog_url);
        try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", catalog_url);
    }

    const scoped = try scopedModelsUrlAlloc(std.testing.allocator, "http://localhost:8082/ai/v1/generate/batch", "owner/gemma 4", .generate);
    defer std.testing.allocator.free(scoped);
    try std.testing.expectEqualStrings(
        "http://localhost:8082/ai/v1/models?model=owner%2Fgemma%204&task=generate",
        scoped,
    );
}

test "remote Antfly capability parser maps legacy zero and explicit null to unknown" {
    const payload =
        \\{"readers":{"florence":{"inputs":["image"],"inference_capabilities":{"task":"read","batch":{"mode":"native","preferred_items":2,"max_items":4,"max_encoded_bytes":null,"max_decoded_pixels":0,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "florence", .read)).?;
    try std.testing.expectEqual(@as(?usize, null), capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, null), capabilities.batch.max_decoded_pixels);
}

test "remote Antfly capability v2 preserves disabled media and pixel limits" {
    const payload =
        \\{"generators":{"text-only":{"inputs":["text"],"inference_capabilities":{"version":2,"task":"generate","batch":{"mode":"none","preferred_items":1,"max_items":1,"max_encoded_media_bytes":0,"max_decoded_pixels":0,"max_media_parts_per_item":0,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "text-only", .generate)).?;
    try std.testing.expectEqual(@as(?usize, 0), capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, 0), capabilities.batch.max_decoded_pixels);
}

test "remote Antfly capability v3 preserves exact task contract" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["text"],"inference_capabilities":{"version":3,"task":"extract","input_modalities":["image"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":false,"batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":32,"max_encoded_media_bytes":4096,"max_decoded_pixels":8192,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "vision-extractor", .extract)).?;
    try std.testing.expect(!capabilities.supports(.{ .text = true }));
    try std.testing.expect(capabilities.supports(.{ .image = true }));
    try std.testing.expect(capabilities.acceptsMimeType("image/png"));
    try std.testing.expect(!capabilities.acceptsMimeType("image/jpeg"));
    try std.testing.expectEqual(work.InputGranularity.page, capabilities.input_granularity);
    try std.testing.expectEqual(work.PromptPolicy.structured_schema, capabilities.prompt_policy);
    try std.testing.expectEqual(work.BatchMode.serial_compatibility, capabilities.batch.mode);
    try std.testing.expectEqual(@as(usize, 32), capabilities.batch.max_items);
    try std.testing.expect(!capabilities.borrowed_attachments);
}

test "remote Antfly capability cannot advertise borrowed HTTP attachments" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["image"],"inference_capabilities":{"version":3,"task":"extract","input_modalities":["image"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":true,"batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":32,"max_encoded_media_bytes":4096,"max_decoded_pixels":8192,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(
        std.testing.allocator,
        payload,
        "vision-extractor",
        .extract,
    )).?;
    try std.testing.expect(!capabilities.borrowed_attachments);
}

test "remote Antfly capability v3 rejects unknown exact values" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["text"],"inference_capabilities":{"version":3,"task":"extract","input_modalities":["image","imagge"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":false,"batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":32,"max_encoded_media_bytes":4096,"max_decoded_pixels":8192,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        parseModelCapabilities(std.testing.allocator, payload, "vision-extractor", .extract),
    );
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
