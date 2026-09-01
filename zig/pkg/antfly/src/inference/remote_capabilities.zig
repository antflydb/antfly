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
    if (!response.ok()) return null;
    return try parseModelCapabilities(alloc, response.body orelse return null, model, task);
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
