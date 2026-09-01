// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

const std = @import("std");
const httpx = @import("httpx");
const readers = @import("antfly_readers");
const asset_producer_runtime = @import("asset_producer_runtime.zig");
const asset_producer = @import("storage/db/enrichment/asset_producer.zig");
const enrichment_runtime = @import("storage/db/enrichment/enrichment_runtime.zig");
const managed_embedder = @import("inference/managed_embedder.zig");
const inference_work = @import("inference/work.zig");
const inference_types = @import("inference/types.zig");
const embedder = @import("storage/db/enrichment/embedder.zig");
const document_extraction = @import("storage/db/enrichment/document_extraction.zig");
const template = @import("template.zig");
const fixture = @import("pdf_integration_fixture");

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const alloc = gpa.allocator();
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();
    _ = args.next();
    if (args.next()) |arg| {
        if (!std.mem.eql(u8, arg, "--qualify-real")) return error.InvalidIntegrationArgument;
        return try runRealModelQualification(alloc, &client);
    }

    const LocalFlorenceBoundary = struct {
        read_calls: usize = 0,
        generator_calls: usize = 0,
        embed_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .embed_dense_parts = embedParts,
                .read_encoded_images = readEncodedImages,
                .read_encoded_images_reported = readEncodedImagesReported,
                .generate_messages_with_attachments = generateMessagesWithAttachments,
                .model_capabilities = modelCapabilities,
            };
        }

        fn modelCapabilities(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            task: inference_work.Task,
        ) !inference_work.InferenceCapabilities {
            return .{
                .task = task,
                .input_modalities = switch (task) {
                    .read => .{ .image = true },
                    .generate => .{ .text = true, .image = true },
                    .embed => .{ .text = true, .image = true },
                },
                .accepted_mime_types = .{ .text_plain = true, .image_png = true, .image_jpeg = true },
                .input_granularity = .page,
                .batch = .{
                    .mode = if (task == .generate) .serial_compatibility else .native,
                    .preferred_items = 2,
                    .max_items = 8,
                    .max_encoded_bytes = 64 * 1024 * 1024,
                    .max_decoded_pixels = 80_000_000,
                    .max_media_parts_per_item = 1,
                    .per_item_failures = true,
                },
                .output = switch (task) {
                    .read => .read_result,
                    .generate => .generated_text,
                    .embed => .embedding,
                },
                .result_cardinality = .one_per_item,
                .prompt_policy = .explicit,
                .borrowed_attachments = true,
            };
        }

        fn embedDense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.UnexpectedIntegrationRoute;
        }

        fn embedSparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]embedder.SparseEmbedding {
            return error.UnexpectedIntegrationRoute;
        }

        fn embedParts(
            ptr: *anyopaque,
            result_alloc: std.mem.Allocator,
            model: []const u8,
            parts: []const template.ContentPart,
        ) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.embed_calls += 1;
            if (!std.mem.eql(u8, model, "clipclap-integration") or parts.len != 2)
                return error.InvalidIntegrationEmbeddingBatch;
            const vectors = try result_alloc.alloc([]f32, parts.len);
            var initialized: usize = 0;
            errdefer {
                for (vectors[0..initialized]) |vector| result_alloc.free(vector);
                result_alloc.free(vectors);
            }
            for (parts, vectors, 0..) |part, *vector, i| {
                if (part != .binary or !std.mem.eql(u8, part.binary.mime_type, "image/png"))
                    return error.InvalidIntegrationEmbeddingInput;
                vector.* = try result_alloc.dupe(f32, &.{ @as(f32, @floatFromInt(i + 1)), 0.5 });
                initialized += 1;
            }
            return vectors;
        }

        fn generateMessagesWithAttachments(
            ptr: *anyopaque,
            result_alloc: std.mem.Allocator,
            model: []const u8,
            _: []const inference_types.ChatMessage,
            attachments: []const inference_work.Attachment,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.generator_calls += 1;
            if (!std.mem.eql(u8, model, "gemma4-integration") or attachments.len != 1)
                return error.InvalidIntegrationGeneratorInput;
            try attachments[0].validate();
            if (!std.mem.eql(u8, attachments[0].content_type, "image/png"))
                return error.InvalidIntegrationGeneratorInput;
            return try result_alloc.dupe(u8, "generated page OCR");
        }

        fn readEncodedImages(
            ptr: *anyopaque,
            result_alloc: std.mem.Allocator,
            model: []const u8,
            request: readers.EncodedRequest,
        ) ![]readers.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.read_calls += 1;
            if (!std.mem.eql(u8, model, "florence2-integration"))
                return error.InvalidIntegrationReaderModel;
            if (request.images.len == 0 or request.images.len > 8 or request.prompt == null or
                !std.mem.eql(u8, request.prompt.?, "<OCR>"))
                return error.InvalidIntegrationReaderBatch;
            for (request.images) |image| {
                if (!std.mem.eql(u8, image.mime_type, "image/png") or image.bytes.len < 8 or
                    !std.mem.eql(u8, image.bytes[0..8], &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }))
                    return error.InvalidIntegrationReaderImage;
            }
            const results = try result_alloc.alloc(readers.Result, request.images.len);
            var initialized: usize = 0;
            errdefer {
                for (results[0..initialized]) |*result| readers.deinitResult(result_alloc, result);
                result_alloc.free(results);
            }
            for (request.images, results) |image, *result| {
                result.* = .{
                    .text = try result_alloc.dupe(u8, "page OCR"),
                    .item_id = if (image.item_id.len > 0) try result_alloc.dupe(u8, image.item_id) else "",
                    .source_fingerprint = if (image.source_fingerprint) |value| try result_alloc.dupe(u8, value) else null,
                    .page_number = image.page_number,
                };
                initialized += 1;
            }
            return results;
        }

        fn readEncodedImagesReported(
            ptr: *anyopaque,
            result_alloc: std.mem.Allocator,
            model: []const u8,
            request: readers.EncodedRequest,
        ) !readers.BatchResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const items = try readEncodedImages(ptr, result_alloc, model, request);
            if (self.read_calls == 3) return .{
                .items = items,
                .execution = .{
                    .requested_items = request.images.len,
                    .serial_items = request.images.len,
                    .fallback_items = request.images.len,
                    .fallback_reason = "integration_fallback",
                },
            };
            return .{
                .items = items,
                .execution = .{
                    .requested_items = request.images.len,
                    .native_batches = 1,
                    .native_items = request.images.len,
                },
            };
        }
    };

    var local = LocalFlorenceBoundary{};
    var runtime = asset_producer_runtime.Runtime.initWithOptions(
        alloc,
        &client,
        .{ .antfly_provider = local.provider() },
    );
    defer runtime.deinit();
    try enrichment_runtime.runNativePdfOcrCoordinatorIntegration(
        alloc,
        fixture.two_page_pdf,
        runtime.producer(),
    );
    if (local.read_calls != 1) return error.IntegrationReaderWasNotBatched;

    // Exercise the same bounded rendered-page window through the other two
    // task contracts. Gemma remains a generator; ClipClap receives one
    // independently addressable embedding item per rendered page.
    var session = try document_extraction.PdfRenderSession.init(alloc, fixture.two_page_pdf);
    defer session.deinit();
    try session.prepareForBatchRendering();
    var rendered = try session.renderPagesBatchAlloc(alloc, &.{
        .{ .page_number = 1, .max_output_bytes = 4 * 1024 * 1024 },
        .{ .page_number = 2, .max_output_bytes = 4 * 1024 * 1024 },
    }, .{
        .max_batch_pages = 2,
        .max_parallel_pages = 1,
        .max_inflight_pixels = 100_000_000,
        .max_inflight_bytes = 512 * 1024 * 1024,
        .max_retained_png_bytes = 8 * 1024 * 1024,
    });
    defer rendered.deinit(alloc);

    const producer = runtime.producer();
    var mixed_reader_media: [9][1]asset_producer.EncodedMedia = undefined;
    var mixed_reader_requests: [9]asset_producer.Request = undefined;
    for (&mixed_reader_requests, 0..) |*request, i| {
        const png = rendered.results[i % rendered.results.len].rendered orelse return error.IntegrationPageRenderFailed;
        mixed_reader_media[i][0] = .{ .bytes = png.png, .mime_type = "image/png" };
        request.* = .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"florence2-integration\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .item_id = "mixed-page",
            .source_fingerprint = if (i < 4) "mixed-doc-a" else "mixed-doc-b",
            .page_number = @intCast(i + 1),
            .media = &mixed_reader_media[i],
        };
    }
    var mixed_reader = try producer.produceBatchReported(alloc, &mixed_reader_requests);
    defer mixed_reader.deinit(alloc);
    if (mixed_reader.execution.native_batches != 1 or mixed_reader.execution.native_items != 8 or
        mixed_reader.execution.serial_items != 1 or mixed_reader.execution.fallback_items != 1 or
        mixed_reader.execution.fallback_reason == null or
        !std.mem.eql(u8, mixed_reader.execution.fallback_reason.?, "integration_fallback"))
    {
        return error.InvalidIntegrationMixedReaderExecutionReport;
    }

    var generator_media: [2][1]asset_producer.EncodedMedia = undefined;
    var generator_requests: [2]asset_producer.Request = undefined;
    for (rendered.results, 0..) |page, i| {
        const png = page.rendered orelse return page.failure orelse error.IntegrationPageRenderFailed;
        generator_media[i][0] = .{ .bytes = png.png, .mime_type = "image/png" };
        generator_requests[i] = .{
            .producer_type = .generator,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"gemma4-integration\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"Transcribe exactly\"}]",
            .inline_media_trusted = true,
            .item_id = if (i == 0) "page-1" else "page-2",
            .page_number = @intCast(i + 1),
            .media = &generator_media[i],
        };
    }
    var generated = try producer.produceBatchReported(alloc, &generator_requests);
    defer generated.deinit(alloc);
    if (generated.execution.serial_items != 2 or generated.execution.native_items != 0)
        return error.InvalidIntegrationGeneratorExecutionReport;
    for (generated.items) |item| switch (item.result) {
        .value => |output| if (!std.mem.eql(u8, output, "generated page OCR"))
            return error.InvalidIntegrationGeneratorOutput,
        .item_error => return error.InvalidIntegrationGeneratorOutput,
    };
    if (local.generator_calls != 2) return error.IntegrationGeneratorWasNotInvoked;

    const indexes_json =
        \\{"visual":{"type":"embeddings","field":"body","dimension":2,"embedder":{"provider":"antfly","model":"clipclap-integration","multimodal":true}}}
    ;
    var managed = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(alloc, indexes_json, local.provider());
    defer managed.deinit();
    var page_embeddings = try enrichment_runtime.embedRenderedPdfPageBatch(
        alloc,
        managed.denseInterface(),
        "visual",
        rendered,
        2,
    );
    defer page_embeddings.deinit(alloc);
    if (page_embeddings.results.len != 2 or local.embed_calls != 1)
        return error.IntegrationEmbedderWasNotBatched;
    for (page_embeddings.results, 1..) |page, expected_page| {
        if (page.failure != null or page.vector == null or page.page_number != expected_page)
            return error.InvalidIntegrationEmbeddingOutput;
    }
}

fn requiredEnv(name: [*:0]const u8) ![]const u8 {
    return if (std.c.getenv(name)) |value| std.mem.span(value) else error.MissingPdfQualificationEnvironment;
}

fn runRealModelQualification(alloc: std.mem.Allocator, client: *httpx.Client) !void {
    const inference_url = try requiredEnv("ANTFLY_PDF_QUALIFICATION_URL");
    const reader_model = try requiredEnv("ANTFLY_PDF_QUALIFICATION_READER_MODEL");
    const generator_model = try requiredEnv("ANTFLY_PDF_QUALIFICATION_GENERATOR_MODEL");
    const embedder_model = try requiredEnv("ANTFLY_PDF_QUALIFICATION_EMBEDDER_MODEL");
    const dims = try std.fmt.parseUnsigned(u32, try requiredEnv("ANTFLY_PDF_QUALIFICATION_EMBED_DIMS"), 10);

    var session = try document_extraction.PdfRenderSession.init(alloc, fixture.two_page_pdf);
    defer session.deinit();
    try session.prepareForBatchRendering();
    var rendered = try session.renderPagesBatchAlloc(alloc, &.{
        .{ .page_number = 1, .max_output_bytes = 8 * 1024 * 1024 },
        .{ .page_number = 2, .max_output_bytes = 8 * 1024 * 1024 },
    }, .{
        .max_batch_pages = 2,
        .max_parallel_pages = 1,
        .max_inflight_pixels = 100_000_000,
        .max_inflight_bytes = 512 * 1024 * 1024,
        .max_retained_png_bytes = 16 * 1024 * 1024,
    });
    defer rendered.deinit(alloc);

    var runtime = asset_producer_runtime.Runtime.initWithOptions(alloc, client, .{});
    defer runtime.deinit();
    const producer = runtime.producer();
    const reader_config = try std.json.Stringify.valueAlloc(alloc, .{
        .provider = "antfly",
        .model = reader_model,
        .url = inference_url,
        .prompt = "<OCR>",
    }, .{});
    defer alloc.free(reader_config);
    var reader_media: [2][1]asset_producer.EncodedMedia = undefined;
    var reader_requests: [2]asset_producer.Request = undefined;
    for (rendered.results, 0..) |page, i| {
        const png = page.rendered orelse return page.failure orelse error.IntegrationPageRenderFailed;
        reader_media[i][0] = .{ .bytes = png.png, .mime_type = "image/png" };
        reader_requests[i] = .{
            .producer_type = .reader,
            .config_json = reader_config,
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .item_id = if (i == 0) "page-1" else "page-2",
            .page_number = @intCast(i + 1),
            .media = &reader_media[i],
        };
    }
    var read = try producer.produceBatchReported(alloc, &reader_requests);
    defer read.deinit(alloc);
    if (read.items.len != 2) return error.InvalidQualificationReaderCardinality;
    for (read.items) |item| switch (item.result) {
        .value => |value| if (std.mem.trim(u8, value, &std.ascii.whitespace).len == 0)
            return error.EmptyQualificationReaderOutput,
        .item_error => return error.EmptyQualificationReaderOutput,
    };

    const generator_config = try std.json.Stringify.valueAlloc(alloc, .{
        .provider = "antfly",
        .model = generator_model,
        .url = inference_url,
    }, .{});
    defer alloc.free(generator_config);
    var generator_media: [2][1]asset_producer.EncodedMedia = undefined;
    var generator_requests: [2]asset_producer.Request = undefined;
    for (rendered.results, 0..) |page, i| {
        const png = page.rendered orelse return page.failure orelse error.IntegrationPageRenderFailed;
        generator_media[i][0] = .{ .bytes = png.png, .mime_type = "image/png" };
        generator_requests[i] = .{
            .producer_type = .generator,
            .config_json = generator_config,
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"Transcribe the page exactly.\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .item_id = if (i == 0) "page-1" else "page-2",
            .page_number = @intCast(i + 1),
            .media = &generator_media[i],
        };
    }
    var generated = try producer.produceBatchReported(alloc, &generator_requests);
    defer generated.deinit(alloc);
    if (generated.items.len != 2) return error.InvalidQualificationGeneratorCardinality;
    for (generated.items) |item| switch (item.result) {
        .value => |value| if (std.mem.trim(u8, value, &std.ascii.whitespace).len == 0)
            return error.EmptyQualificationGeneratorOutput,
        .item_error => return error.EmptyQualificationGeneratorOutput,
    };

    const indexes_json = try std.json.Stringify.valueAlloc(alloc, .{
        .visual = .{
            .type = "embeddings",
            .field = "body",
            .dimension = dims,
            .embedder = .{
                .provider = "antfly",
                .model = embedder_model,
                .url = inference_url,
                .multimodal = true,
            },
        },
    }, .{});
    defer alloc.free(indexes_json);
    var managed = try managed_embedder.ManagedEmbedder.initFromIndexesJson(alloc, indexes_json);
    defer managed.deinit();
    var embeddings = try enrichment_runtime.embedRenderedPdfPageBatch(
        alloc,
        managed.denseInterface(),
        "visual",
        rendered,
        dims,
    );
    defer embeddings.deinit(alloc);
    if (embeddings.results.len != 2) return error.InvalidQualificationEmbeddingCardinality;
    for (embeddings.results) |page| {
        if (page.failure != null or page.vector == null or page.vector.?.len != dims)
            return error.InvalidQualificationEmbeddingOutput;
    }
}
