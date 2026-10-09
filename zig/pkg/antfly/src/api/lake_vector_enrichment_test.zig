// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const local = @import("antfly_local_sources");
const enrichment = @import("lake_vector_enrichment.zig");
const managed = local.inference_managed_embedder;

test "external lake managed embedding completion is reused after restart and recipe changes recompute" {
    const Provider = struct {
        calls: usize = 0,
        fn dense(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const vectors = try a.alloc([]f32, texts.len);
            for (vectors) |*vector| vector.* = try a.dupe(f32, &.{ 1, 0 });
            return vectors;
        }
        fn controlled(raw: *anyopaque, a: std.mem.Allocator, model: []const u8, texts: []const []const u8, _: managed.EmbeddingRequestContext) ![][]f32 {
            return dense(raw, a, model, texts);
        }
        fn sparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]local.storage_db_enrichment_embedder.SparseEmbedding {
            return error.TestUnexpectedResult;
        }
    };
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-managed-enrichment");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    var open = true;
    defer if (open) store.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const declaration = try std.json.parseFromSliceLeaky(std.json.Value, scratch,
        \\{"type":"embeddings","field":"body","dimension":2,"embedder":{"provider":"antfly","model":"local-model"}}
    , .{});
    var provider: Provider = .{};
    const options: managed.InitOptions = .{ .io = std.testing.io, .antfly_provider = .{ .ptr = &provider, .embed_dense_texts = Provider.dense, .embed_dense_texts_with_context = Provider.controlled, .embed_sparse_texts = Provider.sparse } };
    var producer = try enrichment.Producer.init(a, "semantic", "body", declaration, options);
    producer.memo = .{ .store = &store, .table_id = 7, .recipe = @splat(1), .context = .{ .io = std.testing.io } };
    const input: std.json.Value = .{ .string = "document" };
    const first = (try producer.dense(scratch, input, 2)).?;
    try std.testing.expectEqual(@as(f32, 1), first[0]);
    const completed_calls = provider.calls;
    try std.testing.expect(completed_calls > 0);
    producer.deinit();
    store.deinit();
    open = false;
    var reopened = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    defer reopened.deinit();
    var resumed = try enrichment.Producer.init(a, "semantic", "body", declaration, options);
    defer resumed.deinit();
    resumed.memo = .{ .store = &reopened, .table_id = 7, .recipe = @splat(1), .context = .{ .io = std.testing.io } };
    const before_resume = provider.calls;
    _ = try resumed.dense(scratch, input, 2);
    try std.testing.expectEqual(before_resume, provider.calls);
    resumed.memo.?.recipe = @splat(2);
    _ = try resumed.dense(scratch, input, 2);
    try std.testing.expectEqual(before_resume + 1, provider.calls);
    resumed.memo.?.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, resumed.dense(scratch, .{ .string = "different document" }, 2));
    try std.testing.expectEqual(before_resume + 1, provider.calls);
}
