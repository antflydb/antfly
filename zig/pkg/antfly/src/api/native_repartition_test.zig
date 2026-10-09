// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const local = @import("antfly_local_sources");

test "external lake portable native cursor recovers document and vector generations on fresh and repartitioned owners" {
    for ([_]bool{ false, true }) |repartitioned| {
        for ([_]bool{ false, true }) |vector_store| {
            const a = std.testing.allocator;
            var repository_directory = try local.common_test_directory.TestDirectory.init("native-cursor-repository");
            defer repository_directory.cleanup();
            var repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
            var repository_open = true;
            defer if (repository_open) repository.deinit();
            var donor_directory = try local.common_test_directory.TestDirectory.init("native-cursor-donor");
            defer donor_directory.cleanup();
            const donor_pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{donor_directory.path()});
            defer a.free(donor_pins);
            defer std.Io.Dir.cwd().deleteTree(std.testing.io, donor_pins) catch {};
            const DB = local.storage_db_db.DB;
            const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
            var donor = try DB.open(a, donor_directory.path(), .{ .identity_namespace = namespace });
            var donor_open = true;
            defer if (donor_open) donor.close();
            donor.backend_runtime.query_cut_repository = repository.capability();
            if (vector_store) try donor.configureTableStorage(.{ .dense_embeddings = .vector_store });
            try donor.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2,\"metric\":\"l2_squared\"}" });
            try donor.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"original\",\"v\":[1,0]}" }}, .sync_level = .full_index });
            const id: [64]u8 = @splat('c');
            var request: local.storage_db_native_query_cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = local.storage_db_native_query_cut.nowMs() + 60_000, .create = true };
            var foreign_capture = request;
            foreign_capture.origin = .{ .table_id = 7, .shard_id = 2, .range_id = 3 };
            try std.testing.expectError(error.CatalogGenerationChanged, donor.captureQueryCut(foreign_capture, .none));
            try donor.captureQueryCut(request, .none);
            donor.close();
            donor_open = false;
            try std.Io.Dir.cwd().deleteTree(std.testing.io, donor_pins);
            var receiver_directory = try local.common_test_directory.TestDirectory.init("native-cursor-receiver");
            defer receiver_directory.cleanup();
            const receiver_pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{receiver_directory.path()});
            defer a.free(receiver_pins);
            defer std.Io.Dir.cwd().deleteTree(std.testing.io, receiver_pins) catch {};
            const replacement = if (repartitioned) local.storage_db_doc_identity_namespace.Namespace{ .table_id = 7, .shard_id = 2, .range_id = 3 } else namespace;
            var receiver = try DB.open(a, receiver_directory.path(), .{ .identity_namespace = replacement });
            defer receiver.close();
            receiver.backend_runtime.query_cut_repository = repository.capability();
            try receiver.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"live replacement\"}" }}, .sync_level = .full_index });
            request.create = false;
            request.origin = namespace;
            {
                var retained = try receiver.openQueryCut(request, .none);
                defer retained.close();
                const value = (try retained.get(a, "doc")).?;
                defer a.free(value);
                try std.testing.expect(std.mem.indexOf(u8, value, "original") != null);
                var result = try retained.search(a, .{ .index_name = "semantic", .dense = .{ .vector = &.{ 1, 0 }, .k = 1 }, .limit = 1 });
                defer result.deinit();
                try std.testing.expectEqualStrings("doc", result.hits[0].id);
                try std.testing.expectEqual(@as(?f32, 0), result.hits[0].distance);
            }
            try std.Io.Dir.cwd().deleteTree(std.testing.io, receiver_pins);
            repository.deinit();
            repository_open = false;
            // Reopen an empty durable destination: a cursor never recaptures live
            // data when the authoritative manifest has been lost.
            try std.Io.Dir.cwd().deleteTree(std.testing.io, repository_directory.path());
            repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
            repository_open = true;
            receiver.backend_runtime.query_cut_repository = repository.capability();
            request.timeout_ms = 1000;
            try std.testing.expectError(error.CatalogGenerationChanged, receiver.openQueryCut(request, .none));
            // Recovery errors must release the parent lock for the next request.
            try std.testing.expectError(error.CatalogGenerationChanged, receiver.openQueryCut(request, .none));
        }
    }
}
