// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const rewrite = @import("relational_rewrite_staging.zig");
const contract = @import("relational_rewrite_contract.zig");
const staging = @import("restore_staging.zig");
const db_mod = @import("db.zig");
const retained = @import("../retained_effects.zig");
const alloc = std.testing.allocator;
const source_schema =
    \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"add","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":1}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"g":{"type":"integer"}},"required":["x","g"],"additionalProperties":false}}}}
;
const target_schema =
    \\{"version":2,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"multiply","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":3}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"g":{"type":"integer"}},"required":["x","g"],"additionalProperties":false}}}}
;

fn apply(target: *db_mod.DB, value: *const staging.PreparedPage, index: u64) !void {
    const effects = @import("../hot_standby/effects.zig");
    const encoded = try effects.encodeBatchMutationRequestAlloc(alloc, value.batch.?);
    defer alloc.free(encoded);
    var decoded = try effects.decodeBatchMutationRequest(alloc, .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = index, .previous_lsn = index - 1, .payload = encoded });
    defer decoded.deinit();
    try std.testing.expectEqualDeep(value.batch.?.restore_staging, decoded.value.request.restore_staging);
    try target.batchRaftReplicatedApply(decoded.value.request, .{ .term = 1, .index = index });
}

test "relational index system rewrite shared staging preserves post-cut updates deletes partial frames and replay across reopen" {
    try runRewrite(false);
}

test "relational index system rewrite unchanged document cohort preserves snapshot tail timestamps and recovery" {
    try runRewrite(true);
}

fn runRewrite(preserve_document: bool) !void {
    const document_schema = "{\"version\":1,\"storage_mode\":\"document\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"}},\"required\":[\"x\"],\"additionalProperties\":false}}}}";
    const from_schema = if (preserve_document) document_schema else source_schema;
    const to_schema = if (preserve_document) document_schema else target_schema;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const source_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source", .{directory.sub_path});
    const target_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/target", .{directory.sub_path});
    // Routing group12 owns a pre-split logical identity112/113. Rewriting
    // preserves this source namespace instead of conflating it with routing.
    const source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 11, .shard_id = 112, .range_id = 113 }, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 21, .shard_id = 22, .range_id = 22 }, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
    var programs = if (preserve_document) try rewrite.ProgramSet.initDocumentPreservation(alloc, document_schema) else try rewrite.ProgramSet.init(alloc, &.{from_schema}, to_schema, .{});
    defer programs.deinit();
    var source = try db_mod.DB.open(alloc, source_path, source_options);
    var source_open = true;
    defer if (source_open) source.close();
    try source.setSchemaJson(alloc, from_schema);
    try source.batchRaftReplicatedApply(.{ .timestamp_ns = 101, .writes = &.{ .{ .key = "a", .value = "{\"x\":2}" }, .{ .key = "b", .value = "{\"x\":4}" } } }, .{ .term = 1, .index = 1 });
    const owner = try source.relationalTopologyIdentity();
    const source_scope: @import("online_source_contract.zig").Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 1, .attempt = 1, .admission_epoch = owner.next_epoch, .owner_group_id = 12, .peer_group_id = 22, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest },
        .receiver_namespace = target_options.identity_namespace.?,
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    try (@import("online_merge_io_contract.zig").Request{ .scope = source_scope, .operation = .{ .status = .donor } }).validate();
    try std.testing.expectEqualDeep(source_scope, try @import("online_source_contract.zig").Scope.decode(&try source_scope.encode()));
    var merge_alias = source_scope;
    merge_alias.fence.role = .merge_source;
    merge_alias.receiver_namespace.table_id = source_scope.fence.namespace.table_id;
    try std.testing.expectError(error.InvalidOnlineSourceCommand, merge_alias.validate());
    try std.testing.expectError(error.InvalidOnlineSourceCommand, (@import("online_merge_io_contract.zig").Request{ .scope = source_scope, .operation = .{ .status = .receiver } }).validate());
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = source_scope, .limit = retained.default_limit } } }, .{ .term = 1, .index = 2 });
    const start = (try source.onlineSourceStatus(source_scope)).start;
    const certificate = try source.prepareOnlineSourcePublication(source_scope, .none);
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = source_scope, .certificate = certificate } } }, .{ .term = 1, .index = 3 });
    // Restart after durable admission/publication; never recapture the source.
    source.close();
    source_open = false;
    source = try db_mod.DB.open(alloc, source_path, source_options);
    source_open = true;
    try std.testing.expect(certificate.eql((try source.onlineSourceStatus(source_scope)).published_certificate.?));
    const pin_root = try @import("source_pin.zig").pathAlloc(a, source_path, source_scope);
    const artifact_path = try std.fmt.allocPrint(a, "{s}/source.afb2", .{pin_root});
    const decoder_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/decoder", .{directory.sub_path});
    {
        var decoder = try db_mod.DB.open(alloc, decoder_path, source_options);
        defer decoder.close();
        const proof: @import("../portable_backup.zig").SourceCopyProof = .{ .scope = source_scope, .applied_index = certificate.cut.applied_index, .retained_start = start };
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, artifact_path, .{});
        defer file.close(std.testing.io);
        const size = (try file.stat(std.testing.io)).size;
        for (0..1000) |_| {
            if (try @import("../portable_backup.zig").importSourceCopyFilePage(alloc, decoder.core.store, std.testing.io, file, size, proof, source_scope.pin(), 1, .none)) break;
        } else return error.TestUnexpectedResult;
        try @import("../portable_backup.zig").validateCompleteSourceCopyImage(alloc, decoder.core.store, proof);
    }
    var readonly = source_options;
    readonly.open_mode = .query_readonly;
    readonly.primary_only_readonly = true;
    var decoder = try db_mod.DB.open(alloc, decoder_path, readonly);
    defer decoder.close();
    var target = try db_mod.DB.open(alloc, target_path, target_options);
    var target_open = true;
    defer if (target_open) target.close();
    try target.setSchemaJson(alloc, to_schema);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = programs.target_runtime_digest, .rewrite = .{ .program_digest = programs.identity, .retained_pin = source_scope.pin(), .snapshot_certificate = try certificate.digest(), .retained_epoch = 1, .retained_start = start, .source_applied_index = certificate.cut.applied_index, .source_scope = source_scope } };
    try target.reserveRestoreStagingScoped(alloc, scope);
    try target.batchRaftReplicatedApply(.{ .restore_staging = .{ .begin = scope } }, .{ .term = 1, .index = 1 });
    try std.testing.expectError(error.InvalidRestoreStagingCommand, target.prepareRestoreStagingPage(alloc, scope, &decoder, 1, .none));
    var first = try target.prepareRewriteStagingPage(alloc, scope, &decoder, 1, .none, &programs);
    defer first.deinit();
    try apply(&target, &first, 2);
    var last = try target.prepareRewriteStagingPage(alloc, scope, &decoder, 128, .none, &programs);
    defer last.deinit();
    // An acknowledged source transaction occurs after the immutable snapshot
    // page was prepared, but before the receiver has applied that last page.
    try source.batchRaftReplicatedApply(.{ .timestamp_ns = 202, .writes = &.{ .{ .key = "a", .value = "{\"x\":9}" }, .{ .key = "c", .value = "{\"x\":5}" } }, .deletes = &.{"b"} }, .{ .term = 1, .index = 4 });
    try apply(&target, &last, 3);
    try apply(&target, &last, 3); // Lost response, identical committed command.
    try std.testing.expectError(error.RestoreStagingProgressChanged, apply(&target, &first, 4));
    try source.batchRaftReplicatedApply(.{ .relational_topology = .{ .fence = source_scope.fence, .action = .begin } }, .{ .term = 1, .index = 5 });
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .final_fence = .{ .scope = source_scope, .expected_sequence = start + 1 } } }, .{ .term = 1, .index = 6 });
    const final_status = try source.onlineSourceStatus(source_scope);
    const cut: contract.FinalCut = .{ .sequence = final_status.through_sequence, .applied_index = final_status.applied_index, .digest = final_status.cut_digest };
    const final_receipt = try contract.FinalReceipt.fromProgress(scope.rewrite.?, final_status);
    try std.testing.expectEqualDeep(cut, final_receipt.cut);
    var wrong_receipt = final_receipt;
    wrong_receipt.certificate_digest[0] ^= 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, wrong_receipt.validate(scope.rewrite.?));
    wrong_receipt = final_receipt;
    wrong_receipt.cut.sequence += 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, wrong_receipt.validate(scope.rewrite.?));
    var unfenced = final_status;
    unfenced.phase = .retaining;
    try std.testing.expectError(error.RestoreStagingScopeChanged, contract.FinalReceipt.fromProgress(scope.rewrite.?, unfenced));
    try std.testing.expectError(error.RestoreStagingInProgress, rewrite.prepareFinish(&target, alloc, scope, cut));
    {
        var canceled = std.atomic.Value(bool).init(true);
        try std.testing.expectError(error.Canceled, rewrite.prepareTail(&target, alloc, scope, &source, &programs, 1, @import("types.zig").CancellationToken.fromAtomic(&canceled)));
        const frame_key = retained.recordKey(start + 1);
        const original = try source.core.store.get(a, &frame_key);
        const corrupt = try a.dupe(u8, original);
        corrupt[corrupt.len - 1] ^= 1;
        try source.core.store.putBatch(&.{.{ .key = &frame_key, .value = corrupt }}, &.{});
        try std.testing.expectError(error.RetainedEffectsCorrupt, rewrite.prepareTail(&target, alloc, scope, &source, &programs, 1, .none));
        try source.core.store.putBatch(&.{.{ .key = &frame_key, .value = original }}, &.{});
        const unchanged = (try target.restoreStagingStatus(alloc)).?;
        defer unchanged.deinit();
        try std.testing.expectEqual(start, unchanged.value.rewrite.?.sequence);
        try std.testing.expectEqual(@as(u32, 0), unchanged.value.rewrite.?.frame_offset);
    }
    var index: u64 = 4;
    var partial_seen = false;
    var wire_frame: std.ArrayList(u8) = .empty;
    defer wire_frame.deinit(alloc);
    const spool_root = try std.fmt.allocPrint(a, "{s}.spool", .{target_path});
    var frame_digest: [32]u8 = undefined;
    for (0..10000) |_| {
        const wire_json = try @import("online_merge_io.zig").executeJson(&source, alloc, .{ .scope = source_scope, .operation = .{ .rewrite_tail = .{ .after = start, .offset = @intCast(wire_frame.items.len), .max_bytes = 7 } } }, .none);
        defer alloc.free(wire_json);
        var chunk = try std.json.parseFromSlice(?contract.TailChunk, alloc, wire_json, .{});
        defer chunk.deinit();
        const value = chunk.value orelse return error.TestUnexpectedResult;
        try value.validate();
        try std.testing.expectEqualSlices(u8, &source_scope.pin(), &value.pin);
        try std.testing.expectEqual(start + 1, value.sequence);
        try std.testing.expectEqual(wire_frame.items.len, value.offset);
        if (wire_frame.items.len != 0) try std.testing.expectEqualSlices(u8, &frame_digest, &value.frame_digest);
        frame_digest = value.frame_digest;
        const spooled = try @import("../rewrite_tail_spool.zig").receive(alloc, std.testing.io, spool_root, scope, start, value);
        defer spooled.deinit(alloc);
        const repeated = try @import("../rewrite_tail_spool.zig").receive(alloc, std.testing.io, spool_root, scope, start, value);
        defer repeated.deinit(alloc);
        try std.testing.expectEqual(value.offset + value.data.len, spooled.next);
        try std.testing.expectEqual(spooled.next, repeated.next);
        try std.testing.expectEqual(if (spooled.next == value.total) spooled.next - 1 else spooled.next, try @import("../rewrite_tail_spool.zig").resumeOffset(alloc, std.testing.io, spool_root, scope, start));
        try wire_frame.appendSlice(alloc, value.data);
        if (wire_frame.items.len == value.total) {
            try std.testing.expectEqualSlices(u8, wire_frame.items, spooled.frame.?);
            try std.testing.expectEqualSlices(u8, wire_frame.items, repeated.frame.?);
            break;
        }
    } else return error.TestUnexpectedResult;
    try std.testing.expectError(error.RetainedEffectsCorrupt, rewrite.prepareTailFrame(&target, alloc, scope, wire_frame.items, @splat(1), &programs, 1, .none));
    for (0..16) |_| {
        const before_tail = (try target.restoreStagingStatus(alloc)).?;
        const complete = before_tail.value.rewrite.?.sequence == cut.sequence;
        before_tail.deinit();
        if (complete) break;
        var tail = try rewrite.prepareTailFrame(&target, alloc, scope, wire_frame.items, frame_digest, &programs, 1, .none);
        defer tail.deinit();
        if (tail.batch == null) break;
        try apply(&target, &tail, index);
        try apply(&target, &tail, index);
        index += 1;
        const status = (try target.restoreStagingStatus(alloc)).?;
        partial_seen = partial_seen or status.value.rewrite.?.frame_offset != 0;
        if (status.value.rewrite.?.frame_offset != 0) try std.testing.expectEqual(start, status.value.rewrite.?.sequence);
        status.deinit();
        target.close();
        target_open = false;
        target = try db_mod.DB.open(alloc, target_path, target_options);
        target_open = true;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(partial_seen);
    var finish = try rewrite.prepareFinish(&target, alloc, scope, final_receipt.cut);
    defer finish.deinit();
    try apply(&target, &finish, index);
    var repeated_finish = try rewrite.prepareFinish(&target, alloc, scope, final_receipt.cut);
    defer repeated_finish.deinit();
    try std.testing.expect(repeated_finish.batch == null);
    try std.testing.expectEqual(@as(u32, 0), try @import("../rewrite_tail_spool.zig").resumeOffset(alloc, std.testing.io, spool_root, scope, cut.sequence));
    _ = try target.finishRestoreStaging(alloc, scope.digest(), .validated);
    _ = try target.finishRestoreStaging(alloc, scope.digest(), .published);
    const a_json = (try target.get(alloc, "a")).?;
    defer alloc.free(a_json);
    const c_json = (try target.get(alloc, "c")).?;
    defer alloc.free(c_json);
    const parsed_a = try std.json.parseFromSlice(std.json.Value, alloc, a_json, .{});
    defer parsed_a.deinit();
    const parsed_c = try std.json.parseFromSlice(std.json.Value, alloc, c_json, .{});
    defer parsed_c.deinit();
    if (preserve_document) {
        try std.testing.expectEqualStrings("{\"x\":9}", a_json);
        try std.testing.expectEqualStrings("{\"x\":5}", c_json);
    } else {
        try std.testing.expectEqual(@as(i64, 27), parsed_a.value.object.get("g").?.integer);
        try std.testing.expectEqual(@as(i64, 15), parsed_c.value.object.get("g").?.integer);
    }
    try std.testing.expectEqual(@as(?[]u8, null), try target.get(alloc, "b"));
    try std.testing.expectEqual(@as(u64, 202), try target.getTimestamp(alloc, "a"));
    target.close();
    target_open = false;
    target = try db_mod.DB.open(alloc, target_path, target_options);
    target_open = true;
    const status = (try target.restoreStagingStatus(alloc)).?;
    defer status.deinit();
    try std.testing.expectEqual(.published, status.value.phase);
    try std.testing.expectEqual(cut, status.value.rewrite.?.final_cut.?);
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .release = source_scope } }, .{ .term = 1, .index = 7 });
    try std.testing.expectEqual(.released, (try source.onlineSourceStatus(source_scope)).phase);
}

test "relational index system rewrite immutable program set binds policy and all historical schema identities" {
    var programs = try rewrite.ProgramSet.init(alloc, &.{source_schema}, target_schema, .{});
    defer programs.deinit();
    var intent: contract.Intent = .{ .source_schemas = &.{source_schema}, .target_schema = target_schema, .program_digest = programs.identity };
    var reopened = try rewrite.ProgramSet.initIntent(alloc, intent);
    defer reopened.deinit();
    try std.testing.expectEqualSlices(u8, &programs.identity, &reopened.identity);
    intent.apply_defaults_to_absent = true;
    try std.testing.expectError(error.RestoreStagingScopeChanged, rewrite.ProgramSet.initIntent(alloc, intent));
    try std.testing.expectError(error.InvalidRestoreStagingCommand, rewrite.ProgramSet.init(alloc, &.{ source_schema, source_schema }, target_schema, .{}));
    const historical = try std.mem.replaceOwned(u8, alloc, source_schema, "\"version\":1", "\"version\":3");
    defer alloc.free(historical);
    var versions = try rewrite.ProgramSet.init(alloc, &.{ historical, source_schema }, target_schema, .{});
    defer versions.deinit();
    var reordered = try rewrite.ProgramSet.init(alloc, &.{ source_schema, historical }, target_schema, .{});
    defer reordered.deinit();
    try std.testing.expectEqualSlices(u8, &versions.identity, &reordered.identity);
    try std.testing.expect(versions.programs[0].target.epoch == versions.programs[1].target.epoch);
    try std.testing.expect(versions.programs[0].source.epoch != versions.programs[1].source.epoch);
}
