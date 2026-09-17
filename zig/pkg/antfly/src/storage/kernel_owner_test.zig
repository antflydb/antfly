// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const local_query_client = @import("local_query_client");
const client = @import("kernel_owner_client.zig");
const wal_client = @import("kernel_wal_client.zig");
const data_apply_client = @import("data_raft_apply_client.zig");
const metadata_apply_client = @import("metadata_raft_apply_client.zig");

test "opaque owner standalone rewrite authority is durable and cannot be selected by a request" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-72/table-db", .{root});
    defer alloc.free(path);
    var context: client.Context = .{};
    try context.ensure();
    defer context.deinit();
    var options: abi.OpenRequest = .{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("rows"),
        .group_id = 72,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 172,
        .identity_range_id = 272,
        .online_source_authority = 2,
        .schema_json = .fromSlice(
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ),
    };
    const wire = @import("db/online_merge_io_contract.zig");
    const request: wire.Request = .{
        .scope = .{ .authority = .native, .fence = .{ .role = .rewrite_source, .transition_id = 1, .attempt = 0, .admission_epoch = 0, .owner_group_id = 72, .peer_group_id = 82, .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .catalog_digest = @splat(0) }, .receiver_namespace = .{ .table_id = 8, .shard_id = 82, .range_id = 82 }, .consumer_epoch = 0, .copy_attempt = .{} },
        .operation = .{ .admission = .donor },
    };
    for (0..2) |_| {
        var owner = try client.Owner.open(options);
        defer owner.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(encoded);
        var response = try owner.onlineMergeIoJson(.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(encoded) });
        defer response.deinit();
        const facts = try std.json.parseFromSlice(wire.AdmissionFacts, alloc, response.bytes(), .{});
        defer facts.deinit();
        try std.testing.expectEqual(@import("db/online_source_contract.zig").Authority.native, facts.value.authority);
        try std.testing.expectEqual(@as(u64, 0), facts.value.donor_term);
        try std.testing.expect(facts.value.eligible);
        try std.testing.expectEqual(@as(usize, 1), facts.value.source_schemas.len);
        var forged = request;
        forged.scope.authority = .raft;
        const wrong = try std.json.Stringify.valueAlloc(alloc, forged, .{});
        defer alloc.free(wrong);
        try std.testing.expectError(error.OnlineSourceScopeChanged, owner.onlineMergeIoJson(.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(wrong) }));
    }
    options.online_source_authority = 1;
    try std.testing.expectError(error.OnlineSourceScopeChanged, client.Owner.open(options));
}

test "opaque owner cold reopen preserves frozen document and relational backup cohorts" {
    const alloc = std.testing.allocator;
    const topology = @import("db/relational_integrity_topology_contract.zig");
    const json = @import("db/relational_integrity_handoff_contract.zig");
    const transition = @import("db/relational_transition_contract.zig");
    const seal = @import("db/native_backup_seal_contract.zig");
    const Read = struct {
        fn call(comptime T: type, handle: ?*anyopaque, request: transition.Request) !std.json.Parsed(T) {
            const body = try json.encode(std.testing.allocator, request);
            defer std.testing.allocator.free(body);
            var response: abi.OwnedBytes = .{};
            try error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(handle, &.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(body) }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            return std.json.parseFromSlice(T, std.testing.allocator, response.slice(), .{ .allocate = .alloc_always });
        }
    };
    inline for (.{ false, true }) |relational| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
        defer alloc.free(root);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-7117/table-db", .{root});
        defer alloc.free(path);
        const schema = if (relational)
            \\{"version":0,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        else
            \\{"version":0,"storage_mode":"document","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        const indexes = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}";
        var context = client.Context{};
        try context.ensure();
        defer context.deinit();
        const options: abi.OpenRequest = .{
            .context = context.handle,
            .path = .fromSlice(path),
            .table_name = .fromSlice("rows"),
            .group_id = 7117,
            .has_identity_namespace = 1,
            .identity_table_id = 71,
            .identity_shard_id = 7117,
            .identity_range_id = 7117,
            .has_initial_range = 1,
            .initial_range_start = .fromSlice("a"),
            .initial_range_end = .fromSlice("m"),
            .schema_json = .fromSlice(schema),
            .indexes_json = .fromSlice(indexes),
        };
        var owner = try client.Owner.open(options);
        var owner_open = true;
        defer if (owner_open) owner.deinit();
        var inserted = try owner.batchJson("rows", "{\"inserts\":{\"a\":{\"id\":1}},\"sync_level\":\"write\"}");
        inserted.deinit();
        var identity = try Read.call(topology.Identity, owner.handle, .identity);
        defer identity.deinit();
        const fence: topology.Fence = .{
            .admission_epoch = identity.value.next_epoch,
            .transition_id = 117,
            .attempt = 1,
            .peer_group_id = 7117,
            .owner_group_id = 7117,
            .role = .backup_snapshot,
            .namespace = identity.value.namespace,
            .catalog_digest = identity.value.catalog_digest,
        };
        const begin = try json.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .begin, .fence = fence } });
        defer alloc.free(begin);
        var frozen = try owner.replicatedBatchAtRaftEntryJson("rows", begin, 1, 1);
        frozen.deinit();
        var before = try Read.call(topology.Status, owner.handle, .status);
        defer before.deinit();
        try std.testing.expect(before.value.drained and before.value.fence.?.eql(fence));
        owner.deinit();
        owner_open = false;

        // Production backup routing may fault in a cold owner between freeze
        // and pin. Identical metadata rehydration must not become a write.
        owner = try client.Owner.open(options);
        owner_open = true;
        try owner.configure("rows", schema, indexes);
        var after = try Read.call(topology.Status, owner.handle, .status);
        defer after.deinit();
        try std.testing.expect(after.value.drained and after.value.fence.?.eql(fence));
        try std.testing.expectError(error.IntegrityTopologyBusy, owner.batchJson("rows", "{\"inserts\":{\"b\":{\"id\":2}}}"));
        const changed_schema = try std.mem.replaceOwned(u8, alloc, schema, "\"version\":0", "\"version\":1");
        defer alloc.free(changed_schema);
        try std.testing.expectError(error.IntegrityTopologyBusy, owner.configure("rows", changed_schema, indexes));
        var wrong = fence;
        wrong.attempt += 1;
        const wrong_request = try json.encode(alloc, seal.Request{ .seal = .{ .id = "wrong", .fence = wrong } });
        defer alloc.free(wrong_request);
        try std.testing.expectError(error.IntegrityTopologyChanged, owner.backupPinControlJson(.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(wrong_request) }));
        const request = try json.encode(alloc, seal.Request{ .seal = .{ .id = "cut", .fence = fence } });
        defer alloc.free(request);
        var pinned = try owner.backupPinControlJson(.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(request) });
        defer pinned.deinit();
        var handle = try std.json.parseFromSlice(seal.Handle, alloc, pinned.bytes(), .{});
        defer handle.deinit();
        try std.testing.expect(handle.value.fence.eql(fence));
        var retried = try owner.backupPinControlJson(.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(request) });
        defer retried.deinit();
        try std.testing.expectEqualStrings(pinned.bytes(), retried.bytes());
        const release = try json.encode(alloc, seal.Request{ .release = handle.value });
        defer alloc.free(release);
        var released = try owner.backupPinControlJson(.{ .table_name = .fromSlice("rows"), .request_json = .fromSlice(release) });
        released.deinit();
        const cancel = try json.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .cancel, .fence = fence } });
        defer alloc.free(cancel);
        var canceled = try owner.replicatedBatchAtRaftEntryJson("rows", cancel, 1, 2);
        canceled.deinit();
        var resumed = try owner.batchJson("rows", "{\"inserts\":{\"b\":{\"id\":2}}}");
        resumed.deinit();
    }
}

test "local query identity relay preserves origin and attributes protocol defects to consumer" {
    const failure = error_identity.failureFromError(
        error.InvalidQueryRequest,
        .local_query,
        abi.abi_version,
        @intFromEnum(abi.LocalQueryOperation.parse_internal_request),
    );
    var forwarded: abi.FailureIdentity = .{};
    try local_query_client.acceptProviderFailure(
        failure.status,
        failure,
        .validate_provider_response,
        &forwarded,
    );
    try std.testing.expectEqualDeep(failure, forwarded);

    @import("../test_error_logs.zig").expectErrorLogs(1);
    var malformed = failure;
    malformed.operation = 0;
    var replacement: abi.FailureIdentity = .{};
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        local_query_client.acceptProviderFailure(
            malformed.status,
            malformed,
            .validate_provider_response,
            &replacement,
        ),
    );
    try std.testing.expectEqual(abi.Status.invalid_boundary_failure_identity, replacement.status);
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, replacement.boundary);
    try std.testing.expectEqual(abi.abi_version, replacement.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.validate_provider_response),
        replacement.operation,
    );
    try std.testing.expectEqualStrings("InvalidBoundaryFailureIdentity", replacement.errorName());
}

test "opaque owner source artifact transfer resumes across replicas without donor inode authority" {
    const alloc = std.testing.allocator;
    const transfer = @import("db/source_artifact_transfer.zig");
    const contract = @import("../api/local_query_contract.zig");
    const batch = @import("../api/batch.zig");
    const source = @import("db/online_source_contract.zig");
    const Call = struct {
        fn transport(ptr: *anyopaque, allocator: std.mem.Allocator, group: u64, table: []const u8, request: transfer.Request, context: @import("../api/operation.zig").RequestContext) ![]u8 {
            try context.ensureActive();
            try std.testing.expectEqual(@as(u64, 7201), group);
            try std.testing.expectEqualStrings("docs", table);
            const owner: *client.Owner = @ptrCast(@alignCast(ptr));
            const json = try std.json.Stringify.valueAlloc(allocator, request, .{});
            defer allocator.free(json);
            var response = try owner.sourceArtifactJson(.{ .table_name = .fromSlice(table), .request_json = .fromSlice(json) });
            defer response.deinit();
            return allocator.dupe(u8, response.bytes());
        }
        fn run(comptime T: type, owner: *client.Owner, request: transfer.Request) !std.json.Parsed(T) {
            const json = try std.json.Stringify.valueAlloc(std.testing.allocator, request, .{});
            defer std.testing.allocator.free(json);
            var response = try owner.sourceArtifactJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(json) });
            defer response.deinit();
            return std.json.parseFromSlice(T, std.testing.allocator, response.bytes(), .{ .allocate = .alloc_always });
        }
        fn apply(owner: *client.Owner, request: @import("db/types.zig").BatchRequest, index: u64) !void {
            const json = try batch.encodeBatchRequest(std.testing.allocator, request);
            defer std.testing.allocator.free(json);
            var response = try owner.replicatedBatchAtRaftEntryJson("docs", json, 1, index);
            response.deinit();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const donor_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/donor", .{tmp.sub_path});
    defer alloc.free(donor_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const options: abi.OpenRequest = .{ .path = .fromSlice(donor_path), .table_name = .fromSlice("docs"), .group_id = 7201, .has_identity_namespace = 1, .identity_table_id = 72, .identity_shard_id = 7201, .identity_range_id = 7201, .schema_json = .fromSlice("{}") };
    var donor = try client.Owner.open(options);
    defer donor.deinit();
    var target_options = options;
    target_options.path = .fromSlice(target_path);
    var target = try client.Owner.open(target_options);
    defer target.deinit();
    // Incompressible printable JSON guarantees several physical transport
    // chunks without inflating the user-visible row or transport limits.
    const payload = try alloc.alloc(u8, 3 * 1024 * 1024);
    defer alloc.free(payload);
    var random = std.Random.DefaultPrng.init(419);
    for (payload) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
    const document = try std.fmt.allocPrint(alloc, "{{\"text\":\"{s}\"}}", .{payload});
    defer alloc.free(document);
    for ([_]*client.Owner{ &donor, &target }) |owner| try Call.apply(owner, .{ .timestamp_ns = 123, .writes = &.{.{ .key = "row", .value = document }} }, 1);
    const identity_request = try contract.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" });
    defer alloc.free(identity_request);
    var identity_response = try donor.lookupJson("docs", identity_request);
    defer identity_response.deinit();
    var identity = try std.json.parseFromSlice(@import("db/relational_integrity_topology_contract.zig").Identity, alloc, identity_response.bytes(), .{});
    defer identity.deinit();
    const scope: source.Scope = .{ .fence = .{ .admission_epoch = identity.value.next_epoch, .transition_id = 57, .attempt = 1, .owner_group_id = 7201, .peer_group_id = 7202, .role = .merge_source, .namespace = identity.value.namespace, .catalog_digest = identity.value.catalog_digest }, .receiver_namespace = .{ .table_id = 72, .shard_id = 7202, .range_id = 7202 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    for ([_]*client.Owner{ &donor, &target }) |owner| try Call.apply(owner, .{ .online_source = .{ .admit = .{ .scope = scope } } }, 2);
    const pin_json = try std.json.Stringify.valueAlloc(alloc, scope, .{});
    defer alloc.free(pin_json);
    var publication = try donor.prepareSourcePinPublicationJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(pin_json) });
    defer publication.deinit();
    var certificate = try std.json.parseFromSlice(@import("source_snapshot.zig").Certificate, alloc, publication.bytes(), .{});
    defer certificate.deinit();
    for ([_]*client.Owner{ &donor, &target }) |owner| try Call.apply(owner, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate.value } } }, 3);
    // Simulate native-state transfer containing the durable published ledger
    // but none of another replica's filesystem pins or local stat receipts.
    target.deinit();
    const target_pin = try @import("db/source_pin.zig").pathAlloc(alloc, target_path, scope);
    defer alloc.free(target_pin);
    try std.Io.Dir.cwd().deleteTree(std.testing.io, target_pin);
    target = try client.Owner.open(target_options);
    try std.testing.expectError(error.OnlineSourcePinMissing, target.prepareSourcePinPublicationJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(pin_json) }));
    var described = try Call.run(transfer.Descriptor, &donor, .{ .describe = scope });
    defer described.deinit();
    const descriptor = described.value;
    try std.testing.expect(descriptor.total_bytes > transfer.max_chunk_bytes);
    var wrong = descriptor;
    wrong.scope.copy_attempt.sequence += 1;
    try std.testing.expectError(error.OnlineSourceScopeChanged, Call.run(transfer.Status, &target, .{ .status = wrong }));
    var offset: u64 = 0;
    while (offset < descriptor.total_bytes) {
        if (offset != 0) {
            // Subsequent slices use the production replica-transfer adapter:
            // distinct endpoints, same donor group, one bounded chunk/call.
            try std.testing.expect(!try @import("../metadata/online_merge_artifact.zig").step(alloc, .{ .ptr = &donor, .request = Call.transport }, .{ .ptr = &target, .request = Call.transport }, "docs", .{ .scope = scope, .phase = .snapshot, .certificate = certificate.value, .acknowledged = certificate.value.cut.retained_start }, .{}));
            var observed = try Call.run(transfer.Status, &target, .{ .status = descriptor });
            defer observed.deinit();
            try std.testing.expect(observed.value.next_offset > offset);
            try std.testing.expect(observed.value.next_offset - offset <= transfer.max_chunk_bytes);
            offset = observed.value.next_offset;
            continue;
        }
        var chunk = try Call.run(transfer.ReadResponse, &donor, .{ .read = .{ .descriptor = descriptor, .offset = offset } });
        defer chunk.deinit();
        var request: transfer.Request = .{ .write = .{ .descriptor = descriptor, .offset = offset, .data_base64 = chunk.value.data_base64, .digest = chunk.value.digest } };
        if (offset == 0) {
            request.write.digest[0] ^= 1;
            try std.testing.expectError(error.SourceSnapshotCorrupt, Call.run(transfer.Status, &target, request));
            request.write.digest = chunk.value.digest;
            const body = try std.json.Stringify.valueAlloc(alloc, request, .{});
            defer alloc.free(body);
            var canceled = std.atomic.Value(bool).init(true);
            const Cancel = struct {
                fn check(ptr: ?*anyopaque) callconv(.c) u8 {
                    const flag: *std.atomic.Value(bool) = @ptrCast(@alignCast(ptr.?));
                    return @intFromBool(flag.load(.acquire));
                }
            };
            try std.testing.expectError(error.Canceled, target.sourceArtifactJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(body), .cancellation_ctx = &canceled, .cancellation_fn = Cancel.check }));
        }
        var accepted = try Call.run(transfer.Status, &target, request);
        defer accepted.deinit();
        try std.testing.expect(!accepted.value.complete);
        if (offset == 0) {
            target.deinit();
            // Power loss can leave file bytes durable before the matching
            // offset receipt. Preserve that exact disk state without adding
            // a production failpoint; the next accepted chunk must replace
            // the unacknowledged tail rather than treating it as progress.
            const spool_path = try std.fmt.allocPrint(alloc, "{s}/source.receiving", .{target_pin});
            defer alloc.free(spool_path);
            {
                const spool = try std.Io.Dir.cwd().openFile(std.testing.io, spool_path, .{ .mode = .read_write });
                defer spool.close(std.testing.io);
                try spool.writePositionalAll(std.testing.io, "unacknowledged physical tail", accepted.value.next_offset);
                try spool.sync(std.testing.io);
            }
            target = try client.Owner.open(target_options);
            var restarted = try Call.run(transfer.Status, &target, .{ .status = descriptor });
            defer restarted.deinit();
            try std.testing.expectEqual(accepted.value.next_offset, restarted.value.next_offset);
            var duplicate = try Call.run(transfer.Status, &target, request);
            defer duplicate.deinit();
            try std.testing.expectEqualDeep(accepted.value, duplicate.value);
            // Even a self-consistent new transport checksum cannot change an
            // already acknowledged chunk at the same immutable source offset.
            const replacement = try alloc.alloc(u8, transfer.max_chunk_bytes);
            defer alloc.free(replacement);
            @memset(replacement, 'q');
            const replacement64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(replacement.len));
            defer alloc.free(replacement64);
            const changed_chunk: transfer.Request = .{ .write = .{ .descriptor = descriptor, .offset = 0, .data_base64 = std.base64.standard.Encoder.encode(replacement64, replacement), .digest = transfer.checksum(replacement) } };
            try std.testing.expectError(error.SourceSnapshotCorrupt, Call.run(transfer.Status, &target, changed_chunk));
            try std.testing.expectError(error.SourceSnapshotIncomplete, Call.run(transfer.Status, &target, .{ .finish = descriptor }));
        }
        offset = accepted.value.next_offset;
    }
    // Publication may crash after renaming verified bytes but before writing
    // its local receipt. An old chunk retry must not create a new empty spool
    // that hides the complete renamed artifact on recovery.
    target.deinit();
    const receiving_path = try std.fmt.allocPrint(alloc, "{s}/source.receiving", .{target_pin});
    defer alloc.free(receiving_path);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/source.afb2", .{target_pin});
    defer alloc.free(artifact_path);
    try std.Io.Dir.rename(.cwd(), receiving_path, .cwd(), artifact_path, std.testing.io);
    target = try client.Owner.open(target_options);
    var first_retry = try Call.run(transfer.ReadResponse, &donor, .{ .read = .{ .descriptor = descriptor, .offset = 0 } });
    defer first_retry.deinit();
    var retried = try Call.run(transfer.Status, &target, .{ .write = .{ .descriptor = descriptor, .offset = 0, .data_base64 = first_retry.value.data_base64, .digest = first_retry.value.digest } });
    defer retried.deinit();
    try std.testing.expectEqual(descriptor.total_bytes, retried.value.next_offset);
    var verification_complete = false;
    for (0..1000) |_| {
        var finished = try Call.run(transfer.Status, &target, .{ .finish = descriptor });
        defer finished.deinit();
        if (finished.value.complete) {
            verification_complete = true;
            break;
        }
        // Every call reopens the owner: no verifier heap/hash/parser state
        // survives, and the artifact must not be recopied or rescanned.
        target.deinit();
        target = try client.Owner.open(target_options);
    }
    try std.testing.expect(verification_complete);
    target.deinit();
    target = try client.Owner.open(target_options);
    var recovered = try target.prepareSourcePinPublicationJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(pin_json) });
    defer recovered.deinit();
    try std.testing.expectEqualStrings(publication.bytes(), recovered.bytes());
    // The replacement is a donor too; re-export never requires old inode IDs.
    var exported = try Call.run(transfer.ReadResponse, &target, .{ .read = .{ .descriptor = descriptor, .offset = 0 } });
    defer exported.deinit();
    try std.testing.expect(exported.value.data_base64.len != 0);
    try Call.apply(&target, .{ .online_source = .{ .release = scope } }, 4);
    target.deinit();
    target = try client.Owner.open(target_options);
    // Durable cancellation/release wins over delayed transfer traffic, even
    // after owner restart. No stale sender may reopen or reset that scope.
    try std.testing.expectError(error.OnlineSourceScopeChanged, Call.run(transfer.Descriptor, &target, .{ .describe = scope }));
    try std.testing.expectError(error.OnlineSourceScopeChanged, Call.run(transfer.Status, &target, .{ .finish = descriptor }));
    try std.testing.expectError(error.OnlineSourceScopeChanged, Call.run(transfer.Status, &target, .{ .reset = descriptor }));
    try std.testing.expectError(error.OnlineSourceScopeChanged, Call.run(transfer.Status, &target, .{ .write = .{ .descriptor = descriptor, .offset = 0, .data_base64 = first_retry.value.data_base64, .digest = first_retry.value.digest } }));
}

test "opaque owner online source controls and status survive compiled boundary restart" {
    const alloc = std.testing.allocator;
    const contract = @import("../api/local_query_contract.zig");
    const batch = @import("../api/batch.zig");
    const source = @import("db/online_source_contract.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/online-source-controls", .{tmp.sub_path});
    defer alloc.free(path);
    const options: abi.OpenRequest = .{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7101,
        .has_identity_namespace = 1,
        .identity_table_id = 71,
        .identity_shard_id = 7101,
        .identity_range_id = 7101,
        .schema_json = .fromSlice("{}"),
    };
    var owner = try client.Owner.open(options);
    defer owner.deinit();
    const identity_request = try contract.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" });
    defer alloc.free(identity_request);
    var identity_response = try owner.lookupJson("docs", identity_request);
    defer identity_response.deinit();
    var identity = try std.json.parseFromSlice(@import("db/relational_integrity_topology_contract.zig").Identity, alloc, identity_response.bytes(), .{});
    defer identity.deinit();
    const scope: source.Scope = .{
        .fence = .{ .admission_epoch = identity.value.next_epoch, .transition_id = 19, .attempt = 2, .owner_group_id = 7101, .peer_group_id = 7102, .role = .merge_source, .namespace = identity.value.namespace, .catalog_digest = identity.value.catalog_digest },
        .receiver_namespace = .{ .table_id = 71, .shard_id = 7102, .range_id = 7102 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    const admission = try batch.encodeBatchRequest(alloc, .{ .online_source = .{ .admit = .{ .scope = scope } } });
    defer alloc.free(admission);
    var admitted = try owner.replicatedBatchAtRaftEntryJson("docs", admission, 1, 1);
    admitted.deinit();
    // Admission retains changes without freezing normal document writes.
    var written = try owner.replicatedBatchAtRaftEntryJson("docs", "{\"writes\":{\"r\":{\"n\":1}},\"_timestamp_ns\":\"123\"}", 1, 2);
    written.deinit();
    const status_json = try std.json.Stringify.valueAlloc(alloc, .{ .mode = "online_source_status", .scope = scope }, .{});
    defer alloc.free(status_json);
    const status_request = try contract.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = status_json });
    defer alloc.free(status_request);
    const Read = struct {
        fn check(allocator: std.mem.Allocator, handle: *client.Owner, request: []const u8) !void {
            var response = try handle.lookupJson("docs", request);
            defer response.deinit();
            var progress = try std.json.parseFromSlice(struct { admitted_applied_index: u64, acknowledged: u64 }, allocator, response.bytes(), .{ .ignore_unknown_fields = true });
            defer progress.deinit();
            try std.testing.expectEqual(@as(u64, 1), progress.value.admitted_applied_index);
            try std.testing.expectEqual(@as(u64, 0), progress.value.acknowledged);
        }
    };
    try Read.check(alloc, &owner, status_request);
    const pin_json = try std.json.Stringify.valueAlloc(alloc, scope, .{});
    defer alloc.free(pin_json);
    var publication = try owner.prepareSourcePinPublicationJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(pin_json) });
    defer publication.deinit();
    var certificate = try std.json.parseFromSlice(@import("source_snapshot.zig").Certificate, alloc, publication.bytes(), .{});
    defer certificate.deinit();
    try std.testing.expectEqual(@as(u64, 1), certificate.value.cut.applied_index);
    try std.testing.expect(certificate.value.cut.namespace.eql(scope.fence.namespace));
    owner.deinit();
    owner = try client.Owner.open(options);
    try Read.check(alloc, &owner, status_request);
    var repeated_publication = try owner.prepareSourcePinPublicationJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(pin_json) });
    defer repeated_publication.deinit();
    try std.testing.expectEqualStrings(publication.bytes(), repeated_publication.bytes());
    var retry = try owner.replicatedBatchAtRaftEntryJson("docs", admission, 1, 3);
    retry.deinit();
    try Read.check(alloc, &owner, status_request);
}

test "opaque owner relational handoff preserves binary proofs across the compiled boundary" {
    const alloc = std.testing.allocator;
    const topology = @import("db/relational_integrity_topology_contract.zig");
    const handoff = @import("db/relational_integrity_handoff_contract.zig");
    const contract = @import("db/relational_transition_contract.zig");
    const path = "/tmp/antfly-kernel-relational-handoff";
    cleanup(path);
    defer cleanup(path);
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"uq","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"string"}},"additionalProperties":false}}}}
    ;
    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7101,
        .has_identity_namespace = 1,
        .identity_table_id = 71,
        .identity_shard_id = 7101,
        .identity_range_id = 7101,
        .schema_json = .fromSlice(schema_json),
    });
    defer owner.deinit();
    const Read = struct {
        fn call(comptime T: type, allocator: std.mem.Allocator, handle: ?*anyopaque, command: contract.Request) !std.json.Parsed(T) {
            const body = try handoff.encode(allocator, command);
            defer allocator.free(body);
            var response: abi.OwnedBytes = .{};
            try error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(handle, &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(body) }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            return std.json.parseFromSlice(T, allocator, response.slice(), .{ .allocate = .alloc_always });
        }
    };
    var identity = try Read.call(topology.Identity, alloc, owner.handle, .identity);
    defer identity.deinit();
    var before = try Read.call(topology.Status, alloc, owner.handle, .status);
    defer before.deinit();
    try std.testing.expect(before.value.fence == null and before.value.drained);
    const source: topology.Fence = .{
        .admission_epoch = identity.value.next_epoch,
        .transition_id = 17,
        .attempt = 3,
        .peer_group_id = 7102,
        .owner_group_id = 7101,
        .role = .split_source,
        .namespace = identity.value.namespace,
        .catalog_digest = identity.value.catalog_digest,
    };
    var destination = source;
    destination.owner_group_id = 7102;
    destination.peer_group_id = 7101;
    destination.role = .split_destination;
    destination.namespace.shard_id = 7102;
    destination.namespace.range_id = 7102;
    const begin = try handoff.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .begin, .fence = source } });
    defer alloc.free(begin);
    var applied = try owner.replicatedBatchAtRaftEntryJson("docs", begin, 1, 1);
    applied.deinit();
    var frozen = try Read.call(topology.Status, alloc, owner.handle, .status);
    defer frozen.deinit();
    try std.testing.expect(frozen.value.drained and frozen.value.fence.?.eql(source));
    var manifest = try Read.call(handoff.Manifest, alloc, owner.handle, .{ .manifest = .{
        .source = source,
        .destination = destination,
        .lower = "\x00\xff",
        .upper = "\x01\xff",
        .primary_sequence = 1,
    } });
    defer manifest.deinit();
    try std.testing.expectEqualStrings("\x00\xff", manifest.value.lower);
    try std.testing.expectEqualStrings("\x01\xff", manifest.value.upper);
    try std.testing.expect(manifest.value.catalog_bytes.len != 0);
    try std.testing.expect(manifest.value.activation_bytes.len != 0);
    const manifest_bytes = try handoff.encode(alloc, manifest.value);
    defer alloc.free(manifest_bytes);
    var page = try Read.call(handoff.Page, alloc, owner.handle, .{ .page = .{
        .manifest = manifest.value,
        .progress = .{ .manifest_digest = handoff.hash(manifest_bytes) },
    } });
    defer page.deinit();
    try std.testing.expect(page.value.exhausted);
    try std.testing.expectEqual(@as(usize, 0), page.value.records.len);
    const release = try handoff.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .release, .fence = source } });
    defer alloc.free(release);
    try std.testing.expectError(error.IntegrityTopologyCutoverRequired, owner.replicatedBatchAtRaftEntryJson("docs", release, 1, 2));
    // This fixture only inspected the frozen source; it never published a
    // destination. Cancel that attempt rather than manufacturing cutover.
    const cancel = try handoff.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .cancel, .fence = source } });
    defer alloc.free(cancel);
    var released = try owner.replicatedBatchAtRaftEntryJson("docs", cancel, 1, 3);
    released.deinit();
    var after = try Read.call(topology.Status, alloc, owner.handle, .status);
    defer after.deinit();
    try std.testing.expect(after.value.fence == null);
}

test "opaque owner exports exact backup seals and reclaims pins without opening the source" {
    const alloc = std.testing.allocator;
    const topology = @import("db/relational_integrity_topology_contract.zig");
    const handoff = @import("db/relational_integrity_handoff_contract.zig");
    const transition = @import("db/relational_transition_contract.zig");
    const seal = @import("db/native_backup_seal_contract.zig");
    const backup = @import("../api/backup_contract.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7105/table-db", .{root});
    defer alloc.free(path);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    var owner = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7105,
        .has_identity_namespace = 1,
        .identity_table_id = 71,
        .identity_shard_id = 7105,
        .identity_range_id = 7105,
    });
    var owner_open = true;
    defer if (owner_open) owner.deinit();
    const identity_json = try handoff.encode(alloc, @as(transition.Request, .identity));
    defer alloc.free(identity_json);
    var identity_response: abi.OwnedBytes = .{};
    try error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(owner.handle, &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(identity_json) }, &identity_response));
    defer abi.antfly_storage_owner_buffer_destroy(&identity_response);
    var identity = try std.json.parseFromSlice(topology.Identity, alloc, identity_response.slice(), .{});
    defer identity.deinit();
    const fence: topology.Fence = .{
        .admission_epoch = identity.value.next_epoch,
        .transition_id = 19,
        .attempt = 1,
        .peer_group_id = 7105,
        .owner_group_id = 7105,
        .role = .backup_snapshot,
        .namespace = identity.value.namespace,
        .catalog_digest = identity.value.catalog_digest,
    };
    const begin = try handoff.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .begin, .fence = fence } });
    defer alloc.free(begin);
    var applied = try owner.replicatedBatchAtRaftEntryJson("docs", begin, 1, 1);
    applied.deinit();
    const pin_json = try handoff.encode(alloc, seal.Request{ .seal = .{ .id = "cut", .fence = fence } });
    defer alloc.free(pin_json);
    var pin = try owner.backupPinControlJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(pin_json) });
    defer pin.deinit();
    var handle = try std.json.parseFromSlice(seal.Handle, alloc, pin.bytes(), .{});
    defer handle.deinit();
    inline for (.{ abi.BackupFormat.native, abi.BackupFormat.portable }) |format| {
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, @tagName(format) });
        defer alloc.free(destination);
        var exported = try owner.backupWithControl(.{
            .format = @intFromEnum(format),
            .table_name = .fromSlice("docs"),
            .backup_root = .fromSlice(destination),
            .backup_id = .fromSlice("cut"),
            .sealed_handle_json = .fromSlice(pin.bytes()),
        });
        defer exported.deinit();
        var shards = try std.json.parseFromSlice([]backup.ShardSnapshot, alloc, exported.bytes(), .{});
        defer shards.deinit();
        try std.testing.expectEqual(@as(usize, 1), shards.value.len);
        try std.testing.expectEqual(@as(u64, 7105), shards.value[0].group_id);
        try std.testing.expect(shards.value[0].snapshot_path.len > 0);
    }
    var wrong_handle = handle.value;
    wrong_handle.digest[0] ^= 1;
    const wrong_json = try handoff.encode(alloc, wrong_handle);
    defer alloc.free(wrong_json);
    try std.testing.expectError(error.BackupSealMismatch, owner.backupWithControl(.{
        .table_name = .fromSlice("docs"),
        .backup_root = .fromSlice(root),
        .backup_id = .fromSlice("invalid-proof"),
        .sealed_handle_json = .fromSlice(wrong_json),
    }));
    owner.deinit();
    owner_open = false;
    // Reclamation is path-only and idempotent: no resident table/owner is
    // needed after a restore or table drop has retired the original source.
    const release_json = try handoff.encode(alloc, seal.Request{ .release = handle.value });
    defer alloc.free(release_json);
    for (0..2) |_| {
        var released = try context.reclaimBackupPinJson(.{
            .control = .{ .request_json = .fromSlice(release_json) },
            .replica_root = .fromSlice(root),
            .group_id = 7105,
        });
        released.deinit();
    }
}

test "opaque ordinary owner range initialization preserves durable authority and rejects unsafe reconciliation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    const Driver = struct {
        fn put(owner: *client.Owner, key: []const u8) !void {
            const request = try std.fmt.allocPrint(std.testing.allocator, "{{\"inserts\":{{\"{s}\":{{\"value\":1}}}},\"sync_level\":\"write\"}}", .{key});
            defer std.testing.allocator.free(request);
            var response = try owner.batchJson("docs", request);
            response.deinit();
        }
    };
    inline for (.{ "fresh", "contained", "outside", "namespace", "deadline", "frozen" }) |scenario| {
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, scenario });
        defer alloc.free(path);
        var options: abi.OpenRequest = .{
            .context = context.handle,
            .path = .fromSlice(path),
            .table_name = .fromSlice("docs"),
            .group_id = 7191,
            .has_identity_namespace = 1,
            .identity_table_id = 71,
            .identity_shard_id = 7191,
            .identity_range_id = 7191,
        };
        if (comptime !std.mem.eql(u8, scenario, "fresh") and !std.mem.eql(u8, scenario, "deadline")) {
            var unbounded = try client.Owner.open(options);
            defer unbounded.deinit();
            try Driver.put(&unbounded, if (std.mem.eql(u8, scenario, "outside")) "z" else "b");
            if (comptime std.mem.eql(u8, scenario, "frozen")) {
                const topology = @import("db/relational_integrity_topology_contract.zig");
                const handoff = @import("db/relational_integrity_handoff_contract.zig");
                const identity_request = try handoff.encode(alloc, @as(@import("db/relational_transition_contract.zig").Request, .identity));
                defer alloc.free(identity_request);
                var identity_response: abi.OwnedBytes = .{};
                defer abi.antfly_storage_owner_buffer_destroy(&identity_response);
                try error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(unbounded.handle, &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(identity_request) }, &identity_response));
                var identity = try std.json.parseFromSlice(topology.Identity, alloc, identity_response.slice(), .{});
                defer identity.deinit();
                const fence: topology.Fence = .{
                    .role = .backup_snapshot,
                    .transition_id = 9191,
                    .attempt = 1,
                    .peer_group_id = 7191,
                    .owner_group_id = 7191,
                    .admission_epoch = identity.value.next_epoch,
                    .namespace = identity.value.namespace,
                    .catalog_digest = identity.value.catalog_digest,
                };
                const freeze = try handoff.encode(alloc, .{ ._relational_topology = topology.Command{ .action = .begin, .fence = fence } });
                defer alloc.free(freeze);
                var applied = try unbounded.replicatedBatchAtRaftEntryJson("docs", freeze, 1, 1);
                applied.deinit();
            }
        }
        options.has_initial_range = 1;
        options.initial_range_start = .fromSlice("a");
        options.initial_range_end = .fromSlice("m");
        if (comptime std.mem.eql(u8, scenario, "frozen")) {
            try std.testing.expectError(error.IntegrityTopologyBusy, client.Owner.open(options));
            options.has_initial_range = 0;
            options.initial_range_start = .{};
            options.initial_range_end = .{};
            var still_frozen = try client.Owner.open(options);
            defer still_frozen.deinit();
            try std.testing.expectError(error.IntegrityTopologyBusy, Driver.put(&still_frozen, "b"));
            continue;
        } else if (comptime std.mem.eql(u8, scenario, "outside")) {
            try std.testing.expectError(error.KeyOutOfRange, client.Owner.open(options));
            // A failed narrow reconciliation must not install its bounds.
            options.initial_range_start = .fromSlice("m");
            options.initial_range_end = .fromSlice("zz");
        } else if (comptime std.mem.eql(u8, scenario, "namespace")) {
            options.identity_table_id = 72;
            options.initial_range_start = .fromSlice("m");
            options.initial_range_end = .fromSlice("z");
            try std.testing.expectError(error.IdentityNamespaceMismatch, client.Owner.open(options));
            options.identity_table_id = 71;
            options.initial_range_start = .fromSlice("a");
            options.initial_range_end = .fromSlice("m");
        } else if (comptime std.mem.eql(u8, scenario, "deadline")) {
            options.initial_range_control.has_execution_deadline = 1;
            options.initial_range_control.execution_deadline_ns = 1;
            try std.testing.expectError(error.DeadlineExceeded, client.Owner.open(options));
            options.initial_range_control = .{};
        }
        {
            var bounded = try client.Owner.open(options);
            defer bounded.deinit();
            try Driver.put(&bounded, if (std.mem.eql(u8, scenario, "outside")) "z" else "b");
            try std.testing.expectError(error.KeyOutOfRange, Driver.put(&bounded, if (std.mem.eql(u8, scenario, "outside")) "b" else "z"));
        }
        // The creation hint is not transition authority. A stale descriptor
        // cannot change a range that was already committed by its owner.
        options.initial_range_start = .fromSlice("");
        options.initial_range_end = .fromSlice("");
        var reopened = try client.Owner.open(options);
        defer reopened.deinit();
        try std.testing.expectError(error.KeyOutOfRange, Driver.put(&reopened, if (std.mem.eql(u8, scenario, "outside")) "b" else "z"));
    }
}

test "opaque portable and native restore preserve bounded range through captured replicated pages" {
    const alloc = std.testing.allocator;
    const staging = @import("db/restore_staging_contract.zig");
    const restore = @import("../api/restore_owner_contract.zig");
    const topology = @import("db/relational_integrity_topology_contract.zig");
    const handoff = @import("db/relational_integrity_handoff_contract.zig");
    const transition = @import("db/relational_transition_contract.zig");
    const seal = @import("db/native_backup_seal_contract.zig");
    const batch_wire = @import("../api/batch.zig");
    const Driver = struct {
        owner: *client.Owner,
        index: u64 = 0,

        fn call(self: *@This(), input: restore.Request) !restore.Response {
            const body = try handoff.encode(std.testing.allocator, input);
            defer std.testing.allocator.free(body);
            var captured = try self.owner.restoreControlJson(.{ .control = .{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(body) } });
            defer captured.deinit();
            var prepared = try std.json.parseFromSlice(@import("../api/restore_owner.zig").Prepared, std.testing.allocator, captured.bytes(), .{ .allocate = .alloc_always });
            defer prepared.deinit();
            if (prepared.value.batch_json) |encoded| {
                var parsed = try batch_wire.parseInternalBatchRequest(std.testing.allocator, encoded);
                defer parsed.deinit(std.testing.allocator);
                try std.testing.expectEqual(input.scope.digest(), parsed.req.restore_staging_scope.?);
                const replay = try batch_wire.encodeBatchRequest(std.testing.allocator, parsed.req);
                defer std.testing.allocator.free(replay);
                self.index += 1;
                var applied = try self.owner.replicatedBatchAtRaftEntryJson("docs", replay, 1, self.index);
                applied.deinit();
                return self.call(.{ .scope = input.scope, .action = .status });
            }
            return prepared.value.response;
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    const schema_bytes = try @import("schema.zig").serializeSchema(scratch, .{});
    const source_namespace: @import("db/doc_identity.zig").Namespace = .{ .table_id = 71, .shard_id = 7101, .range_id = 7101 };
    const source_path = try std.fmt.allocPrint(scratch, "{s}/source", .{root});
    var source = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice(source_path),
        .table_name = .fromSlice("docs"),
        .group_id = 7101,
        .has_identity_namespace = 1,
        .identity_table_id = 71,
        .identity_shard_id = 7101,
        .identity_range_id = 7101,
        .indexes_json = .fromSlice("{}"),
        .has_initial_range = 1,
        .initial_range_start = .fromSlice("a"),
        .initial_range_end = .fromSlice("m"),
    });
    defer source.deinit();
    var source_driver: Driver = .{ .owner = &source };
    var written = try source.batchJson("docs", "{\"inserts\":{\"b\":{\"value\":1},\"k\":{\"value\":2}},\"sync_level\":\"write\"}");
    written.deinit();
    var identity_response: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&identity_response);
    try error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(source.handle, &.{
        .table_name = .fromSlice("docs"),
        .request_json = .fromSlice(try handoff.encode(scratch, @as(transition.Request, .identity))),
    }, &identity_response));
    const identity = try std.json.parseFromSliceLeaky(topology.Identity, scratch, identity_response.slice(), .{});
    const fence: topology.Fence = .{
        .role = .backup_snapshot,
        .transition_id = 991,
        .attempt = 1,
        .peer_group_id = 7101,
        .owner_group_id = 7101,
        .admission_epoch = identity.next_epoch,
        .namespace = identity.namespace,
        .catalog_digest = identity.catalog_digest,
    };
    source_driver.index += 1;
    var frozen = try source.replicatedBatchAtRaftEntryJson("docs", try handoff.encode(scratch, .{ ._relational_topology = topology.Command{ .action = .begin, .fence = fence } }), 1, source_driver.index);
    frozen.deinit();
    var pin = try source.backupPinControlJson(.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(try handoff.encode(scratch, seal.Request{ .seal = .{ .id = "bounded", .fence = fence } })) });
    defer pin.deinit();
    const pin_handle = try std.json.parseFromSliceLeaky(seal.Handle, scratch, pin.bytes(), .{});
    inline for (.{ abi.BackupFormat.portable, abi.BackupFormat.native }) |format| {
        const backup_id = "bounded-" ++ @tagName(format);
        var exported = try source.backupWithControl(.{
            .format = @intFromEnum(format),
            .table_name = .fromSlice("docs"),
            .backup_root = .fromSlice(root),
            .backup_id = .fromSlice(backup_id),
            .sealed_handle_json = .fromSlice(pin.bytes()),
        });
        defer exported.deinit();
        const shards = try std.json.parseFromSliceLeaky([]@import("../api/backup_contract.zig").ShardSnapshot, scratch, exported.bytes(), .{});
        try std.testing.expectEqual(@as(usize, 1), shards.len);
        try std.testing.expectEqualStrings("a", shards[0].start_key);
        var digest: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&digest, shards[0].artifact_sha256);
        const artifact: @import("../metadata/restore_staging.zig").SourceArtifact = .{
            .target_group_id = 7201,
            .source_namespace = source_namespace,
            .format = if (format == .portable) .portable else .native,
            .snapshot_path = shards[0].snapshot_path,
            .artifact_size_bytes = shards[0].artifact_size_bytes,
            .artifact_sha256 = digest,
            .native_manifest_size_bytes = shards[0].native_manifest_size_bytes,
            .native_manifest_sha256 = shards[0].native_manifest_sha256,
            .cohort_seal = pin_handle,
        };
        const target_scope: staging.Scope = .{
            .plan_id = @splat(4),
            .plan_digest = @splat(5),
            .source_artifact_digest = digest,
            .source_descriptor_digest = try artifact.digest(scratch),
            .source_namespace = source_namespace,
            .target_namespace = .{ .table_id = 72, .shard_id = 7201, .range_id = 7201 },
            .target_schema_digest = staging.digest(schema_bytes),
        };
        const target_bootstrap: staging.OwnerBootstrap = .{
            .scope = target_scope,
            .table_name = "docs",
            .schema_json = "",
            .indexes_json = "{}",
            .byte_range = .{ .start = "a", .end = "m" },
        };
        var target = try client.Owner.open(.{
            .context = context.handle,
            .path = .fromSlice(try std.fmt.allocPrint(scratch, "{s}/target-{s}", .{ root, @tagName(format) })),
            .table_name = .fromSlice("docs"),
            .group_id = 7201,
            .has_identity_namespace = 1,
            .identity_table_id = 72,
            .identity_shard_id = 7201,
            .identity_range_id = 7201,
            .indexes_json = .fromSlice("{}"),
            .restore_bootstrap_json = .fromSlice(try handoff.encode(scratch, target_bootstrap)),
        });
        defer target.deinit();
        var target_driver: Driver = .{ .owner = &target };
        _ = try target_driver.call(.{ .scope = target_scope, .action = .begin });
        const source_request: restore.Source = .{ .location = try std.fmt.allocPrint(scratch, "file://{s}", .{root}), .artifact = artifact };
        var final: restore.Response = undefined;
        for (0..256) |_| {
            final = try target_driver.call(.{ .scope = target_scope, .action = .import_page, .source = source_request, .max_rows = 1 });
            if (final.phase == .imported) break;
        }
        try std.testing.expectEqual(staging.Phase.imported, final.phase);
        try std.testing.expectEqual(@as(u64, 2), final.rows);
        _ = try target_driver.call(.{ .scope = target_scope, .action = .validate });
        _ = try target_driver.call(.{ .scope = target_scope, .action = .publish });
        var restored = try target.lookupJson("docs", "{\"key\":\"b\"}");
        defer restored.deinit();
        try std.testing.expect(std.mem.indexOf(u8, restored.bytes(), "1") != null);
    }
}

test "opaque hidden restore prepares without mutation and reopens exact canceled scope" {
    const alloc = std.testing.allocator;
    const staging = @import("db/restore_staging_contract.zig");
    const restore = @import("../api/restore_owner_contract.zig");
    const Prepared = struct { response: restore.Response, batch_json: ?[]const u8 = null };
    const path = "/tmp/antfly-kernel-hidden-restore";
    cleanup(path);
    defer cleanup(path);
    const snapshot_path = "/tmp/antfly-kernel-hidden-restore-seed";
    cleanup(snapshot_path);
    defer cleanup(snapshot_path);
    var storage_context = client.Context{};
    try storage_context.ensure();
    defer storage_context.deinit();
    const schema_bytes = try @import("schema.zig").serializeSchema(alloc, .{});
    defer alloc.free(schema_bytes);
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = .{ .table_id = 70, .shard_id = 7001, .range_id = 7001 },
        .target_namespace = .{ .table_id = 71, .shard_id = 7102, .range_id = 7102 },
        .target_schema_digest = staging.digest(schema_bytes),
    };
    const bootstrap: staging.OwnerBootstrap = .{ .scope = scope, .table_name = "docs", .schema_json = "", .indexes_json = "{}", .byte_range = .{ .start = "", .end = "" } };
    const bootstrap_json = try std.json.Stringify.valueAlloc(alloc, bootstrap, .{});
    defer alloc.free(bootstrap_json);
    var options: abi.OpenRequest = .{
        .context = storage_context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7102,
        .has_identity_namespace = 1,
        .identity_table_id = 71,
        .identity_shard_id = 7102,
        .identity_range_id = 7102,
        .indexes_json = .fromSlice("{}"),
        .restore_bootstrap_json = .fromSlice(bootstrap_json),
    };
    var owner = try client.Owner.open(options);
    var owner_open = true;
    defer if (owner_open) owner.deinit();
    const Control = struct {
        fn bootstrapAt(target: ?*client.Owner, context: *client.Context, table_id: u64) !std.json.Parsed(staging.OwnerBootstrap) {
            var bytes: abi.OwnedBytes = .{};
            try @import("kernel_error_identity").statusToError(abi.antfly_storage_owner_hidden_restore_json(if (target) |value| value.handle else null, &.{ .operation = .read_bootstrap, .context = context.handle, .path = .fromSlice(path), .table_id = table_id }, &bytes));
            defer abi.antfly_storage_owner_buffer_destroy(&bytes);
            return std.json.parseFromSlice(staging.OwnerBootstrap, std.testing.allocator, bytes.slice(), .{ .allocate = .alloc_always });
        }
        fn call(allocator: std.mem.Allocator, target: *client.Owner, input: restore.Request) !std.json.Parsed(Prepared) {
            const body = try std.json.Stringify.valueAlloc(allocator, input, .{});
            defer allocator.free(body);
            var result = try target.restoreControlJson(.{ .control = .{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(body) } });
            defer result.deinit();
            return std.json.parseFromSlice(Prepared, allocator, result.bytes(), .{ .allocate = .alloc_always });
        }
    };
    var begin = try Control.call(alloc, &owner, .{ .scope = scope, .action = .begin });
    defer begin.deinit();
    try std.testing.expectEqual(staging.Phase.reserved, begin.value.response.phase);
    try std.testing.expect(begin.value.batch_json != null);
    var before = try Control.call(alloc, &owner, .{ .scope = scope, .action = .status });
    defer before.deinit();
    try std.testing.expectEqual(staging.Phase.reserved, before.value.response.phase);
    {
        // Exercise the actual archive boundary, not just the HTTP adapter:
        // valid maximum-length JSON must reach the owner, while one byte over
        // the shared bound is a stable terminal request error. Whitespace
        // avoids building millions of test-only JSON nodes.
        const compact = try std.json.Stringify.valueAlloc(alloc, restore.Request{ .scope = scope, .action = .status }, .{});
        defer alloc.free(compact);
        const padded = try alloc.alloc(u8, restore.max_request_bytes + 1);
        defer alloc.free(padded);
        @memset(padded, ' ');
        @memcpy(padded[0..compact.len], compact);
        var result = try owner.restoreControlJson(.{ .control = .{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(padded[0..restore.max_request_bytes]) } });
        defer result.deinit();
        var status = try std.json.parseFromSlice(Prepared, alloc, result.bytes(), .{});
        defer status.deinit();
        try std.testing.expectEqual(staging.Phase.reserved, status.value.response.phase);
        try std.testing.expect(status.value.batch_json == null);
        try std.testing.expectError(error.InvalidBackupRequest, owner.restoreControlJson(.{ .control = .{ .table_name = .fromSlice("docs"), .request_json = .fromSlice(padded) } }));
    }
    var applied = try owner.replicatedBatchAtRaftEntryJson("docs", begin.value.batch_json.?, 1, 1);
    applied.deinit();
    var cancel = try Control.call(alloc, &owner, .{ .scope = scope, .action = .cancel });
    defer cancel.deinit();
    try std.testing.expectEqual(staging.Phase.importing, cancel.value.response.phase);
    var canceled = try owner.replicatedBatchAtRaftEntryJson("docs", cancel.value.batch_json.?, 1, 2);
    canceled.deinit();
    var final = try Control.call(alloc, &owner, .{ .scope = scope, .action = .cancel });
    defer final.deinit();
    try std.testing.expectEqual(staging.Phase.canceled, final.value.response.phase);
    try std.testing.expect(final.value.batch_json == null);
    var warm_bootstrap = try Control.bootstrapAt(&owner, &storage_context, 71);
    defer warm_bootstrap.deinit();
    try std.testing.expectEqualDeep(scope, warm_bootstrap.value.scope);
    try std.testing.expectError(error.RestoreStagingScopeChanged, Control.bootstrapAt(&owner, &storage_context, 70));
    var snapshot_output: abi.OwnedBytes = .{};
    try @import("kernel_error_identity").statusToError(abi.antfly_storage_owner_hidden_restore_json(owner.handle, &.{ .operation = .capture_snapshot, .table_name = .fromSlice("docs"), .table_id = 71, .scope = scope.digest(), .snapshot_token = .fromSlice("canceled-owner"), .destination_root = .fromSlice(snapshot_path) }, &snapshot_output));
    abi.antfly_storage_owner_buffer_destroy(&snapshot_output);
    var snapshot_io = std.Io.Threaded.init(alloc, .{});
    defer snapshot_io.deinit();
    _ = try std.Io.Dir.cwd().statFile(snapshot_io.io(), snapshot_path ++ "/store.bin", .{});
    owner.deinit();
    owner_open = false;
    var cold_bootstrap = try Control.bootstrapAt(null, &storage_context, 71);
    defer cold_bootstrap.deinit();
    try std.testing.expectEqualDeep(scope, cold_bootstrap.value.scope);
    try std.testing.expectError(error.RestoreStagingCanceled, client.Owner.open(options));
    options.restore_ha_replay = 1;
    owner = try client.Owner.open(options);
    owner.deinit();
    options.restore_ha_replay = 0;
    options.restore_cancel_recovery = 1;
    owner = try client.Owner.open(options);
    owner_open = true;
    var resumed = try Control.call(alloc, &owner, .{ .scope = scope, .action = .cancel });
    defer resumed.deinit();
    try std.testing.expectEqualDeep(final.value.response, resumed.value.response);
    var wrong_scope = scope;
    wrong_scope.plan_id[0] ^= 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, Control.call(alloc, &owner, .{ .scope = wrong_scope, .action = .status }));
    owner.deinit();
    owner_open = false;
    var changed_bootstrap = bootstrap;
    changed_bootstrap.indexes_json = "{} ";
    const changed_json = try std.json.Stringify.valueAlloc(alloc, changed_bootstrap, .{});
    defer alloc.free(changed_json);
    options.indexes_json = .fromSlice(changed_bootstrap.indexes_json);
    options.restore_bootstrap_json = .fromSlice(changed_json);
    try std.testing.expectError(error.RestoreStagingScopeChanged, client.Owner.open(options));
}

test "HA seed storage-owner boundary preserves exact operational errors" {
    try std.testing.expectError(
        error.InvalidArgument,
        client.haSeedActivate("{"),
    );
    try std.testing.expectError(
        error.InvalidStagingRoot,
        client.haSeedActivate(
            \\{"staging_root":"relative","target_root":"/valid","expected":{"generation":"gen","slot_name":"slot","identity":{"cluster_id":1,"timeline_id":1,"epoch":1}}}
        ),
    );
}

fn cleanup(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

const TestWalOptions = struct {
    const Backend = enum { lmdb, lsm, lsm_memory };
    const CommitBackend = enum { sync, worker_thread, async_io, adaptive };
    const Empty = struct {};

    backend: ?Backend = null,
    storage: ?*anyopaque = null,
    lsm_options: Empty = .{},
    clock: @import("sim_runtime.zig").Clock = @import("sim_runtime.zig").real_clock,
    commit_scheduler: @import("sim_runtime.zig").CompletionScheduler = @import("sim_runtime.zig").real_completion_scheduler,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: CommitBackend = .adaptive,
    no_sync: bool = false,
    read_only: bool = false,
    model_commit_backend_completions: bool = false,

    pub fn resolvedBackend(self: @This()) Backend {
        return self.backend orelse .lsm;
    }
};

test "opaque WAL preserves durable operations and exact failure identity" {
    const root = "/tmp/antfly-storage-kernel-wal-owner";
    const path = root ++ "/wal";
    const bootstrap_path = root ++ "/bootstrap";
    const read_only_path = root ++ "/read-only";
    cleanup(root);
    defer cleanup(root);

    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);
    var wal = try wal_client.WAL.open(path_z.ptr, TestWalOptions{});
    defer wal.close();
    try std.testing.expectEqual(@as(u64, 1), try wal.append("alpha"));
    try std.testing.expectEqual(@as(u64, 2), try wal.append("beta"));
    try std.testing.expectEqual(@as(u64, 2), wal.lastLsn());

    const entries = try wal.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(@constCast(entry.data));
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("alpha", entries[0].data);
    try std.testing.expectEqualStrings("beta", entries[1].data);

    const second = (try wal.readAt(std.testing.allocator, 2)).?;
    defer std.testing.allocator.free(@constCast(second.data));
    try std.testing.expectEqualStrings("beta", second.data);
    try wal.truncate(1);
    try std.testing.expect((try wal.readAt(std.testing.allocator, 1)) == null);

    try std.testing.expectError(error.WalLsnMismatch, wal.appendAt(9, "must-not-append"));
    try wal.truncateAfter(1);
    try std.testing.expectEqual(@as(u64, 2), try wal.append("gamma"));
    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 3), stats.append_calls);
    try std.testing.expectEqual(@as(u64, 3), stats.logical_entries);
    try std.testing.expectError(error.Overflow, wal.truncateAfter(std.math.maxInt(u64)));

    const bootstrap_z = try std.testing.allocator.dupeZ(u8, bootstrap_path);
    defer std.testing.allocator.free(bootstrap_z);
    var bootstrap = try wal_client.WAL.open(bootstrap_z.ptr, TestWalOptions{});
    defer bootstrap.close();
    try std.testing.expectEqual(@as(u64, 7), try bootstrap.appendAt(7, "timeline"));

    const read_only_z = try std.testing.allocator.dupeZ(u8, read_only_path);
    defer std.testing.allocator.free(read_only_z);
    {
        var writable = try wal_client.WAL.open(read_only_z.ptr, TestWalOptions{});
        defer writable.close();
        _ = try writable.append("durable");
    }
    var read_only = try wal_client.WAL.open(read_only_z.ptr, TestWalOptions{ .read_only = true });
    defer read_only.close();
    try std.testing.expectError(error.ReadOnly, read_only.append("rejected"));

    try std.testing.expectError(
        error.UnsupportedKernelWalOptions,
        wal_client.WAL.open(path_z.ptr, TestWalOptions{ .backend = .lsm_memory }),
    );
}

test "opaque WAL idempotency survives truncation and reopen" {
    const path = "/tmp/antfly-storage-kernel-wal-idempotency";
    cleanup(path);
    defer cleanup(path);
    {
        var wal = try wal_client.WAL.open(path, wal_client.WalOptions{});
        defer wal.close();
        const first = try wal.appendIdempotent("receipt-1", "digest-a", "payload");
        try std.testing.expect(first.appended);
        try std.testing.expectEqual(@as(u64, 1), first.lsn);
        const retry = try wal.appendIdempotent("receipt-1", "digest-a", "payload");
        try std.testing.expect(!retry.appended);
        try std.testing.expectEqual(first.lsn, retry.lsn);
        try std.testing.expectError(error.IdempotencyConflict, wal.appendIdempotent("receipt-1", "digest-b", "changed"));
        try std.testing.expectError(error.InvalidIdempotencyKey, wal.appendIdempotent("", "digest", "payload"));
        _ = try wal.append("second");
        try wal.truncate(first.lsn);
        try std.testing.expect((try wal.readAt(std.testing.allocator, first.lsn)) == null);
    }
    var reopened = try wal_client.WAL.open(path, wal_client.WalOptions{});
    defer reopened.close();
    const retained = try reopened.appendIdempotent("receipt-1", "digest-a", "payload");
    try std.testing.expect(!retained.appended);
    try std.testing.expectEqual(@as(u64, 1), retained.lsn);
    try std.testing.expectEqual(@as(u64, 2), reopened.lastLsn());
    try std.testing.expectError(error.IdempotencyConflict, reopened.appendIdempotent("receipt-1", "digest-b", "changed"));
}

test "coarse aggregation ABI preserves results and semantic error identities" {
    const hit_bodies = [_][]const u8{
        "{\"category\":\"alpha\",\"price\":10}",
        "{\"category\":\"alpha\",\"price\":20}",
        "{\"category\":\"beta\",\"price\":30}",
    };
    var hits: [hit_bodies.len]client.AggregationHit = undefined;
    for (hit_bodies, 0..) |body, i| hits[i] = .{ .stored_data = .fromSlice(body) };

    const base = client.AggregationRequest{
        .total_hits = hit_bodies.len,
        .context_json = .fromSlice("{}"),
        .hits = &hits,
        .hit_count = hits.len,
    };
    var response = try client.aggregate(.{
        .total_hits = base.total_hits,
        .aggregations_json = .fromSlice("{\"by_category\":{\"type\":\"terms\",\"field\":\"category\",\"size\":10}}"),
        .context_json = base.context_json,
        .hits = base.hits,
        .hit_count = base.hit_count,
    });
    defer response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "by_category") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "beta") != null);

    try std.testing.expectError(error.InvalidAggregation, client.aggregate(.{
        .total_hits = base.total_hits,
        .aggregations_json = .fromSlice("{\"bad\":{\"type\":\"histogram\",\"field\":\"price\",\"interval\":0}}"),
        .context_json = base.context_json,
        .hits = base.hits,
        .hit_count = base.hit_count,
    }));
    try std.testing.expectError(error.UnsupportedAggregation, client.aggregate(.{
        .total_hits = base.total_hits,
        .aggregations_json = .fromSlice("{\"unsupported_without_text_context\":{\"type\":\"significant_terms\",\"field\":\"category\",\"size\":10}}"),
        .context_json = base.context_json,
        .hits = base.hits,
        .hit_count = base.hit_count,
    }));
}

test "opaque storage context owns Lite system namespaces auth and table owners" {
    const root = "/tmp/antfly-storage-kernel-context-lite";
    const lite_path = root ++ "/standalone.aflite";
    const auth_path = root ++ "/auth";
    cleanup(root);
    defer cleanup(root);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), root);

    var context = client.Context{};
    try context.ensureWith(.{
        .storage_kind = .lite,
        .storage_path = .fromSlice(lite_path),
        .auth_storage_path = .fromSlice(auth_path),
    });

    var catalog = try context.systemStore(std.testing.allocator, "system/metadata");
    var write = try catalog.beginWrite();
    try write.put("catalog", "{\"epoch\":2}");
    try write.commit();
    var read = try catalog.beginRead();
    try std.testing.expectEqualStrings("{\"epoch\":2}", try read.get("catalog"));
    read.abort();

    var auth_users = try context.systemStore(std.testing.allocator, "system/auth-users");
    var users = try client.singleNamespaceStore(
        std.testing.allocator,
        &auth_users,
        "usermgr_users",
    );
    var auth_write = try users.beginWrite();
    try auth_write.put(.{ .name = "usermgr_users" }, "userpass:admin", "hash");
    try auth_write.commit();
    var auth_read = try users.beginRead();
    var auth_cursor = try auth_read.openCursor(.{ .name = "usermgr_users" });
    const first = (try auth_cursor.first()).?;
    try std.testing.expectEqualStrings("userpass:admin", first.key);
    try std.testing.expectEqualStrings("hash", first.value);
    auth_cursor.close();
    auth_read.abort();

    var owner = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice("group-7001/table-db"),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    });
    var response = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}},\"sync_level\":\"full_index\"}",
    );
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "\"inserted\":1") != null);
    response.deinit();
    try std.testing.expectEqual(
        abi.Status.busy,
        abi.antfly_storage_context_destroy(context.handle),
    );

    const maintenance_status = context.maintenanceSource().status();
    try std.testing.expectEqualStrings("lite", maintenance_status.engine);
    try std.testing.expect(maintenance_status.maintenance.check);

    owner.deinit();
    users.deinit();
    auth_users.deinit();
    catalog.deinit();
    context.deinit();
}

test "opaque storage owner preserves source-vector policy and status across reopen" {
    const alloc = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("owner-source-vectors");
    defer directory.cleanup();
    const path = std.mem.span(directory.path().ptr);
    for ([_]abi.DenseEmbeddingStorage{ .vector_store, .persisted }, 0..) |policy, iteration| {
        var owner = try client.Owner.open(.{
            .path = .fromSlice(path),
            .table_name = .fromSlice("docs"),
            .group_id = 7001,
            .dense_embedding_storage = policy,
            .indexes_json = .fromSlice("{\"model\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}"),
        });
        defer owner.deinit();
        if (iteration == 0) {
            var response = try owner.batchJson("docs",
                \\{"inserts":{"a":{"_embeddings":{"model":[1,0,0]}}},"sync_level":"full_index"}
            );
            defer response.deinit();
        }
        // Observe immediately after reopen, before a query or write could
        // initialize a missing source store or repair its cached accounting.
        var response = try ownerStatusEventually(&owner);
        defer response.deinit();
        const Status = struct {
            source_vectors: ?struct { retained_payloads: u64 } = null,
        };
        var parsed = try std.json.parseFromSlice(Status, alloc, response.bytes(), .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const source = parsed.value.source_vectors orelse return error.MissingSourceVectorStatus;
        try std.testing.expect(source.retained_payloads > 0);
    }
    var invalid_owner: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_open(&.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .dense_embedding_storage = @enumFromInt(999),
    }, &invalid_owner));
    try std.testing.expect(invalid_owner == null);
}

test "opaque storage owner fences exact source targets before acknowledging writes" {
    const Observer = struct {
        calls: std.atomic.Value(usize) = .init(0),
        additive: std.atomic.Value(bool) = .init(false),
        reducing: std.atomic.Value(bool) = .init(false),
        invalid: std.atomic.Value(bool) = .init(false),
        fn notify(ptr: ?*anyopaque, table: abi.BorrowedBytes, group: u64, sequence: u64, has_sequence: u8, json: abi.BorrowedBytes) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (!std.mem.eql(u8, table.slice(), "docs") or group != 7001 or has_sequence == 0 or sequence == 0) self.invalid.store(true, .release);
            var targets = std.json.parseFromSlice([]@import("db/types.zig").IndexTargetVisibility, std.heap.page_allocator, json.slice(), .{}) catch {
                self.invalid.store(true, .release);
                return;
            };
            defer targets.deinit();
            for (targets.value) |target| {
                if (!std.mem.eql(u8, target.index_name, "model")) continue;
                switch (target.serving_set_effect) {
                    .additive_only => self.additive.store(true, .release),
                    .may_reduce => self.reducing.store(true, .release),
                }
            }
            _ = self.calls.fetchAdd(1, .release);
        }
    };
    var observer = Observer{};
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("owner-target-observer");
    defer directory.cleanup();
    var owner = try client.Owner.open(.{
        .path = .fromSlice(std.mem.span(directory.path().ptr)),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
        .indexes_json = .fromSlice("{\"model\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}"),
        .target_observer = .{ .ctx = &observer, .notify = Observer.notify },
    });
    defer owner.deinit();
    var inserted = try owner.batchJson("docs", "{\"inserts\":{\"a\":{\"_embeddings\":{\"model\":[1,0,0]}}},\"sync_level\":\"full_index\"}");
    defer inserted.deinit();
    try std.testing.expect(observer.calls.load(.acquire) > 0);
    try std.testing.expect(observer.additive.load(.acquire));
    var deleted = try owner.batchJson("docs", "{\"deletes\":[\"a\"],\"sync_level\":\"full_index\"}");
    defer deleted.deinit();
    try std.testing.expect(observer.reducing.load(.acquire));
    try std.testing.expect(!observer.invalid.load(.acquire));
}

test "opaque storage owner schedules source verification after reopen without traffic" {
    const alloc = std.testing.allocator;
    const time = @import("antfly_platform").time;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("owner-source-maintenance");
    defer directory.cleanup();
    const path = std.mem.span(directory.path().ptr);
    {
        var owner = try client.Owner.open(.{
            .path = .fromSlice(path),
            .table_name = .fromSlice("docs"),
            .group_id = 7001,
            .dense_embedding_storage = .vector_store,
            .indexes_json = .fromSlice("{\"model\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}"),
        });
        defer owner.deinit();
        var response = try owner.batchJson("docs",
            \\{"inserts":{"a":{"title":"retained","_embeddings":{"model":[1,0,0]}}},"sync_level":"full_index"}
        );
        defer response.deinit();
    }
    // Reopen with persisted policy and no requests that explicitly drain
    // maintenance. Status observation must not be required to perform GC.
    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
    });
    defer owner.deinit();
    const deadline = time.monotonicNs() + 15 * std.time.ns_per_s;
    while (time.monotonicNs() < deadline) {
        var response = try ownerStatusEventually(&owner);
        defer response.deinit();
        const Status = struct {
            source_vectors: ?struct { collections: u64, retained_payloads: u64, live_payloads_at_collection: u64 } = null,
        };
        var parsed = try std.json.parseFromSlice(Status, alloc, response.bytes(), .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.source_vectors) |source| {
            if (source.collections > 0) {
                try std.testing.expectEqual(@as(u64, 1), source.retained_payloads);
                try std.testing.expectEqual(@as(u64, 1), source.live_payloads_at_collection);
                return;
            }
        }
        try std.testing.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SourceVerificationDidNotRun;
}

fn ownerStatusEventually(owner: *client.Owner) !client.Response {
    const time = @import("antfly_platform").time;
    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        return owner.runtimeStatusJson("docs") catch |err| switch (err) {
            error.StorageBusy => {
                if (time.monotonicNs() >= deadline) return err;
                try std.testing.io.sleep(.fromMilliseconds(2), .awake);
                continue;
            },
            else => return err,
        };
    }
}

test "opaque storage owner preserves dense profiles and captured identity" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-owner-dense-profile";
    cleanup(path);
    defer cleanup(path);
    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
        .indexes_json = .fromSlice("{\"vec\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}"),
    });
    defer owner.deinit();
    var batch = try owner.batchJson("docs",
        \\{"inserts":{"doc:a":{"_embeddings":{"vec":[1,0,0]}},"doc:b":{"_embeddings":{"vec":[0,1,0]}}},"sync_level":"full_index"}
    );
    defer batch.deinit();
    var identity: ?u64 = null;
    for ([_]bool{ false, true }) |profile| {
        const request_json = try std.fmt.allocPrint(alloc, "{{\"embeddings\":{{\"vec\":[1,0,0]}},\"indexes\":[\"vec\"],\"limit\":2,\"fields\":[],\"profile\":{}}}", .{profile});
        defer alloc.free(request_json);
        var response = try owner.queryJson("docs", request_json);
        defer response.deinit();
        if (response.identityReadGeneration() == null) return error.MissingCapturedIdentity;
        if (identity) |expected| try std.testing.expectEqual(expected, response.identityReadGeneration().?);
        identity = response.identityReadGeneration();
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.bytes(), .{});
        defer parsed.deinit();
        const body = parsed.value.object.get("responses").?.array.items[0].object;
        const hits = body.get("hits").?.object.get("hits").?.array.items;
        try std.testing.expectEqual(@as(usize, 2), hits.len);
        try std.testing.expectEqualStrings("doc:a", hits[0].object.get("_id").?.string);
        try std.testing.expectEqualStrings("doc:b", hits[1].object.get("_id").?.string);
        if (profile) {
            const profile_value = body.get("profile") orelse return error.MissingQueryProfile;
            const dense = (profile_value.object.get("dense_search") orelse return error.MissingDenseProfile).object;
            if (dense.get("search_route").?.string.len == 0) return error.MissingDenseRoute;
            try std.testing.expectEqual(@as(i64, 2), dense.get("returned_hit_count").?.integer);
            try std.testing.expectEqual(@as(i64, 0), dense.get("hbc_leaf_payload_stale").?.integer);
            if (!dense.contains("hbc_rerank_read_adaptive_inline_batches")) return error.MissingAdaptiveInlineCounter;
            if (!dense.contains("hbc_rerank_read_adaptive_wide_batches")) return error.MissingAdaptiveWideCounter;
            if (!dense.contains("hbc_rerank_read_adaptive_probe_ns")) return error.MissingAdaptiveProbeCounter;
        } else {
            if (body.get("profile")) |value| if (value != .null) return error.UnexpectedUnprofiledMetadata;
        }
    }
}

test "opaque storage owner performs coarse batch and query on one live DB" {
    const path = "/tmp/antfly-storage-kernel-owner-batch-query";
    const backup_root = "/tmp/antfly-storage-kernel-owner-backups";
    cleanup(path);
    cleanup(backup_root);
    defer cleanup(path);
    defer cleanup(backup_root);

    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 7001,
        .lsm_root_generation = 0,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    });
    defer owner.deinit();

    var duplicate_owner: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.lsm_root_writer_already_open, abi.antfly_storage_owner_open(&.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    }, &duplicate_owner));
    try std.testing.expect(duplicate_owner == null);

    const batch_json =
        \\{"inserts":{"doc:a":{"title":"alpha"},"doc:b":{"title":"beta"}},"sync_level":"full_index"}
    ;
    var batch_response = try owner.batchJson("docs", batch_json);
    defer batch_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, batch_response.bytes(), "\"inserted\":2") != null);

    // Streaming crosses the real provider archive, retains backpressure and
    // propagates caller-owned errors without passing Zig error-set ordinals.
    const ScanCapture = struct {
        starts: usize = 0,
        rows: usize = 0,
        stop_on_start: bool = false,
        stop_on_row: bool = false,
        fn start(ptr: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.starts += 1;
            if (self.stop_on_start) return error.ConsumerStoppedBeforeScan;
        }
        fn write(ptr: ?*anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            try std.testing.expect(std.mem.endsWith(u8, bytes, "\n"));
            self.rows += 1;
            if (self.stop_on_row) return error.ConsumerStoppedAfterRow;
        }
        fn sink(self: *@This()) @import("../runtime_scan_sink.zig").ScanStreamSink {
            return .{ .context = self, .start_fn = start, .write_fn = write };
        }
    };
    const scan_json = "{\"from_key\":\"\",\"to_key\":\"\",\"include_documents\":true}";
    var capture: ScanCapture = .{};
    try owner.scanStream("docs", scan_json, capture.sink());
    try std.testing.expectEqual(@as(usize, 1), capture.starts);
    try std.testing.expectEqual(@as(usize, 2), capture.rows);
    capture = .{ .stop_on_start = true };
    try std.testing.expectError(error.ConsumerStoppedBeforeScan, owner.scanStream("docs", scan_json, capture.sink()));
    try std.testing.expectEqual(@as(usize, 0), capture.rows);
    capture = .{ .stop_on_row = true };
    try std.testing.expectError(error.ConsumerStoppedAfterRow, owner.scanStream("docs", scan_json, capture.sink()));
    try std.testing.expectEqual(@as(usize, 1), capture.rows);
    const CancelScan = struct {
        fn always(_: ?*anyopaque) callconv(.c) u8 {
            return 1;
        }
        fn afterRow(ptr: ?*anyopaque) callconv(.c) u8 {
            const target: *ScanCapture = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(target.rows != 0);
        }
    };
    capture = .{};
    try std.testing.expectError(error.Canceled, owner.scanStreamWithOptions("docs", scan_json, capture.sink(), .{ .cancellation_fn = CancelScan.always }));
    try std.testing.expectEqual(@as(usize, 0), capture.starts);
    try std.testing.expectError(error.DeadlineExceeded, owner.scanStreamWithOptions("docs", scan_json, capture.sink(), .{ .execution_deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 0), capture.starts);
    try std.testing.expectError(error.Canceled, owner.scanNdjsonWithOptions("docs", scan_json, .{ .cancellation_fn = CancelScan.always }));
    try std.testing.expectError(error.DeadlineExceeded, owner.scanNdjsonWithOptions("docs", scan_json, .{ .execution_deadline_ns = 0 }));
    try std.testing.expectError(error.Canceled, owner.scanStreamWithOptions("docs", scan_json, capture.sink(), .{ .cancellation_ctx = &capture, .cancellation_fn = CancelScan.afterRow }));
    try std.testing.expectEqual(@as(usize, 1), capture.rows);
    capture = .{};
    try std.testing.expectError(error.RelationalTableRequired, owner.scanStream("docs",
        \\{"from_key":"","to_key":"","include_documents":true,"relational_query_json":"{\"fields\":[]}"}
    , capture.sink()));
    // Failed typed preparation must never become a successful empty HTTP stream.
    try std.testing.expectEqual(@as(usize, 0), capture.starts);
    try std.testing.expectError(error.InvalidGraphMetricAction, owner.graphMetricMaintenanceJson("docs", "{\"operation\":\"metric_action_v1\"}"));

    // Status reads deliberately avoid waiting under the owner lease for a
    // background writer. A full-index acknowledgement does not make the next
    // observational read uncontended, so retry this transient status here.
    var status_response = status: {
        const time = @import("antfly_platform").time;
        const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
        var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
        defer io_impl.deinit();
        while (true) {
            break :status owner.runtimeStatusJson("docs") catch |err| switch (err) {
                error.StorageBusy => {
                    if (time.monotonicNs() >= deadline) return err;
                    try io_impl.io().sleep(.fromMilliseconds(2), .awake);
                    continue;
                },
                else => return err,
            };
        }
    };
    defer status_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, status_response.bytes(), "\"source_doc_count\":2") != null);

    var observed_response = try owner.observedDynamicFieldCapabilitySetsJson(
        "docs",
        "{\"fields\":[],\"coverage_read_mode\":\"cached_only\"}",
        null,
        null,
        null,
    );
    defer observed_response.deinit();
    try std.testing.expectEqualStrings("[]", observed_response.bytes());

    const maintenance = try owner.maintenance("docs", .inspect);
    try std.testing.expectEqual(abi.abi_version, maintenance.version);
    try std.testing.expectEqual(@as(u8, 0), maintenance.progressed);
    try std.testing.expectError(error.InvalidArgument, owner.maintenance("articles", .inspect));
    const invalid_maintenance = abi.MaintenanceRequest{
        .action = std.math.maxInt(u32),
        .table_name = .fromSlice("docs"),
    };
    var invalid_maintenance_result: abi.MaintenanceResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_maintenance(owner.handle, &invalid_maintenance, &invalid_maintenance_result),
    );

    var replicated_response = try owner.replicatedBatchJson(
        "docs",
        "{\"inserts\":{\"doc:c\":{\"title\":\"gamma\"}},\"sync_level\":\"full_index\"}",
    );
    defer replicated_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, replicated_response.bytes(), "\"inserted\":1") != null);

    // Exact Raft-entry apply must preserve the physical idempotence fence
    // across the opaque storage-owner ABI. In particular, replaying a
    // non-idempotent transform at the same term/index must be a no-op.
    var raft_insert_response = try owner.replicatedBatchAtRaftEntryJson(
        "docs",
        "{\"inserts\":{\"doc:raft-counter\":{\"count\":0}},\"sync_level\":\"write\"}",
        3,
        40,
    );
    defer raft_insert_response.deinit();
    var raft_increment_response = try owner.replicatedBatchAtRaftEntryJson(
        "docs",
        "{\"transforms\":[{\"key\":\"doc:raft-counter\",\"operations\":[{\"op\":\"$inc\",\"path\":\"count\",\"value\":1}]}],\"sync_level\":\"write\"}",
        3,
        41,
    );
    defer raft_increment_response.deinit();
    var raft_replay_response = try owner.replicatedBatchAtRaftEntryJson(
        "docs",
        "{\"transforms\":[{\"key\":\"doc:raft-counter\",\"operations\":[{\"op\":\"$inc\",\"path\":\"count\",\"value\":1}]}],\"sync_level\":\"write\"}",
        3,
        41,
    );
    defer raft_replay_response.deinit();
    var raft_counter = try owner.lookupJson(
        "docs",
        "{\"key\":\"doc:raft-counter\",\"include_all_fields\":true}",
    );
    defer raft_counter.deinit();
    try std.testing.expect(std.mem.indexOf(u8, raft_counter.bytes(), "\"count\":1") != null);
    try std.testing.expectError(
        error.InvalidArgument,
        owner.replicatedBatchAtRaftEntryJson("docs", "{}", 0, 42),
    );
    try std.testing.expectError(error.InvalidBatchRequest, owner.batchJson("docs", "{"));
    try std.testing.expectError(error.InvalidBatchRequest, owner.replicatedBatchJson("docs", "{"));
    try owner.waitForSync("docs", .full_index);
    try owner.applyHAReplicationRecord("docs", .{
        .record_kind = 0x0012,
        .payload_codec = 0,
        .cluster_id = 1,
        .shard_id = 7001,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
    });

    const txn_id: [16]u8 = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    var txn_begin = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"begin\",\"txn_id\":\"000102030405060708090a0b0c0d0e0f\",\"begin_timestamp\":\"42\",\"created_at_ns\":\"43\",\"topology_epoch\":\"7\",\"participants\":[\"table2:00000004:docs:7001\"]},\"sync_level\":\"write\"}",
    );
    defer txn_begin.deinit();
    try std.testing.expectEqual(abi.TxnStatus.pending, try owner.transactionStatus("docs", txn_id));
    var txn_prepare = try owner.replicatedBatchJson(
        "docs",
        "{\"inserts\":{\"doc:txn\":{\"title\":\"transactional\"}},\"_transaction\":{\"phase\":\"prepare\",\"txn_id\":\"000102030405060708090a0b0c0d0e0f\",\"topology_epoch\":\"7\"},\"sync_level\":\"write\"}",
    );
    defer txn_prepare.deinit();
    var txn_resolve = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"resolve\",\"txn_id\":\"000102030405060708090a0b0c0d0e0f\",\"status\":\"committed\",\"commit_version\":\"44\"},\"sync_level\":\"full_index\"}",
    );
    defer txn_resolve.deinit();
    try std.testing.expectEqual(abi.TxnStatus.committed, try owner.transactionStatus("docs", txn_id));

    const query_json =
        \\{"query":{"match_all":{}},"limit":10}
    ;
    try std.testing.expectError(error.Timeout, owner.queryJsonWithOptions("docs", query_json, .{
        .execution_deadline_ns = 1,
    }));
    const MidQueryCancellation = struct {
        checks: usize = 0,
        fn requested(ctx: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.checks += 1;
            // First admission succeeds; cancellation changes as the request
            // crosses from the owner into the physical query provider.
            return @intFromBool(self.checks >= 2);
        }
    };
    var cancellation: MidQueryCancellation = .{};
    try std.testing.expectError(error.Cancelled, owner.queryJsonWithOptions("docs", query_json, .{
        .cancellation_ctx = &cancellation,
        .cancellation_fn = MidQueryCancellation.requested,
    }));
    try std.testing.expect(cancellation.checks >= 2);
    // A canceled operation must release all borrowed controls and read state.
    var after_cancel = try owner.queryJson("docs", query_json);
    after_cancel.deinit();

    try std.testing.expectError(error.InvalidArgument, owner.queryJson("articles", query_json));
    try std.testing.expectError(error.InvalidQueryRequest, owner.queryJson("docs", "{"));

    // The nested distributed -> storage-owner -> local-query path must retain
    // both the semantic status and its originating stage, not merely rethrow a
    // broad failure after the inner provider unwinds.
    var invalid_query_response: abi.QueryOwnedResponse = .{};
    var invalid_query_failure: abi.FailureIdentity = .{};
    const invalid_query_status = abi.antfly_storage_owner_query_json(
        owner.handle,
        &.{ .control = .{
            .table_name = .fromSlice("docs"),
            .request_json = .fromSlice("{"),
        } },
        &invalid_query_response,
        &invalid_query_failure,
    );
    try std.testing.expectEqual(abi.Status.invalid_query, invalid_query_status);
    try std.testing.expectEqual(invalid_query_status, invalid_query_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, invalid_query_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, invalid_query_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.parse_internal_request),
        invalid_query_failure.operation,
    );
    try std.testing.expectEqualStrings("InvalidQueryRequest", invalid_query_failure.errorName());
    try std.testing.expect(invalid_query_failure.error_name_hash != 0);
    try std.testing.expectEqual(@as(u64, 0), invalid_query_response.buffer.len);

    var invalid_abi_response: abi.QueryOwnedResponse = .{};
    var invalid_abi_failure: abi.FailureIdentity = .{};
    var invalid_abi_request = abi.LocalQueryRequest{};
    invalid_abi_request.version = abi.abi_version - 1;
    const invalid_abi_status = abi.antfly_local_query_execute(
        &invalid_abi_request,
        &invalid_abi_response,
        &invalid_abi_failure,
    );
    try std.testing.expectEqual(abi.Status.invalid_abi, invalid_abi_status);
    try std.testing.expectEqual(invalid_abi_status, invalid_abi_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, invalid_abi_failure.boundary);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.validate_request),
        invalid_abi_failure.operation,
    );
    try std.testing.expectEqualStrings("InvalidAbiVersion", invalid_abi_failure.errorName());

    // Every operation family crossing the storage-owner boundary carries the
    // same complete envelope even when it remains in the storage unit.
    var operation_response: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&operation_response);
    var operation_failure: abi.FailureIdentity = .{};
    const invalid_algebraic_status = abi.antfly_storage_owner_algebraic_partials_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_algebraic_status != .ok);
    try std.testing.expectEqual(invalid_algebraic_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.algebraic_partials),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_text_stats_status = abi.antfly_storage_owner_text_stats_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_text_stats_status != .ok);
    try std.testing.expectEqual(invalid_text_stats_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.text_stats),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_preflight_status = abi.antfly_storage_owner_preflight_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_preflight_status != .ok);
    try std.testing.expectEqual(invalid_preflight_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.preflight),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_graph_status = abi.antfly_storage_owner_graph_expand_json(
        owner.handle,
        &.{ .table_name = .fromSlice("docs"), .request_json = .fromSlice("{") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_graph_status != .ok);
    try std.testing.expectEqual(invalid_graph_status, operation_failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.local_query, operation_failure.boundary);
    try std.testing.expectEqual(abi.abi_version, operation_failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.parse_graph_expand),
        operation_failure.operation,
    );
    try std.testing.expect(operation_failure.error_name_hash != 0);

    const invalid_aggregation_status = abi.antfly_storage_aggregate_json(
        &.{ .context_json = .fromSlice("{"), .aggregations_json = .fromSlice("[]") },
        &operation_response,
        &operation_failure,
    );
    try std.testing.expect(invalid_aggregation_status != .ok);
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, operation_failure.boundary);
    try std.testing.expectEqual(
        @intFromEnum(abi.LocalQueryOperation.parse_aggregation),
        operation_failure.operation,
    );

    var query_response = try owner.queryJson("docs", query_json);
    defer query_response.deinit();
    try std.testing.expect(query_response.identityReadGeneration() != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "docs") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:b") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_response.bytes(), "doc:txn") != null);

    var reconciled = false;
    for (0..64) |_| {
        const result = try owner.reconcile(
            "docs",
            "",
            "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
            "full_text_index_v1",
            true,
        );
        try std.testing.expect(result.state != .degraded);
        if (result.state == .complete) {
            reconciled = true;
            break;
        }
    }
    try std.testing.expect(reconciled);
    var text_response = try owner.queryJson(
        "docs",
        "{\"full_text_search\":{\"match\":\"alpha\",\"field\":\"title\"},\"indexes\":[\"full_text_index_v1\"],\"limit\":10}",
    );
    defer text_response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, text_response.bytes(), "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_response.bytes(), "doc:b") == null);

    try std.testing.expectError(
        error.InvalidArgument,
        owner.configure("articles", "", "{}"),
    );
    const replacement_indexes_json =
        \\{"dense_idx":{"type":"embeddings","external":true,"dimension":3},
        \\ "full_text_index_v0":{"type":"full_text","enrichments":[{"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"}]}}
    ;
    try owner.configure("docs", "", replacement_indexes_json);
    var dense_reconciled = false;
    for (0..64) |_| {
        const result = try owner.reconcile(
            "docs",
            "",
            replacement_indexes_json,
            "dense_idx",
            true,
        );
        try std.testing.expect(result.state != .degraded);
        if (result.state == .complete) {
            dense_reconciled = true;
            break;
        }
    }
    try std.testing.expect(dense_reconciled);
    // A target reconcile cannot replace its sibling dense index, even when
    // the desired catalog carries a newer sibling definition.
    const sibling_changed = try std.mem.replaceOwned(u8, std.testing.allocator, replacement_indexes_json, "\"dimension\":3", "\"dimension\":4");
    defer std.testing.allocator.free(sibling_changed);
    _ = try owner.reconcile("docs", "", sibling_changed, "full_text_index_v0", false);

    var indexed_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:artifact\":{\"title\":\"artifact\",\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"},\"doc:c\":{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[1,0,0]}},\"doc:d\":{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}},\"sync_level\":\"full_index\"}",
    );
    defer indexed_batch.deinit();
    var text_memory = try owner.textMemoryJson("docs");
    defer text_memory.deinit();
    try std.testing.expect(std.mem.indexOf(u8, text_memory.bytes(), "\"text_indexes\":1") != null);
    var dense_response = try owner.queryJson(
        "docs",
        "{\"embeddings\":{\"dense_idx\":[1,0,0]},\"indexes\":[\"dense_idx\"],\"limit\":2}",
    );
    defer dense_response.deinit();
    const doc_c = std.mem.indexOf(u8, dense_response.bytes(), "doc:c") orelse return error.MissingDenseHit;
    const doc_d = std.mem.indexOf(u8, dense_response.bytes(), "doc:d") orelse return error.MissingDenseHit;
    try std.testing.expect(doc_c < doc_d);

    var reprocessed = try owner.artifactOperationJson(
        "docs",
        .reprocess_document,
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\"}",
        null,
        null,
        false,
    );
    defer reprocessed.deinit();
    try std.testing.expect(std.mem.indexOf(u8, reprocessed.bytes(), "\"handled\":true") != null);

    var reprocessed_range = try owner.artifactOperationJson(
        "docs",
        .reprocess_document_range,
        "{\"artifact_name\":\"document_units_v1\",\"request\":{\"from_key\":\"doc:artifact\",\"to_key\":\"doc:b\",\"limit\":1}}",
        null,
        null,
        false,
    );
    defer reprocessed_range.deinit();
    try std.testing.expect(std.mem.indexOf(u8, reprocessed_range.bytes(), "\"reprocessed\":1") != null);

    var placement = try owner.artifactOperationJson(
        "docs",
        .update_child_range_placement,
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\",\"update\":{\"range_id\":\"range:000000\",\"placement\":\"remote\",\"owner_group_id\":7002,\"placement_generation\":3,\"route_status\":\"remote_committed\",\"split_eligible\":true}}",
        null,
        null,
        false,
    );
    defer placement.deinit();
    try std.testing.expect(std.mem.indexOf(u8, placement.bytes(), "\"handled\":true") != null);

    const ChildRangeCapture = struct {
        calls: usize = 0,
        owner_group_id: u64 = 0,
        saw_document: bool = false,
        saw_artifact: bool = false,

        fn dispatch(
            ptr: ?*anyopaque,
            owner_group_id: u64,
            request_json: abi.BorrowedBytes,
        ) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
            self.calls += 1;
            self.owner_group_id = owner_group_id;
            self.saw_document = std.mem.indexOf(u8, request_json.slice(), "\"doc_key\":\"doc:artifact\"") != null;
            self.saw_artifact = std.mem.indexOf(u8, request_json.slice(), "\"artifact_name\":\"document_units_v1\"") != null;
            return .ok;
        }
    };
    var child_range_capture = ChildRangeCapture{};
    var routed_batch = try owner.batchJsonWithDocumentChildRangeDispatcher(
        "docs",
        "{\"inserts\":{\"doc:artifact\":{\"title\":\"artifact updated\",\"url\":\"data:text/plain;base64,YmV0YQ==\"}},\"sync_level\":\"full_index\"}",
        &child_range_capture,
        ChildRangeCapture.dispatch,
    );
    defer routed_batch.deinit();
    try std.testing.expectEqual(@as(usize, 1), child_range_capture.calls);
    try std.testing.expectEqual(@as(u64, 7002), child_range_capture.owner_group_id);
    try std.testing.expect(child_range_capture.saw_document);
    try std.testing.expect(child_range_capture.saw_artifact);

    var empty_child_batch = try owner.artifactOperationJson(
        "docs",
        .apply_child_range_batch,
        "{\"doc_key\":\"doc:artifact\",\"artifact_name\":\"document_units_v1\",\"batch\":{}}",
        null,
        null,
        false,
    );
    defer empty_child_batch.deinit();
    try std.testing.expect(std.mem.indexOf(u8, empty_child_batch.bytes(), "\"sequence\":0") != null);

    var corrupted = try owner.artifactOperationJson(
        "docs",
        .corrupt_embedding,
        "{\"doc_key\":\"doc:c\",\"index_name\":\"dense_idx\"}",
        null,
        null,
        false,
    );
    defer corrupted.deinit();
    var repair_issues = try owner.artifactOperationJson(
        "docs",
        .list_repair_issues,
        "{\"artifact_kind\":\"embedding\",\"limit\":10}",
        null,
        null,
        false,
    );
    defer repair_issues.deinit();
    try std.testing.expect(std.mem.indexOf(u8, repair_issues.bytes(), "\"issues\"") != null);

    const Cancel = struct {
        fn requested(_: ?*anyopaque) callconv(.c) u8 {
            return 1;
        }
    };
    try std.testing.expectError(error.Canceled, owner.artifactOperationJson(
        "docs",
        .repair_issues,
        "{\"artifact_kind\":\"embedding\",\"limit\":10}",
        null,
        Cancel.requested,
        false,
    ));
    var repaired = try owner.artifactOperationJson(
        "docs",
        .repair_issues,
        "{\"artifact_kind\":\"embedding\",\"limit\":10}",
        null,
        null,
        false,
    );
    defer repaired.deinit();
    try std.testing.expect(std.mem.indexOf(u8, repaired.bytes(), "\"scanned\":0") != null);

    try std.testing.expectError(error.InvalidArgument, owner.beginBulkIngest("articles"));
    try owner.beginBulkIngest("docs");
    var bulk_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:bulk\":{\"title\":\"bulk\"}},\"sync_level\":\"write\"}",
    );
    defer bulk_batch.deinit();
    try owner.finishBulkIngest(&.{
        .compact = 0,
        .table_name = .fromSlice("docs"),
    });
    var bulk_query = try owner.queryJson(
        "docs",
        "{\"query\":{\"match_all\":{}},\"limit\":10}",
    );
    defer bulk_query.deinit();
    try std.testing.expect(std.mem.indexOf(u8, bulk_query.bytes(), "doc:bulk") != null);

    try owner.beginBulkIngest("docs");
    try owner.abortBulkIngest("docs");
    var post_abort_batch = try owner.batchJson(
        "docs",
        "{\"inserts\":{\"doc:after-abort\":{\"title\":\"ordinary\"}},\"sync_level\":\"full_index\"}",
    );
    defer post_abort_batch.deinit();

    var portable_backup = try owner.backupJson(
        "docs",
        backup_root,
        "portable-owner",
        .portable,
    );
    defer portable_backup.deinit();
    try std.testing.expect(std.mem.indexOf(u8, portable_backup.bytes(), "\"group_id\":7001") != null);
    try std.testing.expect(std.mem.indexOf(u8, portable_backup.bytes(), "portable-owner/groups/7001.afb") != null);
    try std.testing.expect(std.mem.indexOf(u8, portable_backup.bytes(), "\"artifact_sha256\"") != null);

    var native_backup = try owner.backupJson(
        "docs",
        backup_root,
        "native-owner",
        .native,
    );
    defer native_backup.deinit();
    try std.testing.expect(std.mem.indexOf(u8, native_backup.bytes(), "\"group_id\":7001") != null);
    try std.testing.expect(std.mem.indexOf(u8, native_backup.bytes(), "native-owner") != null);
    try std.testing.expectError(
        error.InvalidArgument,
        owner.backupJson("articles", backup_root, "wrong-table", .native),
    );
}

test "opaque storage owner validates ABI and destruction is idempotent" {
    var invalid_context: ?*anyopaque = undefined;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_context_create(&.{ .version = abi.abi_version + 1 }, &invalid_context),
    );
    try std.testing.expect(invalid_context == null);
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_destroy(null));

    var owner: ?*anyopaque = undefined;
    var invalid: abi.OpenRequest = .{};
    invalid.version = abi.abi_version + 1;
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_open(&invalid, &owner));
    try std.testing.expect(owner == null);

    var invalid_configure: abi.ConfigureRequest = .{};
    invalid_configure.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_configure(null, &invalid_configure),
    );

    var invalid_reconcile: abi.ReconcileRequest = .{};
    invalid_reconcile.version = abi.abi_version + 1;
    var reconcile_result: abi.ReconcileResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_reconcile(null, &invalid_reconcile, &reconcile_result),
    );
    try std.testing.expectEqual(abi.abi_version, reconcile_result.version);

    var invalid_table: abi.TableRequest = .{};
    invalid_table.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_bulk_begin(null, &invalid_table),
    );
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_bulk_abort(null, &invalid_table),
    );
    var invalid_txn_status: abi.TransactionStatusRequest = .{};
    invalid_txn_status.version = abi.abi_version + 1;
    var txn_status_result: abi.TransactionStatusResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_transaction_status(null, &invalid_txn_status, &txn_status_result),
    );
    try std.testing.expectEqual(abi.abi_version, txn_status_result.version);
    var invalid_bulk_finish: abi.BulkFinishRequest = .{};
    invalid_bulk_finish.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_bulk_finish(null, &invalid_bulk_finish),
    );
    var invalid_maintenance: abi.MaintenanceRequest = .{};
    invalid_maintenance.version = abi.abi_version + 1;
    var maintenance_result: abi.MaintenanceResult = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_maintenance(null, &invalid_maintenance, &maintenance_result),
    );
    try std.testing.expectEqual(abi.abi_version, maintenance_result.version);
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_maintenance(null, &.{}, &maintenance_result),
    );

    var response: abi.OwnedBytes = .{};
    var invalid_batch_operation: abi.BatchJsonOperationRequest = .{
        .table_name = .fromSlice("docs"),
        .request_json = .fromSlice("{}"),
    };
    invalid_batch_operation.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_batch_json(null, &invalid_batch_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    var invalid_operation: abi.JsonOperationRequest = .{
        .table_name = .fromSlice("docs"),
        .request_json = .fromSlice("{}"),
    };
    invalid_operation.version = abi.abi_version + 1;
    var invalid_sync: abi.SyncRequest = .{};
    invalid_sync.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_wait_for_sync(null, &invalid_sync),
    );
    var invalid_ha: abi.HAReplicationRecordRequest = .{};
    invalid_ha.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_apply_ha_replication_record(null, &invalid_ha),
    );
    var invalid_backup: abi.BackupRequest = .{};
    invalid_backup.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_backup_json(null, &invalid_backup, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_backup_format = abi.BackupRequest{ .format = std.math.maxInt(u32) };
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_backup_json(null, &invalid_backup_format, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    var snapshot: ?*anyopaque = undefined;
    var invalid_snapshot: abi.SnapshotPrepareRequest = .{};
    invalid_snapshot.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_snapshot_prepare(&invalid_snapshot, &snapshot),
    );
    try std.testing.expect(snapshot == null);
    var invalid_restore: abi.RestorePrepareRequest = .{};
    invalid_restore.version = abi.abi_version + 1;
    var restore_result: abi.RestorePrepareResult = .{ .snapshot = @ptrFromInt(1) };
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_restore_prepare(&invalid_restore, &restore_result),
    );
    try std.testing.expectEqual(abi.abi_version, restore_result.version);
    try std.testing.expect(restore_result.snapshot == null);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_restore_reconcile(&invalid_restore),
    );
    var invalid_restore_bootstrap: abi.RestoreBootstrapRequest = .{};
    invalid_restore_bootstrap.version = abi.abi_version + 1;
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_restore_apply_bootstrap(&invalid_restore_bootstrap),
    );
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_restore_repair(null, &invalid_restore),
    );
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_snapshot_promote(null));
    var snapshot_publish_result: abi.SnapshotPublishResult = .{ .durability_uncertain = 1 };
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_snapshot_publish_prepared(null, &snapshot_publish_result),
    );
    try std.testing.expectEqual(@as(u8, 0), snapshot_publish_result.durability_uncertain);
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_snapshot_commit(null));
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_snapshot_rollback(null));
    abi.antfly_storage_snapshot_destroy(null);
    abi.antfly_storage_snapshot_destroy(null);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_replicated_batch_json(null, &invalid_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    var query_failure: abi.FailureIdentity = .{};
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_preflight_json(null, &invalid_operation, &response, &query_failure),
    );
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, query_failure.boundary);
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_text_stats_json(null, &invalid_operation, &response, &query_failure),
    );
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, query_failure.boundary);
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_algebraic_partials_json(null, &invalid_operation, &response, &query_failure),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_controlled = abi.ControlledJsonOperationRequest{ .version = abi.abi_version + 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_expand_json(null, &invalid_controlled, &response, &query_failure));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_hydrate_json(null, &invalid_controlled, &response, &query_failure));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_graph_edges_json(null, &invalid_controlled, &response, &query_failure));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_document_artifact_manifest_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_document_artifact_manifests_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_artifact_operation = abi.ArtifactOperationRequest{ .version = abi.abi_version + 1 };
    try std.testing.expectEqual(
        abi.Status.invalid_abi,
        abi.antfly_storage_owner_artifact_operation_json(null, &invalid_artifact_operation, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    const invalid_artifact_tag = abi.ArtifactOperationRequest{ .operation = std.math.maxInt(u32) };
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_owner_artifact_operation_json(null, &invalid_artifact_tag, &response),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_runtime_status_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_restore_state_json(null, &invalid_operation, &response));
    try std.testing.expectEqual(@as(u64, 0), response.len);

    try std.testing.expectError(error.InvalidQueryRequest, client.statusToError(.invalid_query));
    try std.testing.expectError(error.UnsupportedQueryRequest, client.statusToError(.unsupported_query));
    try std.testing.expectError(error.IndexNotFound, client.statusToError(.index_not_found));
    try std.testing.expectError(error.IdentityReadGenerationChanged, client.statusToError(.identity_read_generation_changed));
    try std.testing.expectError(error.Timeout, client.statusToError(.timeout));
    try std.testing.expectError(error.Cancelled, client.statusToError(.cancelled));
    try std.testing.expectError(
        error.DenseRepairBackpressure,
        client.statusToError(.dense_repair_backpressure),
    );

    var empty: abi.OwnedBytes = .{};
    abi.antfly_storage_owner_buffer_destroy(&empty);
    abi.antfly_storage_owner_buffer_destroy(&empty);
}

test "opaque storage owner transaction recovery crosses callback ABI" {
    const path = "/tmp/antfly-storage-kernel-owner-transaction-recovery";
    cleanup(path);
    defer cleanup(path);

    const Capture = struct {
        calls: std.atomic.Value(u32) = .init(0),

        fn resolve(
            ptr: ?*anyopaque,
            txn_id: *const abi.TxnId,
            participant: abi.BorrowedBytes,
            status: abi.TxnStatus,
            commit_version: u64,
        ) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
            const expected_txn_id: [16]u8 = @splat(0x2a);
            if (!std.mem.eql(u8, &txn_id.bytes, &expected_txn_id) or
                !std.mem.eql(u8, participant.slice(), "table2:00000006:remote:9002") or
                status != .committed or commit_version != 102)
                return .invalid_argument;
            _ = self.calls.fetchAdd(1, .release);
            return .ok;
        }
    };
    var capture = Capture{};
    var owner = try client.Owner.open(.{
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 9001,
        .transaction_recovery = .{
            .enabled = 1,
            .lease_owned = 1,
            .interval_ms = 10,
            .cutoff_ns = 1,
            .callback_ctx = &capture,
            .owner_id = .fromSlice("owner-test"),
            .resolve_participant_fn = Capture.resolve,
        },
    });
    defer owner.deinit();

    const txn_id: [16]u8 = @splat(0x2a);
    var begin = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"begin\",\"txn_id\":\"2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a\",\"begin_timestamp\":\"100\",\"created_at_ns\":\"1\",\"topology_epoch\":\"0\",\"participants\":[\"table2:00000004:docs:9001\",\"table2:00000006:remote:9002\"]},\"sync_level\":\"write\"}",
    );
    begin.deinit();
    var resolve = try owner.replicatedBatchJson(
        "docs",
        "{\"_transaction\":{\"phase\":\"resolve\",\"txn_id\":\"2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a\",\"status\":\"committed\",\"commit_version\":\"102\"},\"sync_level\":\"write\"}",
    );
    resolve.deinit();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    for (0..500) |_| {
        if (capture.calls.load(.acquire) > 0) break;
        try io_impl.io().sleep(.fromMilliseconds(2), .awake);
    }
    try std.testing.expectEqual(@as(u32, 1), capture.calls.load(.acquire));

    var cleaned = false;
    for (0..500) |_| {
        _ = owner.transactionStatus("docs", txn_id) catch |err| {
            if (err == error.TxnNotFound) {
                cleaned = true;
                break;
            }
            return err;
        };
        try io_impl.io().sleep(.fromMilliseconds(2), .awake);
    }
    try std.testing.expect(cleaned);
}

test "opaque storage context enforces owner lifetime and shares process storage state" {
    const first_path = "/tmp/antfly-storage-kernel-context-first";
    const second_path = "/tmp/antfly-storage-kernel-context-second";
    cleanup(first_path);
    cleanup(second_path);
    defer cleanup(first_path);
    defer cleanup(second_path);

    var context: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_create(&.{}, &context));
    try std.testing.expect(context != null);
    var metrics: abi.ContextMetricsResult = undefined;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_context_metrics(null, &metrics));
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_metrics(context, &metrics));
    try std.testing.expectEqual(abi.abi_version, metrics.version);
    try std.testing.expectEqual(@as(u64, 0), metrics.lsm_cache_entry_count);
    try std.testing.expectEqual(
        abi.Status.invalid_argument,
        abi.antfly_storage_context_attach_inference_provider(context, null),
    );
    const fake_inference_handle: *anyopaque = @ptrFromInt(@alignOf(usize));
    try std.testing.expectEqual(
        abi.Status.ok,
        abi.antfly_storage_context_attach_inference_provider(context, fake_inference_handle),
    );

    var first = try client.Owner.open(.{
        .context = context,
        .path = .fromSlice(first_path),
        .table_name = .fromSlice("first"),
    });
    var second = try client.Owner.open(.{
        .context = context,
        .path = .fromSlice(second_path),
        .table_name = .fromSlice("second"),
    });
    try std.testing.expectEqual(
        abi.Status.busy,
        abi.antfly_storage_context_attach_inference_provider(context, fake_inference_handle),
    );
    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_context_destroy(context));

    first.deinit();
    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_context_destroy(context));
    second.deinit();
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_destroy(context));
}

test "opaque rejected Raft entry advances through empty batch without effects across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/rejected-entry", .{root});
    defer alloc.free(path);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    const options: abi.OpenRequest = .{ .context = context.handle, .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 9191, .has_identity_namespace = 1, .identity_table_id = 91, .identity_shard_id = 9191, .identity_range_id = 9191, .schema_json = .fromSlice("{}") };
    {
        var owner = try client.Owner.open(options);
        defer owner.deinit();
        var initial = try owner.replicatedBatchAtRaftEntryJson("rows", "{\"inserts\":{\"a\":{\"n\":1}},\"sync_level\":\"write\"}", 3, 1);
        initial.deinit();
        // The raw authority has rejected entry two; only its exact native
        // applied marker advances. The rejected command is never forwarded.
        var rejected = try owner.replicatedBatchAtRaftEntryJson("rows", "{}", 3, 2);
        rejected.deinit();
    }
    {
        var owner = try client.Owner.open(options);
        defer owner.deinit();
        // A synthetic nonempty replay proves that the empty command persisted
        // its marker, not merely returned success without a durable effect.
        var replay = try owner.replicatedBatchAtRaftEntryJson("rows", "{\"inserts\":{\"a\":{\"n\":999}},\"sync_level\":\"write\"}", 3, 2);
        replay.deinit();
        var row = try owner.lookupJson("rows", "{\"key\":\"a\"}");
        defer row.deinit();
        try std.testing.expect(std.mem.indexOf(u8, row.bytes(), "\"n\":1") != null);
        try std.testing.expect(std.mem.indexOf(u8, row.bytes(), "999") == null);
        var next = try owner.replicatedBatchAtRaftEntryJson("rows", "{\"inserts\":{\"a\":{\"n\":2}},\"sync_level\":\"write\"}", 3, 3);
        next.deinit();
        var changed = try owner.lookupJson("rows", "{\"key\":\"a\"}");
        defer changed.deinit();
        try std.testing.expect(std.mem.indexOf(u8, changed.bytes(), "\"n\":2") != null);
    }
}

test "opaque native Raft snapshot captures once and stages native plus logical projection" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const source_path = try std.fmt.allocPrint(alloc, "{s}/native-source", .{root});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, "{s}/native-target", .{root});
    defer alloc.free(target_path);
    const raw_source_path = try std.fmt.allocPrint(alloc, "{s}/raw-source", .{root});
    defer alloc.free(raw_source_path);
    const raw_target_path = try std.fmt.allocPrint(alloc, "{s}/raw-target", .{root});
    defer alloc.free(raw_target_path);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"body":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const options: abi.OpenRequest = .{ .context = context.handle, .path = .fromSlice(source_path), .table_name = .fromSlice("rows"), .group_id = 8181, .has_identity_namespace = 1, .identity_table_id = 81, .identity_shard_id = 8181, .identity_range_id = 8181, .schema_json = .fromSlice(schema), .indexes_json = .fromSlice("{\"full_text_index_v0\":{\"type\":\"full_text\"}}") };
    var owner = try client.Owner.open(options);
    defer owner.deinit();
    var response = try owner.replicatedBatchAtRaftEntryJson("rows", "{\"inserts\":{\"a\":{\"n\":9007199254740993,\"body\":\"aardvark\"}},\"_timestamp_ns\":\"1234\",\"sync_level\":\"write\"}", 2, 5);
    response.deinit();
    var source = try data_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = raw_source_path, .context = context.handle });
    defer source.deinit();
    const barrier = "{\"table\":\"rows\",\"protocol_barrier\":10,\"batch\":null}";
    var log: [25 + barrier.len]u8 = undefined;
    std.mem.writeInt(u32, log[0..4], 1, .little);
    std.mem.writeInt(u64, log[4..12], 2, .little);
    std.mem.writeInt(u64, log[12..20], 7, .little);
    log[20] = 0;
    std.mem.writeInt(u32, log[21..25], barrier.len, .little);
    @memcpy(log[25..], barrier);
    try source.applyBatch(8181, 7, &log);
    var prepared = (try source.prepareSnapshot(8181, 7)) orelse return error.TestExpectedEqual;
    defer prepared.deinit();
    try std.testing.expect(prepared.requiresNative());
    var capture = try owner.captureNativeRaftSnapshot(8181, 7);
    defer capture.deinit();
    var released: bool = false;
    const Release = struct {
        fn call(raw: ?*anyopaque) callconv(.c) void {
            const flag: *bool = @ptrCast(@alignCast(raw.?));
            flag.* = true;
        }
    };
    try capture.bindLease(&released, Release.call);
    try prepared.attachNative(capture.handle.?);
    capture.handle = null;
    try std.testing.expect(!released);
    var later = try owner.replicatedBatchAtRaftEntryJson("rows", "{\"inserts\":{\"later\":{\"n\":2}},\"_timestamp_ns\":\"1235\",\"sync_level\":\"write\"}", 2, 8);
    later.deinit();
    var artifact = try prepared.materializeFile(alloc);
    defer artifact.deinit(alloc);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, artifact.path, alloc, .limited(artifact.size + 1));
    defer alloc.free(bytes);
    var target = try data_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = raw_target_path, .context = context.handle });
    defer target.deinit();
    var target_options = options;
    target_options.path = .fromSlice(target_path);
    {
        var previous = try client.Owner.open(target_options);
        defer previous.deinit();
        var old_row = try previous.replicatedBatchAtRaftEntryJson("rows", "{\"inserts\":{\"old\":{\"n\":5,\"body\":\"previous\"}},\"_timestamp_ns\":\"1200\",\"sync_level\":\"write\"}", 1, 5);
        old_row.deinit();
    }
    var request: abi.SnapshotPrepareRequest = .{ .path = .fromSlice(target_path), .table_name = .fromSlice("rows"), .group_id = 8181, .identity_table_id = 81, .identity_shard_id = 8181, .identity_range_id = 8181, .schema_json = .fromSlice(schema), .encoded_snapshot = .fromSlice(bytes), .projection_store = target.handle, .expected_applied_index = 6 };
    var invalid: ?*anyopaque = null;
    try std.testing.expect(abi.antfly_storage_snapshot_prepare(&request, &invalid) != .ok);
    try std.testing.expect(invalid == null);
    request.expected_applied_index = 7;
    request.identity_shard_id = 8182;
    try std.testing.expect(abi.antfly_storage_snapshot_prepare(&request, &invalid) != .ok);
    try std.testing.expect(invalid == null);
    try std.testing.expect(try target.latestBatch(8181) == null);
    request.identity_shard_id = 8181;
    request.lsm_root_generation = 2;
    // Simulate losing native publication after raw projection preparation.
    // Retrying the same Raft snapshot must install the same cut, not recapture
    // the donor (whose live primary already advanced to index eight).
    var interrupted = try client.Snapshot.prepare(request);
    interrupted.deinit();
    try std.testing.expectEqual(@as(u64, 7), (try target.latestBatch(8181)).?.commit_index);
    // Crash with the incoming Raft snapshot durable but native publication
    // incomplete. Reopen through the real shared runtime recovery queue,
    // whose normal state-machine wrapper must install before marking applied.
    const raft_engine = @import("raft_engine");
    const replica_storage = @import("../raft/storage/mod.zig");
    const state_machine = @import("../raft/state_machine/mod.zig");
    var layout = try replica_storage.ReplicaPathLayout.initForReplica(alloc, root, 8181, 1);
    defer layout.deinit(alloc);
    {
        var persisted = try replica_storage.PersistentReplicaState.init(alloc, layout);
        defer persisted.deinit();
        try persisted.groupStorage().persistReady(8181, .{
            .hard_state = .{ .current_term = 1, .voted_for = 1, .commit_index = 5 },
            .snapshot = .{ .metadata = .{ .index = 5, .term = 1, .conf_state = .{ .voters = @constCast(&[_]u64{1}) } }, .data = @constCast("previous completed root") },
        });
        try persisted.setAppliedIndex(5);
        try persisted.groupStorage().persistReady(8181, .{
            .hard_state = .{ .current_term = 2, .voted_for = 1, .commit_index = 7 },
            .snapshot = .{ .metadata = .{ .index = 7, .term = 2, .conf_state = .{ .voters = @constCast(&[_]u64{1}) } }, .data = @constCast(bytes) },
        });
        try std.testing.expectEqual(@as(u64, 7), persisted.appliedIndex());
        try std.testing.expectEqual(@as(u64, 5), persisted.completedAppliedIndex());
    }
    var recovered = try replica_storage.PersistentReplicaState.init(alloc, layout);
    defer recovered.deinit();
    const Recovery = struct {
        request: abi.SnapshotPrepareRequest,
        options: abi.OpenRequest,
        state: *replica_storage.PersistentReplicaState,
        blocked: bool = true,
        installs: usize = 0,
        batches: usize = 0,
        verified: bool = false,

        fn build(_: *anyopaque, _: std.mem.Allocator, _: u64) ![]u8 {
            return error.UnexpectedSnapshotBuild;
        }
        fn batch(ptr: *anyopaque, value: state_machine.ApplyBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const entries = try state_machine.decodeCommittedEntries(std.testing.allocator, value.entries_bytes);
            defer std.testing.allocator.free(entries);
            var reopened = try client.Owner.open(self.options);
            defer reopened.deinit();
            for (entries) |entry| {
                var result = try reopened.replicatedBatchAtRaftEntryJson("rows", entry.data, entry.term, entry.index);
                result.deinit();
            }
            self.batches += 1;
        }
        fn install(ptr: *anyopaque, _: std.mem.Allocator, group: u64, index: u64, encoded: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.blocked) return error.RaftApplyWriterUnavailable;
            try std.testing.expectEqual(@as(u64, 8181), group);
            try std.testing.expectEqual(@as(u64, 7), index);
            var exact = self.request;
            exact.encoded_snapshot = .fromSlice(encoded);
            var staged = try client.Snapshot.prepare(exact);
            defer staged.deinit();
            try staged.promote();
            _ = try staged.publishPrepared();
            try staged.commit();
            self.installs += 1;
        }
        fn verify(ptr: *anyopaque, _: u64, snapshot: ?raft_engine.core.types.Snapshot, entries: []const raft_engine.core.Entry, _: []const raft_engine.core.ReadState) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (snapshot == null and entries.len == 0) return;
            var reopened = try client.Owner.open(self.options);
            defer reopened.deinit();
            var row_at_cut = try reopened.lookupJson("rows", "{\"key\":\"a\"}");
            defer row_at_cut.deinit();
            try std.testing.expectError(error.NotFound, reopened.lookupJson("rows", "{\"key\":\"old\"}"));
            if (self.batches == 0) {
                try std.testing.expectError(error.NotFound, reopened.lookupJson("rows", "{\"key\":\"later\"}"));
            } else {
                var later_row = try reopened.lookupJson("rows", "{\"key\":\"later\"}");
                later_row.deinit();
            }
            self.verified = true;
        }
        fn complete(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.verified);
            try self.state.setAppliedIndex(index);
        }
    };
    var published_options = target_options;
    published_options.lsm_root_generation = 2;
    var recovery = Recovery{ .request = request, .options = published_options, .state = &recovered };
    var apply = state_machine.DataStateMachine{
        .alloc = alloc,
        .snapshot_builder = .{ .ptr = &recovery, .vtable = &.{ .build_snapshot = Recovery.build, .install_snapshot = Recovery.install, .apply_batch = Recovery.batch } },
        .delegate = .{ .ptr = &recovery, .vtable = &.{ .apply_ready = Recovery.verify } },
        .applied_sink = .{ .ptr = &recovery, .vtable = &.{ .set_applied_index = Recovery.complete } },
    };
    var runtime = raft_engine.runtime.MultiRaft.init(alloc, .{}, .{ .state_machine = apply.stateMachine(), .group_storage = recovered.groupStorage() });
    defer runtime.deinit();
    var reads = @import("../raft/read_gate.zig").AppliedReadTracker.init(alloc, 999);
    defer reads.deinit();
    var read_buffer: [96]u8 = undefined;
    const read = try reads.register(8181, &read_buffer);
    reads.observeReadStates(8181, &.{.{ .index = 7, .request_ctx = @constCast(read.request_ctx) }});
    _ = try runtime.ensureReplica(.{
        .group = .{ .group_id = 8181, .local_node_id = 1, .raft_config = .{ .id = 1, .group_id = 8181, .peers = &.{1}, .election_tick = 5, .heartbeat_tick = 1, .applied = recovered.completedAppliedIndex() }, .storage = recovered.storage() },
        .recover_persisted_snapshot = true,
    });
    _ = try runtime.processReady(8181);
    try reads.noteApplied(8181, recovered.completedAppliedIndex());
    try std.testing.expect(!reads.takeCompleted(read.token));
    try std.testing.expectEqual(@as(usize, 0), recovery.installs);
    {
        var previous = try client.Owner.open(target_options);
        defer previous.deinit();
        var old_row = try previous.lookupJson("rows", "{\"key\":\"old\"}");
        old_row.deinit();
        try std.testing.expectError(error.NotFound, previous.lookupJson("rows", "{\"key\":\"a\"}"));
    }
    recovery.blocked = false;
    for (0..8) |_| {
        _ = try runtime.processReady(8181);
        if (recovered.completedAppliedIndex() == 7) break;
    } else return error.SnapshotRecoveryDidNotComplete;
    try std.testing.expectEqual(@as(usize, 1), recovery.installs);
    try reads.noteApplied(8181, recovered.completedAppliedIndex());
    try std.testing.expect(reads.takeCompleted(read.token));
    target_options = published_options;
    {
        var restored = try client.Owner.open(target_options);
        defer restored.deinit();
        var row = try restored.lookupJson("rows", "{\"key\":\"a\"}");
        defer row.deinit();
        try std.testing.expect(std.mem.indexOf(u8, row.bytes(), "9007199254740993") != null);
        try std.testing.expectError(error.NotFound, restored.lookupJson("rows", "{\"key\":\"old\"}"));
        var search = try restored.queryJson("rows", "{\"full_text_search\":{\"match\":\"aardvark\",\"field\":\"body\"},\"indexes\":[\"full_text_index_v0\"],\"limit\":10}");
        defer search.deinit();
        try std.testing.expect(std.mem.indexOf(u8, search.bytes(), "aardvark") != null);
    }
    var range = try target.currentRange(alloc, 8181);
    defer range.deinit(alloc);
    var page = try target.groupStatePageInRange(alloc, 8181, .{ .start = range.start, .end = range.end }, null, 10, 1024 * 1024);
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try std.testing.expectEqualStrings("a", page.entries[0].key);
    try std.testing.expect(std.mem.indexOf(u8, page.entries[0].value, "9007199254740993") != null);
    try std.testing.expectEqual(@as(u64, 7), (try target.latestBatch(8181)).?.commit_index);

    // Legacy (or batched-watermark) recovery can find native data ahead of
    // its completed cursor. Reinstalling the retained snapshot must replay
    // the durable suffix before satisfying an already-observed current read.
    const suffix = "{\"inserts\":{\"later\":{\"n\":2}},\"_timestamp_ns\":\"1235\",\"sync_level\":\"write\"}";
    {
        var ahead = try client.Owner.open(target_options);
        defer ahead.deinit();
        var result = try ahead.replicatedBatchAtRaftEntryJson("rows", suffix, 2, 8);
        result.deinit();
    }
    const legacy_root = try std.fmt.allocPrint(alloc, "{s}/legacy-provider", .{root});
    defer alloc.free(legacy_root);
    var legacy_layout = try replica_storage.ReplicaPathLayout.initForReplica(alloc, legacy_root, 8181, 1);
    defer legacy_layout.deinit(alloc);
    {
        var persisted = try replica_storage.PersistentReplicaState.init(alloc, legacy_layout);
        defer persisted.deinit();
        try persisted.groupStorage().persistReady(8181, .{
            .hard_state = .{ .current_term = 2, .voted_for = 1, .commit_index = 8 },
            .snapshot = .{ .metadata = .{ .index = 7, .term = 2, .conf_state = .{ .voters = @constCast(&[_]u64{1}) } }, .data = @constCast(bytes) },
            .entries = &.{.{ .term = 2, .index = 8, .data = @constCast(suffix) }},
        });
        try std.testing.expectEqual(@as(u64, 0), persisted.completedAppliedIndex());
    }
    var legacy = try replica_storage.PersistentReplicaState.init(alloc, legacy_layout);
    defer legacy.deinit();
    var legacy_request = request;
    legacy_request.lsm_root_generation = 3;
    var legacy_options = target_options;
    legacy_options.lsm_root_generation = 3;
    var legacy_recovery = Recovery{ .request = legacy_request, .options = legacy_options, .state = &legacy };
    var legacy_apply = state_machine.DataStateMachine{
        .alloc = alloc,
        .snapshot_builder = .{ .ptr = &legacy_recovery, .vtable = &.{ .build_snapshot = Recovery.build, .install_snapshot = Recovery.install, .apply_batch = Recovery.batch } },
        .delegate = .{ .ptr = &legacy_recovery, .vtable = &.{ .apply_ready = Recovery.verify } },
        .applied_sink = .{ .ptr = &legacy_recovery, .vtable = &.{ .set_applied_index = Recovery.complete } },
    };
    var legacy_runtime = raft_engine.runtime.MultiRaft.init(alloc, .{}, .{ .state_machine = legacy_apply.stateMachine(), .group_storage = legacy.groupStorage() });
    defer legacy_runtime.deinit();
    const latest_read = try reads.register(8181, &read_buffer);
    reads.observeReadStates(8181, &.{.{ .index = 8, .request_ctx = @constCast(latest_read.request_ctx) }});
    _ = try legacy_runtime.ensureReplica(.{
        .group = .{ .group_id = 8181, .local_node_id = 1, .raft_config = .{ .id = 1, .group_id = 8181, .peers = &.{1}, .election_tick = 5, .heartbeat_tick = 1, .applied = legacy.completedAppliedIndex() }, .storage = legacy.storage() },
        .recover_persisted_snapshot = true,
    });
    _ = try legacy_runtime.processReady(8181);
    try std.testing.expectEqual(@as(u64, 0), legacy.completedAppliedIndex());
    try std.testing.expect(!reads.takeCompleted(latest_read.token));
    legacy_recovery.blocked = false;
    for (0..8) |_| {
        _ = try legacy_runtime.processReady(8181);
        try reads.noteApplied(8181, legacy.completedAppliedIndex());
        if (legacy.completedAppliedIndex() == 8) break;
        try std.testing.expect(!reads.takeCompleted(latest_read.token));
    } else return error.LegacySnapshotRecoveryDidNotComplete;
    try std.testing.expectEqual(@as(usize, 1), legacy_recovery.installs);
    try std.testing.expectEqual(@as(usize, 1), legacy_recovery.batches);
    try std.testing.expect(reads.takeCompleted(latest_read.token));
}

test "opaque data raft apply owner preserves batch snapshot and placement lifecycle" {
    const source_root = "/tmp/antfly-storage-kernel-data-apply-source";
    const restored_root = "/tmp/antfly-storage-kernel-data-apply-restored";
    const authoritative_root = "/tmp/antfly-storage-kernel-data-apply-authoritative";
    cleanup(source_root);
    cleanup(restored_root);
    cleanup(authoritative_root);
    defer cleanup(source_root);
    defer cleanup(restored_root);
    defer cleanup(authoritative_root);

    var context = client.Context{};
    try context.ensure();
    defer context.deinit();

    var invalid_store: ?*anyopaque = @ptrFromInt(1);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_data_apply_store_open(&.{
        .version = abi.abi_version + 1,
        .root_dir = .fromSlice(source_root),
    }, &invalid_store));
    try std.testing.expect(invalid_store == null);

    var source = try data_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = source_root,
        .context = context.handle,
    });
    defer source.deinit();

    const payload = "opaque-data-apply";
    var encoded: [4 + 8 + 8 + 1 + 4 + payload.len]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, encoded[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u64, encoded[pos..][0..8], 4, .little);
    pos += 8;
    std.mem.writeInt(u64, encoded[pos..][0..8], 9, .little);
    pos += 8;
    encoded[pos] = 0;
    pos += 1;
    std.mem.writeInt(u32, encoded[pos..][0..4], payload.len, .little);
    pos += 4;
    @memcpy(encoded[pos..], payload);
    try source.applyBatch(81, 9, &encoded);
    const latest = (try source.latestBatch(81)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 9), latest.commit_index);
    try std.testing.expectEqual(@as(u64, 9), latest.last_entry_index);
    try std.testing.expectEqual(@as(usize, 1), latest.normal_entry_count);
    const transition_latest = (try source.latestBatchForTransition(81)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(latest, transition_latest);
    var empty_observation = try source.observeSplitControl(std.testing.allocator, 81);
    defer empty_observation.deinit(std.testing.allocator);
    try std.testing.expect(empty_observation.state == null);
    try std.testing.expectEqual(@as(u64, 0), empty_observation.delta_sequence);

    var authoritative = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice(authoritative_root),
        .table_name = .fromSlice("docs"),
        .group_id = 81,
    });
    defer authoritative.deinit();
    var authoritative_batch = try authoritative.batchJson(
        "docs",
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"},\"doc:b\":{\"title\":\"beta\"}},\"sync_level\":\"write\"}",
    );
    authoritative_batch.deinit();
    var reconcile = try source.reconcileAuthoritativeOwner(
        std.testing.allocator,
        authoritative.handle,
        81,
        latest,
        false,
        1,
        1024 * 1024,
    );
    defer reconcile.deinit(std.testing.allocator);
    try std.testing.expect(reconcile == .reconciled);
    var projection_range = try source.currentRange(std.testing.allocator, 81);
    defer projection_range.deinit(std.testing.allocator);
    var projection_page = try source.groupStatePageInRange(
        std.testing.allocator,
        81,
        .{ .start = projection_range.start, .end = projection_range.end },
        null,
        8,
        1024 * 1024,
    );
    defer projection_page.deinit(std.testing.allocator);
    try std.testing.expect(projection_page.entries.len > 0);
    var key_page = try source.groupStateKeysPageInRange(
        std.testing.allocator,
        81,
        .{ .start = projection_range.start, .end = projection_range.end },
        null,
        1,
        1024,
    );
    defer key_page.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), key_page.entries.len);
    try std.testing.expectEqualStrings(projection_page.entries[0].key, key_page.entries[0].key);
    try std.testing.expectEqualStrings("", key_page.entries[0].value);
    try std.testing.expect(projection_page.entries[0].value.len != 0);
    try std.testing.expect(!key_page.exhausted);
    var following_keys = try source.groupStateKeysPageInRange(
        std.testing.allocator,
        81,
        .{ .start = projection_range.start, .end = projection_range.end },
        key_page.entries[0].key,
        8,
        1024,
    );
    defer following_keys.deinit(std.testing.allocator);
    try std.testing.expectEqual(projection_page.entries.len - 1, following_keys.entries.len);
    for (following_keys.entries) |entry| try std.testing.expectEqualStrings("", entry.value);
    var invalid_projection: abi.OwnedBytes = .{};
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_data_apply_store_projection(
        source.handle,
        &.{ .version = abi.abi_version + 1 },
        &invalid_projection,
    ));
    try std.testing.expectEqual(@as(u64, 0), invalid_projection.len);

    var placement = try source.beginActiveGroupTransition(&.{81});
    placement.commit();
    placement.deinit();
    try source.retainActiveGroups(&.{81});
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_data_apply_store_retain_groups(
        source.handle,
        &.{ .group_count = 1 },
    ));
    var aborted = try source.beginActiveGroupTransition(&.{ 81, 82 });
    aborted.abort();
    aborted.deinit();

    var prepared = (try source.prepareSnapshot(81, 9)) orelse return error.TestExpectedEqual;
    defer prepared.deinit();
    var materialized = try prepared.materializeFile(std.testing.allocator);
    defer materialized.deinit(std.testing.allocator);
    const materialized_bytes = try std.Io.Dir.cwd().readFileAlloc(
        std.Options.debug_io,
        materialized.path,
        std.testing.allocator,
        .limited(materialized.size + 1),
    );
    defer std.testing.allocator.free(materialized_bytes);
    try std.testing.expectEqual(materialized.size, @as(u64, @intCast(materialized_bytes.len)));

    var cancelled = (try source.prepareSnapshot(81, 9)) orelse return error.TestExpectedEqual;
    defer cancelled.deinit();
    cancelled.cancel();
    try std.testing.expectError(error.SnapshotBuildCancelled, cancelled.materializeFile(std.testing.allocator));

    const snapshot = try source.buildSnapshot(std.testing.allocator, 81);
    defer std.testing.allocator.free(snapshot);
    try std.testing.expect(snapshot.len > 0);

    var restored = try data_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = restored_root,
        .context = context.handle,
    });
    defer restored.deinit();
    try restored.installSnapshot(81, 9, snapshot);
    const restored_latest = (try restored.latestBatch(81)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(latest.commit_index, restored_latest.commit_index);
    try std.testing.expectEqual(latest.last_entry_index, restored_latest.last_entry_index);

    try std.testing.expectEqual(abi.Status.busy, abi.antfly_storage_context_destroy(context.handle));
}

test "opaque metadata HA callback preserves lost ack replay and full checkpoint authority" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const root = "/tmp/antfly-storage-kernel-metadata-ha-port";
    cleanup(root);
    defer cleanup(root);
    try std.Io.Dir.cwd().createDirPath(io, root);
    const primary_mod = @import("hot_standby/primary.zig");
    var primary = try primary_mod.Primary.open(alloc, root ++ "/ha.log", root ++ "/slots", .{ .cluster_id = 7, .shard_id = 0, .table_id = 0, .timeline_id = 1, .epoch = 1 }, .{});
    defer primary.close();
    var target = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = root ++ "/target" });
    defer target.deinit();
    const group = @import("../common/group_ids.zig").main_metadata_group_id;
    const key = "\x00\x00__api_restore_jobs__:000000000000002a";
    const initial = "{\"job_id\":42,\"phase\":\"queued\"}";
    const updated = "{\"job_id\":42,\"phase\":\"importing\"}";
    const Failure = struct {
        fn wait(_: *anyopaque, _: *primary_mod.Primary, _: u64, _: primary_mod.SyncPolicy) !void {
            return error.HASyncCommitWouldBlock;
        }
    };
    var wait_ctx: u8 = 0;
    var transition: std.atomic.Mutex = .unlocked;
    {
        var source = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = root ++ "/source" });
        defer source.deinit();
        try source.migrateStandaloneRestoreJobs(&.{});
        // An opaque owner cannot retain a pointer to this stack-local Port;
        // only its heap-owned creator adapter survives each completed bind.
        try std.testing.expect(transition.tryLock());
        try source.bindHA(.{ .primary = &primary }, .{ .primary = &primary, .transition_mutex = &transition });
        transition.unlock();
        try std.testing.expectError(error.MetadataHAMigrationAfterBinding, source.migrateStandaloneRestoreJobs(&.{}));
        try source.applyStandaloneCommand(group, .{ .upsert_restore_job = .{ .key = key, .value = initial } });
        var first = (try primary.log.entryAt(alloc, 1)).?;
        defer first.deinit(alloc);
        try target.applyHARecord(first.record);
        try target.applyHARecord(first.record);
        const actual = (try target.getRestoreJobValue(alloc, group, key)).?;
        defer alloc.free(actual);
        try std.testing.expectEqualStrings(initial, actual);
        try source.bindHA(.{ .primary = &primary }, .{ .primary = &primary, .transition_mutex = &transition, .sync_policy = .{ .mode = .remote_apply }, .sync_wait_ctx = &wait_ctx, .sync_wait_fn = Failure.wait });
        try std.testing.expectError(error.HASyncCommitWouldBlock, source.applyStandaloneCommand(group, .{ .upsert_restore_job = .{ .key = key, .value = updated } }));
        try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
        try std.testing.expectError(error.MetadataHAOutboxPending, source.exportHACheckpoint(io, root ++ "/checkpoint"));
        try std.testing.expect(transition.tryLock());
        transition.unlock();
    }
    {
        var source = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = root ++ "/source" });
        defer source.deinit();
        try source.bindHA(.{ .primary = &primary }, .{ .primary = &primary, .transition_mutex = &transition });
        try source.flushHAOutbox();
        try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
        const checkpoint = try source.exportHACheckpoint(io, root ++ "/checkpoint");
        var restored = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = root ++ "/restored" });
        defer restored.deinit();
        try restored.importHACheckpoint(io, root ++ "/checkpoint", checkpoint.size_bytes);
        const actual = (try restored.getRestoreJobValue(alloc, group, key)).?;
        defer alloc.free(actual);
        try std.testing.expectEqualStrings(updated, actual);
        var second = (try primary.log.entryAt(alloc, 2)).?;
        defer second.deinit(alloc);
        try restored.applyHARecord(second.record);
        try std.testing.expectError(error.MetadataHACheckpointTargetNotEmpty, restored.importHACheckpoint(io, root ++ "/checkpoint", checkpoint.size_bytes));
        // Catalog outcomes cross the separately compiled owner boundary too.
        // A stale bootstrap CAS is definitely rejected, while a failed remote
        // acknowledgement after the exact catalog commit is ambiguous.
        const revision = try source.standaloneRevision();
        try std.testing.expect(revision > 0);
        try std.testing.expectError(error.TableLifecycleConflict, source.replaceStandaloneCatalog(group, revision - 1, &.{}, &.{}, "{}"));
        try std.testing.expectEqual(revision, try source.standaloneRevision());
        try source.bindHA(.{ .primary = &primary }, .{ .primary = &primary, .transition_mutex = &transition, .sync_policy = .{ .mode = .remote_apply }, .sync_wait_ctx = &wait_ctx, .sync_wait_fn = Failure.wait });
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, source.replaceStandaloneCatalog(group, revision, &.{}, &.{}, "{}"));
        try std.testing.expectEqual(revision + 1, try source.standaloneRevision());
        const catalog = (try source.loadStandaloneCatalog(alloc)).?;
        defer alloc.free(catalog);
        try std.testing.expectEqualStrings("{}", catalog);
    }
}

test "opaque metadata standby acknowledgement cannot retire an outbox across promotion" {
    const alloc = std.testing.allocator;
    const root = "/tmp/antfly-storage-kernel-metadata-promotion";
    cleanup(root);
    defer cleanup(root);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, root);
    const primary_mod = @import("hot_standby/primary.zig");
    const gate_mod = @import("hot_standby/public_gate_state.zig");
    var primary = try primary_mod.Primary.open(alloc, root ++ "/standby.log", root ++ "/slots", .{ .cluster_id = 7, .shard_id = 0, .table_id = 0, .timeline_id = 1, .epoch = 1 }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    var gate: gate_mod.State = .{};
    gate.configurePrimary(&primary, false);
    var transition: std.atomic.Mutex = .unlocked;
    const Promote = struct {
        gate: *gate_mod.State,
        transition: *std.atomic.Mutex,

        fn wait(ptr: *anyopaque, owner: *primary_mod.Primary, lsn: u64, _: primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Remote acknowledgement waits must not retain the promotion
            // lock. Simulate a new authority generation before returning.
            if (!self.transition.tryLock()) return error.TestUnexpectedResult;
            defer self.transition.unlock();
            try owner.standbyStatusUpdate("standby-a", owner.identity.timeline_id, lsn, lsn);
            self.gate.publishPrimary(owner, false);
        }
    };
    var promote: Promote = .{ .gate = &gate, .transition = &transition };
    var source = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = root ++ "/source" });
    defer source.deinit();
    try source.bindHA(.{ .shared = .{ .state = &gate } }, .{
        .primary = &primary,
        .transition_mutex = &transition,
        .sync_policy = .{ .mode = .remote_apply, .standby_names = &.{"standby-a"} },
        .sync_wait_ctx = &promote,
        .sync_wait_fn = Promote.wait,
    });
    const group = @import("../common/group_ids.zig").main_metadata_group_id;
    try std.testing.expectError(error.HAPromotedStandbyRequiresPrimaryOpen, source.applyStandaloneCommand(group, .{ .upsert_restore_job = .{
        .key = "\x00\x00__api_restore_jobs__:000000000000002a",
        .value = "{\"job_id\":42,\"phase\":\"queued\"}",
    } }));
    try std.testing.expectError(error.MetadataHAOutboxPending, source.exportHACheckpoint(std.testing.io, root ++ "/checkpoint"));
    try std.testing.expect(transition.tryLock());
    transition.unlock();
    const prior_lsn = primary.lastLsn();
    try source.bindHA(.{ .shared = .{ .state = &gate } }, .{ .primary = &primary, .transition_mutex = &transition });
    try source.flushHAOutbox();
    try std.testing.expectEqual(prior_lsn, primary.lastLsn());
    _ = try source.exportHACheckpoint(std.testing.io, root ++ "/checkpoint");
}

test "opaque metadata staging authority and binary receipts survive compiled projection and snapshot" {
    const alloc = std.testing.allocator;
    const staging = @import("../metadata/restore_staging.zig");
    const Helper = struct {
        fn apply(store: *metadata_apply_client.RaftApplyStore, group: u64, command: staging.Command) !void {
            const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, command, .{});
            defer std.testing.allocator.free(encoded);
            try store.applyStandaloneCommand(group, .{ .apply_restore_staging = encoded });
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const source_path = try std.fmt.allocPrint(alloc, "{s}/source", .{root});
    defer alloc.free(source_path);
    const restored_path = try std.fmt.allocPrint(alloc, "{s}/restored", .{root});
    defer alloc.free(restored_path);
    var store = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = source_path, .no_sync = true });
    defer store.deinit();
    const group = @import("../common/group_ids.zig").main_metadata_group_id;
    const target: staging.Target = .{ .source_table_id = 1, .table = .{ .table_id = 11, .name = "private", .schema_json = "{}" }, .ranges = &.{.{ .table_id = 11, .group_id = 701, .range_id = 701, .doc_identity_shard_id = 701, .doc_identity_range_id = 701, .start_key = "" }} };
    const plan: staging.Plan = .{ .id = @splat(255), .cohort_digest = @splat(9), .targets = &.{target} };
    try Helper.apply(&store, group, .{ .id = plan.id, .action = .reserve, .plan = plan });
    try std.testing.expect(!try store.restoreStagingAuthorityAllowed(alloc, group, plan.id, 7, null));
    try store.applyStandaloneCommand(group, .{ .register_node = .{ .node_id = 7, .role = "data", .lifecycle = @import("../metadata/table_manager.zig").node_lifecycle_active } });
    try store.applyStandaloneCommand(group, .{ .upsert_replica_intent = .{
        .expected_metadata_version = null,
        .expected_version_fence = 0,
        .expected_target_drain_requested = false,
        .replacement = .{ .record = .{ .group_id = 701, .replica_id = 1, .local_node_id = 7 }, .store_id = 0, .peer_node_ids = &.{7} },
    } });
    try std.testing.expect(try store.restoreStagingAuthorityAllowed(alloc, group, plan.id, 7, null));
    try std.testing.expect(try store.restoreStagingAuthorityAllowed(alloc, group, plan.id, 7, 701));
    try std.testing.expect(!try store.restoreStagingAuthorityAllowed(alloc, group, plan.id, 8, 701));
    try std.testing.expect(!try store.restoreStagingAuthorityAllowed(alloc, group, @splat(254), 7, 701));
    try std.testing.expect(!try store.restoreStagingAuthorityAllowed(alloc, group, plan.id, 7, 0));
    try std.testing.expect((try store.loadRestoreStagingReceipt(alloc, group, plan.id, .importing, 701)) == null);

    var binary_digest: staging.Digest = @splat(255);
    binary_digest[0] = 0;
    const utf8_digest: staging.Digest = ([_]u8{ 0xc3, 0xa9, 0, 127 } ** 8);
    const plan_digest = try plan.digest(alloc);
    try Helper.apply(&store, group, .{ .id = plan.id, .action = .imported, .expected_revision = 1, .receipt = .{ .group_id = 701, .range_id = 701, .plan_digest = plan_digest, .completion_digest = binary_digest } });
    const imported = (try store.loadRestoreStagingProgress(alloc, group, plan.id)).?;
    try std.testing.expectEqual(staging.State.validating, imported.state);
    try Helper.apply(&store, group, .{ .id = plan.id, .action = .begin_cancel, .expected_revision = imported.revision });
    const canceling = (try store.loadRestoreStagingProgress(alloc, group, plan.id)).?;
    try std.testing.expectEqual(staging.State.canceling, canceling.state);
    try Helper.apply(&store, group, .{ .id = plan.id, .action = .canceled, .expected_revision = canceling.revision, .receipt = .{ .group_id = 701, .range_id = 701, .plan_digest = plan_digest, .completion_digest = utf8_digest } });
    const canceled_owner = (try store.loadRestoreStagingProgress(alloc, group, plan.id)).?;
    try std.testing.expectEqual(@as(u32, 1), canceled_owner.completed_owners);
    try Helper.apply(&store, group, .{ .id = plan.id, .action = .finish_cancel, .expected_revision = canceled_owner.revision });
    try store.applyStandaloneCommand(group, .{ .remove_replica_intent = .{ .group_id = 701, .local_node_id = 7, .expected_metadata_version = 1 } });
    const snapshot = try store.snapshotBuilder().buildSnapshot(alloc, group);
    defer alloc.free(snapshot);
    var restored = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = restored_path, .no_sync = true });
    defer restored.deinit();
    try std.testing.expect(try restored.snapshotBuilder().installSnapshot(alloc, group, 1, snapshot));
    for ([_]*metadata_apply_client.RaftApplyStore{ &store, &restored }) |owner| {
        try std.testing.expect(try owner.restoreStagingAuthorityAllowed(alloc, group, plan.id, 7, 701));
        try std.testing.expectEqual(staging.State.canceled, (try owner.loadRestoreStagingProgress(alloc, group, plan.id)).?.state);
        var job = (try owner.loadRestoreStaging(alloc, group, plan.id)).?;
        defer job.deinit();
        try std.testing.expectEqual(plan.id, job.value.plan.id);
        const binary = (try owner.loadRestoreStagingReceipt(alloc, group, plan.id, .importing, 701)).?;
        defer alloc.free(binary);
        try std.testing.expectEqualSlices(u8, &binary_digest, binary);
        const utf8 = (try owner.loadRestoreStagingReceipt(alloc, group, plan.id, .canceling, 701)).?;
        defer alloc.free(utf8);
        try std.testing.expectEqualSlices(u8, &utf8_digest, utf8);
    }
}

test "opaque metadata compound rewrite admission preserves job and source reservation across native snapshot" {
    const alloc = std.testing.allocator;
    const stages = @import("../metadata/restore_staging.zig");
    const group = @import("../common/group_ids.zig").main_metadata_group_id;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    var source = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = try std.fmt.allocPrint(a, "{s}/source", .{root}), .no_sync = true });
    defer source.deinit();
    const original: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 9, .name = "rows", .schema_json = "{\"version\":1,\"storage_mode\":\"document\"}" };
    const range: @import("../metadata/table_manager.zig").RangeRecord = .{ .table_id = 9, .group_id = 301, .range_id = 301, .start_key = "" };
    try source.applyStandaloneCommand(group, .{ .upsert_table = original });
    try source.applyStandaloneCommand(group, .{ .upsert_range = range });
    const scope: @import("db/online_source_contract.zig").Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .owner_group_id = 301, .peer_group_id = 401, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) },
        .receiver_namespace = .{ .table_id = 10, .shard_id = 401, .range_id = 401 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    const plan: stages.Plan = .{
        .id = try stages.idForAttempt(7, 1),
        .cohort_digest = @splat(7),
        .preparing_sources = true,
        .targets = &.{.{
            .source_table_id = 9,
            .table = .{ .table_id = 10, .name = "rows", .schema_json = original.schema_json },
            .ranges = &.{.{ .table_id = 10, .group_id = 401, .range_id = 401, .doc_identity_shard_id = 401, .doc_identity_range_id = 401, .start_key = "" }},
            .replace = .{ .table = original, .ranges = &.{range}, .fences = &.{scope.fence} },
            .rewrite_sources = &.{scope},
            .rewrite = .{ .preserve_document = true, .source_schemas = &.{original.schema_json}, .target_schema = original.schema_json, .program_digest = @splat(6) },
        }},
    };
    const plan_json = try std.json.Stringify.valueAlloc(a, plan, .{});
    const key = "\x00\x00__api_restore_jobs__:0000000000000007";
    const value = "{\"job_id\":7,\"attempt_id\":1,\"staging_attempt_id\":1,\"source_kind\":\"schema_rewrite\",\"phase\":\"queued\"}";
    try source.applyStandaloneCommand(group, .{ .create_restore_job_with_staging = .{ .key = key, .value = value, .plan_json = plan_json } });
    const encoded = try source.snapshotBuilder().buildSnapshot(alloc, group);
    defer alloc.free(encoded);
    var replacement = try metadata_apply_client.RaftApplyStore.init(alloc, .{ .root_dir = try std.fmt.allocPrint(a, "{s}/replacement", .{root}), .no_sync = true });
    defer replacement.deinit();
    try std.testing.expect(try replacement.snapshotBuilder().installSnapshot(alloc, group, 1, encoded));
    for ([_]*metadata_apply_client.RaftApplyStore{ &source, &replacement }) |owner| {
        const stored = (try owner.getRestoreJobValue(alloc, group, key)).?;
        defer alloc.free(stored);
        try std.testing.expectEqualStrings(value, stored);
        var admitted = (try owner.loadRestoreStaging(alloc, group, plan.id)).?;
        defer admitted.deinit();
        try std.testing.expectEqual(stages.State.preparing_sources, admitted.value.state);
        try std.testing.expectEqualSlices(u8, &try plan.digest(alloc), &admitted.value.plan_digest);
        try std.testing.expectEqual(stages.State.preparing_sources, (try owner.loadRestoreStagingProgress(alloc, group, plan.id)).?.state);
    }
}

test "opaque metadata apply owner preserves semantic error identity" {
    const path = "/tmp/antfly-storage-kernel-metadata-errors";
    cleanup(path);
    defer cleanup(path);

    var store = try metadata_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = path,
        .no_sync = true,
    });
    defer store.deinit();
    const snapshots = store.snapshotBuilder();

    // A present empty cursor is invalid, not an alias for the first page.
    try std.testing.expectError(error.InvalidBackupCohort, store.listBackupCohorts(std.testing.allocator, 91, "", 8));
    const cohorts = try store.listBackupCohorts(std.testing.allocator, 91, null, 8);
    defer {
        for (cohorts) |row| {
            std.testing.allocator.free(row.key);
            std.testing.allocator.free(row.value);
        }
        std.testing.allocator.free(cohorts);
    }
    try std.testing.expectEqual(@as(usize, 0), cohorts.len);

    var catalog_json: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&catalog_json);
    try std.testing.expectEqual(
        abi.Status.ok,
        abi.antfly_metadata_apply_store_projection(
            store.handle,
            &.{ .kind = .catalog_projection, .group_id = 91 },
            &catalog_json,
        ),
    );
    try std.testing.expect(std.mem.indexOf(u8, catalog_json.slice(), "\"tables\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, catalog_json.slice(), "\"ranges\":[]") != null);
    var timeout_json: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&timeout_json);
    try std.testing.expectEqual(
        abi.Status.timeout,
        abi.antfly_metadata_apply_store_projection(
            store.handle,
            &.{ .kind = .catalog_projection, .group_id = 91, .arg0 = 1 },
            &timeout_json,
        ),
    );

    try std.testing.expectError(
        error.InvalidMetadataSnapshot,
        snapshots.installSnapshot(std.testing.allocator, 91, 7, "not-a-metadata-snapshot"),
    );

    // The concrete store deliberately preserves the committed watermark even
    // when a batch has no projectable metadata commands. That gives this
    // cross-archive test a real provider-side index mismatch to round-trip.
    try snapshots.applyBatch(.{
        .group_id = 91,
        .commit_index = 7,
        .entries_bytes = "not-projectable-entries",
    });
    const latest = (try store.latestBatch(91)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 7), latest.commit_index);
    try std.testing.expectEqualStrings("not-projectable-entries", latest.entries_bytes);
    try std.testing.expectError(
        error.AppliedSnapshotIndexMismatch,
        snapshots.prepareSnapshot(91, 8),
    );
}

test "opaque metadata listener boundary preserves incarnation commit ordering" {
    const path = "/tmp/antfly-storage-kernel-metadata-listener-ordering";
    cleanup(path);
    defer cleanup(path);

    var store = try metadata_apply_client.RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = path,
        .no_sync = true,
    });
    defer store.deinit();

    const Capture = struct {
        barrier_active: bool = false,
        ordering_violation: bool = false,
        began: usize = 0,
        ended: usize = 0,
        signals: usize = 0,
        last_kind: ?metadata_apply_client.ProjectionSignalKind = null,
        last_group_id: u64 = 0,

        fn begin(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.barrier_active) self.ordering_violation = true;
            self.barrier_active = true;
            self.began += 1;
        }

        fn end(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.barrier_active) self.ordering_violation = true;
            self.barrier_active = false;
            self.ended += 1;
        }

        fn matchesKey(_: *anyopaque, _: metadata_apply_client.CommittedKeySignal) bool {
            return false;
        }
        fn onKey(_: *anyopaque, _: metadata_apply_client.CommittedKeySignal) void {}

        fn onProjection(ptr: *anyopaque, signal: metadata_apply_client.ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.barrier_active) self.ordering_violation = true;
            self.signals += 1;
            self.last_kind = signal.kind;
            self.last_group_id = signal.metadata_group_id;
        }
    };
    const RawCallbacks = struct {
        fn projection(_: ?*anyopaque, _: *const abi.MetadataProjectionSignal) callconv(.c) void {}
        fn barrier(_: ?*anyopaque) callconv(.c) void {}
    };

    // ABI booleans are canonical 0/1 values. Rejecting other bit patterns
    // keeps foreign callers from accidentally selecting a different contract
    // than the storage owner installed.
    var registration_id: u64 = 0;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_metadata_apply_store_add_listeners(
        store.handle,
        &.{
            .projection_fn = RawCallbacks.projection,
            .has_commit_barrier_kind = 2,
            .before_projection_commit_fn = RawCallbacks.barrier,
            .after_projection_commit_fn = RawCallbacks.barrier,
        },
        &registration_id,
    ));

    var capture = Capture{};
    try std.testing.expectError(error.InvalidProjectionCommitBarrier, store.addProjectionListener(.{
        .ptr = &capture,
        .commit_barrier_kind = .metadata_incarnation,
        .vtable = &.{ .on_projection_signal = Capture.onProjection },
    }));
    const registration = try store.addLifecycleListeners(.{
        .ptr = &capture,
        .commit_barrier_kind = .metadata_incarnation,
        .vtable = &.{
            .on_projection_signal = Capture.onProjection,
            .before_projection_commit = Capture.begin,
            .after_projection_commit = Capture.end,
        },
    }, .{ .ptr = &capture, .vtable = &.{ .matches_key = Capture.matchesKey, .on_committed_key = Capture.onKey } });

    // Keep this fixture at the actual compiled-owner wire. The transition is
    // `initialize_metadata_incarnation` (tag 45), wrapped in one normal Raft
    // entry using the stable committed-entry envelope.
    const transition = "afmd1\x2d0123456789abcdef0123456789abcdef";
    var encoded: [4 + 8 + 8 + 1 + 4 + transition.len]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, encoded[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u64, encoded[pos..][0..8], 1, .little);
    pos += 8;
    std.mem.writeInt(u64, encoded[pos..][0..8], 1, .little);
    pos += 8;
    encoded[pos] = 0;
    pos += 1;
    std.mem.writeInt(u32, encoded[pos..][0..4], transition.len, .little);
    pos += 4;
    @memcpy(encoded[pos..], transition);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 91,
        .commit_index = 1,
        .entries_bytes = &encoded,
    });
    try std.testing.expectEqual(@as(usize, 1), capture.began);
    try std.testing.expectEqual(@as(usize, 1), capture.signals);
    try std.testing.expectEqual(@as(usize, 1), capture.ended);
    try std.testing.expectEqual(metadata_apply_client.ProjectionSignalKind.metadata_incarnation, capture.last_kind.?);
    try std.testing.expectEqual(@as(u64, 91), capture.last_group_id);
    try std.testing.expect(!capture.barrier_active);
    try std.testing.expect(!capture.ordering_violation);
    try std.testing.expect(store.removeLifecycleListeners(registration));
    try std.testing.expect(!store.removeLifecycleListeners(registration));
    // A different metadata group initializes the same incarnation after detach.
    // No callback may retain the caller-owned capture after removal returns.
    try store.snapshotBuilder().applyBatch(.{ .group_id = 92, .commit_index = 1, .entries_bytes = &encoded });
    try std.testing.expectEqual(@as(usize, 1), capture.signals);
    try std.testing.expectEqual(@as(usize, 1), capture.began);
    try std.testing.expectEqual(@as(usize, 1), capture.ended);
}

test "storage kernel status registry is unique and lossless" {
    try error_identity.validateForTest();
    const runtime_error = @import("../runtime_error_abi.zig");
    for ([_]anyerror{ error.IndexRebuilding, error.IncompletePublishedSnapshot, error.DistributedQueryUnavailable }) |expected| {
        const failure = error_identity.failureFromError(
            expected,
            .local_query,
            abi.abi_version,
            @intFromEnum(abi.LocalQueryOperation.execute_internal_query),
        );
        var forwarded: abi.FailureIdentity = .{};
        try local_query_client.acceptProviderFailure(failure.status, failure, .validate_provider_response, &forwarded);
        const received = blk: {
            client.statusToError(forwarded.status) catch |err| break :blk err;
            return error.ExpectedReadinessFailure;
        };
        try std.testing.expectEqual(expected, received);
        const public_status = runtime_error.statusFromError(received);
        try std.testing.expectEqual(@intFromEnum(runtime_error.Code.retryable), public_status.code);
        try std.testing.expectEqual(expected, runtime_error.errorFromStatus(public_status));
    }
}

test "failed owner configuration releases its writer and context lease" {
    const path = "/tmp/antfly-storage-owner-failed-configuration";
    cleanup(path);
    defer cleanup(path);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    var owner: ?*anyopaque = null;
    const failed = abi.antfly_storage_owner_open(&.{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .indexes_json = .fromSlice("{"),
    }, &owner);
    try std.testing.expect(failed != .ok);
    try std.testing.expect(owner == null);
    var reopened = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
    });
    reopened.deinit();
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_context_destroy(context.handle));
    context.handle = null;
}

test "status text preserves its string wire representation and rejects oversized input" {
    const Text = @import("db/types.zig").InlineStatusText(4);
    const alloc = std.testing.allocator;
    const json = try std.json.Stringify.valueAlloc(alloc, Text.init("test"), .{});
    defer alloc.free(json);
    var decoded = try std.json.parseFromSlice(Text, alloc, json, .{});
    defer decoded.deinit();
    try std.testing.expectEqualStrings("test", decoded.value.slice());
    try std.testing.expectError(error.Overflow, std.json.parseFromSlice(Text, alloc, "\"large\"", .{}));
}

test "interactive admission state is shared with the physical owner" {
    const activity = @import("db/enrichment/enrichment_types.zig");
    const before = abi.antfly_storage_interactive_activity(0, 0);
    _ = activity.interactive_embed_inflight.fetchAdd(1, .monotonic);
    defer _ = activity.interactive_embed_inflight.fetchSub(1, .monotonic);
    try std.testing.expectEqual(before + 1, abi.antfly_storage_interactive_activity(0, 0));
    const generating = abi.antfly_storage_interactive_activity(1, 0);
    _ = activity.interactive_generate_inflight.fetchAdd(1, .monotonic);
    defer _ = activity.interactive_generate_inflight.fetchSub(1, .monotonic);
    try std.testing.expectEqual(generating + 1, abi.antfly_storage_interactive_activity(1, 0));
}

test "storage query wire preserves empty projection and decoded sort profile lifetime" {
    const alloc = std.testing.allocator;
    const contract = @import("../api/local_query_contract.zig");
    const query = @import("../api/query_contract.zig");
    const types = @import("db/types.zig");
    const wire = try contract.encodeStorageKernelQueryRequest(alloc, .{
        .include_all_fields = false,
        .include_stored = false,
    });
    defer alloc.free(wire);
    var request = try query.parseQueryRequest(alloc, null, "docs", wire);
    defer request.deinit(alloc);
    try std.testing.expect(!request.req.include_all_fields);
    try std.testing.expect(!request.req.include_stored);
    try std.testing.expectEqual(@as(usize, 0), request.req.fields.len);

    const native: types.SearchResult = .{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .sort_profile = .{
            .plan = "ordered_scan",
            .source = "native_doc_values",
            .candidate_source = "primary_key",
            .candidate_count = 13,
            .require_native = true,
            .sort_rejection_field = .init("nested.created_at"),
        },
    };
    var decoded = blk: {
        var encoded = try query.encodeQueryResponses(alloc, "docs", .{ .profile = true }, .{}, native);
        defer encoded.deinit(alloc);
        break :blk try contract.parseStorageKernelSearchResult(alloc, encoded.json);
    };
    defer decoded.deinit();
    const profile = decoded.sort_profile orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("ordered_scan", profile.plan);
    try std.testing.expectEqualStrings("native_doc_values", profile.source);
    try std.testing.expectEqualStrings("primary_key", profile.candidate_source);
    try std.testing.expectEqual(@as(u64, 13), profile.candidate_count);
    try std.testing.expect(profile.require_native);
    try std.testing.expectEqualStrings("nested.created_at", profile.sort_rejection_field.slice());
    // Replacing a decoded profile frees the previous string block, including
    // when the replacement's source strings alias the current owned block.
    try decoded.setOwnedSortProfile(profile);
    try std.testing.expectEqualStrings("ordered_scan", decoded.sort_profile.?.plan);
    var allocation_wire = try query.encodeQueryResponses(alloc, "docs", .{ .profile = true }, .{}, native);
    defer allocation_wire.deinit(alloc);
    try std.testing.checkAllAllocationFailures(alloc, struct {
        fn decode(failing: std.mem.Allocator, bytes: []const u8) !void {
            var result = try @import("../api/local_query_contract.zig").parseStorageKernelSearchResult(failing, bytes);
            defer result.deinit();
            try result.setOwnedSortProfile(result.sort_profile.?);
        }
    }.decode, .{allocation_wire.json});
}

test "storage query contract preserves each vector candidate budget" {
    const alloc = std.testing.allocator;
    const contract = @import("../api/local_query_contract.zig");
    const query = @import("../api/query_contract.zig");
    const controls = @import("local_query_controls.zig");
    const wire = try contract.encodeStorageKernelQueryRequest(alloc, .{
        .limit = 100,
        .dense_queries = &.{
            .{ .name = "a", .index_name = "a", .query = .{ .vector = &.{ 1, 0 }, .k = 3 } },
            .{ .name = "b", .index_name = "b", .query = .{ .vector = &.{ 0, 1 }, .k = 9 } },
        },
        .sparse_queries = &.{
            .{ .name = "s", .index_name = "s", .query = .{ .indices = &.{1}, .values = &.{1}, .k = 0 } },
        },
    });
    defer alloc.free(wire);
    var parsed = try query.parseQueryRequest(alloc, null, "docs", wire);
    defer parsed.deinit(alloc);
    controls.applyExecutionOptions(&parsed.req, .{ .enabled = 1 });
    try std.testing.expectEqual(@as(u32, 3), parsed.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(u32, 9), parsed.req.dense_queries[1].query.k);
    try std.testing.expectEqual(@as(u32, 0), parsed.req.sparse_queries[0].query.k);
    try std.testing.expectEqual(@as(u32, 100), parsed.req.limit);
    const default_wire = try contract.encodeStorageKernelQueryRequest(alloc, .{
        .limit = 7,
        .dense_queries = &.{.{ .name = "a", .index_name = "a", .query = .{ .vector = &.{ 1, 0 }, .k = 7 } }},
    });
    defer alloc.free(default_wire);
    try std.testing.expect(std.mem.indexOf(u8, default_wire, "_embedding_limits") == null);
    // The extension is admitted only on internal query paths.
    try std.testing.expectError(error.InvalidQueryRequest, query.parsePublicQueryRequest(alloc, null, "docs", wire));

    const public_wire = "{\"embeddings\":{\"sparse_idx\":{\"indices\":[1,5],\"values\":[0.5,0.75],\"k\":4}},\"indexes\":[\"sparse_idx\"],\"limit\":9}";
    var original = try query.parsePublicQueryRequest(alloc, null, "docs", public_wire);
    defer original.deinit(alloc);
    const internal_wire = try contract.encodeStorageKernelQueryRequest(alloc, original.req);
    defer alloc.free(internal_wire);
    var restored = try query.parseQueryRequest(alloc, null, "docs", internal_wire);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 4), restored.req.sparse_queries[0].query.k);

    const legacy_wire = try contract.encodeStorageKernelQueryRequest(alloc, .{
        .index_name = "a",
        .dense = .{ .vector = &.{ 1, 0 }, .k = 7 },
        .limit = 100,
    });
    defer alloc.free(legacy_wire);
    var legacy = try query.parseQueryRequest(alloc, null, "docs", legacy_wire);
    defer legacy.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 7), legacy.req.dense_queries[0].query.k);

    // Match by index, not iteration order, and reject malformed or unknown
    // budgets instead of silently reverting to the public result limit.
    const reordered = "{\"embeddings\":{\"a\":[1,0],\"b\":[0,1]},\"indexes\":[\"b\",\"a\"],\"_embedding_limits\":{\"a\":3,\"b\":9},\"limit\":100}";
    var order = try query.parseQueryRequest(alloc, null, "docs", reordered);
    defer order.deinit(alloc);
    try std.testing.expectEqualStrings("b", order.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 9), order.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(u32, 3), order.req.dense_queries[1].query.k);
    for ([_][]const u8{
        "{\"embeddings\":{\"a\":[1,0]},\"_embedding_limits\":{\"missing\":3}}",
        "{\"embeddings\":{\"a\":[1,0]},\"_embedding_limits\":{\"a\":-1}}",
        "{\"embeddings\":{\"a\":[1,0]},\"_embedding_limits\":{\"a\":4294967296}}",
    }) |invalid| {
        try std.testing.expectError(error.InvalidQueryRequest, query.parseQueryRequest(alloc, null, "docs", invalid));
    }
}

fn contextAllocationLifecycle(alloc: std.mem.Allocator) !void {
    const services = @import("kernel_runtime_services.zig");
    var bridge = services.memory.Allocator.fromStd(&alloc);
    // Keep executor creation outside the allocation sweep. std.Io.Threaded
    // reports thread admission failure as ConcurrencyUnavailable; this sweep
    // verifies the context's own fallible construction and unwind paths.
    var executor = services.executor.Borrow.init(&std.testing.io);
    var context = client.Context{};
    try context.ensureWithRuntime(.{ .allocator = &bridge, .io = &executor });
    defer context.deinit();
    try std.testing.expectEqual(@as(u64, 0), (try context.metrics()).lsm_cache_entry_count);
}

test "opaque context allocator failures release partial construction" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, contextAllocationLifecycle, .{});
}

test "opaque context rejects invalid runtime contracts before allocation" {
    const services = @import("kernel_runtime_services.zig");
    var context = client.Context{};
    try std.testing.expectError(error.InvalidAbiVersion, context.ensureWithRuntime(.{ .version = services.abi_version + 1 }));
    try std.testing.expect(context.handle == null);
    const alloc = std.testing.allocator;
    var bridge = services.memory.Allocator.fromStd(&alloc);
    bridge.version += 1;
    try std.testing.expectError(error.InvalidArgument, context.ensureWithRuntime(.{ .allocator = &bridge }));
    try std.testing.expect(context.handle == null);
}

test "opaque context stores use the borrowed VOPR filesystem and release handles" {
    const services = @import("kernel_runtime_services.zig");
    const alloc = std.testing.allocator;
    var simulator = try @import("vopr").vopr_io.VoprIo.init(.{
        .task_allocator = alloc,
        .file_allocator = alloc,
        .net_allocator = alloc,
        .process_allocator = alloc,
        .instrumentation_allocator = alloc,
    });
    defer simulator.deinit();
    const io = simulator.io();
    var borrow = services.executor.Borrow.init(&io);
    var allocator_bridge = services.memory.Allocator.fromStd(&alloc);
    {
        var context = client.Context{};
        try context.ensureWithRuntime(.{
            .context = .{ .auth_storage_path = .fromSlice("/vopr/linked-context/auth") },
            .allocator = &allocator_bridge,
            .io = &borrow,
        });
        defer context.deinit();
        var users = try context.systemStore(alloc, "system/auth-users");
        defer users.deinit();
        var write = try users.beginWrite();
        errdefer write.abort();
        try write.put("user", "value");
        try write.commit();
        var read = try users.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("value", try read.get("user"));
        try std.testing.expect(try simulator.storageBytesUnderPrefix("/vopr/linked-context") > 0);
    }
    const resources = simulator.resourceSnapshot();
    try std.testing.expectEqual(@as(usize, 0), resources.open_file_handles);
    try std.testing.expectEqual(@as(usize, 0), resources.active_tasks);
}

test "opaque owner lookup and typed scans preserve metadata scope digest and row versions" {
    const alloc = std.testing.allocator;
    const contract = @import("../api/local_query_contract.zig");
    const path = "/tmp/antfly-kernel-relational-lookup-scan";
    cleanup(path);
    defer cleanup(path);
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"count_id","keys":[{"column":"count","direction":"desc"},{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"string"},"count":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var owner = try client.Owner.open(.{ .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 7301, .has_identity_namespace = 1, .identity_table_id = 73, .identity_shard_id = 7301, .identity_range_id = 7301, .schema_json = .fromSlice(schema) });
    defer owner.deinit();
    var applied = try owner.batchJson("rows", "{\"inserts\":{\"r\":{\"id\":\"a\",\"count\":9007199254740993}},\"sync_level\":\"write\"}");
    applied.deinit();
    const full_request = try contract.encodeStorageKernelLookupRequest(alloc, "r", .{ .include_primary_digest = true });
    defer alloc.free(full_request);
    var full = try owner.lookupJson("rows", full_request);
    defer full.deinit();
    try std.testing.expect(full.expectedContentDigest() != null);
    const projection_request = try contract.encodeStorageKernelLookupRequest(alloc, "r", .{ .include_primary_digest = true, .include_all_fields = false, .fields = &.{"id"} });
    defer alloc.free(projection_request);
    var projection = try owner.lookupJson("rows", projection_request);
    defer projection.deinit();
    try std.testing.expectEqual(full.version(), projection.version());
    try std.testing.expectEqualSlices(u8, &full.expectedContentDigest().?, &projection.expectedContentDigest().?);
    try std.testing.expect(std.mem.indexOf(u8, full.bytes(), "9007199254740993") != null);
    try std.testing.expect(std.mem.indexOf(u8, projection.bytes(), "count") == null);
    const wrong_scope = try contract.encodeStorageKernelLookupRequest(alloc, "r", .{ .restore_staging_scope = @splat(0xff) });
    defer alloc.free(wrong_scope);
    try std.testing.expectError(error.RestoreStagingScopeChanged, owner.lookupJson("rows", wrong_scope));
    const topology_request = try contract.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" });
    defer alloc.free(topology_request);
    var topology = try owner.lookupJson("rows", topology_request);
    defer topology.deinit();
    try std.testing.expectEqual(@as(u64, 0), topology.version());
    try std.testing.expect(topology.expectedContentDigest() == null);
    try std.testing.expect(std.mem.indexOf(u8, topology.bytes(), "catalog_digest") != null);
    const scan_request = try contract.encodeStorageKernelScanRequest(alloc, "", "", .{ .relational_query_json = "{\"schema_version\":1,\"fields\":[\"count\"]}", .include_documents = true, .limit = 4 });
    defer alloc.free(scan_request);
    var scanned = try owner.scanNdjson("rows", scan_request);
    defer scanned.deinit();
    var decoded = try std.json.parseFromSlice(std.json.Value, alloc, std.mem.trim(u8, scanned.bytes(), "\n"), .{ .parse_numbers = false });
    defer decoded.deinit();
    try std.testing.expectEqualStrings("1", decoded.value.object.get("schema_version").?.number_string);
    const expected_version = try std.fmt.allocPrint(alloc, "{d}", .{full.version()});
    defer alloc.free(expected_version);
    try std.testing.expectEqualStrings(expected_version, decoded.value.object.get("version").?.string);
    try std.testing.expectEqualStrings("9007199254740993", decoded.value.object.get("row").?.object.get("count").?.number_string);
    try std.testing.expect(decoded.value.object.get("row").?.object.get("id") == null);
    var second = try owner.batchJson("rows", "{\"inserts\":{\"s\":{\"id\":\"b\",\"count\":9007199254740992}},\"sync_level\":\"write\"}");
    second.deinit();
    const indexed_request = try contract.encodeStorageKernelScanRequest(alloc, "", "", .{
        .relational_query_json = "{\"schema_version\":1,\"index\":\"count_id\",\"fields\":[]}",
        .include_documents = true,
        .limit = 1,
    });
    defer alloc.free(indexed_request);
    const deadline = @import("antfly_platform").time.monotonicNs() + 15 * std.time.ns_per_s;
    var indexed = while (true) {
        break owner.scanNdjsonWithOptions("rows", indexed_request, .{ .execution_deadline_ns = deadline }) catch |err| switch (err) {
            error.RelationalIndexNotReady => {
                if (@import("antfly_platform").time.monotonicNs() >= deadline) return err;
                try std.testing.io.sleep(.fromMilliseconds(5), .awake);
                continue;
            },
            else => return err,
        };
    };
    defer indexed.deinit();
    var index_row = try std.json.parseFromSlice(std.json.Value, alloc, std.mem.trim(u8, indexed.bytes(), "\n"), .{});
    defer index_row.deinit();
    try std.testing.expectEqualStrings("r", index_row.value.object.get("_id").?.string);
    try std.testing.expectEqual(@as(usize, 0), index_row.value.object.get("row").?.object.count());
    const after = index_row.value.object.get("cursor").?.string;
    const resumed_query = try std.fmt.allocPrint(alloc, "{{\"schema_version\":1,\"index\":\"count_id\",\"fields\":[],\"after\":\"{s}\"}}", .{after});
    defer alloc.free(resumed_query);
    const resumed_request = try contract.encodeStorageKernelScanRequest(alloc, "", "", .{ .relational_query_json = resumed_query, .include_documents = true, .limit = 1 });
    defer alloc.free(resumed_request);
    var resumed = try owner.scanNdjson("rows", resumed_request);
    defer resumed.deinit();
    try std.testing.expect(std.mem.startsWith(u8, resumed.bytes(), "{\"_id\":\"s\""));
}

test "opaque WAL rejects custom simulation hooks even without a context pointer" {
    const Hooks = struct {
        fn now(_: ?*anyopaque) u64 {
            return 1;
        }
        fn sleep(_: ?*anyopaque, _: u64) void {}
        fn wait(_: ?*anyopaque, _: u64) !void {}
    };
    try std.testing.expectError(error.UnsupportedKernelWalOptions, wal_client.WAL.open("/unused", wal_client.WalOptions{ .clock = .{ .now_ns_fn = Hooks.now, .sleep_ns_fn = Hooks.sleep } }));
    try std.testing.expectError(error.UnsupportedKernelWalOptions, wal_client.WAL.open("/unused", TestWalOptions{ .commit_scheduler = .{ .wait_ns_fn = Hooks.wait } }));
}
