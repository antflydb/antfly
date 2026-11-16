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

// Consumer-owned completion references. Receipts preserve uncertain registration
// identities; acknowledgements follow the consumer's durable decision.
const std = @import("std");
const wire = @import("../pgwire/completion.zig");
const server_mod = @import("http_server.zig");
const operation = @import("antfly_local_sources").api_operation;
const refs = @import("../metadata/sql_completion_references.zig");
const catalog = @import("antfly_local_sources").sql_catalog;
fn registrationStore(server: *server_mod.ApiHttpServer) !*@import("transactions.zig").DurableSessionStore {
    const store = server.txn_sessions.durable orelse return error.DurabilityUnavailable;
    const issuer = if (server.txn_sessions.durable_scope == .cluster_shared) blk: {
        const router = server.cfg.session_router orelse return error.CompletionOwnerUnavailable;
        const node = router.localNodeId();
        if (node == 0) return error.CompletionOwnerUnavailable;
        break :blk node;
    } else 0;
    try store.bindRegistrationIssuer(issuer);
    return store;
}
fn parseId(encoded: []const u8) ![16]u8 {
    var id: [16]u8 = undefined;
    if (encoded.len != 32) return error.InvalidArgument;
    _ = try std.fmt.hexToBytes(&id, encoded);
    return id;
}
fn owner(receipt: catalog.DdlReceipt) !refs.Owner {
    var result: refs.Owner = .{ .table_id = try std.fmt.parseInt(u64, receipt.table_id, 10), .schema_version = receipt.schema_version };
    if (receipt.restore_job_id) |job| {
        result.kind = .restore;
        result.table_id = try std.fmt.parseInt(u64, job, 10);
    } else if (receipt.fk_generation_publication_id) |id| {
        result.kind = if (receipt.schema_version == 0) .fk_initial else .fk_generation;
        result.publication_id = try parseId(id);
    }
    return result;
}
fn change(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, reference: [16]u8, target: refs.Owner, action: @FieldType(refs.Command, "action"), context: operation.RequestContext, managed: bool, retired_through: u64) !void {
    var authority = context;
    authority.fk_generation_publication_authority = true;
    authority.setting_admin = true;
    const bytes = try server.source.systemCatalog(alloc, authority, .{ .sql_completion_reference_mutate = .{ .reference_id = reference, .owner = target, .action = action, .managed = managed, .retired_through = retired_through } });
    defer alloc.free(bytes);
}
// Keep permits until diagnostics and disconnect have joined. Taking capacity
// never allocates an outbox ID; successful reads incur no durable writes.
pub fn reserveJoined(server: *server_mod.ApiHttpServer, completion: *wire.Completion, desired: usize, network: std.Io) !void {
    if (completion.capacity.remaining >= desired) return;
    const io = server_mod.ApiHttpServer.configuredDurableIo(server.cfg) orelse return error.SqlWriteCapacityUnavailable;
    const store = try registrationStore(server);
    const Job = struct {
        store: *@import("transactions.zig").DurableSessionStore,
        count: usize,
        network: std.Io,
        failure: ?anyerror = null,
        done: std.Io.Event = .unset,
        fn run(job: *@This()) void {
            defer job.done.set(job.network);
            job.store.reserveRegistrations(job.count) catch |err| {
                job.failure = err;
            };
        }
        fn release(raw: *anyopaque, count: usize) void {
            const durable: *@import("transactions.zig").DurableSessionStore = @ptrCast(@alignCast(raw));
            durable.releaseRegistrations(count);
        }
    };
    var job = Job{ .store = store, .count = desired - completion.capacity.remaining, .network = network };
    var future = try io.concurrent(Job.run, .{&job});
    job.done.waitUncancelable(network);
    @import("protected_future.zig").wait(io, &future);
    if (job.failure) |err| return err;
    completion.capacity = .{ .context = store, .remaining = desired, .release = Job.release };
}
// Persist the issuer intent and the target proof before the coordinator may
// seal its plan. A failed/ambiguous preparation never admits the mutation.
pub fn prepareTransaction(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, completion: *wire.Completion, id: [16]u8) !void {
    const encoded_id = std.fmt.bytesToHex(id, .lower);
    const store = try registrationStore(server);
    for (completion.outcomes.items) |outcome| if (outcome.transaction_id) |old| {
        if (!std.mem.eql(u8, old, &encoded_id) or outcome.registration_node_id == null) continue;
        var record = (try store.loadRegistration(alloc, try parseId(outcome.reference_id.?))) orelse continue;
        defer record.deinit();
        if (record.value.prepared and !record.value.joined) {
            if (!record.value.execution_admitted) return error.CompletionUnavailable;
            return;
        }
    };
    if (completion.capacity.remaining == 0 or completion.outcomes.items.len >= wire.max_outcomes) return error.SessionLimitExceeded;
    const owned_id = try alloc.dupe(u8, &encoded_id);
    completion.outcomes.append(alloc, .{ .transaction_id = owned_id }) catch |err| {
        alloc.free(owned_id);
        return err;
    };
    const outcome = &completion.outcomes.items[completion.outcomes.items.len - 1];
    const encoded = try alloc.alloc(u8, 32);
    errdefer if (outcome.reference_id == null) alloc.free(encoded);
    var seed: [8]u8 = undefined;
    try server.inferenceIo().randomSecure(&seed);
    const local_node = if (server.cfg.session_router) |router| router.localNodeId() else 0;
    const target_node = (try server.txn_sessions.getOwnerNodeId(alloc, id)) orelse @import("transactions.zig").sessionOwnerNodeId(id);
    const local = server.txn_sessions.durable_scope == .cluster_shared or target_node == local_node;
    const reference = if (local) try store.createLocalPreparedRegistration(seed, outcome.*, id) else try store.createPreparedRegistration(seed, outcome.*);
    completion.capacity.remaining -= 1;
    completion.execution_owned = true;
    @memcpy(encoded, &std.fmt.bytesToHex(reference, .lower));
    outcome.reference_id = encoded;
    outcome.registration_node_id = if (server.cfg.session_router) |router| router.localNodeId() else 0;
    if (completion.join_begin == null) completion.join_begin = completion.outcomes.items.len - 1;
    if (local) return;
    const floor = try store.registrationFloor();
    if (!try ownerAction(server, alloc, .{ .transaction_id = id, .reference_id = reference, .action = .prepare, .managed = true, .retired_through = floor }, true)) return error.CompletionUnavailable;
    try store.putRegistration(reference, .{ .outcome = .{ .transaction_id = outcome.transaction_id }, .phase = .registered, .prepared = true, .execution_admitted = true });
}
fn metadataTarget(input: operation.SQLCompletionOwner) refs.Owner {
    return .{ .table_id = input.id, .schema_version = input.schema_version, .kind = switch (input.kind) {
        .table => .table,
        .fk_initial => .fk_initial,
        .fk_generation => .fk_generation,
        .restore => .restore,
    }, .publication_id = input.publication_id };
}
pub fn prepareMetadata(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, completion: *wire.Completion, target: operation.SQLCompletionOwner) !void {
    const store = try registrationStore(server);
    if (target.id == 0 or ((target.kind == .fk_initial or target.kind == .fk_generation) and target.publication_id == null)) return error.InvalidArgument;
    if (completion.join_begin) |begin| for (completion.outcomes.items[begin..]) |outcome| {
        const json = outcome.ddl_receipt_json orelse continue;
        var receipt = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, json, .{});
        defer receipt.deinit();
        if (!refs.sameOwner(try owner(receipt.value), metadataTarget(target))) continue;
        var record = (try store.loadRegistration(alloc, try parseId(outcome.reference_id.?))) orelse continue;
        defer record.deinit();
        if (record.value.prepared and !record.value.joined) {
            if (!record.value.execution_admitted) return error.CompletionUnavailable;
            return;
        }
    };
    if (completion.capacity.remaining == 0 or completion.outcomes.items.len >= wire.max_outcomes) return error.SessionLimitExceeded;
    const identity = try std.fmt.allocPrint(alloc, "{d}", .{target.id});
    defer alloc.free(identity);
    const publication_hex = if (target.publication_id) |id| std.fmt.bytesToHex(id, .lower) else @as([32]u8, @splat(0));
    const receipt: catalog.DdlReceipt = .{ .database = "", .namespace = "", .table = "", .table_id = identity, .schema_version = target.schema_version, .state = .pending, .restore_job_id = if (target.kind == .restore) identity else null, .fk_generation_publication_id = if (target.publication_id != null) &publication_hex else null };
    const json = try std.json.Stringify.valueAlloc(alloc, receipt, .{});
    var transferred = false;
    errdefer if (!transferred) alloc.free(json);
    const encoded = try alloc.alloc(u8, 32);
    errdefer if (!transferred) alloc.free(encoded);
    var seed: [8]u8 = undefined;
    try server.inferenceIo().randomSecure(&seed);
    try completion.outcomes.ensureUnusedCapacity(alloc, 1);
    const reference = try store.createPreparedRegistration(seed, .{ .ddl_receipt_json = json });
    completion.capacity.remaining -= 1;
    @memcpy(encoded, &std.fmt.bytesToHex(reference, .lower));
    const index = completion.outcomes.items.len;
    completion.outcomes.appendAssumeCapacity(.{ .reference_id = encoded, .ddl_receipt_json = json, .registration_node_id = if (server.cfg.session_router) |router| router.localNodeId() else 0 });
    // The collector now owns both allocations even if preparation is refused.
    // From here on return errors through a value so errdefer cannot free them.
    transferred = true;
    const admitted = metadataPreparation(server, alloc, store, reference, target, json);
    if (completion.join_begin == null) completion.join_begin = index;
    completion.execution_owned = true;
    return admitted;
}
fn metadataPreparation(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, store: *@import("transactions.zig").DurableSessionStore, reference: [16]u8, target: operation.SQLCompletionOwner, json: []const u8) !void {
    try change(server, alloc, reference, metadataTarget(target), .prepare, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s }, true, try store.registrationFloor());
    try store.putRegistration(reference, .{ .outcome = .{ .ddl_receipt_json = json }, .prepared = true, .execution_admitted = true, .phase = .registered });
}
pub fn rejectMetadata(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, completion: *wire.Completion, target: operation.SQLCompletionOwner) !void {
    const store = try registrationStore(server);
    const begin = completion.join_begin orelse return;
    for (completion.outcomes.items[begin..]) |outcome| {
        const json = outcome.ddl_receipt_json orelse continue;
        var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, json, .{});
        defer parsed.deinit();
        if (!refs.sameOwner(try owner(parsed.value), metadataTarget(target))) continue;
        const reference = try parseId(outcome.reference_id orelse continue);
        var record = (try store.loadRegistration(alloc, reference)) orelse continue;
        defer record.deinit();
        if (!record.value.prepared or record.value.joined) continue;
        record.value.execution_admitted = false;
        try store.putRegistration(reference, record.value);
    }
}
pub fn prepareMetadataWithContext(context: operation.RequestContext, target: operation.SQLCompletionOwner) !void {
    const capability = context.sql_completion orelse return;
    const prepare = capability.prepare_metadata orelse return error.SqlWriteCapacityUnavailable;
    try prepare(capability.ptr, target);
}
pub fn rejectMetadataBeforeSubmission(context: operation.RequestContext, target: operation.SQLCompletionOwner) !void {
    const capability = context.sql_completion orelse return;
    const reject = capability.reject_metadata orelse return error.SqlWriteCapacityUnavailable;
    try reject(capability.ptr, target);
}
pub fn rejectMetadataWithContext(context: operation.RequestContext, target: operation.SQLCompletionOwner, err: anyerror) !void {
    // Only native domain rejections prove non-admission. Transport, timeout,
    // cancellation, allocation and unknown-outcome errors keep the owner pinned.
    const definite = switch (err) {
        error.NotLeader, error.MetadataMutationNotApplied, error.RaftMutationRequestNotSent, error.TableTopologyProtocolUpgradeRequired, error.CatalogAlreadyExists, error.CatalogNotFound, error.TableAlreadyExists, error.TableNotFound, error.Forbidden, error.InvalidArgument, error.InvalidCatalogMutation, error.InvalidGenerationPublication, error.GenerationPublicationChanged, error.GenerationPublicationInProgress, error.SchemaVersionChanged, error.CatalogGenerationChanged, error.TableGenerationChanged, error.TableTransitionActive, error.ForeignKeyReferenced, error.ForeignKeyParentMissing, error.ConstraintRetirementRequired, error.UnsupportedOperation, error.SqlDependentConstraint, error.IdempotencyConflict, error.SqlWriteCapacityUnavailable, error.RestoreValidationPending, error.InvalidIdempotencyKey, error.InvalidSchemaUpdateRequest, error.SchemaVersionExhausted => true,
        else => false,
    };
    if (!definite) return;
    const capability = context.sql_completion orelse return;
    const reject = capability.reject_metadata orelse return error.SqlWriteCapacityUnavailable;
    try reject(capability.ptr, target);
}
// All these identities are known before submission, including CREATE's physical
// hash ID. Private preparation is independent of the target payload's existence.
pub fn metadataOwnerForCall(alloc: std.mem.Allocator, input: @import("../system_catalog/server_call.zig").Call) !?operation.SQLCompletionOwner {
    return switch (input) {
        .mutate => |request| blk: {
            if (request.mutation.action != .create or request.mutation.kind != .table) break :blk null;
            const physical = request.physical_name orelse break :blk null;
            var parsed = try @import("tables.zig").parseStoredCreateTableRequest(alloc, request.create_table_json orelse break :blk null);
            defer parsed.deinit(alloc);
            const table = @import("tables.zig").deriveTableRecord(physical, parsed);
            break :blk .{ .id = table.table_id, .schema_version = try @import("tables.zig").schemaVersion(table.schema_json) };
        },
        .fk_initial_create_begin => |plan| .{ .id = plan.child.table_id, .schema_version = 0, .kind = .fk_initial, .publication_id = plan.id },
        .fk_generation_publication_begin => |plan| .{ .id = plan.child_before.table_id, .schema_version = try @import("tables.zig").schemaVersion(plan.child_after.schema_json), .kind = .fk_generation, .publication_id = plan.id },
        else => null,
    };
}
fn discardOutcome(completion: *wire.Completion, alloc: std.mem.Allocator, index: usize) void {
    const removed = completion.outcomes.orderedRemove(index);
    if (removed.reference_id) |value| alloc.free(value);
    if (removed.transaction_id) |value| alloc.free(value);
    if (removed.ddl_receipt_json) |value| alloc.free(value);
}
fn joinPrepared(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, completion: *wire.Completion) !void {
    const store = try registrationStore(server);
    var index = completion.join_begin orelse return;
    const local = if (server.cfg.session_router) |router| router.localNodeId() else 0;
    while (index < completion.outcomes.items.len) {
        const outcome = completion.outcomes.items[index];
        if (outcome.registration_node_id == null or outcome.registration_node_id.? != local or outcome.reference_id == null) {
            index += 1;
            continue;
        }
        const reference = try parseId(outcome.reference_id.?);
        var record = (try store.loadRegistration(alloc, reference)) orelse {
            index += 1;
            continue;
        };
        defer record.deinit();
        if (!record.value.prepared or record.value.joined) {
            index += 1;
            continue;
        }
        const id = if (record.value.outcome.transaction_id) |encoded| try parseId(encoded) else null;
        const owner_node = if (id) |txn_id| (try server.txn_sessions.getOwnerNodeId(alloc, txn_id)) orelse @import("transactions.zig").sessionOwnerNodeId(txn_id) else local;
        // These references have never escaped the execution owner. Retire a
        // successful local proof and its intent in one atomic batch rather than
        // accumulating every autocommit in the connection's bounded manifest.
        if (id != null and record.value.execution_admitted and (server.txn_sessions.durable_scope == .cluster_shared or owner_node == local) and try store.completionReferenceSettled(id.?, reference)) {
            if (store.retireLocalPreparedRegistration(reference, id.?)) |_| {
                discardOutcome(completion, alloc, index);
                continue;
            } else |_| {}
        }
        record.value.joined = true;
        try store.putRegistration(reference, record.value);
        const request: OwnerRequest = .{ .reference_id = reference, .consumer_node_id = outcome.registration_node_id, .action = .status };
        const complete = consumerAction(server, alloc, request, true) catch false;
        if (complete) {
            var ack = request;
            ack.action = .acknowledge;
            if (consumerAction(server, alloc, ack, true) catch false) {
                discardOutcome(completion, alloc, index);
                continue;
            }
        }
        index += 1;
    }
    completion.join_begin = null;
}
pub fn pinJoined(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, completion: *wire.Completion, network: std.Io) void {
    if (completion.unknown or completion.outcomes.items.len == 0) return;
    const fresh = for (completion.outcomes.items) |outcome| {
        if (outcome.registration_node_id == null) break true;
    } else false;
    const has_prepared = completion.join_begin != null;
    if (!fresh and !has_prepared) return;
    const io = server_mod.ApiHttpServer.configuredDurableIo(server.cfg) orelse {
        completion.unknown = true;
        return;
    };
    const Job = struct {
        server: *server_mod.ApiHttpServer,
        alloc: std.mem.Allocator,
        completion: *wire.Completion,
        network: std.Io,
        done: std.Io.Event = .unset,
        fn run(job: *@This()) void {
            defer job.done.set(job.network);
            const store = registrationStore(job.server) catch {
                job.completion.unknown = true;
                return;
            };
            joinPrepared(job.server, job.alloc, job.completion) catch {
                job.completion.unknown = true;
                return;
            };
            for (job.completion.outcomes.items) |*outcome| {
                // Subsequent SQL execution must not re-probe the entire manifest.
                // Once issued, the durable consumer's settlement path owns retries.
                if (outcome.registration_node_id != null) continue;
                if (outcome.registration_node_id == null) {
                    var seed: [8]u8 = undefined;
                    job.server.inferenceIo().randomSecure(&seed) catch {
                        job.completion.unknown = true;
                        return;
                    };
                    // Persist the retry owner before attempting remote registration.
                    const encoded = job.alloc.alloc(u8, 32) catch {
                        job.completion.unknown = true;
                        return;
                    };
                    const reserved = job.completion.capacity.remaining > 0;
                    const reference = (if (reserved) store.createReservedRegistration(seed, outcome.*) else store.createRegistration(seed, outcome.*)) catch {
                        job.alloc.free(encoded);
                        job.completion.unknown = true;
                        return;
                    };
                    if (reserved) job.completion.capacity.remaining -= 1;
                    @memcpy(encoded, &std.fmt.bytesToHex(reference, .lower));
                    outcome.reference_id = encoded;
                    outcome.registration_node_id = if (job.server.cfg.session_router) |router| router.localNodeId() else 0;
                }
                const reference = parseId(outcome.reference_id.?) catch {
                    job.completion.unknown = true;
                    return;
                };
                _ = consumerAction(job.server, job.alloc, .{ .reference_id = reference, .consumer_node_id = outcome.registration_node_id, .action = .pin }, true) catch {
                    outcome.registration_pending = true;
                    continue;
                };
                outcome.registration_pending = false;
            }
        }
    };
    var job = Job{ .server = server, .alloc = alloc, .completion = completion, .network = network };
    var future = io.concurrent(Job.run, .{&job}) catch {
        completion.unknown = true;
        return;
    };
    job.done.waitUncancelable(network);
    @import("protected_future.zig").wait(io, &future);
}
fn pin(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, outcome: wire.Outcome, reference: [16]u8, retired_through: u64) !void {
    if (outcome.ddl_receipt_json) |encoded| {
        var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, encoded, .{});
        defer parsed.deinit();
        try change(server, alloc, reference, try owner(parsed.value), .pin, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s }, true, retired_through);
    } else if (outcome.transaction_id) |encoded| {
        if (!try ownerAction(server, alloc, .{ .transaction_id = try parseId(encoded), .reference_id = reference, .action = .pin, .managed = true, .retired_through = retired_through }, true)) return error.CompletionUnavailable;
    } else return error.InvalidArgument;
}
pub fn settled(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, receipt: wire.Receipt, context: operation.RequestContext) !bool {
    if ((receipt.version != 3 and receipt.version != 4 and receipt.version != 5 and receipt.version != 6 and receipt.version != 7 and receipt.version != 8 and receipt.version != 9 and receipt.version != 10 and receipt.version != 11 and receipt.version != 12) or receipt.unknown or receipt.outcomes.len > wire.max_outcomes) return false;
    for (receipt.outcomes) |outcome| {
        try context.ensureActive();
        const reference = try parseId(outcome.reference_id orelse return false);
        if (outcome.registration_node_id) |node| {
            if (!try consumerAction(server, alloc, .{ .reference_id = reference, .consumer_node_id = node, .action = .status }, true)) return false;
            continue;
        }
        if (outcome.ddl_receipt_json) |encoded| {
            var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, encoded, .{});
            defer parsed.deinit();
            const target = try owner(parsed.value);
            var authority = context;
            authority.fk_generation_publication_authority = true;
            const bytes = try server.source.systemCatalog(alloc, authority, .{ .sql_completion_reference = reference });
            defer alloc.free(bytes);
            var status = try std.json.parseFromSlice(refs.Status, alloc, bytes, .{});
            defer status.deinit();
            const record = status.value.record orelse return false;
            if (!refs.sameOwner(target, record.owner)) return false;
            if (record.complete) continue;
            if (target.kind != .table) return false;
            // Resolve by immutable physical identity, never a reusable SQL name.
            const physical = status.value.physical_name orelse return false;
            const state = try @import("relational_constraint_status.zig").collect(alloc, server.source, server.table_reads orelse return false, physical, context);
            defer alloc.free(state);
            var coverage = try std.json.parseFromSlice(struct { schema_version: u32, state: []const u8, retirement: ?std.json.Value = null }, alloc, state, .{ .ignore_unknown_fields = true });
            defer coverage.deinit();
            if (coverage.value.schema_version < target.schema_version or coverage.value.retirement != null or !(std.mem.eql(u8, coverage.value.state, "enforced") or std.mem.eql(u8, coverage.value.state, "invalid"))) return false;
            try change(server, alloc, reference, target, .complete, context, false, 0);
        } else if (outcome.transaction_id) |encoded| {
            if (!try ownerAction(server, alloc, .{ .transaction_id = try parseId(encoded), .reference_id = reference, .action = .status }, true)) return false;
        } else return false;
    }
    return true;
}
pub fn acknowledge(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, receipt: wire.Receipt, context: operation.RequestContext) !bool {
    if ((receipt.version != 3 and receipt.version != 4 and receipt.version != 5 and receipt.version != 6 and receipt.version != 7 and receipt.version != 8 and receipt.version != 9 and receipt.version != 10 and receipt.version != 11 and receipt.version != 12) or receipt.unknown or receipt.outcomes.len > wire.max_outcomes) return false;
    for (receipt.outcomes) |outcome| {
        try context.ensureActive();
        const reference = try parseId(outcome.reference_id orelse return false);
        if (outcome.registration_node_id) |node| {
            if (!try consumerAction(server, alloc, .{ .reference_id = reference, .consumer_node_id = node, .action = .acknowledge }, true)) return false;
            continue;
        }
        if (outcome.ddl_receipt_json) |encoded| {
            var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, encoded, .{});
            defer parsed.deinit();
            try change(server, alloc, reference, try owner(parsed.value), .acknowledge, context, false, 0);
        } else if (outcome.transaction_id) |encoded| {
            if (!try ownerAction(server, alloc, .{ .transaction_id = try parseId(encoded), .reference_id = reference, .action = .acknowledge }, true)) return false;
        } else return false;
    }
    return true;
}
// A bounded probe can persist partial progress without discarding other owners.
pub fn remaining(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, receipt: wire.Receipt, context: operation.RequestContext) !wire.Receipt {
    if ((receipt.version != 3 and receipt.version != 4 and receipt.version != 5 and receipt.version != 6 and receipt.version != 7 and receipt.version != 8 and receipt.version != 9 and receipt.version != 10 and receipt.version != 11 and receipt.version != 12) or receipt.unknown or receipt.outcomes.len > wire.max_outcomes) return .{ .unknown = true, .outcomes = try alloc.dupe(wire.Outcome, receipt.outcomes) };
    var pending: std.ArrayList(wire.Outcome) = .empty;
    errdefer pending.deinit(alloc);
    for (receipt.outcomes, 0..) |outcome, index| {
        context.ensureActive() catch {
            try pending.appendSlice(alloc, receipt.outcomes[index..]);
            break;
        };
        const complete = settled(server, alloc, .{ .outcomes = &.{outcome} }, context) catch false;
        if (!complete) try pending.append(alloc, outcome);
    }
    return .{ .outcomes = try pending.toOwnedSlice(alloc) };
}

pub const OwnerRequest = struct { transaction_id: [16]u8 = @splat(0), reference_id: [16]u8, action: enum { pin, prepare, quiesce, abandon, status, acknowledge, forget }, consumer_node_id: ?u64 = null, managed: bool = false, retired_through: u64 = 0 };
// The originating node serializes registration, probing, and acknowledgement.
// Remote stores retain compact retirement checkpoints, so delayed retries cannot
// recreate acknowledged proofs, including across process restarts.
fn consumerAction(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, request: OwnerRequest, forward: bool) anyerror!bool {
    if (request.action != .pin and request.action != .status and request.action != .acknowledge) return error.InvalidArgument;
    const local = if (server.cfg.session_router) |router| router.localNodeId() else 0;
    const node = request.consumer_node_id orelse return error.InvalidArgument;
    if (node != local) return forwardOwner(server, alloc, request, node, forward);
    const store = try registrationStore(server);
    const lock = store.lockRegistration(request.reference_id);
    defer lock.unlock();
    var record = (try store.loadRegistration(alloc, request.reference_id)) orelse return request.action == .acknowledge;
    defer record.deinit();
    const floor = try store.registrationFloor();
    var outcome = record.value.outcome;
    const hex = std.fmt.bytesToHex(request.reference_id, .lower);
    outcome.reference_id = &hex;
    outcome.registration_node_id = null;
    if (record.value.prepared and !record.value.joined) return false;
    // This durable phase is a positive terminal witness, including when an
    // issuer-side delete failed after remote retirement before publication.
    if (record.value.phase == .acknowledging and request.action == .status) return true;
    if (record.value.prepared and !record.value.execution_admitted) {
        if (request.action != .acknowledge) return true;
        if (record.value.outcome.ddl_receipt_json) |json| {
            var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, json, .{});
            defer parsed.deinit();
            try change(server, alloc, request.reference_id, try owner(parsed.value), .abandon, .{}, true, floor);
            try change(server, alloc, request.reference_id, try owner(parsed.value), .acknowledge, .{}, true, floor);
        } else {
            const target_id = try parseId(record.value.outcome.transaction_id.?);
            _ = try ownerAction(server, alloc, .{ .transaction_id = target_id, .reference_id = request.reference_id, .action = .abandon, .managed = true, .retired_through = floor }, true);
            _ = try ownerAction(server, alloc, .{ .transaction_id = target_id, .reference_id = request.reference_id, .action = .acknowledge, .managed = true, .retired_through = floor }, true);
        }
        try store.removeRegistration(request.reference_id);
        return true;
    }
    if (record.value.prepared and record.value.outcome.transaction_id != null and record.value.phase != .acknowledging) {
        _ = try ownerAction(server, alloc, .{ .transaction_id = try parseId(record.value.outcome.transaction_id.?), .reference_id = request.reference_id, .action = .quiesce, .managed = true, .retired_through = floor }, true);
    }
    if (record.value.phase == .registering) {
        pin(server, alloc, outcome, request.reference_id, floor) catch |err| {
            const probe_context: operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s };
            if (!(settled(server, alloc, .{ .outcomes = &.{outcome} }, probe_context) catch false)) return err;
        };
        record.value.phase = .registered;
        try store.putRegistration(request.reference_id, record.value);
    }
    if (request.action == .pin) return record.value.phase != .acknowledging;
    const context: operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s };
    if (request.action == .status) {
        if (record.value.phase == .acknowledging) return true;
        return settled(server, alloc, .{ .outcomes = &.{outcome} }, context);
    }
    if (record.value.phase != .acknowledging) {
        // A premature acknowledgement must not close registration and strand
        // a live consumer. Once terminal proof is observed it remains terminal.
        if (!try settled(server, alloc, .{ .outcomes = &.{outcome} }, context)) return false;
        record.value.phase = .acknowledging;
        try store.putRegistration(request.reference_id, record.value);
    }
    const complete = if (outcome.ddl_receipt_json) |encoded| blk: {
        var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, encoded, .{});
        defer parsed.deinit();
        try change(server, alloc, request.reference_id, try owner(parsed.value), .acknowledge, context, true, floor);
        break :blk true;
    } else try ownerAction(server, alloc, .{ .transaction_id = try parseId(outcome.transaction_id.?), .reference_id = request.reference_id, .action = .acknowledge, .managed = true, .retired_through = floor }, true);
    if (complete) try store.removeRegistration(request.reference_id);
    return complete;
}
fn forwardOwner(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, request: OwnerRequest, node: u64, forward: bool) !bool {
    if (!forward) return error.CompletionWrongOwner;
    const router = server.cfg.session_router orelse return error.CompletionOwnerUnavailable;
    const executor = server.cfg.session_executor orelse return error.CompletionOwnerUnavailable;
    const target = (try router.nodeBaseUri(alloc, node)) orelse return error.CompletionOwnerUnavailable;
    defer alloc.free(target);
    const uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/sql", .{target});
    defer alloc.free(uri);
    const body = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(body);
    const auth = @import("internal_service_auth.zig");
    const token = try auth.tokenAlloc(alloc, .{ .secret = server.cfg.internal_service_secret orelse return error.CompletionOwnerUnavailable, .issuer = server.cfg.internal_service_issuer orelse return error.CompletionOwnerUnavailable }, @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    defer alloc.free(token);
    var response = try executor.execute(alloc, .{ .method = .POST, .uri = uri, .content_type = "application/json", .body = body, .timeout_ms = 1500, .headers = &.{ .{ .name = auth.header_name, .value = token }, .{ .name = "X-Antfly-SQL-Settlement", .value = "owner-v12" } } });
    defer response.deinit(alloc);
    if (response.status != 200) return error.CompletionOwnerUnavailable;
    var result = try std.json.parseFromSlice(struct { complete: bool }, alloc, response.body, .{});
    defer result.deinit();
    return result.value.complete;
}
// Node-local records and their proofs retain the original node identity after
// payload expiry. Shared stores remain directly authoritative on every replica.
pub fn ownerAction(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, request: OwnerRequest, forward: bool) !bool {
    if (request.consumer_node_id != null) return consumerAction(server, alloc, request, forward);
    const txn = @import("transactions.zig");
    if (server.txn_sessions.durable_scope != .cluster_shared) {
        const node = (try server.txn_sessions.getOwnerNodeId(alloc, request.transaction_id)) orelse txn.sessionOwnerNodeId(request.transaction_id);
        const local = if (server.cfg.session_router) |router| router.localNodeId() else 0;
        if (node != local) return forwardOwner(server, alloc, request, node, forward);
    }
    const store = server.txn_sessions.durable orelse return error.DurabilityUnavailable;
    if (server.txn_sessions.durable_scope == .cluster_shared and !store.supportsSharedMutations())
        return error.CompletionOwnerUnavailable;
    return switch (request.action) {
        .forget => try store.changeCompletionReference(request.transaction_id, request.reference_id, false, if (request.managed) request.retired_through else null, .forget),
        .quiesce => if (try store.completionReferenceSettled(request.transaction_id, request.reference_id)) true else store.changeCompletionReference(request.transaction_id, request.reference_id, false, if (request.managed) request.retired_through else null, .quiesce),
        .prepare, .abandon => store.changeCompletionReference(request.transaction_id, request.reference_id, false, if (request.managed) request.retired_through else null, switch (request.action) {
            .prepare => .prepare,
            .quiesce => .quiesce,
            .abandon => .abandon,
            else => unreachable,
        }),
        .pin => blk: {
            if (request.managed) _ = try store.managedCompletion(request.transaction_id, request.reference_id, false, request.retired_through) else try store.pinCompletion(request.transaction_id, request.reference_id);
            break :blk true;
        },
        .status => store.completionReferenceSettled(request.transaction_id, request.reference_id),
        .acknowledge => if (request.managed) store.managedCompletion(request.transaction_id, request.reference_id, true, request.retired_through) else store.acknowledgeCompletion(request.transaction_id, request.reference_id),
    };
}
// Only pre-existing, unpublished subscriptions are eligible. Dropping one
// does not assert completion: missing consumer status remains unresolved, and
// the transaction/job's own recovery and retention rules remain authoritative.
pub fn recoverUnpublished(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, limit: usize) !void {
    if (server.txn_sessions.durable == null) return;
    const store = try registrationStore(server);
    const page = try store.registrationRecoveryPage(alloc, server.sql_completion_recovery_after, limit);
    defer alloc.free(page.references);
    const deadline = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
    for (page.references) |reference| {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return;
        server.sql_completion_recovery_after = @import("completion_retirement.zig").sequence(reference);
        const lock = store.lockRegistration(reference);
        defer lock.unlock();
        var record = (try store.loadRegistration(alloc, reference)) orelse continue;
        defer record.deinit();
        if (!record.value.prepared or record.value.joined) continue;
        if (record.value.phase != .discarding) {
            record.value.phase = .discarding;
            try store.putRegistration(reference, record.value);
        }
        const floor = try store.registrationFloor();
        if (record.value.outcome.ddl_receipt_json) |json| {
            var parsed = try std.json.parseFromSlice(catalog.DdlReceipt, alloc, json, .{});
            defer parsed.deinit();
            change(server, alloc, reference, try owner(parsed.value), .forget, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s }, true, floor) catch continue;
        } else {
            if (!(ownerAction(server, alloc, .{ .transaction_id = try parseId(record.value.outcome.transaction_id.?), .reference_id = reference, .action = .forget, .managed = true, .retired_through = floor }, true) catch false)) continue;
        }
        try store.removeRegistration(reference);
    }
    server.sql_completion_recovery_after = page.after;
}
pub fn capableJoined(server: *server_mod.ApiHttpServer, network: std.Io) bool {
    if (server.txn_sessions.durable == null or !server.source.vtable.tracks_sql_submission) return false;
    _ = registrationStore(server) catch return false;
    // Registration outboxes belong to their issuing node even with shared
    // transaction storage; every distributed issuer needs authenticated routing.
    if (server.cfg.session_router) |router| {
        if (router.localNodeId() != 0 and (server.cfg.session_executor == null or server.cfg.internal_service_secret == null or server.cfg.internal_service_issuer == null)) return false;
    }
    const io = server_mod.ApiHttpServer.configuredDurableIo(server.cfg) orelse return false;
    const Job = struct {
        server: *server_mod.ApiHttpServer,
        network: std.Io,
        done: std.Io.Event = .unset,
        ready: bool = false,
        fn run(job: *@This()) void {
            defer job.done.set(job.network);
            const bytes = job.server.source.systemCatalog(job.server.alloc, .{ .fk_generation_publication_authority = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s }, .sql_completion_ready) catch return;
            defer job.server.alloc.free(bytes);
            job.ready = std.mem.eql(u8, bytes, "true");
        }
    };
    var job = Job{ .server = server, .network = network };
    var future = io.concurrent(Job.run, .{&job}) catch return false;
    job.done.waitUncancelable(network);
    @import("protected_future.zig").wait(io, &future);
    return job.ready;
}
