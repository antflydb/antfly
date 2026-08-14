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
const metadata_mod = @import("../metadata/domain.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");

const EncodedTransitionAction = struct {
    kind: enum {
        prepare_split_source,
        start_split_source,
        bootstrap_split_destination,
        catch_up_split_destination,
        finalize_split_source,
        rollback_split,
        accept_merge_receiver,
        catch_up_merge_receiver,
        finalize_merge,
        rollback_merge,
    },
    transition_id: u64,
    attempt_epoch: u64 = 0,
    source_group_id: ?u64 = null,
    destination_group_id: ?u64 = null,
    donor_group_id: ?u64 = null,
    receiver_group_id: ?u64 = null,
    allow_doc_identity_reassignment: bool = false,
    split_key: ?[]const u8 = null,
    source_range_end: ?[]const u8 = null,
    table_contract: metadata_transition_state.TransitionTableContract = .{},
};

const test_transition_table_contract: metadata_transition_state.TransitionTableContract = .{
    .table_id = 7,
    .table_name = "docs",
    .schema_json = "",
    .indexes_json = "{}",
    .source_identity = .{ .shard_id = 7, .range_id = 7 },
    .target_identity = .{ .shard_id = 7, .range_id = 7 },
};

fn requiredTransitionGroupId(value: ?u64) !u64 {
    const group_id = value orelse return error.InvalidTransitionActionRequest;
    if (group_id == 0) return error.InvalidTransitionActionRequest;
    return group_id;
}

pub fn parseSplitTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.SplitTransitionRecord {
    var parsed = try std.json.parseFromSlice(metadata_transition_state.SplitTransitionRecord, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.transition_id == 0 or parsed.value.attempt_epoch == 0 or
        parsed.value.source_group_id == 0 or parsed.value.destination_group_id == 0)
    {
        return error.InvalidTransitionActionRequest;
    }
    try parsed.value.table_contract.validateForSplit();
    const split_key = if (parsed.value.split_key) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (split_key) |value| alloc.free(value);
    const source_range_end = if (parsed.value.source_range_end) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (source_range_end) |value| alloc.free(value);
    const rollback_reason = if (parsed.value.rollback_reason) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (rollback_reason) |value| alloc.free(value);
    const table_contract = try parsed.value.table_contract.clone(alloc);
    return .{
        .transition_id = parsed.value.transition_id,
        .attempt_epoch = parsed.value.attempt_epoch,
        .source_group_id = parsed.value.source_group_id,
        .destination_group_id = parsed.value.destination_group_id,
        .phase = parsed.value.phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
        .table_contract = table_contract,
    };
}

pub fn parseMergeTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.MergeTransitionRecord {
    var parsed = try std.json.parseFromSlice(metadata_transition_state.MergeTransitionRecord, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.transition_id == 0 or parsed.value.donor_group_id == 0 or
        parsed.value.receiver_group_id == 0)
    {
        return error.InvalidTransitionActionRequest;
    }
    try parsed.value.table_contract.validateForMerge(
        parsed.value.allow_doc_identity_reassignment,
    );
    const rollback_reason = if (parsed.value.rollback_reason) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (rollback_reason) |value| alloc.free(value);
    const table_contract = try parsed.value.table_contract.clone(alloc);
    return .{
        .transition_id = parsed.value.transition_id,
        .donor_group_id = parsed.value.donor_group_id,
        .receiver_group_id = parsed.value.receiver_group_id,
        .phase = parsed.value.phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
        .table_contract = table_contract,
    };
}

pub fn parseTransitionAction(alloc: std.mem.Allocator, body: []const u8) !metadata_mod.TransitionAction {
    var parsed = try std.json.parseFromSlice(EncodedTransitionAction, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    switch (parsed.value.kind) {
        .prepare_split_source,
        .start_split_source,
        .bootstrap_split_destination,
        .catch_up_split_destination,
        .finalize_split_source,
        .rollback_split,
        => if (parsed.value.attempt_epoch == 0) return error.InvalidTransitionActionRequest,
        else => {},
    }
    if (parsed.value.transition_id == 0)
        return error.InvalidTransitionActionRequest;
    switch (parsed.value.kind) {
        .prepare_split_source,
        .start_split_source,
        .bootstrap_split_destination,
        .catch_up_split_destination,
        .finalize_split_source,
        .rollback_split,
        => try parsed.value.table_contract.validateForSplit(),
        .accept_merge_receiver,
        .catch_up_merge_receiver,
        .finalize_merge,
        .rollback_merge,
        => try parsed.value.table_contract.validateForMerge(
            parsed.value.allow_doc_identity_reassignment,
        ),
    }
    return switch (parsed.value.kind) {
        .prepare_split_source => try parsePrepareSplitTransitionAction(
            alloc,
            parsed.value,
        ),
        .start_split_source => .{
            .start_split_source = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .bootstrap_split_destination => .{
            .bootstrap_split_destination = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .catch_up_split_destination => .{
            .catch_up_split_destination = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .finalize_split_source => .{
            .finalize_split_source = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .rollback_split => .{
            .rollback_split = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .accept_merge_receiver => .{
            .accept_merge_receiver = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .catch_up_merge_receiver => .{
            .catch_up_merge_receiver = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .finalize_merge => .{
            .finalize_merge = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .rollback_merge => .{
            .rollback_merge = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
    };
}

fn parsePrepareSplitTransitionAction(
    alloc: std.mem.Allocator,
    encoded: EncodedTransitionAction,
) !metadata_mod.TransitionAction {
    const source_group_id = try requiredTransitionGroupId(encoded.source_group_id);
    const destination_group_id = try requiredTransitionGroupId(
        encoded.destination_group_id,
    );
    const raw_split_key = encoded.split_key orelse
        return error.InvalidTransitionActionRequest;
    if (raw_split_key.len == 0) return error.InvalidTransitionActionRequest;
    const split_key = try alloc.dupe(u8, raw_split_key);
    errdefer alloc.free(split_key);
    const source_range_end = if (encoded.source_range_end) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (source_range_end) |value| alloc.free(value);
    const table_contract = try encoded.table_contract.clone(alloc);
    return .{
        .prepare_split_source = .{
            .transition_id = encoded.transition_id,
            .attempt_epoch = encoded.attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .split_key = split_key,
            .source_range_end = source_range_end,
            .table_contract = table_contract,
        },
    };
}

pub fn freeSplitTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.SplitTransitionRecord) void {
    if (record.split_key) |value| alloc.free(value);
    if (record.source_range_end) |value| alloc.free(value);
    if (record.rollback_reason) |value| alloc.free(value);
    record.table_contract.deinitOwned(alloc);
    record.* = undefined;
}

pub fn freeMergeTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.MergeTransitionRecord) void {
    if (record.rollback_reason) |value| alloc.free(value);
    record.table_contract.deinitOwned(alloc);
    record.* = undefined;
}

pub fn freeTransitionActionOwned(alloc: std.mem.Allocator, action: *metadata_mod.TransitionAction) void {
    const table_contract: ?metadata_transition_state.TransitionTableContract = switch (action.*) {
        .none => null,
        inline else => |op| op.table_contract,
    };
    switch (action.*) {
        .prepare_split_source => |op| {
            alloc.free(op.split_key);
            if (op.source_range_end) |value| alloc.free(value);
        },
        else => {},
    }
    if (table_contract) |contract| {
        var owned = contract;
        owned.deinitOwned(alloc);
    }
    action.* = undefined;
}

test "transition wire parses merge identity reassignment" {
    const alloc = std.testing.allocator;
    const body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(
        EncodedTransitionAction{
            .kind = .catch_up_merge_receiver,
            .transition_id = 4,
            .donor_group_id = 10,
            .receiver_group_id = 9,
            .allow_doc_identity_reassignment = true,
            .table_contract = test_transition_table_contract,
        },
        .{},
    )});
    defer alloc.free(body);
    var action = try parseTransitionAction(alloc, body);
    defer freeTransitionActionOwned(alloc, &action);

    try std.testing.expect(action == .catch_up_merge_receiver);
    try std.testing.expect(action.catch_up_merge_receiver.allow_doc_identity_reassignment);
    try std.testing.expect(action.catch_up_merge_receiver.table_contract.eql(test_transition_table_contract));
}

test "transition wire rejects incomplete contracts" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidTransitionTableContract,
        parseTransitionAction(alloc,
            \\{"kind":"catch_up_merge_receiver","transition_id":4,"donor_group_id":10,"receiver_group_id":9}
        ),
    );
    try std.testing.expectError(
        error.InvalidTransitionActionRequest,
        parseTransitionAction(alloc,
            \\{"kind":"prepare_split_source","transition_id":4,"attempt_epoch":1,"source_group_id":10,"destination_group_id":9,"split_key":"","table_contract":{"table_id":7,"table_name":"docs","schema_json":"","indexes_json":"{}","source_identity":{"shard_id":7,"range_id":7},"target_identity":{"shard_id":7,"range_id":7}}}
        ),
    );
}

test "transition wire owns split record fields" {
    const alloc = std.testing.allocator;
    var record = try parseSplitTransitionRecord(alloc,
        \\{"transition_id":1,"attempt_epoch":1,"source_group_id":7,"destination_group_id":8,"split_key":"doc:m","rollback_reason":"retry","table_contract":{"table_id":7,"table_name":"docs","schema_json":"","indexes_json":"{}","source_identity":{"shard_id":7,"range_id":7},"target_identity":{"shard_id":7,"range_id":7}}}
    );
    defer freeSplitTransitionRecordOwned(alloc, &record);
    try std.testing.expectEqualStrings("doc:m", record.split_key.?);
    try std.testing.expectEqualStrings("retry", record.rollback_reason.?);
}
