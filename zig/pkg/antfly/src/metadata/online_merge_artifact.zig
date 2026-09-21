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

//! Executable immutable-artifact slice for the online merge driver. Transport
//! offsets live in the receiver's fsync-before-receipt ledger, not a duplicate
//! metadata cursor. Retrying a lost write response first reads that ledger.
const std = @import("std");
const online = @import("online_merge.zig");
const transfer = @import("../storage/db/source_artifact_transfer.zig");
const Context = @import("../api/operation.zig").RequestContext;
const Allocator = std.mem.Allocator;

pub const Transport = struct {
    ptr: *anyopaque,
    request: *const fn (*anyopaque, Allocator, u64, []const u8, transfer.Request, Context) anyerror![]u8,

    /// Binds the existing compiled-owner operation (or authenticated routed
    /// equivalent), never a public table route or local-file path supplied by a
    /// caller. The caller owns transport/context lifetime for this one slice.
    pub fn from(owner: anytype) Transport {
        return .{ .ptr = owner, .request = struct {
            fn call(ptr: *anyopaque, alloc: Allocator, group: u64, table: []const u8, request: transfer.Request, context: Context) ![]u8 {
                const value: @TypeOf(owner) = @ptrCast(@alignCast(ptr));
                return value.onlineSourceArtifact(alloc, group, table, request, context);
            }
        }.call };
    }
};

fn call(comptime T: type, alloc: Allocator, transport: Transport, group: u64, table: []const u8, request: transfer.Request, context: Context, max_bytes: usize) !std.json.Parsed(T) {
    try context.ensureActive();
    const raw = try transport.request(transport.ptr, alloc, group, table, request, context);
    defer alloc.free(raw);
    try context.ensureActive();
    if (raw.len > max_bytes) return error.OnlineMergeReceiptMismatch;
    return std.json.parseFromSlice(T, alloc, raw, .{ .allocate = .alloc_always });
}

/// Transfers at most one MiB per invocation. Once transfer completes, each
/// invocation advances the durable verifier by one bounded page. Verification
/// initialization retains the shared manifest parse's 16 MiB hard limit; a
/// false result at EOF means verification work remains, not transfer failure.
pub fn step(alloc: Allocator, donor: Transport, replica: Transport, table: []const u8, state: online.State, context: Context) !bool {
    try state.validate();
    if (state.phase != .snapshot) return error.InvalidOnlineMergeState;
    const certificate = state.certificate orelse return error.InvalidOnlineMergeState;
    // Both endpoints are replicas of the donor group. This is source-pin
    // failover recovery, NOT admission of source authority on a merge receiver.
    var described = try call(transfer.Descriptor, alloc, donor, state.scope.fence.owner_group_id, table, .{ .describe = state.scope }, context, 16 * 1024);
    defer described.deinit();
    const descriptor = described.value;
    if (!std.meta.eql(descriptor.scope, state.scope) or !descriptor.certificate.eql(certificate) or descriptor.total_bytes == 0)
        return error.OnlineMergeReceiptMismatch;
    var observed = try call(transfer.Status, alloc, replica, state.scope.fence.owner_group_id, table, .{ .status = descriptor }, context, 4096);
    defer observed.deinit();
    const status = observed.value;
    if (status.next_offset > descriptor.total_bytes or (status.complete and status.next_offset != descriptor.total_bytes)) return error.OnlineMergeReceiptMismatch;
    if (status.complete) return true;
    if (status.next_offset == descriptor.total_bytes) {
        var finished = try call(transfer.Status, alloc, replica, state.scope.fence.owner_group_id, table, .{ .finish = descriptor }, context, 4096);
        defer finished.deinit();
        if (finished.value.next_offset != descriptor.total_bytes) return error.OnlineMergeReceiptMismatch;
        return finished.value.complete;
    }
    var read = try call(transfer.ReadResponse, alloc, donor, state.scope.fence.owner_group_id, table, .{ .read = .{ .descriptor = descriptor, .offset = status.next_offset } }, context, 2 * transfer.max_chunk_bytes);
    defer read.deinit();
    const chunk = read.value;
    const size = std.base64.standard.Decoder.calcSizeForSlice(chunk.data_base64) catch return error.OnlineMergeReceiptMismatch;
    if (chunk.offset != status.next_offset or size != @min(transfer.max_chunk_bytes, descriptor.total_bytes - status.next_offset)) return error.OnlineMergeReceiptMismatch;
    const decoded = try alloc.alloc(u8, size);
    defer alloc.free(decoded);
    std.base64.standard.Decoder.decode(decoded, chunk.data_base64) catch return error.OnlineMergeReceiptMismatch;
    if (!std.mem.eql(u8, &transfer.checksum(decoded), &chunk.digest)) return error.OnlineMergeReceiptMismatch;
    var written = try call(transfer.Status, alloc, replica, state.scope.fence.owner_group_id, table, .{ .write = .{ .descriptor = descriptor, .offset = chunk.offset, .data_base64 = chunk.data_base64, .digest = chunk.digest } }, context, 4096);
    defer written.deinit();
    if (written.value.next_offset != status.next_offset + size or written.value.complete) return error.OnlineMergeReceiptMismatch;
    return false;
}
