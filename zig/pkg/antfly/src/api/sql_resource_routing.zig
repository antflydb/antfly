// HTTP SQL handles carry their immutable issuer, independently of transactions.
const std = @import("std");
const transactions = @import("transactions.zig");
pub const Kind = enum { prepared, connection };
pub fn encode(issuer: u64, entropy: [16]u8) [32]u8 {
    var bytes = entropy;
    std.mem.writeInt(u64, bytes[0..8], issuer, .big);
    return std.fmt.bytesToHex(bytes, .lower);
}
pub fn owner(id: []const u8) !u64 {
    if (id.len != 32) return error.InvalidArgument;
    var bytes: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&bytes, id);
    return std.mem.readInt(u64, bytes[0..8], .big);
}
fn owns(txn: anytype, alloc: std.mem.Allocator, id: []const u8, kind: Kind, local_node_id: u64) !bool {
    const key: []const u8 = switch (kind) {
        .prepared => &(try @import("sql_prepared.zig").resourceKey(id)),
        .connection => &(try @import("sql_connection_record.zig").key(id)),
    };
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    const limit: usize = switch (kind) {
        .connection => @import("sql_connection_record.zig").max_record_bytes,
        .prepared => @import("sql_prepared.zig").max_resource_bytes,
    };
    if (raw.len > limit) return error.SqlProgramLimitExceeded;
    // Read only routing metadata. Authentication, expiry and resource validation
    // remain the responsibility of the normal load on the selected issuer.
    const Ownership = struct { id: [32]u8, owner_node_id: u64 };
    var parsed = try std.json.parseFromSlice(Ownership, alloc, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return std.mem.eql(u8, &parsed.value.id, id) and parsed.value.owner_node_id == local_node_id;
}
// Legacy random handles may override their encoded issuer only when the
// persisted record is owned here. Shared-store visibility does not imply ownership.
pub fn ownsLocal(store: *transactions.DurableSessionStore, id: []const u8, kind: Kind, local_node_id: u64) !bool {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try owns(&txn, store.alloc, id, kind, local_node_id);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try owns(&txn, store.alloc, id, kind, local_node_id);
        },
    };
}
test "SQL pgwire native HTTP handles bind issuer and preserve entropy" {
    const id = encode(7, @splat(0xaa));
    try std.testing.expectEqual(@as(u64, 7), try owner(&id));
    try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaa", id[16..]);
    try std.testing.expectError(error.InvalidArgument, owner("bad"));
}
