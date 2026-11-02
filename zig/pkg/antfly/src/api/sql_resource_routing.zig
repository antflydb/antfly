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
fn contains(txn: anytype, id: []const u8, kind: Kind) !bool {
    const key: []const u8 = switch (kind) {
        .prepared => &(try @import("sql_prepared.zig").resourceKey(id)),
        .connection => &(try @import("sql_connection_record.zig").key(id)),
    };
    _ = txn.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return true;
}
// Legacy random handles remain usable on their issuing node. Existence never
// grants access: normal resource loading still checks principal and ownership.
pub fn containsLocal(store: *transactions.DurableSessionStore, id: []const u8, kind: Kind) !bool {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try contains(&txn, id, kind);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try contains(&txn, id, kind);
        },
    };
}
test "SQL pgwire native HTTP handles bind issuer and preserve entropy" {
    const id = encode(7, @splat(0xaa));
    try std.testing.expectEqual(@as(u64, 7), try owner(&id));
    try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaa", id[16..]);
    try std.testing.expectError(error.InvalidArgument, owner("bad"));
}
