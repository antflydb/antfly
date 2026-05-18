const std = @import("std");
const db_mod = @import("../pkg/antfly/src/storage/db/mod.zig");
const metadata_mod = @import("../pkg/antfly/src/metadata/mod.zig");

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    const replica_root = "/tmp/antfly-vdbbench-fixed";
    const group_id: u64 = 6916251015964685912;

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root, group_id);
    defer alloc.free(path);

    std.debug.print("opening path={s}\n", .{path});

    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
        .text_merge = .{ .enabled = false },
    });
    defer db.close();

    std.debug.print("opened\n", .{});

    const debt = db.listDerivedReplayDebt(alloc) catch |err| {
        std.debug.print("listDerivedReplayDebt err={}\n", .{err});
        return;
    };
    defer {
        for (debt) |*status| status.deinit(alloc);
        alloc.free(debt);
    }
    std.debug.print("replay_debt_len={d}\n", .{debt.len});

    const pending_dense_rebuild = db.hasPendingDenseArtifactRebuild(alloc) catch |err| {
        std.debug.print("hasPendingDenseArtifactRebuild err={}\n", .{err});
        return;
    };
    std.debug.print("pending_dense_rebuild={any}\n", .{pending_dense_rebuild});

    const stats = db.stats(alloc) catch |err| {
        std.debug.print("stats err={}\n", .{err});
        return;
    };
    defer db_mod.types.freeDBStats(alloc, stats);
    std.debug.print("stats doc_count={d} index_count={d}\n", .{ stats.doc_count, stats.index_count });
}
