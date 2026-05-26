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
const Allocator = std.mem.Allocator;
const backend_types = @import("../backend_types.zig");
const repository_mod = @import("repository.zig");
const compaction_mod = @import("compaction.zig");
const runtime_mod = @import("runtime.zig");
const storage_io = @import("storage_io.zig");

const open_path_gate_count = 128;
var open_path_gates: [open_path_gate_count]std.atomic.Mutex = [_]std.atomic.Mutex{.unlocked} ** open_path_gate_count;

fn lockHostOpenPathGate(root_dir: []const u8) *std.atomic.Mutex {
    const hash = std.hash.Wyhash.hash(0, root_dir);
    const gate = &open_path_gates[@intCast(hash % open_path_gate_count)];
    while (!gate.tryLock()) std.atomic.spinLoopHint();
    return gate;
}

pub fn open(comptime BackendType: type, allocator: Allocator, root_dir: []const u8, options: backend_types.OpenOptions, backend_options: anytype) !BackendType {
    var backend = BackendType.init(allocator, backend_options);
    backend.root_dir = try allocator.dupe(u8, root_dir);

    // Host storage can race same-path recovery with create-table/read/write opens.
    const host_open_gate = if (backend_options.storage == null)
        lockHostOpenPathGate(backend.root_dir.?)
    else
        null;
    defer if (host_open_gate) |gate| gate.unlock();

    if (backend_options.storage) |storage| {
        backend.storage = storage;
    } else {
        const owned = try std.heap.page_allocator.create(storage_io.NativeStorage);
        errdefer std.heap.page_allocator.destroy(owned);
        owned.* = try storage_io.NativeStorage.init(std.heap.page_allocator, backend_options.io_runtime);
        backend.storage_owner = owned;
        backend.storage = owned.storage();
    }
    errdefer cleanup(BackendType, &backend, false);

    const loaded_manifest = try repository_mod.loadManifestIfPresentWithStorage(
        backend.storage.?,
        allocator,
        backend.root_dir.?,
        &backend.manifest_backing,
        &backend.next_run_id,
        &backend.runs,
        &backend.obsolete_paths,
    );
    if (!loaded_manifest and options.create_if_missing) {
        try repository_mod.ensureOpenDirsWithStorage(backend.storage.?, root_dir);
    }
    {
        const locked = runtime_mod.lockBackend(BackendType, &backend);
        defer runtime_mod.unlockBackend(BackendType, &backend, locked);

        if (@hasDecl(BackendType, "replayWalIntoMutable")) {
            try backend.replayWalIntoMutable();
        }
        compaction_mod.sortRuns(backend.runs.items);
    }
    if (@hasDecl(BackendType, "refreshMaintenanceDebtHint")) {
        backend.refreshMaintenanceDebtHint();
    }
    return backend;
}

pub fn close(comptime BackendType: type, backend: *BackendType) void {
    cleanup(BackendType, backend, true);
}

fn cleanup(comptime BackendType: type, backend: *BackendType, finalize_deferred: bool) void {
    if (finalize_deferred and backend.root_dir != null and !backend.options.backend.read_only) {
        if (@hasDecl(BackendType, "finalizeDeferredStorageWork")) {
            backend.finalizeDeferredStorageWork() catch |err| {
                if (err == error.FileNotFound) {
                    std.log.warn("lsm backend close skipped deferred storage finalization root={?s} err={}", .{ backend.root_dir, err });
                    return;
                }
                std.log.err("lsm backend close skipped deferred storage finalization root={?s} err={}", .{ backend.root_dir, err });
            };
        } else if (backend.mutable.entries.items.len > 0) {
            compaction_mod.flushMutable(BackendType, backend) catch |err| {
                std.log.err("lsm backend close skipped mutable flush root={?s} err={}", .{ backend.root_dir, err });
            };
        }
    }
    if (@hasField(BackendType, "mutable_read_snapshot")) {
        if (backend.mutable_read_snapshot) |state| {
            state.deinit(backend.allocator);
            backend.allocator.destroy(state);
        }
    }
    backend.mutable.deinit(backend.allocator);
    if (@hasField(BackendType, "immutable_memtables")) {
        for (backend.immutable_memtables.items) |state| {
            state.deinit(backend.allocator);
            backend.allocator.destroy(state);
        }
        backend.immutable_memtables.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "immutable_wal_ranges")) {
        backend.immutable_wal_ranges.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "retired_immutable_memtables")) {
        for (backend.retired_immutable_memtables.items) |state| {
            state.deinit(backend.allocator);
            backend.allocator.destroy(state);
        }
        backend.retired_immutable_memtables.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "retired_mutable_snapshots")) {
        for (backend.retired_mutable_snapshots.items) |state| {
            state.deinit(backend.allocator);
            backend.allocator.destroy(state);
        }
        backend.retired_mutable_snapshots.deinit(backend.allocator);
    }
    for (backend.runs.items) |*run| run.deinit(backend.allocator);
    backend.runs.deinit(backend.allocator);
    if (@hasField(BackendType, "obsolete_paths")) {
        for (backend.obsolete_paths.items) |*obsolete| {
            obsolete.deinit(backend.allocator);
        }
        backend.obsolete_paths.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "obsolete_runs")) {
        for (backend.obsolete_runs.items) |*runs| {
            for (runs.items) |*run| run.deinit(backend.allocator);
            runs.deinit(backend.allocator);
        }
        backend.obsolete_runs.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "run_state_cache")) {
        for (backend.run_state_cache.items) |*cached| cached.deinit(backend.allocator);
        backend.run_state_cache.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "run_index_cache")) {
        for (backend.run_index_cache.items) |*cached| cached.deinit(backend.allocator);
        backend.run_index_cache.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "run_block_cache")) {
        for (backend.run_block_cache.items) |*cached| cached.deinit(backend.allocator);
        backend.run_block_cache.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "run_table_cache")) {
        for (backend.run_table_cache.items) |*cached| cached.deinit(backend.allocator);
        backend.run_table_cache.deinit(backend.allocator);
    }
    if (@hasField(BackendType, "manifest_backing")) {
        if (backend.manifest_backing) |raw| backend.allocator.free(raw);
    }
    if (@hasField(BackendType, "storage_owner")) {
        if (backend.storage_owner) |owned| {
            owned.deinit();
            std.heap.page_allocator.destroy(owned);
        }
    }
    if (backend.root_dir) |root_dir| backend.allocator.free(root_dir);
    backend.* = undefined;
}
