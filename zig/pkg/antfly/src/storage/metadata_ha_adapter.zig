// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! This adapter always executes in the archive that owns the HA Primary.
const std = @import("std");
const policy = @import("db/ha_contract.zig");
const port = @import("metadata_ha_port.zig");
const chunks = @import("hot_standby/metadata_effect_chunks.zig");

pub const Adapter = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    gate: ?policy.WriteGate,
    mirror: ?policy.AsyncEffectMirror,

    pub fn asPort(self: *Adapter) port.Port {
        return .{ .ptr = self, .vtable = &.{ .lock = lock, .unlock = unlock, .check = check, .identity = identity, .publish_and_lock = publishAndLock }, .has_mirror = self.mirror != null };
    }
    fn cast(ptr: *anyopaque) *Adapter {
        return @ptrCast(@alignCast(ptr));
    }
    fn lock(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.mirror) |mirror| if (mirror.transition_mutex) |mutex| {
            @import("antfly_platform").sync.lockYieldingIo(mutex, self.io_impl.io());
        };
    }
    fn unlock(ptr: *anyopaque) void {
        if (cast(ptr).mirror) |mirror| if (mirror.transition_mutex) |mutex| mutex.unlock();
    }
    fn check(ptr: *anyopaque) !void {
        if (cast(ptr).gate) |gate| try gate.check();
    }
    fn identity(ptr: *anyopaque) !port.Identity {
        const mirror = cast(ptr).mirror orelse return error.MetadataHAOutboxPending;
        return .{ .next_lsn = mirror.primary.nextLsn(), .timeline_id = mirror.primary.identity.timeline_id, .epoch = mirror.primary.identity.epoch };
    }
    fn publishAndLock(ptr: *anyopaque, raw: []const u8) !void {
        const self = cast(ptr);
        const mirror = self.mirror orelse return error.MetadataHAOutboxPending;
        if (raw.len < 24) return error.InvalidMetadataHAEffect;
        const descriptor = try chunks.Descriptor.fromEffect(raw[24..]);
        try lock(ptr);
        var locked = true;
        errdefer if (locked) unlock(ptr);
        const gate = if (self.gate) |value| value.pinned() else null;
        if (gate) |value| try value.check();
        const same_timeline = std.mem.readInt(u64, raw[8..16], .little) == mirror.primary.identity.timeline_id and std.mem.readInt(u64, raw[16..24], .little) == mirror.primary.identity.epoch;
        var search_from = std.mem.readInt(u64, raw[0..8], .little);
        var lsn: u64 = 0;
        var index: u32 = 0;
        while (index < descriptor.chunk_count) : (index += 1) {
            unlock(ptr);
            locked = false;
            const offset = @as(usize, index) * chunks.max_chunk_payload_bytes;
            const end = @min(raw.len - 24, offset + chunks.max_chunk_payload_bytes);
            const frame = try chunks.encodeFrame(self.alloc, descriptor, index, raw[24..][offset..end]);
            defer self.alloc.free(frame);
            try lock(ptr);
            locked = true;
            if (gate) |value| try value.check();
            const prior = if (same_timeline) try mirror.primary.findMatchingRecordFrom(search_from, .metadata_mutation, frame, 0, 0) else null;
            lsn = prior orelse try mirror.primary.append(.{ .kind = .metadata_mutation, .payload_codec = .binary, .shard_id = 0, .table_id = 0, .payload = frame });
            search_from = lsn +| 1;
        }
        if (mirror.last_lsn) |last| last.store(lsn, .release);
        unlock(ptr);
        locked = false;
        if (mirror.sync_policy.mode != .async) {
            if (mirror.sync_wait_fn) |wait| try wait(mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext, mirror.primary, lsn, mirror.sync_policy);
            const decision = try @import("hot_standby/commit_gate.zig").evaluate(mirror.primary, lsn, mirror.sync_policy);
            if (!decision.shouldAcknowledge()) return error.HASyncCommitWouldBlock;
        }
        try lock(ptr);
        locked = true;
        if (gate) |value| try value.check();
    }
};
