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

//! Independent SDK custody, separate from a pure quota Charge. This cold cell
//! owns only completed SDK handles, process metadata and its exact metadata
//! lease/Domain. Neither registration nor callback borrows a Model/provider,
//! runtime, IO, task, request allocator or cleanup Epoch.
const std = @import("std");
const platform = @import("antfly_platform");
const runtime = @import("metal_runtime.zig");
const memory = @import("../runtime/tier/memory.zig");
const admission = @import("../runtime/tier/admission_domain.zig");
const retirement = @import("../execution/resource_retirement.zig");
const Cleanup = @import("../execution/cleanup_context.zig").Context;

pub const Cell = struct {
    const State = enum(u8) { idle, queued };
    allocator: std.mem.Allocator,
    custody: *runtime.DecoderCacheCustody,
    references: std.atomic.Value(usize) = .init(1),
    state: std.atomic.Value(State) = .init(.idle),
    node: retirement.Node = undefined,
    metadata_lease: ?memory.AdmissionLease = null,
    metadata_domain: ?admission.Pin = null,

    pub fn metadataBytes() !usize {
        return std.math.add(usize, @sizeOf(Cell), runtime.antfly_metal_decoder_cache_custody_metadata_bytes());
    }
    pub fn prepare() !*Cell {
        const a = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
        return prepareWithAllocator(a);
    }
    fn prepareWithAllocator(a: std.mem.Allocator) !*Cell {
        const cell = try a.create(Cell);
        errdefer a.destroy(cell);
        const custody = runtime.antfly_metal_decoder_cache_custody_create() orelse return error.OutOfMemory;
        cell.* = .{ .allocator = a, .custody = custody };
        return cell;
    }
    /// Partition the SAME cold epoch metadata declaration, before all lanes.
    /// Preparation OOM changes neither the parent's credit nor this cell.
    pub fn adoptMetadata(self: *Cell, source: *memory.AdmissionLease, old_domain: *const admission.Pin) !void {
        if (self.metadata_lease != null or self.metadata_domain != null or self.state.load(.acquire) != .idle) return error.CustodyMetadataAlreadyOwned;
        if (source.controller != old_domain.controller()) return error.ForeignAdmissionDomain;
        var partition = try source.preparePartition(self.allocator, .{ .host_scratch_bytes = try metadataBytes() });
        // No fallible action follows: child quota and its independent OLD
        // controller lifetime move together, without reserve/refund callbacks.
        self.metadata_domain = old_domain.retain();
        self.metadata_lease = partition.commit();
    }
    /// Register BEFORE the SDK exchange. Caller still owns NOW's real guard,
    /// so this already-owned node cannot run until all actual gates leave.
    /// The cell stores no boundary/creation authority after registration.
    pub fn queueForCurrentBoundary(self: *Cell, current: Cleanup) !void {
        if (current.outside_execution) return error.MissingCurrentCleanupEpoch;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        // A sealed boundary would invoke registration immediately, before the
        // caller's SDK exchange. Refuse it before state/node/ref mutation.
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        const boundary = try epoch.boundary();
        if (self.state.cmpxchgStrong(.idle, .queued, .acq_rel, .acquire) != null) return error.CustodyCellStillPending;
        errdefer self.state.store(.idle, .release);
        var previous = self.references.load(.monotonic);
        while (true) {
            if (previous == 0 or previous == std.math.maxInt(usize)) return error.CustodyCellReferenceOverflow;
            previous = self.references.cmpxchgWeak(previous, previous + 1, .monotonic, .monotonic) orelse break;
        }
        errdefer self.release();
        self.node = .{ .context = self, .release_fn = retire };
        try boundary.register(boundary.context, boundary.nonce, &self.node);
    }
    fn retire(raw: *anyopaque) callconv(.c) void {
        const self: *Cell = @ptrCast(@alignCast(raw));
        std.debug.assert(self.state.load(.acquire) == .queued);
        if (runtime.antfly_metal_decoder_cache_custody_clear(self.custody) != 0) platform.inference_process_supervisor.restartWorker();
        // Sink detached node.next BEFORE invoking us. SDK ownership is now
        // empty; subsequent reuse may rewrite that detached node. After idle
        // publication this invocation touches only its independent ref count.
        self.state.store(.idle, .release);
        self.release();
    }
    pub fn release(self: *Cell) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        std.debug.assert(self.state.load(.acquire) == .idle);
        const a = self.allocator;
        var lease = self.metadata_lease;
        const domain = self.metadata_domain;
        // Actual SDK and both C/Zig metadata backings die BEFORE credit.
        runtime.antfly_metal_decoder_cache_custody_destroy(self.custody);
        a.destroy(self);
        if (lease) |*owned| owned.release();
        if (domain) |pin| pin.release();
    }
};
