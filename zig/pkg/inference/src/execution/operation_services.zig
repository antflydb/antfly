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

//! Shared host-side operation exclusion for canonical Native/CUDA providers.
//! Metadata mode never drains unrelated numerical work. Successful CUDA scopes
//! release after submission/encoding without introducing a synchronization;
//! only uncertain numerical cleanup invokes its provider's checked drain.
const operation = @import("backend_operation.zig");
const owner_module = @import("operation_owner.zig");
const Owner = owner_module.Owner;
const ops = @import("../ops/ops.zig");

pub fn services(comptime Provider: type) *const operation.Services {
    return &Adapter(Provider).service;
}
fn Adapter(comptime Provider: type) type {
    return struct {
        const service = operation.Services{ .accepts = accepts, .prepare_into = prepareInto };
        const vtable = operation.Lease.VTable{ .validate = validate, .validate_active = validateActive, .begin = begin, .finish = finish, .drain = drain, .release = release, .cleanup_epoch = &owner_module.cleanup_epoch_services };
        fn accepts(table: *const ops.ComputeBackend.VTable) bool {
            return Provider.accepts(table);
        }
        fn prepareInto(ctx: *anyopaque, request: operation.Request, output: *operation.Lease) !bool {
            if (request.implementation.context != ctx or !Provider.accepts(request.implementation.table)) return error.BackendOperationChanged;
            const owner = Provider.owner(ctx) orelse return false;
            const prepared = try owner.prepare(ctx, request);
            output.* = .{ .context = prepared.record, .nonce = prepared.nonce, .mode = request.mode, .implementation = request.implementation, .vtable = &vtable };
            return true;
        }
        fn record(raw: *anyopaque) *Owner.Record {
            return @ptrCast(@alignCast(raw));
        }
        fn validateSnapshot(snapshot: *const Owner.Record.Snapshot, request: operation.Request) !void {
            if (!operation.allowsMode(snapshot.mode, request.mode) or snapshot.source.context != snapshot.provider or request.implementation.context != snapshot.provider or !Provider.accepts(snapshot.source.table) or !Provider.accepts(request.implementation.table)) return error.BackendOperationChanged;
        }
        fn validate(raw: *anyopaque, nonce: u64, request: operation.Request) !void {
            const snapshot = try record(raw).snapshot(nonce);
            try validateSnapshot(&snapshot, request);
        }
        fn validateActive(raw: *anyopaque, nonce: u64, request: operation.Request) !void {
            const snapshot = try record(raw).snapshot(nonce);
            try validateSnapshot(&snapshot, request);
            if (!snapshot.entered or !try snapshot.ticket.ownsGate()) return error.BackendOperationInactive;
        }
        fn begin(raw: *anyopaque, nonce: u64, request: operation.Request) !void {
            const value = record(raw);
            if (comptime !@hasDecl(Provider, "beforeBegin")) return value.beginMetadata(nonce, request, validateSnapshot);
            const snapshot = try value.snapshot(nonce);
            try validateSnapshot(&snapshot, request);
            try snapshot.ticket.begin();
            if (@hasDecl(Provider, "beforeBegin")) try Provider.beforeBegin(snapshot.provider, snapshot.mode);
            try value.setEntered(nonce, true);
        }
        fn finish(raw: *anyopaque, nonce: u64) !void {
            try record(raw).finishAndRelease(nonce);
        }
        fn drain(raw: *anyopaque, nonce: u64) !void {
            const value = record(raw);
            const snapshot = try value.snapshot(nonce);
            if (try snapshot.ticket.ownsGate()) {
                if (snapshot.entered and snapshot.mode == .managed) try Provider.drain(snapshot.provider);
            }
            try value.finishMetadata(nonce);
        }
        fn release(raw: *anyopaque, nonce: u64) void {
            record(raw).release(nonce) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
    };
}
