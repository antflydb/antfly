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

//! Optional process-local services for embedded storage owners. These use
//! the existing allocator and same-toolchain executor bridges. Provider
//! implementations retain copies, never pointers to this request.
const abi = @import("kernel_owner_abi");
pub const memory = @import("runtime_memory_abi");
pub const executor = @import("antfly_runtime_abi").io_abi;

pub const abi_version: u32 = 1;
pub const Request = extern struct {
    version: u32 = abi_version,
    _reserved: u32 = 0,
    context: abi.ContextRequest = .{},
    /// Explicit process budget. Zero with borrowed I/O uses fixed defaults;
    /// it must not probe host RAM during deterministic execution.
    memory_limit_bytes: u64 = 0,
    /// The allocator's callback context must outlive the storage context and
    /// every borrowed owner. The callback table itself is copied.
    allocator: ?*const memory.Allocator = null,
    /// A borrowed executor supplies storage, clock, scheduling, and
    /// cancellation together. Its runtime must outlive context destruction.
    io: ?*const executor.Borrow = null,
};

pub extern fn antfly_storage_context_create_with_runtime(
    request: *const Request,
    out_context: *?*anyopaque,
) abi.Status;

/// Borrowed publisher; callbacks and context outlive the storage context.
/// JSON crosses the compilation boundary, and its producer releases it.
/// Version 1 carries ContentSecurityConfig; version 2 carries
/// scraping.RemoteContentSnapshotData, including scoped S3, HTTP and GCS
/// credentials. Version 3 requires the prefix of a CachedRemoteContentSource.
pub const RemoteContentSource = extern struct {
    version: u32 = 1,
    _reserved: u32 = 0,
    context: *anyopaque,
    acquire: *const fn (*anyopaque, *abi.OwnedBytes) callconv(.c) abi.Status,
    release: *const fn (*anyopaque, abi.OwnedBytes) callconv(.c) void,
};

/// Version 3 extends the unchanged v1/v2 prefix. The configure call copies
/// this callback as well as the prefix; the table itself need not stay alive.
/// A successful update with null bytes means `previous` is still current.
/// Otherwise bytes contain a complete v2 JSON payload, owned until release.
/// Errors must propagate even when the consumer holds an older cached value.
pub const CachedRemoteContentSource = extern struct {
    source: RemoteContentSource,
    /// Always consult the publisher, outside consumer locks. It must report
    /// acquisition errors, and bind a known revision to the acquired immutable
    /// publication. Mutable publishers report known=0 and use the full path.
    probe: Probe,
    acquire_if_changed: AcquireIfChanged,

    pub const Revision = extern struct {
        value: [32]u8 = @splat(0),
        known: u8 = 0,
    };
    pub const Update = extern struct {
        revision: [32]u8 = @splat(0),
        bytes: abi.OwnedBytes = .{},
    };
    pub const Probe = *const fn (*anyopaque, *Revision) callconv(.c) abi.Status;
    pub const AcquireIfChanged = *const fn (*anyopaque, ?*const [32]u8, *Update) callconv(.c) abi.Status;
};

pub extern fn antfly_storage_context_configure_remote_content_source(
    context: ?*anyopaque,
    source: *const RemoteContentSource,
) abi.Status;
