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
pub const executor = @import("../runtime_io_abi.zig");

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
