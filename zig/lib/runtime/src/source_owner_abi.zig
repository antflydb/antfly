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

//! Borrowed canonical reader-registry lookup supplied by the static host.
//! The opaque callback is interpreted only after the native dispatcher checks
//! its method/signature/layout contract. The host image lives for the process.
const callback = @import("runtime_callback_abi.zig");
const native = @import("runtime_native_abi.zig");

/// Compatibility evidence only. This process-local descriptor grants no reads.
pub const Descriptor = extern struct {
    version: u32 = 1,
    bytes: u32 = @sizeOf(Descriptor),
    open_owner: native.CallContract,
    reader_layouts: [32]u8,
};
pub const Port = extern struct {
    version: u32 = 1,
    bytes: u32 = @sizeOf(Port),
    descriptor: *const Descriptor,
    context: ?*anyopaque = null,
    callback: *const anyopaque,
    dispatch: callback.CallbackDispatch,
    pub fn valid(self: Port) bool {
        return self.version == 1 and self.bytes == @sizeOf(Port) and
            self.descriptor.version == 1 and self.descriptor.bytes == @sizeOf(Descriptor);
    }
};
