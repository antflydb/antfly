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

//! Give C ABI shared-library links an undefined reference into the reusable
//! storage-kernel archive. The archive currently contains one Zig object, so
//! resolving this symbol retains all of its public C ABI exports.

extern fn antfly_abi_version() callconv(.c) u32;

fn storageKernelLinkAnchor() callconv(.c) u32 {
    return antfly_abi_version();
}

comptime {
    @export(&storageKernelLinkAnchor, .{
        .name = "antfly_storage_kernel_link_anchor",
        .visibility = .hidden,
    });
}
