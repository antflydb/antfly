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

//! Give C ABI shared-library links an undefined reference into the reusable
//! storage-kernel archive. The archive currently contains one Zig object, so
//! resolving this symbol retains all of its public C ABI exports.

extern fn antfly_abi_version() callconv(.c) u32;

fn storageKernelLinkAnchor() callconv(.c) u32 {
    return antfly_abi_version();
}

// The reusable archive also owns an executable-only API-kernel runtime entry
// point. That hidden path calls the separately compiled API-kernel unit, but
// no public C ABI operation can enter it. Resolve its private reference with
// a trap in the shared-library consumer so the linker does not retain the
// unrelated API-kernel archive. The executable consumer resolves the real
// symbol and never links this anchor module.
//
// The inference entry point (`antfly_standalone_inference_get_function_
// table`) is NOT trapped here: libantfly embeds the standalone inference
// runtime in-process, the same as the `antfly` executable (2026-09-17
// product decision -- see COMPILATION.md's "C API composition" section), so
// this shared-library link genuinely includes the inference archive (see
// `pkg/antfly/build/runtime.zig`'s storage_kernel unit and the `.inference`
// unit linked into `libantfly_link_mod`), and the real definition exported
// by `runtime_inference_root.zig` satisfies the storage-kernel archive's
// reference.
fn unavailableExecutableRuntimeDependency() callconv(.c) noreturn {
    @trap();
}

comptime {
    @export(&storageKernelLinkAnchor, .{
        .name = "antfly_storage_kernel_link_anchor",
        .visibility = .hidden,
    });
    @export(&unavailableExecutableRuntimeDependency, .{
        .name = "antfly_api_kernel_get_function_table",
        .visibility = .hidden,
    });
}
