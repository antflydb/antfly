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

// The reusable archive also owns executable runtime entry points. Those
// hidden paths call the separately compiled API and inference units, but no
// public C ABI operation can enter them. Resolve their private references with
// a trap in the shared-library consumer so the linker does not retain the
// unrelated API/inference archives. The executable consumer resolves the real
// symbols and never links this anchor module.
fn unavailableExecutableRuntimeDependency() callconv(.c) noreturn {
    @trap();
}

comptime {
    @export(&storageKernelLinkAnchor, .{
        .name = "antfly_storage_kernel_link_anchor",
        .visibility = .hidden,
    });
    for ([_][]const u8{
        "antfly_api_kernel_attach_replicated_restore_store",
        "antfly_api_kernel_attach_runtime_restore_store",
        "antfly_api_kernel_create",
        "antfly_api_kernel_destroy",
        "antfly_api_kernel_executor",
        "antfly_api_kernel_handle",
        "antfly_api_kernel_handle_internal",
        "antfly_api_kernel_handler_create",
        "antfly_api_kernel_handler_destroy",
        "antfly_api_kernel_handler_init",
        "antfly_api_kernel_handler_register_routes",
        "antfly_api_kernel_handler_stats",
        "antfly_api_kernel_poll_restore_jobs",
        "antfly_api_kernel_prepare_restore_leadership",
        "antfly_api_kernel_request_stats",
        "antfly_api_kernel_resume_restore_jobs",
        "antfly_api_kernel_schedule_session_maintenance",
        "antfly_api_kernel_set_ha_executor",
        "antfly_api_kernel_set_provider",
        "antfly_api_kernel_storage_maintenance_active",
        "antfly_api_kernel_streaming_executor",
        "antfly_standalone_inference_configure",
        "antfly_standalone_inference_create",
        "antfly_standalone_inference_destroy",
        "antfly_standalone_inference_provider",
        "antfly_standalone_inference_register_routes",
    }) |name| {
        @export(&unavailableExecutableRuntimeDependency, .{
            .name = name,
            .visibility = .hidden,
        });
    }
}
