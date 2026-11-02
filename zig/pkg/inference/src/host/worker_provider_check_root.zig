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

//! Semantic owner check: address-taking alone must not skip the provider body.
const host = @import("host.zig");
const bridge = @import("antfly_inference_bridge");
export fn antfly_worker_provider_entry_check(context: *const bridge.ProviderInvokeContext) callconv(.c) void {
    host.linkedInferenceInvokeProvider(context) catch {};
}

export fn antfly_worker_child_entry_check(io: *const @import("std").Io) callconv(.c) void {
    host.worker_module.runChild(@import("std").heap.c_allocator, io.*) catch {};
}

export fn antfly_worker_http_entry_check(context: *const bridge.HttpHandleContext) callconv(.c) void {
    host.linkedInferenceHandleHttp(context) catch {};
}
