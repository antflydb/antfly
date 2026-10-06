// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

pub const provider_registry = @import("antfly_local_sources").common_provider_registry;
pub const listener_security = @import("listener_security.zig");
pub const config = @import("antfly_local_sources").common_config;
pub const vector_migration = @import("antfly_local_sources").common_vector_migration;
pub const table_storage = @import("antfly_local_sources").common_table_storage;
pub const http = @import("http/mod.zig");
pub const audio_runtime = @import("audio_runtime.zig");
pub const secrets = @import("antfly_local_sources").common_secrets;
pub const secret_contract = @import("antfly_local_sources").common_secret_contract;
pub const secret_record = @import("antfly_local_sources").common_secret_record;
pub const credential_source_identity = @import("antfly_local_sources").common_credential_source_identity;
pub const remote_content_runtime = @import("remote_content_runtime.zig");
pub const health_server = @import("health_server.zig");
pub const runtime_lifecycle = @import("runtime_lifecycle.zig");
pub const prometheus = @import("antfly_local_sources").common_prometheus;
pub const request_admission = @import("antfly_local_sources").common_request_admission;
pub const group_ids = @import("antfly_local_sources").common_group_ids;
pub const data_format = @import("data_format.zig");
pub const fs_paths = @import("antfly_runtime_fs").fs_paths;
pub const byte_copy = @import("antfly_local_sources").common_byte_copy;
pub const cache_budget = @import("antfly_cache_budget");
pub const threaded_io_limits = @import("antfly_runtime_fs").threaded_io_limits;
pub const threaded_connect_io = @import("antfly_local_sources").common_threaded_connect_io;

test {
    _ = provider_registry;
    _ = config;
    _ = table_storage;
    _ = vector_migration;
    _ = http;
    _ = audio_runtime;
    _ = secrets;
    _ = secret_contract;
    _ = secret_record;
    _ = credential_source_identity;
    _ = remote_content_runtime;
    _ = health_server;
    _ = runtime_lifecycle;
    _ = prometheus;
    _ = request_admission;
    _ = group_ids;
    _ = data_format;
    _ = fs_paths;
    _ = byte_copy;
    _ = cache_budget;
    _ = threaded_io_limits;
    _ = threaded_connect_io;
}
