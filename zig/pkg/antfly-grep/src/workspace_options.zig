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

const std = @import("std");
const Options = @import("options.zig").Options;
pub fn prepare(a: std.mem.Allocator, base: *const Options, args: []const []const u8, compact: ?bool) !Options {
    var o: Options = .{};
    errdefer o.deinit(a);
    // Provider identity and credentials remain process-owned. Requests may
    // choose a query/model but never mutate the captured discovery contract.
    inline for (.{ "typesafe_api_key", "endpoint", "model", "token", "backend", "decision_context", "models_dir", "library_path", "provider_name", "cache_dir", "cache_ttl_seconds", "model_revision", "jobs", "jobs_auto", "decision_concurrency", "batch_size", "decision_batch_wait_ms", "no_cache", "retries", "retry_base_ms", "timeout_ms", "home", "global_ignore", "git_config", "temporary_dir", "hostname" }) |name| @field(o, name) = @field(base.*, name);
    if (base.jobs_auto) {
        o.jobs = 0;
        o.jobs_auto = false;
    }
    try o.parse(a, args);
    if (compact) |enabled| o.json_compact = enabled;
    try o.finalizeProvider(a);
    if (o.jobs == 0) {
        o.jobs_auto = true;
        o.jobs = base.jobs;
    }
    if (o.paths.items.len != 0 or o.table_config != null or o.pre != null or o.hostname_bin != null or o.search_zip or o.pattern_files.items.len != 0 or o.inventory or o.files or o.cache_clear or o.generate != null or o.help or o.version or o.pcre2_version or o.type_list or o.sort != null or
        o.globs.items.len != 0 or o.iglobs.items.len != 0 or o.types.items.len != 0 or o.types_not.items.len != 0 or o.ignore_files.items.len != 0 or o.type_clear.items.len != 0 or o.type_add.items.len != 0 or o.exclude.items.len != 0 or o.triage or o.workspace_watch) return error.UnsupportedWorkspaceQueryOptions;
    const defaults: Options = .{};
    inline for (.{ "hidden", "no_ignore", "no_ignore_vcs", "no_ignore_global", "no_ignore_parent", "no_ignore_dot", "no_ignore_exclude", "no_ignore_files", "follow", "one_file_system", "max_depth", "max_files", "include_dependencies", "include_sensitive" }) |name| {
        if (@field(o, name) != @field(defaults, name)) return error.UnsupportedWorkspaceQueryOptions;
    }
    // The protocol always carries ordinary JSON result events. Discovery
    // filters belong to capture; queries cannot silently read the live tree.
    o.json = true;
    o.heading = false;
    o.color = .never;
    o.max_files = std.math.maxInt(usize);
    o.temporary_dir = base.temporary_dir;
    try o.paths.append(a, ".");
    return o;
}
