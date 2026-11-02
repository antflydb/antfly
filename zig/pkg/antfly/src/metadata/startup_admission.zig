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

//! One frozen metadata policy and scheduler owner, shared by all consumers.
const std = @import("std");
const config = @import("antfly_local_sources").common_config;
const kernel = @import("../api/kernel_bridge.zig");
const api = @import("../api/http_server.zig");
const runnable = @import("antfly_runtime_abi").runnable_abi;

pub fn resolve(requested: config.Config.AdmissionConfig, memory_bytes: u64) !config.Config.AdmissionConfig {
    return requested.resolveForStartup(memory_bytes);
}

pub const Owner = struct {
    effective: config.Config.AdmissionConfig,
    node: ?kernel.NodeRunnableOwner,
    pub fn init(allocator: std.mem.Allocator, requested: config.Config.AdmissionConfig, memory_bytes: u64) !Owner {
        const effective = try resolve(requested, memory_bytes);
        return .{ .effective = effective, .node = if (effective.runnable) |policy| try kernel.NodeRunnableOwner.init(allocator, policy) else null };
    }
    pub fn port(self: *const Owner) !?runnable.Owner {
        return if (self.node) |*node| try node.executionPort() else null;
    }
    pub fn configureApi(self: *const Owner, base: api.ApiHttpServerConfig) api.ApiHttpServerConfig {
        var result = base;
        result.query_max_concurrent_requests = self.effective.query.max_concurrent_requests;
        result.query_waiting = self.effective.query.waiting;
        result.write_max_concurrent_requests = self.effective.write.max_concurrent_requests;
        result.inference_max_concurrent_requests = self.effective.inference.max_concurrent_requests;
        result.request_memory_bytes = @intCast(self.effective.max_request_memory_bytes);
        result.retained_memory_bytes = @intCast(self.effective.max_retained_memory_bytes);
        result.runnable_policy = null;
        result.runnable_owner_handle = if (self.node) |node| node.handle else null;
        return result;
    }
    pub fn close(self: *const Owner) !void {
        if (self.node) |*node| try node.close();
    }
    pub fn deinit(self: *Owner) !void {
        try self.close();
        if (self.node) |*node| try node.deinit();
        self.* = undefined;
    }
};
