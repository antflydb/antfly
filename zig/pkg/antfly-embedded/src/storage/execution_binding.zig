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

//! Stable context-local metadata for a shared native execution-services scope.
const std = @import("std");
const services = @import("execution_services.zig");
const io_abi = @import("antfly_runtime_abi").io_abi;

pub const Binding = struct {
    allocator: std.mem.Allocator,
    client: services.Client,
    info: services.Info,
    views: [@typeInfo(services.Purpose).@"enum".field_names.len]?io_abi.Receiver = @splat(null),

    pub fn create(allocator: std.mem.Allocator, offer: *const services.Offer) !*Binding {
        const self = try allocator.create(Binding);
        errdefer allocator.destroy(self);
        var client = try services.Client.open(offer);
        errdefer client.close() catch @panic("execution binding rollback");
        self.* = .{ .allocator = allocator, .client = client, .info = try client.info() };
        try self.info.limits.validate();
        // Cache views before publication. Their receiver addresses never move,
        // and cleanup can keep using them after new admission is fenced.
        inline for (.{ .general, .filesystem, .api, .api_network, .api_filesystem, .inference, .control, .raft_inbound, .raft_outbound }) |purpose| {
            self.views[@backingInt(@as(services.Purpose, purpose))] = try self.client.ioView(purpose);
        }
        return self;
    }
    pub fn io(self: *Binding, purpose: services.Purpose) std.Io {
        return self.views[@backingInt(purpose)].?.io();
    }
    pub fn quiesce(self: *Binding) void {
        self.client.quiesce() catch @panic("execution binding quiescence contract");
    }
    pub fn takeAndDestroyMetadata(self: *Binding) services.Client {
        const client = self.client;
        const allocator = self.allocator;
        allocator.destroy(self);
        return client;
    }
    pub fn destroy(self: *Binding) void {
        self.client.close() catch @panic("execution binding still retained");
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};
