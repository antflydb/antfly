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

//! Immutable component loading plan. Services enforce process protection,
//! admission and health; this owner preserves the validated artifact/backend
//! set across immediate and lazy component construction.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const ManagedSession = @import("session_loader.zig").ManagedSession;
const ManagedHfTokenizer = @import("managed_tokenizer.zig").Managed;
const ComponentPlanKey = [32]u8;
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const Services = struct {
    allocator: std.mem.Allocator,
    context: *anyopaque,
    load_session_fn: *const fn (*anyopaque, []const u8, []const backends.BackendType, ?*backends.imported_onnx_session.SharedBackendContext, ?InferenceExecutionControl) anyerror!ManagedSession,
    load_tokenizer_fn: *const fn (*anyopaque, []const u8) anyerror!ManagedHfTokenizer,
};

pub const ComponentLoader = struct {
    const max_component_paths = 32;
    const PathDigest = [std.crypto.hash.sha2.Sha256.digest_length]u8;

    services: Services,
    validated_generation: ?ComponentPlanKey = null,
    allowed_backends: [7]backends.BackendType = undefined,
    allowed_backend_count: usize = 0,
    component_path_digests: [max_component_paths]PathDigest = undefined,
    component_path_count: usize = 0,

    pub fn preferredBackends(self: *const ComponentLoader) []const backends.BackendType {
        return self.allowed_backends[0..self.allowed_backend_count];
    }

    pub fn addComponentPath(self: *ComponentLoader, path: []const u8) !void {
        var digest: PathDigest = undefined;
        std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
        for (self.component_path_digests[0..self.component_path_count]) |existing| {
            if (std.mem.eql(u8, existing[0..], digest[0..])) return;
        }
        if (self.component_path_count == self.component_path_digests.len)
            return error.TooManyModelComponents;
        self.component_path_digests[self.component_path_count] = digest;
        self.component_path_count += 1;
    }

    fn ensureComponentPath(self: *const ComponentLoader, path: []const u8) !void {
        var digest: PathDigest = undefined;
        std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
        for (self.component_path_digests[0..self.component_path_count]) |expected| {
            if (std.mem.eql(u8, expected[0..], digest[0..])) return;
        }
        return error.UnvalidatedModelComponent;
    }

    pub fn restrictToBackend(
        self: *const ComponentLoader,
        backend: backends.BackendType,
    ) !ComponentLoader {
        for (self.preferredBackends()) |allowed| {
            if (allowed == backend) {
                var restricted = self.*;
                restricted.allowed_backends[0] = backend;
                restricted.allowed_backend_count = 1;
                return restricted;
            }
        }
        return error.IncompatibleModel;
    }

    pub fn load(
        self: *const ComponentLoader,
        model_path: []const u8,
    ) !ManagedSession {
        try self.ensureComponentPath(model_path);
        return self.services.load_session_fn(
            self.services.context,
            model_path,
            self.preferredBackends(),
            null,
            null,
        );
    }

    pub fn loadWithControl(
        self: *const ComponentLoader,
        model_path: []const u8,
        control: InferenceExecutionControl,
    ) !ManagedSession {
        try self.ensureComponentPath(model_path);
        return self.services.load_session_fn(
            self.services.context,
            model_path,
            self.preferredBackends(),
            null,
            control,
        );
    }

    pub fn loadWithImportedOnnxContext(
        self: *const ComponentLoader,
        model_path: []const u8,
        shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
    ) !ManagedSession {
        try self.ensureComponentPath(model_path);
        return self.services.load_session_fn(
            self.services.context,
            model_path,
            self.preferredBackends(),
            shared_backend_ctx,
            null,
        );
    }

    /// The context is borrowed through construction; the loaded session retains
    /// its own reference. Backend selection and cancellation are unchanged.
    pub fn loadWithImportedOnnxContextAndControl(
        self: *const ComponentLoader,
        model_path: []const u8,
        shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
        control: ?InferenceExecutionControl,
    ) !ManagedSession {
        try self.ensureComponentPath(model_path);
        return self.services.load_session_fn(self.services.context, model_path, self.preferredBackends(), shared_backend_ctx, control);
    }

    /// Use the supplied resource owner for tokenizer admission and cache policy.
    pub fn loadHfTokenizerFile(
        self: *const ComponentLoader,
        tokenizer_path: []const u8,
    ) !ManagedHfTokenizer {
        return self.services.load_tokenizer_fn(self.services.context, tokenizer_path);
    }

    /// Adapter for SessionPool's lazy factory. The pool takes ownership of
    /// a heap copy of this immutable plan, so lazy loads cannot retain a
    /// pointer to a caller's stack-local ComponentLoader.
    pub fn sessionPoolLoader(self: *const ComponentLoader) !backends.SessionPool.Loader {
        const context = try self.services.allocator.create(PoolLoaderContext);
        context.* = .{
            .allocator = self.services.allocator,
            .loader = self.*,
        };
        return .{
            .context = context,
            .load_fn = loadForSessionPool,
            .deinit_fn = deinitSessionPoolLoader,
        };
    }

    fn loadForSessionPool(
        context: *anyopaque,
        model_path: []const u8,
    ) !backends.SessionPool.OwnedSession {
        const pool_context: *PoolLoaderContext = @ptrCast(@alignCast(context));
        const managed = try pool_context.loader.load(model_path);
        const boxed = pool_context.allocator.create(PoolManagedSession) catch |err| {
            var cleanup = managed;
            cleanup.deinit();
            return err;
        };
        boxed.* = .{
            .allocator = pool_context.allocator,
            .managed = managed,
        };
        return .{
            .session = managed.session,
            .close_context = boxed,
            .close_fn = closeSessionPoolSession,
        };
    }

    fn deinitSessionPoolLoader(context: *anyopaque) void {
        const pool_context: *PoolLoaderContext = @ptrCast(@alignCast(context));
        const allocator = pool_context.allocator;
        allocator.destroy(pool_context);
    }

    fn closeSessionPoolSession(context: ?*anyopaque, _: backends.Session) void {
        const boxed: *PoolManagedSession = @ptrCast(@alignCast(context.?));
        const allocator = boxed.allocator;
        boxed.managed.deinit();
        allocator.destroy(boxed);
    }

    const PoolManagedSession = struct {
        allocator: std.mem.Allocator,
        managed: ManagedSession,
    };

    const PoolLoaderContext = struct {
        allocator: std.mem.Allocator,
        loader: ComponentLoader,
    };
};
