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

//! Borrowed source-stamped commit participation across storage type erasure.
//! Empty/control-only writes and inactive source authorities do not invoke the
//! final callback; this is not a general post-commit notification mechanism.
//! The participant
//! cannot commit or abort its host. Its final callback may stage only private
//! metadata, after the host has stamped the observed mutations but before the
//! physical commit. Callback failure aborts the entire host transaction.

pub const View = struct {
    ptr: *anyopaque,
    get_fn: *const fn (*anyopaque, []const u8) anyerror![]const u8,
    put_fn: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,

    pub fn get(self: View, key: []const u8) ![]const u8 {
        return self.get_fn(self.ptr, key);
    }

    pub fn put(self: View, key: []const u8, value: []const u8) !void {
        // Public rows and derived artifacts must not change after their
        // revision stamps were finalized. Only the private metadata namespace
        // is writable through this restricted commit view.
        if (key.len < 2 or key[0] != 0 or key[1] != 0) return error.InvalidCommitMetadata;
        return self.put_fn(self.ptr, key, value);
    }

    /// The view and every returned slice are borrowed only through callback
    /// return. It carries no allocation, ownership, or transaction lifecycle.
    pub fn from(txn: anytype) View {
        const T = @TypeOf(txn);
        const Bridge = struct {
            fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.get(key);
            }
            fn put(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.put(key, value);
            }
        };
        return .{ .ptr = @ptrCast(txn), .get_fn = Bridge.get, .put_fn = Bridge.put };
    }
};

/// Borrowed native preparation, never part of a KV or durable/wire record.
/// The writer still validates the requested bytes and exact current manifest.
pub const DocumentPreparations = struct {
    ptr: *const anyopaque,
    find: *const fn (*const anyopaque, []const u8) ?*const @import("document_payload.zig").PreparedWrite,
};

/// Exact, already validated native inputs. No public request can manufacture
/// this capability; extraction that changes the source must validate again.
pub const DocumentValidations = struct {
    ptr: *const anyopaque,
    find: *const fn (*const anyopaque, []const u8, []const u8) ?@import("document_validation.zig").Stamp,
};

pub const Participant = struct {
    ptr: *anyopaque,
    /// Called on attachment to a fresh writer attempt, before any mutation.
    reset: *const fn (*anyopaque) void,
    observe: *const fn (*anyopaque, []const u8, ?[]const u8) void,
    /// The host's source-position encoding is opaque to the backend erasure
    /// layer. A participant must validate the encoding it expects.
    stage: *const fn (*anyopaque, View, []const u8) anyerror!void,
    document_preparations: ?DocumentPreparations = null,
    document_validations: ?DocumentValidations = null,

    pub fn withDocumentValidations(prior: ?Participant, validated: DocumentValidations) Participant {
        const Empty = struct {
            fn reset(_: *anyopaque) void {}
            fn observe(_: *anyopaque, _: []const u8, _: ?[]const u8) void {}
            fn stage(_: *anyopaque, _: View, _: []const u8) anyerror!void {}
        };
        var result = prior orelse Participant{ .ptr = @constCast(validated.ptr), .reset = Empty.reset, .observe = Empty.observe, .stage = Empty.stage };
        result.document_validations = validated;
        return result;
    }

    /// Preserve the original observer/stamping callbacks. A preparation-only
    /// participant has no metadata effects and grants no publication authority.
    pub fn withDocumentPreparations(prior: ?Participant, prepared: DocumentPreparations) Participant {
        const Empty = struct {
            fn reset(_: *anyopaque) void {}
            fn observe(_: *anyopaque, _: []const u8, _: ?[]const u8) void {}
            fn stage(_: *anyopaque, _: View, _: []const u8) anyerror!void {}
        };
        var result = prior orelse Participant{ .ptr = @constCast(prepared.ptr), .reset = Empty.reset, .observe = Empty.observe, .stage = Empty.stage };
        result.document_preparations = prepared;
        return result;
    }
};
