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

//! Host SELinux checks for trusted SQL ingress. Consensus apply consumes
//! signed approvals and does not consult the local kernel.
const std = @import("std");
const builtin = @import("builtin");
const labels = @import("security_labels.zig");

const Context = [*:0]const u8;
const OwnedContext = [*:0]u8;
const Policy = labels.KernelRelabel.Policy;
const Decision = extern struct {
    allowed: c_uint,
    decided: c_uint,
    auditallow: c_uint,
    auditdeny: c_uint,
    seqno: c_uint,
    flags: c_uint,
};
const permissive_domain = 1;
const max_policy_bytes = 64 * 1024 * 1024;
var status_owned = std.atomic.Value(bool).init(false);

const Api = struct {
    is_selinux_enabled: *const fn () callconv(.c) c_int,
    selinux_status_open: *const fn (c_int) callconv(.c) c_int,
    selinux_status_close: *const fn () callconv(.c) void,
    selinux_status_getenforce: *const fn () callconv(.c) c_int,
    selinux_status_policyload: *const fn () callconv(.c) c_int,
    getpeercon_raw: *const fn (c_int, *?OwnedContext) callconv(.c) c_int,
    freecon: *const fn (OwnedContext) callconv(.c) void,
    security_get_initial_context_raw: *const fn (Context, *?OwnedContext) callconv(.c) c_int,
    security_check_context_raw: *const fn (Context) callconv(.c) c_int,
    string_to_security_class: *const fn (Context) callconv(.c) c_ushort,
    string_to_av_perm: *const fn (c_ushort, Context) callconv(.c) c_uint,
    security_compute_av_flags_raw: *const fn (Context, Context, c_ushort, c_uint, *Decision) callconv(.c) c_int,
};

/// Owns libselinux's process-wide status mapping. Keep one live instance
/// per process and drain its callers before deinitializing it.
pub const LinuxPolicy = struct {
    lib: std.DynLib,
    api: Api,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    policy_cut: Policy.Snapshot,

    /// Hash the policy image exposed by the running kernel and bind it
    /// to the observed generation. There is no per-statement policy read.
    pub fn init(io: std.Io, allocator: std.mem.Allocator) !LinuxPolicy {
        if (comptime builtin.os.tag != .linux or !builtin.link_libc) return error.SelinuxUnavailable;
        if (status_owned.swap(true, .acq_rel)) return error.SelinuxStatusAlreadyOwned;
        errdefer status_owned.store(false, .release);
        var lib = std.DynLib.open("libselinux.so.1") catch return error.SelinuxUnavailable;
        errdefer lib.close();
        var api: Api = undefined;
        inline for (@typeInfo(Api).@"struct".field_names) |field| {
            @field(api, field) = lib.lookup(@TypeOf(@field(api, field)), field) orelse return error.SelinuxUnavailable;
        }
        if (api.is_selinux_enabled() != 1 or api.selinux_status_open(0) != 0)
            return error.SelinuxUnavailable;
        errdefer api.selinux_status_close();
        const sequence = try enforcingGeneration(api);
        const image = try std.Io.Dir.cwd().readFileAlloc(io, "/sys/fs/selinux/policy", allocator, .limited(max_policy_bytes));
        defer allocator.free(image);
        if (image.len == 0 or try enforcingGeneration(api) != sequence) return error.SelinuxAdmissionChanged;
        var digest: labels.KernelRelabel.Digest = undefined;
        std.crypto.hash.Blake3.hash(image, &digest, .{});
        return .{
            .lib = lib,
            .api = api,
            .io = io,
            .policy_cut = .{ .digest = digest, .sequence = sequence },
        };
    }

    pub fn deinit(self: *LinuxPolicy) void {
        self.api.selinux_status_close();
        self.lib.close();
        self.* = undefined;
        status_owned.store(false, .release);
    }

    /// Owned by the listener for the lifetime of the accepted connection.
    pub const Connection = struct {
        policy: *LinuxPolicy,
        socket: c_int,
        key: *const std.crypto.sign.Ed25519.KeyPair,

        pub fn ingress(self: *Connection) labels.KernelRelabel.Ingress {
            return .{ .ptr = self, .approve = approve };
        }

        fn approve(ptr: *anyopaque, allocator: std.mem.Allocator, authority: labels.KernelRelabel.Authority, cut: labels.KernelRelabel.Authority.CatalogCut) !labels.KernelRelabel.Approval {
            const self: *Connection = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, &authority.verifier, &self.key.public_key.toBytes())) return error.InvalidSelinuxApproval;
            const actual = try self.policy.currentPolicy();
            if (!std.meta.eql(actual, authority.policy)) return error.SelinuxAdmissionChanged;
            return self.policy.approveRelabel(allocator, self.socket, .{
                .object = cut.object,
                .catalog_revision = cut.revision,
                .command_nonce = cut.nonce,
                .actor = cut.actor,
                .subject = @splat(0),
                .policy = actual.digest,
                .policy_sequence = actual.sequence,
                .authority_epoch = authority.epoch,
                .previous = labels.KernelRelabel.labelDigest(cut.previous),
                .proposed = labels.KernelRelabel.labelDigest(cut.proposed),
            }, cut.previous, cut.proposed, self.key.*);
        }
    };

    pub fn currentPolicy(self: *LinuxPolicy) !Policy.Snapshot {
        return snapshot(self);
    }

    pub fn approveRelabel(
        self: *LinuxPolicy,
        allocator: std.mem.Allocator,
        socket: c_int,
        cut: labels.KernelRelabel,
        previous: ?[]const u8,
        proposed: ?[]const u8,
        key: std.crypto.sign.Ed25519.KeyPair,
    ) !labels.KernelRelabel.Approval {
        const peer = try self.peerContext(allocator, socket);
        defer allocator.free(peer);
        const subject = labels.KernelRelabel.labelDigest(peer).?;
        var authenticated = cut;
        authenticated.subject = subject;
        const policy: Policy = .{ .ptr = self, .snapshot = snapshot, .authorize = authorize };
        return policy.approve(authenticated, peer, previous, proposed, key);
    }

    /// Read the kernel-authenticated context from a directly accepted
    /// socket. The caller owns the returned bytes.
    pub fn peerContext(self: *LinuxPolicy, allocator: std.mem.Allocator, socket: c_int) ![:0]u8 {
        var context: ?OwnedContext = null;
        if (self.api.getpeercon_raw(socket, &context) != 0) return error.SelinuxPeerUnavailable;
        const raw = context orelse return error.SelinuxPeerUnavailable;
        defer self.api.freecon(raw);
        const bytes = std.mem.span(raw);
        if (bytes.len == 0 or bytes.len > labels.max_label_bytes) return error.SelinuxPeerUnavailable;
        return allocator.dupeSentinel(u8, bytes, 0);
    }

    fn enforcingGeneration(api: Api) !u32 {
        if (api.selinux_status_getenforce() != 1) return error.SelinuxUnavailable;
        const generation = api.selinux_status_policyload();
        if (generation < 0) return error.SelinuxUnavailable;
        return @intCast(generation);
    }

    fn snapshot(ptr: *anyopaque) !Policy.Snapshot {
        const self: *LinuxPolicy = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (try enforcingGeneration(self.api) != self.policy_cut.sequence) return error.SelinuxAdmissionChanged;
        return self.policy_cut;
    }

    fn contextZ(allocator: std.mem.Allocator, context: []const u8) ![:0]const u8 {
        if (context.len == 0 or context.len > labels.max_label_bytes or std.mem.indexOfScalar(u8, context, 0) != null)
            return error.InvalidSelinuxContext;
        return allocator.dupeSentinel(u8, context, 0);
    }

    fn authorize(ptr: *anyopaque, object: labels.Object, peer: []const u8, previous: ?[]const u8, proposed: ?[]const u8) !void {
        const self: *LinuxPolicy = @ptrCast(@alignCast(ptr));
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        const subject = try contextZ(allocator, peer);
        const old_z = if (previous) |value| try contextZ(allocator, value) else null;
        const new_z = if (proposed) |value| try contextZ(allocator, value) else null;

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.api.security_check_context_raw(subject) != 0) return error.SelinuxPeerUnavailable;
        if (new_z) |context| if (self.api.security_check_context_raw(context) != 0) return error.InvalidSelinuxContext;
        const valid_old = if (old_z) |context| self.api.security_check_context_raw(context) == 0 else false;
        var unlabeled: ?OwnedContext = null;
        defer if (unlabeled) |context| self.api.freecon(context);
        if (!valid_old or new_z == null) {
            if (self.api.security_get_initial_context_raw("unlabeled", &unlabeled) != 0 or unlabeled == null)
                return error.SelinuxUnavailable;
        }
        const old: Context = if (valid_old) old_z.? else unlabeled.?;
        const new: Context = if (new_z) |value| value else unlabeled.?;
        const class = self.api.string_to_security_class(switch (object.class) {
            .table => "db_table",
            .namespace => "db_schema",
            .procedure => "db_procedure",
        });
        if (class == 0) return error.SelinuxUnavailable;
        const setattr = self.api.string_to_av_perm(class, "setattr");
        const relabelfrom = self.api.string_to_av_perm(class, "relabelfrom");
        const relabelto = self.api.string_to_av_perm(class, "relabelto");
        if (setattr == 0 or relabelfrom == 0 or relabelto == 0) return error.SelinuxUnavailable;
        const from_generation = try self.check(subject, old, class, setattr | relabelfrom);
        const to_generation = try self.check(subject, new, class, relabelto);
        // A newly mapped status page reports zero until its first policy
        // event. Even in that case both decisions must name the same policy.
        if (from_generation != to_generation or
            (self.policy_cut.sequence != 0 and to_generation != self.policy_cut.sequence))
            return error.SelinuxAdmissionChanged;
    }

    fn check(self: *LinuxPolicy, subject: Context, target: Context, class: c_ushort, requested: c_uint) !u32 {
        var decision: Decision = std.mem.zeroes(Decision);
        if (self.api.security_compute_av_flags_raw(subject, target, class, requested, &decision) != 0)
            return error.SelinuxUnavailable;
        if (decision.flags & permissive_domain != 0 or decision.allowed & requested != requested)
            return error.SelinuxDenied;
        return decision.seqno;
    }
};

test "unsupported host cannot load an SELinux policy" {
    if (comptime builtin.os.tag == .linux and builtin.link_libc) return error.SkipZigTest;
    const result = LinuxPolicy.init(std.testing.io, std.testing.allocator);
    try std.testing.expectError(error.SelinuxUnavailable, result);
    std.testing.refAllDecls(LinuxPolicy);
    std.testing.refAllDecls(LinuxPolicy.Connection);
}

test "live SELinux authorizes a Unix peer and rejects invalid or denied table labels" {
    if (comptime builtin.os.tag != .linux or !builtin.link_libc) return error.SkipZigTest;
    // Both contexts must be valid under the installed test policy. The
    // current test domain needs relabelto on only the allowed context.
    const allowed = std.mem.span(std.c.getenv("ANTFLY_TEST_SELINUX_ALLOW_CONTEXT") orelse return error.SkipZigTest);
    const denied = std.mem.span(std.c.getenv("ANTFLY_TEST_SELINUX_DENY_CONTEXT") orelse return error.MissingSelinuxTestContext);
    const a = std.testing.allocator;
    const io = std.testing.io;
    var policy = try LinuxPolicy.init(io, a);
    defer policy.deinit();
    const generation = try policy.currentPolicy();
    try std.testing.expectError(error.SelinuxStatusAlreadyOwned, LinuxPolicy.init(io, a));

    var random: [8]u8 = undefined;
    try io.randomSecure(&random);
    var name: [64]u8 = undefined;
    const address = try std.Io.net.UnixAddress.init(try std.fmt.bufPrint(&name, "\x00antfly-selinux-{x}", .{std.mem.readInt(u64, &random, .little)}));
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    const client = try address.connect(io);
    defer client.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);

    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(19));
    var connection: LinuxPolicy.Connection = .{ .policy = &policy, .socket = accepted.socket.handle, .key = &key };
    const ingress = connection.ingress();
    const authority: labels.KernelRelabel.Authority = .{ .verifier = key.public_key.toBytes(), .policy = generation, .epoch = 3 };
    var cut: labels.KernelRelabel.Authority.CatalogCut = .{
        .object = .{ .incarnation = @splat(4), .database_id = 1, .class = .table, .id = 2 },
        .revision = 3,
        .nonce = @splat(5),
        .actor = @splat(6),
        .previous = null,
        .proposed = "internal",
    };
    try std.testing.expectError(error.InvalidSelinuxContext, ingress.approve(ingress.ptr, a, authority, cut));
    cut.proposed = denied;
    try std.testing.expectError(error.SelinuxDenied, ingress.approve(ingress.ptr, a, authority, cut));
    cut.proposed = allowed;
    const approval = try ingress.approve(ingress.ptr, a, authority, cut);
    try authority.verify(cut, approval);
    const peer = try policy.peerContext(a, accepted.socket.handle);
    defer a.free(peer);
    try std.testing.expectEqual(labels.KernelRelabel.labelDigest(peer).?, approval.cut.subject);
}
