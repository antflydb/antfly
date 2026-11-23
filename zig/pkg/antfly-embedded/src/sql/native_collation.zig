// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native ICU provider. No SQL name aliases or ASCII/casefold approximations.
//! The owner loads a provider at service admission and retains it until every
//! prepared collator has closed. Prepared collators have one execution owner;
//! share immutable definition/version data, not mutable ICU handles.
const std = @import("std");
const builtin = @import("builtin");
pub const supported = (builtin.os.tag == .macos or builtin.os.tag == .linux) and builtin.link_libc;
const Library = if (supported) std.DynLib else struct {};

const Handle = opaque {};
const Open = *const fn ([*:0]const u8, *i32) callconv(.c) ?*Handle;
const Close = *const fn (*Handle) callconv(.c) void;
const Compare = *const fn (*const Handle, [*]const u8, i32, [*]const u8, i32, *i32) callconv(.c) i32;
const GetVersion = *const fn (*const Handle, *[4]u8) callconv(.c) void;
const GetSortKey = *const fn (*const Handle, [*]const u16, i32, [*]u8, i32) callconv(.c) i32;

const provider_abi = @import("antfly_runtime_abi").collation_abi;
pub const Version = provider_abi.Version;

pub const Provider = struct {
    receiver: ?provider_abi.Receiver = null,
    library: Library,
    open_fn: Open,
    close_fn: Close,
    compare_fn: Compare,
    version_fn: GetVersion,
    uca_version_fn: GetVersion,
    key_fn: GetSortKey,

    /// An explicit absolute deployment library path is supported. Otherwise use
    /// the platform's native loader search; deployment must pin the ICU package.
    /// Missing providers are an admission error, never a binary fallback.
    pub fn load(path: ?[]const u8) !Provider {
        if (comptime !supported) return error.SqlIcuProviderUnavailable;
        if (path) |name| return loadLibrary(name);
        switch (builtin.os.tag) {
            .macos => return loadLibrary("/usr/lib/libicucore.dylib"),
            .linux => {
                if (loadLibrary("libicui18n.so")) |provider| return provider else |err| {
                    if (err != error.SqlIcuProviderUnavailable) return err;
                }
                // Runtime-only packages need not install the unversioned
                // development symlink. The advertised collation/UCA versions,
                // rather than the library filename, govern execution admission.
                var major: u16 = 255;
                while (major != 0) : (major -= 1) {
                    var buffer: [64]u8 = undefined;
                    const name = std.fmt.bufPrint(&buffer, "libicui18n.so.{d}", .{major}) catch unreachable;
                    if (loadLibrary(name)) |provider| return provider else |err| {
                        if (err != error.SqlIcuProviderUnavailable) return err;
                    }
                }
                return error.SqlIcuProviderUnavailable;
            },
            else => return error.SqlIcuProviderUnavailable,
        }
    }

    /// Acquires the same trusted startup provider selected by the physical
    /// runtime. The immutable factory owns its library independently of views.
    pub fn fromReceiver(receiver: provider_abi.Receiver) Provider {
        return .{ .receiver = receiver.clone(), .library = undefined, .open_fn = undefined, .close_fn = undefined, .compare_fn = undefined, .version_fn = undefined, .uca_version_fn = undefined, .key_fn = undefined };
    }

    fn loadLibrary(name: []const u8) !Provider {
        if (comptime !supported) return error.SqlIcuProviderUnavailable;
        var library = std.DynLib.open(name) catch return error.SqlIcuProviderUnavailable;
        errdefer library.close();
        if (bind(&library, null)) |api| return api;
        // ICU's stable C ABI is normally exported with a major-version suffix.
        // Resolve a complete coherent family; never mix symbols across versions.
        var major: u16 = 255;
        while (major != 0) : (major -= 1) {
            if (bind(&library, @intCast(major))) |api| return api;
        }
        return error.SqlIcuProviderAbiMismatch;
    }

    fn symbol(library: *Library, comptime T: type, comptime name: []const u8, major: ?u8) ?T {
        var buffer: [96]u8 = undefined;
        const spelling = if (major) |version|
            std.fmt.bufPrintSentinel(&buffer, "{s}_{d}", .{ name, version }, 0) catch unreachable
        else
            std.fmt.bufPrintSentinel(&buffer, "{s}", .{name}, 0) catch unreachable;
        return library.lookup(T, spelling);
    }

    fn bind(library: *Library, major: ?u8) ?Provider {
        return .{
            .library = library.*,
            .open_fn = symbol(library, Open, "ucol_open", major) orelse return null,
            .close_fn = symbol(library, Close, "ucol_close", major) orelse return null,
            .compare_fn = symbol(library, Compare, "ucol_strcollUTF8", major) orelse return null,
            .version_fn = symbol(library, GetVersion, "ucol_getVersion", major) orelse return null,
            .uca_version_fn = symbol(library, GetVersion, "ucol_getUCAVersion", major) orelse return null,
            .key_fn = symbol(library, GetSortKey, "ucol_getSortKey", major) orelse return null,
        };
    }

    pub fn deinit(self: *Provider) void {
        if (self.receiver) |*receiver| receiver.deinit() else if (comptime supported) self.library.close();
        self.* = undefined;
    }

    pub fn prepare(self: *const Provider, a: std.mem.Allocator, locale: []const u8, deterministic: bool, expected: ?Version) !Prepared {
        if (self.receiver) |receiver| {
            const remote = try receiver.prepare(locale, deterministic, expected);
            return .{ .provider = self, .handle = undefined, .deterministic = deterministic, .version = remote.version, .remote = remote, .open_warning = 0 };
        }
        if (locale.len == 0 or locale.len > 1024 or std.mem.indexOfScalar(u8, locale, 0) != null or !std.unicode.utf8ValidateSlice(locale))
            return error.SqlInvalidCollationLocale;
        const terminated = try a.dupeSentinel(u8, locale, 0);
        defer a.free(terminated);
        var status: i32 = 0;
        const handle = self.open_fn(terminated, &status) orelse return error.SqlInvalidCollationLocale;
        errdefer self.close_fn(handle);
        // ICU warnings are negative; positive values are failures. In particular
        // fallback/default warnings do not mean the provider is unavailable.
        if (status > 0) return error.SqlInvalidCollationLocale;
        var version: Version = undefined;
        self.version_fn(handle, &version.collation);
        self.uca_version_fn(handle, &version.uca);
        if (expected) |required| if (!version.eql(required)) return error.SqlCollationVersionMismatch;
        return .{ .provider = self, .handle = handle, .deterministic = deterministic, .version = version, .open_warning = status };
    }
};

pub const Prepared = struct {
    remote: ?provider_abi.Collator = null,
    provider: *const Provider,
    handle: *Handle,
    deterministic: bool,
    version: Version,
    /// Caller can surface PostgreSQL-style provider fallback notices at CREATE.
    open_warning: i32,

    pub fn deinit(self: *Prepared) void {
        if (self.remote) |*remote| remote.deinit() else self.provider.close_fn(self.handle);
        self.* = undefined;
    }

    fn validateText(text: []const u8) !i32 {
        if (text.len > std.math.maxInt(i32)) return error.SqlCollationInputTooLarge;
        if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.SqlInvalidTextEncoding;
        return @intCast(text.len);
    }

    /// Allocation-free row comparison. PostgreSQL deterministic collations add
    /// bytewise tie-breaking even when ICU strength ignores case/normalization.
    pub fn compare(self: Prepared, lhs: []const u8, rhs: []const u8) !std.math.Order {
        if (self.remote) |remote| return remote.compare(lhs, rhs);
        const left_length = try validateText(lhs);
        const right_length = try validateText(rhs);
        var status: i32 = 0;
        const result = self.provider.compare_fn(self.handle, lhs.ptr, left_length, rhs.ptr, right_length, &status);
        if (status > 0) return error.SqlIcuComparisonFailed;
        if (result < 0) return .lt;
        if (result > 0) return .gt;
        return if (self.deterministic) std.mem.order(u8, lhs, rhs) else .eq;
    }

    /// Raw ICU weights include their terminating zero byte. Use compareKeys for
    /// verification, or encode weights and source as separate ordered tuple
    /// components in the native index codec. Never concatenate unterminated
    /// weights or treat equal weights as equal deterministic SQL text.
    pub fn weightsAlloc(self: Prepared, a: std.mem.Allocator, text: []const u8) ![]u8 {
        return self.weightsAllocLimit(a, text, std.math.maxInt(i32));
    }

    fn weightsAllocLimit(self: Prepared, a: std.mem.Allocator, text: []const u8, max_bytes: usize) ![]u8 {
        _ = try validateText(text);
        const utf16 = try std.unicode.utf8ToUtf16LeAlloc(a, text);
        if (comptime builtin.cpu.arch.endian() == .big) for (utf16) |*unit| {
            unit.* = @byteSwap(unit.*);
        };
        defer a.free(utf16);
        if (utf16.len > std.math.maxInt(i32)) return error.SqlCollationInputTooLarge;
        var empty: [1]u8 = undefined;
        const required: i32 = if (self.remote) |remote| std.math.cast(i32, try remote.weights(utf16, empty[0..0])) orelse return error.SqlCollationKeyTooLarge else self.provider.key_fn(self.handle, utf16.ptr, @intCast(utf16.len), &empty, 0);
        if (required <= 0) return error.SqlIcuComparisonFailed;
        if (@as(usize, @intCast(required)) > max_bytes) return error.SqlCollationKeyTooLarge;
        const weights = try a.alloc(u8, @intCast(required));
        errdefer a.free(weights);
        const actual: i32 = if (self.remote) |remote| std.math.cast(i32, try remote.weights(utf16, weights)) orelse return error.SqlCollationKeyTooLarge else self.provider.key_fn(self.handle, utf16.ptr, @intCast(utf16.len), weights.ptr, required);
        if (actual != required or weights[weights.len - 1] != 0) return error.SqlIcuComparisonFailed;
        return weights;
    }

    /// Complete memcomparable text component for native index storage. Its
    /// bytes agree with compare(), including deterministic ties. The caller's
    /// tuple codec owns SQL NULL tags and descending inversion. Account source
    /// text and weights together against the caller's stored-key byte budget.
    pub fn orderedKeyAlloc(self: Prepared, a: std.mem.Allocator, text: []const u8, max_bytes: usize) ![]u8 {
        if (self.deterministic and (max_bytes < 4 or text.len > max_bytes - 4)) return error.SqlCollationKeyTooLarge;
        const weights = try self.weightsAllocLimit(a, text, max_bytes);
        defer a.free(weights);
        var required: usize = 0;
        const components: []const []const u8 = if (self.deterministic) &.{ weights, text } else &.{weights};
        for (components) |bytes| {
            required = std.math.add(usize, required, 2) catch return error.SqlCollationKeyTooLarge;
            for (bytes) |byte| {
                required = std.math.add(usize, required, if (byte == 0) 2 else 1) catch return error.SqlCollationKeyTooLarge;
            }
        }
        if (required > max_bytes) return error.SqlCollationKeyTooLarge;
        const result = try a.alloc(u8, required);
        var offset: usize = 0;
        for (components) |bytes| {
            for (bytes) |byte| {
                result[offset] = byte;
                offset += 1;
                if (byte == 0) {
                    result[offset] = 255;
                    offset += 1;
                }
            }
            result[offset] = 0;
            result[offset + 1] = 0;
            offset += 2;
        }
        std.debug.assert(offset == result.len);
        return result;
    }

    pub fn compareKeys(self: Prepared, left_weights: []const u8, left_text: []const u8, right_weights: []const u8, right_text: []const u8) std.math.Order {
        const primary = std.mem.order(u8, left_weights, right_weights);
        return if (primary != .eq or !self.deterministic) primary else std.mem.order(u8, left_text, right_text);
    }
};

test "SQL native ICU deterministic ties and Unicode weights agree" {
    // This is a required-provider test: absence fails, never skip or fallback.
    var provider = try Provider.load(null);
    defer provider.deinit();
    const a = std.testing.allocator;
    var deterministic = try provider.prepare(a, "und-u-ks-level2", true, null);
    defer deterministic.deinit();
    var insensitive = try provider.prepare(a, "und-u-ks-level2", false, deterministic.version);
    defer insensitive.deinit();
    // PostgreSQL18.6 oracle: original CREATE defaults deterministic=true.
    try std.testing.expectEqual(std.math.Order.lt, try deterministic.compare("A", "a"));
    try std.testing.expectEqual(std.math.Order.gt, try deterministic.compare("é", "e\u{301}"));
    try std.testing.expectEqual(std.math.Order.lt, try deterministic.compare("ä", "z"));
    try std.testing.expectEqual(std.math.Order.eq, try insensitive.compare("A", "a"));
    try std.testing.expectEqual(std.math.Order.eq, try insensitive.compare("É", "é"));
    try std.testing.expectEqual(std.math.Order.eq, try insensitive.compare("é", "e\u{301}"));
    try std.testing.expect((try insensitive.compare("e", "é")) != .eq);
    const texts: []const []const u8 = &.{ "", "A", "a", "e", "e\u{301}", "é", "É", "z", "ä", "æ", "Ω", "🐜" };
    for (texts) |left| for (texts) |right| {
        const left_key = try deterministic.weightsAlloc(a, left);
        defer a.free(left_key);
        const right_key = try deterministic.weightsAlloc(a, right);
        defer a.free(right_key);
        try std.testing.expectEqual(try deterministic.compare(left, right), deterministic.compareKeys(left_key, left, right_key, right));
        try std.testing.expectEqual(try insensitive.compare(left, right), insensitive.compareKeys(left_key, left, right_key, right));
        inline for (.{ &deterministic, &insensitive }) |collator| {
            const left_ordered = try collator.orderedKeyAlloc(a, left, 4096);
            defer a.free(left_ordered);
            const right_ordered = try collator.orderedKeyAlloc(a, right, 4096);
            defer a.free(right_ordered);
            try std.testing.expectEqual(try collator.compare(left, right), std.mem.order(u8, left_ordered, right_ordered));
        }
    };
    try std.testing.expectError(error.SqlInvalidTextEncoding, deterministic.compare("\xff", "a"));
    try std.testing.expectError(error.SqlInvalidTextEncoding, deterministic.compare("a\x00b", "a"));
    var wrong_version = deterministic.version;
    wrong_version.collation[0] ^= 1;
    try std.testing.expectError(error.SqlCollationVersionMismatch, provider.prepare(a, "und-u-ks-level2", true, wrong_version));
    try std.testing.expectError(error.SqlInvalidCollationLocale, provider.prepare(a, "und\x00", true, null));
    try std.testing.expectError(error.SqlCollationKeyTooLarge, deterministic.orderedKeyAlloc(a, "a", 1));
}

test "SQL native ICU provider absence fails closed" {
    try std.testing.expectError(error.SqlIcuProviderUnavailable, Provider.load("/antfly-no-such-icu-library"));
}

/// Trusted startup-only configuration. Paths are copied before startup returns;
/// neither SQL text nor a request envelope can select a library.
pub const Config = struct {
    enabled: bool = true,
    library_path: ?[]const u8 = null,
    required: bool = false,
};

/// A stable allocation owns one native library and its immutable configuration.
/// Factory receivers and prepared collators retain this allocation separately.
/// No runtime teardown can unload code while a retained schema still calls it.
pub const Owner = struct {
    allocator: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    provider: Provider,
    library_path: ?[]const u8,
    pub fn create(a: std.mem.Allocator, config: Config) !?*Owner {
        if (!config.enabled) {
            if (config.required or config.library_path != null) return error.SqlIcuProviderUnavailable;
            return null;
        }
        if (config.library_path) |path| if (!std.fs.path.isAbsolute(path)) return error.SqlIcuProviderUnavailable;
        const path = if (config.library_path) |value| try a.dupe(u8, value) else null;
        errdefer if (path) |value| a.free(value);
        var provider = Provider.load(path) catch |err| {
            if (!config.required and path == null and (err == error.SqlIcuProviderUnavailable or err == error.SqlIcuProviderAbiMismatch)) return null;
            return err;
        };
        errdefer provider.deinit();
        // The advertised feature is based on this provider's real C API and
        // collation preparation, not merely the presence of a shared object.
        var probe = try provider.prepare(a, "und-u-ks-level2", true, null);
        defer probe.deinit();
        if (try probe.compare("ä", "z") != .lt) return error.SqlIcuComparisonFailed;
        const key = try probe.orderedKeyAlloc(a, "ä", 4096);
        a.free(key);
        const owner = try a.create(Owner);
        owner.* = .{ .allocator = a, .provider = provider, .library_path = path };
        return owner;
    }
    pub fn retain(self: *Owner) void {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
    }
    pub fn release(self: *Owner) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.allocator;
        self.provider.deinit();
        if (self.library_path) |path| a.free(path);
        a.destroy(self);
    }
    pub fn borrow(self: *Owner) provider_abi.Borrow {
        return .{ .context = self, .vtable = &vtable };
    }
    const Instance = struct { owner: *Owner, prepared: Prepared };
    fn cast(raw: *anyopaque) *Owner {
        return @ptrCast(@alignCast(raw));
    }
    fn instance(raw: *anyopaque) *Instance {
        return @ptrCast(@alignCast(raw));
    }
    fn retainCallback(raw: *anyopaque) callconv(.c) void {
        cast(raw).retain();
    }
    fn releaseCallback(raw: *anyopaque) callconv(.c) void {
        cast(raw).release();
    }
    const error_abi = @import("antfly_runtime_abi").error_abi;
    fn prepareCallback(raw: *anyopaque, locale: [*]const u8, len: usize, deterministic: u8, expected: ?*const Version, output: *?*anyopaque, version: *Version) callconv(.c) error_abi.Status {
        output.* = null;
        if (deterministic > 1 or len > 1024) return error_abi.statusFromError(error.SqlInvalidCollationLocale);
        const owner = cast(raw);
        var prepared = owner.provider.prepare(owner.allocator, locale[0..len], deterministic == 1, if (expected) |value| value.* else null) catch |err| return error_abi.statusFromError(err);
        const result = owner.allocator.create(Instance) catch |err| {
            prepared.deinit();
            return error_abi.statusFromError(err);
        };
        owner.retain();
        result.* = .{ .owner = owner, .prepared = prepared };
        version.* = prepared.version;
        output.* = result;
        return .{};
    }
    fn compareCallback(raw: *anyopaque, lhs: [*]const u8, lhs_len: usize, rhs: [*]const u8, rhs_len: usize, output: *i8) callconv(.c) error_abi.Status {
        const result = instance(raw).prepared.compare(lhs[0..lhs_len], rhs[0..rhs_len]) catch |err| return error_abi.statusFromError(err);
        output.* = switch (result) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        };
        return .{};
    }
    fn weightsCallback(raw: *anyopaque, text: [*]const u16, len: usize, output: [*]u8, capacity: u32, required: *u32) callconv(.c) error_abi.Status {
        const prepared = instance(raw).prepared;
        const input_len = std.math.cast(i32, len) orelse return error_abi.statusFromError(error.SqlCollationInputTooLarge);
        const output_len = std.math.cast(i32, capacity) orelse return error_abi.statusFromError(error.SqlCollationKeyTooLarge);
        const count = prepared.provider.key_fn(prepared.handle, text, input_len, output, output_len);
        if (count <= 0) return error_abi.statusFromError(error.SqlIcuComparisonFailed);
        required.* = @intCast(count);
        return .{};
    }
    fn closeCallback(raw: *anyopaque) callconv(.c) void {
        const value = instance(raw);
        const owner = value.owner;
        value.prepared.deinit();
        owner.allocator.destroy(value);
        owner.release();
    }
    const vtable: provider_abi.VTable = .{ .retain = retainCallback, .release = releaseCallback, .prepare = prepareCallback, .compare = compareCallback, .weights = weightsCallback, .close = closeCallback };
};

test "SQL ICU configured provider port retains native library through runtime and factory teardown" {
    const a = std.testing.allocator;
    const owner = (try Owner.create(a, .{ .required = true })).?;
    var invalid = owner.borrow();
    invalid.abi_version += 1;
    try std.testing.expectError(error.SqlIcuProviderAbiMismatch, invalid.receive());
    try std.testing.expectEqual(@as(usize, 1), owner.refs.load(.monotonic));
    var receiver = try owner.borrow().receive();
    var facade = Provider.fromReceiver(receiver);
    var prepared = try facade.prepare(a, "und-u-ks-level2", true, null);
    // Only the prepared opaque collator remains after all startup/factory owners
    // close. Its own provider lease must protect comparison and sort-key calls.
    owner.release();
    receiver.deinit();
    facade.deinit();
    defer prepared.deinit();
    try std.testing.expectEqual(std.math.Order.lt, try prepared.compare("ä", "z"));
    try std.testing.expectError(error.SqlInvalidTextEncoding, prepared.compare("\xff", "z"));
    const key = try prepared.orderedKeyAlloc(a, "ä", 1024);
    defer a.free(key);
    try std.testing.expect(key.len > 0);
}

pub const Context = provider_abi.Context;
