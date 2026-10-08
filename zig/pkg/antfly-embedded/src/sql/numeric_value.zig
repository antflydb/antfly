// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared exact NUMERIC kernel. Immutable canonical base-10000 limbs retain
//! display scale separately from value identity; no operation converts through
//! floating point. SQL binding, typed storage and wire activation are separate
//! consumers, not implied by the existence of this kernel.
//! Semantics are checked against PostgreSQL 18 numeric.c and a live oracle.
const std = @import("std");
const A = std.mem.Allocator;
const base: u32 = 10000;
const powers = [_]u16{ 1, 10, 100, 1000 };
pub const maximum_scale = 16383;
pub const maximum_weight = 32767;
pub const Kind = enum { finite, nan, positive_infinity, negative_infinity };
pub const Rounding = enum { half_away, truncate };

pub const TypeModifier = struct {
    precision: u16,
    scale: i16 = 0,

    pub fn validate(self: TypeModifier) !void {
        if (self.precision < 1 or self.precision > 1000 or self.scale < -1000 or self.scale > 1000)
            return error.SqlInvalidParameterValue;
    }
};

pub const Context = struct {
    alloc: A,
    remaining: u64 = 8 * 1024 * 1024,
    max_input_bytes: usize = 1024 * 1024,
    max_output_bytes: usize = 1024 * 1024,
    max_groups: usize = 73730,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    ptr: ?*anyopaque = null,
    since_poll: u16 = 256,
    failure: ?anyerror = null,

    pub fn charge(self: *Context, count: u64) !void {
        if (self.failure) |err| return err;
        if (count > self.remaining) {
            self.failure = error.SqlProgramLimitExceeded;
            return error.SqlProgramLimitExceeded;
        }
        self.remaining -= count;
        if (count >= 256 -| self.since_poll) {
            self.since_poll = 0;
            if (self.checkpoint) |poll| poll(self.ptr) catch |err| {
                self.failure = err;
                return err;
            };
        } else self.since_poll += @intCast(count);
    }

    fn limit(self: *Context) anyerror {
        if (self.failure) |err| return err;
        self.failure = error.SqlProgramLimitExceeded;
        return error.SqlProgramLimitExceeded;
    }

    fn allocate(self: *Context, count: usize) ![]u16 {
        if (count > self.max_groups) return self.limit();
        try self.charge(count);
        return self.alloc.alloc(u16, count);
    }
};

/// Borrowed immutable view. Limbs have neither leading nor trailing zeroes.
/// The highest limb has exponent `weight` in base 10000; zero has no limbs,
/// no sign and weight zero. Special values have no payload or display scale.
pub const Value = struct {
    kind: Kind = .finite,
    negative: bool = false,
    weight: i32 = 0,
    scale: u16 = 0,
    digits: []const u16 = &.{},

    pub fn isZero(self: Value) bool {
        return self.kind == .finite and self.digits.len == 0;
    }

    fn group(self: Value, exponent: i32) u16 {
        const index: i64 = @as(i64, self.weight) - exponent;
        return if (index >= 0 and index < self.digits.len) self.digits[@intCast(index)] else 0;
    }

    fn decimalDigit(self: Value, exponent: i32) u8 {
        return @intCast(self.group(@divFloor(exponent, 4)) / powers[@intCast(@mod(exponent, 4))] % 10);
    }

    fn lowest(self: Value) i32 {
        return self.weight - @as(i32, @intCast(self.digits.len)) + 1;
    }

    fn negated(self: Value) Value {
        var result = self;
        switch (self.kind) {
            .finite => if (!self.isZero()) {
                result.negative = !self.negative;
            },
            .positive_infinity => result.kind = .negative_infinity,
            .negative_infinity => result.kind = .positive_infinity,
            .nan => {},
        }
        return result;
    }
};

/// Own the complete allocation, not a trimmed interior slice. Moving this
/// owner never changes the address of its immutable digit payload.
pub const Owned = struct {
    value: Value,
    allocation: []u16 = &.{},
    alloc: A,

    pub fn deinit(self: *Owned) void {
        self.alloc.free(self.allocation);
        self.* = undefined;
    }
};

fn normalized(storage: []u16, weight: i32, scale: u16, negative: bool) Value {
    var first: usize = 0;
    var last = storage.len;
    while (first < last and storage[first] == 0) first += 1;
    while (last > first and storage[last - 1] == 0) last -= 1;
    return .{ .digits = storage[first..last], .weight = if (first == last) 0 else weight - @as(i32, @intCast(first)), .scale = scale, .negative = negative and first != last };
}

fn finish(a: A, storage: []u16, weight: i32, scale: u16, negative: bool) !Owned {
    const value = normalized(storage, weight, scale, negative);
    if (value.weight > maximum_weight or scale > maximum_scale) return error.InvalidSqlNumber;
    return .{ .alloc = a, .allocation = storage, .value = value };
}

fn special(a: A, kind: Kind) Owned {
    return .{ .alloc = a, .value = .{ .kind = kind } };
}

fn zero(a: A, scale: u16) Owned {
    return .{ .alloc = a, .value = .{ .scale = scale } };
}

fn clone(ctx: *Context, value: Value, scale: u16) !Owned {
    if (value.kind != .finite) return special(ctx.alloc, value.kind);
    const storage = try ctx.allocate(value.digits.len);
    @memcpy(storage, value.digits);
    return .{ .alloc = ctx.alloc, .allocation = storage, .value = .{ .digits = storage, .weight = value.weight, .scale = scale, .negative = value.negative } };
}

fn whitespace(byte: u8) bool {
    return byte == ' ' or (byte >= '\t' and byte <= '\r');
}

pub fn parse(ctx: *Context, input: []const u8) !Owned {
    if (input.len > ctx.max_input_bytes) return ctx.limit();
    var start: usize = 0;
    var end = input.len;
    while (start < end and whitespace(input[start])) : (start += 1) try ctx.charge(1);
    while (end > start and whitespace(input[end - 1])) : (end -= 1) try ctx.charge(1);
    const text = input[start..end];
    if (text.len == 0) return error.SqlInvalidTextRepresentation;
    var pos: usize = 0;
    const has_sign = text[0] == '+' or text[0] == '-';
    const negative = text[0] == '-';
    if (has_sign) pos += 1;
    const body = text[pos..];
    try ctx.charge(1);
    if (!has_sign and std.ascii.eqlIgnoreCase(body, "nan")) return special(ctx.alloc, .nan);
    if (std.ascii.eqlIgnoreCase(body, "inf") or std.ascii.eqlIgnoreCase(body, "infinity")) return special(ctx.alloc, if (negative) .negative_infinity else .positive_infinity);
    if (body.len >= 2 and body[0] == '0') {
        const radix: u8 = switch (body[1]) {
            'x', 'X' => 16,
            'o', 'O' => 8,
            'b', 'B' => 2,
            else => 10,
        };
        if (radix != 10) return parseRadix(ctx, body[2..], radix, negative);
    }
    const mantissa_start = pos;
    var point = false;
    var count: i64 = 0;
    var integral: i64 = 0;
    var first_nonzero: ?i64 = null;
    var last_nonzero: i64 = 0;
    var previous_digit = false;
    while (pos < text.len) : (pos += 1) {
        try ctx.charge(1);
        const byte = text[pos];
        if (std.ascii.isDigit(byte)) {
            if (byte != '0') {
                if (first_nonzero == null) first_nonzero = count;
                last_nonzero = count;
            }
            count += 1;
            if (!point) integral += 1;
            previous_digit = true;
        } else if (byte == '.' and !point) {
            point = true;
            previous_digit = false;
        } else if (byte == '_' and previous_digit and pos + 1 < text.len and std.ascii.isDigit(text[pos + 1])) {
            previous_digit = false;
        } else break;
    }
    if (count == 0) return error.SqlInvalidTextRepresentation;
    const mantissa_end = pos;
    var exponent: i64 = 0;
    if (pos < text.len and (text[pos] == 'e' or text[pos] == 'E')) {
        pos += 1;
        var exponent_negative = false;
        if (pos < text.len and (text[pos] == '+' or text[pos] == '-')) {
            exponent_negative = text[pos] == '-';
            pos += 1;
        }
        var exponent_digits: usize = 0;
        previous_digit = false;
        while (pos < text.len) : (pos += 1) {
            try ctx.charge(1);
            const byte = text[pos];
            if (std.ascii.isDigit(byte)) {
                exponent = exponent * 10 + byte - '0';
                if (exponent > 1073741823) return error.InvalidSqlNumber;
                exponent_digits += 1;
                previous_digit = true;
            } else if (byte == '_' and previous_digit and pos + 1 < text.len and std.ascii.isDigit(text[pos + 1])) {
                previous_digit = false;
            } else break;
        }
        if (exponent_digits == 0) return error.SqlInvalidTextRepresentation;
        if (exponent_negative) exponent = -exponent;
    }
    if (pos != text.len) return error.SqlInvalidTextRepresentation;
    const scale: i64 = @max(count - integral - exponent, 0);
    if (scale > maximum_scale) return error.InvalidSqlNumber;
    const first = first_nonzero orelse return zero(ctx.alloc, @intCast(scale));
    const high: i64 = @divFloor(integral + exponent - 1 - first, 4);
    const low = @divFloor(integral + exponent - 1 - last_nonzero, 4);
    if (high > maximum_weight) return error.InvalidSqlNumber;
    const storage = try ctx.allocate(@intCast(high - low + 1));
    errdefer ctx.alloc.free(storage);
    @memset(storage, 0);
    var index: i64 = 0;
    for (text[mantissa_start..mantissa_end]) |byte| {
        try ctx.charge(1);
        if (!std.ascii.isDigit(byte)) continue;
        if (byte != '0') {
            const power = integral + exponent - 1 - index;
            storage[@intCast(high - @divFloor(power, 4))] += @as(u16, byte - '0') * powers[@intCast(@mod(power, 4))];
        }
        index += 1;
    }
    return finish(ctx.alloc, storage, @intCast(high), @intCast(scale), negative);
}

fn radixDigit(byte: u8, radix: u8) ?u8 {
    const digit: u8 = switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => return null,
    };
    return if (digit < radix) digit else null;
}

fn parseRadix(ctx: *Context, text: []const u8, radix: u8, negative: bool) !Owned {
    var digits: usize = 0;
    var first_nonzero: ?usize = null;
    var previous = false;
    for (text, 0..) |byte, i| {
        try ctx.charge(1);
        if (radixDigit(byte, radix)) |digit| {
            if (digit != 0 and first_nonzero == null) first_nonzero = digits;
            digits += 1;
            previous = true;
        } else if (byte == '_' and (previous or i == 0) and i + 1 < text.len and radixDigit(text[i + 1], radix) != null) {
            previous = false;
        } else return error.SqlInvalidTextRepresentation;
    }
    if (digits == 0) return error.SqlInvalidTextRepresentation;
    const first = first_nonzero orelse return zero(ctx.alloc, 0);
    const bits = (digits - first) * @as(usize, switch (radix) {
        2 => 1,
        8 => 3,
        16 => 4,
        else => unreachable,
    });
    // A conservative integer upper bound on log10(2), not float conversion.
    const decimal_digits = (@as(u64, bits) * 30103 + 99999) / 100000;
    const capacity: usize = @intCast(@min((decimal_digits + 3) / 4 + 1, maximum_weight + 2));
    const storage = try ctx.allocate(capacity);
    errdefer ctx.alloc.free(storage);
    @memset(storage, 0);
    var used: usize = 0;
    var multiplier: u32 = 1;
    var chunk: u32 = 0;
    for (text) |byte| {
        try ctx.charge(1);
        const digit = radixDigit(byte, radix) orelse continue;
        if (multiplier > std.math.maxInt(u32) / @as(u32, radix)) {
            try radixChunk(ctx, storage, &used, multiplier, chunk);
            multiplier = 1;
            chunk = 0;
        }
        multiplier *= radix;
        chunk = chunk * radix + digit;
    }
    try radixChunk(ctx, storage, &used, multiplier, chunk);
    std.mem.reverse(u16, storage[0..used]);
    return finish(ctx.alloc, storage, @as(i32, @intCast(used)) - 1, 0, negative);
}

fn radixChunk(ctx: *Context, storage: []u16, used: *usize, multiplier: u32, chunk: u32) !void {
    var carry: u64 = chunk;
    for (storage[0..used.*]) |*digit| {
        try ctx.charge(1);
        const total = @as(u64, digit.*) * multiplier + carry;
        digit.* = @intCast(total % base);
        carry = total / base;
    }
    while (carry != 0) {
        try ctx.charge(1);
        if (used.* >= storage.len) return error.InvalidSqlNumber;
        storage[used.*] = @intCast(carry % base);
        used.* += 1;
        carry /= base;
    }
}

pub fn format(ctx: *Context, value: Value) ![]u8 {
    try ctx.charge(1);
    const token: ?[]const u8 = switch (value.kind) {
        .nan => "NaN",
        .positive_infinity => "Infinity",
        .negative_infinity => "-Infinity",
        .finite => null,
    };
    if (token) |text| {
        if (text.len > ctx.max_output_bytes) return ctx.limit();
        try ctx.charge(text.len);
        return ctx.alloc.dupe(u8, text);
    }
    var integral: usize = 1;
    if (!value.isZero() and value.weight >= 0) {
        var first = value.digits[0];
        var width: usize = 1;
        while (first >= 10) : (width += 1) first /= 10;
        integral = @as(usize, @intCast(value.weight)) * 4 + width;
    }
    const size = integral + @as(usize, value.scale) + @intFromBool(value.scale != 0) + @intFromBool(value.negative);
    if (size > ctx.max_output_bytes) return ctx.limit();
    const result = try ctx.alloc.alloc(u8, size);
    errdefer ctx.alloc.free(result);
    var at: usize = 0;
    if (value.negative) {
        result[at] = '-';
        at += 1;
    }
    for (0..integral) |i| {
        try ctx.charge(1);
        result[at] = '0' + value.decimalDigit(@intCast(integral - i - 1));
        at += 1;
    }
    if (value.scale != 0) {
        result[at] = '.';
        at += 1;
    }
    for (0..value.scale) |i| {
        try ctx.charge(1);
        result[at] = '0' + value.decimalDigit(-1 - @as(i32, @intCast(i)));
        at += 1;
    }
    return result;
}

fn rank(value: Value) u8 {
    return switch (value.kind) {
        .negative_infinity => 0,
        .finite => 1,
        .positive_infinity => 2,
        .nan => 3,
    };
}

fn magnitude(ctx: *Context, left: Value, right: Value) !std.math.Order {
    try ctx.charge(1);
    if (left.isZero() or right.isZero()) return std.math.order(@intFromBool(!left.isZero()), @intFromBool(!right.isZero()));
    if (left.weight != right.weight) return std.math.order(left.weight, right.weight);
    for (0..@max(left.digits.len, right.digits.len)) |i| {
        try ctx.charge(1);
        const l = if (i < left.digits.len) left.digits[i] else 0;
        const r = if (i < right.digits.len) right.digits[i] else 0;
        if (l != r) return std.math.order(l, r);
    }
    return .eq;
}

pub fn order(ctx: *Context, left: Value, right: Value) !std.math.Order {
    try ctx.charge(1);
    if (left.kind != .finite or right.kind != .finite) return std.math.order(rank(left), rank(right));
    if (left.negative != right.negative) return if (left.negative) .lt else .gt;
    const result = try magnitude(ctx, left, right);
    return if (left.negative) result.invert() else result;
}

/// Equal logical values hash equally, independent of display scale, input
/// spelling or machine byte order. Consumers choose their own outer hash seed.
pub fn hash(ctx: *Context, value: Value, hasher: anytype) !void {
    try ctx.charge(1);
    hasher.update(&.{ rank(value), @intFromBool(value.negative) });
    var weight: [4]u8 = undefined;
    std.mem.writeInt(i32, &weight, value.weight, .big);
    hasher.update(&weight);
    var count: [4]u8 = undefined;
    std.mem.writeInt(u32, &count, @intCast(value.digits.len), .big);
    hasher.update(&count);
    for (value.digits) |digit| {
        try ctx.charge(1);
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, digit, .big);
        hasher.update(&bytes);
    }
}

pub fn add(ctx: *Context, left: Value, right: Value) !Owned {
    try ctx.charge(1);
    if (left.kind == .nan or right.kind == .nan) return special(ctx.alloc, .nan);
    if (left.kind != .finite or right.kind != .finite) {
        if (left.kind != .finite and right.kind != .finite and left.kind != right.kind) return special(ctx.alloc, .nan);
        return special(ctx.alloc, if (left.kind != .finite) left.kind else right.kind);
    }
    const scale = @max(left.scale, right.scale);
    if (left.isZero()) return clone(ctx, right, scale);
    if (right.isZero()) return clone(ctx, left, scale);
    var large = left;
    var small = right;
    const subtracting = left.negative != right.negative;
    if (subtracting) switch (try magnitude(ctx, left, right)) {
        .eq => return zero(ctx.alloc, scale),
        .lt => {
            large = right;
            small = left;
        },
        .gt => {},
    };
    const high = @max(large.weight, small.weight) + @as(i32, @intFromBool(!subtracting));
    const low = @min(large.lowest(), small.lowest());
    const storage = try ctx.allocate(@intCast(high - low + 1));
    errdefer ctx.alloc.free(storage);
    var carry: i32 = 0;
    var i = storage.len;
    while (i != 0) {
        i -= 1;
        try ctx.charge(1);
        const exponent = high - @as(i32, @intCast(i));
        var digit = @as(i32, large.group(exponent)) + carry;
        if (subtracting) {
            digit -= small.group(exponent);
            carry = if (digit < 0) -1 else 0;
            if (digit < 0) digit += base;
        } else {
            digit += small.group(exponent);
            carry = @divTrunc(digit, base);
            digit = @mod(digit, base);
        }
        storage[i] = @intCast(digit);
    }
    return finish(ctx.alloc, storage, high, scale, large.negative);
}

pub fn subtract(ctx: *Context, left: Value, right: Value) !Owned {
    return add(ctx, left, right.negated());
}

pub fn quantize(ctx: *Context, value: Value, requested_scale: i32, mode: Rounding) !Owned {
    try ctx.charge(1);
    if (value.kind != .finite) return special(ctx.alloc, value.kind);
    const scale = std.math.clamp(requested_scale, -131073, maximum_scale);
    const display: u16 = @intCast(@max(scale, 0));
    if (value.isZero()) return zero(ctx.alloc, display);
    const cut = -scale;
    const unit = @divFloor(cut, 4);
    if (unit < value.lowest()) return clone(ctx, value, display);
    const high = value.weight + 1;
    if (unit > high) return zero(ctx.alloc, display);
    const storage = try ctx.allocate(@intCast(high - unit + 1));
    errdefer ctx.alloc.free(storage);
    for (storage, 0..) |*digit, i| {
        try ctx.charge(1);
        digit.* = value.group(high - @as(i32, @intCast(i)));
    }
    const factor: u16 = powers[@intCast(@mod(cut, 4))];
    storage[storage.len - 1] = storage[storage.len - 1] / factor * factor;
    var carry: u32 = if (mode == .half_away and value.decimalDigit(cut - 1) >= 5) factor else 0;
    var index = storage.len;
    while (carry != 0) {
        try ctx.charge(1);
        index -= 1;
        const total = storage[index] + carry;
        storage[index] = @intCast(total % base);
        carry = total / base;
    }
    return finish(ctx.alloc, storage, high, display, value.negative);
}

/// Round before checking precision, including scales greater than precision
/// and negative scales. The canonical leading limb proves overflow without
/// formatting or allocating an expanded decimal string.
pub fn applyTypeModifier(ctx: *Context, value: Value, modifier: TypeModifier) !Owned {
    try ctx.charge(1);
    try modifier.validate();
    if (value.kind == .nan) return special(ctx.alloc, .nan);
    if (value.kind != .finite) return error.InvalidSqlNumber;
    var rounded = try quantize(ctx, value, modifier.scale, .half_away);
    errdefer rounded.deinit();
    if (!rounded.value.isZero()) {
        var leading = rounded.value.digits[0];
        var decimal_digits: i32 = 1;
        while (leading >= 10) : (decimal_digits += 1) leading /= 10;
        const integral_digits = rounded.value.weight * 4 + decimal_digits;
        if (integral_digits > @as(i32, modifier.precision) - modifier.scale) return error.InvalidSqlNumber;
    }
    return rounded;
}

/// PostgreSQL NUMERIC-to-integer casts round ties away from zero. Accumulate
/// against an unsigned magnitude bound so the asymmetric signed minimum is
/// accepted without ever overflowing a signed intermediate.
pub fn toInteger(comptime T: type, ctx: *Context, value: Value) !T {
    comptime std.debug.assert(T == i16 or T == i32 or T == i64);
    try ctx.charge(1);
    if (value.kind != .finite) return error.SqlFeatureNotSupported;
    var rounded = try quantize(ctx, value, 0, .half_away);
    defer rounded.deinit();
    const v = rounded.value;
    if (v.isZero()) return 0;
    if (v.weight > 4) return error.InvalidSqlNumber;
    const bound: u64 = @as(u64, std.math.maxInt(T)) + @intFromBool(v.negative);
    var integer: u64 = 0;
    var exponent = v.weight;
    while (exponent >= 0) : (exponent -= 1) {
        try ctx.charge(1);
        const digit: u64 = v.group(exponent);
        if (digit > bound or integer > (bound - digit) / base) return error.InvalidSqlNumber;
        integer = integer * base + digit;
    }
    if (!v.negative) return @intCast(integer);
    if (integer == @as(u64, std.math.maxInt(T)) + 1) return std.math.minInt(T);
    return -@as(T, @intCast(integer));
}

pub fn multiply(ctx: *Context, left: Value, right: Value) !Owned {
    try ctx.charge(1);
    if (left.kind == .nan or right.kind == .nan) return special(ctx.alloc, .nan);
    if (left.kind != .finite or right.kind != .finite) {
        if (left.isZero() or right.isZero()) return special(ctx.alloc, .nan);
        const lneg = left.kind == .negative_infinity or left.negative;
        const rneg = right.kind == .negative_infinity or right.negative;
        return special(ctx.alloc, if (lneg != rneg) .negative_infinity else .positive_infinity);
    }
    const scale: u16 = left.scale + right.scale;
    if (left.isZero() or right.isZero()) return zero(ctx.alloc, @min(scale, maximum_scale));
    if (@as(u64, left.digits.len) * right.digits.len > ctx.remaining) return ctx.limit();
    const storage = try ctx.allocate(left.digits.len + right.digits.len);
    errdefer ctx.alloc.free(storage);
    @memset(storage, 0);
    var i = left.digits.len;
    while (i != 0) {
        i -= 1;
        var carry: u32 = 0;
        var j = right.digits.len;
        while (j != 0) {
            j -= 1;
            try ctx.charge(1);
            const total = @as(u32, left.digits[i]) * right.digits[j] + storage[i + j + 1] + carry;
            storage[i + j + 1] = @intCast(total % base);
            carry = total / base;
        }
        storage[i] = @intCast(carry);
    }
    const weight = left.weight + right.weight + 1;
    if (scale > maximum_scale) {
        const result = try quantize(ctx, normalized(storage, weight, scale, left.negative != right.negative), maximum_scale, .half_away);
        ctx.alloc.free(storage);
        return result;
    }
    return finish(ctx.alloc, storage, weight, scale, left.negative != right.negative);
}

const OracleCase = struct {
    op: enum { parse, add, subtract, multiply, order, round, truncate, typmod, int16, int32, int64 },
    left: []const u8,
    right: ?[]const u8 = null,
    precision: u16 = 0,
    scale: i32 = 0,
    expected: ?std.json.Value = null,
    @"error": ?[]const u8 = null,
};

fn oracleText(ctx: *Context, entry: OracleCase) ![]u8 {
    var left = try parse(ctx, entry.left);
    defer left.deinit();
    var right = if (entry.right) |text| try parse(ctx, text) else zero(ctx.alloc, 0);
    defer right.deinit();
    switch (entry.op) {
        .order => {
            const actual: i8 = switch (try order(ctx, left.value, right.value)) {
                .lt => -1,
                .eq => 0,
                .gt => 1,
            };
            return std.fmt.allocPrint(ctx.alloc, "{d}", .{actual});
        },
        .int16 => return std.fmt.allocPrint(ctx.alloc, "{d}", .{try toInteger(i16, ctx, left.value)}),
        .int32 => return std.fmt.allocPrint(ctx.alloc, "{d}", .{try toInteger(i32, ctx, left.value)}),
        .int64 => return std.fmt.allocPrint(ctx.alloc, "{d}", .{try toInteger(i64, ctx, left.value)}),
        else => {},
    }
    var result = switch (entry.op) {
        .parse => try clone(ctx, left.value, left.value.scale),
        .add => try add(ctx, left.value, right.value),
        .subtract => try subtract(ctx, left.value, right.value),
        .multiply => try multiply(ctx, left.value, right.value),
        .round => try quantize(ctx, left.value, entry.scale, .half_away),
        .truncate => try quantize(ctx, left.value, entry.scale, .truncate),
        .typmod => try applyTypeModifier(ctx, left.value, .{ .precision = entry.precision, .scale = @intCast(entry.scale) }),
        else => unreachable,
    };
    defer result.deinit();
    return format(ctx, result.value);
}

test "SQL exact NUMERIC kernel matches independent PostgreSQL oracle" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const OracleCase }, a, @embedFile("fixtures/sql_exact_numeric_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqualStrings("PostgreSQL exact NUMERIC kernel", fixture.value.reference);
    try std.testing.expectEqual(@as(usize, 312), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var ctx: Context = .{ .alloc = a };
        if (entry.@"error") |state| {
            const expected: anyerror = if (std.mem.eql(u8, state, "22P02")) error.SqlInvalidTextRepresentation else if (std.mem.eql(u8, state, "22003")) error.InvalidSqlNumber else if (std.mem.eql(u8, state, "22023")) error.SqlInvalidParameterValue else if (std.mem.eql(u8, state, "0A000")) error.SqlFeatureNotSupported else return error.UnexpectedNumericOracleError;
            try std.testing.expectError(expected, oracleText(&ctx, entry));
            continue;
        }
        const text = try oracleText(&ctx, entry);
        defer a.free(text);
        var expected_buffer: [32]u8 = undefined;
        const expected = if (entry.expected.? == .integer) try std.fmt.bufPrint(&expected_buffer, "{d}", .{entry.expected.?.integer}) else entry.expected.?.string;
        try std.testing.expectEqualStrings(expected, text);
    }
}

test "SQL exact NUMERIC identity ignores spelling and display scale without rounding" {
    var ctx: Context = .{ .alloc = std.testing.allocator };
    for ([_][2][]const u8{
        .{ "00012.3400", "1.234e1" },
        .{ "0", "-0.00000" },
        .{ "0x10000", "65536.0000" },
        .{ "nan", "NaN" },
        .{ "-inf", "-Infinity" },
    }) |pair| {
        var left = try parse(&ctx, pair[0]);
        defer left.deinit();
        var right = try parse(&ctx, pair[1]);
        defer right.deinit();
        try std.testing.expectEqual(std.math.Order.eq, try order(&ctx, left.value, right.value));
        var l = std.hash.Wyhash.init(42);
        var r = std.hash.Wyhash.init(42);
        try hash(&ctx, left.value, &l);
        try hash(&ctx, right.value, &r);
        try std.testing.expectEqual(l.final(), r.final());
    }
    var left = try parse(&ctx, "9007199254740993.0000000000000001");
    defer left.deinit();
    var right = try parse(&ctx, "9007199254740993.0000000000000002");
    defer right.deinit();
    try std.testing.expectEqual(std.math.Order.lt, try order(&ctx, left.value, right.value));
}

test "SQL exact NUMERIC range extremes retain compact limbs and bounded output" {
    const a = std.testing.allocator;
    var ctx: Context = .{ .alloc = a };
    var tiny = try parse(&ctx, "1e-16383");
    defer tiny.deinit();
    var huge = try parse(&ctx, "1e131071");
    defer huge.deinit();
    try std.testing.expectEqual(@as(usize, 1), tiny.value.digits.len);
    try std.testing.expectEqual(@as(usize, 1), huge.value.digits.len);
    const text = try format(&ctx, huge.value);
    defer a.free(text);
    try std.testing.expectEqual(@as(usize, 131072), text.len);
    try std.testing.expectEqual(@as(u8, '1'), text[0]);
    for (text[1..]) |byte| try std.testing.expectEqual(@as(u8, '0'), byte);
    const tiny_text = try format(&ctx, tiny.value);
    defer a.free(tiny_text);
    try std.testing.expectEqual(@as(usize, 16385), tiny_text.len);
    try std.testing.expectEqual(@as(u8, '1'), tiny_text[tiny_text.len - 1]);
    var product = try multiply(&ctx, tiny.value, tiny.value);
    defer product.deinit();
    try std.testing.expect(product.value.isZero());
    try std.testing.expectEqual(@as(u16, maximum_scale), product.value.scale);
    var lower = try quantize(&ctx, huge.value, std.math.minInt(i32), .half_away);
    defer lower.deinit();
    try std.testing.expect(lower.value.isZero());
    try std.testing.expectEqual(@as(u16, 0), lower.value.scale);
    var one = try parse(&ctx, "1.0");
    defer one.deinit();
    var upper = try quantize(&ctx, one.value, std.math.maxInt(i32), .half_away);
    defer upper.deinit();
    try std.testing.expectEqual(@as(u16, maximum_scale), upper.value.scale);
    const radix = try a.alloc(u8, 4098);
    defer a.free(radix);
    @memcpy(radix[0..2], "0x");
    @memset(radix[2..], 'f');
    var integer = try parse(&ctx, radix);
    defer integer.deinit();
    const integer_text = try format(&ctx, integer.value);
    defer a.free(integer_text);
    try std.testing.expectEqual(@as(usize, 4933), integer_text.len);
    var limited: Context = .{ .alloc = a, .max_output_bytes = 128 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, format(&limited, huge.value));
    var ten = try parse(&ctx, "10");
    defer ten.deinit();
    try std.testing.expectError(error.InvalidSqlNumber, multiply(&ctx, huge.value, ten.value));
}

test "SQL exact NUMERIC ownership unwinds every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var ctx: Context = .{ .alloc = a };
            var left = try parse(&ctx, "-0xFFFF_FFFF_FFFF_FFFF_FFFF");
            defer left.deinit();
            var right = try parse(&ctx, "1234.567890123456789");
            defer right.deinit();
            var constrained = try applyTypeModifier(&ctx, right.value, .{ .precision = 8, .scale = 3 });
            defer constrained.deinit();
            try std.testing.expectEqual(@as(i16, 1235), try toInteger(i16, &ctx, right.value));
            var sum = try add(&ctx, left.value, right.value);
            defer sum.deinit();
            var difference = try subtract(&ctx, left.value, right.value);
            defer difference.deinit();
            var product = try multiply(&ctx, sum.value, difference.value);
            defer product.deinit();
            var rounded = try quantize(&ctx, product.value, 3, .half_away);
            defer rounded.deinit();
            const text = try format(&ctx, rounded.value);
            defer a.free(text);
            var tiny = try parse(&ctx, "1e-8192");
            defer tiny.deinit();
            var underflow = try multiply(&ctx, tiny.value, tiny.value);
            defer underflow.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL exact NUMERIC quota and cancellation failures are sticky and retry safely" {
    const a = std.testing.allocator;
    const Poll = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.fail_at != 0 and self.calls == self.fail_at) return error.Canceled;
        }
    };
    const Harness = struct {
        fn run(ctx: *Context) !void {
            const input = try ctx.alloc.alloc(u8, 512);
            defer ctx.alloc.free(input);
            @memset(input, '9');
            var left = try parse(ctx, input);
            defer left.deinit();
            var product = try multiply(ctx, left.value, left.value);
            defer product.deinit();
            var rounded = try quantize(ctx, product.value, -16, .half_away);
            defer rounded.deinit();
            const text = try format(ctx, rounded.value);
            defer ctx.alloc.free(text);
            var constrained = try applyTypeModifier(ctx, left.value, .{ .precision = 1000 });
            defer constrained.deinit();
            var minimum = try parse(ctx, "-9223372036854775808.49");
            defer minimum.deinit();
            try std.testing.expectEqual(std.math.minInt(i64), try toInteger(i64, ctx, minimum.value));
        }
    };
    var poll: Poll = .{};
    var ctx: Context = .{ .alloc = a, .checkpoint = Poll.check, .ptr = &poll };
    try Harness.run(&ctx);
    const checkpoints = poll.calls;
    for (1..checkpoints + 1) |at| {
        poll = .{ .fail_at = at };
        ctx = .{ .alloc = a, .checkpoint = Poll.check, .ptr = &poll };
        try std.testing.expectError(error.Canceled, Harness.run(&ctx));
        try std.testing.expectError(error.Canceled, ctx.charge(0));
    }
    ctx = .{ .alloc = a, .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, parse(&ctx, "123"));
    try std.testing.expectError(error.SqlProgramLimitExceeded, ctx.charge(0));
    ctx = .{ .alloc = a };
    try Harness.run(&ctx);
}

test "SQL exact NUMERIC admission limits remain sticky across all operations" {
    const a = std.testing.allocator;
    var healthy: Context = .{ .alloc = a };
    var number = try parse(&healthy, "12345.6789");
    defer number.deinit();
    var ctx: Context = .{ .alloc = a, .max_input_bytes = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, parse(&ctx, "12"));
    try std.testing.expectError(error.SqlProgramLimitExceeded, parse(&ctx, "0"));
    ctx = .{ .alloc = a, .max_groups = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, parse(&ctx, "12345"));
    try std.testing.expectError(error.SqlProgramLimitExceeded, toInteger(i64, &ctx, .{}));
    ctx = .{ .alloc = a, .max_output_bytes = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, format(&ctx, number.value));
    try std.testing.expectError(error.SqlProgramLimitExceeded, applyTypeModifier(&ctx, .{}, .{ .precision = 1 }));
    ctx = .{ .alloc = a, .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, multiply(&ctx, number.value, number.value));
    try std.testing.expectError(error.SqlProgramLimitExceeded, order(&ctx, .{}, .{}));
}

test "SQL exact NUMERIC multiplication benchmark retains one bounded output allocation" {
    const a = std.testing.allocator;
    for ([_]usize{ 64, 256, 1024 }) |digits| {
        const input = try a.alloc(u8, digits);
        defer a.free(input);
        @memset(input, '9');
        var ctx: Context = .{ .alloc = a };
        var operand = try parse(&ctx, input);
        defer operand.deinit();
        var tracked = std.testing.FailingAllocator.init(a, .{});
        ctx = .{ .alloc = tracked.allocator() };
        const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        var result = try multiply(&ctx, operand.value, operand.value);
        defer result.deinit();
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
        try std.testing.expectEqual(@as(usize, 1), tracked.alloc_index);
        try std.testing.expectEqual(operand.value.digits.len * 2, result.value.digits.len);
        std.debug.print("NUMERIC multiply: decimal_digits={d} groups={d} allocations={d} work={d} elapsed_ns={d}\n", .{ digits, operand.value.digits.len, tracked.alloc_index, 8 * 1024 * 1024 - ctx.remaining, elapsed });
    }
}
