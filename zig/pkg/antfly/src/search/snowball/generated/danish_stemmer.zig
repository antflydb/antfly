//! Generated from danish.sbl by Snowball 3.0.0 - https://snowballstem.org/

const snowball = @import("env.zig");

fn suppress_any_unused_warning(ctx: *anyopaque) void {
    _ = ctx;
}

const a_0 = [_]snowball.Among{
    snowball.Among{ .s = "hed", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "ethed", .substring_i = 0, .result = 1, .method = null },
    snowball.Among{ .s = "ered", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "e", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "erede", .substring_i = 3, .result = 1, .method = null },
    snowball.Among{ .s = "ende", .substring_i = 3, .result = 1, .method = null },
    snowball.Among{ .s = "erende", .substring_i = 5, .result = 1, .method = null },
    snowball.Among{ .s = "ene", .substring_i = 3, .result = 1, .method = null },
    snowball.Among{ .s = "erne", .substring_i = 3, .result = 1, .method = null },
    snowball.Among{ .s = "ere", .substring_i = 3, .result = 1, .method = null },
    snowball.Among{ .s = "en", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "heden", .substring_i = 10, .result = 1, .method = null },
    snowball.Among{ .s = "eren", .substring_i = 10, .result = 1, .method = null },
    snowball.Among{ .s = "er", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "heder", .substring_i = 13, .result = 1, .method = null },
    snowball.Among{ .s = "erer", .substring_i = 13, .result = 1, .method = null },
    snowball.Among{ .s = "s", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "heds", .substring_i = 16, .result = 1, .method = null },
    snowball.Among{ .s = "es", .substring_i = 16, .result = 1, .method = null },
    snowball.Among{ .s = "endes", .substring_i = 18, .result = 1, .method = null },
    snowball.Among{ .s = "erendes", .substring_i = 19, .result = 1, .method = null },
    snowball.Among{ .s = "enes", .substring_i = 18, .result = 1, .method = null },
    snowball.Among{ .s = "ernes", .substring_i = 18, .result = 1, .method = null },
    snowball.Among{ .s = "eres", .substring_i = 18, .result = 1, .method = null },
    snowball.Among{ .s = "ens", .substring_i = 16, .result = 1, .method = null },
    snowball.Among{ .s = "hedens", .substring_i = 24, .result = 1, .method = null },
    snowball.Among{ .s = "erens", .substring_i = 24, .result = 1, .method = null },
    snowball.Among{ .s = "ers", .substring_i = 16, .result = 1, .method = null },
    snowball.Among{ .s = "ets", .substring_i = 16, .result = 1, .method = null },
    snowball.Among{ .s = "erets", .substring_i = 28, .result = 1, .method = null },
    snowball.Among{ .s = "et", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "eret", .substring_i = 30, .result = 1, .method = null },
};

const a_1 = [_]snowball.Among{
    snowball.Among{ .s = "gd", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "dt", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "gt", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "kt", .substring_i = -1, .result = -1, .method = null },
};

const a_2 = [_]snowball.Among{
    snowball.Among{ .s = "ig", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "lig", .substring_i = 0, .result = 1, .method = null },
    snowball.Among{ .s = "elig", .substring_i = 1, .result = 1, .method = null },
    snowball.Among{ .s = "els", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "løst", .substring_i = -1, .result = 2, .method = null },
};

const G_c = [_]u8{ 119, 223, 119, 1 };

const G_v = [_]u8{ 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 0, 128 };

const G_s_ending = [_]u8{ 239, 254, 42, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16 };

const Context = struct {
    i_p1: i32 = 0,
    S_ch: snowball.String = .{},
};

fn r_mark_regions(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var i_x: i32 = 0;
    context.i_p1 = @as(i32, @intCast(env.limit));
    const v_1 = env.cursor;
    if (!env.hop(3)) {
        return false;
    }
    i_x = @as(i32, @intCast(env.cursor));
    env.cursor = v_1;
    if (!env.goOutGrouping(&G_v, 97, 248)) {
        return false;
    }
    env.nextChar();
    if (!env.goInGrouping(&G_v, 97, 248)) {
        return false;
    }
    env.nextChar();
    context.i_p1 = @as(i32, @intCast(env.cursor));
    lab0: while (true) {
        if (context.i_p1 >= i_x) {
            break :lab0;
        }
        context.i_p1 = i_x;
        break :lab0;
    }
    return true;
}

fn r_main_suffix(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var among_var: i32 = 0;
    if (@as(i32, @intCast(env.cursor)) < context.i_p1) {
        return false;
    }
    const v_1 = env.limit_backward;
    env.limit_backward = @intCast(@as(u32, @intCast(context.i_p1)));
    env.ket = env.cursor;
    among_var = env.findAmongB(&a_0, @as(*anyopaque, @ptrCast(context)));
    if (among_var == 0) {
        env.limit_backward = v_1;
        return false;
    }
    env.bra = env.cursor;
    env.limit_backward = v_1;
    switch (among_var) {
        1 => {
            env.sliceDel() catch return false;
        },
        2 => {
            if (!env.inGroupingB(&G_s_ending, 97, 229)) {
                return false;
            }
            env.sliceDel() catch return false;
        },
        else => {},
    }
    return true;
}

fn r_consonant_pair(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    const v_1 = env.limit - env.cursor;
    if (@as(i32, @intCast(env.cursor)) < context.i_p1) {
        return false;
    }
    const v_2 = env.limit_backward;
    env.limit_backward = @intCast(@as(u32, @intCast(context.i_p1)));
    env.ket = env.cursor;
    if (env.findAmongB(&a_1, @as(*anyopaque, @ptrCast(context))) == 0) {
        env.limit_backward = v_2;
        return false;
    }
    env.bra = env.cursor;
    env.limit_backward = v_2;
    env.cursor = env.limit - v_1;
    if (env.cursor <= env.limit_backward) {
        return false;
    }
    env.prevChar();
    env.bra = env.cursor;
    env.sliceDel() catch return false;
    return true;
}

fn r_other_suffix(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var among_var: i32 = 0;
    const v_1 = env.limit - env.cursor;
    lab0: while (true) {
        env.ket = env.cursor;
        if (!env.eqSB("st")) {
            break :lab0;
        }
        env.bra = env.cursor;
        if (!env.eqSB("ig")) {
            break :lab0;
        }
        env.sliceDel() catch return false;
        break :lab0;
    }
    env.cursor = env.limit - v_1;
    if (@as(i32, @intCast(env.cursor)) < context.i_p1) {
        return false;
    }
    const v_2 = env.limit_backward;
    env.limit_backward = @intCast(@as(u32, @intCast(context.i_p1)));
    env.ket = env.cursor;
    among_var = env.findAmongB(&a_2, @as(*anyopaque, @ptrCast(context)));
    if (among_var == 0) {
        env.limit_backward = v_2;
        return false;
    }
    env.bra = env.cursor;
    env.limit_backward = v_2;
    switch (among_var) {
        1 => {
            env.sliceDel() catch return false;
            const v_3 = env.limit - env.cursor;
            _ = r_consonant_pair(env, @ptrCast(context));
            env.cursor = env.limit - v_3;
        },
        2 => {
            env.sliceFrom("løs") catch return false;
        },
        else => {},
    }
    return true;
}

fn r_undouble(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    if (@as(i32, @intCast(env.cursor)) < context.i_p1) {
        return false;
    }
    const v_1 = env.limit_backward;
    env.limit_backward = @intCast(@as(u32, @intCast(context.i_p1)));
    env.ket = env.cursor;
    if (!env.inGroupingB(&G_c, 98, 122)) {
        env.limit_backward = v_1;
        return false;
    }
    env.bra = env.cursor;
    context.S_ch.assign(env.allocator, env.sliceTo()) catch return false;
    env.limit_backward = v_1;
    if (!env.eqSB(context.S_ch.slice())) {
        return false;
    }
    env.sliceDel() catch return false;
    return true;
}

pub fn stem(env: *snowball.Env) bool {
    var context_val = Context{};
    const context = &context_val;
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    defer context.S_ch.deinit(env.allocator);
    const v_1 = env.cursor;
    _ = r_mark_regions(env, @ptrCast(context));
    env.cursor = v_1;
    env.limit_backward = env.cursor;
    env.cursor = env.limit;
    const v_2 = env.limit - env.cursor;
    _ = r_main_suffix(env, @ptrCast(context));
    env.cursor = env.limit - v_2;
    const v_3 = env.limit - env.cursor;
    _ = r_consonant_pair(env, @ptrCast(context));
    env.cursor = env.limit - v_3;
    const v_4 = env.limit - env.cursor;
    _ = r_other_suffix(env, @ptrCast(context));
    env.cursor = env.limit - v_4;
    const v_5 = env.limit - env.cursor;
    _ = r_undouble(env, @ptrCast(context));
    env.cursor = env.limit - v_5;
    env.cursor = env.limit_backward;
    return true;
}
