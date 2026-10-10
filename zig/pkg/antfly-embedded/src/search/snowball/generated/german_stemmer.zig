//! Generated from german.sbl by Snowball 3.0.0 - https://snowballstem.org/

const snowball = @import("env.zig");

fn suppress_any_unused_warning(ctx: *anyopaque) void {
    _ = ctx;
}

const a_0 = [_]snowball.Among{
    snowball.Among{ .s = "", .substring_i = -1, .result = 5, .method = null },
    snowball.Among{ .s = "ae", .substring_i = 0, .result = 2, .method = null },
    snowball.Among{ .s = "oe", .substring_i = 0, .result = 3, .method = null },
    snowball.Among{ .s = "qu", .substring_i = 0, .result = -1, .method = null },
    snowball.Among{ .s = "ue", .substring_i = 0, .result = 4, .method = null },
    snowball.Among{ .s = "ß", .substring_i = 0, .result = 1, .method = null },
};

const a_1 = [_]snowball.Among{
    snowball.Among{ .s = "", .substring_i = -1, .result = 5, .method = null },
    snowball.Among{ .s = "U", .substring_i = 0, .result = 2, .method = null },
    snowball.Among{ .s = "Y", .substring_i = 0, .result = 1, .method = null },
    snowball.Among{ .s = "ä", .substring_i = 0, .result = 3, .method = null },
    snowball.Among{ .s = "ö", .substring_i = 0, .result = 4, .method = null },
    snowball.Among{ .s = "ü", .substring_i = 0, .result = 2, .method = null },
};

const a_2 = [_]snowball.Among{
    snowball.Among{ .s = "e", .substring_i = -1, .result = 3, .method = null },
    snowball.Among{ .s = "em", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "en", .substring_i = -1, .result = 3, .method = null },
    snowball.Among{ .s = "erinnen", .substring_i = 2, .result = 2, .method = null },
    snowball.Among{ .s = "erin", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "ln", .substring_i = -1, .result = 5, .method = null },
    snowball.Among{ .s = "ern", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "er", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "s", .substring_i = -1, .result = 4, .method = null },
    snowball.Among{ .s = "es", .substring_i = 8, .result = 3, .method = null },
    snowball.Among{ .s = "lns", .substring_i = 8, .result = 5, .method = null },
};

const a_3 = [_]snowball.Among{
    snowball.Among{ .s = "tick", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "plan", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "geordn", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "intern", .substring_i = -1, .result = -1, .method = null },
    snowball.Among{ .s = "tr", .substring_i = -1, .result = -1, .method = null },
};

const a_4 = [_]snowball.Among{
    snowball.Among{ .s = "en", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "er", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "et", .substring_i = -1, .result = 3, .method = null },
    snowball.Among{ .s = "st", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "est", .substring_i = 3, .result = 1, .method = null },
};

const a_5 = [_]snowball.Among{
    snowball.Among{ .s = "ig", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "lich", .substring_i = -1, .result = 1, .method = null },
};

const a_6 = [_]snowball.Among{
    snowball.Among{ .s = "end", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "ig", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "ung", .substring_i = -1, .result = 1, .method = null },
    snowball.Among{ .s = "lich", .substring_i = -1, .result = 3, .method = null },
    snowball.Among{ .s = "isch", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "ik", .substring_i = -1, .result = 2, .method = null },
    snowball.Among{ .s = "heit", .substring_i = -1, .result = 3, .method = null },
    snowball.Among{ .s = "keit", .substring_i = -1, .result = 4, .method = null },
};

const G_v = [_]u8{ 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32, 8 };

const G_et_ending = [_]u8{ 1, 128, 198, 227, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128 };

const G_s_ending = [_]u8{ 117, 30, 5 };

const G_st_ending = [_]u8{ 117, 30, 4 };

const Context = struct {
    i_p2: i32 = 0,
    i_p1: i32 = 0,
};

fn r_prelude(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var among_var: i32 = 0;
    const v_1 = env.cursor;
    replab0: while (true) {
        const v_2 = env.cursor;
        lab1: while (true) {
            golab2: while (true) {
                const v_3 = env.cursor;
                lab3: while (true) {
                    if (!env.inGrouping(&G_v, 97, 252)) {
                        break :lab3;
                    }
                    env.bra = env.cursor;
                    lab4: while (true) {
                        const v_4 = env.cursor;
                        lab5: while (true) {
                            if (!env.eqS("u")) {
                                break :lab5;
                            }
                            env.ket = env.cursor;
                            if (!env.inGrouping(&G_v, 97, 252)) {
                                break :lab5;
                            }
                            env.sliceFrom("U") catch return false;
                            break :lab4;
                        }
                        env.cursor = v_4;
                        if (!env.eqS("y")) {
                            break :lab3;
                        }
                        env.ket = env.cursor;
                        if (!env.inGrouping(&G_v, 97, 252)) {
                            break :lab3;
                        }
                        env.sliceFrom("Y") catch return false;
                        break :lab4;
                    }
                    env.cursor = v_3;
                    break :golab2;
                }
                env.cursor = v_3;
                if (env.cursor >= env.limit) {
                    break :lab1;
                }
                env.nextChar();
            }
            continue :replab0;
        }
        env.cursor = v_2;
        break :replab0;
    }
    env.cursor = v_1;
    replab6: while (true) {
        const v_5 = env.cursor;
        lab7: while (true) {
            env.bra = env.cursor;
            among_var = env.findAmong(&a_0, @as(*anyopaque, @ptrCast(context)));
            env.ket = env.cursor;
            switch (among_var) {
                1 => {
                    env.sliceFrom("ss") catch return false;
                },
                2 => {
                    env.sliceFrom("ä") catch return false;
                },
                3 => {
                    env.sliceFrom("ö") catch return false;
                },
                4 => {
                    env.sliceFrom("ü") catch return false;
                },
                5 => {
                    if (env.cursor >= env.limit) {
                        break :lab7;
                    }
                    env.nextChar();
                },
                else => {},
            }
            continue :replab6;
        }
        env.cursor = v_5;
        break :replab6;
    }
    return true;
}

fn r_mark_regions(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var i_x: i32 = 0;
    context.i_p1 = @as(i32, @intCast(env.limit));
    context.i_p2 = @as(i32, @intCast(env.limit));
    const v_1 = env.cursor;
    if (!env.hop(3)) {
        return false;
    }
    i_x = @as(i32, @intCast(env.cursor));
    env.cursor = v_1;
    if (!env.goOutGrouping(&G_v, 97, 252)) {
        return false;
    }
    env.nextChar();
    if (!env.goInGrouping(&G_v, 97, 252)) {
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
    if (!env.goOutGrouping(&G_v, 97, 252)) {
        return false;
    }
    env.nextChar();
    if (!env.goInGrouping(&G_v, 97, 252)) {
        return false;
    }
    env.nextChar();
    context.i_p2 = @as(i32, @intCast(env.cursor));
    return true;
}

fn r_postlude(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var among_var: i32 = 0;
    replab0: while (true) {
        const v_1 = env.cursor;
        lab1: while (true) {
            env.bra = env.cursor;
            among_var = env.findAmong(&a_1, @as(*anyopaque, @ptrCast(context)));
            env.ket = env.cursor;
            switch (among_var) {
                1 => {
                    env.sliceFrom("y") catch return false;
                },
                2 => {
                    env.sliceFrom("u") catch return false;
                },
                3 => {
                    env.sliceFrom("a") catch return false;
                },
                4 => {
                    env.sliceFrom("o") catch return false;
                },
                5 => {
                    if (env.cursor >= env.limit) {
                        break :lab1;
                    }
                    env.nextChar();
                },
                else => {},
            }
            continue :replab0;
        }
        env.cursor = v_1;
        break :replab0;
    }
    return true;
}

fn r_R1(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    return context.i_p1 <= @as(i32, @intCast(env.cursor));
}

fn r_R2(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    return context.i_p2 <= @as(i32, @intCast(env.cursor));
}

fn r_standard_suffix(env: *snowball.Env, ctx: *anyopaque) bool {
    const context: *Context = @ptrCast(@alignCast(ctx));
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    var among_var: i32 = 0;
    const v_1 = env.limit - env.cursor;
    lab0: while (true) {
        env.ket = env.cursor;
        among_var = env.findAmongB(&a_2, @as(*anyopaque, @ptrCast(context)));
        if (among_var == 0) {
            break :lab0;
        }
        env.bra = env.cursor;
        if (!r_R1(env, @ptrCast(context))) {
            break :lab0;
        }
        switch (among_var) {
            1 => {
                const v_2 = env.limit - env.cursor;
                lab1: while (true) {
                    if (!env.eqSB("syst")) {
                        break :lab1;
                    }
                    break :lab0;
                }
                env.cursor = env.limit - v_2;
                env.sliceDel() catch return false;
            },
            2 => {
                env.sliceDel() catch return false;
            },
            3 => {
                env.sliceDel() catch return false;
                const v_3 = env.limit - env.cursor;
                lab2: while (true) {
                    env.ket = env.cursor;
                    if (!env.eqSB("s")) {
                        env.cursor = env.limit - v_3;
                        break :lab2;
                    }
                    env.bra = env.cursor;
                    if (!env.eqSB("nis")) {
                        env.cursor = env.limit - v_3;
                        break :lab2;
                    }
                    env.sliceDel() catch return false;
                    break :lab2;
                }
            },
            4 => {
                if (!env.inGroupingB(&G_s_ending, 98, 116)) {
                    break :lab0;
                }
                env.sliceDel() catch return false;
            },
            5 => {
                env.sliceFrom("l") catch return false;
            },
            else => {},
        }
        break :lab0;
    }
    env.cursor = env.limit - v_1;
    const v_4 = env.limit - env.cursor;
    lab3: while (true) {
        env.ket = env.cursor;
        among_var = env.findAmongB(&a_4, @as(*anyopaque, @ptrCast(context)));
        if (among_var == 0) {
            break :lab3;
        }
        env.bra = env.cursor;
        if (!r_R1(env, @ptrCast(context))) {
            break :lab3;
        }
        switch (among_var) {
            1 => {
                env.sliceDel() catch return false;
            },
            2 => {
                if (!env.inGroupingB(&G_st_ending, 98, 116)) {
                    break :lab3;
                }
                if (!env.hopBack(3)) {
                    break :lab3;
                }
                env.sliceDel() catch return false;
            },
            3 => {
                const v_5 = env.limit - env.cursor;
                if (!env.inGroupingB(&G_et_ending, 85, 228)) {
                    break :lab3;
                }
                env.cursor = env.limit - v_5;
                const v_6 = env.limit - env.cursor;
                lab4: while (true) {
                    if (env.findAmongB(&a_3, @as(*anyopaque, @ptrCast(context))) == 0) {
                        break :lab4;
                    }
                    break :lab3;
                }
                env.cursor = env.limit - v_6;
                env.sliceDel() catch return false;
            },
            else => {},
        }
        break :lab3;
    }
    env.cursor = env.limit - v_4;
    const v_7 = env.limit - env.cursor;
    lab5: while (true) {
        env.ket = env.cursor;
        among_var = env.findAmongB(&a_6, @as(*anyopaque, @ptrCast(context)));
        if (among_var == 0) {
            break :lab5;
        }
        env.bra = env.cursor;
        if (!r_R2(env, @ptrCast(context))) {
            break :lab5;
        }
        switch (among_var) {
            1 => {
                env.sliceDel() catch return false;
                const v_8 = env.limit - env.cursor;
                lab6: while (true) {
                    env.ket = env.cursor;
                    if (!env.eqSB("ig")) {
                        env.cursor = env.limit - v_8;
                        break :lab6;
                    }
                    env.bra = env.cursor;
                    const v_9 = env.limit - env.cursor;
                    lab7: while (true) {
                        if (!env.eqSB("e")) {
                            break :lab7;
                        }
                        env.cursor = env.limit - v_8;
                        break :lab6;
                    }
                    env.cursor = env.limit - v_9;
                    if (!r_R2(env, @ptrCast(context))) {
                        env.cursor = env.limit - v_8;
                        break :lab6;
                    }
                    env.sliceDel() catch return false;
                    break :lab6;
                }
            },
            2 => {
                const v_10 = env.limit - env.cursor;
                lab8: while (true) {
                    if (!env.eqSB("e")) {
                        break :lab8;
                    }
                    break :lab5;
                }
                env.cursor = env.limit - v_10;
                env.sliceDel() catch return false;
            },
            3 => {
                env.sliceDel() catch return false;
                const v_11 = env.limit - env.cursor;
                lab9: while (true) {
                    env.ket = env.cursor;
                    lab10: while (true) {
                        const v_12 = env.limit - env.cursor;
                        lab11: while (true) {
                            if (!env.eqSB("er")) {
                                break :lab11;
                            }
                            break :lab10;
                        }
                        env.cursor = env.limit - v_12;
                        if (!env.eqSB("en")) {
                            env.cursor = env.limit - v_11;
                            break :lab9;
                        }
                        break :lab10;
                    }
                    env.bra = env.cursor;
                    if (!r_R1(env, @ptrCast(context))) {
                        env.cursor = env.limit - v_11;
                        break :lab9;
                    }
                    env.sliceDel() catch return false;
                    break :lab9;
                }
            },
            4 => {
                env.sliceDel() catch return false;
                const v_13 = env.limit - env.cursor;
                lab12: while (true) {
                    env.ket = env.cursor;
                    if (env.findAmongB(&a_5, @as(*anyopaque, @ptrCast(context))) == 0) {
                        env.cursor = env.limit - v_13;
                        break :lab12;
                    }
                    env.bra = env.cursor;
                    if (!r_R2(env, @ptrCast(context))) {
                        env.cursor = env.limit - v_13;
                        break :lab12;
                    }
                    env.sliceDel() catch return false;
                    break :lab12;
                }
            },
            else => {},
        }
        break :lab5;
    }
    env.cursor = env.limit - v_7;
    return true;
}

pub fn stem(env: *snowball.Env) bool {
    var context_val = Context{};
    const context = &context_val;
    suppress_any_unused_warning(@as(*anyopaque, @ptrCast(context)));
    const v_1 = env.cursor;
    _ = r_prelude(env, @ptrCast(context));
    env.cursor = v_1;
    const v_2 = env.cursor;
    _ = r_mark_regions(env, @ptrCast(context));
    env.cursor = v_2;
    env.limit_backward = env.cursor;
    env.cursor = env.limit;
    _ = r_standard_suffix(env, @ptrCast(context));
    env.cursor = env.limit_backward;
    const v_3 = env.cursor;
    _ = r_postlude(env, @ptrCast(context));
    env.cursor = v_3;
    return true;
}
