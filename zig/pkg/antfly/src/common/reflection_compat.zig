pub const Field = struct {
    name: [:0]const u8,
    type: type = void,
    value: comptime_int = 0,
    is_comptime: bool = false,
    default_value_ptr: ?*const anyopaque = null,
};

pub fn fields(comptime T: type) [fieldCount(T)]Field {
    const info = @typeInfo(T);
    var result: [fieldCount(T)]Field = undefined;
    switch (info) {
        .@"enum" => |e| {
            for (e.field_names, e.field_values, 0..) |name, value, i| result[i] = .{ .name = name, .value = value };
        },
        .@"struct" => |s| {
            for (s.field_names, s.field_types, s.field_attrs, 0..) |name, FieldType, attrs, i| result[i] = .{
                .name = name,
                .type = FieldType,
                .is_comptime = attrs.@"comptime",
                .default_value_ptr = attrs.default_value_ptr,
            };
        },
        .@"union" => |u| {
            for (u.field_names, u.field_types, 0..) |name, FieldType, i| result[i] = .{ .name = name, .type = FieldType };
        },
        else => unreachable,
    }
    return result;
}

fn fieldCount(comptime T: type) usize {
    return switch (@typeInfo(T)) {
        .@"enum" => |info| info.field_names.len,
        .@"struct" => |info| info.field_names.len,
        .@"union" => |info| info.field_names.len,
        else => @compileError("fields requires a struct, enum, or union"),
    };
}
