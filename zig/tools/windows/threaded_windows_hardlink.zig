// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Appended to std.Io.Threaded by the overlay generator. These operations use
// that runtime's path handling, syscall cancellation, and directory handles.

fn antflyDirHardLinkWindows(
    old_dir: Dir,
    old_path: []const u8,
    new_dir: Dir,
    new_path: []const u8,
    options: Dir.HardLinkOptions,
) Dir.HardLinkError!void {
    const path_buf = try sliceToPrefixedFileW(old_dir.handle, old_path, .{});
    const handle = OpenFile(path_buf.span(), .{
        .dir = old_dir.handle,
        .access_mask = .{ .STANDARD = .{ .SYNCHRONIZE = true } },
        .creation = .OPEN,
        .follow_symlinks = options.follow_symlinks,
    }) catch |err| switch (err) {
        error.IsDir => return error.PermissionDenied,
        error.NoDevice, error.NetworkNotFound => return error.FileNotFound,
        error.PipeBusy => return error.OperationUnsupported,
        error.FileBusy, error.AntivirusInterference => return error.AccessDenied,
        error.WouldBlock => unreachable,
        else => |e| return e,
    };
    defer windows.CloseHandle(handle);
    return antflyFileHardLinkWindows(handle, new_dir, new_path);
}

fn antflyFileHardLinkWindows(handle: windows.HANDLE, new_dir: Dir, new_path: []const u8) File.HardLinkError!void {
    const path_buf = try sliceToPrefixedFileW(new_dir.handle, new_path, .{});
    const path = path_buf.span();
    // FILE_LINK_INFORMATION's first field is a BOOLEAN (or a ULONG union
    // with FileLinkInformationEx). Zero also initializes the ABI padding.
    const LinkInformation = extern struct {
        ReplaceIfExists: windows.ULONG,
        RootDirectory: ?windows.HANDLE,
        FileNameLength: windows.ULONG,
        FileName: [windows.PATH_MAX_WIDE]windows.WCHAR,
    };
    var info: LinkInformation = .{
        .ReplaceIfExists = 0,
        .RootDirectory = if (Dir.path.isAbsoluteWindowsWtf16(path)) null else new_dir.handle,
        .FileNameLength = @intCast(path.len * @sizeOf(windows.WCHAR)),
        .FileName = undefined,
    };
    @memcpy(info.FileName[0..path.len], path);
    const minimum = std.mem.alignForward(usize, @offsetOf(LinkInformation, "FileName") + @sizeOf(windows.WCHAR), @alignOf(LinkInformation));
    const length = @max(@offsetOf(LinkInformation, "FileName") + info.FileNameLength, minimum);
    var status: windows.IO_STATUS_BLOCK = undefined;
    const syscall: Syscall = try .start();
    while (true) {
        const rc = windows.ntdll.NtSetInformationFile(handle, &status, &info, @intCast(length), .Link);
        switch (rc) {
            .SUCCESS => return syscall.finish(),
            .CANCELLED => try syscall.checkCancel(),
            .ACCESS_DENIED, .SHARING_VIOLATION => return syscall.fail(error.AccessDenied),
            .NO_SUCH_FILE, .OBJECT_NAME_NOT_FOUND, .OBJECT_PATH_NOT_FOUND, .DELETE_PENDING => return syscall.fail(error.FileNotFound),
            .OBJECT_NAME_COLLISION => return syscall.fail(error.PathAlreadyExists),
            .NOT_SAME_DEVICE => return syscall.fail(error.CrossDevice),
            .NOT_A_DIRECTORY => return syscall.fail(error.NotDir),
            .FILE_IS_A_DIRECTORY => return syscall.fail(error.PermissionDenied),
            .DISK_FULL => return syscall.fail(error.NoSpaceLeft),
            .QUOTA_EXCEEDED, .DISK_QUOTA_EXCEEDED => return syscall.fail(error.DiskQuota),
            .MEDIA_WRITE_PROTECTED => return syscall.fail(error.ReadOnlyFileSystem),
            .TOO_MANY_LINKS => return syscall.fail(error.LinkQuotaExceeded),
            .INSUFFICIENT_RESOURCES, .NO_MEMORY => return syscall.fail(error.SystemResources),
            .NOT_SUPPORTED, .INVALID_DEVICE_REQUEST, .NOT_IMPLEMENTED, .INVALID_INFO_CLASS => return syscall.fail(error.OperationUnsupported),
            .OBJECT_NAME_INVALID => return syscall.fail(error.BadPathName),
            else => return syscall.unexpectedNtstatus(rc),
        }
    }
}
