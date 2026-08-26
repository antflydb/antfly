// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic in-memory stream and datagram network for VoprIo.
//!
//! Writes enqueue logical packets. Packet delivery, including eligible
//! reordering/drop/duplication, is selected by the VOPR scheduler. Blocking
//! accept/read/backpressure parks the calling fiber on stable resource IDs.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const transition = @import("transition.zig");

pub const Config = struct {
    max_sockets: usize = 4096,
    stream_capacity: usize = 1024 * 1024,
};

pub const Faults = struct {
    network_down: bool = false,
    delivery_paused: bool = false,
    partition_source: ?std.Io.net.Socket.Handle = null,
    partition_destination: ?std.Io.net.Socket.Handle = null,
    drop_next: bool = false,
    duplicate_next: bool = false,
    reorder: bool = false,
    partial_write_limit: ?usize = null,
};

pub const WaitPort = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        wait: *const fn (*anyopaque, ids.StableId) anyerror!void,
        wake: *const fn (*anyopaque, ids.StableId, u32) anyerror!void,
        ready: ?*const fn (*anyopaque, ids.StableId) anyerror!void = null,
    };

    pub fn wait(self: WaitPort, resource_id: ids.StableId) !void {
        return self.vtable.wait(self.ptr, resource_id);
    }

    pub fn wake(self: WaitPort, resource_id: ids.StableId, max_waiters: u32) !void {
        return self.vtable.wake(self.ptr, resource_id, max_waiters);
    }

    /// Park and publish one scheduler-visible readiness completion atomically.
    /// Modeled resources use this when readiness existed before the consumer
    /// called wait, keeping producer-first and consumer-first arrival orders
    /// on the same explicit completion path.
    pub fn ready(self: WaitPort, resource_id: ids.StableId) !void {
        const ready_fn = self.vtable.ready orelse return error.ReadyWaitUnsupported;
        return ready_fn(self.ptr, resource_id);
    }
};

const ListenerAddress = union(enum) {
    ip: std.Io.net.IpAddress,
    unix: []u8,
};

const SocketKind = enum { listener, stream, datagram };

const Datagram = struct {
    from: std.Io.net.IpAddress,
    bytes: []u8,
};

const SocketState = struct {
    handle: std.Io.net.Socket.Handle,
    id: ids.StableId,
    kind: SocketKind,
    address: std.Io.net.IpAddress,
    listener_address: ?ListenerAddress = null,
    backlog: usize = 0,
    pending_accept: std.ArrayListUnmanaged(std.Io.net.Socket.Handle) = .empty,
    peer: ?std.Io.net.Socket.Handle = null,
    receive: std.ArrayListUnmanaged(u8) = .empty,
    datagrams: std.ArrayListUnmanaged(Datagram) = .empty,
    read_open: bool = true,
    write_open: bool = true,
    peer_write_open: bool = true,
    /// Unlike an orderly peer FIN, a hard abort means the peer abandoned both
    /// directions and an in-flight application request may be canceled even
    /// when pipelined bytes remain unread.
    peer_hard_disconnected: bool = false,
    closed: bool = false,
    next_packet_sequence: u64 = 1,

    fn readResource(self: *const SocketState) ids.StableId {
        return ids.derive("sim-io.socket-read", self.id, 0);
    }

    fn writeResource(self: *const SocketState) ids.StableId {
        return ids.derive("sim-io.socket-write", self.id, 0);
    }

    fn acceptResource(self: *const SocketState) ids.StableId {
        return ids.derive("sim-io.socket-accept", self.id, 0);
    }
};

const Packet = struct {
    id: ids.StableId,
    sequence: u64,
    source: std.Io.net.Socket.Handle,
    destination: std.Io.net.Socket.Handle,
    bytes: []u8,
    drop: bool,
    duplicate: bool,
    close_write: bool = false,
    datagram: bool = false,
};

const IdentitySequence = struct {
    parent: ids.StableId,
    next_sequence: u64 = 1,
};

pub const Network = struct {
    allocator: std.mem.Allocator,
    config: Config,
    wait_port: ?WaitPort = null,
    next_handle: std.Io.net.Socket.Handle = 0x5000_0000,
    next_pair_sequence: u64 = 1,
    next_ephemeral_port: u16 = 20_000,
    sockets: std.AutoHashMapUnmanaged(std.Io.net.Socket.Handle, *SocketState) = .empty,
    socket_order: std.ArrayListUnmanaged(*SocketState) = .empty,
    identity_sequences: std.ArrayListUnmanaged(IdentitySequence) = .empty,
    open_sockets: usize = 0,
    packets: std.ArrayListUnmanaged(Packet) = .empty,
    queued_bytes: usize = 0,
    faults: Faults = .{},

    pub fn init(allocator: std.mem.Allocator, config: Config) !Network {
        if (config.max_sockets == 0) return error.InvalidVoprIoSocketLimit;
        if (config.stream_capacity == 0) return error.InvalidVoprIoStreamCapacity;
        return .{ .allocator = allocator, .config = config };
    }

    pub fn bindWaitPort(self: *Network, wait_port: WaitPort) void {
        self.wait_port = wait_port;
    }

    pub fn deinit(self: *Network) void {
        for (self.packets.items) |packet| if (packet.bytes.len != 0) self.allocator.free(packet.bytes);
        self.packets.deinit(self.allocator);
        self.sockets.deinit(self.allocator);
        for (self.socket_order.items) |socket_state| {
            if (socket_state.listener_address) |address| switch (address) {
                .ip => {},
                .unix => |path| self.allocator.free(path),
            };
            socket_state.pending_accept.deinit(self.allocator);
            socket_state.receive.deinit(self.allocator);
            for (socket_state.datagrams.items) |datagram| self.allocator.free(datagram.bytes);
            socket_state.datagrams.deinit(self.allocator);
            self.allocator.destroy(socket_state);
        }
        self.socket_order.deinit(self.allocator);
        self.identity_sequences.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn listenIp(self: *Network, requested: *const std.Io.net.IpAddress, options: std.Io.net.IpAddress.ListenOptions) std.Io.net.IpAddress.ListenError!std.Io.net.Socket {
        if (self.faults.network_down) return error.NetworkDown;
        if (options.mode != .stream) return error.SocketModeUnsupported;
        var address = requested.*;
        if (address.getPort() == 0) {
            address.setPort(self.allocateEphemeralPort() catch return error.AddressInUse);
        }
        if (self.findIpListener(address) != null) return error.AddressInUse;
        const listener = self.createSocket(.listener, address, ipEndpointIdentity("sim-io.ip-listener", address)) catch |err| return mapListenResourceError(err);
        listener.listener_address = .{ .ip = address };
        listener.backlog = options.kernel_backlog;
        return .{ .handle = listener.handle, .address = address };
    }

    pub fn listenUnix(self: *Network, requested: *const std.Io.net.UnixAddress, options: std.Io.net.UnixAddress.ListenOptions) std.Io.net.UnixAddress.ListenError!std.Io.net.Socket.Handle {
        if (self.faults.network_down) return error.NetworkDown;
        if (self.findUnixListener(requested.path) != null) return error.AddressInUse;
        const listener = self.createSocket(.listener, .{ .ip4 = .loopback(0) }, ids.stable("sim-io.unix-listener", requested.path)) catch |err|
            return mapUnixListenResourceError(err);
        errdefer self.destroyLastSocket(listener);
        listener.listener_address = .{ .unix = self.allocator.dupe(u8, requested.path) catch return error.SystemResources };
        listener.backlog = options.kernel_backlog;
        return listener.handle;
    }

    pub fn connectIp(self: *Network, address: *const std.Io.net.IpAddress, options: std.Io.net.IpAddress.ConnectOptions) std.Io.net.IpAddress.ConnectError!std.Io.net.Socket {
        if (self.faults.network_down) return error.NetworkDown;
        if (options.mode != .stream) return error.SocketModeUnsupported;
        const listener = self.findIpListener(address.*) orelse return error.ConnectionRefused;
        const client = self.connectToListener(listener, address.*) catch |err| return mapConnectResourceError(err);
        return .{ .handle = client.handle, .address = address.* };
    }

    pub fn bindIp(self: *Network, requested: *const std.Io.net.IpAddress, options: std.Io.net.IpAddress.BindOptions) std.Io.net.IpAddress.BindError!std.Io.net.Socket {
        if (self.faults.network_down) return error.NetworkDown;
        if (options.mode != .dgram) return error.SocketModeUnsupported;
        if (options.protocol) |protocol| if (protocol != .udp) return error.ProtocolUnsupportedBySystem;
        var address = requested.*;
        if (address.getPort() == 0)
            address.setPort(self.allocateEphemeralPort() catch return error.AddressInUse);
        if (self.findDatagram(address) != null) return error.AddressInUse;
        const socket_state = self.createSocket(.datagram, address, ipEndpointIdentity("sim-io.datagram-endpoint", address)) catch |err| return mapBindResourceError(err);
        return .{ .handle = socket_state.handle, .address = address };
    }

    pub fn connectUnix(self: *Network, address: *const std.Io.net.UnixAddress) std.Io.net.UnixAddress.ConnectError!std.Io.net.Socket.Handle {
        if (self.faults.network_down) return error.NetworkDown;
        const listener = self.findUnixListener(address.path) orelse return error.FileNotFound;
        const client = self.connectToListener(listener, .{ .ip4 = .loopback(0) }) catch |err|
            return mapUnixConnectResourceError(err);
        return client.handle;
    }

    pub fn createPair(self: *Network, options: std.Io.net.Socket.CreatePairOptions) std.Io.net.Socket.CreatePairError![2]std.Io.net.Socket {
        if (options.mode != .stream) return error.SocketModeUnsupported;
        const address: std.Io.net.IpAddress = switch (options.family) {
            .ip4 => .{ .ip4 = .loopback(0) },
            .ip6 => .{ .ip6 = .loopback(0) },
        };
        if (self.next_pair_sequence == std.math.maxInt(u64)) return error.SystemResources;
        const pair_parent = ids.derive("sim-io.socket-pair", network_id, self.next_pair_sequence);
        self.next_pair_sequence += 1;
        const left = self.createSocket(.stream, address, pair_parent) catch |err| return mapPairResourceError(err);
        errdefer self.destroyLastSocket(left);
        const right = self.createSocket(.stream, address, pair_parent) catch |err| return mapPairResourceError(err);
        left.peer = right.handle;
        right.peer = left.handle;
        return .{
            .{ .handle = left.handle, .address = address },
            .{ .handle = right.handle, .address = address },
        };
    }

    pub fn accept(self: *Network, handle: std.Io.net.Socket.Handle) std.Io.net.Server.AcceptError!std.Io.net.Socket {
        const listener = self.getSocket(handle) orelse return error.SocketNotListening;
        if (listener.kind != .listener or listener.closed) return error.SocketNotListening;
        if (listener.pending_accept.items.len != 0) {
            self.wait_port.?.ready(listener.acceptResource()) catch |err| return mapWaitAcceptError(err);
            if (listener.closed) return error.SocketNotListening;
            if (self.faults.network_down) return error.NetworkDown;
        }
        while (listener.pending_accept.items.len == 0) {
            self.wait_port.?.wait(listener.acceptResource()) catch |err| return mapWaitAcceptError(err);
            if (listener.closed) return error.SocketNotListening;
            if (self.faults.network_down) return error.NetworkDown;
        }
        const accepted_handle = listener.pending_accept.orderedRemove(0);
        const accepted = self.getSocket(accepted_handle).?;
        return .{ .handle = accepted.handle, .address = accepted.address };
    }

    pub fn read(self: *Network, handle: std.Io.net.Socket.Handle, buffers: [][]u8) std.Io.net.Stream.Reader.Error!usize {
        const socket_state = self.getSocket(handle) orelse return error.SocketUnconnected;
        if (socket_state.kind != .stream or socket_state.closed or !socket_state.read_open)
            return error.SocketUnconnected;
        while (socket_state.receive.items.len == 0) {
            if (!socket_state.peer_write_open) return 0;
            self.wait_port.?.wait(socket_state.readResource()) catch |err| return mapWaitReadError(err);
            if (socket_state.closed or !socket_state.read_open) return error.SocketUnconnected;
            if (self.faults.network_down) return error.NetworkDown;
        }
        var consumed: usize = 0;
        for (buffers) |buffer| {
            const count = @min(buffer.len, socket_state.receive.items.len - consumed);
            @memcpy(buffer[0..count], socket_state.receive.items[consumed..][0..count]);
            consumed += count;
            if (consumed == socket_state.receive.items.len or count != buffer.len) break;
        }
        std.mem.copyForwards(u8, socket_state.receive.items[0 .. socket_state.receive.items.len - consumed], socket_state.receive.items[consumed..]);
        socket_state.receive.items.len -= consumed;
        return consumed;
    }

    pub fn write(self: *Network, handle: std.Io.net.Socket.Handle, header: []const u8, buffers: []const []const u8, splat: usize) std.Io.net.Stream.Writer.Error!usize {
        const source = self.getSocket(handle) orelse return error.SocketUnconnected;
        if (source.kind != .stream or source.closed or !source.write_open or source.peer == null)
            return error.SocketUnconnected;
        if (self.faults.network_down) return error.NetworkDown;
        const destination = self.getSocket(source.peer.?) orelse return error.ConnectionResetByPeer;
        if (self.linkBlocked(source.handle, destination.handle)) return error.NetworkDown;
        if (destination.closed or !destination.read_open) return error.ConnectionResetByPeer;

        var requested = header.len;
        for (0..splat) |_| {
            for (buffers) |buffer| {
                requested = std.math.add(usize, requested, buffer.len) catch return error.SystemResources;
            }
        }
        while (self.queued_bytes >= self.config.stream_capacity) {
            self.wait_port.?.wait(source.writeResource()) catch |err| return mapWaitWriteError(err);
            if (source.closed or !source.write_open) return error.SocketUnconnected;
            if (self.faults.network_down) return error.NetworkDown;
        }
        var allowed = @min(requested, self.config.stream_capacity - self.queued_bytes);
        if (self.faults.partial_write_limit) |limit| {
            allowed = @min(allowed, limit);
            self.faults.partial_write_limit = null;
        }
        if (allowed == 0) return error.SystemResources;
        const bytes = self.allocator.alloc(u8, allowed) catch return error.SystemResources;
        errdefer self.allocator.free(bytes);
        var written = copyLimited(bytes, header, 0);
        outer: for (0..splat) |_| for (buffers) |buffer| {
            written += copyLimited(bytes, buffer, written);
            if (written == bytes.len) break :outer;
        };
        const sequence = self.allocatePacketSequence(source) catch return error.SystemResources;
        self.packets.append(self.allocator, .{
            .id = ids.derive("sim-io.packet", source.id, sequence),
            .sequence = sequence,
            .source = source.handle,
            .destination = destination.handle,
            .bytes = bytes,
            .drop = takeBool(&self.faults.drop_next),
            .duplicate = takeBool(&self.faults.duplicate_next),
        }) catch return error.SystemResources;
        self.queued_bytes += bytes.len;
        return bytes.len;
    }

    pub fn send(self: *Network, handle: std.Io.net.Socket.Handle, messages: []std.Io.net.OutgoingMessage) struct { ?std.Io.net.Socket.SendError, usize } {
        const source = self.getSocket(handle) orelse return .{ error.SocketUnconnected, 0 };
        if (source.kind != .datagram or source.closed) return .{ error.SocketUnconnected, 0 };
        if (self.faults.network_down) return .{ error.NetworkDown, 0 };
        for (messages, 0..) |*message, index| {
            const destination = self.findDatagram(message.address.*) orelse return .{ error.ConnectionRefused, index };
            if (message.data_len > self.config.stream_capacity -| self.queued_bytes)
                return .{ error.SystemResources, index };
            const bytes = self.allocator.dupe(u8, message.data_ptr[0..message.data_len]) catch
                return .{ error.SystemResources, index };
            const sequence = self.allocatePacketSequence(source) catch
                return .{ error.SystemResources, index };
            self.packets.append(self.allocator, .{
                .id = ids.derive("sim-io.datagram", source.id, sequence),
                .sequence = sequence,
                .source = source.handle,
                .destination = destination.handle,
                .bytes = bytes,
                .drop = takeBool(&self.faults.drop_next),
                .duplicate = takeBool(&self.faults.duplicate_next),
                .datagram = true,
            }) catch {
                self.allocator.free(bytes);
                return .{ error.SystemResources, index };
            };
            self.queued_bytes += bytes.len;
        }
        return .{ null, messages.len };
    }

    pub fn receiveDatagrams(
        self: *Network,
        handle: std.Io.net.Socket.Handle,
        message_buffer: []std.Io.net.IncomingMessage,
        data_buffer: []u8,
        flags: std.Io.net.ReceiveFlags,
    ) std.Io.Cancelable!std.Io.Operation.NetReceive.Result {
        const socket_state = self.getSocket(handle) orelse return .{ error.SocketUnconnected, 0 };
        if (socket_state.kind != .datagram or socket_state.closed) return .{ error.SocketUnconnected, 0 };
        while (socket_state.datagrams.items.len == 0) {
            self.wait_port.?.wait(socket_state.readResource()) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return .{ error.SystemResources, 0 },
            };
            if (socket_state.closed) return .{ error.SocketUnconnected, 0 };
            if (self.faults.network_down) return .{ error.NetworkDown, 0 };
        }
        var data_offset: usize = 0;
        var count: usize = 0;
        while (count < message_buffer.len and count < socket_state.datagrams.items.len) : (count += 1) {
            const datagram = socket_state.datagrams.items[count];
            const available = data_buffer.len - data_offset;
            const copied = @min(available, datagram.bytes.len);
            @memcpy(data_buffer[data_offset..][0..copied], datagram.bytes[0..copied]);
            const control = message_buffer[count].control;
            message_buffer[count] = .{
                .from = datagram.from,
                .data = data_buffer[data_offset..][0..copied],
                .control = control,
                .flags = .{
                    .eor = true,
                    .trunc = copied != datagram.bytes.len,
                    .ctrunc = false,
                    .oob = false,
                    .errqueue = false,
                },
            };
            data_offset += copied;
            if (copied != datagram.bytes.len or data_offset == data_buffer.len) {
                count += 1;
                break;
            }
        }
        if (!flags.peek) {
            for (0..count) |_| {
                const removed = socket_state.datagrams.orderedRemove(0);
                self.allocator.free(removed.bytes);
            }
        }
        return .{ null, count };
    }

    pub fn shutdown(self: *Network, handle: std.Io.net.Socket.Handle, how: std.Io.net.ShutdownHow) std.Io.net.ShutdownError!void {
        const socket_state = self.getSocket(handle) orelse return error.SocketUnconnected;
        switch (how) {
            .recv => socket_state.read_open = false,
            .send => self.closeWrite(socket_state) catch return error.Unexpected,
            .both => {
                socket_state.read_open = false;
                self.closeWrite(socket_state) catch return error.Unexpected;
            },
        }
    }

    pub fn close(self: *Network, handles: []const std.Io.net.Socket.Handle) bool {
        var valid = true;
        for (handles) |handle| {
            const socket_state = self.getSocket(handle) orelse {
                valid = false;
                continue;
            };
            if (socket_state.closed) {
                valid = false;
                continue;
            }
            socket_state.closed = true;
            std.debug.assert(self.open_sockets > 0);
            self.open_sockets -= 1;
            socket_state.read_open = false;
            self.closeWrite(socket_state) catch {};
            self.wait_port.?.wake(socket_state.readResource(), std.math.maxInt(u32)) catch {};
            self.wait_port.?.wake(socket_state.acceptResource(), std.math.maxInt(u32)) catch {};
        }
        return valid;
    }

    pub fn abort(self: *Network, handle: std.Io.net.Socket.Handle) !void {
        const socket_state = self.getSocket(handle) orelse return error.SocketUnconnected;
        if (socket_state.closed) return error.SocketUnconnected;
        socket_state.closed = true;
        std.debug.assert(self.open_sockets > 0);
        self.open_sockets -= 1;
        socket_state.read_open = false;
        socket_state.write_open = false;
        if (socket_state.peer) |peer_handle| if (self.getSocket(peer_handle)) |peer| {
            if (!peer.closed) {
                peer.peer_write_open = false;
                peer.peer_hard_disconnected = true;
                try self.wait_port.?.wake(peer.readResource(), std.math.maxInt(u32));
                try self.wait_port.?.wake(peer.writeResource(), std.math.maxInt(u32));
            }
        };
        try self.wait_port.?.wake(socket_state.readResource(), std.math.maxInt(u32));
    }

    pub fn peerHardDisconnected(self: *const Network, handle: std.Io.net.Socket.Handle) bool {
        const socket_state = self.getSocket(handle) orelse return true;
        return socket_state.peer_hard_disconnected;
    }

    pub fn enumerateReady(self: *const Network, list: *transition.List, allocator: std.mem.Allocator) !void {
        if (self.faults.network_down or self.faults.delivery_paused) return;
        for (self.packets.items, 0..) |packet, index| {
            if (self.linkBlocked(packet.source, packet.destination)) continue;
            const has_earlier = self.hasEarlierPacketForLink(index, packet);
            // Datagram order is observable and scheduler-controlled. TCP may
            // reorder packets internally, but the socket API exposes an ordered
            // byte stream, including FIN after all preceding bytes.
            if (has_earlier and (!self.faults.reorder or !packet.datagram)) continue;
            try list.append(allocator, .{
                .id = packet.id,
                .name = packetName(packet),
                .kind = .scheduler,
                .actor_id = self.getSocket(packet.source).?.id,
                .resource_id = self.getSocket(packet.destination).?.id,
                .parameter = @intCast(packet.bytes.len),
                .semantic_digest = if (packet.close_write) 0 else ids.digest(packet.bytes),
            });
        }
    }

    pub fn executeReady(self: *Network, transition_id: ids.StableId, sink: *event.Sink, allocator: std.mem.Allocator) !bool {
        for (self.packets.items, 0..) |packet, index| {
            if (packet.id != transition_id) continue;
            const packet_len = packet.bytes.len;
            const destination = self.getSocket(packet.destination);
            const source = self.getSocket(packet.source);
            if (packet.drop and !packet.datagram and !packet.close_write) {
                // Model a lost TCP packet as a scheduler-visible retransmission
                // delay. The receiver must never observe a hole in the stream.
                self.packets.items[index].drop = false;
                try sink.emit(allocator, .{
                    .id = ids.derive("sim-io.packet-event", packet.id, 1),
                    .kind = .injected_error,
                    .actor_id = if (source) |s| s.id else 0,
                    .resource_id = if (destination) |d| d.id else 0,
                    .payload_digest = @intCast(packet_len),
                    .name = packetName(packet),
                });
                return true;
            }
            if (packet.close_write) {
                if (destination != null and !destination.?.closed) {
                    destination.?.peer_write_open = false;
                    try self.wait_port.?.wake(destination.?.readResource(), 1);
                }
            } else if (!packet.drop and destination != null and !destination.?.closed and destination.?.read_open) {
                if (packet.datagram) {
                    const source_address = if (source) |source_state| source_state.address else std.Io.net.IpAddress{ .ip4 = .loopback(0) };
                    const initial_count = destination.?.datagrams.items.len;
                    errdefer while (destination.?.datagrams.items.len > initial_count) {
                        const removed = destination.?.datagrams.pop().?;
                        self.allocator.free(removed.bytes);
                    };
                    try self.appendDatagram(destination.?, source_address, packet.bytes);
                    if (packet.duplicate) try self.appendDatagram(destination.?, source_address, packet.bytes);
                } else {
                    try destination.?.receive.appendSlice(self.allocator, packet.bytes);
                    // Duplicate TCP packets are removed by stream reassembly.
                }
                try self.wait_port.?.wake(destination.?.readResource(), 1);
            }
            if (source) |source_state| try self.wait_port.?.wake(source_state.writeResource(), 1);
            self.queued_bytes -= packet_len;
            if (packet.bytes.len != 0) self.allocator.free(packet.bytes);
            _ = self.packets.orderedRemove(index);
            try sink.emit(allocator, .{
                .id = ids.derive("sim-io.packet-event", packet.id, @intFromBool(packet.drop)),
                .kind = if (packet.drop and !packet.close_write) .injected_error else .domain,
                .actor_id = if (source) |s| s.id else 0,
                .resource_id = if (destination) |d| d.id else 0,
                .payload_digest = @intCast(packet_len),
                .name = packetName(packet),
            });
            return true;
        }
        return false;
    }

    pub fn isQuiescent(self: *const Network) bool {
        return self.packets.items.len == 0;
    }

    pub fn openSocketCount(self: *const Network) usize {
        return self.open_sockets;
    }

    fn connectToListener(self: *Network, listener: *SocketState, address: std.Io.net.IpAddress) !*SocketState {
        if (listener.pending_accept.items.len >= listener.backlog) return error.ConnectionRefused;
        const client = try self.createSocket(.stream, address, listener.id);
        const server = self.createSocket(.stream, address, listener.id) catch |err| {
            self.destroyLastSocket(client);
            return err;
        };
        client.peer = server.handle;
        server.peer = client.handle;
        listener.pending_accept.append(self.allocator, server.handle) catch |err| {
            self.destroyLastSocket(server);
            self.destroyLastSocket(client);
            return err;
        };
        try self.wait_port.?.wake(listener.acceptResource(), 1);
        return client;
    }

    fn createSocket(self: *Network, kind: SocketKind, address: std.Io.net.IpAddress, identity_parent: ids.StableId) !*SocketState {
        if (self.open_sockets >= self.config.max_sockets) return error.ProcessFdQuotaExceeded;
        if (self.next_handle == std.math.maxInt(std.Io.net.Socket.Handle)) return error.ProcessFdQuotaExceeded;
        const socket_state = try self.allocator.create(SocketState);
        errdefer self.allocator.destroy(socket_state);
        const handle = self.next_handle;
        self.next_handle += 1;
        const sequence = try self.allocateIdentitySequence(identity_parent);
        socket_state.* = .{
            .handle = handle,
            .id = ids.derive("sim-io.socket", identity_parent, sequence),
            .kind = kind,
            .address = address,
        };
        try self.socket_order.append(self.allocator, socket_state);
        errdefer _ = self.socket_order.pop();
        try self.sockets.put(self.allocator, handle, socket_state);
        self.open_sockets += 1;
        return socket_state;
    }

    fn destroyLastSocket(self: *Network, socket_state: *SocketState) void {
        std.debug.assert(self.socket_order.getLast() == socket_state);
        _ = self.socket_order.pop();
        _ = self.sockets.remove(socket_state.handle);
        std.debug.assert(!socket_state.closed);
        std.debug.assert(self.open_sockets > 0);
        self.open_sockets -= 1;
        socket_state.pending_accept.deinit(self.allocator);
        socket_state.receive.deinit(self.allocator);
        for (socket_state.datagrams.items) |datagram| self.allocator.free(datagram.bytes);
        socket_state.datagrams.deinit(self.allocator);
        self.allocator.destroy(socket_state);
    }

    fn closeWrite(self: *Network, socket_state: *SocketState) !void {
        if (!socket_state.write_open) return;
        socket_state.write_open = false;
        if (socket_state.peer) |peer_handle| if (self.getSocket(peer_handle)) |peer| {
            if (peer.closed) return;
            const sequence = try self.allocatePacketSequence(socket_state);
            try self.packets.append(self.allocator, .{
                .id = ids.derive("sim-io.stream-fin", socket_state.id, sequence),
                .sequence = sequence,
                .source = socket_state.handle,
                .destination = peer.handle,
                .bytes = &.{},
                .drop = false,
                .duplicate = false,
                .close_write = true,
            });
        };
    }

    fn findIpListener(self: *const Network, address: std.Io.net.IpAddress) ?*SocketState {
        for (self.socket_order.items) |socket_state| {
            if (socket_state.closed or socket_state.kind != .listener) continue;
            const listener_address = socket_state.listener_address orelse continue;
            switch (listener_address) {
                .ip => |candidate| if (ipAddressMatches(candidate, address)) return socket_state,
                .unix => {},
            }
        }
        return null;
    }

    fn appendDatagram(self: *Network, destination: *SocketState, from: std.Io.net.IpAddress, bytes: []const u8) !void {
        const owned = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(owned);
        try destination.datagrams.append(self.allocator, .{ .from = from, .bytes = owned });
    }

    fn findDatagram(self: *const Network, address: std.Io.net.IpAddress) ?*SocketState {
        for (self.socket_order.items) |socket_state| {
            if (socket_state.closed or socket_state.kind != .datagram) continue;
            if (ipAddressMatches(socket_state.address, address)) return socket_state;
        }
        return null;
    }

    fn findUnixListener(self: *const Network, path: []const u8) ?*SocketState {
        for (self.socket_order.items) |socket_state| {
            if (socket_state.closed or socket_state.kind != .listener) continue;
            const listener_address = socket_state.listener_address orelse continue;
            switch (listener_address) {
                .ip => {},
                .unix => |candidate| if (std.mem.eql(u8, candidate, path)) return socket_state,
            }
        }
        return null;
    }

    fn hasEarlierPacketForLink(self: *const Network, index: usize, packet: Packet) bool {
        for (self.packets.items[0..index]) |earlier| {
            if (earlier.source == packet.source and earlier.destination == packet.destination) return true;
        }
        return false;
    }

    fn linkBlocked(self: *const Network, source: std.Io.net.Socket.Handle, destination: std.Io.net.Socket.Handle) bool {
        const blocked_source = self.faults.partition_source orelse return false;
        const blocked_destination = self.faults.partition_destination orelse return false;
        return source == blocked_source and destination == blocked_destination;
    }

    fn getSocket(self: *const Network, handle: std.Io.net.Socket.Handle) ?*SocketState {
        return self.sockets.get(handle);
    }

    fn allocateIdentitySequence(self: *Network, parent: ids.StableId) !u64 {
        for (self.identity_sequences.items) |*identity| {
            if (identity.parent != parent) continue;
            if (identity.next_sequence == std.math.maxInt(u64)) return error.VoprIoSequenceExhausted;
            const result = identity.next_sequence;
            identity.next_sequence += 1;
            return result;
        }
        try self.identity_sequences.append(self.allocator, .{ .parent = parent, .next_sequence = 2 });
        return 1;
    }

    fn allocatePacketSequence(_: *Network, socket_state: *SocketState) !u64 {
        if (socket_state.next_packet_sequence == std.math.maxInt(u64)) return error.VoprIoSequenceExhausted;
        const result = socket_state.next_packet_sequence;
        socket_state.next_packet_sequence += 1;
        return result;
    }

    fn allocateEphemeralPort(self: *Network) !u16 {
        var attempts: usize = 0;
        while (attempts < std.math.maxInt(u16)) : (attempts += 1) {
            const candidate = self.next_ephemeral_port;
            self.next_ephemeral_port +%= 1;
            if (self.next_ephemeral_port < 20_000) self.next_ephemeral_port = 20_000;
            const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(candidate) };
            if (self.findIpListener(address) == null) return candidate;
        }
        return error.AddressInUse;
    }

    const network_id = ids.stable("sim-io", "network-v1");
};

fn copyLimited(destination: []u8, source: []const u8, offset: usize) usize {
    if (offset >= destination.len) return 0;
    const count = @min(destination.len - offset, source.len);
    @memcpy(destination[offset..][0..count], source[0..count]);
    return count;
}

fn takeBool(value: *bool) bool {
    const result = value.*;
    value.* = false;
    return result;
}

fn packetName(packet: Packet) []const u8 {
    if (packet.close_write) return "sim-io.stream_fin";
    if (packet.datagram) return if (packet.drop) "sim-io.datagram_drop" else "sim-io.datagram_deliver";
    return if (packet.drop) "sim-io.packet_drop" else "sim-io.packet_deliver";
}

fn ipAddressMatches(listener: std.Io.net.IpAddress, requested: std.Io.net.IpAddress) bool {
    if (listener.getPort() != requested.getPort()) return false;
    return switch (listener) {
        .ip4 => |left| switch (requested) {
            .ip4 => |right| std.mem.allEqual(u8, &left.bytes, 0) or std.mem.eql(u8, &left.bytes, &right.bytes),
            .ip6 => false,
        },
        .ip6 => |left| switch (requested) {
            .ip4 => false,
            .ip6 => |right| std.mem.allEqual(u8, &left.bytes, 0) or std.mem.eql(u8, &left.bytes, &right.bytes),
        },
    };
}

fn ipEndpointIdentity(namespace: []const u8, address: std.Io.net.IpAddress) ids.StableId {
    const address_id = switch (address) {
        .ip4 => |ip4| ids.derive("sim-io.ip4-address", ids.digest(&ip4.bytes), ip4.port),
        .ip6 => |ip6| blk: {
            const scoped = ids.derive(
                "sim-io.ip6-scope",
                ids.digest(&ip6.bytes),
                (@as(u64, ip6.flow) << 32) | ip6.interface.index,
            );
            break :blk ids.derive("sim-io.ip6-address", scoped, ip6.port);
        },
    };
    return ids.derive(namespace, Network.network_id, address_id);
}

fn mapListenResourceError(err: anyerror) std.Io.net.IpAddress.ListenError {
    return switch (err) {
        error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
        else => error.SystemResources,
    };
}

fn mapBindResourceError(err: anyerror) std.Io.net.IpAddress.BindError {
    return switch (err) {
        error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
        else => error.SystemResources,
    };
}

fn mapUnixListenResourceError(err: anyerror) std.Io.net.UnixAddress.ListenError {
    return switch (err) {
        error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
        else => error.SystemResources,
    };
}

fn mapConnectResourceError(err: anyerror) std.Io.net.IpAddress.ConnectError {
    return switch (err) {
        error.ConnectionRefused => error.ConnectionRefused,
        error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
        else => error.SystemResources,
    };
}

fn mapUnixConnectResourceError(err: anyerror) std.Io.net.UnixAddress.ConnectError {
    return switch (err) {
        error.ConnectionRefused => error.AccessDenied,
        error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
        else => error.SystemResources,
    };
}

fn mapPairResourceError(err: anyerror) std.Io.net.Socket.CreatePairError {
    return switch (err) {
        error.ProcessFdQuotaExceeded => error.ProcessFdQuotaExceeded,
        else => error.SystemResources,
    };
}

fn mapWaitAcceptError(err: anyerror) std.Io.net.Server.AcceptError {
    return switch (err) {
        error.Canceled => error.Canceled,
        else => error.Unexpected,
    };
}

fn mapWaitReadError(err: anyerror) std.Io.net.Stream.Reader.Error {
    return switch (err) {
        error.Canceled => error.Canceled,
        else => error.Unexpected,
    };
}

fn mapWaitWriteError(err: anyerror) std.Io.net.Stream.Writer.Error {
    return switch (err) {
        error.Canceled => error.Canceled,
        else => error.Unexpected,
    };
}

test "listener and connection identities are scoped to their logical endpoint" {
    const NoopWaitPort = struct {
        fn wait(_: *anyopaque, _: ids.StableId) anyerror!void {
            return error.UnexpectedWait;
        }

        fn wake(_: *anyopaque, _: ids.StableId, _: u32) anyerror!void {}

        const vtable = WaitPort.VTable{ .wait = wait, .wake = wake };
    };
    const address_a: std.Io.net.IpAddress = .{ .ip4 = .loopback(21_001) };
    const address_b: std.Io.net.IpAddress = .{ .ip4 = .loopback(21_002) };

    var first = try Network.init(std.testing.allocator, .{});
    defer first.deinit();
    var first_context: u8 = 0;
    first.bindWaitPort(.{ .ptr = &first_context, .vtable = &NoopWaitPort.vtable });
    const first_listener_a = try first.listenIp(&address_a, .{});
    const first_listener_b = try first.listenIp(&address_b, .{});
    const first_client_a = try first.connectIp(&address_a, .{ .mode = .stream });
    const first_client_b = try first.connectIp(&address_b, .{ .mode = .stream });

    var second = try Network.init(std.testing.allocator, .{});
    defer second.deinit();
    var second_context: u8 = 0;
    second.bindWaitPort(.{ .ptr = &second_context, .vtable = &NoopWaitPort.vtable });
    const second_listener_b = try second.listenIp(&address_b, .{});
    const second_listener_a = try second.listenIp(&address_a, .{});
    const second_client_b = try second.connectIp(&address_b, .{ .mode = .stream });
    const second_client_a = try second.connectIp(&address_a, .{ .mode = .stream });

    try std.testing.expectEqual(first.getSocket(first_listener_a.handle).?.id, second.getSocket(second_listener_a.handle).?.id);
    try std.testing.expectEqual(first.getSocket(first_listener_b.handle).?.id, second.getSocket(second_listener_b.handle).?.id);
    try std.testing.expectEqual(first.getSocket(first_client_a.handle).?.id, second.getSocket(second_client_a.handle).?.id);
    try std.testing.expectEqual(first.getSocket(first_client_b.handle).?.id, second.getSocket(second_client_b.handle).?.id);
}

test "stream FIN never overtakes earlier payload when reordering is enabled" {
    const NoopWaitPort = struct {
        fn wait(_: *anyopaque, _: ids.StableId) anyerror!void {
            return error.UnexpectedWait;
        }

        fn wake(_: *anyopaque, _: ids.StableId, _: u32) anyerror!void {}

        const vtable = WaitPort.VTable{ .wait = wait, .wake = wake };
    };

    var network = try Network.init(std.testing.allocator, .{});
    defer network.deinit();
    var wait_context: u8 = 0;
    network.bindWaitPort(.{ .ptr = &wait_context, .vtable = &NoopWaitPort.vtable });
    const pair = try network.createPair(.{ .family = .ip4, .mode = .stream });
    try std.testing.expectEqual(
        @as(usize, "payload".len),
        try network.write(pair[0].handle, "", &.{"payload"}, 1),
    );
    try network.shutdown(pair[0].handle, .send);
    network.faults.reorder = true;

    var ready: transition.List = .{};
    defer ready.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);

    try network.enumerateReady(&ready, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expectEqualStrings("sim-io.packet_deliver", ready.items.items[0].name);
    try std.testing.expect(try network.executeReady(ready.items.items[0].id, &sink, std.testing.allocator));

    ready.items.clearRetainingCapacity();
    try network.enumerateReady(&ready, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expectEqualStrings("sim-io.stream_fin", ready.items.items[0].name);
    try std.testing.expect(try network.executeReady(ready.items.items[0].id, &sink, std.testing.allocator));

    var bytes: [16]u8 = undefined;
    var buffers = [_][]u8{bytes[0..]};
    const received = try network.read(pair[1].handle, &buffers);
    try std.testing.expectEqualStrings("payload", bytes[0..received]);
    try std.testing.expectEqual(@as(usize, 0), try network.read(pair[1].handle, &buffers));
}

test "stream delivery preserves bytes across reorder drop and duplicate faults" {
    const NoopWaitPort = struct {
        fn wait(_: *anyopaque, _: ids.StableId) anyerror!void {
            return error.UnexpectedWait;
        }

        fn wake(_: *anyopaque, _: ids.StableId, _: u32) anyerror!void {}

        const vtable = WaitPort.VTable{ .wait = wait, .wake = wake };
    };

    var network = try Network.init(std.testing.allocator, .{});
    defer network.deinit();
    var wait_context: u8 = 0;
    network.bindWaitPort(.{ .ptr = &wait_context, .vtable = &NoopWaitPort.vtable });
    const pair = try network.createPair(.{ .family = .ip4, .mode = .stream });
    network.faults.drop_next = true;
    network.faults.duplicate_next = true;
    try std.testing.expectEqual(@as(usize, 5), try network.write(pair[0].handle, "", &.{"first"}, 1));
    try std.testing.expectEqual(@as(usize, 6), try network.write(pair[0].handle, "", &.{"second"}, 1));
    network.faults.reorder = true;

    var ready: transition.List = .{};
    defer ready.deinit(std.testing.allocator);
    var sink: event.Sink = .{};
    defer sink.deinit(std.testing.allocator);

    // The first transition loses a TCP packet but retains it for retransmit.
    try network.enumerateReady(&ready, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expect(try network.executeReady(ready.items.items[0].id, &sink, std.testing.allocator));

    // Retransmission delivers once; the later write remains ordered behind it.
    ready.items.clearRetainingCapacity();
    try network.enumerateReady(&ready, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expect(try network.executeReady(ready.items.items[0].id, &sink, std.testing.allocator));
    ready.items.clearRetainingCapacity();
    try network.enumerateReady(&ready, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expect(try network.executeReady(ready.items.items[0].id, &sink, std.testing.allocator));

    var bytes: [16]u8 = undefined;
    var buffers = [_][]u8{bytes[0..]};
    const received = try network.read(pair[1].handle, &buffers);
    try std.testing.expectEqualStrings("firstsecond", bytes[0..received]);
}
