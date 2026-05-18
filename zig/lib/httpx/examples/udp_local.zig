//! UDP Local Send/Recv Example
//!
//! Demonstrates using `httpx.UdpSocket` to send a datagram to a socket bound
//! on loopback. This is self-contained and does not require internet access.

const std = @import("std");
const httpx = @import("httpx");

pub fn main(init: std.process.Init) !void {
    std.debug.print("=== UDP Local Send/Recv Example ===\n\n", .{});

    const bind_addr = httpx.socket.Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };

    var recv_sock = try httpx.UdpSocket.bind(bind_addr, init.io);
    defer recv_sock.close();

    const recv_addr = recv_sock.socket.address;

    var send_sock = try httpx.UdpSocket.bind(bind_addr, init.io);
    defer send_sock.close();

    const msg = "hello over udp";
    _ = try send_sock.sendTo(recv_addr, msg);

    var buf: [256]u8 = undefined;
    const got = try recv_sock.recvFrom(&buf);

    std.debug.print("Sent: {s}\n", .{msg});
    std.debug.print("Recv: {s}\n", .{buf[0..got.n]});
    std.debug.print("From: {any}\n", .{got.addr});
}
