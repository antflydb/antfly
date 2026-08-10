// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Capture one Zig 0.16 compiler report from the build web UI's binary protocol.
// Start `zig build --time-report --webui=127.0.0.1:PORT`, then run:
//
//   node tools/capture_zig_time_report.mjs \
//     ws://127.0.0.1:PORT/ STEP_NAME_SUBSTRING report.json

import { randomBytes, createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import net from "node:net";
import tls from "node:tls";

const url = process.argv[2];
const stepNeedle = process.argv[3];
const outputPath = process.argv[4];
const minLlvmSeconds = Number(process.argv[5] ?? "0");
if (!url || !stepNeedle || !outputPath) {
  throw new Error(
    "usage: node capture_zig_time_report.mjs URL STEP_NEEDLE OUTPUT.json [MIN_LLVM_SECONDS]",
  );
}
if (!Number.isFinite(minLlvmSeconds) || minLlvmSeconds < 0) {
  throw new Error("MIN_LLVM_SECONDS must be a non-negative number");
}

let stepNames = [];

function u64(view, offset) {
  return Number(view.getBigUint64(offset, true));
}

function cString(bytes, offset) {
  let end = offset;
  while (bytes[end] !== 0) end += 1;
  return [new TextDecoder().decode(bytes.subarray(offset, end)), end + 1];
}

function parseHello(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(12, true);
  let offset = 16;
  const lengths = [];
  for (let i = 0; i < count; i += 1) {
    lengths.push(view.getUint32(offset, true));
    offset += 4;
  }
  stepNames = lengths.map((length) => {
    const value = new TextDecoder().decode(bytes.subarray(offset, offset + length));
    offset += length;
    return value;
  });
}

function parseCompile(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const stepIndex = view.getUint32(1, true);
  const stepName = stepNames[stepIndex] ?? `step-${stepIndex}`;
  const statsOffset = 6;
  const stats = {
    reachable_files: view.getUint32(statsOffset, true),
    imported_files: view.getUint32(statsOffset + 4, true),
    generic_instances: view.getUint32(statsOffset + 8, true),
    inline_calls: view.getUint32(statsOffset + 12, true),
    cpu_ns_parse: u64(view, statsOffset + 16),
    cpu_ns_astgen: u64(view, statsOffset + 24),
    cpu_ns_sema: u64(view, statsOffset + 32),
    cpu_ns_codegen: u64(view, statsOffset + 40),
    cpu_ns_link: u64(view, statsOffset + 48),
    real_ns_files: u64(view, statsOffset + 56),
    real_ns_decls: u64(view, statsOffset + 64),
    real_ns_llvm_emit: u64(view, statsOffset + 72),
    real_ns_link_flush: u64(view, statsOffset + 80),
  };
  const totalNs = u64(view, 94);
  const llvmLength = view.getUint32(102, true);
  const fileCount = view.getUint32(106, true);
  const declCount = view.getUint32(110, true);
  let offset = 114 + llvmLength;
  const files = [];
  for (let i = 0; i < fileCount; i += 1) {
    let name;
    [name, offset] = cString(bytes, offset);
    files.push({ name, sema_ns: 0, codegen_ns: 0, link_ns: 0, total_ns: 0 });
  }
  const declarations = [];
  for (let i = 0; i < declCount; i += 1) {
    let name;
    [name, offset] = cString(bytes, offset);
    const fileIndex = view.getUint32(offset, true);
    const semaCount = view.getUint32(offset + 4, true);
    const semaNs = u64(view, offset + 8);
    const codegenNs = u64(view, offset + 16);
    const linkNs = u64(view, offset + 24);
    offset += 32;
    const file = files[fileIndex];
    file.sema_ns += semaNs;
    file.codegen_ns += codegenNs;
    file.link_ns += linkNs;
    file.total_ns += semaNs + codegenNs + linkNs;
    declarations.push({
      file: file.name,
      name,
      sema_count: semaCount,
      sema_ns: semaNs,
      codegen_ns: codegenNs,
      link_ns: linkNs,
      total_ns: semaNs + codegenNs + linkNs,
    });
  }
  const allFiles = files.map((file) => file.name);
  files.sort((left, right) => right.total_ns - left.total_ns);
  declarations.sort((left, right) => right.total_ns - left.total_ns);
  return {
    step_index: stepIndex,
    step_name: stepName,
    use_llvm: (bytes[5] & 1) !== 0,
    total_ns: totalNs,
    stats,
    file_count: fileCount,
    declaration_count: declCount,
    all_files: allFiles,
    top_files: files.slice(0, 100),
    top_declarations: declarations.slice(0, 100),
  };
}

function handleMessage(bytes) {
  if (bytes[0] === 0) {
    parseHello(bytes);
    return;
  }
  if (bytes[0] !== 7 || stepNames.length === 0) return;
  const report = parseCompile(bytes);
  if (!report.step_name.includes(stepNeedle)) return;
  if (report.stats.real_ns_llvm_emit < minLlvmSeconds * 1e9) return;
  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`captured ${report.step_name} to ${outputPath}`);
  process.exit(0);
}

function connectRawWebSocket(rawUrl) {
  const parsed = new URL(rawUrl);
  const secure = parsed.protocol === "wss:";
  if (!secure && parsed.protocol !== "ws:") {
    throw new Error(`unsupported WebSocket URL protocol: ${parsed.protocol}`);
  }
  const port = parsed.port ? Number(parsed.port) : secure ? 443 : 80;
  const path = `${parsed.pathname || "/"}${parsed.search || ""}`;
  const key = randomBytes(16).toString("base64");
  const expectedAccept = createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");
  const socket = (secure ? tls : net).connect({
    host: parsed.hostname,
    port,
    servername: parsed.hostname,
  });

  let handshake = false;
  let buffer = Buffer.alloc(0);
  socket.on("connect", () => {
    socket.write(
      [
        `GET ${path} HTTP/1.1`,
        `Host: ${parsed.host}`,
        "Connection: Upgrade",
        "Upgrade: websocket",
        "Sec-WebSocket-Version: 13",
        `Sec-WebSocket-Key: ${key}`,
        "",
        "",
      ].join("\r\n"),
    );
  });
  socket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    if (!handshake) {
      const headerEnd = buffer.indexOf("\r\n\r\n");
      if (headerEnd < 0) return;
      const header = buffer.subarray(0, headerEnd).toString("latin1");
      if (!header.startsWith("HTTP/1.1 101")) {
        throw new Error(`WebSocket upgrade failed:\n${header}`);
      }
      const accept = /^sec-websocket-accept:\s*(.+)$/im.exec(header)?.[1]?.trim();
      if (accept !== expectedAccept) throw new Error("WebSocket accept key mismatch");
      buffer = buffer.subarray(headerEnd + 4);
      handshake = true;
    }

    while (buffer.length >= 2) {
      const opcode = buffer[0] & 0x0f;
      const masked = (buffer[1] & 0x80) !== 0;
      let length = buffer[1] & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (buffer.length < offset + 2) return;
        length = buffer.readUInt16BE(offset);
        offset += 2;
      } else if (length === 127) {
        if (buffer.length < offset + 8) return;
        const wide = buffer.readBigUInt64BE(offset);
        if (wide > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("WebSocket frame too large");
        length = Number(wide);
        offset += 8;
      }
      const mask = masked ? buffer.subarray(offset, offset + 4) : null;
      if (masked) offset += 4;
      if (buffer.length < offset + length) return;
      let payload = buffer.subarray(offset, offset + length);
      buffer = buffer.subarray(offset + length);
      if (mask) {
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i += 1) payload[i] ^= mask[i % 4];
      }
      if (opcode === 8) {
        socket.end();
        process.exitCode = 1;
        return;
      }
      if (opcode === 1 || opcode === 2) handleMessage(new Uint8Array(payload));
    }
  });
  socket.on("error", (error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

connectRawWebSocket(url);
