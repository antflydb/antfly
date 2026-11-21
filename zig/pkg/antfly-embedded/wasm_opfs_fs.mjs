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

// Origin Private File System backend for the embedded Antfly WASM database.
//
// Antfly's storage interface is synchronous. OPFS offers synchronous file I/O
// only through FileSystemSyncAccessHandle (dedicated workers), and only for an
// already-open file: opening, renaming and deleting files are asynchronous.
// Like SQLite's "opfs-sahpool" VFS, this backend therefore pre-opens a pool of
// physical slot files and keeps the logical namespace inside them:
//
//   slot file = [header record A | header record B]  (4096 bytes)
//               [file data ...]                       (logical bytes)
//
// Each record holds {generation, logical path, CRC-32}. The newest valid
// record of a slot is authoritative; an empty path marks a free slot. Updates
// always overwrite the slot's *older* record and are flushed before returning,
// so a torn header write leaves the previous record in force: creating,
// renaming and deleting a file are each all-or-nothing across crashes. A
// rename writes the moved slot's record with a fresh generation before it
// frees the replaced destination; if both survive a crash, the higher
// generation wins on reopen. File data becomes durable at `sync`.
//
// Slots are opened ahead of use (`ensureCapacity`, asynchronous). Creating a
// file with no free slot fails with "no space"; hosts call `ensureCapacity`
// between database operations to keep a reserve.

export const status = Object.freeze({ notFound: -1, endOfStream: -2, noSpace: -3, accessDenied: -4, io: -5 });

const HEADER = 4096;
const RECORD = 2048;
const MAGIC = [0x41, 0x46, 0x4f, 0x50, 0x46, 0x53, 0x30, 0x31]; // "AFOPFS01"
const PATH_OFFSET = 24;
const MAX_PATH = RECORD - PATH_OFFSET;
const SLOT_NAME = /^slot-(\d{6})\.afs$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const CRC_TABLE = (() => {
    const table = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
        let c = n;
        for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
        table[n] = c >>> 0;
    }
    return table;
})();
function crc32(bytes) {
    let c = 0xffffffff;
    for (const byte of bytes) c = CRC_TABLE[(c ^ byte) & 0xff] ^ (c >>> 8);
    return (c ^ 0xffffffff) >>> 0;
}

function encodeRecord(generation, path) {
    const pathBytes = encoder.encode(path);
    if (pathBytes.length > MAX_PATH) throw Object.assign(new Error("path too long"), { status: status.io });
    const record = new Uint8Array(RECORD);
    const view = new DataView(record.buffer);
    record.set(MAGIC, 0);
    view.setBigUint64(8, BigInt(generation), true);
    view.setUint32(16, pathBytes.length, true);
    record.set(pathBytes, PATH_OFFSET);
    view.setUint32(20, crc32(checksumInput(record, pathBytes.length)), true);
    return record;
}
function checksumInput(record, pathLength) {
    // generation (8) + path length (4) + path bytes
    const input = new Uint8Array(12 + pathLength);
    input.set(record.subarray(8, 20), 0);
    input.set(record.subarray(PATH_OFFSET, PATH_OFFSET + pathLength), 12);
    return input;
}
function decodeRecord(record) {
    for (let i = 0; i < MAGIC.length; i++) if (record[i] !== MAGIC[i]) return null;
    const view = new DataView(record.buffer, record.byteOffset, record.byteLength);
    const pathLength = view.getUint32(16, true);
    if (pathLength > MAX_PATH) return null;
    if (crc32(checksumInput(record, pathLength)) !== view.getUint32(20, true)) return null;
    return {
        generation: Number(view.getBigUint64(8, true)),
        path: pathLength ? decoder.decode(record.subarray(PATH_OFFSET, PATH_OFFSET + pathLength)) : null,
    };
}

function contains(prefix, path) {
    // Matches the storage layer's directory containment, including "/" and "".
    let trimmed = prefix;
    while (trimmed.length > 1 && trimmed.endsWith("/")) trimmed = trimmed.slice(0, -1);
    if (trimmed === "" || trimmed === "/") return path.startsWith("/");
    return path === trimmed || path.startsWith(trimmed + "/");
}

class FsError extends Error {
    constructor(code, message) {
        super(message);
        this.status = code;
    }
}

/**
 * Open (or create) a slot pool in `directory`, a FileSystemDirectoryHandle in
 * a dedicated worker. Throws `OpfsBusy` if another tab holds the pool.
 */
export async function openOpfsFileSystem(directory, { initialSlots = 64, reserve = 32 } = {}) {
    const slots = []; // { name, number, handle, generation, record, path }
    const byPath = new Map();
    const free = [];
    let generation = 0;
    let lastNow = 0n;
    let closed = false;
    let growing = Promise.resolve();

    async function openSlot(name, number) {
        const file = await directory.getFileHandle(name, { create: true });
        try {
            return { name, number, handle: await file.createSyncAccessHandle() };
        } catch (error) {
            if (error?.name === "NoModificationAllowedError") throw new FsError(status.accessDenied, "OpfsBusy");
            throw error;
        }
    }

    function readRecords(slot) {
        const header = new Uint8Array(HEADER);
        const size = slot.handle.getSize();
        if (size > 0) slot.handle.read(header, { at: 0 });
        const records = [decodeRecord(header.subarray(0, RECORD)), decodeRecord(header.subarray(RECORD, HEADER))];
        const index = !records[1] || (records[0] && records[0].generation >= records[1].generation) ? 0 : 1;
        const current = records[index];
        slot.record = current ? index : 1; // the next write goes to the other record
        slot.generation = current?.generation ?? 0;
        slot.path = current?.path ?? null;
    }

    // Write `path` (or null = free) as the slot's newest record and flush it.
    function writeRecord(slot, path) {
        const target = slot.record === 0 ? 1 : 0;
        const record = encodeRecord(++generation, path ?? "");
        if (slot.handle.getSize() < HEADER) slot.handle.truncate(HEADER);
        slot.handle.write(record, { at: target * RECORD });
        slot.handle.flush();
        slot.record = target;
        slot.generation = generation;
        slot.path = path;
    }

    function release(slot) {
        writeRecord(slot, null);
        slot.handle.truncate(HEADER);
        slot.handle.flush();
        free.push(slot);
    }

    // --- open: adopt existing slots, resolve crash leftovers ---
    const names = [];
    for await (const [name, handle] of directory.entries()) {
        const match = SLOT_NAME.exec(name);
        if (match && handle.kind !== "directory") names.push([name, Number(match[1])]);
    }
    names.sort((a, b) => a[1] - b[1]);
    try {
        for (const [name, number] of names) slots.push(await openSlot(name, number));
    } catch (error) {
        for (const slot of slots) slot.handle.close();
        throw error;
    }
    for (const slot of slots) {
        readRecords(slot);
        generation = Math.max(generation, slot.generation);
    }
    const loser = [];
    for (const slot of slots) {
        if (!slot.path) continue;
        const holder = byPath.get(slot.path);
        if (!holder) byPath.set(slot.path, slot);
        else if (holder.generation < slot.generation) {
            // A rename moved this path here; the older holder was being replaced.
            loser.push(holder);
            byPath.set(slot.path, slot);
        } else loser.push(slot);
    }
    for (const slot of slots) if (!slot.path) free.push(slot);
    for (const slot of loser) release(slot);
    for (const slot of free) if (slot.handle.getSize() > HEADER) slot.handle.truncate(HEADER);

    async function grow(minimum) {
        let next = slots.reduce((max, slot) => Math.max(max, slot.number), 0) + 1;
        while (free.length < minimum) {
            if (next > 999999) throw new FsError(status.noSpace, "OPFS pool limit reached");
            const slot = await openSlot(`slot-${String(next).padStart(6, "0")}.afs`, next);
            next++;
            slot.handle.truncate(0);
            slot.record = 1;
            slot.generation = 0;
            slot.path = null;
            slots.push(slot);
            free.push(slot);
        }
    }

    function claim(path) {
        if (encoder.encode(path).length > MAX_PATH) throw new FsError(status.io, "path too long");
        const slot = free.pop();
        if (!slot) throw new FsError(status.noSpace, "OPFS pool exhausted; call ensureCapacity");
        slot.handle.truncate(HEADER);
        return slot;
    }
    function lookup(path) {
        const slot = byPath.get(path);
        if (!slot) throw new FsError(status.notFound, "not found");
        return slot;
    }
    function guard(fn) {
        if (closed) return status.io;
        try {
            return fn();
        } catch (error) {
            if (typeof error?.status === "number") return error.status;
            if (error?.name === "QuotaExceededError") return status.noSpace;
            fs.lastError = error;
            return status.io;
        }
    }

    const fs = {
        lastError: null,
        /** Logical size, or status.notFound. */
        size: (path) => guard(() => Math.max(0, lookup(path).handle.getSize() - HEADER)),
        /** Read into `out` (a Uint8Array) at `offset`; returns bytes read. */
        read: (path, offset, out) =>
            guard(() => {
                const slot = lookup(path);
                const size = slot.handle.getSize() - HEADER;
                if (offset >= size || out.length === 0) return 0;
                const length = Math.min(out.length, size - offset);
                return slot.handle.read(out.subarray(0, length), { at: HEADER + offset });
            }),
        /** Replace or append, creating the file (durably named) if missing. */
        write: (path, bytes, append) =>
            guard(() => {
                let slot = byPath.get(path);
                if (slot) {
                    if (!append) slot.handle.truncate(HEADER);
                    const at = slot.handle.getSize();
                    if (bytes.length) slot.handle.write(bytes, { at });
                    return 0;
                }
                slot = claim(path);
                try {
                    if (bytes.length) slot.handle.write(bytes, { at: HEADER });
                    // Data first, then the name; writeRecord flushes both.
                    writeRecord(slot, path);
                } catch (error) {
                    // The name never became durable: return the slot to the pool.
                    try {
                        slot.handle.truncate(HEADER);
                    } catch {
                        /* Reclaimed on the next open. */
                    }
                    free.push(slot);
                    throw error;
                }
                byPath.set(path, slot);
                return 0;
            }),
        sync: (path) =>
            guard(() => {
                lookup(path).handle.flush();
                return 0;
            }),
        rename: (oldPath, newPath) =>
            guard(() => {
                const slot = lookup(oldPath);
                if (oldPath === newPath) return 0;
                if (encoder.encode(newPath).length > MAX_PATH) throw new FsError(status.io, "path too long");
                const replaced = byPath.get(newPath);
                // The moved name becomes durable (and newest) before the old
                // destination is freed; reopen resolves an interruption.
                writeRecord(slot, newPath);
                byPath.delete(oldPath);
                byPath.set(newPath, slot);
                if (replaced) release(replaced);
                return 0;
            }),
        remove: (path, tree) =>
            guard(() => {
                if (!tree) {
                    const slot = lookup(path);
                    byPath.delete(path);
                    release(slot);
                    return 0;
                }
                for (const [candidate, slot] of [...byPath]) {
                    if (!contains(path, candidate)) continue;
                    byPath.delete(candidate);
                    release(slot);
                }
                return 0;
            }),
        /** Immediate child file or directory names below `path`. */
        list(path, dirs) {
            const directory = path.replace(/\/+$/, "");
            const prefix = directory + "/";
            const names = new Set();
            for (const candidate of byPath.keys()) {
                if (!candidate.startsWith(prefix)) continue;
                const relative = candidate.slice(prefix.length);
                const separator = relative.indexOf("/");
                if (dirs !== separator >= 0) continue;
                const name = separator >= 0 ? relative.slice(0, separator) : relative;
                if (name) names.add(name);
            }
            return [...names];
        },
        nowNs() {
            const clock = globalThis.performance;
            const now = BigInt(Math.round(((clock?.timeOrigin ?? 0) + (clock?.now?.() ?? Date.now())) * 1e6));
            lastNow = now > lastNow ? now : lastNow + 1n;
            return lastNow;
        },

        /**
         * Open slots until at least `minimum` are free (asynchronous). Calls
         * are serialized, so overlapping callers never race for a slot file.
         */
        ensureCapacity(minimum = reserve) {
            const run = growing.then(() => grow(minimum));
            growing = run.catch(() => {});
            return run;
        },
        stats() {
            let bytes = 0;
            for (const slot of byPath.values()) bytes += Math.max(0, slot.handle.getSize() - HEADER);
            return { files: byPath.size, slots: slots.length, free: free.length, bytes };
        },
        paths: () => [...byPath.keys()].sort(),
        /** Release every handle; the pool can then be reopened (e.g. by another tab). */
        close() {
            if (closed) return;
            closed = true;
            for (const slot of slots) {
                try {
                    slot.handle.flush();
                } finally {
                    slot.handle.close();
                }
            }
        },
    };
    // At least `initialSlots` in total and `reserve` free.
    await fs.ensureCapacity(Math.max(reserve, initialSlots - (slots.length - free.length)));
    return fs;
}

/** Remove a pool directory (opt-out, sign-out, or a corrupt copy). Close it first. */
export async function destroyOpfsFileSystem(parent, name) {
    try {
        await parent.removeEntry(name, { recursive: true });
    } catch (error) {
        if (error?.name !== "NotFoundError") throw error;
    }
}
