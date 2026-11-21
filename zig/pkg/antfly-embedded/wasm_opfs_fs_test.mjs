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

// OPFS slot pool against a simulated OPFS in which only flushed bytes survive
// a crash. The WASM bridge is exercised by wasm_smoke_run.mjs.
import assert from "node:assert/strict";
import { openOpfsFileSystem, destroyOpfsFileSystem, status } from "./wasm_opfs_fs.mjs";

const notFound = () => Object.assign(new Error("missing"), { name: "NotFoundError" });

const faults = { quota: false };
class FakeFile {
    constructor(bytes = new Uint8Array(0)) {
        this.durable = bytes.slice();
        this.current = bytes.slice();
        this.locked = false;
    }
}
function grow(bytes, size) {
    if (bytes.length >= size) return bytes;
    const out = new Uint8Array(size);
    out.set(bytes);
    return out;
}
function syncHandle(file) {
    if (file.locked) throw Object.assign(new Error("locked"), { name: "NoModificationAllowedError" });
    file.locked = true;
    let open = true;
    const live = () => {
        if (!open) throw Object.assign(new Error("closed"), { name: "InvalidStateError" });
    };
    return {
        getSize: () => (live(), file.current.length),
        read(out, { at = 0 } = {}) {
            live();
            const n = Math.max(0, Math.min(out.length, file.current.length - at));
            out.set(file.current.subarray(at, at + n));
            return n;
        },
        write(bytes, { at = file.current.length } = {}) {
            live();
            if (faults.quota) throw Object.assign(new Error("quota"), { name: "QuotaExceededError" });
            file.current = grow(file.current, at + bytes.length);
            file.current.set(bytes, at);
            return bytes.length;
        },
        truncate(size) {
            live();
            file.current = size <= file.current.length ? file.current.slice(0, size) : grow(file.current, size);
        },
        flush() {
            live();
            file.durable = file.current.slice();
        },
        close() {
            open = false;
            file.locked = false;
        },
    };
}
function directory(files = new Map()) {
    return {
        files,
        async *entries() {
            for (const [name] of files) yield [name, { kind: "file" }];
        },
        async getFileHandle(name, { create = false } = {}) {
            if (!files.has(name)) {
                if (!create) throw notFound();
                files.set(name, new FakeFile());
            }
            const file = files.get(name);
            return { kind: "file", createSyncAccessHandle: async () => syncHandle(file) };
        },
        async removeEntry(name) {
            if (!files.delete(name)) throw notFound();
        },
    };
}
/** The directory a restarted browser would see: only flushed bytes. */
function crashed(dir, override = new Map()) {
    const files = new Map();
    for (const [name, file] of dir.files) files.set(name, new FakeFile(override.get(name) ?? file.durable));
    return directory(files);
}
function snapshot(dir) {
    return new Map([...dir.files].map(([name, file]) => [name, file.durable.slice()]));
}
const bytes = (text) => new TextEncoder().encode(text);
function readAll(fs, path) {
    const size = fs.size(path);
    if (size < 0) return size;
    const out = new Uint8Array(size);
    assert.equal(fs.read(path, 0, out), size);
    return new TextDecoder().decode(out);
}

// Contract: the same semantics as the in-memory storage.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 4, reserve: 2 });
    assert.equal(fs.size("/db/missing"), status.notFound);
    assert.equal(fs.write("/db/a/one", bytes("hello"), false), 0);
    assert.equal(fs.write("/db/a/one", bytes(" world"), true), 0);
    assert.equal(readAll(fs, "/db/a/one"), "hello world");
    const tail = new Uint8Array(10);
    assert.equal(fs.read("/db/a/one", 6, tail), 5, "reads stop at end of file");
    assert.equal(fs.read("/db/a/one", 99, tail), 0);
    assert.equal(fs.write("/db/a/one", bytes("replaced"), false), 0);
    assert.equal(readAll(fs, "/db/a/one"), "replaced");
    fs.write("/db/a/two", bytes("2"), false);
    fs.write("/db/b/deep/three", bytes("3"), false);
    assert.deepEqual(fs.list("/db/a", false).sort(), ["one", "two"]);
    assert.deepEqual(fs.list("/db/", true).sort(), ["a", "b"]);
    assert.deepEqual(fs.list("/db/b", true), ["deep"]);
    assert.equal(fs.rename("/db/missing", "/db/x"), status.notFound);
    assert.equal(fs.rename("/db/a/two", "/db/a/one"), 0);
    assert.equal(readAll(fs, "/db/a/one"), "2");
    assert.equal(fs.size("/db/a/two"), status.notFound);
    assert.equal(fs.remove("/db/b", true), 0);
    assert.equal(fs.size("/db/b/deep/three"), status.notFound);
    assert.equal(fs.remove("/db/a/one", false), 0);
    assert.equal(fs.remove("/db/a/one", false), status.notFound);
    assert.equal(fs.write("/" + "x".repeat(3000), bytes("x"), false), status.io, "overlong paths are rejected");
    assert.ok(fs.nowNs() < fs.nowNs());
    // Freed slots are reused instead of growing the pool.
    for (let i = 0; i < 10; i++) {
        fs.write(`/db/tmp${i}`, bytes("t"), false);
        fs.remove(`/db/tmp${i}`, false);
    }
    assert.equal(fs.stats().slots, 4);
    fs.close();
    assert.equal(fs.size("/db/a/one"), status.io, "a closed pool refuses work");
}

// Durability: names are durable on return; data at sync.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 4, reserve: 2 });
    fs.write("/db/wal", bytes("committed"), false);
    fs.sync("/db/wal");
    fs.write("/db/wal", bytes("+unsynced"), true);
    fs.write("/db/new", bytes("created"), false);
    // The browser dies here: no close, no further flushes.
    const after = await openOpfsFileSystem(crashed(dir));
    assert.equal(readAll(after, "/db/wal"), "committed", "unsynced appends are not durable");
    assert.equal(readAll(after, "/db/new"), "created", "a created file and its first bytes are durable");
    after.close();
}

// Rename is atomic, including a crash before the replaced slot is freed.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 4, reserve: 2 });
    fs.write("/db/CURRENT", bytes("v1"), false);
    fs.write("/db/CURRENT.tmp", bytes("v2"), false);
    fs.sync("/db/CURRENT.tmp");
    const before = snapshot(dir);
    fs.rename("/db/CURRENT.tmp", "/db/CURRENT");
    const after = snapshot(dir);
    const changed = [...after].filter(([name, data]) => Buffer.compare(Buffer.from(data), Buffer.from(before.get(name))) !== 0).map(([name]) => name);
    assert.equal(changed.length, 2, "rename touches the moved slot and the replaced slot");

    // Crash after the moved slot's record was flushed but before the replaced
    // slot was freed (the only intermediate state, since the move is flushed
    // first): both slots claim /db/CURRENT and the newer generation must win.
    const replaced = changed.find((name) => after.get(name).length <= 4096);
    assert.ok(replaced, "the replaced destination's slot is freed and truncated");
    const view = await openOpfsFileSystem(crashed(dir, new Map([[replaced, before.get(replaced)]])));
    assert.deepEqual(view.paths(), ["/db/CURRENT"]);
    assert.equal(readAll(view, "/db/CURRENT"), "v2");
    view.close();
    const clean = await openOpfsFileSystem(crashed(dir));
    assert.deepEqual(clean.paths(), ["/db/CURRENT"]);
    assert.equal(readAll(clean, "/db/CURRENT"), "v2");
    clean.close();
}

// A torn header write leaves the slot's previous record in force.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 2, reserve: 1 });
    fs.write("/db/a", bytes("A"), false);
    const before = snapshot(dir);
    fs.rename("/db/a", "/db/b");
    const after = snapshot(dir);
    const [slot] = [...after].filter(([name, data]) => Buffer.compare(Buffer.from(data), Buffer.from(before.get(name))) !== 0).map(([n]) => n);
    // Corrupt the half of the 4 KiB header that the rename rewrote.
    const torn = after.get(slot).slice();
    const record = [0, 2048].find((at) => Buffer.compare(Buffer.from(torn.subarray(at, at + 2048)), Buffer.from(before.get(slot).subarray(at, at + 2048))) !== 0);
    torn.fill(0xee, record + 24, record + 27); // inside the checksummed path
    const view = await openOpfsFileSystem(crashed(dir, new Map([[slot, torn]])));
    assert.deepEqual(view.paths(), ["/db/a"]);
    assert.equal(readAll(view, "/db/a"), "A");
    view.close();
}

// Deleting is all-or-nothing and frees the slot's data.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 2, reserve: 1 });
    fs.write("/db/big", new Uint8Array(100_000), false);
    fs.remove("/db/big", false);
    assert.ok([...dir.files.values()].every((file) => file.durable.length <= 4096));
    const view = await openOpfsFileSystem(crashed(dir));
    assert.deepEqual(view.paths(), []);
    view.close();
}

// Capacity: exhaustion is reported, never silently lost; the pool grows.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 2, reserve: 0 });
    assert.equal(fs.write("/db/1", bytes("1"), false), 0);
    assert.equal(fs.write("/db/2", bytes("2"), false), 0);
    assert.equal(fs.write("/db/3", bytes("3"), false), status.noSpace);
    assert.equal(fs.size("/db/3"), status.notFound);
    await fs.ensureCapacity(3);
    assert.equal(fs.stats().free, 3);
    assert.equal(fs.write("/db/3", bytes("3"), false), 0);
    fs.close();
    const reopened = await openOpfsFileSystem(dir, { initialSlots: 2, reserve: 1 });
    assert.deepEqual(reopened.paths(), ["/db/1", "/db/2", "/db/3"]);
    assert.ok(reopened.stats().free >= 1);
    // One tab at a time: a second pool cannot open the same slots.
    await assert.rejects(openOpfsFileSystem(dir), /OpfsBusy/);
    reopened.close();
    await destroyOpfsFileSystem({ removeEntry: async () => {} }, "pool");
}

// A create that fails partway (quota) returns its slot to the pool.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 2, reserve: 0 });
    faults.quota = true;
    for (let i = 0; i < 5; i++) assert.equal(fs.write("/db/x", bytes("x"), false), status.noSpace);
    faults.quota = false;
    assert.equal(fs.stats().free, 2, "failed creates do not leak slots");
    assert.equal(fs.size("/db/x"), status.notFound);
    assert.equal(fs.write("/db/x", bytes("x"), false), 0);
    fs.close();
}

// Overlapping capacity requests are serialized instead of racing for a file.
{
    const dir = directory();
    const fs = await openOpfsFileSystem(dir, { initialSlots: 1, reserve: 0 });
    await Promise.all([fs.ensureCapacity(4), fs.ensureCapacity(6), fs.ensureCapacity(5)]);
    assert.equal(fs.stats().free, 6);
    assert.equal(new Set(dir.files.keys()).size, fs.stats().slots);
    fs.close();
}

console.log("antfly embedded OPFS file system tests passed");
