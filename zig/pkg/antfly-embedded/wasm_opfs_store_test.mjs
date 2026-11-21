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

// OPFS checkpoint store against an in-memory directory and a fake tracked
// database, without WASM. The real module is exercised by wasm_smoke_run.mjs.
import assert from "node:assert/strict";
import { openOpfsStore, destroyOpfsStore } from "./wasm_opfs_store.mjs";

const notFound = () => Object.assign(new Error("missing"), { name: "NotFoundError" });
function directory(faults = {}) {
    const entries = new Map();
    return {
        entries: () => entries.entries(),
        raw: entries,
        async getDirectoryHandle(name, options = {}) {
            if (!entries.has(name)) {
                if (!options.create) throw notFound();
                entries.set(name, directory(faults));
            }
            return entries.get(name);
        },
        async getFileHandle(name, options = {}) {
            if (!entries.has(name)) {
                if (!options.create) throw notFound();
                entries.set(name, { bytes: new Uint8Array(0) });
            }
            const file = entries.get(name);
            return {
                async getFile() {
                    return new Blob([file.bytes]);
                },
                async createWritable() {
                    const parts = [];
                    return {
                        async write(bytes) {
                            parts.push(new Uint8Array(bytes));
                        },
                        async close() {
                            if (faults.failWrite?.(name)) throw new Error(`injected write failure: ${name}`);
                            const bytes = new Uint8Array(await new Blob(parts).arrayBuffer());
                            // A torn write keeps only a prefix of the new contents.
                            file.bytes = faults.tear?.(name) ? bytes.slice(0, Math.floor(bytes.length / 2)) : bytes;
                        },
                        async abort() {},
                    };
                },
            };
        },
        async removeEntry(name) {
            if (!entries.delete(name)) throw notFound();
        },
    };
}

// Mimics the tracked-storage exports of wasm_client.mjs.
function fakeDatabase() {
    let files = new Map(); // path -> { bytes, version }
    let next = 1;
    let open = false;
    return {
        files: () => files,
        storageReset() {
            files = new Map();
            open = false;
        },
        storageImport(path, bytes) {
            assert.equal(open, false, "imports happen before open");
            files.set(path, { bytes: new Uint8Array(bytes), version: next++ });
        },
        openTracked() {
            open = true;
        },
        storageManifest() {
            return [...files].map(([path, f]) => ({ path, size: f.bytes.length, version: f.version }));
        },
        storageFile(path, size) {
            const file = files.get(path);
            if (!file || (size != null && file.bytes.length !== size)) return null;
            return file.bytes.slice();
        },
        write(path, text) {
            files.set(path, { bytes: new TextEncoder().encode(text), version: next++ });
        },
        remove(path) {
            files.delete(path);
        },
        read(path) {
            const file = files.get(path);
            return file ? new TextDecoder().decode(file.bytes) : undefined;
        },
    };
}
const blobNames = (root) => [...root.raw.get("blobs").raw.keys()].sort();

// Empty store opens an empty database.
{
    const root = directory();
    const db = fakeDatabase();
    const store = await openOpfsStore(root);
    assert.deepEqual(await store.load(db), { restored: false, generation: 0, metadata: null });

    db.write("/db/wal", "a");
    db.write("/db/seg-1", "segment one");
    const first = await store.checkpoint(db, { cursor: "1" });
    assert.deepEqual([first.generation, first.written, first.files], [1, 2, 2]);
    // Unchanged files are not rewritten.
    assert.equal((await store.checkpoint(db, { cursor: "1" })).written, 0);
    db.write("/db/wal", "ab");
    db.remove("/db/seg-1");
    db.write("/db/seg-2", "segment two");
    const third = await store.checkpoint(db, { cursor: "2" });
    assert.deepEqual([third.generation, third.written, third.files], [3, 2, 2]);
    // Blobs of the two newest generations remain: gen 3 (2) + gen 2's wal and seg-1.
    assert.equal(blobNames(root).length, 4);

    const reopened = fakeDatabase();
    const loaded = await (await openOpfsStore(root)).load(reopened);
    assert.deepEqual([loaded.restored, loaded.generation, loaded.metadata.cursor], [true, 3, "2"]);
    assert.equal(reopened.read("/db/wal"), "ab");
    assert.equal(reopened.read("/db/seg-1"), undefined, "deleted files stay deleted");
    assert.equal(reopened.read("/db/seg-2"), "segment two");
}

// A torn newest manifest falls back to the previous complete generation.
{
    let tearManifest = false;
    const root = directory({ tear: (name) => tearManifest && name.startsWith("manifest-") });
    const db = fakeDatabase();
    const store = await openOpfsStore(root);
    await store.load(db);
    db.write("/db/wal", "v1");
    await store.checkpoint(db, { cursor: "a" });
    db.write("/db/wal", "v2");
    tearManifest = true;
    await store.checkpoint(db, { cursor: "b" });
    tearManifest = false;
    const reopened = fakeDatabase();
    const loaded = await (await openOpfsStore(root)).load(reopened);
    assert.deepEqual([loaded.generation, loaded.metadata.cursor], [1, "a"]);
    assert.equal(reopened.read("/db/wal"), "v1");
}

// A failure while writing blobs leaves the previous generation authoritative,
// and a retried checkpoint succeeds.
{
    let failBlobs = false;
    const root = directory({ failWrite: (name) => failBlobs && name.startsWith("g") });
    const db = fakeDatabase();
    const store = await openOpfsStore(root);
    await store.load(db);
    db.write("/db/wal", "kept");
    await store.checkpoint(db, { cursor: "1" });
    db.write("/db/wal", "lost");
    failBlobs = true;
    await assert.rejects(store.checkpoint(db, { cursor: "2" }), /injected write failure/);
    assert.equal(store.generation, 1);
    const reopened = fakeDatabase();
    assert.equal((await (await openOpfsStore(root)).load(reopened)).metadata.cursor, "1");
    assert.equal(reopened.read("/db/wal"), "kept");
    failBlobs = false;
    assert.equal((await store.checkpoint(db, { cursor: "2" })).generation, 2);
    const retried = fakeDatabase();
    assert.equal((await (await openOpfsStore(root)).load(retried)).metadata.cursor, "2");
    assert.equal(retried.read("/db/wal"), "lost");
}

// A manifest whose blobs are missing is skipped; with no complete generation
// the database opens empty, and orphaned blobs are reclaimed by the next load.
{
    const root = directory();
    const db = fakeDatabase();
    const store = await openOpfsStore(root);
    await store.load(db);
    db.write("/db/wal", "x");
    await store.checkpoint(db, {});
    for (const name of blobNames(root)) await root.raw.get("blobs").removeEntry(name);
    const reopened = fakeDatabase();
    assert.equal((await (await openOpfsStore(root)).load(reopened)).restored, false);
    assert.equal(reopened.files().size, 0);
}

// destroy removes the whole store and tolerates a missing one.
{
    const parent = directory();
    await parent.getDirectoryHandle("mail", { create: true });
    await destroyOpfsStore(parent, "mail");
    await destroyOpfsStore(parent, "mail");
    assert.equal(parent.raw.has("mail"), false);
}

console.log("antfly embedded OPFS store tests passed");
