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

// Checkpoints an embedded Antfly WASM database into an Origin Private File
// System (OPFS) directory, or any object with the same async handle API.
//
// Layout under the supplied directory:
//   blobs/<id>        immutable copies of database files
//   manifest-0.json   A/B generation slots mapping database paths to blobs
//   manifest-1.json   plus caller metadata (e.g. a sync cursor)
//
// A checkpoint writes new blobs first, then the next manifest slot, then
// deletes blobs referenced by neither of the two newest generations. A torn or
// missing manifest falls back to the other slot, so a crash at any point leaves
// at least one complete generation. OPFS does not document crash-atomic renames, so this
// store never relies on rename; callers should still treat the copy as a
// rebuildable cache and call `destroy()` + resync when `load()` reports
// nothing usable.

const FORMAT = 1;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function sha256Hex(text) {
    const digest = await crypto.subtle.digest("SHA-256", encoder.encode(text));
    return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function readFileBytes(dir, name) {
    try {
        const handle = await dir.getFileHandle(name);
        const file = await handle.getFile();
        return new Uint8Array(await file.arrayBuffer());
    } catch (error) {
        if (error?.name === "NotFoundError") return null;
        throw error;
    }
}

async function writeFileBytes(dir, name, bytes) {
    const handle = await dir.getFileHandle(name, { create: true });
    if (typeof handle.createWritable === "function") {
        // Chromium and Firefox stage createWritable output and swap it in on close.
        const writable = await handle.createWritable({ keepExistingData: false });
        try {
            await writable.write(bytes);
            await writable.close();
        } catch (error) {
            await writable.abort?.().catch(() => {});
            throw error;
        }
        return;
    }
    // Safari workers: synchronous access handles are the portable write path.
    const access = await handle.createSyncAccessHandle();
    try {
        access.truncate(0);
        access.write(bytes, { at: 0 });
        access.flush();
    } finally {
        access.close();
    }
}

async function removeEntry(dir, name, options) {
    try {
        await dir.removeEntry(name, options);
    } catch (error) {
        if (error?.name !== "NotFoundError") throw error;
    }
}

async function listNames(dir) {
    const names = [];
    for await (const [name] of dir.entries()) names.push(name);
    return names;
}

/**
 * @param {FileSystemDirectoryHandle} directory dedicated directory for one database
 */
export async function openOpfsStore(directory) {
    const blobs = await directory.getDirectoryHandle("blobs", { create: true });
    let generation = 0;
    let files = new Map(); // database path -> { blob, size, version }
    let metadata = null;
    let nextBlob = 0;

    async function readManifest(slot) {
        const bytes = await readFileBytes(directory, `manifest-${slot}.json`);
        if (!bytes) return null;
        try {
            const envelope = JSON.parse(decoder.decode(bytes));
            if (envelope?.format !== FORMAT || typeof envelope.body !== "string") return null;
            if ((await sha256Hex(envelope.body)) !== envelope.sha256) return null;
            const body = JSON.parse(envelope.body);
            if (!Number.isSafeInteger(body.generation) || body.generation < 1) return null;
            return body;
        } catch {
            return null;
        }
    }

    async function writeManifest(body) {
        const text = JSON.stringify(body);
        const envelope = JSON.stringify({ format: FORMAT, sha256: await sha256Hex(text), body: text });
        await writeFileBytes(directory, `manifest-${body.generation % 2}.json`, encoder.encode(envelope));
    }

    async function collect(referenced) {
        for (const name of await listNames(blobs)) {
            if (!referenced.has(name)) await removeEntry(blobs, name);
        }
    }

    return {
        get generation() {
            return generation;
        },
        get metadata() {
            return metadata;
        },

        /**
         * Restore the newest complete generation into `api` (a wasm_client
         * instance) and open it. Returns `{ restored, metadata }`; a store with
         * no usable generation opens an empty database.
         */
        async load(api) {
            const candidates = (await Promise.all([readManifest(0), readManifest(1)]))
                .filter(Boolean)
                .sort((a, b) => b.generation - a.generation);
            for (const candidate of candidates) {
                api.storageReset();
                let complete = true;
                for (const [path, entry] of Object.entries(candidate.files)) {
                    const bytes = await readFileBytes(blobs, entry.blob);
                    if (!bytes || bytes.length !== entry.size) {
                        complete = false;
                        break;
                    }
                    api.storageImport(path, bytes);
                }
                if (!complete) continue;
                // Versions are assigned on import; bind them to the restored blobs.
                const restored = new Map();
                for (const file of api.storageManifest()) {
                    const entry = candidate.files[file.path];
                    if (entry) restored.set(file.path, { blob: entry.blob, size: entry.size, version: file.version });
                }
                api.openTracked();
                generation = candidate.generation;
                files = restored;
                metadata = candidate.metadata ?? null;
                nextBlob = 0;
                // Keep every blob a valid manifest names, so the older slot
                // remains a usable fallback.
                const keep = new Set();
                for (const manifest of candidates) for (const entry of Object.values(manifest.files)) keep.add(entry.blob);
                await collect(keep);
                return { restored: true, generation, metadata };
            }
            api.storageReset();
            api.openTracked();
            generation = 0;
            files = new Map();
            metadata = null;
            return { restored: false, generation: 0, metadata: null };
        },

        /**
         * Persist every file changed since the previous checkpoint together
         * with `nextMetadata`. Call between API operations, never concurrently.
         */
        async checkpoint(api, nextMetadata = metadata) {
            const next = generation + 1;
            const current = new Map();
            let written = 0;
            let bytesWritten = 0;
            for (const file of api.storageManifest()) {
                const previous = files.get(file.path);
                if (previous && previous.version === file.version && previous.size === file.size) {
                    current.set(file.path, previous);
                    continue;
                }
                const bytes = api.storageFile(file.path, file.size);
                if (!bytes) throw new Error(`database file changed during checkpoint: ${file.path}`);
                const blob = `g${next}-${nextBlob++}`;
                await writeFileBytes(blobs, blob, bytes);
                current.set(file.path, { blob, size: file.size, version: file.version });
                written++;
                bytesWritten += bytes.length;
            }
            const body = {
                generation: next,
                created_at: new Date().toISOString(),
                metadata: nextMetadata ?? null,
                files: Object.fromEntries([...current].map(([path, e]) => [path, { blob: e.blob, size: e.size }])),
            };
            await writeManifest(body);
            // The slot just overwritten held generation next-2; keep the blobs of
            // the new generation and of its predecessor, which is the fallback.
            const keep = new Set([...current.values(), ...files.values()].map((entry) => entry.blob));
            generation = next;
            files = current;
            metadata = body.metadata;
            nextBlob = 0;
            await collect(keep);
            return { generation, written, bytesWritten, files: current.size };
        },

        /** Total bytes referenced by the current generation. */
        size() {
            let total = 0;
            for (const entry of files.values()) total += entry.size;
            return total;
        },
    };
}

/** Remove a store directory entirely (sign-out, opt-out, or corrupt copy). */
export async function destroyOpfsStore(parent, name) {
    await removeEntry(parent, name, { recursive: true });
}
