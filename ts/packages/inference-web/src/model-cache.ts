// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import type { CatalogFile, CatalogModel, Progress } from "./index.js";

const DIRECTORY = "antfly-inference-models-v1";
const DATABASE = "antfly-inference-catalog-v1";
const MAX_FILE = 1024 ** 3;
function request<T>(value: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    value.onsuccess = () => resolve(value.result);
    value.onerror = () => reject(value.error);
  });
}
async function db() {
  const opening = indexedDB.open(DATABASE, 1);
  opening.onupgradeneeded = () => opening.result.createObjectStore("files");
  return request(opening);
}
async function metadata(key: string, value?: unknown) {
  const database = await db();
  try {
    const tx = database.transaction("files", value === undefined ? "readonly" : "readwrite");
    const store = tx.objectStore("files");
    const result = request(value === undefined ? store.get(key) : store.put(value, key));
    const completion = new Promise<void>((resolve, reject) => {
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
    const [storedResult] = await Promise.all([result, completion]);
    return storedResult;
  } finally {
    database.close();
  }
}
function validatePin(pin: CatalogFile) {
  if (
    !/^[a-f0-9]{64}$/.test(pin.sha256) ||
    !Number.isSafeInteger(pin.size_bytes) ||
    pin.size_bytes < 1 ||
    pin.size_bytes > MAX_FILE
  )
    throw new Error("Invalid catalog file pin");
  if (
    pin.path.startsWith("/") ||
    pin.path.includes("\\") ||
    pin.path.split("/").some((part) => !part || part === "..")
  )
    throw new Error("Unsafe catalog path");
  const url = new URL(pin.url, location.href);
  if (url.protocol !== "https:" && url.origin !== location.origin)
    throw new Error("Catalog downloads require HTTPS");
}
export async function downloadCatalogModel(
  model: CatalogModel,
  options: {
    signal?: AbortSignal;
    onProgress?: (p: Progress) => void;
    cache?: boolean;
    assets?: string;
  } = {}
) {
  if (!model.files.length)
    throw new Error(
      "This catalog precision has not been published yet. Load a local bundle instead."
    );
  const paths = new Set<string>();
  let total = 0;
  for (const pin of model.files) {
    validatePin(pin);
    if (paths.has(pin.path)) throw new Error("Duplicate catalog path");
    paths.add(pin.path);
    total += pin.size_bytes;
  }
  if (total > 1536 * 1024 ** 2) throw new Error("Catalog bundle exceeds browser memory budget");
  const worker = new Worker(
    new URL(
      "runtime/extraction-hash-worker.js",
      new URL(options.assets ?? "/inference/", location.href)
    ),
    { type: "module" }
  );
  let rejectHash: ((error: unknown) => void) | undefined;
  const abort = () => {
    worker.terminate();
    rejectHash?.(new DOMException("Download cancelled", "AbortError"));
  };
  options.signal?.addEventListener("abort", abort, { once: true });
  const hash = (file: Blob) =>
    new Promise<string>((resolve, reject) => {
      options.signal?.throwIfAborted();
      rejectHash = reject;
      worker.onmessage = ({ data }) =>
        data.error ? reject(new Error(data.error)) : resolve(data.hash);
      worker.onerror = (event) => reject(new Error(event.message));
      worker.onmessageerror = () => reject(new Error("Hash worker message failed"));
      worker.postMessage({ file });
    });
  let directory: FileSystemDirectoryHandle | undefined;
  try {
    options.signal?.throwIfAborted();
    if (options.cache !== false) {
      try {
        directory = await (await navigator.storage.getDirectory()).getDirectoryHandle(DIRECTORY, {
          create: true,
        });
      } catch {
        /* Private browsing/quota/security restrictions: session only. */
      }
    }
    const files = new Map<string, Blob>();
    for (const pin of model.files) {
      options.signal?.throwIfAborted();
      let file: Blob | undefined;
      if (directory) {
        try {
          if (await metadata(pin.sha256)) {
            const candidate = await (await directory.getFileHandle(pin.sha256)).getFile();
            if (candidate.size === pin.size_bytes && (await hash(candidate)) === pin.sha256)
              file = candidate;
          }
        } catch {
          options.signal?.throwIfAborted();
        }
      }
      if (!file) {
        const fetchFile = async (persist: boolean): Promise<Blob> => {
          const response = await fetch(pin.url, {
            signal: options.signal,
            credentials: "omit",
            referrerPolicy: "no-referrer",
          });
          if (!response.ok || !response.body)
            throw new Error(`Model download failed (${response.status})`);
          const reader = response.body.getReader();
          const parts: Uint8Array<ArrayBuffer>[] = [];
          let writer: FileSystemWritableFileStream | undefined;
          let handle: FileSystemFileHandle | undefined;
          let size = 0;
          try {
            if (persist && directory) {
              handle = await directory.getFileHandle(`${pin.sha256}.partial`, { create: true });
              writer = await handle.createWritable();
            }
            for (;;) {
              const { value, done } = await reader.read();
              if (done) break;
              size += value.byteLength;
              if (size > pin.size_bytes) throw new Error("Model download exceeds pinned size");
              if (writer) await writer.write(value);
              else parts.push(value);
              options.onProgress?.({
                stage: "download",
                file: pin.path,
                loaded: size,
                total: pin.size_bytes,
              });
            }
            if (size !== pin.size_bytes) throw new Error("Truncated model download");
            if (writer) {
              await writer.close();
              writer = undefined;
              return await handle!.getFile();
            }
            return new Blob(parts);
          } catch (error) {
            await writer?.abort().catch(() => {});
            await reader.cancel().catch(() => {});
            if (persist) await directory?.removeEntry(`${pin.sha256}.partial`).catch(() => {});
            throw error;
          } finally {
            reader.releaseLock();
          }
        };
        try {
          file = await fetchFile(Boolean(directory));
        } catch (error) {
          if (
            !directory ||
            options.signal?.aborted ||
            !(error instanceof DOMException) ||
            !["QuotaExceededError", "NotAllowedError", "InvalidStateError"].includes(error.name)
          )
            throw error;
          directory = undefined;
          file = await fetchFile(false);
        }
        try {
          if ((await hash(file)) !== pin.sha256)
            throw new Error(`Integrity check failed: ${pin.path}`);
        } catch (error) {
          await directory?.removeEntry(`${pin.sha256}.partial`).catch(() => {});
          throw error;
        }
        if (directory) {
          try {
            const target = await directory.getFileHandle(pin.sha256, { create: true });
            await file.stream().pipeTo(await target.createWritable(), { signal: options.signal });
            await metadata(pin.sha256, {
              path: pin.path,
              size: pin.size_bytes,
              verifiedAt: Date.now(),
            });
            file = await target.getFile();
            await directory.removeEntry(`${pin.sha256}.partial`);
          } catch {
            options.signal?.throwIfAborted(); /* Verified session file still works. */
          }
        }
      }
      files.set(pin.path, file);
    }
    return files;
  } finally {
    worker.terminate();
    options.signal?.removeEventListener("abort", abort);
  }
}
export async function clearModelCache() {
  const root = await navigator.storage.getDirectory();
  await root.removeEntry(DIRECTORY, { recursive: true }).catch((error) => {
    if (error.name !== "NotFoundError") throw error;
  });
  await request(indexedDB.deleteDatabase(DATABASE));
}
