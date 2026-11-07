import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { chmod, link, lstat, mkdir, open, realpath, rm } from "node:fs/promises";
import { join } from "node:path";

async function syncDirectory(path: string): Promise<void> {
  const directory = await open(
    path,
    constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW
  );
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

async function verifyExistingObject(
  path: string,
  digest: string,
  size: number,
  check: () => void
): Promise<boolean> {
  let existing: Awaited<ReturnType<typeof open>>;
  try {
    existing = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
  try {
    const info = await existing.stat();
    if (!info.isFile() || info.size !== size)
      throw new Error(
        "Existing local object differs from its content address; repair it before retrying"
      );
    const hash = createHash("sha256");
    const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, size));
    let position = 0;
    while (position < info.size) {
      check();
      const { bytesRead } = await existing.read(
        buffer,
        0,
        Math.min(buffer.length, info.size - position),
        position
      );
      if (!bytesRead) throw new Error("Existing local object was truncated");
      hash.update(buffer.subarray(0, bytesRead));
      position += bytesRead;
    }
    if (hash.digest("hex") !== digest)
      throw new Error(
        "Existing local object differs from its content address; repair it before retrying"
      );
    if ((info.mode & 0o077) !== 0) await existing.chmod(0o600);
    await existing.sync();
  } finally {
    await existing.close();
  }
  return true;
}

/** Immutable, private, durable publication; concurrent writers never replace an intact object. */
export async function publishLocalObject(
  root: string,
  prefix: string,
  filename: string,
  digest: string,
  data: Uint8Array,
  check: () => void,
  signal?: AbortSignal
): Promise<void> {
  let directory = await realpath(root);
  check();
  const directories = [directory];
  const segments = [...(prefix ? prefix.split("/") : []), "objects", "sha256"];
  for (const [index, segment] of segments.entries()) {
    const next = join(directory, segment);
    try {
      await mkdir(next, { mode: 0o700 });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    const info = await lstat(next);
    if (!info.isDirectory()) throw new Error("Local object store path is not a directory");
    // Tighten the actual store, including directories created by older SDKs.
    if (index >= segments.length - 2 && (info.mode & 0o077) !== 0) await chmod(next, 0o700);
    check();
    directory = next;
    directories.push(directory);
  }
  const path = join(directory, filename);
  // Repeated uploads avoid rewriting and flushing an already verified object.
  if (!(await verifyExistingObject(path, digest, data.byteLength, check))) {
    const temporary = join(directory, `.upload-${randomUUID()}`);
    const file = await open(temporary, "wx", 0o600);
    try {
      try {
        await file.writeFile(data, { signal });
        check();
        await file.sync();
      } finally {
        await file.close();
      }
      check();
      try {
        await link(temporary, path);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        if (!(await verifyExistingObject(path, digest, data.byteLength, check))) throw error;
      }
    } finally {
      await rm(temporary, { force: true });
    }
  }
  // Persist both the final name and removal of the private temporary name.
  for (const parent of directories.reverse()) {
    await syncDirectory(parent);
    check();
  }
}
