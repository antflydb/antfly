// Download immutable reference artifacts for offline parity tests. No model is
// automatically downloaded by the app or its build.
import { readFile, mkdir, rename, unlink } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { createHash } from "node:crypto";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { resolve, dirname } from "node:path";
const [id, output] = process.argv.slice(2);
if (!id || !output)
  throw new Error("Usage: node scripts/fetch-reference.mjs <catalog-id> <output-directory>");
const catalog = JSON.parse(await readFile(new URL("../src/catalog.json", import.meta.url), "utf8"));
const model = catalog.find((item) => item.id === id);
if (!model?.files.length) throw new Error("No published reference files for this catalog row");
await mkdir(resolve(output), { recursive: false });
for (const file of model.files) {
  const target = resolve(output, file.path),
    temporary = target + ".partial";
  await mkdir(dirname(target), { recursive: true });
  const response = await fetch(file.url);
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${file.path}`);
  let bytes = 0;
  const hash = createHash("sha256");
  try {
    await pipeline(
      Readable.fromWeb(response.body),
      new Transform({
        transform(chunk, _, callback) {
          bytes += chunk.length;
          hash.update(chunk);
          callback(bytes > file.size_bytes ? new Error("Pinned size exceeded") : null, chunk);
        },
      }),
      createWriteStream(temporary, { flags: "wx" })
    );
    if (bytes !== file.size_bytes || hash.digest("hex") !== file.sha256)
      throw new Error(`Hash mismatch: ${file.path}`);
    await rename(temporary, target);
    console.log(`Verified ${file.path}: ${bytes} bytes`);
  } catch (error) {
    await unlink(temporary).catch(() => {});
    throw error;
  }
}
