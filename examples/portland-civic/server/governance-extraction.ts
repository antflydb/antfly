import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { inference, readerModel } from "./governance-models.ts";

export function textQuality(text: string): string[] {
  const flags: string[] = [];
  const words = text.match(/[\p{L}\p{N}]+/gu) || [];
  if (text.replace(/\s/g, "").length < 40) flags.push("insufficient-text");
  if ((text.match(/�/g)?.length || 0) / Math.max(text.length, 1) > 0.01)
    flags.push("replacement-characters");
  if (
    words.length > 30 &&
    words.filter((w) => w.length === 1 && /\p{L}/u.test(w)).length /
      words.length >
      0.45
  )
    flags.push("fragmented-text");
  return flags;
}

export async function cachedInference<T>(
  stage: string,
  request: unknown,
  compute: () => Promise<T>,
): Promise<T> {
  const key = createHash("sha256")
    .update(JSON.stringify(request))
    .digest("hex");
  const path = `data/governance/cache/${stage}-${key}.json`;
  try {
    return JSON.parse(await readFile(path, "utf8")) as T;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const result = await compute();
  await mkdir("data/governance/cache", { recursive: true });
  await writeFile(`${path}.partial`, JSON.stringify(result));
  await rename(`${path}.partial`, path);
  return result;
}

export async function readPage(image: Buffer, embeddedText: string) {
  const quality_flags = textQuality(embeddedText);
  if (!quality_flags.length)
    return {
      text: embeddedText,
      extraction: "native-text" as const,
      quality_flags,
    };
  const images = [{ url: `data:image/png;base64,${image.toString("base64")}` }];
  const read = async (prompt: string) => {
    const request = { model: readerModel, images, prompt, max_tokens: 768 };
    const result = await cachedInference("florence-v1", request, () =>
      inference<{ data: { text: string }[] }>("read", request),
    );
    if (typeof result.data?.[0]?.text !== "string")
      throw new Error("Florence returned no page text");
    return result.data[0].text.trim();
  };
  const ocr = await read("<OCR>");
  if (!textQuality(ocr).length)
    return {
      text: ocr,
      extraction: "florence-ocr" as const,
      quality_flags,
      reader_model: readerModel,
    };
  // A caption aids discovery but is never promoted to verbatim source text.
  const caption = await read("<CAPTION>");
  return {
    text: embeddedText,
    extraction: "unreadable" as const,
    quality_flags: [...quality_flags, "ocr-insufficient"],
    caption,
    reader_model: readerModel,
  };
}
