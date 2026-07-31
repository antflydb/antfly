export const MODEL_FORMAT_GGUF = "gguf";

export const DEFAULT_TEXT_EMBEDDING_MODEL = "antflydb/clipclap:gguf:Q4_K";
export const DEFAULT_IMAGE_EMBEDDING_MODEL = "antflydb/clipclap:gguf:Q4_K";
export const DEFAULT_EXTRACTOR_MODEL = "antflydb/gliner2-base-v1:gguf:Q4_K";
export const DEFAULT_RERANKER_MODEL = "antflydb/mxbai-rerank-base-v1";

export interface SupportedModel {
  name: string;
  task: "embedder" | "extractor" | "reranker";
  defaultFormat?: string;
  defaultVariant?: string;
}

export interface ModelRef {
  owner: string;
  repo: string;
  format?: string;
  variant?: string;
}

export const SUPPORTED_MODELS: readonly SupportedModel[] = Object.freeze([
  {
    name: DEFAULT_TEXT_EMBEDDING_MODEL,
    task: "embedder",
    defaultFormat: MODEL_FORMAT_GGUF,
    defaultVariant: "Q4_K",
  },
  {
    name: DEFAULT_EXTRACTOR_MODEL,
    task: "extractor",
    defaultFormat: MODEL_FORMAT_GGUF,
    defaultVariant: "Q4_K",
  },
  {
    name: DEFAULT_RERANKER_MODEL,
    task: "reranker",
    defaultFormat: MODEL_FORMAT_GGUF,
    defaultVariant: "Q4_K",
  },
]);

export interface ModelPullProgress {
  model: string;
  file: string;
  downloaded: number;
  total: number;
  fileDownloaded: number;
  fileTotal: number;
  blobsDone: number;
  blobsTotal: number;
  resumedBytes: number;
}

export interface ModelPullOptions {
  modelsDir?: string;
  variant?: string;
  huggingFaceToken?: string;
  huggingFaceBaseUrl?: string;
  fetch?: typeof fetch;
  diskFreeBytes?: (dir: string) => number | Promise<number>;
  onProgress?: (progress: ModelPullProgress) => void;
}

export class ModelPullError extends Error {
  constructor(
    public readonly code: string,
    message: string
  ) {
    super(message);
    this.name = "ModelPullError";
  }
}

interface HuggingFaceTreeEntry {
  path: string;
  type?: string;
  size?: number;
}

export function isSupportedModel(model: string): boolean {
  return supportedModel(model) !== undefined;
}

function supportedModel(model: string): SupportedModel | undefined {
  let ref: ModelRef;
  try {
    ref = parseModelRef(model);
  } catch {
    return undefined;
  }
  return SUPPORTED_MODELS.find((m) => {
    const metaRef = parseModelRef(m.name);
    return metaRef.owner === ref.owner && metaRef.repo === ref.repo;
  });
}

export function defaultModelsDir(): string {
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) throw new Error("HOME is not set");
  return `${home}/.antfly/inference/models`;
}

export function modelDir(modelsDir: string | undefined, model: string): string {
  const ref = parseModelRef(model);
  const root = modelsDir || defaultModelsDir();
  return `${root.replace(/\/+$/, "")}/${ref.owner}/${ref.repo}`;
}

export async function pullHuggingFaceModel(
  model: string,
  options: ModelPullOptions = {}
): Promise<string> {
  const meta = supportedModel(model);
  if (!meta) throw new Error(`unsupported Antfly model ${JSON.stringify(model)}`);

  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const ref = parseModelRef(model);
  const repoId = baseModelName(model);
  const targetDir = modelDir(options.modelsDir, model);
  await fs.mkdir(targetDir, { recursive: true });

  const format = ref.format || meta.defaultFormat || "";
  const variant = options.variant?.trim() || ref.variant || meta.defaultVariant || "";
  const baseUrl = (options.huggingFaceBaseUrl || "https://huggingface.co").replace(/\/+$/, "");
  const fetchImpl = options.fetch || fetch;
  const entries = await listHuggingFaceFiles(fetchImpl, baseUrl, repoId, options.huggingFaceToken);
  const selected = selectModelFiles(entries, format, variant);
  if (selected.length === 0) throw new Error(`no downloadable files found for ${model}`);

  const totalBytes = selected.reduce((sum, entry) => sum + (entry.size || 0), 0);
  let completedBytes = 0;
  let resumedBytes = 0;
  let blobsDone = 0;
  const files: Array<{ name: string; size: number }> = [];
  for (const entry of selected) {
    const { destPath } = localModelFile(path, targetDir, entry.path);
    const expectedSize = entry.size || 0;
    try {
      const stat = await fs.stat(destPath);
      if (expectedSize > 0 && stat.size === expectedSize) {
        resumedBytes += stat.size;
        blobsDone += 1;
      }
    } catch {
      // Missing file; download below.
    }
  }
  completedBytes = resumedBytes;
  const neededBytes = Math.max(0, totalBytes - resumedBytes);
  if (options.diskFreeBytes && neededBytes > 0) {
    const free = await options.diskFreeBytes(targetDir);
    if (free > 0) {
      const required = neededBytes + Math.floor(neededBytes / 5);
      if (free < required) {
        throw new ModelPullError(
          "disk_space",
          `need ${formatBytes(required)} free on ${targetDir}, only ${formatBytes(free)} available`
        );
      }
    }
  }
  reportProgress(options, {
    model: repoId,
    file: "",
    downloaded: completedBytes,
    total: totalBytes,
    fileDownloaded: 0,
    fileTotal: 0,
    blobsDone,
    blobsTotal: selected.length,
    resumedBytes,
  });
  for (const entry of selected) {
    const { name: destName, destPath } = localModelFile(path, targetDir, entry.path);
    const expectedSize = entry.size || 0;
    try {
      const stat = await fs.stat(destPath);
      if (expectedSize > 0 && stat.size === expectedSize) {
        files.push({ name: destName, size: stat.size });
        reportProgress(options, {
          model: repoId,
          file: destName,
          downloaded: completedBytes,
          total: totalBytes,
          fileDownloaded: 0,
          fileTotal: expectedSize,
          blobsDone,
          blobsTotal: selected.length,
          resumedBytes,
        });
        continue;
      }
    } catch {
      // Missing file; download below.
    }
    await downloadFile(
      fetchImpl,
      baseUrl,
      repoId,
      entry,
      destPath,
      completedBytes,
      totalBytes,
      blobsDone,
      selected.length,
      resumedBytes,
      options
    );
    const stat = await fs.stat(destPath);
    completedBytes += stat.size;
    blobsDone += 1;
    files.push({ name: destName, size: stat.size });
    reportProgress(options, {
      model: repoId,
      file: destName,
      downloaded: completedBytes,
      total: totalBytes,
      fileDownloaded: stat.size,
      fileTotal: expectedSize,
      blobsDone,
      blobsTotal: selected.length,
      resumedBytes,
    });
  }

  const manifest = {
    schemaVersion: 1,
    name: ref.repo,
    owner: ref.owner,
    source: modelRefString({ ...ref, format, variant }),
    type: meta.task,
    ...(variant ? { variant } : {}),
    files,
    provenance: {
      downloadedFrom: "huggingface",
      downloadedAt: new Date().toISOString(),
    },
  };
  await fs.writeFile(
    path.join(targetDir, "model_manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`
  );
  return targetDir;
}

export function parseModelRef(model: string): ModelRef {
  const parts = model.split(":");
  if (parts.length > 3) {
    throw new Error(`model must be owner/repo[:format[:variant]], got ${JSON.stringify(model)}`);
  }
  const base = parts[0];
  if (!base) {
    throw new Error(`model must be owner/repo[:format[:variant]], got ${JSON.stringify(model)}`);
  }
  const nameParts = base.split("/");
  const owner = nameParts[0];
  const repo = nameParts[1];
  if (nameParts.length !== 2 || !validPathComponent(owner) || !validPathComponent(repo)) {
    throw new Error(`model must be owner/repo[:format[:variant]], got ${JSON.stringify(model)}`);
  }
  const ref: ModelRef = { owner, repo };
  if (parts.length === 2) {
    ref.variant = parts[1];
  } else if (parts.length === 3) {
    const format = parts[1];
    if (!format) {
      throw new Error(`model must be owner/repo[:format[:variant]], got ${JSON.stringify(model)}`);
    }
    ref.format = format.toLowerCase();
    ref.variant = parts[2];
  }
  return ref;
}

export function baseModelName(model: string): string {
  const ref = parseModelRef(model);
  return `${ref.owner}/${ref.repo}`;
}

export function modelRefString(ref: ModelRef): string {
  const out = `${ref.owner}/${ref.repo}`;
  if (ref.format && ref.variant) return `${out}:${ref.format}:${ref.variant}`;
  if (ref.variant) return `${out}:${ref.variant}`;
  return out;
}

function localModelFile(
  path: typeof import("node:path"),
  modelDir: string,
  remotePath: string
): { name: string; destPath: string } {
  const name = safeLocalModelRelPath(remotePath);
  return { name, destPath: path.join(modelDir, ...name.split("/")) };
}

function safeLocalModelRelPath(remotePath: string): string {
  const normalized = remotePath.trim().split("\\").join("/");
  if (!normalized || normalized.startsWith("/")) {
    throw new Error(`unsafe HuggingFace file path ${JSON.stringify(remotePath)}`);
  }
  const parts = normalized.split("/");
  if (parts.some((part) => !validPathComponent(part))) {
    throw new Error(`unsafe HuggingFace file path ${JSON.stringify(remotePath)}`);
  }
  return parts.join("/");
}

async function listHuggingFaceFiles(
  fetchImpl: typeof fetch,
  baseUrl: string,
  repoId: string,
  token?: string
): Promise<HuggingFaceTreeEntry[]> {
  const response = await fetchImpl(
    `${baseUrl}/api/models/${escapeRepoId(repoId)}/tree/main?recursive=1`,
    {
      headers: authHeaders(token),
    }
  );
  if (!response.ok)
    throw new Error(
      `listing HuggingFace files failed: ${response.status} ${await response.text()}`
    );
  return (await response.json()) as HuggingFaceTreeEntry[];
}

function selectModelFiles(
  entries: HuggingFaceTreeEntry[],
  format: string,
  variant: string
): HuggingFaceTreeEntry[] {
  const normalizedFormat = format.trim().toLowerCase();
  const normalizedVariant = normalizeVariant(variant);
  const support: HuggingFaceTreeEntry[] = [];
  const artifacts: HuggingFaceTreeEntry[] = [];
  const onnxData: HuggingFaceTreeEntry[] = [];
  for (const entry of entries) {
    if (entry.type && entry.type !== "file") continue;
    const base = entry.path.split("/").pop()?.toLowerCase() || "";
    if (/\.(json|txt|model|spm|tiktoken)$/.test(base)) {
      support.push(entry);
      continue;
    }
    if (base.endsWith(".onnx_data") || base.endsWith(".onnx.data")) {
      if (normalizedFormat !== MODEL_FORMAT_GGUF) onnxData.push(entry);
      continue;
    }
    if (base.endsWith(".onnx")) {
      if (normalizedFormat === MODEL_FORMAT_GGUF) continue;
      if (
        !normalizedVariant ||
        normalizeVariant(base).includes(normalizedVariant) ||
        !looksVariantFile(base)
      )
        artifacts.push(entry);
      continue;
    }
    if (base.endsWith(".gguf")) {
      if (normalizedFormat && normalizedFormat !== MODEL_FORMAT_GGUF) continue;
      if (!normalizedVariant || normalizeVariant(base).includes(normalizedVariant))
        artifacts.push(entry);
    }
  }
  if (artifacts.length === 0) return [];
  return [...artifacts, ...onnxData, ...support];
}

async function downloadFile(
  fetchImpl: typeof fetch,
  baseUrl: string,
  repoId: string,
  entry: HuggingFaceTreeEntry,
  destPath: string,
  completedBeforeFile: number,
  totalBytes: number,
  blobsDone: number,
  blobsTotal: number,
  resumedBytes: number,
  options: ModelPullOptions
): Promise<void> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const { createWriteStream } = await import("node:fs");
  const { once } = await import("node:events");
  const response = await fetchImpl(
    `${baseUrl}/${escapeRepoId(repoId)}/resolve/main/${escapeFilePath(entry.path)}`,
    {
      headers: authHeaders(options.huggingFaceToken),
    }
  );
  if (!response.ok)
    throw new Error(
      `downloading ${entry.path} failed: ${response.status} ${await response.text()}`
    );
  if (!response.body) throw new Error(`downloading ${entry.path} failed: empty response body`);

  let downloaded = 0;
  const reader = response.body.getReader();
  await fs.mkdir(path.dirname(destPath), { recursive: true });
  const tmpPath = `${destPath}.tmp`;
  const out = createWriteStream(tmpPath);
  let streamError: Error | undefined;
  out.on("error", (err) => {
    streamError = err;
  });
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      const chunk = Buffer.from(value);
      if (!out.write(chunk)) {
        await Promise.race([
          once(out, "drain"),
          once(out, "error").then(([err]) => {
            throw err;
          }),
        ]);
      }
      if (streamError) throw streamError;
      downloaded += value.byteLength;
      reportProgress(options, {
        model: repoId,
        file: entry.path.split("/").pop() || entry.path,
        downloaded: completedBeforeFile + downloaded,
        total: totalBytes,
        fileDownloaded: downloaded,
        fileTotal: entry.size || 0,
        blobsDone,
        blobsTotal,
        resumedBytes,
      });
    }
    await new Promise<void>((resolve, reject) => {
      if (streamError) {
        reject(streamError);
        return;
      }
      out.once("error", reject);
      out.end(() => resolve());
    });
  } catch (err) {
    out.destroy();
    await fs.rm(tmpPath, { force: true });
    throw err;
  }
  if (entry.size && downloaded !== entry.size) {
    await fs.rm(tmpPath, { force: true });
    throw new Error(`downloaded ${entry.path} size = ${downloaded}, want ${entry.size}`);
  }
  await fs.rename(tmpPath, destPath);
}

function authHeaders(token?: string): Record<string, string> {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

function escapeRepoId(repoId: string): string {
  return repoId.split("/").map(encodeURIComponent).join("/");
}

function escapeFilePath(filePath: string): string {
  return filePath.split("/").map(encodeURIComponent).join("/");
}

function normalizeVariant(value: string): string {
  return value.trim().toLowerCase().replace(/[.-]/g, "_");
}

function looksVariantFile(name: string): boolean {
  const normalized = normalizeVariant(name);
  return ["q4", "q5", "q6", "q8", "int8", "i8", "fp16", "f16", "bf16", "quantized"].some((marker) =>
    normalized.includes(marker)
  );
}

function validPathComponent(part: string | undefined): part is string {
  return !!part && part !== "." && part !== ".." && !part.includes("/") && !part.includes("\\");
}

function reportProgress(options: ModelPullOptions, progress: ModelPullProgress): void {
  options.onProgress?.(progress);
}

function formatBytes(n: number): string {
  const kb = 1024;
  const mb = kb * 1024;
  const gb = mb * 1024;
  if (n >= gb) return `${(n / gb).toFixed(1)} GB`;
  if (n >= mb) return `${Math.round(n / mb)} MB`;
  return `${Math.floor(n / kb)} KB`;
}
