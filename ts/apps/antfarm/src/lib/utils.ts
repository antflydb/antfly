import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface InferenceErrorBody {
  error?: unknown;
  message?: unknown;
  retryable?: unknown;
  retry_after_ms?: unknown;
}

export interface InferenceRetryOptions {
  maxRetries?: number;
  minDelayMs?: number;
  maxDelayMs?: number;
}

export class InferenceHttpError extends Error {
  readonly status: number;
  readonly code?: string;
  readonly retryable: boolean;
  readonly retryAfterMs?: number;

  constructor(
    message: string,
    options: {
      status: number;
      code?: string;
      retryable?: boolean;
      retryAfterMs?: number;
    }
  ) {
    super(message);
    this.name = "InferenceHttpError";
    this.status = options.status;
    this.code = options.code;
    this.retryable = options.retryable ?? false;
    this.retryAfterMs = options.retryAfterMs;
  }
}

interface AbortControllerSlot {
  current: AbortController | null;
}

/** Replace the active request and cancel the previous owner of this slot. */
export function beginAbortableRequest(slot: AbortControllerSlot): AbortController {
  slot.current?.abort();
  const controller = new AbortController();
  slot.current = controller;
  return controller;
}

/** Clear a request slot only when the completing request still owns it. */
export function finishAbortableRequest(
  slot: AbortControllerSlot,
  controller: AbortController
): boolean {
  if (slot.current !== controller) return false;
  slot.current = null;
  return true;
}

const DEFAULT_RETRY_DELAY_MS = 250;
const MIN_RETRY_DELAY_MS = 50;
const MAX_RETRY_DELAY_MS = 5_000;

function boundedDelay(value: unknown, fallback: number, minimum: number, maximum: number): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return Math.min(maximum, Math.max(minimum, parsed));
}

async function responseErrorBody(response: Response): Promise<InferenceErrorBody> {
  if (!response.headers.get("content-type")?.toLowerCase().includes("application/json")) return {};
  try {
    const value: unknown = await response.clone().json();
    return value && typeof value === "object" && !Array.isArray(value)
      ? (value as InferenceErrorBody)
      : {};
  } catch {
    return {};
  }
}

async function retryDelayMs(
  response: Response,
  fallback: number,
  minimum: number,
  maximum: number
): Promise<number | null> {
  if (response.status !== 503) return null;
  const body = await responseErrorBody(response);
  if (body.error !== "MODEL_RESOURCE_BUSY" || body.retryable !== true) return null;
  if (typeof body.retry_after_ms === "number") {
    return boundedDelay(body.retry_after_ms, fallback, minimum, maximum);
  }
  const retryAfter = response.headers.get("retry-after");
  return boundedDelay(
    retryAfter == null ? fallback : Number(retryAfter) * 1_000,
    fallback,
    minimum,
    maximum
  );
}

function abortableDelay(delayMs: number, signal?: AbortSignal | null): Promise<void> {
  if (signal?.aborted)
    return Promise.reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
  return new Promise((resolve, reject) => {
    const timeout = globalThis.setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    const onAbort = () => {
      globalThis.clearTimeout(timeout);
      reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Retry only server-declared, transient inference admission failures. */
export async function fetchWithRetry(
  input: RequestInfo | URL,
  init?: RequestInit,
  maxRetriesOrOptions: number | InferenceRetryOptions = 1
): Promise<Response> {
  const options =
    typeof maxRetriesOrOptions === "number"
      ? { maxRetries: maxRetriesOrOptions }
      : maxRetriesOrOptions;
  const maxRetries = Math.max(0, options.maxRetries ?? 1);
  const minimum = Math.max(0, options.minDelayMs ?? MIN_RETRY_DELAY_MS);
  const maximum = Math.max(minimum, options.maxDelayMs ?? MAX_RETRY_DELAY_MS);

  for (let attempt = 0; ; attempt++) {
    const response = await fetch(input, init);
    const delay = await retryDelayMs(response, DEFAULT_RETRY_DELAY_MS, minimum, maximum);
    if (delay == null || attempt >= maxRetries) return response;
    response.body?.cancel().catch(() => undefined);
    await abortableDelay(delay, init?.signal);
  }
}

/** Fetch and decode an inference response with a typed HTTP error. */
export async function fetchInferenceJson<T>(
  input: RequestInfo | URL,
  init?: RequestInit,
  retryOptions?: InferenceRetryOptions
): Promise<T> {
  const response = await fetchWithRetry(input, init, retryOptions ?? {});
  if (response.ok) return (await response.json()) as T;

  const body = await responseErrorBody(response);
  const retryAfterHeader = response.headers.get("retry-after");
  const retryAfterMs =
    typeof body.retry_after_ms === "number"
      ? body.retry_after_ms
      : retryAfterHeader == null
        ? undefined
        : boundedDelay(Number(retryAfterHeader) * 1_000, 0, 0, MAX_RETRY_DELAY_MS);
  throw new InferenceHttpError(
    typeof body.message === "string" && body.message.trim()
      ? body.message
      : `Inference request failed (${response.status})`,
    {
      status: response.status,
      code: typeof body.error === "string" ? body.error : undefined,
      retryable: body.retryable === true,
      retryAfterMs,
    }
  );
}
