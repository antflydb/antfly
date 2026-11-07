import { InferenceAPIError, InferenceCapacityError } from "@antfly/sdk";

/**
 * Known `.code` values this package's asset flow can see, verified by
 * reading the server rather than guessed. Two different envelopes surface
 * them (both normalized into `InferenceAPIError.code` by `@antfly/sdk`):
 *
 * - `source`-reference resolution failures (zig/pkg/antfly/src/api/
 *   httpx_handler.zig's `gatewayFailure`): the Zig error name itself, e.g.
 *   `"Forbidden"`, `"InvalidInferenceSource"`, `"InferenceSourceMismatch"`,
 *   `"InferenceCapacityUnavailable"`.
 * - generation-time media failures (zig/pkg/inference/src/server/
 *   server.zig): an explicit upper-snake-case code, e.g.
 *   `"VIDEO_CODEC_UNAVAILABLE"`.
 */
export const ASSET_ERROR_CODES = {
  /** `source` URL not in `source_media_base_urls`/`allowed_hosts`. HTTP 403. */
  sourceForbidden: "Forbidden",
  /** Malformed `source` part, unsupported scheme, or unsupported MIME type. HTTP 400. */
  invalidSource: "InvalidInferenceSource",
  /** `source.uri` scheme/host this route does not fetch. HTTP 400. */
  sourceUnsupported: "InferenceSourceUnsupported",
  /** The pinned version no longer matches what the source serves. HTTP 409. */
  sourceVersionMismatch: "InferenceSourceMismatch",
  /** The shared 2 GiB media/fetch budget is exhausted. HTTP 429, retryable. */
  sourceCapacityUnavailable: "InferenceCapacityUnavailable",
  /** Malformed media bytes (bad image/video header). HTTP 400. */
  invalidMedia: "InvalidInferenceMedia",
  /** Expanded request exceeds the inline-media size ceiling. HTTP 413. */
  requestTooLarge: "InferenceRequestTooLarge",
  /** HEVC on a host without hardware decoding. HTTP 400. */
  videoCodecUnavailable: "VIDEO_CODEC_UNAVAILABLE",
  /** Video exceeds the 60s/256MiB backend limit. HTTP 400. */
  videoTooLarge: "VIDEO_TOO_LARGE",
  /** CUDA video decoder unavailable. HTTP 503, retryable. */
  videoBackendUnavailable: "VIDEO_BACKEND_UNAVAILABLE",
  /** Corrupt or unrecognized video bytes. HTTP 400. */
  invalidVideo: "INVALID_VIDEO",
  /** More than one video part in a request to local Gemma 4. HTTP 400. */
  oneVideoPerRequest: "INVALID_MEDIA_LAYOUT",
  /** `context_management` with any media part (video included). HTTP 400. */
  contextManagementWithMedia: "UNSUPPORTED_FEATURE",
  /** Shared inference admission/model-load capacity exhausted. HTTP 503, retryable. */
  modelResourceBusy: "MODEL_RESOURCE_BUSY",
} as const;

/** Recover Antfly details from provider errors and exhausted AI SDK retries. */
export function getInferenceError(error: unknown): InferenceAPIError | undefined {
  const seen = new Set<unknown>();
  for (let depth = 0; depth < 16 && error instanceof Error && !seen.has(error); depth++) {
    if (error instanceof InferenceAPIError) return error;
    seen.add(error);
    error = "lastError" in error ? error.lastError : error.cause;
  }
  return undefined;
}

function codeOf(error: unknown): string | undefined {
  return getInferenceError(error)?.code;
}

export function isAssetCode(
  error: unknown,
  code: (typeof ASSET_ERROR_CODES)[keyof typeof ASSET_ERROR_CODES]
): boolean {
  return codeOf(error) === code;
}

export const isSourceForbidden = (error: unknown) =>
  isAssetCode(error, ASSET_ERROR_CODES.sourceForbidden);
export const isSourceVersionMismatch = (error: unknown) =>
  isAssetCode(error, ASSET_ERROR_CODES.sourceVersionMismatch);
export const isVideoCodecUnavailable = (error: unknown) =>
  isAssetCode(error, ASSET_ERROR_CODES.videoCodecUnavailable);
export const isVideoTooLarge = (error: unknown) =>
  isAssetCode(error, ASSET_ERROR_CODES.videoTooLarge);
export const isOneVideoPerRequest = (error: unknown) =>
  isAssetCode(error, ASSET_ERROR_CODES.oneVideoPerRequest);
export const isContextManagementWithMedia = (error: unknown) =>
  isAssetCode(error, ASSET_ERROR_CODES.contextManagementWithMedia);

/**
 * True for a transient capacity response that is safe to retry: the shared
 * media/fetch budget (429, `InferenceCapacityUnavailable`), the CUDA video
 * backend (503, `VIDEO_BACKEND_UNAVAILABLE`), or shared model-load admission
 * (503, `MODEL_RESOURCE_BUSY`, already a distinct `InferenceCapacityError`).
 */
export function isRetryableCapacityError(error: unknown): boolean {
  error = getInferenceError(error);
  if (!(error instanceof InferenceAPIError) || error.retryable === false) return false;
  if (error instanceof InferenceCapacityError) return true;
  return (
    isAssetCode(error, ASSET_ERROR_CODES.sourceCapacityUnavailable) ||
    isAssetCode(error, ASSET_ERROR_CODES.videoBackendUnavailable)
  );
}

/** A caller-chosen retry delay for a capacity error that doesn't carry its own `retryAfterMs`. */
export function retryDelayMs(error: unknown, fallbackMs = 1000): number {
  const inference = getInferenceError(error);
  return inference instanceof InferenceCapacityError ? inference.retryAfterMs : fallbackMs;
}
