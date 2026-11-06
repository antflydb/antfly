import { InferenceAPIError, InferenceCapacityError } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import {
  isContextManagementWithMedia,
  isOneVideoPerRequest,
  isRetryableCapacityError,
  isSourceForbidden,
  isSourceVersionMismatch,
  isVideoCodecUnavailable,
  isVideoTooLarge,
  retryDelayMs,
} from "../src/asset/errors.js";

function apiError(code: string, status = 400) {
  return new InferenceAPIError(status, code, code, false);
}

describe("asset error guards", () => {
  it("recognizes source-resolution codes from the gateway's generic failure envelope", () => {
    expect(isSourceForbidden(apiError("Forbidden", 403))).toBe(true);
    expect(isSourceForbidden(apiError("InvalidInferenceSource"))).toBe(false);
    expect(isSourceVersionMismatch(apiError("InferenceSourceMismatch", 409))).toBe(true);
  });

  it("recognizes video-specific codes from the generation server", () => {
    expect(isVideoCodecUnavailable(apiError("VIDEO_CODEC_UNAVAILABLE"))).toBe(true);
    expect(isVideoTooLarge(apiError("VIDEO_TOO_LARGE"))).toBe(true);
    expect(isOneVideoPerRequest(apiError("INVALID_MEDIA_LAYOUT"))).toBe(true);
    expect(isContextManagementWithMedia(apiError("UNSUPPORTED_FEATURE"))).toBe(true);
  });

  it("does not misclassify an unrelated error", () => {
    expect(isVideoCodecUnavailable(new Error("boom"))).toBe(false);
    expect(isVideoCodecUnavailable(apiError("INVALID_VIDEO"))).toBe(false);
  });

  it("treats the gateway's 429 capacity code and the CUDA backend's 503 as retryable", () => {
    expect(isRetryableCapacityError(apiError("InferenceCapacityUnavailable", 429))).toBe(true);
    expect(isRetryableCapacityError(apiError("VIDEO_BACKEND_UNAVAILABLE", 503))).toBe(true);
    expect(isRetryableCapacityError(apiError("INVALID_VIDEO", 400))).toBe(false);
  });

  it("treats InferenceCapacityError (MODEL_RESOURCE_BUSY) as retryable with its own delay", () => {
    const error = new InferenceCapacityError({
      error: "MODEL_RESOURCE_BUSY",
      message: "busy",
      reason: "inference_capacity",
      retryable: true,
      retry_after_ms: 1500,
    });
    expect(isRetryableCapacityError(error)).toBe(true);
    expect(retryDelayMs(error)).toBe(1500);
  });

  it("falls back to a default retry delay for a code-only capacity error", () => {
    expect(retryDelayMs(apiError("InferenceCapacityUnavailable", 429))).toBe(1000);
    expect(retryDelayMs(apiError("InferenceCapacityUnavailable", 429), 2500)).toBe(2500);
  });
});
