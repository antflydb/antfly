import { APICallError } from "@ai-sdk/provider";
import { InferenceAPIError, InferenceCapacityError, InferenceTransportError } from "@antfly/sdk";

/** Preserve Antfly details without retaining prompts or signed source URLs. */
export function providerInferenceError(
  error: unknown,
  endpoint: string,
  signal?: AbortSignal
): unknown {
  signal?.throwIfAborted();
  const transport = InferenceTransportError.from(error);
  if (transport) {
    return new APICallError({
      message: transport.message,
      url: `antfly:/ai/v1/${endpoint}`,
      requestBodyValues: undefined,
      isRetryable: true,
      cause: transport,
    });
  }
  if (!(error instanceof InferenceAPIError)) return error;
  const headers = { ...error.responseHeaders };
  if (error instanceof InferenceCapacityError) {
    headers["retry-after-ms"] = String(error.retryAfterMs);
  }
  return new APICallError({
    message: error.message,
    url: `antfly:/ai/v1/${endpoint}`,
    requestBodyValues: undefined,
    statusCode: error.status,
    responseHeaders: headers,
    isRetryable:
      error.retryable ?? (error.status === 408 || error.status === 429 || error.status >= 500),
    cause: error,
  });
}
