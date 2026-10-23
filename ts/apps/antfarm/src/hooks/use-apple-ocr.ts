import { useEffect, useState } from "react";
import { useApiConfig } from "@/hooks/use-api-config";
import { fetchInferenceJson } from "@/lib/utils";

export const APPLE_OCR_READER = "apple-vision-ocr";

/** Availability belongs to the selected server, never the browser's OS. */
export function useAppleOcr() {
  const { inferenceApiUrl, inferenceConnectionId } = useApiConfig();
  const localConnection = inferenceConnectionId === "local-inference";
  const endpoint = `${inferenceApiUrl}/ai/v1/_antfarm/readers`;
  const identity = `${inferenceConnectionId}:${endpoint}`;
  const [state, setState] = useState({ identity: "", available: false, loading: true });
  useEffect(() => {
    // Connection proxies do not expose the private Apple OCR routes.
    if (!localConnection) {
      setState({ identity, available: false, loading: false });
      return;
    }
    const controller = new AbortController();
    setState({ identity, available: false, loading: true });
    void fetchInferenceJson<{ apple_ocr?: { available: boolean } }>(endpoint, {
      signal: controller.signal,
    })
      .then((result) => {
        if (!controller.signal.aborted)
          setState({ identity, available: result.apple_ocr?.available === true, loading: false });
      })
      .catch(() => {
        // Older or non-Apple servers continue to offer their normal models.
        if (!controller.signal.aborted) setState({ identity, available: false, loading: false });
      });
    return () => controller.abort();
  }, [endpoint, identity, localConnection]);
  if (!localConnection) return { available: false, loading: false };
  return state.identity === identity ? state : { available: false, loading: true };
}
