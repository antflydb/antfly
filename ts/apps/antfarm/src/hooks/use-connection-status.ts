import { useCallback, useEffect, useRef, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useApiConfig } from "@/hooks/use-api-config";

export type ServerStatus =
  | "connected"
  | "disconnected"
  | "checking"
  | "unauthorized"
  | "forbidden"
  | "unavailable";

export interface ConnectionStatus {
  antfly: ServerStatus;
  inference: ServerStatus;
  retry: () => void;
}

const CHECK_INTERVAL_DISCONNECTED = 30000; // 30 seconds when disconnected
const CONNECTION_CHECK_TIMEOUT = 5000; // 5 seconds timeout for health checks

function responseStatus(response: Response): ServerStatus {
  if (response.ok) return "connected";
  if (response.status === 401) return "unauthorized";
  if (response.status === 403) return "forbidden";
  return "unavailable";
}

function checkSignal(signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(CONNECTION_CHECK_TIMEOUT);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

function shouldRetry(status: ServerStatus): boolean {
  // Auth failures need sign-in or permission changes, not background polling.
  return status === "disconnected" || status === "unavailable";
}

export function useConnectionStatus(): ConnectionStatus {
  const { apiUrl, inferenceApiUrl } = useApiConfig();
  const [antflyStatus, setAntflyStatus] = useState<ServerStatus>("checking");
  const [inferenceStatus, setInferenceStatus] = useState<ServerStatus>("checking");
  const isMountedRef = useRef(true);

  const checkAntfly = useCallback(
    async (signal?: AbortSignal) => {
      if (!isProductEnabled("antfly")) {
        setAntflyStatus("connected"); // Skip check if product disabled
        return;
      }

      try {
        const response = await fetch(`${apiUrl}/status`, {
          method: "GET",
          signal: checkSignal(signal),
        });
        if (isMountedRef.current && !signal?.aborted) {
          setAntflyStatus(responseStatus(response));
        }
      } catch {
        if (isMountedRef.current && !signal?.aborted) {
          setAntflyStatus("disconnected");
        }
      }
    },
    [apiUrl]
  );

  const checkInference = useCallback(
    async (signal?: AbortSignal) => {
      if (!isProductEnabled("inference")) {
        setInferenceStatus("connected"); // Skip check if product disabled
        return;
      }

      try {
        const response = await fetch(`${inferenceApiUrl}/healthz`, {
          method: "GET",
          signal: checkSignal(signal),
        });
        if (isMountedRef.current && !signal?.aborted) {
          setInferenceStatus(responseStatus(response));
        }
      } catch {
        if (isMountedRef.current && !signal?.aborted) {
          setInferenceStatus("disconnected");
        }
      }
    },
    [inferenceApiUrl]
  );

  const retry = useCallback(() => {
    if (isProductEnabled("antfly")) {
      setAntflyStatus("checking");
    }
    if (isProductEnabled("inference")) {
      setInferenceStatus("checking");
    }
    checkAntfly();
    checkInference();
  }, [checkAntfly, checkInference]);

  // Initial check on mount with cleanup
  useEffect(() => {
    isMountedRef.current = true;
    const controller = new AbortController();

    checkAntfly(controller.signal);
    checkInference(controller.signal);

    return () => {
      isMountedRef.current = false;
      controller.abort();
    };
  }, [checkAntfly, checkInference]);

  // Re-check transient failures every 30 seconds.
  useEffect(() => {
    if (!shouldRetry(antflyStatus) && !shouldRetry(inferenceStatus)) return;

    const interval = setInterval(() => {
      if (shouldRetry(antflyStatus)) {
        checkAntfly();
      }
      if (shouldRetry(inferenceStatus)) {
        checkInference();
      }
    }, CHECK_INTERVAL_DISCONNECTED);

    return () => clearInterval(interval);
  }, [antflyStatus, inferenceStatus, checkAntfly, checkInference]);

  return {
    antfly: antflyStatus,
    inference: inferenceStatus,
    retry,
  };
}
