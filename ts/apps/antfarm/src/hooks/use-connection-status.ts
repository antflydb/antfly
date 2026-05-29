import { useCallback, useEffect, useRef, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useApiConfig } from "@/hooks/use-api-config";

export type ServerStatus = "connected" | "disconnected" | "checking";

export interface ConnectionStatus {
  antfly: ServerStatus;
  inference: ServerStatus;
  retry: () => void;
}

const CHECK_INTERVAL_DISCONNECTED = 30000; // 30 seconds when disconnected
const CONNECTION_CHECK_TIMEOUT = 5000; // 5 seconds timeout for health checks

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
          signal: signal ?? AbortSignal.timeout(CONNECTION_CHECK_TIMEOUT),
        });
        if (isMountedRef.current) {
          setAntflyStatus(response.ok ? "connected" : "disconnected");
        }
      } catch {
        if (isMountedRef.current) {
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
          signal: signal ?? AbortSignal.timeout(CONNECTION_CHECK_TIMEOUT),
        });
        if (isMountedRef.current) {
          setInferenceStatus(response.ok ? "connected" : "disconnected");
        }
      } catch {
        if (isMountedRef.current) {
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

  // Re-check every 30 seconds if any server is disconnected
  useEffect(() => {
    const shouldRetry = antflyStatus === "disconnected" || inferenceStatus === "disconnected";

    if (!shouldRetry) return;

    const interval = setInterval(() => {
      if (antflyStatus === "disconnected") {
        checkAntfly();
      }
      if (inferenceStatus === "disconnected") {
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
