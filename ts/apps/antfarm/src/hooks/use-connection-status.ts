import { useCallback, useEffect, useRef, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useApiConfig } from "@/hooks/use-api-config";

export type ServerStatus = "connected" | "disconnected" | "checking";

export interface ConnectionStatus {
  antfly: ServerStatus;
  termite: ServerStatus;
  retry: () => void;
}

const CHECK_INTERVAL_DISCONNECTED = 30000; // 30 seconds when disconnected
const CONNECTION_CHECK_TIMEOUT = 5000; // 5 seconds timeout for health checks

export function useConnectionStatus(): ConnectionStatus {
  const { termiteApiUrl } = useApiConfig();
  const [antflyStatus, setAntflyStatus] = useState<ServerStatus>("checking");
  const [termiteStatus, setTermiteStatus] = useState<ServerStatus>("checking");
  const isMountedRef = useRef(true);

  const checkAntfly = useCallback(async (signal?: AbortSignal) => {
    if (!isProductEnabled("antfly")) {
      setAntflyStatus("connected"); // Skip check if product disabled
      return;
    }

    try {
      const response = await fetch("/api/v1/status", {
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
  }, []);

  const checkTermite = useCallback(
    async (signal?: AbortSignal) => {
      if (!isProductEnabled("termite")) {
        setTermiteStatus("connected"); // Skip check if product disabled
        return;
      }

      try {
        const response = await fetch(`${termiteApiUrl}/`, {
          method: "GET",
          signal: signal ?? AbortSignal.timeout(CONNECTION_CHECK_TIMEOUT),
        });
        if (isMountedRef.current) {
          setTermiteStatus(response.ok ? "connected" : "disconnected");
        }
      } catch {
        if (isMountedRef.current) {
          setTermiteStatus("disconnected");
        }
      }
    },
    [termiteApiUrl]
  );

  const retry = useCallback(() => {
    if (isProductEnabled("antfly")) {
      setAntflyStatus("checking");
    }
    if (isProductEnabled("termite")) {
      setTermiteStatus("checking");
    }
    checkAntfly();
    checkTermite();
  }, [checkAntfly, checkTermite]);

  // Initial check on mount with cleanup
  useEffect(() => {
    isMountedRef.current = true;
    const controller = new AbortController();

    checkAntfly(controller.signal);
    checkTermite(controller.signal);

    return () => {
      isMountedRef.current = false;
      controller.abort();
    };
  }, [checkAntfly, checkTermite]);

  // Re-check every 30 seconds if any server is disconnected
  useEffect(() => {
    const shouldRetry = antflyStatus === "disconnected" || termiteStatus === "disconnected";

    if (!shouldRetry) return;

    const interval = setInterval(() => {
      if (antflyStatus === "disconnected") {
        checkAntfly();
      }
      if (termiteStatus === "disconnected") {
        checkTermite();
      }
    }, CHECK_INTERVAL_DISCONNECTED);

    return () => clearInterval(interval);
  }, [antflyStatus, termiteStatus, checkAntfly, checkTermite]);

  return {
    antfly: antflyStatus,
    termite: termiteStatus,
    retry,
  };
}
