import { Badge, Button, Skeleton } from "@antfly/design-system";
import { Cpu, Settings, Wifi, WifiOff } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { SettingsDialog } from "@/components/SettingsDialog";
import { useApiConfig } from "@/hooks/use-api-config";

interface RuntimeInfo {
  native?: boolean;
  onnx?: boolean;
  metal?: boolean;
  mlx?: boolean;
  cuda?: boolean;
  xla?: boolean;
  wasm?: boolean;
}

type ConnectionState = "connected" | "disconnected" | "checking";

export function BackendInfoBar() {
  const { inferenceApiUrl } = useApiConfig();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [status, setStatus] = useState<ConnectionState>("checking");
  const isMountedRef = useRef(true);
  const enabledBackends = runtime
    ? Object.entries(runtime)
        .filter(([, enabled]) => enabled)
        .map(([name]) => name)
    : [];

  const fetchInfo = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const modelsRes = await fetch(`${inferenceApiUrl}/ai/v1/models`, {
          signal: signal ?? AbortSignal.timeout(5000),
        });

        if (!isMountedRef.current) return;

        if (modelsRes.ok) {
          const modelsData = await modelsRes.json();
          setRuntime(modelsData.backends || null);
        }

        setStatus(modelsRes.ok ? "connected" : "disconnected");
      } catch {
        if (isMountedRef.current) {
          setStatus("disconnected");
          setRuntime(null);
        }
      }
    },
    [inferenceApiUrl]
  );

  useEffect(() => {
    isMountedRef.current = true;
    const controller = new AbortController();
    setStatus("checking");
    fetchInfo(controller.signal);

    return () => {
      isMountedRef.current = false;
      controller.abort();
    };
  }, [fetchInfo]);

  // Re-check every 30s when disconnected
  useEffect(() => {
    if (status !== "disconnected") return;
    const interval = setInterval(() => fetchInfo(), 30000);
    return () => clearInterval(interval);
  }, [status, fetchInfo]);

  if (status === "checking") {
    return (
      <div className="flex items-center gap-2 mb-4 p-3 rounded-none border bg-muted/30">
        <Skeleton className="h-4 w-4 rounded-full" />
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-4 w-24" />
      </div>
    );
  }

  if (status === "disconnected") {
    return (
      <div className="flex items-center justify-between mb-4 p-3 rounded-none border border-destructive/30 bg-destructive/5">
        <div className="flex items-center gap-2 text-sm text-destructive">
          <WifiOff className="h-4 w-4" />
          <span>Antfly inference disconnected</span>
          <code className="text-xs bg-muted px-1.5 py-0.5 rounded-none">{inferenceApiUrl}</code>
        </div>
        <div className="flex items-center gap-2">
          <SettingsDialog
            trigger={
              <Button variant="outline" size="sm" className="h-7 text-xs">
                <Settings className="h-3 w-3 mr-1" />
                Configure
              </Button>
            }
          />
          <Button variant="outline" size="sm" onClick={() => fetchInfo()} className="h-7 text-xs">
            Retry
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-2 mb-4 p-3 rounded-none border bg-muted/30 flex-wrap">
      {/* Connection status */}
      <div className="flex items-center gap-1.5">
        <span className="relative flex h-2.5 w-2.5">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full af-status-bar-success opacity-75" />
          <span className="relative inline-flex rounded-full h-2.5 w-2.5 af-status-bar-success" />
        </span>
        <Wifi className="h-3.5 w-3.5 text-muted-foreground" />
      </div>

      {/* Runtime info */}
      {runtime && (
        <Badge variant="outline" className="gap-1 text-xs">
          <Cpu className="h-3 w-3" />
          {enabledBackends.length > 0 ? enabledBackends.join(", ") : "runtime"}
        </Badge>
      )}

      {/* Available backends */}
      {enabledBackends.length > 1 && (
        <span className="text-xs text-muted-foreground ml-auto">{enabledBackends.length} backends</span>
      )}
    </div>
  );
}
