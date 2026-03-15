import { Cpu, MonitorCog, Settings, Wifi, WifiOff } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { SettingsDialog } from "@/components/SettingsDialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useApiConfig } from "@/hooks/use-api-config";

interface RuntimeInfo {
  backend: string;
  gpu: {
    available: boolean;
    type: string;
    device_name: string;
  };
  available_backends: string[];
}

interface VersionInfo {
  version: string;
  git_commit: string;
}

type ConnectionState = "connected" | "disconnected" | "checking";

export function BackendInfoBar() {
  const { termiteApiUrl } = useApiConfig();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [version, setVersion] = useState<VersionInfo | null>(null);
  const [status, setStatus] = useState<ConnectionState>("checking");
  const isMountedRef = useRef(true);

  const fetchInfo = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const [modelsRes, versionRes] = await Promise.all([
          fetch(`${termiteApiUrl}/api/models`, {
            signal: signal ?? AbortSignal.timeout(5000),
          }),
          fetch(`${termiteApiUrl}/api/version`, {
            signal: signal ?? AbortSignal.timeout(5000),
          }),
        ]);

        if (!isMountedRef.current) return;

        if (modelsRes.ok) {
          const modelsData = await modelsRes.json();
          setRuntime(modelsData.runtime || null);
        }

        if (versionRes.ok) {
          const versionData = await versionRes.json();
          setVersion(versionData);
        }

        setStatus(modelsRes.ok ? "connected" : "disconnected");
      } catch {
        if (isMountedRef.current) {
          setStatus("disconnected");
          setRuntime(null);
          setVersion(null);
        }
      }
    },
    [termiteApiUrl]
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
      <div className="flex items-center gap-2 mb-4 p-3 rounded-lg border bg-muted/30">
        <Skeleton className="h-4 w-4 rounded-full" />
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-4 w-24" />
      </div>
    );
  }

  if (status === "disconnected") {
    return (
      <div className="flex items-center justify-between mb-4 p-3 rounded-lg border border-destructive/30 bg-destructive/5">
        <div className="flex items-center gap-2 text-sm text-destructive">
          <WifiOff className="h-4 w-4" />
          <span>Termite disconnected</span>
          <code className="text-xs bg-muted px-1.5 py-0.5 rounded">{termiteApiUrl}</code>
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
    <div className="flex items-center gap-2 mb-4 p-3 rounded-lg border bg-muted/30 flex-wrap">
      {/* Connection status */}
      <div className="flex items-center gap-1.5">
        <span className="relative flex h-2.5 w-2.5">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-green-500" />
        </span>
        <Wifi className="h-3.5 w-3.5 text-muted-foreground" />
      </div>

      {/* Backend name */}
      {runtime?.backend && (
        <Badge variant="secondary" className="gap-1 text-xs">
          <MonitorCog className="h-3 w-3" />
          {runtime.backend}
        </Badge>
      )}

      {/* GPU info */}
      {runtime?.gpu?.available && (
        <Badge variant="outline" className="gap-1 text-xs">
          <Cpu className="h-3 w-3" />
          {runtime.gpu.device_name || runtime.gpu.type}
        </Badge>
      )}

      {/* Version */}
      {version && (
        <Badge variant="outline" className="text-xs text-muted-foreground">
          {version.version}
        </Badge>
      )}

      {/* Available backends */}
      {runtime?.available_backends && runtime.available_backends.length > 1 && (
        <span className="text-xs text-muted-foreground ml-auto">
          {runtime.available_backends.length} backends
        </span>
      )}
    </div>
  );
}
