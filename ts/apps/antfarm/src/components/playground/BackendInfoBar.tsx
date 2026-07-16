import { Button, Skeleton } from "@antfly/design-system";
import { Settings, Wifi, WifiOff } from "lucide-react";
import { useEffect, useMemo } from "react";
import { SettingsDialog } from "@/components/SettingsDialog";
import { useApiConfig } from "@/hooks/use-api-config";
import { useConnectedModels } from "@/hooks/use-connections";

type ConnectionState = "connected" | "disconnected" | "checking";

export function BackendInfoBar() {
  const { inferenceConnectionId, setInferenceConnectionId } = useApiConfig();
  const { providers, loading, error, retry } = useConnectedModels();
  const compatible = useMemo(
    () =>
      providers
        .filter((provider) => provider.inference?.provider === "antfly")
        .sort((a, b) => {
          if (a.id === "local-inference") return -1;
          if (b.id === "local-inference") return 1;
          return (a.display_name ?? a.name).localeCompare(b.display_name ?? b.name);
        }),
    [providers]
  );
  const selected = compatible.find((provider) => provider.id === inferenceConnectionId);
  const status: ConnectionState = loading
    ? "checking"
    : selected?.status === "connected"
      ? "connected"
      : "disconnected";

  useEffect(() => {
    if (!loading && compatible.length > 0 && !selected) {
      setInferenceConnectionId(compatible[0].id);
    }
  }, [compatible, loading, selected, setInferenceConnectionId]);

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
          <span>
            {error ??
              (compatible.length === 0
                ? "No Antfly-compatible inference connection is configured"
                : "Selected inference connection is unavailable")}
          </span>
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
          <Button variant="outline" size="sm" onClick={retry} className="h-7 text-xs">
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

      <label className="flex items-center gap-2 text-xs">
        <span className="text-muted-foreground">Connection</span>
        <select
          value={inferenceConnectionId}
          onChange={(event) => setInferenceConnectionId(event.target.value)}
          className="h-7 border bg-background px-2"
          aria-label="Inference connection"
        >
          {compatible.map((provider) => (
            <option key={provider.id} value={provider.id}>
              {provider.display_name ?? provider.name}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}
