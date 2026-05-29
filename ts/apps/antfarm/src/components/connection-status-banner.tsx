import { Button } from "@antfly/design-system";
import { AlertTriangle, RefreshCw, X } from "lucide-react";
import { useEffect, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useConnectionStatus } from "@/hooks/use-connection-status";

interface ServerInfo {
  name: string;
  port: number;
  hint: string;
}

const SERVER_INFO: Record<string, ServerInfo> = {
  antfly: {
    name: "Antfly",
    port: 8080,
    hint: "Make sure the Antfly server is running on localhost:8080",
  },
  inference: {
    name: "Antfly inference",
    port: 11433,
    hint: "Make sure the Antfly inference runtime is running (check Settings for URL)",
  },
};

export function ConnectionStatusBanner() {
  const { antfly, inference, retry } = useConnectionStatus();
  const [dismissed, setDismissed] = useState(false);

  // Reset dismissed state when both servers reconnect
  useEffect(() => {
    if (antfly === "connected" && inference === "connected") {
      setDismissed(false);
    }
  }, [antfly, inference]);

  const handleDismiss = () => {
    setDismissed(true);
  };

  // Determine which servers are disconnected
  const disconnectedServers: string[] = [];
  if (isProductEnabled("antfly") && antfly === "disconnected") {
    disconnectedServers.push("antfly");
  }
  if (isProductEnabled("inference") && inference === "disconnected") {
    disconnectedServers.push("inference");
  }

  // Check if any server is still checking
  const isChecking =
    (isProductEnabled("antfly") && antfly === "checking") ||
    (isProductEnabled("inference") && inference === "checking");

  // Don't show if dismissed, checking, or all connected
  if (dismissed || isChecking || disconnectedServers.length === 0) {
    return null;
  }

  return (
    <div className="af-connection-banner px-4 py-3">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="af-connection-banner-icon h-5 w-5 mt-0.5 flex-shrink-0" />
          <div className="space-y-1">
            {disconnectedServers.map((server) => {
              const info = SERVER_INFO[server];
              return (
                <div key={server}>
                  <p className="af-connection-banner-title">
                    Unable to connect to {info.name} server
                  </p>
                  <p className="af-connection-banner-description">{info.hint}</p>
                </div>
              );
            })}
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <Button
            variant="outline"
            size="sm"
            onClick={retry}
            className="af-connection-banner-action h-7 px-2 text-xs"
          >
            <RefreshCw className="h-3.5 w-3.5 mr-1" />
            Retry
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleDismiss}
            aria-label="Dismiss"
            className="af-connection-banner-action h-7 w-7 p-0"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}
