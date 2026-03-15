import { AlertTriangle, RefreshCw, X } from "lucide-react";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
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
  termite: {
    name: "Termite",
    port: 11433,
    hint: "Make sure the Termite server is running (check Settings for URL)",
  },
};

export function ConnectionStatusBanner() {
  const { antfly, termite, retry } = useConnectionStatus();
  const [dismissed, setDismissed] = useState(false);

  // Reset dismissed state when both servers reconnect
  useEffect(() => {
    if (antfly === "connected" && termite === "connected") {
      setDismissed(false);
    }
  }, [antfly, termite]);

  const handleDismiss = () => {
    setDismissed(true);
  };

  // Determine which servers are disconnected
  const disconnectedServers: string[] = [];
  if (isProductEnabled("antfly") && antfly === "disconnected") {
    disconnectedServers.push("antfly");
  }
  if (isProductEnabled("termite") && termite === "disconnected") {
    disconnectedServers.push("termite");
  }

  // Check if any server is still checking
  const isChecking =
    (isProductEnabled("antfly") && antfly === "checking") ||
    (isProductEnabled("termite") && termite === "checking");

  // Don't show if dismissed, checking, or all connected
  if (dismissed || isChecking || disconnectedServers.length === 0) {
    return null;
  }

  return (
    <div className="bg-amber-50 dark:bg-amber-950/30 border-b border-amber-200 dark:border-amber-800 px-4 py-3">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-600 dark:text-amber-400 mt-0.5 flex-shrink-0" />
          <div className="space-y-1">
            {disconnectedServers.map((server) => {
              const info = SERVER_INFO[server];
              return (
                <div key={server}>
                  <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
                    Unable to connect to {info.name} server
                  </p>
                  <p className="text-xs text-amber-600 dark:text-amber-400">{info.hint}</p>
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
            className="h-7 px-2 text-xs border-amber-300 dark:border-amber-700 hover:bg-amber-100 dark:hover:bg-amber-900/50"
          >
            <RefreshCw className="h-3.5 w-3.5 mr-1" />
            Retry
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleDismiss}
            aria-label="Dismiss"
            className="h-7 w-7 p-0 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}
