import { Button } from "@antfly/design-system";
import { AlertTriangle, RefreshCw, X } from "lucide-react";
import { useEffect, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useApiConfig } from "@/hooks/use-api-config";
import { useConnectionStatus } from "@/hooks/use-connection-status";

export function ConnectionStatusBanner() {
  const { antfly, inference, retry } = useConnectionStatus();
  const { apiUrl, inferenceApiUrl } = useApiConfig();
  const [dismissed, setDismissed] = useState(false);
  const devProxyTarget = import.meta.env.VITE_ANTFARM_API_PROXY_TARGET as string | undefined;

  const antflyTarget =
    apiUrl.startsWith("/") && devProxyTarget ? `${devProxyTarget}${apiUrl}` : apiUrl;
  const inferenceTarget =
    inferenceApiUrl || (devProxyTarget ? `${devProxyTarget}/ai/v1` : "/ai/v1");

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

  const bothDisconnected =
    disconnectedServers.includes("antfly") && disconnectedServers.includes("inference");
  const title = bothDisconnected
    ? "Antfly is not running"
    : disconnectedServers.includes("antfly")
      ? "Antfly data is not connected"
      : "Antfly inference is not connected";
  const description = bothDisconnected ? (
    <>
      Run <code>./antfly standalone</code>, or use Settings to connect to another API.
    </>
  ) : disconnectedServers.includes("antfly") ? (
    <>
      Checking <code>{antflyTarget}</code>. Run <code>./antfly standalone</code> or update Settings.
    </>
  ) : (
    <>
      Checking <code>{inferenceTarget}</code>. Run <code>./antfly standalone</code> or update
      Settings.
    </>
  );

  return (
    <div className="af-connection-banner px-4 py-3">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="af-connection-banner-icon h-5 w-5 mt-0.5 flex-shrink-0" />
          <div>
            <p className="af-connection-banner-title">{title}</p>
            <p className="af-connection-banner-description">{description}</p>
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
