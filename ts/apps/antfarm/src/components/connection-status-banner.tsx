import { Button } from "@antfly/design-system";
import { AlertTriangle, RefreshCw, X } from "lucide-react";
import { useEffect, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useApiConfig } from "@/hooks/use-api-config";
import { type ServerStatus, useConnectionStatus } from "@/hooks/use-connection-status";
import { isExternalAuthMode } from "@/runtime-config";

function failureTitle(status: ServerStatus): string {
  switch (status) {
    case "unauthorized":
      return "Sign-in required";
    case "forbidden":
      return "Access denied";
    case "unavailable":
      return "Service unavailable";
    default:
      return "Unable to connect";
  }
}

function failureDescription(status: ServerStatus, target: string, hosted: boolean) {
  switch (status) {
    case "unauthorized":
      return hosted
        ? "Your hosted session could not be verified. Sign in again through your hosting dashboard, then retry."
        : "Sign in to Antfarm, then retry.";
    case "forbidden":
      return "Your account does not have access. Ask an administrator to review your permissions.";
    case "unavailable":
      return hosted
        ? "The hosted service could not complete the request. Retry, or check the instance in your hosting dashboard."
        : "The server could not complete the health check. Retry and check the server logs.";
    default:
      return hosted ? (
        "The hosted service could not be reached. Check your connection and the instance in your hosting dashboard, then retry."
      ) : (
        <>
          Checking <code>{target}</code>. Run <code>./antfly standalone</code> or update Settings.
        </>
      );
  }
}

export function ConnectionStatusBanner() {
  const { antfly, inference, retry } = useConnectionStatus();
  const { apiUrl, inferenceApiUrl } = useApiConfig();
  const [dismissed, setDismissed] = useState(false);
  const hosted = isExternalAuthMode();
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

  const failures = [
    { product: "antfly", label: "Antfly data", status: antfly, target: antflyTarget },
    { product: "inference", label: "Antfly inference", status: inference, target: inferenceTarget },
  ].filter(
    ({ product, status }) =>
      isProductEnabled(product as "antfly" | "inference") &&
      status !== "connected" &&
      status !== "checking"
  );

  // A pending check must not hide a known failure in the other service.
  if (dismissed || failures.length === 0) {
    return null;
  }

  return (
    <div role="alert" className="af-connection-banner px-4 py-3">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="af-connection-banner-icon h-5 w-5 mt-0.5 flex-shrink-0" />
          <div className="space-y-2">
            {failures.map(({ product, label, status, target }) => (
              <div key={product}>
                <p className="af-connection-banner-title">
                  {label}: {failureTitle(status)}
                </p>
                <p className="af-connection-banner-description">
                  {failureDescription(status, target, hosted)}
                </p>
              </div>
            ))}
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
