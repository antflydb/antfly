import { MonoLabel } from "@antfly/design-system";
import type { Connection } from "@antfly/sdk";
import { ArrowRight } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { isProductEnabled } from "@/config/products";
import { providerTypeLabel } from "@/data/connected-providers";
import { useConnectedModels } from "@/hooks/use-connections";
import { cn } from "@/lib/utils";

function modelCount(connection: Connection): number {
  const models = connection.inference?.models;
  if (!models) return 0;
  return Object.values(models).reduce((total, group) => total + (group?.length ?? 0), 0);
}

/**
 * Compact strip of connected inference providers shown on the Models page.
 * Renders nothing when the server does not support /db/v1/connections, when
 * the antfly product (which owns the /connections route) is disabled, or on
 * error — it never degrades the page it sits on.
 */
export function ConnectedProvidersSummary() {
  const navigate = useNavigate();
  const { providers, supported, loading, error } = useConnectedModels();

  if (!isProductEnabled("antfly")) return null;
  if (error || !supported) return null;

  if (loading) {
    return (
      <div className="flex items-center gap-2">
        <div className="h-6 w-40 animate-pulse rounded-none bg-muted" />
        <div className="h-6 w-24 animate-pulse rounded-none bg-muted" />
      </div>
    );
  }

  if (providers.length === 0) return null;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <MonoLabel>Connected providers</MonoLabel>
      {providers.map((connection) => {
        const count = modelCount(connection);
        return (
          <button
            key={connection.id}
            type="button"
            onClick={() => navigate("/connections")}
            title={`${providerTypeLabel(connection.inference?.provider ?? "")}${
              connection.error ? ` — ${connection.error}` : ""
            }`}
            className="inline-flex items-center gap-1.5 px-2 py-1 rounded-none text-xs font-mono border bg-muted text-muted-foreground hover:text-foreground transition-colors"
          >
            <span
              className={cn(
                "w-1.5 h-1.5 rounded-full shrink-0",
                connection.status === "connected"
                  ? "bg-success"
                  : connection.status === "error"
                    ? "bg-destructive"
                    : "bg-muted-foreground"
              )}
            />
            {connection.name}
            {count > 0 && <span className="tabular-nums">{count}</span>}
          </button>
        );
      })}
      <button
        type="button"
        onClick={() => navigate("/connections")}
        className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
      >
        View all
        <ArrowRight className="w-3 h-3" />
      </button>
    </div>
  );
}
