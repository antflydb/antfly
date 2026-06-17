import {
  Button,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  MonoLabel,
} from "@antfly/design-system";
import type { ConnectedModel, Connection } from "@antfly/sdk";
import {
  AlertCircle,
  Check,
  ChevronDown,
  Database,
  Globe,
  HardDrive,
  RefreshCw,
} from "lucide-react";
import type { ComponentType } from "react";
import { useState } from "react";
import { AntyEmptyState, ErrorState } from "@/components/branded-empty-state";
import {
  CONNECTED_MODEL_KINDS,
  OTHER_MODELS_GROUP,
  providerTypeIcon,
  providerTypeLabel,
} from "@/data/connected-providers";
import { useConnectionsWithModels } from "@/hooks/use-connections";
import { cn } from "@/lib/utils";

function StatusBadge({ connection }: { connection: Connection }) {
  switch (connection.status) {
    case "connected":
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-none text-xs border bg-success/10 text-success border-success/20">
          <Check className="w-3 h-3" />
          Connected
        </span>
      );
    case "error":
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-none text-xs border bg-destructive/10 text-destructive border-destructive/20">
          <AlertCircle className="w-3 h-3" />
          Error
        </span>
      );
    case "unsupported":
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-none text-xs border bg-muted text-muted-foreground border-border">
          Unsupported
        </span>
      );
    default:
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-none text-xs border bg-muted text-muted-foreground border-border">
          Configured
        </span>
      );
  }
}

function ModelGroup({
  label,
  icon: Icon,
  models,
}: {
  label: string;
  icon: ComponentType<{ className?: string }>;
  models: ConnectedModel[];
}) {
  const [open, setOpen] = useState(false);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger asChild>
        <button
          type="button"
          className="flex w-full items-center gap-2 py-1.5 text-sm text-left hover:text-foreground text-muted-foreground transition-colors"
        >
          <Icon className="w-3.5 h-3.5" />
          <span className="flex-1">{label}</span>
          <span className="tabular-nums text-xs">{models.length}</span>
          <ChevronDown className={cn("w-3.5 h-3.5 transition-transform", open && "rotate-180")} />
        </button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <ul className="pl-5 pb-2 space-y-0.5">
          {models.map((model) => (
            <li
              key={model.name}
              className="font-mono text-xs text-muted-foreground flex items-center gap-2"
            >
              <span className="truncate" title={model.display_name ?? model.name}>
                {model.name}
              </span>
              {model.configured && (
                <span className="px-1 py-0 rounded-none text-[10px] border bg-primary/10 text-primary border-primary/20 shrink-0">
                  configured
                </span>
              )}
            </li>
          ))}
        </ul>
      </CollapsibleContent>
    </Collapsible>
  );
}

function ProviderCard({ connection }: { connection: Connection }) {
  const provider = connection.inference;
  if (!provider) return null;
  const label = connection.display_name ?? connection.name;
  const Icon = providerTypeIcon(provider.provider);
  const models = provider.models ?? {};

  const groups = [
    ...CONNECTED_MODEL_KINDS.map((info) => ({
      key: info.kind,
      label: info.label,
      icon: info.icon,
      models: models[`${info.kind}s`] ?? [],
    })),
    {
      key: OTHER_MODELS_GROUP.key,
      label: OTHER_MODELS_GROUP.label,
      icon: OTHER_MODELS_GROUP.icon,
      models: models.other ?? [],
    },
  ].filter((group) => group.models.length > 0);

  return (
    <div className="bg-card border border-border rounded-none p-5">
      <div className="flex items-start gap-3 mb-3">
        <div className="w-9 h-9 bg-muted rounded-none flex items-center justify-center shrink-0">
          <Icon className="w-4.5 h-4.5 text-muted-foreground" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-medium truncate" title={label}>
              {label}
            </h3>
            <StatusBadge connection={connection} />
          </div>
          <p className="text-xs text-muted-foreground truncate">
            {providerTypeLabel(provider.provider)}
            {provider.url ? (
              <span className="font-mono" title={provider.url}>
                {" · "}
                {provider.url}
              </span>
            ) : null}
            {provider.region ? ` · ${provider.region}` : null}
          </p>
        </div>
      </div>

      {connection.error && (
        <p className="text-xs text-destructive mb-3 line-clamp-2" title={connection.error}>
          {connection.error}
        </p>
      )}

      {(provider.names?.length ?? 0) > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-3">
          {provider.names?.map((name) => (
            <span
              key={name}
              className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
            >
              {name}
            </span>
          ))}
        </div>
      )}

      {groups.length > 0 && (
        <div className="border-t border-border pt-2">
          {groups.map((group) => (
            <ModelGroup
              key={group.key}
              label={group.label}
              icon={group.icon}
              models={group.models}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function InfrastructureCard({ connection }: { connection: Connection }) {
  const externalIo = connection.external_io;
  if (!externalIo) return null;
  const label = connection.display_name ?? connection.name;
  const Icon = externalIo.protocol === "http" ? Globe : HardDrive;
  const capabilities = connection.capabilities ?? [];

  return (
    <div className="bg-card border border-border rounded-none p-5">
      <div className="flex items-start gap-3">
        <div className="w-9 h-9 bg-muted rounded-none flex items-center justify-center shrink-0">
          <Icon className="w-4.5 h-4.5 text-muted-foreground" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-medium truncate" title={label}>
              {label}
            </h3>
            <StatusBadge connection={connection} />
          </div>
          <p className="text-xs text-muted-foreground truncate">
            {externalIo.protocol.toUpperCase()}
            {externalIo.endpoint ? (
              <span className="font-mono" title={externalIo.endpoint}>
                {" · "}
                {externalIo.endpoint}
              </span>
            ) : null}
            {(externalIo.hosts?.length ?? 0) > 0 ? (
              <span className="font-mono" title={externalIo.hosts?.join(", ")}>
                {" · "}
                {externalIo.hosts?.join(", ")}
              </span>
            ) : null}
          </p>
          {(externalIo.buckets?.length ?? 0) > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {externalIo.buckets?.map((bucket) => (
                <span
                  key={bucket}
                  className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                >
                  {bucket}
                  {externalIo.prefix ? `/${externalIo.prefix}` : ""}
                </span>
              ))}
            </div>
          )}
          {capabilities.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {capabilities.map((capability) => (
                <span
                  key={capability}
                  className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                >
                  {capability}
                </span>
              ))}
            </div>
          )}
          {connection.error && (
            <p className="text-xs text-destructive mt-2 line-clamp-2" title={connection.error}>
              {connection.error}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

function WebSearchCard({ connection }: { connection: Connection }) {
  const webSearch = connection.web_search;
  if (!webSearch) return null;
  const label = connection.display_name ?? connection.name;
  const provider = connection.provider ?? "web_search";
  const capabilities = connection.capabilities ?? [];
  const domains = [...(webSearch.include_domains ?? []), ...(webSearch.exclude_domains ?? [])];

  return (
    <div className="bg-card border border-border rounded-none p-5">
      <div className="flex items-start gap-3">
        <div className="w-9 h-9 bg-muted rounded-none flex items-center justify-center shrink-0">
          <Globe className="w-4.5 h-4.5 text-muted-foreground" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-medium truncate" title={label}>
              {label}
            </h3>
            <StatusBadge connection={connection} />
          </div>
          <p className="text-xs text-muted-foreground truncate">
            {provider.toUpperCase()}
            {webSearch.service ? ` · ${webSearch.service}` : null}
            {webSearch.location ? ` · ${webSearch.location}` : null}
            {webSearch.max_results ? ` · ${webSearch.max_results} results` : null}
          </p>
          <div className="flex flex-wrap gap-1.5 mt-2">
            {webSearch.include_content && (
              <span className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground">
                content
              </span>
            )}
            {webSearch.include_highlights && (
              <span className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground">
                highlights
              </span>
            )}
            {webSearch.safe_search && (
              <span className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground">
                safe
              </span>
            )}
            {webSearch.data_store && (
              <span
                className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                title={webSearch.data_store}
              >
                {webSearch.data_store}
              </span>
            )}
          </div>
          {domains.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {domains.map((domain) => (
                <span
                  key={domain}
                  className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                >
                  {domain}
                </span>
              ))}
            </div>
          )}
          {capabilities.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {capabilities.map((capability) => (
                <span
                  key={capability}
                  className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                >
                  {capability}
                </span>
              ))}
            </div>
          )}
          {connection.error && (
            <p className="text-xs text-destructive mt-2 line-clamp-2" title={connection.error}>
              {connection.error}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

function CdcCard({ connection }: { connection: Connection }) {
  const cdc = connection.cdc;
  if (!cdc) return null;
  const label = connection.display_name ?? connection.name;

  return (
    <div className="bg-card border border-border rounded-none p-5">
      <div className="flex items-start gap-3">
        <div className="w-9 h-9 bg-muted rounded-none flex items-center justify-center shrink-0">
          <Database className="w-4.5 h-4.5 text-muted-foreground" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-medium truncate" title={label}>
              {label}
            </h3>
            <StatusBadge connection={connection} />
          </div>
          <p className="text-xs text-muted-foreground truncate">
            {cdc.provider.toUpperCase()} · {cdc.table_name}
            {cdc.external_table ? (
              <span className="font-mono" title={cdc.external_table}>
                {" <- "}
                {cdc.external_table}
              </span>
            ) : null}
          </p>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {cdc.phase && (
              <span className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground">
                {cdc.phase}
              </span>
            )}
            {cdc.lag_millis !== undefined && (
              <span className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground">
                lag {cdc.lag_millis}ms
              </span>
            )}
            {cdc.slot_name && (
              <span
                className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                title={cdc.slot_name}
              >
                slot {cdc.slot_name}
              </span>
            )}
            {cdc.publication_name && (
              <span
                className="px-2 py-0.5 rounded-none text-xs font-mono border bg-muted text-muted-foreground"
                title={cdc.publication_name}
              >
                pub {cdc.publication_name}
              </span>
            )}
          </div>
          {connection.error && (
            <p className="text-xs text-destructive mt-2 line-clamp-2" title={connection.error}>
              {connection.error}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

export default function ConnectionsPage() {
  const { connections, supported, loading, error, retry } = useConnectionsWithModels();

  const providers = connections.filter((connection) => connection.kind === "inference");
  const webSearchConnections = connections.filter((connection) => connection.kind === "web_search");
  const infrastructure = connections.filter((connection) => connection.kind === "external_io");
  const cdcConnections = connections.filter((connection) => connection.kind === "cdc");
  const connectedCount = connections.filter(
    (connection) => connection.status === "connected"
  ).length;
  const errorCount = connections.filter((connection) => connection.status === "error").length;

  if (loading) {
    return (
      <DashboardPage>
        <DashboardPageHeader>
          <div>
            <div className="mb-3 h-7 w-64 animate-pulse rounded-none bg-muted" />
            <div className="h-4 w-full max-w-md animate-pulse rounded-none bg-muted" />
          </div>
        </DashboardPageHeader>
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: skeleton placeholders are positional
            <div key={i} className="bg-card border border-border rounded-none p-5">
              <div className="flex items-start gap-3 mb-3">
                <div className="w-9 h-9 bg-muted animate-pulse rounded-none" />
                <div className="flex-1">
                  <div className="h-5 w-32 bg-muted animate-pulse rounded-none mb-2" />
                  <div className="h-3 w-24 bg-muted animate-pulse rounded-none" />
                </div>
              </div>
              <div className="h-10 bg-muted animate-pulse rounded-none" />
            </div>
          ))}
        </div>
      </DashboardPage>
    );
  }

  if (error) {
    return (
      <DashboardPage className="min-h-full items-center justify-center">
        <ErrorState
          message="Could not load connections from the Antfly server. Check the connection and try again."
          onRetry={retry}
        />
      </DashboardPage>
    );
  }

  if (!supported) {
    return (
      <DashboardPage className="min-h-full items-center justify-center">
        <AntyEmptyState
          title="Connections not available"
          description="This Antfly server does not report configured connections. Upgrade the server to use this page."
        />
      </DashboardPage>
    );
  }

  return (
    <DashboardPage>
      <DashboardPageHeader>
        <div>
          <DashboardPageTitle className="font-aeonik">Connections</DashboardPageTitle>
          <DashboardPageDescription>
            External services this node is configured to use: inference providers with their live
            model inventories, web search providers, external IO endpoints, and CDC sources.
          </DashboardPageDescription>
        </div>
        <DashboardPageActions>
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            <span>
              <span className="font-medium text-foreground tabular-nums">{connectedCount}</span>{" "}
              connected
            </span>
            {errorCount > 0 && (
              <span>
                <span className="font-medium text-destructive tabular-nums">{errorCount}</span>{" "}
                failing
              </span>
            )}
            <Button variant="outline" size="sm" onClick={retry}>
              <RefreshCw className="w-3.5 h-3.5 mr-1.5" />
              Refresh
            </Button>
          </div>
        </DashboardPageActions>
      </DashboardPageHeader>

      {connections.length === 0 ? (
        <AntyEmptyState
          title="No connections configured"
          description="No public connection resources are present in the node config."
        />
      ) : (
        <div className="space-y-6">
          {providers.length > 0 && (
            <section>
              <MonoLabel className="mb-3 block">Inference providers</MonoLabel>
              <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                {providers.map((connection) => (
                  <ProviderCard key={connection.id} connection={connection} />
                ))}
              </div>
            </section>
          )}

          {webSearchConnections.length > 0 && (
            <section>
              <MonoLabel className="mb-3 block">Web search</MonoLabel>
              <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                {webSearchConnections.map((connection) => (
                  <WebSearchCard key={connection.id} connection={connection} />
                ))}
              </div>
            </section>
          )}

          {infrastructure.length > 0 && (
            <section>
              <MonoLabel className="mb-3 block">External IO</MonoLabel>
              <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                {infrastructure.map((connection) => (
                  <InfrastructureCard key={connection.id} connection={connection} />
                ))}
              </div>
            </section>
          )}

          {cdcConnections.length > 0 && (
            <section>
              <MonoLabel className="mb-3 block">CDC sources</MonoLabel>
              <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                {cdcConnections.map((connection) => (
                  <CdcCard key={connection.id} connection={connection} />
                ))}
              </div>
            </section>
          )}
        </div>
      )}
    </DashboardPage>
  );
}
