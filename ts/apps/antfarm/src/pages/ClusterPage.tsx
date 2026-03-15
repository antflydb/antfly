import {
  AlertTriangle,
  Database,
  GitBranch,
  Loader2,
  Network,
  RefreshCw,
  Scissors,
} from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  type ShardInfoData,
  type ShardStatus,
  type StoreInfo,
  useClusterStatus,
} from "@/hooks/use-cluster-status";
import { cn } from "@/lib/utils";

// --- Utilities ---

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / k ** i).toFixed(1))} ${sizes[i]}`;
}

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  if (diff < 1000) return "now";
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  return `${Math.floor(diff / 3600000)}h ago`;
}

function truncateId(id: string, len = 8): string {
  return id.length > len ? `${id.slice(0, len)}...` : id;
}

function truncateRange(range: string): string {
  if (!range) return "";
  return range.length > 6 ? `${range.slice(0, 6)}..` : range;
}

type ShardRole = "leader" | "follower" | "initializing" | "splitting" | "unassigned";

function getShardRole(shard: ShardStatus, storeId: string): ShardRole {
  if (shard.info.initializing) return "initializing";
  if (shard.info.splitting) return "splitting";
  if (!shard.info.peers?.includes(storeId)) return "unassigned";
  if (shard.info.raft_status?.leader_id === storeId) return "leader";
  return "follower";
}

function getShardDiskSize(shard: ShardStatus): number {
  return shard.info.shard_stats?.storage?.disk_size ?? 0;
}

// --- Derived data helpers ---

interface TableRow {
  name: string;
  shardCount: number;
  totalDisk: number;
  shards: ShardStatus[];
}

function buildTableRows(shards: Record<string, ShardStatus>): TableRow[] {
  const tables = new Map<string, ShardStatus[]>();

  for (const shard of Object.values(shards)) {
    const existing = tables.get(shard.table) ?? [];
    existing.push(shard);
    tables.set(shard.table, existing);
  }

  return Array.from(tables.entries())
    .map(([name, tableShards]) => ({
      name,
      shardCount: tableShards.length,
      totalDisk: tableShards.reduce((sum, s) => sum + getShardDiskSize(s), 0),
      shards: tableShards,
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

function getShardsForCell(tableShards: ShardStatus[], storeId: string): ShardStatus[] {
  return tableShards.filter((s) => s.info.peers?.includes(storeId));
}

function getTotalDisk(shards: Record<string, ShardStatus>): number {
  return Object.values(shards).reduce((sum, s) => sum + getShardDiskSize(s), 0);
}

function getStoreShardCount(shards: Record<string, ShardStatus>, storeId: string): number {
  return Object.values(shards).filter((s) => s.info.peers?.includes(storeId)).length;
}

// --- Components ---

function HealthBadge({ health }: { health: string }) {
  const config: Record<string, { className: string; label: string }> = {
    healthy: {
      className: "bg-[var(--success-50)] text-[var(--success-700)] border-[var(--success-200)]",
      label: "Healthy",
    },
    degraded: {
      className: "bg-[var(--warning-50)] text-[var(--warning-700)] border-[var(--warning-200)]",
      label: "Degraded",
    },
    unhealthy: {
      className: "bg-[var(--danger-50)] text-[var(--danger-700)] border-[var(--danger-200)]",
      label: "Unhealthy",
    },
    error: {
      className: "bg-[var(--danger-50)] text-[var(--danger-700)] border-[var(--danger-200)]",
      label: "Error",
    },
    unknown: {
      className: "bg-[var(--gray-3)] text-[var(--gray-7)] border-[var(--gray-4)]",
      label: "Unknown",
    },
  };
  const { className, label } = config[health] ?? config.unknown;

  return (
    <Badge className={cn("text-xs font-medium", className)}>
      <span
        className={cn(
          "inline-block w-1.5 h-1.5 rounded-full mr-1",
          health === "healthy" && "bg-[var(--success-500)]",
          health === "degraded" && "bg-[var(--warning-500)]",
          (health === "unhealthy" || health === "error") && "bg-[var(--danger-500)]",
          health === "unknown" && "bg-[var(--gray-6)]"
        )}
      />
      {label}
    </Badge>
  );
}

function HealthDot({ healthy }: { healthy: boolean }) {
  return (
    <span
      className={cn(
        "inline-block w-2 h-2 rounded-full shrink-0",
        healthy ? "bg-[var(--success-500)]" : "bg-[var(--danger-500)]"
      )}
    />
  );
}

function StatValue({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex items-baseline gap-1.5">
      <span className="text-xl font-semibold tabular-nums">{value}</span>
      <span className="text-xs text-muted-foreground uppercase tracking-wide">{label}</span>
    </div>
  );
}

function ShardBlock({ shard, storeId }: { shard: ShardStatus; storeId: string }) {
  const role = getShardRole(shard, storeId);
  const diskSize = getShardDiskSize(shard);
  const voterCount = shard.info.raft_status?.voters?.length ?? 0;
  const byteRange = shard.info.byte_range ?? [];

  const roleStyles: Record<ShardRole, string> = {
    leader: "bg-[var(--info-600)] text-white border-[var(--info-600)]",
    follower: "bg-[var(--info-100)] text-[var(--info-700)] border-[var(--info-300)]",
    initializing: "bg-[var(--info-500)] text-white border-[var(--info-500)] animate-pulse",
    splitting: "bg-[var(--warning-100)] text-[var(--warning-700)] border-[var(--warning-300)]",
    unassigned:
      "bg-[var(--danger-100)] text-[var(--danger-600)] border-[var(--danger-300)] border-dashed",
  };

  const roleLabels: Record<ShardRole, string> = {
    leader: "L",
    follower: "F",
    initializing: "I",
    splitting: "S",
    unassigned: "!",
  };

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          className={cn(
            "inline-flex items-center justify-center min-w-[28px] h-6 px-1.5 rounded text-[10px] font-mono font-medium border cursor-default transition-colors",
            roleStyles[role]
          )}
        >
          {role === "splitting" && <Scissors className="w-2.5 h-2.5 mr-0.5" />}
          {role === "unassigned" && <AlertTriangle className="w-2.5 h-2.5 mr-0.5" />}
          {roleLabels[role]}
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-[220px]">
        <div className="grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5 text-[11px]">
          <span className="text-muted-foreground">Shard</span>
          <span className="font-mono">{truncateId(shard.id, 12)}</span>
          <span className="text-muted-foreground">Role</span>
          <span className="capitalize">{role}</span>
          <span className="text-muted-foreground">State</span>
          <span>{shard.state}</span>
          {byteRange.length === 2 && (
            <>
              <span className="text-muted-foreground">Range</span>
              <span className="font-mono">
                {truncateRange(byteRange[0])} .. {truncateRange(byteRange[1])}
              </span>
            </>
          )}
          <span className="text-muted-foreground">Disk</span>
          <span>{formatBytes(diskSize)}</span>
          <span className="text-muted-foreground">Voters</span>
          <span>{voterCount}</span>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

function NodeCard({ store, shardCount }: { store: StoreInfo; shardCount: number }) {
  const isHealthy = store.state === "Healthy";
  // Extract host:port from API URL
  let apiHost = store.api_url;
  try {
    const url = new URL(store.api_url);
    apiHost = `${url.hostname}:${url.port || (url.protocol === "https:" ? "443" : "80")}`;
  } catch {
    /* use raw */
  }

  return (
    <Card className="py-3 gap-2 min-w-[180px]">
      <CardContent className="px-4 space-y-2">
        <div className="flex items-center gap-2">
          <HealthDot healthy={isHealthy} />
          <span className="font-mono text-xs font-medium truncate">{truncateId(store.id, 12)}</span>
        </div>
        <div className="text-[11px] text-muted-foreground font-mono truncate">{apiHost}</div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-muted-foreground">{timeAgo(store.last_seen)}</span>
          <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
            {shardCount} shards
          </Badge>
        </div>
      </CardContent>
    </Card>
  );
}

function MetadataRaftCard({ metadataInfo }: { metadataInfo: ShardInfoData }) {
  const leaderId = metadataInfo.raft_status?.leader_id;
  const voterCount = metadataInfo.raft_status?.voters?.length ?? 0;

  return (
    <Card className="py-3 gap-2">
      <CardContent className="px-4">
        <div className="flex items-center gap-2 mb-2">
          <GitBranch className="w-3.5 h-3.5 text-muted-foreground" />
          <span className="text-xs font-medium">Metadata Raft</span>
        </div>
        <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px]">
          <span className="text-muted-foreground">Leader</span>
          <span className="font-mono">{leaderId ? truncateId(leaderId, 10) : "none"}</span>
          <span className="text-muted-foreground">Voters</span>
          <span>{voterCount}</span>
        </div>
      </CardContent>
    </Card>
  );
}

// --- Legend ---

function ShardLegend() {
  const items = [
    { label: "Leader", className: "bg-[var(--info-600)] text-white border-[var(--info-600)]" },
    {
      label: "Follower",
      className: "bg-[var(--info-100)] text-[var(--info-700)] border-[var(--info-300)]",
    },
    {
      label: "Initializing",
      className: "bg-[var(--info-500)] text-white border-[var(--info-500)] animate-pulse",
    },
    {
      label: "Splitting",
      className: "bg-[var(--warning-100)] text-[var(--warning-700)] border-[var(--warning-300)]",
    },
    {
      label: "Unassigned",
      className:
        "bg-[var(--danger-100)] text-[var(--danger-600)] border-[var(--danger-300)] border-dashed",
    },
  ];

  return (
    <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
      {items.map((item) => (
        <div key={item.label} className="flex items-center gap-1.5">
          <span className={cn("inline-block w-4 h-3 rounded border", item.className)} />
          <span>{item.label}</span>
        </div>
      ))}
    </div>
  );
}

// --- Main Page ---

const ClusterPage: React.FC = () => {
  const [refreshInterval, setRefreshInterval] = useState<number | null>(10000);
  const cluster = useClusterStatus(refreshInterval);

  const storeList = useMemo(
    () => Object.values(cluster.stores).sort((a, b) => a.id.localeCompare(b.id)),
    [cluster.stores]
  );

  const tableRows = useMemo(() => buildTableRows(cluster.shards), [cluster.shards]);

  const totalShards = Object.keys(cluster.shards).length;
  const uniqueTables = new Set(Object.values(cluster.shards).map((s) => s.table)).size;
  const totalDisk = getTotalDisk(cluster.shards);

  if (cluster.error && Object.keys(cluster.stores).length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 gap-4">
        <Network className="w-10 h-10 text-muted-foreground" />
        <p className="text-muted-foreground">Failed to load cluster status</p>
        <p className="text-xs text-muted-foreground">{cluster.error}</p>
        <Button variant="outline" size="sm" onClick={cluster.refresh}>
          <RefreshCw className="w-3.5 h-3.5 mr-1.5" />
          Retry
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Section A: Cluster Health Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-4">
          <h2 className="text-2xl font-bold">Cluster</h2>
          <HealthBadge health={cluster.health} />
        </div>

        <div className="flex items-center gap-6">
          <div className="flex items-center gap-6">
            <StatValue label="nodes" value={storeList.length} />
            <StatValue label="tables" value={uniqueTables} />
            <StatValue label="shards" value={totalShards} />
            {totalDisk > 0 && <StatValue label="disk" value={formatBytes(totalDisk)} />}
          </div>

          <div className="flex items-center gap-2 border-l pl-4 border-border">
            <Button
              variant="outline"
              size="icon"
              className="h-7 w-7"
              onClick={cluster.refresh}
              disabled={cluster.isLoading}
            >
              <RefreshCw className={cn("w-3.5 h-3.5", cluster.isLoading && "animate-spin")} />
            </Button>
            <Select
              value={refreshInterval === null ? "off" : String(refreshInterval)}
              onValueChange={(v) => setRefreshInterval(v === "off" ? null : Number(v))}
            >
              <SelectTrigger className="h-7 w-[72px] text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="5000">5s</SelectItem>
                <SelectItem value="10000">10s</SelectItem>
                <SelectItem value="30000">30s</SelectItem>
                <SelectItem value="off">Off</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {cluster.message && <p className="text-sm text-muted-foreground">{cluster.message}</p>}

      {/* Loading state */}
      {cluster.isLoading && storeList.length === 0 && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* Section B: Node Cards */}
      {storeList.length > 0 && (
        <div className="space-y-3">
          <div className="flex flex-wrap gap-3">
            {storeList.map((store) => (
              <NodeCard
                key={store.id}
                store={store}
                shardCount={getStoreShardCount(cluster.shards, store.id)}
              />
            ))}
            {!cluster.swarmMode && cluster.metadataInfo && (
              <MetadataRaftCard metadataInfo={cluster.metadataInfo} />
            )}
          </div>
        </div>
      )}

      {/* Section C: Shard Matrix */}
      {tableRows.length > 0 ? (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-medium text-muted-foreground">Shard Distribution</h3>
            <ShardLegend />
          </div>

          <div className="border rounded-lg overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/30">
                  <th className="text-left px-4 py-2.5 font-medium text-muted-foreground text-xs uppercase tracking-wide">
                    Table
                  </th>
                  {storeList.map((store) => (
                    <th
                      key={store.id}
                      className="text-center px-3 py-2.5 font-mono text-xs font-medium text-muted-foreground"
                    >
                      <div className="flex items-center justify-center gap-1.5">
                        <HealthDot healthy={store.state === "Healthy"} />
                        <span>{truncateId(store.id, 8)}</span>
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tableRows.map((table) => (
                  <tr
                    key={table.name}
                    className="border-b last:border-b-0 hover:bg-muted/20 transition-colors"
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <Database className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                        <Link
                          to={`/tables/${table.name}`}
                          className="font-medium hover:underline text-foreground"
                        >
                          {table.name}
                        </Link>
                      </div>
                      <div className="flex items-center gap-3 mt-0.5 text-[11px] text-muted-foreground ml-5.5">
                        <span>{table.shardCount} shards</span>
                        {table.totalDisk > 0 && <span>{formatBytes(table.totalDisk)}</span>}
                      </div>
                    </td>
                    {storeList.map((store) => {
                      const cellShards = getShardsForCell(table.shards, store.id);
                      return (
                        <td key={store.id} className="px-3 py-3 text-center">
                          {cellShards.length > 0 ? (
                            <div className="flex flex-wrap items-center justify-center gap-1">
                              {cellShards.map((shard) => (
                                <ShardBlock key={shard.id} shard={shard} storeId={store.id} />
                              ))}
                            </div>
                          ) : (
                            <span className="text-muted-foreground/30">&mdash;</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        !cluster.isLoading &&
        storeList.length > 0 && (
          <div className="flex flex-col items-center justify-center py-16 gap-3 border rounded-lg bg-muted/20">
            <Database className="w-8 h-8 text-muted-foreground" />
            <p className="text-muted-foreground">No tables yet</p>
            <Button variant="outline" size="sm" asChild>
              <Link to="/create">Create a table</Link>
            </Button>
          </div>
        )
      )}
    </div>
  );
};

export default ClusterPage;
