import { useCallback, useEffect, useRef, useState } from "react";
import { useApi } from "@/hooks/use-api-config";

// TypeScript interfaces for the untyped AdditionalProperties
// from GET /db/v1/status response

export interface StoreInfo {
  id: string;
  raft_url: string;
  api_url: string;
  state: "Healthy" | "Unhealthy";
  last_seen: string;
  shards: Record<string, ShardInfoData>;
}

export interface ShardInfoData {
  byte_range?: string[];
  shard_stats?: {
    storage?: { disk_size: number; empty?: boolean };
    indexes?: Record<string, unknown>;
  };
  peers: string[];
  raft_status?: { leader_id: string; voters: string[] };
  reported_by?: string[];
  has_snapshot?: boolean;
  initializing?: boolean;
  splitting?: boolean;
}

export interface ShardStatus {
  info: ShardInfoData;
  id: string;
  table: string;
  state: string;
}

export interface ClusterDataNodeStatus {
  data_id: number;
  node_id: number;
  api_url?: string;
  raft_url?: string;
  state?: string;
  health_class?: string;
  live?: boolean;
}

export interface ClusterDataRangeStatus {
  group_id: number;
  range_id?: number;
  table_id: number;
  table_name?: string;
  start_key?: string;
  end_key?: string | null;
  state?: string;
  leader_data_id?: number | null;
  voter_count?: number;
  doc_count?: number;
  disk_bytes?: number;
  empty?: boolean;
}

export interface ClusterDataReplicaStatus {
  group_id: number;
  data_id: number;
  node_id?: number;
  replica_id?: number;
}

export interface ClusterDataSection {
  nodes?: ClusterDataNodeStatus[];
  ranges?: ClusterDataRangeStatus[];
  replicas?: ClusterDataReplicaStatus[];
}

export interface ClusterData {
  health: string;
  message?: string;
  swarmMode: boolean;
  authEnabled: boolean;
  stores: Record<string, StoreInfo>;
  shards: Record<string, ShardStatus>;
  metadataInfo?: ShardInfoData;
  isLoading: boolean;
  error?: string;
  refresh: () => void;
}

function statusState(value: string | undefined, live: boolean | undefined): "Healthy" | "Unhealthy" {
  if (live === false) return "Unhealthy";
  return value === "unhealthy" || value === "error" || value === "degraded" ? "Unhealthy" : "Healthy";
}

function storesFromDataSection(data: ClusterDataSection | undefined): Record<string, StoreInfo> | undefined {
  if (!data?.nodes || data.nodes.length === 0) return undefined;
  return Object.fromEntries(
    data.nodes.map((node) => {
      const id = String(node.data_id);
      return [
        id,
        {
          id,
          api_url: node.api_url ?? "",
          raft_url: node.raft_url ?? "",
          state: statusState(node.state ?? node.health_class, node.live),
          last_seen: new Date().toISOString(),
          shards: {},
        } satisfies StoreInfo,
      ];
    })
  );
}

function shardsFromDataSection(data: ClusterDataSection | undefined): Record<string, ShardStatus> | undefined {
  if (!data?.ranges || data.ranges.length === 0) return undefined;

  const replicasByGroup = new Map<number, ClusterDataReplicaStatus[]>();
  for (const replica of data.replicas ?? []) {
    const replicas = replicasByGroup.get(replica.group_id) ?? [];
    replicas.push(replica);
    replicasByGroup.set(replica.group_id, replicas);
  }

  return Object.fromEntries(
    data.ranges.map((range) => {
      const id = String(range.group_id);
      const replicas = replicasByGroup.get(range.group_id) ?? [];
      const peers = replicas.map((replica) => String(replica.data_id));
      const leaderId = range.leader_data_id === null || range.leader_data_id === undefined ? "" : String(range.leader_data_id);
      const voters = peers.length > 0 ? peers : leaderId ? [leaderId] : [];
      return [
        id,
        {
          id,
          table: range.table_name || String(range.table_id),
          state: range.state ?? "unknown",
          info: {
            byte_range: [range.start_key ?? "", range.end_key ?? ""],
            shard_stats: {
              storage: {
                disk_size: range.disk_bytes ?? 0,
                empty: range.empty ?? true,
              },
            },
            peers,
            raft_status: leaderId ? { leader_id: leaderId, voters } : { leader_id: "", voters },
          },
        } satisfies ShardStatus,
      ];
    })
  );
}

export function useClusterStatus(refreshInterval: number | null = 10000): ClusterData {
  const client = useApi();
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | undefined>();
  const [health, setHealth] = useState("unknown");
  const [message, setMessage] = useState<string | undefined>();
  const [swarmMode, setSwarmMode] = useState(false);
  const [authEnabled, setAuthEnabled] = useState(false);
  const [stores, setStores] = useState<Record<string, StoreInfo>>({});
  const [shards, setShards] = useState<Record<string, ShardStatus>>({});
  const [metadataInfo, setMetadataInfo] = useState<ShardInfoData | undefined>();
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStatus = useCallback(async () => {
    try {
      const data = await client.getClusterStatus();
      if (!data) {
        setError("No data returned from cluster endpoint");
        return;
      }

      const raw = data as Record<string, unknown>;

      // Extract typed fields
      setHealth((raw.health as string) ?? "unknown");
      setMessage(raw.message as string | undefined);
      setSwarmMode(Boolean(raw.swarm_mode));
      setAuthEnabled(Boolean(raw.auth_enabled));
      const statusData = raw.data as ClusterDataSection | undefined;

      // Legacy Go shape: stores.statuses. Zig shape: data.nodes.
      const storesWrapper = raw.stores as { statuses?: Record<string, StoreInfo> } | undefined;
      setStores(storesWrapper?.statuses ?? storesFromDataSection(statusData) ?? {});

      // Legacy Go shape: shards.statuses. Zig shape: data.ranges + data.replicas.
      const shardsWrapper = raw.shards as { statuses?: Record<string, ShardStatus> } | undefined;
      setShards(shardsWrapper?.statuses ?? shardsFromDataSection(statusData) ?? {});

      // metadata_info
      setMetadataInfo(raw.metadata_info as ShardInfoData | undefined);

      setError(undefined);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch cluster status");
    } finally {
      setIsLoading(false);
    }
  }, [client]);

  const refresh = useCallback(() => {
    setIsLoading(true);
    fetchStatus();
  }, [fetchStatus]);

  // Initial fetch
  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  // Auto-refresh
  useEffect(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }

    if (refreshInterval !== null && refreshInterval > 0) {
      intervalRef.current = setInterval(fetchStatus, refreshInterval);
    }

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [refreshInterval, fetchStatus]);

  return {
    health,
    message,
    swarmMode,
    authEnabled,
    stores,
    shards,
    metadataInfo,
    isLoading,
    error,
    refresh,
  };
}
