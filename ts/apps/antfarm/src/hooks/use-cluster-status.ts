import { useCallback, useEffect, useRef, useState } from "react";
import { useApi } from "@/hooks/use-api-config";

// TypeScript interfaces for the untyped AdditionalProperties
// from GET /api/v1/status response

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
      const data = await client.getStatus();
      if (!data) {
        setError("No data returned from status endpoint");
        return;
      }

      // Extract typed fields
      setHealth((data.health as string) ?? "unknown");
      setMessage(data.message as string | undefined);
      setSwarmMode(Boolean(data.swarm_mode));
      setAuthEnabled(Boolean(data.auth_enabled));

      // Extract untyped AdditionalProperties
      const raw = data as Record<string, unknown>;

      // stores.statuses
      const storesWrapper = raw.stores as { statuses?: Record<string, StoreInfo> } | undefined;
      setStores(storesWrapper?.statuses ?? {});

      // shards.statuses
      const shardsWrapper = raw.shards as { statuses?: Record<string, ShardStatus> } | undefined;
      setShards(shardsWrapper?.statuses ?? {});

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
