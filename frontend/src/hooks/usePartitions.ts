import { useState, useCallback } from "react";
import { useApi } from "./useApi";
import type {
  PortalPartition,
  PartitionsApiResponse,
} from "../types";

export function usePartitions() {
  const fetchApi = useApi();
  const [partitions, setPartitions] = useState<PortalPartition[]>([]);
  const [defaultRefreshType, setDefaultRefreshType] = useState("Full");
  const [supportsTableRefresh, setSupportsTableRefresh] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(
    async (databaseName: string, tableName: string) => {
      if (!databaseName || !tableName) {
        setPartitions([]);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const data = await fetchApi<PartitionsApiResponse>(
          `/api/DHRefreshAAS_PortalPartitions?databaseName=${encodeURIComponent(databaseName)}&tableName=${encodeURIComponent(tableName)}`
        );
        const payload = data.data ?? {
          partitions: [],
          defaultRefreshType: "Full",
          supportsTableRefresh: true,
        };
        setPartitions(payload.partitions ?? []);
        setDefaultRefreshType(payload.defaultRefreshType ?? "Full");
        setSupportsTableRefresh(payload.supportsTableRefresh ?? true);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    },
    [fetchApi]
  );

  const reset = useCallback(() => {
    setPartitions([]);
    setDefaultRefreshType("Full");
    setSupportsTableRefresh(true);
  }, []);

  return {
    partitions,
    defaultRefreshType,
    supportsTableRefresh,
    loading,
    error,
    load,
    reset,
  };
}
