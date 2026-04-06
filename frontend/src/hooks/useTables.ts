import { useState, useCallback } from "react";
import { useApi } from "./useApi";
import type { PortalTable, TablesResponse } from "../types";

export function useTables() {
  const fetchApi = useApi();
  const [tables, setTables] = useState<PortalTable[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(
    async (databaseName: string) => {
      if (!databaseName) {
        setTables([]);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const data = await fetchApi<TablesResponse>(
          `/api/DHRefreshAAS_PortalTables?databaseName=${encodeURIComponent(databaseName)}`
        );
        setTables(data.tables ?? []);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    },
    [fetchApi]
  );

  const reset = useCallback(() => setTables([]), []);

  return { tables, loading, error, load, reset };
}
