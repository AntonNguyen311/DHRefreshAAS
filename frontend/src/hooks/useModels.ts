import { useState, useCallback } from "react";
import { useApi } from "./useApi";
import type { PortalModel, PortalUser, ModelsResponse } from "../types";

export function useModels() {
  const fetchApi = useApi();
  const [models, setModels] = useState<PortalModel[]>([]);
  const [user, setUser] = useState<PortalUser | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchApi<ModelsResponse>(
        "/api/DHRefreshAAS_PortalModels"
      );
      setModels(data.models ?? []);
      setUser(data.user ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [fetchApi]);

  return { models, user, loading, error, load };
}
