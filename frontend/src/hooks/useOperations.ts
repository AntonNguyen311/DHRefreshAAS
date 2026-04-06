import { useState, useCallback } from "react";
import { useApi } from "./useApi";
import type {
  RefreshPayload,
  SubmitResponse,
  RecentOperation,
  StatusResponse,
} from "../types";

export function useOperations() {
  const fetchApi = useApi();
  const [latestOperationId, setLatestOperationId] = useState("");
  const [submitResult, setSubmitResult] = useState<SubmitResponse | null>(null);
  const [operationDetail, setOperationDetail] = useState<unknown>(null);
  const [history, setHistory] = useState<RecentOperation[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = useCallback(
    async (payload: RefreshPayload) => {
      setLoading(true);
      setError(null);
      try {
        const data = await fetchApi<SubmitResponse>(
          "/api/DHRefreshAAS_PortalSubmitRefresh",
          { method: "POST", body: JSON.stringify(payload) }
        );
        setSubmitResult(data);
        setLatestOperationId(data.operationId ?? "");
        setOperationDetail(data);
        return data;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        setError(msg);
        return null;
      } finally {
        setLoading(false);
      }
    },
    [fetchApi]
  );

  const poll = useCallback(
    async (operationId?: string) => {
      const id = operationId ?? latestOperationId;
      if (!id) return;
      setLoading(true);
      setError(null);
      try {
        const data = await fetchApi<unknown>(
          `/api/DHRefreshAAS_PortalStatus?operationId=${encodeURIComponent(id)}`
        );
        setOperationDetail(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    },
    [fetchApi, latestOperationId]
  );

  const loadHistory = useCallback(async () => {
    try {
      const data = await fetchApi<StatusResponse>(
        "/api/DHRefreshAAS_PortalStatus"
      );
      setHistory(data.recentOperations ?? []);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [fetchApi]);

  const selectOperation = useCallback(
    async (operationId: string) => {
      setLatestOperationId(operationId);
      await poll(operationId);
    },
    [poll]
  );

  return {
    latestOperationId,
    submitResult,
    operationDetail,
    history,
    loading,
    error,
    submit,
    poll,
    loadHistory,
    selectOperation,
    setError,
  };
}
