import { useMsal } from "@azure/msal-react";
import { useCallback } from "react";
import { apiFetch } from "../api/client";

export function useApi() {
  const { instance } = useMsal();

  const fetchApi = useCallback(
    <T = unknown>(path: string, options?: RequestInit) =>
      apiFetch<T>(instance, path, options),
    [instance]
  );

  return fetchApi;
}
