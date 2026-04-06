import type { IPublicClientApplication } from "@azure/msal-browser";
import { apiScope } from "../auth/msalConfig";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

export async function apiFetch<T = unknown>(
  msalInstance: IPublicClientApplication,
  path: string,
  options?: RequestInit
): Promise<T> {
  const accounts = msalInstance.getAllAccounts();
  if (accounts.length === 0) {
    throw new Error("No signed-in account.");
  }

  let accessToken: string;
  try {
    const tokenResponse = await msalInstance.acquireTokenSilent({
      account: accounts[0],
      scopes: [apiScope],
    });
    accessToken = tokenResponse.accessToken;
  } catch {
    const tokenResponse = await msalInstance.acquireTokenPopup({
      account: accounts[0],
      scopes: [apiScope],
    });
    accessToken = tokenResponse.accessToken;
  }

  const response = await fetch(API_BASE + path, {
    ...options,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  const text = await response.text();
  let data: T;
  try {
    data = text ? (JSON.parse(text) as T) : ({} as T);
  } catch {
    data = { raw: text } as T;
  }

  if (!response.ok) {
    const err = data as Record<string, unknown>;
    const message =
      (err.error as string) ??
      (err.message as string) ??
      response.statusText ??
      "Request failed.";
    throw new Error(message);
  }

  return data;
}
