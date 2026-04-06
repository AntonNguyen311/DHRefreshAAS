import { Configuration, LogLevel } from "@azure/msal-browser";

export const msalConfig: Configuration = {
  auth: {
    clientId: import.meta.env.VITE_MSAL_CLIENT_ID ?? "",
    authority: import.meta.env.VITE_MSAL_AUTHORITY ?? "",
    redirectUri: window.location.origin,
  },
  cache: {
    cacheLocation: "sessionStorage",
  },
  system: {
    loggerOptions: {
      logLevel: LogLevel.Warning,
    },
  },
};

export const loginScopes = [
  "openid",
  "profile",
  "email",
  import.meta.env.VITE_API_SCOPE ?? "",
];

export const apiScope = import.meta.env.VITE_API_SCOPE ?? "";
