import { useIsAuthenticated } from "@azure/msal-react";
import { Navigate } from "react-router-dom";
import type { ReactNode } from "react";

interface Props {
  children: ReactNode;
}

export default function AuthGuard({ children }: Props) {
  const isAuthenticated = useIsAuthenticated();

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
}
