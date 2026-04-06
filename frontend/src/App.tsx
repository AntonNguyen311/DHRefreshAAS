import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { MsalProvider } from "@azure/msal-react";
import { PublicClientApplication } from "@azure/msal-browser";
import { msalConfig } from "./auth/msalConfig";
import AuthGuard from "./auth/AuthGuard";
import LoginPage from "./pages/LoginPage";
import DashboardPage from "./pages/DashboardPage";
import "./App.css";

const msalInstance = new PublicClientApplication(msalConfig);

export default function App() {
  return (
    <MsalProvider instance={msalInstance}>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/dashboard"
            element={
              <AuthGuard>
                <DashboardPage />
              </AuthGuard>
            }
          />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </BrowserRouter>
    </MsalProvider>
  );
}
