import { useMsal, useIsAuthenticated } from "@azure/msal-react";
import { useNavigate } from "react-router-dom";
import { useEffect } from "react";
import { Database, RefreshCw, Activity, LayoutDashboard } from "lucide-react";
import { loginScopes } from "../auth/msalConfig";
import { Button } from "../components/ui/button";
import { Card, CardContent } from "../components/ui/card";

export default function LoginPage() {
  const { instance } = useMsal();
  const isAuthenticated = useIsAuthenticated();
  const navigate = useNavigate();

  useEffect(() => {
    if (isAuthenticated) {
      navigate("/dashboard", { replace: true });
    }
  }, [isAuthenticated, navigate]);

  const handleSignIn = async () => {
    try {
      await instance.loginPopup({ scopes: loginScopes });
      navigate("/dashboard", { replace: true });
    } catch (err) {
      console.error("Login failed:", err);
    }
  };

  const features = [
    {
      icon: <Database className="h-5 w-5" />,
      title: "Browse Allowed Metadata",
      description: "Load only the models, tables, and partitions approved for self-service refresh.",
    },
    {
      icon: <RefreshCw className="h-5 w-5" />,
      title: "Submit Refresh Requests",
      description: "Trigger refreshes from a guided dashboard instead of manually calling the Function App.",
    },
    {
      icon: <Activity className="h-5 w-5" />,
      title: "Track Recent Activity",
      description: "View your latest operations and inspect the current status of each submitted request.",
    },
  ];

  return (
    <main className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 via-blue-50/30 to-zinc-50">
      <div className="w-full max-w-5xl mx-auto px-6 py-12 grid lg:grid-cols-2 gap-8 items-center">
        {/* Hero */}
        <div className="space-y-6">
          <div className="flex items-center gap-2.5">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary text-primary-foreground">
              <LayoutDashboard className="h-5 w-5" />
            </div>
            <span className="text-xs font-bold uppercase tracking-widest text-primary">
              DH Refresh Portal
            </span>
          </div>

          <h1 className="text-4xl font-bold tracking-tight text-foreground leading-tight">
            Sign in to refresh approved AAS models, tables, and partitions
          </h1>

          <p className="text-base text-muted-foreground leading-relaxed max-w-md">
            Use your Microsoft Entra account to open the self-service dashboard,
            browse the metadata you are allowed to use, and trigger refreshes safely.
          </p>

          <div className="flex items-center gap-3">
            <Button size="lg" onClick={handleSignIn}>
              Sign In with Microsoft
            </Button>
            <Button
              variant="outline"
              size="lg"
              disabled={!isAuthenticated}
              onClick={() => navigate("/dashboard")}
            >
              Open Dashboard
            </Button>
          </div>

          <p className="text-xs text-muted-foreground">
            Protected by Microsoft Entra ID. Only authorized users can access this portal.
          </p>
        </div>

        {/* Features */}
        <div className="space-y-4">
          {features.map((f) => (
            <Card key={f.title} className="hover:shadow-md transition-shadow">
              <CardContent className="p-5 flex items-start gap-4">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
                  {f.icon}
                </div>
                <div>
                  <h3 className="text-sm font-semibold mb-1">{f.title}</h3>
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    {f.description}
                  </p>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </main>
  );
}
