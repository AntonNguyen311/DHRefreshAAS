import { useMsal } from "@azure/msal-react";
import { useNavigate } from "react-router-dom";
import { LogOut, LayoutDashboard } from "lucide-react";
import { Button } from "./ui/button";

interface Props {
  userName: string;
}

export default function Header({ userName }: Props) {
  const { instance } = useMsal();
  const navigate = useNavigate();

  const handleSignOut = async () => {
    const account = instance.getActiveAccount();
    if (account) {
      await instance.logoutPopup({ account });
    }
    navigate("/login");
  };

  return (
    <header className="flex items-center justify-between border-b border-border bg-card px-6 py-4 mb-6">
      <div className="flex items-center gap-3">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary text-primary-foreground">
          <LayoutDashboard className="h-5 w-5" />
        </div>
        <div>
          <h1 className="text-lg font-semibold tracking-tight">DH Refresh Portal</h1>
          <p className="text-xs text-muted-foreground">{userName}</p>
        </div>
      </div>
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" onClick={handleSignOut}>
          <LogOut className="h-4 w-4" />
          Sign Out
        </Button>
      </div>
    </header>
  );
}
