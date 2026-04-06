import { User, Database, Layers, Activity } from "lucide-react";
import { Card, CardContent } from "./ui/card";

interface StatCardProps {
  label: string;
  value: string | number;
  icon: React.ReactNode;
  accent?: string;
}

function StatCard({ label, value, icon, accent = "bg-primary/10 text-primary" }: StatCardProps) {
  return (
    <Card>
      <CardContent className="p-5">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
              {label}
            </p>
            <p className="text-2xl font-bold tracking-tight">{value}</p>
          </div>
          <div className={`flex h-10 w-10 items-center justify-center rounded-lg ${accent}`}>
            {icon}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

interface Props {
  userEmail: string;
  modelsCount: number;
  partitionsCount: number;
  recentOperationsCount: number;
}

export default function StatsGrid({
  userEmail,
  modelsCount,
  partitionsCount,
  recentOperationsCount,
}: Props) {
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4 mb-6">
      <StatCard
        label="Signed-in User"
        value={userEmail}
        icon={<User className="h-5 w-5" />}
      />
      <StatCard
        label="Available Models"
        value={modelsCount}
        icon={<Database className="h-5 w-5" />}
        accent="bg-emerald-50 text-emerald-600"
      />
      <StatCard
        label="Loaded Partitions"
        value={partitionsCount}
        icon={<Layers className="h-5 w-5" />}
        accent="bg-amber-50 text-amber-600"
      />
      <StatCard
        label="Recent Operations"
        value={recentOperationsCount}
        icon={<Activity className="h-5 w-5" />}
        accent="bg-violet-50 text-violet-600"
      />
    </div>
  );
}
