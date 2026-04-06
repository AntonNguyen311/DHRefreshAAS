import { Eye, Clock, Table2, User } from "lucide-react";
import { Card, CardHeader, CardTitle, CardContent } from "./ui/card";
import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import type { RecentOperation } from "../types";
import type { BadgeProps } from "./ui/badge";

function statusVariant(status: string): BadgeProps["variant"] {
  switch (status.toLowerCase()) {
    case "completed":
      return "success";
    case "failed":
      return "destructive";
    case "running":
      return "default";
    case "queued":
      return "warning";
    default:
      return "secondary";
  }
}

interface Props {
  operations: RecentOperation[];
  onViewOperation: (operationId: string) => void;
}

export default function OperationHistory({ operations, onViewOperation }: Props) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>My Recent Operations</CardTitle>
          {operations.length > 0 && (
            <Badge variant="secondary">{operations.length} operations</Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {operations.length === 0 ? (
          <p className="text-sm text-muted-foreground py-8 text-center">
            No recent operations found.
          </p>
        ) : (
          <div className="divide-y divide-border rounded-lg border border-border overflow-hidden">
            {operations.map((op) => (
              <div
                key={op.operationId}
                className="flex items-center justify-between gap-4 px-4 py-3 hover:bg-muted/50 transition-colors"
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <code className="text-xs font-mono text-muted-foreground truncate">
                      {op.operationId}
                    </code>
                    <Badge variant={statusVariant(op.status)}>{op.status}</Badge>
                  </div>
                  <div className="flex items-center gap-4 text-xs text-muted-foreground">
                    <span className="inline-flex items-center gap-1">
                      <Table2 className="h-3 w-3" />
                      {op.tablesCount} tables
                    </span>
                    <span className="inline-flex items-center gap-1">
                      <Clock className="h-3 w-3" />
                      {op.elapsedMinutes} min
                    </span>
                    <span className="inline-flex items-center gap-1">
                      <User className="h-3 w-3" />
                      {op.requestedBy?.email ?? "n/a"}
                    </span>
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => onViewOperation(op.operationId)}
                >
                  <Eye className="h-4 w-4" />
                </Button>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
