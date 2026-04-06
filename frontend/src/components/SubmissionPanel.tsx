import { Search, History } from "lucide-react";
import { Card, CardHeader, CardTitle, CardContent } from "./ui/card";
import { Button } from "./ui/button";
import CodeBlock from "./ui/code-block";
import type { SubmitResponse } from "../types";

interface Props {
  submitResult: SubmitResponse | null;
  payload: string;
  hasLatestOperation: boolean;
  loading: boolean;
  onPollLatest: () => void;
  onLoadHistory: () => void;
}

export default function SubmissionPanel({
  submitResult,
  payload,
  hasLatestOperation,
  loading,
  onPollLatest,
  onLoadHistory,
}: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Latest Submission</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          {submitResult?.message ?? "No refresh submitted yet."}
        </p>
        <CodeBlock title="Request Payload">{payload}</CodeBlock>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={!hasLatestOperation || loading}
            onClick={onPollLatest}
          >
            <Search className="h-4 w-4" />
            Poll Status
          </Button>
          <Button variant="outline" size="sm" disabled={loading} onClick={onLoadHistory}>
            <History className="h-4 w-4" />
            My History
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
