import { Card, CardHeader, CardTitle, CardContent } from "./ui/card";
import CodeBlock from "./ui/code-block";

interface Props {
  detail: unknown;
}

export default function OperationStatus({ detail }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Operation Status</CardTitle>
      </CardHeader>
      <CardContent>
        <CodeBlock title="Response">
          {detail ? JSON.stringify(detail, null, 2) : "No operation selected."}
        </CodeBlock>
      </CardContent>
    </Card>
  );
}
