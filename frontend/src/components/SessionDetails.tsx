import { Card, CardHeader, CardTitle, CardContent } from "./ui/card";
import CodeBlock from "./ui/code-block";
import type { PortalUser } from "../types";

interface Props {
  user: PortalUser | null;
}

export default function SessionDetails({ user }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Session Details</CardTitle>
      </CardHeader>
      <CardContent>
        <CodeBlock title="User Profile">
          {user ? JSON.stringify(user, null, 2) : "No user profile loaded."}
        </CodeBlock>
      </CardContent>
    </Card>
  );
}
