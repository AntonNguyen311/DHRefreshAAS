import { AlertCircle, CheckCircle2, Info, AlertTriangle } from "lucide-react";
import type { BannerKind } from "../types";
import { cn } from "../lib/utils";

interface Props {
  kind: BannerKind;
  message: string | null;
}

const config: Record<BannerKind, { icon: typeof Info; bg: string; border: string; text: string }> = {
  info: { icon: Info, bg: "bg-blue-50", border: "border-blue-200", text: "text-blue-800" },
  success: { icon: CheckCircle2, bg: "bg-emerald-50", border: "border-emerald-200", text: "text-emerald-800" },
  warning: { icon: AlertTriangle, bg: "bg-amber-50", border: "border-amber-200", text: "text-amber-800" },
  error: { icon: AlertCircle, bg: "bg-red-50", border: "border-red-200", text: "text-red-800" },
};

export default function Banner({ kind, message }: Props) {
  if (!message) return null;

  const { icon: Icon, bg, border, text } = config[kind];

  return (
    <div className={cn("flex items-center gap-3 rounded-lg border px-4 py-3 text-sm font-medium mb-6", bg, border, text)}>
      <Icon className="h-4 w-4 shrink-0" />
      {message}
    </div>
  );
}
