import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { cn } from "../../lib/utils";

interface Props {
  title?: string;
  children: string;
  className?: string;
}

export default function CodeBlock({ title, children, className }: Props) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(children);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className={cn("rounded-lg border border-border overflow-hidden", className)}>
      {title && (
        <div className="flex items-center justify-between bg-zinc-900 px-4 py-2 border-b border-zinc-700">
          <span className="text-xs font-medium text-zinc-400 font-mono">{title}</span>
          <button
            onClick={handleCopy}
            className="inline-flex items-center gap-1.5 text-xs text-zinc-400 hover:text-zinc-200 transition-colors"
          >
            {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
            {copied ? "Copied" : "Copy"}
          </button>
        </div>
      )}
      {!title && (
        <button
          onClick={handleCopy}
          className="absolute top-2 right-2 inline-flex items-center gap-1 text-xs text-zinc-500 hover:text-zinc-300 transition-colors bg-zinc-800/80 rounded px-2 py-1"
        >
          {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
        </button>
      )}
      <pre className="relative bg-zinc-950 p-4 text-sm text-zinc-300 font-mono leading-relaxed overflow-auto max-h-80 whitespace-pre-wrap break-words">
        {children}
      </pre>
    </div>
  );
}
