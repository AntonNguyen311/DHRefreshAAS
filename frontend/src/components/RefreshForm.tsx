import { useState, useEffect } from "react";
import { Send, RefreshCw } from "lucide-react";
import { Card, CardHeader, CardTitle, CardContent } from "./ui/card";
import { Button } from "./ui/button";
import { Select } from "./ui/select";
import { Badge } from "./ui/badge";
import type { PortalModel, PortalTable, PortalPartition } from "../types";

interface Props {
  models: PortalModel[];
  tables: PortalTable[];
  partitions: PortalPartition[];
  defaultRefreshType: string;
  supportsTableRefresh: boolean;
  loading: boolean;
  onModelChange: (databaseName: string) => void;
  onTableChange: (databaseName: string, tableName: string) => void;
  onReloadMetadata: () => void;
  onSubmit: (
    databaseName: string,
    tableName: string,
    partitionName: string,
    refreshType: string
  ) => void;
}

export default function RefreshForm({
  models,
  tables,
  partitions,
  defaultRefreshType,
  supportsTableRefresh,
  loading,
  onModelChange,
  onTableChange,
  onReloadMetadata,
  onSubmit,
}: Props) {
  const [selectedModel, setSelectedModel] = useState("");
  const [selectedTable, setSelectedTable] = useState("");
  const [selectedPartition, setSelectedPartition] = useState("");
  const [refreshType, setRefreshType] = useState("Full");
  const [wholeTable, setWholeTable] = useState(true);

  useEffect(() => {
    setRefreshType(defaultRefreshType);
  }, [defaultRefreshType]);

  useEffect(() => {
    setWholeTable(supportsTableRefresh);
  }, [supportsTableRefresh]);

  const handleModelChange = (value: string) => {
    setSelectedModel(value);
    setSelectedTable("");
    setSelectedPartition("");
    onModelChange(value);
  };

  const handleTableChange = (value: string) => {
    setSelectedTable(value);
    setSelectedPartition("");
    onTableChange(selectedModel, value);
  };

  const canSubmit =
    !loading && selectedModel && selectedTable && (wholeTable || selectedPartition);

  const handleSubmit = () => {
    if (!canSubmit) return;
    onSubmit(selectedModel, selectedTable, wholeTable ? "" : selectedPartition, refreshType);
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Refresh Request</CardTitle>
          <Button variant="ghost" size="sm" onClick={onReloadMetadata} disabled={loading}>
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <label className="text-sm font-medium mb-1.5 block">
            Model
            {models.length > 0 && (
              <Badge variant="secondary" className="ml-2">{models.length}</Badge>
            )}
          </label>
          <Select
            value={selectedModel}
            disabled={models.length === 0}
            onChange={(e) => handleModelChange(e.target.value)}
          >
            <option value="">Select a model</option>
            {models.map((m) => (
              <option key={m.databaseName} value={m.databaseName}>
                {m.databaseName} ({m.allowedTableCount} tables)
              </option>
            ))}
          </Select>
        </div>

        <div>
          <label className="text-sm font-medium mb-1.5 block">
            Table
            {tables.length > 0 && (
              <Badge variant="secondary" className="ml-2">{tables.length}</Badge>
            )}
          </label>
          <Select
            value={selectedTable}
            disabled={tables.length === 0}
            onChange={(e) => handleTableChange(e.target.value)}
          >
            <option value="">Select a table</option>
            {tables.map((t) => (
              <option key={t.tableName} value={t.tableName}>
                {t.tableName} ({t.partitionCount} partitions)
              </option>
            ))}
          </Select>
        </div>

        <label className="flex items-center gap-2.5 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={wholeTable}
            disabled={!supportsTableRefresh}
            onChange={(e) => setWholeTable(e.target.checked)}
            className="h-4 w-4 rounded border-border accent-primary"
          />
          <span className="text-sm font-medium">Refresh the whole table</span>
        </label>

        <div>
          <label className="text-sm font-medium mb-1.5 block">
            Partition
            {partitions.length > 0 && (
              <Badge variant="secondary" className="ml-2">{partitions.length}</Badge>
            )}
          </label>
          <Select
            value={selectedPartition}
            disabled={wholeTable || partitions.length === 0}
            onChange={(e) => setSelectedPartition(e.target.value)}
          >
            <option value="">Select a partition</option>
            {partitions.map((p) => (
              <option key={p.partitionName} value={p.partitionName}>
                {p.partitionName}
              </option>
            ))}
          </Select>
        </div>

        <div>
          <label className="text-sm font-medium mb-1.5 block">Refresh Type</label>
          <Select
            value={refreshType}
            disabled={tables.length === 0}
            onChange={(e) => setRefreshType(e.target.value)}
          >
            <option value="Full">Full</option>
            <option value="DataOnly">DataOnly</option>
          </Select>
        </div>

        <Button className="w-full" disabled={!canSubmit} onClick={handleSubmit}>
          <Send className="h-4 w-4" />
          Submit Refresh
        </Button>
      </CardContent>
    </Card>
  );
}
