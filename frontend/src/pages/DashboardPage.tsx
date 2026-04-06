import { useEffect, useState, useCallback } from "react";
import { useMsal } from "@azure/msal-react";
import Header from "../components/Header";
import Banner from "../components/Banner";
import StatsGrid from "../components/StatsGrid";
import RefreshForm from "../components/RefreshForm";
import SessionDetails from "../components/SessionDetails";
import SubmissionPanel from "../components/SubmissionPanel";
import OperationStatus from "../components/OperationStatus";
import OperationHistory from "../components/OperationHistory";
import { useModels } from "../hooks/useModels";
import { useTables } from "../hooks/useTables";
import { usePartitions } from "../hooks/usePartitions";
import { useOperations } from "../hooks/useOperations";
import type { BannerKind } from "../types";

export default function DashboardPage() {
  const { instance } = useMsal();
  const account = instance.getActiveAccount();
  const userName = account?.username ?? account?.name ?? "Unknown";

  const {
    models,
    user,
    loading: modelsLoading,
    error: modelsError,
    load: loadModels,
  } = useModels();
  const {
    tables,
    loading: tablesLoading,
    error: tablesError,
    load: loadTables,
    reset: resetTables,
  } = useTables();
  const {
    partitions,
    defaultRefreshType,
    supportsTableRefresh,
    loading: partitionsLoading,
    error: partitionsError,
    load: loadPartitions,
    reset: resetPartitions,
  } = usePartitions();
  const {
    latestOperationId,
    submitResult,
    operationDetail,
    history,
    loading: opsLoading,
    error: opsError,
    submit,
    poll,
    loadHistory,
    selectOperation,
  } = useOperations();

  const [banner, setBanner] = useState<{ kind: BannerKind; message: string } | null>(null);
  const [lastPayload, setLastPayload] = useState("{}");

  const loading = modelsLoading || tablesLoading || partitionsLoading || opsLoading;

  const activeError = modelsError ?? tablesError ?? partitionsError ?? opsError;
  useEffect(() => {
    if (activeError) {
      setBanner({ kind: "error", message: activeError });
    }
  }, [activeError]);

  useEffect(() => {
    const init = async () => {
      await loadModels();
      await loadHistory();
      setBanner({ kind: "success", message: "Metadata loaded successfully." });
    };
    init();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleModelChange = useCallback(
    async (databaseName: string) => {
      resetTables();
      resetPartitions();
      if (databaseName) {
        await loadTables(databaseName);
        setBanner({ kind: "success", message: "Tables loaded successfully." });
      }
    },
    [loadTables, resetTables, resetPartitions]
  );

  const handleTableChange = useCallback(
    async (databaseName: string, tableName: string) => {
      resetPartitions();
      if (databaseName && tableName) {
        await loadPartitions(databaseName, tableName);
        setBanner({ kind: "success", message: "Partitions loaded successfully." });
      }
    },
    [loadPartitions, resetPartitions]
  );

  const handleSubmit = useCallback(
    async (
      databaseName: string,
      tableName: string,
      partitionName: string,
      refreshType: string
    ) => {
      const payload = {
        databaseName,
        refreshObjects: [{ table: tableName, partition: partitionName, refreshType }],
      };
      setLastPayload(JSON.stringify(payload, null, 2));
      const result = await submit(payload);
      if (result) {
        setBanner({ kind: "success", message: result.message ?? "Refresh accepted." });
        await loadHistory();
      }
    },
    [submit, loadHistory]
  );

  const handlePoll = useCallback(async () => {
    await poll();
    setBanner({ kind: "info", message: "Latest operation status refreshed." });
  }, [poll]);

  return (
    <div className="min-h-screen bg-background">
      <Header userName={userName} />

      <main className="max-w-7xl mx-auto px-6 pb-12">
        <Banner kind={banner?.kind ?? "info"} message={banner?.message ?? null} />

        <StatsGrid
          userEmail={userName}
          modelsCount={models.length}
          partitionsCount={partitions.length}
          recentOperationsCount={history.length}
        />

        <div className="grid lg:grid-cols-2 gap-6 mb-6">
          <RefreshForm
            models={models}
            tables={tables}
            partitions={partitions}
            defaultRefreshType={defaultRefreshType}
            supportsTableRefresh={supportsTableRefresh}
            loading={loading}
            onModelChange={handleModelChange}
            onTableChange={handleTableChange}
            onReloadMetadata={loadModels}
            onSubmit={handleSubmit}
          />
          <SessionDetails user={user} />
        </div>

        <div className="grid lg:grid-cols-2 gap-6 mb-6">
          <SubmissionPanel
            submitResult={submitResult}
            payload={lastPayload}
            hasLatestOperation={!!latestOperationId}
            loading={loading}
            onPollLatest={handlePoll}
            onLoadHistory={loadHistory}
          />
          <OperationStatus detail={operationDetail} />
        </div>

        <OperationHistory operations={history} onViewOperation={selectOperation} />
      </main>
    </div>
  );
}
