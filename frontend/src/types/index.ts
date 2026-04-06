export interface PortalModel {
  databaseName: string;
  allowedTableCount: number;
}

export interface PortalTable {
  tableName: string;
  partitionCount: number;
  defaultRefreshType: string;
}

export interface PortalPartition {
  partitionName: string;
}

export interface PartitionResponse {
  partitions: PortalPartition[];
  defaultRefreshType: string;
  supportsTableRefresh: boolean;
}

export interface PortalUser {
  userId?: string;
  displayName?: string;
  email?: string;
  roles?: string[];
}

export interface ModelsResponse {
  models: PortalModel[];
  user: PortalUser;
}

export interface TablesResponse {
  tables: PortalTable[];
}

export interface PartitionsApiResponse {
  data: PartitionResponse;
}

export interface RefreshPayload {
  databaseName: string;
  refreshObjects: {
    table: string;
    partition: string;
    refreshType: string;
  }[];
}

export interface SubmitResponse {
  operationId: string;
  message: string;
  [key: string]: unknown;
}

export interface OperationRequestedBy {
  userId: string | null;
  displayName: string | null;
  email: string | null;
}

export interface OperationProgress {
  percentage: number;
  completed: number;
  failed: number;
  inProgress: number;
}

export interface RecentOperation {
  operationId: string;
  status: string;
  enqueuedTime: string;
  startTime: string;
  tablesCount: number;
  elapsedMinutes: number;
  queue?: { scope: string };
  progress?: OperationProgress;
  requestedBy?: OperationRequestedBy;
  requestSource?: string;
}

export interface StatusResponse {
  recentOperations: RecentOperation[];
  [key: string]: unknown;
}

export type BannerKind = "info" | "success" | "warning" | "error";
