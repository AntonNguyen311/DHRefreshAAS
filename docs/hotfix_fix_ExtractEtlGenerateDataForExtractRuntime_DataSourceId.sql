-- Hotfix for SQL 209 after ETL.EtlLog gained DataSourceId.
-- Apply in UAT first, then prod, using the dynamicInvoke pattern in docs/AzureCliAndDatabaseOperations.md.

CREATE OR ALTER PROC [ETL].[ExtractEtlGenerateDataForExtractRuntime]
    @DataSourceId INT,
    @PipelineRunId VARCHAR(36),
    @MasterEtlLineageKey INT
AS
BEGIN
/*
-- CHANGES HISTORY
-- ====================================================================================
-- Date         Author              Changes
-- 2022-05-23   Thao Nguyen         Initial create
-- 2022-07-26   Thao Nguyen         Remove 2-month-old records before inserting new ones
-- 2026-03-30   Cursor              Qualify shared column names after ETL.EtlLog.DataSourceId was added
-- ====================================================================================
*/
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME = SYSUTCDATETIME();
    DECLARE @2MonthsAgo DATETIME = DATEADD(MONTH, -2, @currentDate);

    DELETE FROM [ETL].[ExtractControlTableRuntime]
    WHERE CreatedDateUtc <= @2MonthsAgo;

    INSERT INTO [ETL].[ExtractControlTableRuntime] (
        PipelineRunId,
        LineageKey,
        ExtractLoadControlTableId,
        RowNumber,
        CreatedDateUtc
    )
    SELECT
        @PipelineRunId,
        @MasterEtlLineageKey,
        tbControl.Id,
        ROW_NUMBER() OVER (ORDER BY tbControl.[ExtractOrder], tbControl.[Id] ASC) AS RowNumber,
        @currentDate
    FROM [ETL].[ExtractLoadControlTable] tbControl
    OUTER APPLY (
        SELECT TOP 1
            etlLog.*
        FROM [ETL].[EtlLog] etlLog
        WHERE etlLog.LineageKey = @MasterEtlLineageKey
          AND etlLog.StageName = 'EXTRACT'
          AND etlLog.ExtractLoadControlTableId = tbControl.Id
        ORDER BY etlLog.CreatedDateUtc DESC
    ) tbLog
    WHERE tbControl.DataSourceId = @DataSourceId
      AND tbControl.ExtractEnabled = 1
      AND (tbLog.EtlStatus = 'FAILED' OR tbLog.EtlStatus IS NULL);
END;
GO
