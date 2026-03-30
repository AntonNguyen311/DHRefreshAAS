-- Add typed ETL observability and lineage-aware failure policy objects.
-- This migration is idempotent and intended for the prod ETL database first.

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'ParentPipelineRunId'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD ParentPipelineRunId VARCHAR(36) NULL;

    PRINT 'Column ETL.EtlLog.ParentPipelineRunId added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.ParentPipelineRunId already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'ChildPipelineRunId'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD ChildPipelineRunId VARCHAR(36) NULL;

    PRINT 'Column ETL.EtlLog.ChildPipelineRunId added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.ChildPipelineRunId already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'DataSourceId'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD DataSourceId INT NULL;

    PRINT 'Column ETL.EtlLog.DataSourceId added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.DataSourceId already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'SqlErrorNumber'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD SqlErrorNumber INT NULL;

    PRINT 'Column ETL.EtlLog.SqlErrorNumber added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.SqlErrorNumber already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'FailureCategory'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD FailureCategory VARCHAR(50) NULL;

    PRINT 'Column ETL.EtlLog.FailureCategory added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.FailureCategory already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'IsTransient'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD IsTransient BIT NULL;

    PRINT 'Column ETL.EtlLog.IsTransient added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.IsTransient already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'RetryAttempt'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD RetryAttempt INT NULL;

    PRINT 'Column ETL.EtlLog.RetryAttempt added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.RetryAttempt already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'RawActivityPayload'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD RawActivityPayload NVARCHAR(MAX) NULL;

    PRINT 'Column ETL.EtlLog.RawActivityPayload added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.RawActivityPayload already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'OutcomeCode'
)
BEGIN
    ALTER TABLE ETL.EtlLog
    ADD OutcomeCode VARCHAR(40) NULL;

    PRINT 'Column ETL.EtlLog.OutcomeCode added';
END
ELSE
BEGIN
    PRINT 'Column ETL.EtlLog.OutcomeCode already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('ETL.EtlLog')
      AND name = 'IX_EtlLog_LineageStageTableCreatedUtc'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_EtlLog_LineageStageTableCreatedUtc
    ON ETL.EtlLog (
        LineageKey,
        StageName,
        ExtractLoadControlTableId,
        CreatedDateUtc
    )
    INCLUDE (
        EtlStatus,
        OutcomeCode,
        IsTransient,
        RetryAttempt,
        DataSourceId,
        TableName,
        PipelineRunId,
        ActivityRunId,
        SqlErrorNumber
    );

    PRINT 'Index IX_EtlLog_LineageStageTableCreatedUtc created';
END
ELSE
BEGIN
    PRINT 'Index IX_EtlLog_LineageStageTableCreatedUtc already exists';
END
GO

IF OBJECT_ID('ETL.TableExecutionPolicy', 'U') IS NULL
BEGIN
    CREATE TABLE ETL.TableExecutionPolicy (
        TableExecutionPolicyId INT IDENTITY(1,1) NOT NULL,
        ExtractLoadControlTableId INT NOT NULL,
        PolicyName NVARCHAR(100) NULL,
        MaxRetryCount INT NOT NULL CONSTRAINT DF_TableExecutionPolicy_MaxRetryCount DEFAULT (2),
        CooldownSeconds INT NOT NULL CONSTRAINT DF_TableExecutionPolicy_CooldownSeconds DEFAULT (0),
        ForceSerialExecution BIT NOT NULL CONSTRAINT DF_TableExecutionPolicy_ForceSerialExecution DEFAULT (0),
        IsQuarantined BIT NOT NULL CONSTRAINT DF_TableExecutionPolicy_IsQuarantined DEFAULT (0),
        QuarantineReason NVARCHAR(1000) NULL,
        IsEnabled BIT NOT NULL CONSTRAINT DF_TableExecutionPolicy_IsEnabled DEFAULT (1),
        ModifiedBy NVARCHAR(256) NULL,
        ModifiedAtUtc DATETIME2(0) NOT NULL CONSTRAINT DF_TableExecutionPolicy_ModifiedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_TableExecutionPolicy PRIMARY KEY CLUSTERED (TableExecutionPolicyId)
    );

    PRINT 'Table ETL.TableExecutionPolicy created';
END
ELSE
BEGIN
    PRINT 'Table ETL.TableExecutionPolicy already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('ETL.TableExecutionPolicy')
      AND name = 'UX_TableExecutionPolicy_ExtractLoadControlTableId'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_TableExecutionPolicy_ExtractLoadControlTableId
    ON ETL.TableExecutionPolicy (ExtractLoadControlTableId)
    WHERE IsEnabled = 1;

    PRINT 'Index UX_TableExecutionPolicy_ExtractLoadControlTableId created';
END
ELSE
BEGIN
    PRINT 'Index UX_TableExecutionPolicy_ExtractLoadControlTableId already exists';
END
GO

IF OBJECT_ID('ETL.EtlFailureDiagnosticSnapshot', 'U') IS NULL
BEGIN
    CREATE TABLE ETL.EtlFailureDiagnosticSnapshot (
        EtlFailureDiagnosticSnapshotId INT IDENTITY(1,1) NOT NULL,
        LineageKey INT NOT NULL,
        StageName VARCHAR(20) NOT NULL,
        ExtractLoadControlTableId INT NULL,
        DataSourceId INT NULL,
        TableName VARCHAR(100) NULL,
        PipelineRunId VARCHAR(36) NULL,
        ActivityRunId VARCHAR(36) NULL,
        SqlErrorNumber INT NULL,
        FailureCategory VARCHAR(50) NULL,
        DatabaseName SYSNAME NOT NULL CONSTRAINT DF_EtlFailureDiagnosticSnapshot_DatabaseName DEFAULT (DB_NAME()),
        ServiceObjective VARCHAR(128) NULL,
        ElasticPoolName SYSNAME NULL,
        SnapshotEndTimeUtc DATETIME NULL,
        AvgCpuPercent DECIMAL(9,2) NULL,
        AvgDataIoPercent DECIMAL(9,2) NULL,
        AvgLogWritePercent DECIMAL(9,2) NULL,
        Notes NVARCHAR(2000) NULL,
        CreatedDateUtc DATETIME2(0) NOT NULL CONSTRAINT DF_EtlFailureDiagnosticSnapshot_CreatedDateUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_EtlFailureDiagnosticSnapshot PRIMARY KEY CLUSTERED (EtlFailureDiagnosticSnapshotId)
    );

    PRINT 'Table ETL.EtlFailureDiagnosticSnapshot created';
END
ELSE
BEGIN
    PRINT 'Table ETL.EtlFailureDiagnosticSnapshot already exists';
END
GO

CREATE OR ALTER FUNCTION ETL.fn_ExtractSqlErrorNumber (
    @ActivityError NVARCHAR(MAX)
)
RETURNS INT
AS
BEGIN
    DECLARE @result INT = NULL;
    DECLARE @segment NVARCHAR(100);
    DECLARE @start INT;
    DECLARE @len INT;

    IF @ActivityError IS NULL OR LEN(@ActivityError) = 0
    BEGIN
        RETURN NULL;
    END

    SET @start = PATINDEX('%SqlErrorNumber=[0-9]%', @ActivityError);
    IF @start > 0
    BEGIN
        SET @segment = SUBSTRING(@ActivityError, @start + LEN('SqlErrorNumber='), 50);
        SET @len = PATINDEX('%[^0-9]%', @segment + 'X') - 1;
        SET @result = TRY_CONVERT(INT, LEFT(@segment, @len));
        RETURN @result;
    END

    SET @start = PATINDEX('%Error code [0-9]%', @ActivityError);
    IF @start > 0
    BEGIN
        SET @segment = SUBSTRING(@ActivityError, @start + LEN('Error code '), 50);
        SET @len = PATINDEX('%[^0-9]%', @segment + 'X') - 1;
        SET @result = TRY_CONVERT(INT, LEFT(@segment, @len));
        RETURN @result;
    END

    SET @start = PATINDEX('%Number=[0-9]%', @ActivityError);
    IF @start > 0
    BEGIN
        SET @segment = SUBSTRING(@ActivityError, @start + LEN('Number='), 50);
        SET @len = PATINDEX('%[^0-9]%', @segment + 'X') - 1;
        SET @result = TRY_CONVERT(INT, LEFT(@segment, @len));
        RETURN @result;
    END

    RETURN NULL;
END;
GO

CREATE OR ALTER PROC ETL.EtlAddLog
(
    @I_LineageKey INT,
    @V_StageName VARCHAR(20),
    @V_SourceName VARCHAR(10),
    @V_SchemaName VARCHAR(20),
    @V_TableName VARCHAR(100),
    @V_StoredProcedureName VARCHAR(300),
    @V_EtlStatus VARCHAR(20),
    @V_DataFactory VARCHAR(100),
    @V_PipelineName VARCHAR(140),
    @V_PipelineRunId VARCHAR(36),
    @N_ActivityError NVARCHAR(MAX),
    @V_ActivityStatus VARCHAR(20),
    @V_ActivityDuration VARCHAR(20),
    @V_ActivityRunId VARCHAR(36),
    @N_ActivityOutput NVARCHAR(MAX),
    @D_ActivityExecutionStartTime DATETIME,
    @D_ActivityExecutionEndTime DATETIME,
    @V_ActivityStatusCode VARCHAR(20),
    @N_Parametters NVARCHAR(MAX),
    @V_RaiseErrorIfActivityFailed VARCHAR(3),
    @V_ParentPipelineRunId VARCHAR(36) = NULL,
    @V_ChildPipelineRunId VARCHAR(36) = NULL,
    @I_DataSourceId INT = NULL,
    @I_ExtractLoadControlTableId INT = NULL,
    @I_SqlErrorNumber INT = NULL,
    @V_FailureCategory VARCHAR(50) = NULL,
    @B_IsTransient BIT = NULL,
    @I_RetryAttempt INT = NULL,
    @N_RawActivityPayload NVARCHAR(MAX) = NULL,
    @V_OutcomeCode VARCHAR(40) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME = SYSUTCDATETIME();
    DECLARE @errorMessage NVARCHAR(MAX);
    DECLARE @effectiveExtractLoadControlTableId INT =
        COALESCE(NULLIF(@I_ExtractLoadControlTableId, 0), TRY_CONVERT(INT, JSON_VALUE(@N_Parametters, '$.ExtractLoadControlTableSettings.Id')));
    DECLARE @effectiveDataSourceId INT =
        COALESCE(NULLIF(@I_DataSourceId, 0), TRY_CONVERT(INT, JSON_VALUE(@N_Parametters, '$.ExtractLoadControlTableSettings.DataSourceId')));
    DECLARE @effectiveParentPipelineRunId VARCHAR(36) =
        COALESCE(NULLIF(@V_ParentPipelineRunId, ''), JSON_VALUE(@N_Parametters, '$.ParentPipelineRunId'));
    DECLARE @effectiveChildPipelineRunId VARCHAR(36) =
        COALESCE(NULLIF(@V_ChildPipelineRunId, ''), @V_PipelineRunId);
    DECLARE @effectiveSqlErrorNumber INT = COALESCE(@I_SqlErrorNumber, ETL.fn_ExtractSqlErrorNumber(@N_ActivityError));
    DECLARE @effectiveFailureCategory VARCHAR(50);
    DECLARE @effectiveIsTransient BIT;
    DECLARE @effectiveRetryAttempt INT;
    DECLARE @effectiveOutcomeCode VARCHAR(40);
    DECLARE @effectiveRawActivityPayload NVARCHAR(MAX) = COALESCE(@N_RawActivityPayload, @N_ActivityOutput, @N_ActivityError, @N_Parametters);

    SET @effectiveFailureCategory = COALESCE(
        NULLIF(@V_FailureCategory, ''),
        CASE
            WHEN UPPER(ISNULL(@V_EtlStatus, '')) = 'SKIPPED' THEN 'SkippedByPolicy'
            WHEN UPPER(ISNULL(@V_ActivityStatus, '')) NOT IN ('FAILED') AND UPPER(ISNULL(@V_EtlStatus, '')) NOT LIKE 'FAILED%' THEN NULL
            WHEN @effectiveSqlErrorNumber = 40197 THEN 'SqlTransient'
            WHEN ISNULL(@N_ActivityError, '') LIKE '%Error code 9001%' THEN 'SqlLogWritePressure'
            WHEN ISNULL(@N_ActivityError, '') LIKE '%Error code 3314%' THEN 'SqlRecovery'
            WHEN ISNULL(@N_ActivityError, '') LIKE '%severe error occurred on the current command%' THEN 'SqlSevereAbort'
            ELSE 'DeterministicFailure'
        END
    );

    SET @effectiveIsTransient = COALESCE(
        @B_IsTransient,
        CASE
            WHEN @effectiveFailureCategory IN ('SqlTransient', 'SqlLogWritePressure', 'SqlRecovery', 'SqlSevereAbort') THEN 1
            ELSE 0
        END
    );

    SET @effectiveRetryAttempt = COALESCE(
        @I_RetryAttempt,
        (
            SELECT COUNT(1) + 1
            FROM ETL.EtlLog l
            WHERE l.LineageKey = @I_LineageKey
              AND l.StageName = @V_StageName
              AND ISNULL(l.ExtractLoadControlTableId, -1) = ISNULL(@effectiveExtractLoadControlTableId, -1)
        )
    );

    SET @effectiveOutcomeCode = COALESCE(
        NULLIF(@V_OutcomeCode, ''),
        CASE
            WHEN UPPER(ISNULL(@V_EtlStatus, '')) = 'SKIPPED' THEN 'SkippedByPolicy'
            WHEN UPPER(ISNULL(@V_ActivityStatus, '')) = 'SUCCEEDED' OR UPPER(ISNULL(@V_EtlStatus, '')) = 'COMPLETED' THEN 'Succeeded'
            WHEN UPPER(ISNULL(@V_ActivityStatus, '')) = 'FAILED' OR UPPER(ISNULL(@V_EtlStatus, '')) LIKE 'FAILED%' THEN
                CASE
                    WHEN @effectiveIsTransient = 1 THEN 'FailedTransient'
                    ELSE 'FailedDeterministic'
                END
            ELSE NULL
        END
    );

    INSERT INTO ETL.EtlLog
           (
            LineageKey,
            StageName,
            ExtractLoadControlTableId,
            SourceName,
            SchemaName,
            TableName,
            StoredProcedureName,
            EtlStatus,
            DataFactory,
            PipelineName,
            PipelineRunId,
            ActivityError,
            ActivityStatus,
            ActivityDuration,
            ActivityRunId,
            ActivityOutput,
            ActivityExecutionStartTime,
            ActivityExecutionEndTime,
            ActivityStatusCode,
            Parametters,
            CreatedDateUtc,
            ParentPipelineRunId,
            ChildPipelineRunId,
            DataSourceId,
            SqlErrorNumber,
            FailureCategory,
            IsTransient,
            RetryAttempt,
            RawActivityPayload,
            OutcomeCode
           )
     VALUES
           (
            @I_LineageKey,
            @V_StageName,
            @effectiveExtractLoadControlTableId,
            @V_SourceName,
            @V_SchemaName,
            @V_TableName,
            @V_StoredProcedureName,
            @V_EtlStatus,
            @V_DataFactory,
            @V_PipelineName,
            @V_PipelineRunId,
            @N_ActivityError,
            @V_ActivityStatus,
            @V_ActivityDuration,
            @V_ActivityRunId,
            @N_ActivityOutput,
            @D_ActivityExecutionStartTime,
            @D_ActivityExecutionEndTime,
            @V_ActivityStatusCode,
            @N_Parametters,
            @currentDate,
            @effectiveParentPipelineRunId,
            @effectiveChildPipelineRunId,
            @effectiveDataSourceId,
            @effectiveSqlErrorNumber,
            @effectiveFailureCategory,
            @effectiveIsTransient,
            @effectiveRetryAttempt,
            @effectiveRawActivityPayload,
            @effectiveOutcomeCode
           );

    IF (UPPER(@V_RaiseErrorIfActivityFailed) = 'YES' AND UPPER(@V_ActivityStatus) = 'FAILED')
    BEGIN
        SET @errorMessage = 'ActivityRunId = ' + @V_ActivityRunId + ' was Failed.';
        RAISERROR (@errorMessage, 16, 1);
    END
END;
GO

CREATE OR ALTER VIEW ETL.vw_EtlLog
AS
    SELECT
        l.*,
        JSON_VALUE(l.ActivityOutput, '$.rowsRead') AS ActivityOutput_ExtractRowsRead,
        JSON_VALUE(l.ActivityOutput, '$.rowsCopied') AS ActivityOutput_ExtractRowsCopied,
        JSON_VALUE(l.ActivityOutput, '$.firstRow.ReadRows') AS ActivityOutput_LoadReadRows,
        JSON_VALUE(l.ActivityOutput, '$.firstRow.InsertedRows') AS ActivityOutput_LoadInsertedRows,
        JSON_VALUE(l.ActivityOutput, '$.firstRow.UpdatedRows') AS ActivityOutput_LoadUpdatedRows,
        JSON_VALUE(l.ActivityOutput, '$.firstRow.DeletedRows') AS ActivityOutput_LoadDeletedRows,
        JSON_VALUE(l.ActivityOutput, '$.firstRow.UnchangedRows') AS ActivityOutput_LoadUnchangedRows,
        JSON_VALUE(l.Parametters, '$.MasterSettings.CountryKey') AS MasterSettings_CountryKey,
        COALESCE(l.ChildPipelineRunId, l.PipelineRunId) AS EffectiveChildPipelineRunId,
        COALESCE(
            NULLIF(l.OutcomeCode, ''),
            CASE
                WHEN UPPER(ISNULL(l.EtlStatus, '')) = 'SKIPPED' THEN 'SkippedByPolicy'
                WHEN UPPER(ISNULL(l.ActivityStatus, '')) = 'SUCCEEDED' OR UPPER(ISNULL(l.EtlStatus, '')) = 'COMPLETED' THEN 'Succeeded'
                WHEN ISNULL(l.IsTransient, 0) = 1 THEN 'FailedTransient'
                WHEN UPPER(ISNULL(l.ActivityStatus, '')) = 'FAILED' OR UPPER(ISNULL(l.EtlStatus, '')) LIKE 'FAILED%' THEN 'FailedDeterministic'
                ELSE NULL
            END
        ) AS EffectiveOutcomeCode,
        ml.LogId AS MasterLogId,
        ml.PipelineRunId AS MasterPipelineRunId,
        ml.PipelineName AS MasterPipelineName,
        ml.EtlStatus AS MasterETLStatus,
        ml.ExecutionStartUtc AS MasterExecutionStartUtc,
        ml.ExecutionEndUtc AS MasterExecutionEndUtc,
        ml.ErrorCode AS MasterErrorCode,
        ml.ErrorMessage AS MasterErrorMessage
    FROM ETL.EtlLog l
    OUTER APPLY (
        SELECT TOP 1 mlInner.*
        FROM ETL.MasterEtlLog mlInner
        WHERE mlInner.LineageKey = l.LineageKey
        ORDER BY mlInner.CreatedDateUtc DESC, mlInner.LogId DESC
    ) ml;
GO

CREATE OR ALTER VIEW ETL.vw_EtlLatestTableOutcome
AS
WITH ranked AS (
    SELECT
        l.EtlLogId,
        l.LineageKey,
        l.StageName,
        l.ExtractLoadControlTableId,
        l.DataSourceId,
        l.SchemaName,
        l.TableName,
        l.PipelineRunId,
        l.ParentPipelineRunId,
        l.ChildPipelineRunId,
        l.EtlStatus,
        l.ActivityStatus,
        l.SqlErrorNumber,
        l.FailureCategory,
        l.IsTransient,
        l.RetryAttempt,
        l.CreatedDateUtc,
        COALESCE(
            NULLIF(l.OutcomeCode, ''),
            CASE
                WHEN UPPER(ISNULL(l.EtlStatus, '')) = 'SKIPPED' THEN 'SkippedByPolicy'
                WHEN UPPER(ISNULL(l.ActivityStatus, '')) = 'SUCCEEDED' OR UPPER(ISNULL(l.EtlStatus, '')) = 'COMPLETED' THEN 'Succeeded'
                WHEN ISNULL(l.IsTransient, 0) = 1 THEN 'FailedTransient'
                WHEN UPPER(ISNULL(l.ActivityStatus, '')) = 'FAILED' OR UPPER(ISNULL(l.EtlStatus, '')) LIKE 'FAILED%' THEN 'FailedDeterministic'
                ELSE NULL
            END
        ) AS OutcomeCode,
        ROW_NUMBER() OVER (
            PARTITION BY l.LineageKey, l.StageName, l.ExtractLoadControlTableId
            ORDER BY l.CreatedDateUtc DESC, l.EtlLogId DESC
        ) AS rn
    FROM ETL.EtlLog l
    WHERE l.ExtractLoadControlTableId IS NOT NULL
)
SELECT
    EtlLogId,
    LineageKey,
    StageName,
    ExtractLoadControlTableId,
    DataSourceId,
    SchemaName,
    TableName,
    PipelineRunId,
    ParentPipelineRunId,
    ChildPipelineRunId,
    EtlStatus,
    ActivityStatus,
    SqlErrorNumber,
    FailureCategory,
    IsTransient,
    RetryAttempt,
    CreatedDateUtc,
    OutcomeCode
FROM ranked
WHERE rn = 1;
GO

CREATE OR ALTER VIEW ETL.vw_EtlStageSummary
AS
SELECT
    lo.LineageKey,
    lo.StageName,
    COUNT(1) AS TotalTableCount,
    SUM(CASE WHEN lo.OutcomeCode = 'Succeeded' THEN 1 ELSE 0 END) AS SucceededCount,
    SUM(CASE WHEN lo.OutcomeCode = 'FailedTransient' THEN 1 ELSE 0 END) AS FailedTransientCount,
    SUM(CASE WHEN lo.OutcomeCode = 'FailedDeterministic' THEN 1 ELSE 0 END) AS FailedDeterministicCount,
    SUM(CASE WHEN lo.OutcomeCode = 'SkippedByPolicy' THEN 1 ELSE 0 END) AS SkippedByPolicyCount,
    MAX(lo.CreatedDateUtc) AS LatestActivityUtc
FROM ETL.vw_EtlLatestTableOutcome lo
GROUP BY
    lo.LineageKey,
    lo.StageName;
GO

CREATE OR ALTER PROC ETL.p_GetLoadTableExecutionQueue
    @LineageKey INT,
    @LoadOrder INT,
    @ForceSerialExecution BIT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    WITH latestOutcome AS (
        SELECT *
        FROM ETL.vw_EtlLatestTableOutcome
        WHERE LineageKey = @LineageKey
          AND StageName = 'LOAD'
    )
    SELECT
        ctl.Id,
        ctl.DataSourceId,
        ctl.SourceObjectSchema,
        ctl.SourceObjectName,
        ctl.SourceObjectSettings,
        ctl.ExtractSourceSettings,
        ctl.SinkObjectSchema,
        ctl.SinkObjectName,
        ctl.SinkObjectSettings,
        ctl.ExtractSinkSettings,
        ctl.ExtractCopyActivitySettings,
        ctl.DataExtractBehaviorSettings,
        ctl.ExtractOrder,
        ctl.ExtractEnabled,
        ctl.LoadOrder,
        ctl.LoadEnabled,
        COALESCE(policy.PolicyName, 'default') AS PolicyName,
        COALESCE(policy.MaxRetryCount, 2) AS PolicyMaxRetryCount,
        COALESCE(policy.CooldownSeconds, 0) AS PolicyCooldownSeconds,
        COALESCE(policy.ForceSerialExecution, 0) AS PolicyForceSerialExecution,
        COALESCE(policy.IsQuarantined, 0) AS PolicyIsQuarantined,
        policy.QuarantineReason,
        latestOutcome.OutcomeCode AS LatestOutcomeCode,
        latestOutcome.IsTransient AS LatestIsTransient,
        latestOutcome.RetryAttempt AS LatestRetryAttempt,
        latestOutcome.CreatedDateUtc AS LatestOutcomeCreatedUtc
    FROM ETL.ExtractLoadControlTable ctl
    LEFT JOIN latestOutcome
        ON latestOutcome.ExtractLoadControlTableId = ctl.Id
    LEFT JOIN ETL.TableExecutionPolicy policy
        ON policy.ExtractLoadControlTableId = ctl.Id
       AND policy.IsEnabled = 1
    WHERE ctl.LoadOrder = @LoadOrder
      AND ctl.LoadEnabled = 1
      AND (@ForceSerialExecution IS NULL OR COALESCE(policy.ForceSerialExecution, 0) = @ForceSerialExecution)
      AND COALESCE(policy.IsQuarantined, 0) = 0
      AND (
            latestOutcome.ExtractLoadControlTableId IS NULL
            OR latestOutcome.OutcomeCode = 'FailedTransient'
            OR (
                latestOutcome.OutcomeCode IS NULL
                AND ISNULL(latestOutcome.EtlStatus, '') = 'FAILED'
            )
          )
      AND (
            latestOutcome.ExtractLoadControlTableId IS NULL
            OR COALESCE(latestOutcome.RetryAttempt, 0) < COALESCE(policy.MaxRetryCount, 2)
          )
      AND (
            latestOutcome.ExtractLoadControlTableId IS NULL
            OR DATEADD(SECOND, COALESCE(policy.CooldownSeconds, 0), latestOutcome.CreatedDateUtc) <= SYSUTCDATETIME()
          )
    ORDER BY
        ctl.LoadOrder,
        ctl.Id;
END;
GO

CREATE OR ALTER PROC ETL.p_GetEtlStageSummary
    @LineageKey INT,
    @StageName VARCHAR(20),
    @LoadOrder INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        @LineageKey AS LineageKey,
        @StageName AS StageName,
        COALESCE(COUNT(1), 0) AS TotalTableCount,
        COALESCE(SUM(CASE WHEN lo.OutcomeCode = 'Succeeded' THEN 1 ELSE 0 END), 0) AS SucceededCount,
        COALESCE(SUM(CASE WHEN lo.OutcomeCode = 'FailedTransient' THEN 1 ELSE 0 END), 0) AS FailedTransientCount,
        COALESCE(SUM(CASE WHEN lo.OutcomeCode = 'FailedDeterministic' THEN 1 ELSE 0 END), 0) AS FailedDeterministicCount,
        COALESCE(SUM(CASE WHEN lo.OutcomeCode = 'SkippedByPolicy' THEN 1 ELSE 0 END), 0) AS SkippedByPolicyCount,
        COALESCE(SUM(CASE WHEN lo.OutcomeCode IN ('FailedTransient', 'FailedDeterministic') THEN 1 ELSE 0 END), 0) AS FailureCount,
        MAX(lo.CreatedDateUtc) AS LatestActivityUtc
    FROM ETL.vw_EtlLatestTableOutcome lo
    LEFT JOIN ETL.ExtractLoadControlTable ctl
        ON ctl.Id = lo.ExtractLoadControlTableId
    WHERE lo.LineageKey = @LineageKey
      AND lo.StageName = @StageName
      AND (@LoadOrder IS NULL OR ctl.LoadOrder = @LoadOrder);
END;
GO

CREATE OR ALTER PROC ETL.p_CaptureFailureDiagnosticSnapshot
    @LineageKey INT,
    @StageName VARCHAR(20),
    @ExtractLoadControlTableId INT = NULL,
    @DataSourceId INT = NULL,
    @TableName VARCHAR(100) = NULL,
    @PipelineRunId VARCHAR(36) = NULL,
    @ActivityRunId VARCHAR(36) = NULL,
    @SqlErrorNumber INT = NULL,
    @FailureCategory VARCHAR(50) = NULL,
    @Notes NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @serviceObjective VARCHAR(128);
    DECLARE @elasticPoolName SYSNAME;

    SELECT
        @serviceObjective = CAST(dso.edition + ':' + dso.service_objective AS VARCHAR(128)),
        @elasticPoolName = dso.elastic_pool_name
    FROM sys.database_service_objectives dso
    WHERE dso.database_id = DB_ID();

    INSERT INTO ETL.EtlFailureDiagnosticSnapshot (
        LineageKey,
        StageName,
        ExtractLoadControlTableId,
        DataSourceId,
        TableName,
        PipelineRunId,
        ActivityRunId,
        SqlErrorNumber,
        FailureCategory,
        DatabaseName,
        ServiceObjective,
        ElasticPoolName,
        SnapshotEndTimeUtc,
        AvgCpuPercent,
        AvgDataIoPercent,
        AvgLogWritePercent,
        Notes
    )
    SELECT TOP 1
        @LineageKey,
        @StageName,
        @ExtractLoadControlTableId,
        @DataSourceId,
        @TableName,
        @PipelineRunId,
        @ActivityRunId,
        @SqlErrorNumber,
        @FailureCategory,
        DB_NAME(),
        @serviceObjective,
        @elasticPoolName,
        rs.end_time,
        TRY_CONVERT(DECIMAL(9,2), rs.avg_cpu_percent),
        TRY_CONVERT(DECIMAL(9,2), rs.avg_data_io_percent),
        TRY_CONVERT(DECIMAL(9,2), rs.avg_log_write_percent),
        @Notes
    FROM sys.dm_db_resource_stats rs
    ORDER BY rs.end_time DESC;
END;
GO

MERGE ETL.TableExecutionPolicy AS target
USING (
    SELECT
        ctl.Id AS ExtractLoadControlTableId,
        'hot-fact-serial' AS PolicyName,
        2 AS MaxRetryCount,
        300 AS CooldownSeconds,
        CAST(1 AS BIT) AS ForceSerialExecution,
        CAST(0 AS BIT) AS IsQuarantined,
        CAST(NULL AS NVARCHAR(1000)) AS QuarantineReason,
        CAST(1 AS BIT) AS IsEnabled,
        'migration_add_ETL_ObservabilityAndFailurePolicy' AS ModifiedBy
    FROM ETL.ExtractLoadControlTable ctl
    WHERE ctl.SinkObjectSchema = 'DWH'
      AND ctl.SinkObjectName = 'fInventoryTransaction_D365'
) AS source
ON target.ExtractLoadControlTableId = source.ExtractLoadControlTableId
WHEN MATCHED THEN
    UPDATE SET
        PolicyName = source.PolicyName,
        MaxRetryCount = source.MaxRetryCount,
        CooldownSeconds = source.CooldownSeconds,
        ForceSerialExecution = source.ForceSerialExecution,
        IsQuarantined = source.IsQuarantined,
        QuarantineReason = source.QuarantineReason,
        IsEnabled = source.IsEnabled,
        ModifiedBy = source.ModifiedBy,
        ModifiedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        ExtractLoadControlTableId,
        PolicyName,
        MaxRetryCount,
        CooldownSeconds,
        ForceSerialExecution,
        IsQuarantined,
        QuarantineReason,
        IsEnabled,
        ModifiedBy
    )
    VALUES (
        source.ExtractLoadControlTableId,
        source.PolicyName,
        source.MaxRetryCount,
        source.CooldownSeconds,
        source.ForceSerialExecution,
        source.IsQuarantined,
        source.QuarantineReason,
        source.IsEnabled,
        source.ModifiedBy
    );
GO
