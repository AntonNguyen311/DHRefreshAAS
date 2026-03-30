-- Add automatic transient-failure master recovery policy and decision logic.
-- Apply batch-by-batch with dynamicInvoke, splitting on GO.

IF OBJECT_ID('ETL.MasterAutoRecoveryPolicy', 'U') IS NULL
BEGIN
    CREATE TABLE ETL.MasterAutoRecoveryPolicy (
        MasterAutoRecoveryPolicyId INT IDENTITY(1,1) NOT NULL,
        PolicyName NVARCHAR(100) NOT NULL,
        MaxAutoRetryCount INT NOT NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_MaxAutoRetryCount DEFAULT (1),
        CooldownSeconds INT NOT NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_CooldownSeconds DEFAULT (0),
        RequireRelatedJob BIT NOT NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_RequireRelatedJob DEFAULT (1),
        RequiredJobStatus VARCHAR(50) NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_RequiredJobStatus DEFAULT ('FAILED'),
        AllowWhenOnlyTransientFailures BIT NOT NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_AllowWhenOnlyTransientFailures DEFAULT (1),
        IsEnabled BIT NOT NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_IsEnabled DEFAULT (1),
        ModifiedBy NVARCHAR(256) NULL,
        ModifiedAtUtc DATETIME2(0) NOT NULL CONSTRAINT DF_MasterAutoRecoveryPolicy_ModifiedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_MasterAutoRecoveryPolicy PRIMARY KEY CLUSTERED (MasterAutoRecoveryPolicyId)
    );

    PRINT 'Table ETL.MasterAutoRecoveryPolicy created';
END
ELSE
BEGIN
    PRINT 'Table ETL.MasterAutoRecoveryPolicy already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('ETL.MasterAutoRecoveryPolicy')
      AND name = 'UX_MasterAutoRecoveryPolicy_PolicyName'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_MasterAutoRecoveryPolicy_PolicyName
    ON ETL.MasterAutoRecoveryPolicy (PolicyName)
    WHERE IsEnabled = 1;

    PRINT 'Index UX_MasterAutoRecoveryPolicy_PolicyName created';
END
ELSE
BEGIN
    PRINT 'Index UX_MasterAutoRecoveryPolicy_PolicyName already exists';
END
GO

MERGE ETL.MasterAutoRecoveryPolicy AS target
USING (
    SELECT
        N'default-transient-master-rerun' AS PolicyName,
        1 AS MaxAutoRetryCount,
        0 AS CooldownSeconds,
        CAST(1 AS BIT) AS RequireRelatedJob,
        'FAILED' AS RequiredJobStatus,
        CAST(1 AS BIT) AS AllowWhenOnlyTransientFailures,
        CAST(1 AS BIT) AS IsEnabled,
        N'migration_add_MasterAutoRecoveryPolicy' AS ModifiedBy
) AS source
ON target.PolicyName = source.PolicyName
WHEN MATCHED THEN
    UPDATE SET
        MaxAutoRetryCount = source.MaxAutoRetryCount,
        CooldownSeconds = source.CooldownSeconds,
        RequireRelatedJob = source.RequireRelatedJob,
        RequiredJobStatus = source.RequiredJobStatus,
        AllowWhenOnlyTransientFailures = source.AllowWhenOnlyTransientFailures,
        IsEnabled = source.IsEnabled,
        ModifiedBy = source.ModifiedBy,
        ModifiedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        PolicyName,
        MaxAutoRetryCount,
        CooldownSeconds,
        RequireRelatedJob,
        RequiredJobStatus,
        AllowWhenOnlyTransientFailures,
        IsEnabled,
        ModifiedBy
    )
    VALUES (
        source.PolicyName,
        source.MaxAutoRetryCount,
        source.CooldownSeconds,
        source.RequireRelatedJob,
        source.RequiredJobStatus,
        source.AllowWhenOnlyTransientFailures,
        source.IsEnabled,
        source.ModifiedBy
    );
GO

CREATE OR ALTER PROC ETL.p_GetMasterAutoRecoveryDecision
    @TargetLineageKey INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME2(0) = SYSUTCDATETIME();
    DECLARE @currentStatus VARCHAR(50);
    DECLARE @masterPipelineRunId VARCHAR(36);
    DECLARE @policyName NVARCHAR(100) = NULL;
    DECLARE @maxAutoRetryCount INT = 0;
    DECLARE @cooldownSeconds INT = 0;
    DECLARE @requireRelatedJob BIT = 0;
    DECLARE @requiredJobStatus VARCHAR(50) = NULL;
    DECLARE @allowWhenOnlyTransientFailures BIT = 0;
    DECLARE @relatedJobID INT = NULL;
    DECLARE @relatedJobStatus VARCHAR(50) = NULL;
    DECLARE @failedTransientCount INT = 0;
    DECLARE @failedDeterministicCount INT = 0;
    DECLARE @failureCount INT = 0;
    DECLARE @latestFailureUtc DATETIME = NULL;
    DECLARE @autoRecoveryGrantCount INT = 0;
    DECLARE @activeOverrideCount INT = 0;
    DECLARE @decisionCode VARCHAR(50);
    DECLARE @decisionReason NVARCHAR(1000);

    SELECT
        @currentStatus = ml.CurrentStatus,
        @masterPipelineRunId = ml.PipelineRunId
    FROM ETL.MasterEtlLineage ml
    WHERE ml.LineageKey = @TargetLineageKey;

    IF @currentStatus IS NULL
    BEGIN
        RAISERROR ('Target lineage does not exist.', 16, 1);
        RETURN;
    END

    UPDATE job
    SET job.LineageKey = @TargetLineageKey,
        job.LastLinkedDateUtc = @currentDate
    FROM dbo.SSISJobInfo job
    WHERE job.MasterPipelineRunId = @masterPipelineRunId
      AND ISNULL(job.LineageKey, -1) <> @TargetLineageKey;

    SELECT TOP 1
        @policyName = policy.PolicyName,
        @maxAutoRetryCount = policy.MaxAutoRetryCount,
        @cooldownSeconds = policy.CooldownSeconds,
        @requireRelatedJob = policy.RequireRelatedJob,
        @requiredJobStatus = policy.RequiredJobStatus,
        @allowWhenOnlyTransientFailures = policy.AllowWhenOnlyTransientFailures
    FROM ETL.MasterAutoRecoveryPolicy policy
    WHERE policy.IsEnabled = 1
    ORDER BY policy.ModifiedAtUtc DESC, policy.MasterAutoRecoveryPolicyId DESC;

    SELECT TOP 1
        @relatedJobID = job.JobID,
        @relatedJobStatus = job.JobStatus
    FROM dbo.SSISJobInfo job
    WHERE job.LineageKey = @TargetLineageKey
       OR (@masterPipelineRunId IS NOT NULL AND job.MasterPipelineRunId = @masterPipelineRunId)
    ORDER BY
        CASE WHEN job.LineageKey = @TargetLineageKey THEN 0 ELSE 1 END,
        ISNULL(job.LastLinkedDateUtc, '19000101') DESC,
        job.InsertedDatetime DESC,
        job.JobID DESC;

    SELECT
        @failedTransientCount = COALESCE(SUM(summary.FailedTransientCount), 0),
        @failedDeterministicCount = COALESCE(SUM(summary.FailedDeterministicCount), 0),
        @failureCount = COALESCE(SUM(summary.FailedTransientCount + summary.FailedDeterministicCount), 0),
        @latestFailureUtc = MAX(summary.LatestActivityUtc)
    FROM ETL.vw_EtlStageSummary summary
    WHERE summary.LineageKey = @TargetLineageKey
      AND summary.StageName IN ('EXTRACT', 'LOAD');

    IF @latestFailureUtc IS NULL
    BEGIN
        SELECT @latestFailureUtc = MAX(log.CreatedDateUtc)
        FROM ETL.MasterEtlLog log
        WHERE log.LineageKey = @TargetLineageKey;
    END

    SELECT @autoRecoveryGrantCount = COUNT(1)
    FROM ETL.MasterLineageOverride overrideRow
    WHERE overrideRow.TargetLineageKey = @TargetLineageKey
      AND overrideRow.OverrideAction = 'AUTO_ALLOW_NEXT_MASTER_RUN';

    SELECT @activeOverrideCount = COUNT(1)
    FROM ETL.MasterLineageOverride overrideRow
    WHERE overrideRow.TargetLineageKey = @TargetLineageKey
      AND overrideRow.IsConsumed = 0
      AND overrideRow.IsRevoked = 0
      AND overrideRow.ExpiresAtUtc >= @currentDate;

    SET @decisionCode =
        CASE
            WHEN @currentStatus LIKE '%MASTER COMPLETED' THEN 'NoRecoveryNeeded'
            WHEN @policyName IS NULL THEN 'PolicyDisabled'
            WHEN @currentStatus <> 'MASTER FAILED' THEN 'ManualResolutionRequired'
            WHEN @activeOverrideCount > 0 THEN 'OverrideAlreadyExists'
            WHEN @failureCount = 0 THEN 'ManualResolutionRequired'
            WHEN @failedDeterministicCount > 0 THEN 'ManualResolutionRequired'
            WHEN @failedTransientCount = 0 AND @allowWhenOnlyTransientFailures = 1 THEN 'ManualResolutionRequired'
            WHEN @requireRelatedJob = 1 AND @relatedJobID IS NULL THEN 'ManualResolutionRequired'
            WHEN @requiredJobStatus IS NOT NULL AND ISNULL(@relatedJobStatus, '') <> @requiredJobStatus THEN 'ManualResolutionRequired'
            WHEN @autoRecoveryGrantCount >= @maxAutoRetryCount THEN 'ManualResolutionRequired'
            WHEN @latestFailureUtc IS NOT NULL
                 AND DATEADD(SECOND, @cooldownSeconds, @latestFailureUtc) > @currentDate THEN 'CooldownActive'
            ELSE 'AutoRerunEligible'
        END;

    SET @decisionReason =
        CASE @decisionCode
            WHEN 'NoRecoveryNeeded' THEN N'Latest lineage is already completed.'
            WHEN 'PolicyDisabled' THEN N'No enabled auto recovery policy is configured.'
            WHEN 'OverrideAlreadyExists' THEN N'An active override already exists for this lineage.'
            WHEN 'CooldownActive' THEN N'The auto recovery cooldown window is still active.'
            WHEN 'AutoRerunEligible' THEN
                N'Only transient ETL failures were detected and the related SSIS job is in a safe failed state.'
            WHEN 'ManualResolutionRequired' THEN
                CASE
                    WHEN @currentStatus <> 'MASTER FAILED' THEN N'Latest lineage is not in MASTER FAILED state.'
                    WHEN @failureCount = 0 THEN N'No ETL table failure evidence was found for the failed lineage.'
                    WHEN @failedDeterministicCount > 0 THEN N'Deterministic ETL failures were detected; require manual resolution.'
                    WHEN @failedTransientCount = 0 THEN N'No transient ETL failures were detected; require manual resolution.'
                    WHEN @requireRelatedJob = 1 AND @relatedJobID IS NULL THEN N'No related SSIS job row was linked to the failed lineage.'
                    WHEN @requiredJobStatus IS NOT NULL AND ISNULL(@relatedJobStatus, '') <> @requiredJobStatus THEN
                        N'Related SSIS job status does not match the required safe status for auto rerun.'
                    WHEN @autoRecoveryGrantCount >= @maxAutoRetryCount THEN N'The maximum automatic recovery attempts for this lineage were already used.'
                    ELSE N'Manual resolution is required by the recovery policy.'
                END
        END;

    SELECT
        @TargetLineageKey AS TargetLineageKey,
        @currentStatus AS CurrentStatus,
        @masterPipelineRunId AS MasterPipelineRunId,
        @policyName AS PolicyName,
        @maxAutoRetryCount AS MaxAutoRetryCount,
        @cooldownSeconds AS CooldownSeconds,
        @relatedJobID AS RelatedJobID,
        @relatedJobStatus AS RelatedJobStatus,
        @failedTransientCount AS FailedTransientCount,
        @failedDeterministicCount AS FailedDeterministicCount,
        @failureCount AS FailureCount,
        @autoRecoveryGrantCount AS AutoRecoveryGrantCount,
        @latestFailureUtc AS LatestFailureUtc,
        CASE WHEN @decisionCode = 'AutoRerunEligible' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS IsAutoRerunEligible,
        @decisionCode AS DecisionCode,
        @decisionReason AS DecisionReason,
        DATEADD(MINUTE, 30, @currentDate) AS OverrideExpiresAtUtc,
        @requiredJobStatus AS ExpectedJobStatus;
END;
GO

CREATE OR ALTER PROC ETL.MasterEtlGetLineageKey
    @PipelineRunId VARCHAR(36),
    @PipelineName VARCHAR(140),
    @PreviousCutoffTime DATETIME,
    @CurrentCutoffTime DATETIME
AS
BEGIN
/*
-- CHANGES HISTORY
-- ====================================================================================
-- Date         Author              Changes
-- 2021-10-28   Thao Nguyen         Initial create
-- 2026-03-30   Cursor              Allow scoped manual and automatic transient-failure recovery
-- ====================================================================================
*/
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME = SYSUTCDATETIME();
    DECLARE @currentStatus VARCHAR(50);
    DECLARE @latestLineageKey INT;
    DECLARE @ErrorMessage VARCHAR(500);
    DECLARE @overrideId INT = NULL;
    DECLARE @overrideAction VARCHAR(50) = NULL;
    DECLARE @overrideRelatedJobID INT = NULL;
    DECLARE @overrideExpectedJobStatus VARCHAR(50) = NULL;
    DECLARE @jobStatus VARCHAR(50) = NULL;
    DECLARE @newLineageKey INT;

    DECLARE @autoDecision TABLE (
        TargetLineageKey INT,
        CurrentStatus VARCHAR(50),
        MasterPipelineRunId VARCHAR(36),
        PolicyName NVARCHAR(100),
        MaxAutoRetryCount INT,
        CooldownSeconds INT,
        RelatedJobID INT,
        RelatedJobStatus VARCHAR(50),
        FailedTransientCount INT,
        FailedDeterministicCount INT,
        FailureCount INT,
        AutoRecoveryGrantCount INT,
        LatestFailureUtc DATETIME,
        IsAutoRerunEligible BIT,
        DecisionCode VARCHAR(50),
        DecisionReason NVARCHAR(1000),
        OverrideExpiresAtUtc DATETIME2(0),
        ExpectedJobStatus VARCHAR(50)
    );

    DECLARE @decisionIsAutoRerunEligible BIT = 0;
    DECLARE @decisionCode VARCHAR(50) = NULL;
    DECLARE @decisionReason NVARCHAR(1000) = NULL;
    DECLARE @decisionRelatedJobID INT = NULL;
    DECLARE @decisionOverrideExpiresAtUtc DATETIME2(0) = NULL;
    DECLARE @decisionExpectedJobStatus VARCHAR(50) = NULL;

    BEGIN TRY
        SELECT TOP 1
            @latestLineageKey = LineageKey,
            @currentStatus = CurrentStatus
        FROM ETL.MasterEtlLineage
        ORDER BY CreatedDateUtc DESC, LineageKey DESC;

        IF @currentStatus IS NOT NULL
           AND @currentStatus NOT LIKE '%MASTER COMPLETED'
        BEGIN
            SELECT TOP 1
                @overrideId = o.MasterLineageOverrideId,
                @overrideAction = o.OverrideAction,
                @overrideRelatedJobID = o.RelatedJobID,
                @overrideExpectedJobStatus = o.ExpectedJobStatus
            FROM ETL.MasterLineageOverride o
            WHERE o.TargetLineageKey = @latestLineageKey
              AND o.ExpectedCurrentStatus = @currentStatus
              AND o.OverrideAction IN ('ALLOW_NEXT_MASTER_RUN', 'AUTO_ALLOW_NEXT_MASTER_RUN')
              AND o.IsConsumed = 0
              AND o.IsRevoked = 0
              AND o.ExpiresAtUtc >= @currentDate
            ORDER BY o.CreatedDateUtc DESC, o.MasterLineageOverrideId DESC;

            IF @overrideId IS NULL
            BEGIN
                INSERT INTO @autoDecision
                EXEC ETL.p_GetMasterAutoRecoveryDecision
                    @TargetLineageKey = @latestLineageKey;

                SELECT TOP 1
                    @decisionIsAutoRerunEligible = IsAutoRerunEligible,
                    @decisionCode = DecisionCode,
                    @decisionReason = DecisionReason,
                    @decisionRelatedJobID = RelatedJobID,
                    @decisionOverrideExpiresAtUtc = OverrideExpiresAtUtc,
                    @decisionExpectedJobStatus = ExpectedJobStatus
                FROM @autoDecision;

                IF ISNULL(@decisionIsAutoRerunEligible, 0) = 1
                BEGIN
                    INSERT INTO ETL.MasterLineageOverride (
                        TargetLineageKey,
                        ExpectedCurrentStatus,
                        OverrideAction,
                        RelatedJobID,
                        ExpectedJobStatus,
                        Reason,
                        RootCause,
                        ValidatedBy,
                        ApprovedBy,
                        ExpiresAtUtc
                    )
                    VALUES (
                        @latestLineageKey,
                        @currentStatus,
                        'AUTO_ALLOW_NEXT_MASTER_RUN',
                        @decisionRelatedJobID,
                        @decisionExpectedJobStatus,
                        @decisionReason,
                        N'Automatic transient-only master recovery decision.',
                        N'SYSTEM_AUTO_RECOVERY',
                        N'SYSTEM_AUTO_RECOVERY',
                        COALESCE(@decisionOverrideExpiresAtUtc, DATEADD(MINUTE, 30, @currentDate))
                    );

                    SET @overrideId = SCOPE_IDENTITY();
                    SET @overrideAction = 'AUTO_ALLOW_NEXT_MASTER_RUN';
                    SET @overrideRelatedJobID = @decisionRelatedJobID;
                    SET @overrideExpectedJobStatus = @decisionExpectedJobStatus;

                    INSERT INTO ETL.MasterEtlLog (
                        LineageKey,
                        PipelineRunId,
                        PipelineName,
                        EtlStatus,
                        ExecutionStartUtc,
                        ExecutionEndUtc,
                        ErrorCode,
                        ErrorMessage,
                        CreatedDateUtc
                    )
                    VALUES (
                        @latestLineageKey,
                        NULL,
                        'ETL.MasterEtlGetLineageKey',
                        'MASTER AUTO OVERRIDE GRANTED',
                        @currentDate,
                        @currentDate,
                        'MASTER_AUTO_RERUN_GRANTED',
                        @decisionReason,
                        @currentDate
                    );
                END
            END

            IF @overrideId IS NULL
            BEGIN
                SET @ErrorMessage = 'Cannot get LineageKey to run as the last run was NOT completed, status of last run = ' + @currentStatus;

                IF @decisionCode IS NOT NULL AND @decisionReason IS NOT NULL
                BEGIN
                    SET @ErrorMessage = @ErrorMessage + '. RecoveryDecision=' + @decisionCode + '. ' + CONVERT(VARCHAR(400), @decisionReason);
                END

                RAISERROR (@ErrorMessage, 16, 1);
            END

            IF @overrideRelatedJobID IS NOT NULL
            BEGIN
                SELECT @jobStatus = job.JobStatus
                FROM dbo.SSISJobInfo job
                WHERE job.JobID = @overrideRelatedJobID;

                IF @overrideExpectedJobStatus IS NOT NULL
                   AND ISNULL(@jobStatus, '') <> @overrideExpectedJobStatus
                BEGIN
                    RAISERROR ('Cannot consume the master override because the related SSIS job status no longer matches the expected status.', 16, 1);
                END
            END
        END

        INSERT INTO ETL.MasterEtlLineage (
            PipelineRunId,
            PipelineName,
            PreviousCutoffTime,
            CurrentCutoffTime,
            CurrentStatus,
            CreatedDateUtc
        )
        VALUES (
            @PipelineRunId,
            @PipelineName,
            @PreviousCutoffTime,
            @CurrentCutoffTime,
            'MASTER STARTED',
            @currentDate
        );

        SELECT TOP 1 @newLineageKey = LineageKey
        FROM ETL.MasterEtlLineage
        WHERE PipelineRunId = @PipelineRunId
        ORDER BY LineageKey DESC;

        IF @overrideId IS NOT NULL
        BEGIN
            UPDATE ETL.MasterLineageOverride
            SET IsConsumed = 1,
                ConsumedByPipelineRunId = @PipelineRunId,
                ConsumedDateUtc = @currentDate
            WHERE MasterLineageOverrideId = @overrideId;

            INSERT INTO ETL.MasterEtlLog (
                LineageKey,
                PipelineRunId,
                PipelineName,
                EtlStatus,
                ExecutionStartUtc,
                ExecutionEndUtc,
                ErrorCode,
                ErrorMessage,
                CreatedDateUtc
            )
            VALUES (
                @newLineageKey,
                @PipelineRunId,
                @PipelineName,
                CASE
                    WHEN @overrideAction = 'AUTO_ALLOW_NEXT_MASTER_RUN' THEN 'MASTER AUTO OVERRIDE CONSUMED'
                    ELSE 'MASTER OVERRIDE CONSUMED'
                END,
                @currentDate,
                @currentDate,
                CASE
                    WHEN @overrideAction = 'AUTO_ALLOW_NEXT_MASTER_RUN' THEN 'MASTER_AUTO_OVERRIDE_CONSUMED'
                    ELSE 'MASTER_OVERRIDE_CONSUMED'
                END,
                CASE
                    WHEN @overrideAction = 'AUTO_ALLOW_NEXT_MASTER_RUN' THEN 'Automatic transient-failure recovery reopened the next master lineage.'
                    ELSE 'Scoped master lineage override consumed to reopen processing after a validated failure.'
                END,
                @currentDate
            );
        END

        SELECT @newLineageKey AS LineageKey;
    END TRY

    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO
