-- Extend ETL.p_GetMasterAutoRecoveryDecision: allow AutoRerunEligible when EXTRACT stage failed at
-- orchestration level (MasterEtlLog EXTRACT FAILED) but vw_EtlStageSummary shows zero table failures.
-- Apply in datalakeprod after DBA review (same batch style as other migrations).

SET NOCOUNT ON;
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
    DECLARE @orchestrationStageFailure BIT = 0;

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

    IF EXISTS (
        SELECT 1
        FROM ETL.MasterEtlLog log
        WHERE log.LineageKey = @TargetLineageKey
          AND log.EtlStatus = N'EXTRACT FAILED'
    )
        SET @orchestrationStageFailure = 1;

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
            WHEN @currentStatus = 'MASTER FAILED'
                 AND @failureCount = 0
                 AND ISNULL(@orchestrationStageFailure, 0) = 1
                 AND ISNULL(@requireRelatedJob, 0) = 1
                 AND @relatedJobID IS NOT NULL
                 AND (@requiredJobStatus IS NULL OR ISNULL(@relatedJobStatus, '') = @requiredJobStatus)
                 AND @autoRecoveryGrantCount < @maxAutoRetryCount
                 AND NOT (
                     @latestFailureUtc IS NOT NULL
                     AND DATEADD(SECOND, @cooldownSeconds, @latestFailureUtc) > @currentDate
                 )
            THEN 'AutoRerunEligible'
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
                CASE
                    WHEN @failureCount = 0 AND ISNULL(@orchestrationStageFailure, 0) = 1 THEN
                        N'Orchestration-level EXTRACT FAILED in MasterEtlLog with no table-level failures in vw_EtlStageSummary; auto retry allowed per policy.'
                    ELSE N'Only transient ETL failures were detected and the related SSIS job is in a safe failed state.'
                END
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
