-- One-time manual grant: allow next master run for a stuck MASTER FAILED lineage.
-- Adjust @TargetLineageKey, @RelatedJobID, and text fields; run in SSMS against datalakeprod after approval.

DECLARE @ExpiresAtUtc DATETIME2(0) = DATEADD(HOUR, 2, SYSUTCDATETIME());

EXEC ETL.p_GrantMasterLineageOverride
    @TargetLineageKey = 29519,
    @ExpectedCurrentStatus = N'MASTER FAILED',
    @Reason = N'Manual unblock: EXTRACT orchestration failed with 0 table failures (see MasterEtlLog / ADF Fail Extract Stage).',
    @RootCause = N'001 Extract child pipeline failed; RouteJobsBasedOnExtractBehavior / Fail Extract Stage.',
    @ValidatedBy = N'ops',
    @ApprovedBy = N'ops',
    @ExpiresAtUtc = @ExpiresAtUtc,
    @OverrideAction = N'ALLOW_NEXT_MASTER_RUN',
    @RelatedJobID = 18260,
    @ExpectedJobStatus = N'FAILED';
