-- Make SSIS job claiming atomic for ADF queue orchestration.
-- Apply batch-by-batch with dynamicInvoke, splitting on GO.

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.SSISJobInfo')
      AND name = 'IX_SSISJobInfo_JobStatus_JobID'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_SSISJobInfo_JobStatus_JobID
    ON dbo.SSISJobInfo (JobStatus, JobID)
    INCLUDE (JobTables, DataSource, FromDatetime, ToDatetime, JobScope, LineageKey, MasterPipelineRunId);

    PRINT 'Index IX_SSISJobInfo_JobStatus_JobID created';
END
ELSE
BEGIN
    PRINT 'Index IX_SSISJobInfo_JobStatus_JobID already exists';
END
GO

CREATE OR ALTER PROC ETL.p_ClaimNextSsisJobForAdf
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    -- READPAST is not valid under SERIALIZABLE, and this database has
    -- READ_COMMITTED_SNAPSHOT enabled. REPEATABLE READ keeps the claim
    -- transactional while allowing the locked-row skip behavior.
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    BEGIN TRAN;

    IF EXISTS (
        SELECT 1
        FROM dbo.SSISJobInfo WITH (UPDLOCK, HOLDLOCK)
        WHERE JobStatus = 'RUNNING'
    )
    BEGIN
        COMMIT TRAN;

        SELECT TOP (0)
            JobID,
            JobTables,
            DataSource,
            FromDatetime,
            ToDatetime,
            JobScope,
            JobStatus,
            LineageKey,
            MasterPipelineRunId
        FROM dbo.SSISJobInfo;

        RETURN;
    END

    ;WITH nextJob AS (
        SELECT TOP (1)
            JobID,
            JobTables,
            DataSource,
            FromDatetime,
            ToDatetime,
            JobScope,
            JobStatus,
            LineageKey,
            MasterPipelineRunId
        -- Keep the RUNNING gate protected by HOLDLOCK above, but avoid
        -- combining HOLDLOCK with READPAST on the claim statement itself.
        FROM dbo.SSISJobInfo WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE JobStatus = 'PENDING'
        ORDER BY JobID
    )
    UPDATE nextJob
    SET JobStatus = 'RUNNING'
    OUTPUT
        inserted.JobID,
        inserted.JobTables,
        inserted.DataSource,
        inserted.FromDatetime,
        inserted.ToDatetime,
        inserted.JobScope,
        inserted.JobStatus,
        inserted.LineageKey,
        inserted.MasterPipelineRunId;

    COMMIT TRAN;
END;
GO
