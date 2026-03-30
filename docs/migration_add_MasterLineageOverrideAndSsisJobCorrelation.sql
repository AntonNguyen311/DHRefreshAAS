-- Add SSIS job correlation and formal master lineage override support.
-- Apply batch-by-batch with dynamicInvoke, splitting on GO.

IF COL_LENGTH('dbo.SSISJobInfo', 'LineageKey') IS NULL
BEGIN
    ALTER TABLE dbo.SSISJobInfo
    ADD LineageKey INT NULL;

    PRINT 'Column dbo.SSISJobInfo.LineageKey added';
END
ELSE
BEGIN
    PRINT 'Column dbo.SSISJobInfo.LineageKey already exists';
END
GO

IF COL_LENGTH('dbo.SSISJobInfo', 'MasterPipelineRunId') IS NULL
BEGIN
    ALTER TABLE dbo.SSISJobInfo
    ADD MasterPipelineRunId VARCHAR(36) NULL;

    PRINT 'Column dbo.SSISJobInfo.MasterPipelineRunId added';
END
ELSE
BEGIN
    PRINT 'Column dbo.SSISJobInfo.MasterPipelineRunId already exists';
END
GO

IF COL_LENGTH('dbo.SSISJobInfo', 'LastLinkedDateUtc') IS NULL
BEGIN
    ALTER TABLE dbo.SSISJobInfo
    ADD LastLinkedDateUtc DATETIME2(0) NULL;

    PRINT 'Column dbo.SSISJobInfo.LastLinkedDateUtc added';
END
ELSE
BEGIN
    PRINT 'Column dbo.SSISJobInfo.LastLinkedDateUtc already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.SSISJobInfo')
      AND name = 'IX_SSISJobInfo_LineageKey_InsertedDatetime'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_SSISJobInfo_LineageKey_InsertedDatetime
    ON dbo.SSISJobInfo (LineageKey, InsertedDatetime)
    INCLUDE (JobStatus, JobTables, DataSource, FromDatetime, ToDatetime, MasterPipelineRunId);

    PRINT 'Index IX_SSISJobInfo_LineageKey_InsertedDatetime created';
END
ELSE
BEGIN
    PRINT 'Index IX_SSISJobInfo_LineageKey_InsertedDatetime already exists';
END
GO

IF OBJECT_ID('ETL.MasterLineageOverride', 'U') IS NULL
BEGIN
    CREATE TABLE ETL.MasterLineageOverride (
        MasterLineageOverrideId INT IDENTITY(1,1) NOT NULL,
        TargetLineageKey INT NOT NULL,
        ExpectedCurrentStatus VARCHAR(50) NOT NULL,
        OverrideAction VARCHAR(50) NOT NULL CONSTRAINT DF_MasterLineageOverride_OverrideAction DEFAULT ('ALLOW_NEXT_MASTER_RUN'),
        RelatedJobID INT NULL,
        RelatedJobTables VARCHAR(255) NULL,
        RelatedDataSource VARCHAR(255) NULL,
        RelatedFromDatetime DATETIME NULL,
        RelatedToDatetime DATETIME NULL,
        ExpectedJobStatus VARCHAR(50) NULL,
        Reason NVARCHAR(1000) NOT NULL,
        RootCause NVARCHAR(2000) NULL,
        ValidatedBy NVARCHAR(256) NULL,
        ApprovedBy NVARCHAR(256) NULL,
        ExpiresAtUtc DATETIME2(0) NOT NULL,
        IsConsumed BIT NOT NULL CONSTRAINT DF_MasterLineageOverride_IsConsumed DEFAULT (0),
        ConsumedByPipelineRunId VARCHAR(36) NULL,
        ConsumedDateUtc DATETIME2(0) NULL,
        IsRevoked BIT NOT NULL CONSTRAINT DF_MasterLineageOverride_IsRevoked DEFAULT (0),
        RevokedBy NVARCHAR(256) NULL,
        RevokedDateUtc DATETIME2(0) NULL,
        CreatedDateUtc DATETIME2(0) NOT NULL CONSTRAINT DF_MasterLineageOverride_CreatedDateUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_MasterLineageOverride PRIMARY KEY CLUSTERED (MasterLineageOverrideId)
    );

    PRINT 'Table ETL.MasterLineageOverride created';
END
ELSE
BEGIN
    PRINT 'Table ETL.MasterLineageOverride already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('ETL.MasterLineageOverride')
      AND name = 'IX_MasterLineageOverride_TargetLineageKey_Active'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_MasterLineageOverride_TargetLineageKey_Active
    ON ETL.MasterLineageOverride (TargetLineageKey, ExpiresAtUtc, CreatedDateUtc)
    INCLUDE (ExpectedCurrentStatus, OverrideAction, RelatedJobID, ExpectedJobStatus, IsConsumed, IsRevoked)
    WHERE IsConsumed = 0 AND IsRevoked = 0;

    PRINT 'Index IX_MasterLineageOverride_TargetLineageKey_Active created';
END
ELSE
BEGIN
    PRINT 'Index IX_MasterLineageOverride_TargetLineageKey_Active already exists';
END
GO

CREATE OR ALTER PROC ETL.p_LinkMasterLineageToSSISJobInfo
    @LineageKey INT,
    @MasterPipelineRunId VARCHAR(36) = NULL,
    @JobID INT = NULL,
    @JobTables VARCHAR(255) = NULL,
    @DataSource VARCHAR(255) = NULL,
    @FromDatetime DATETIME = NULL,
    @ToDatetime DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME2(0) = SYSUTCDATETIME();

    IF NOT EXISTS (
        SELECT 1
        FROM ETL.MasterEtlLineage
        WHERE LineageKey = @LineageKey
    )
    BEGIN
        RAISERROR ('Target lineage does not exist.', 16, 1);
        RETURN;
    END

    IF @JobID IS NULL AND @JobTables IS NULL AND @DataSource IS NULL
    BEGIN
        RAISERROR ('Provide @JobID or at least one SSIS job filter to link job rows.', 16, 1);
        RETURN;
    END

    UPDATE job
    SET job.LineageKey = @LineageKey,
        job.MasterPipelineRunId = COALESCE(@MasterPipelineRunId, job.MasterPipelineRunId),
        job.LastLinkedDateUtc = @currentDate
    FROM dbo.SSISJobInfo job
    WHERE (@JobID IS NULL OR job.JobID = @JobID)
      AND (@JobTables IS NULL OR job.JobTables = @JobTables)
      AND (@DataSource IS NULL OR job.DataSource = @DataSource)
      AND (@FromDatetime IS NULL OR job.FromDatetime = @FromDatetime)
      AND (@ToDatetime IS NULL OR job.ToDatetime = @ToDatetime);

    SELECT @@ROWCOUNT AS LinkedRowCount;
END;
GO

CREATE OR ALTER PROC ETL.p_GrantMasterLineageOverride
    @TargetLineageKey INT,
    @ExpectedCurrentStatus VARCHAR(50),
    @Reason NVARCHAR(1000),
    @RootCause NVARCHAR(2000) = NULL,
    @ValidatedBy NVARCHAR(256) = NULL,
    @ApprovedBy NVARCHAR(256) = NULL,
    @ExpiresAtUtc DATETIME2(0),
    @OverrideAction VARCHAR(50) = 'ALLOW_NEXT_MASTER_RUN',
    @RelatedJobID INT = NULL,
    @RelatedJobTables VARCHAR(255) = NULL,
    @RelatedDataSource VARCHAR(255) = NULL,
    @RelatedFromDatetime DATETIME = NULL,
    @RelatedToDatetime DATETIME = NULL,
    @ExpectedJobStatus VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME2(0) = SYSUTCDATETIME();
    DECLARE @currentStatus VARCHAR(50);
    DECLARE @matchedJobID INT = NULL;
    DECLARE @matchedJobTables VARCHAR(255) = NULL;
    DECLARE @matchedDataSource VARCHAR(255) = NULL;
    DECLARE @matchedFromDatetime DATETIME = NULL;
    DECLARE @matchedToDatetime DATETIME = NULL;
    DECLARE @matchedJobStatus VARCHAR(50) = NULL;

    SELECT @currentStatus = CurrentStatus
    FROM ETL.MasterEtlLineage
    WHERE LineageKey = @TargetLineageKey;

    IF @currentStatus IS NULL
    BEGIN
        RAISERROR ('Target lineage does not exist.', 16, 1);
        RETURN;
    END

    IF @currentStatus <> @ExpectedCurrentStatus
    BEGIN
        RAISERROR ('Target lineage current status does not match the expected status.', 16, 1);
        RETURN;
    END

    IF @ExpiresAtUtc <= @currentDate
    BEGIN
        RAISERROR ('Override expiry must be in the future.', 16, 1);
        RETURN;
    END

    IF EXISTS (
        SELECT 1
        FROM ETL.MasterLineageOverride o
        WHERE o.TargetLineageKey = @TargetLineageKey
          AND o.IsConsumed = 0
          AND o.IsRevoked = 0
          AND o.ExpiresAtUtc >= @currentDate
    )
    BEGIN
        RAISERROR ('An active override already exists for the target lineage.', 16, 1);
        RETURN;
    END

    IF @RelatedJobID IS NOT NULL
    BEGIN
        SELECT
            @matchedJobID = job.JobID,
            @matchedJobTables = job.JobTables,
            @matchedDataSource = job.DataSource,
            @matchedFromDatetime = job.FromDatetime,
            @matchedToDatetime = job.ToDatetime,
            @matchedJobStatus = job.JobStatus
        FROM dbo.SSISJobInfo job
        WHERE job.JobID = @RelatedJobID;
    END
    ELSE IF @RelatedJobTables IS NOT NULL OR @RelatedDataSource IS NOT NULL
    BEGIN
        SELECT TOP 1
            @matchedJobID = job.JobID,
            @matchedJobTables = job.JobTables,
            @matchedDataSource = job.DataSource,
            @matchedFromDatetime = job.FromDatetime,
            @matchedToDatetime = job.ToDatetime,
            @matchedJobStatus = job.JobStatus
        FROM dbo.SSISJobInfo job
        WHERE (@RelatedJobTables IS NULL OR job.JobTables = @RelatedJobTables)
          AND (@RelatedDataSource IS NULL OR job.DataSource = @RelatedDataSource)
          AND (@RelatedFromDatetime IS NULL OR job.FromDatetime = @RelatedFromDatetime)
          AND (@RelatedToDatetime IS NULL OR job.ToDatetime = @RelatedToDatetime)
        ORDER BY job.InsertedDatetime DESC, job.JobID DESC;
    END

    IF @ExpectedJobStatus IS NOT NULL
    BEGIN
        IF @matchedJobID IS NULL
        BEGIN
            RAISERROR ('Expected a matching SSIS job row for override validation, but none was found.', 16, 1);
            RETURN;
        END

        IF ISNULL(@matchedJobStatus, '') <> @ExpectedJobStatus
        BEGIN
            RAISERROR ('The matching SSIS job status does not match the expected job status.', 16, 1);
            RETURN;
        END
    END

    INSERT INTO ETL.MasterLineageOverride (
        TargetLineageKey,
        ExpectedCurrentStatus,
        OverrideAction,
        RelatedJobID,
        RelatedJobTables,
        RelatedDataSource,
        RelatedFromDatetime,
        RelatedToDatetime,
        ExpectedJobStatus,
        Reason,
        RootCause,
        ValidatedBy,
        ApprovedBy,
        ExpiresAtUtc
    )
    VALUES (
        @TargetLineageKey,
        @ExpectedCurrentStatus,
        @OverrideAction,
        @matchedJobID,
        COALESCE(@matchedJobTables, @RelatedJobTables),
        COALESCE(@matchedDataSource, @RelatedDataSource),
        COALESCE(@matchedFromDatetime, @RelatedFromDatetime),
        COALESCE(@matchedToDatetime, @RelatedToDatetime),
        @ExpectedJobStatus,
        @Reason,
        @RootCause,
        @ValidatedBy,
        @ApprovedBy,
        @ExpiresAtUtc
    );

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
        @TargetLineageKey,
        NULL,
        'ETL.p_GrantMasterLineageOverride',
        'MASTER OVERRIDE GRANTED',
        @currentDate,
        @currentDate,
        'MASTER_OVERRIDE_GRANTED',
        @Reason,
        @currentDate
    );

    SELECT SCOPE_IDENTITY() AS MasterLineageOverrideId;
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
-- 2026-03-30   Cursor              Allow a scoped one-time override to reopen the next lineage
-- ====================================================================================
*/
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @currentDate DATETIME = SYSUTCDATETIME();
    DECLARE @currentStatus VARCHAR(50);
    DECLARE @latestLineageKey INT;
    DECLARE @ErrorMessage VARCHAR(500);
    DECLARE @overrideId INT = NULL;
    DECLARE @overrideRelatedJobID INT = NULL;
    DECLARE @overrideExpectedJobStatus VARCHAR(50) = NULL;
    DECLARE @jobStatus VARCHAR(50) = NULL;
    DECLARE @newLineageKey INT;

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
                @overrideRelatedJobID = o.RelatedJobID,
                @overrideExpectedJobStatus = o.ExpectedJobStatus
            FROM ETL.MasterLineageOverride o
            WHERE o.TargetLineageKey = @latestLineageKey
              AND o.ExpectedCurrentStatus = @currentStatus
              AND o.OverrideAction = 'ALLOW_NEXT_MASTER_RUN'
              AND o.IsConsumed = 0
              AND o.IsRevoked = 0
              AND o.ExpiresAtUtc >= @currentDate
            ORDER BY o.CreatedDateUtc DESC, o.MasterLineageOverrideId DESC;

            IF @overrideId IS NULL
            BEGIN
                SET @ErrorMessage = 'Cannot get LineageKey to run as the last run was NOT completed, status of last run = ' + @currentStatus;
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
                'MASTER OVERRIDE CONSUMED',
                @currentDate,
                @currentDate,
                'MASTER_OVERRIDE_CONSUMED',
                'Scoped master lineage override consumed to reopen processing after a validated failure.',
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
