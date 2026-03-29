-- Add ignore / owner / guidance columns to etl.datawarehouseandcubemapping
-- so Logic Apps can skip risky tables before refresh and send actionable emails.

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'IsDisabled'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD IsDisabled BIT NOT NULL CONSTRAINT DF_datawarehouseandcubemapping_IsDisabled DEFAULT (0);

    PRINT 'Column IsDisabled added';
END
ELSE
BEGIN
    PRINT 'Column IsDisabled already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'IsIgnored'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD IsIgnored BIT NOT NULL CONSTRAINT DF_datawarehouseandcubemapping_IsIgnored DEFAULT (0);

    PRINT 'Column IsIgnored added';
END
ELSE
BEGIN
    PRINT 'Column IsIgnored already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'IgnoreReason'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD IgnoreReason NVARCHAR(500) NULL;

    PRINT 'Column IgnoreReason added';
END
ELSE
BEGIN
    PRINT 'Column IgnoreReason already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'FixGuide'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD FixGuide NVARCHAR(2000) NULL;

    PRINT 'Column FixGuide added';
END
ELSE
BEGIN
    PRINT 'Column FixGuide already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'OwnerEmails'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD OwnerEmails NVARCHAR(1000) NULL;

    PRINT 'Column OwnerEmails added';
END
ELSE
BEGIN
    PRINT 'Column OwnerEmails already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'GuardrailType'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD GuardrailType NVARCHAR(100) NULL;

    PRINT 'Column GuardrailType added';
END
ELSE
BEGIN
    PRINT 'Column GuardrailType already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'RequirePartition'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD RequirePartition BIT NOT NULL CONSTRAINT DF_datawarehouseandcubemapping_RequirePartition DEFAULT (0);

    PRINT 'Column RequirePartition added';
END
ELSE
BEGIN
    PRINT 'Column RequirePartition already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'MaxRowsPerRun'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD MaxRowsPerRun BIGINT NULL;

    PRINT 'Column MaxRowsPerRun added';
END
ELSE
BEGIN
    PRINT 'Column MaxRowsPerRun already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'MaxLookbackMonths'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD MaxLookbackMonths INT NULL;

    PRINT 'Column MaxLookbackMonths added';
END
ELSE
BEGIN
    PRINT 'Column MaxLookbackMonths already exists';
END
GO

-- Seed the first quarantined table based on repeated live failures.
UPDATE etl.datawarehouseandcubemapping
SET
    IsIgnored = 1,
    IgnoreReason = 'Repeated SQL datasource failures on datalakeprod (40197 / 823) during AAS SaveChanges.',
    FixGuide = 'Review the source view/query for large scans, reduce data volume, add or refine monthly partitions for the active periods, and limit lookback before re-enabling this table.',
    OwnerEmails = 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com',
    GuardrailType = 'ManualIgnore',
    RequirePartition = 1,
    MaxLookbackMonths = COALESCE(MaxLookbackMonths, 12)
WHERE CubeName = 'VN_CubeModel'
  AND CubeTableName = 'VNFIN vw_fSalesNAV_Petfood';
GO

-- Verify structure and the first quarantine seed.
SELECT
    CubeName,
    CubeTableName,
    Partition,
    RefreshType,
    IsDisabled,
    IsIgnored,
    IgnoreReason,
    FixGuide,
    OwnerEmails,
    GuardrailType,
    RequirePartition,
    MaxRowsPerRun,
    MaxLookbackMonths
FROM etl.datawarehouseandcubemapping
WHERE CubeTableName = 'VNFIN vw_fSalesNAV_Petfood'
   OR IsIgnored = 1
ORDER BY CubeName, CubeTableName, Partition;
GO
