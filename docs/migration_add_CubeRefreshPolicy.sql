-- Add SQL policy-driven refresh metadata so Logic Apps can resolve
-- ordering, guardrails, and recipients from SQL instead of hard-coded logic.

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'PolicyGroup'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD PolicyGroup NVARCHAR(100) NULL;

    PRINT 'Column PolicyGroup added';
END
ELSE
BEGIN
    PRINT 'Column PolicyGroup already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'RefreshPriority'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD RefreshPriority INT NULL;

    PRINT 'Column RefreshPriority added';
END
ELSE
BEGIN
    PRINT 'Column RefreshPriority already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'RefreshWave'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD RefreshWave INT NULL;

    PRINT 'Column RefreshWave added';
END
ELSE
BEGIN
    PRINT 'Column RefreshWave already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'TableOwnerRecipients'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD TableOwnerRecipients NVARCHAR(1000) NULL;

    PRINT 'Column TableOwnerRecipients added';
END
ELSE
BEGIN
    PRINT 'Column TableOwnerRecipients already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'IsPolicyEnabled'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD IsPolicyEnabled BIT NOT NULL CONSTRAINT DF_datawarehouseandcubemapping_IsPolicyEnabled DEFAULT (1);

    PRINT 'Column IsPolicyEnabled added';
END
ELSE
BEGIN
    PRINT 'Column IsPolicyEnabled already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'ModifiedBy'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD ModifiedBy NVARCHAR(256) NULL;

    PRINT 'Column ModifiedBy added';
END
ELSE
BEGIN
    PRINT 'Column ModifiedBy already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
      AND name = 'ModifiedAtUtc'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD ModifiedAtUtc DATETIME2(0) NULL;

    PRINT 'Column ModifiedAtUtc added';
END
ELSE
BEGIN
    PRINT 'Column ModifiedAtUtc already exists';
END
GO

IF OBJECT_ID('etl.cuberefreshnotificationpolicy', 'U') IS NULL
BEGIN
    CREATE TABLE etl.cuberefreshnotificationpolicy (
        NotificationPolicyId INT IDENTITY(1,1) NOT NULL,
        EnvironmentName NVARCHAR(50) NOT NULL CONSTRAINT DF_cuberefreshnotificationpolicy_EnvironmentName DEFAULT ('*'),
        PolicyGroup NVARCHAR(100) NULL,
        CubeName NVARCHAR(200) NULL,
        GuardrailType NVARCHAR(100) NULL,
        NotificationType NVARCHAR(50) NOT NULL,
        RecipientRole NVARCHAR(50) NOT NULL,
        Recipients NVARCHAR(2000) NOT NULL,
        Severity NVARCHAR(50) NULL,
        IsEnabled BIT NOT NULL CONSTRAINT DF_cuberefreshnotificationpolicy_IsEnabled DEFAULT (1),
        SortOrder INT NOT NULL CONSTRAINT DF_cuberefreshnotificationpolicy_SortOrder DEFAULT (100),
        ModifiedBy NVARCHAR(256) NULL,
        ModifiedAtUtc DATETIME2(0) NOT NULL CONSTRAINT DF_cuberefreshnotificationpolicy_ModifiedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_cuberefreshnotificationpolicy PRIMARY KEY CLUSTERED (NotificationPolicyId)
    );

    PRINT 'Table etl.cuberefreshnotificationpolicy created';
END
ELSE
BEGIN
    PRINT 'Table etl.cuberefreshnotificationpolicy already exists';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('etl.cuberefreshnotificationpolicy')
      AND name = 'IX_cuberefreshnotificationpolicy_lookup'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_cuberefreshnotificationpolicy_lookup
    ON etl.cuberefreshnotificationpolicy (
        EnvironmentName,
        NotificationType,
        PolicyGroup,
        CubeName,
        GuardrailType,
        IsEnabled,
        SortOrder
    );

    PRINT 'Index IX_cuberefreshnotificationpolicy_lookup created';
END
ELSE
BEGIN
    PRINT 'Index IX_cuberefreshnotificationpolicy_lookup already exists';
END
GO

-- Seed policy metadata on the existing mapping rows.
UPDATE cube
SET
    TableOwnerRecipients = COALESCE(
        NULLIF(LTRIM(RTRIM(cube.TableOwnerRecipients)), ''),
        NULLIF(LTRIM(RTRIM(cube.OwnerEmails)), '')
    ),
    PolicyGroup = COALESCE(
        NULLIF(LTRIM(RTRIM(cube.PolicyGroup)), ''),
        CASE
            WHEN ISNULL(cube.IsIgnored, 0) = 1 THEN 'manual-quarantine'
            WHEN cube.MaxRowsPerRun IS NOT NULL THEN 'heavy-fact'
            WHEN ISNULL(cube.RequirePartition, 0) = 1 THEN 'partitioned-fact'
            ELSE 'default'
        END
    ),
    RefreshWave = COALESCE(
        cube.RefreshWave,
        CASE
            WHEN ISNULL(cube.IsIgnored, 0) = 1 THEN 40
            WHEN cube.MaxRowsPerRun IS NOT NULL THEN 30
            WHEN ISNULL(cube.RequirePartition, 0) = 1 THEN 20
            ELSE 10
        END
    ),
    RefreshPriority = COALESCE(cube.RefreshPriority, 100),
    ModifiedBy = COALESCE(NULLIF(LTRIM(RTRIM(cube.ModifiedBy)), ''), 'migration_add_CubeRefreshPolicy'),
    ModifiedAtUtc = COALESCE(cube.ModifiedAtUtc, SYSUTCDATETIME())
FROM etl.datawarehouseandcubemapping cube
WHERE ISNULL(cube.IsPolicyEnabled, 1) = 1;
GO

-- Seed recipient policy rows used by the pilot UAT workflow.
WITH seed AS (
    SELECT
        '*' AS EnvironmentName,
        'default' AS PolicyGroup,
        CAST(NULL AS NVARCHAR(200)) AS CubeName,
        CAST(NULL AS NVARCHAR(100)) AS GuardrailType,
        'Warning' AS NotificationType,
        'Support' AS RecipientRole,
        'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com' AS Recipients,
        'Warning' AS Severity,
        CAST(1 AS BIT) AS IsEnabled,
        100 AS SortOrder,
        'migration_add_CubeRefreshPolicy' AS ModifiedBy
    UNION ALL SELECT '*', 'default', NULL, NULL, 'Failure', 'Support', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'High', 1, 100, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'default', NULL, NULL, 'SuccessSummary', 'Operational', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'Info', 1, 900, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'heavy-fact', NULL, 'MaxRowsPerRun', 'Warning', 'Support', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'Warning', 1, 10, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'heavy-fact', NULL, 'MaxRowsPerRun', 'Failure', 'Support', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'High', 1, 10, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'partitioned-fact', NULL, 'RequirePartition', 'Warning', 'Support', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'Warning', 1, 20, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'partitioned-fact', NULL, 'RequirePartition', 'Failure', 'Support', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'High', 1, 20, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'manual-quarantine', NULL, 'ManualIgnore', 'Warning', 'Owner', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'High', 1, 5, 'migration_add_CubeRefreshPolicy'
    UNION ALL SELECT '*', 'manual-quarantine', NULL, 'RuntimeFailure', 'Failure', 'Owner', 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com', 'High', 1, 5, 'migration_add_CubeRefreshPolicy'
)
INSERT INTO etl.cuberefreshnotificationpolicy (
    EnvironmentName,
    PolicyGroup,
    CubeName,
    GuardrailType,
    NotificationType,
    RecipientRole,
    Recipients,
    Severity,
    IsEnabled,
    SortOrder,
    ModifiedBy
)
SELECT
    seed.EnvironmentName,
    seed.PolicyGroup,
    seed.CubeName,
    seed.GuardrailType,
    seed.NotificationType,
    seed.RecipientRole,
    seed.Recipients,
    seed.Severity,
    seed.IsEnabled,
    seed.SortOrder,
    seed.ModifiedBy
FROM seed
WHERE NOT EXISTS (
    SELECT 1
    FROM etl.cuberefreshnotificationpolicy policy
    WHERE ISNULL(policy.EnvironmentName, '') = ISNULL(seed.EnvironmentName, '')
      AND ISNULL(policy.PolicyGroup, '') = ISNULL(seed.PolicyGroup, '')
      AND ISNULL(policy.CubeName, '') = ISNULL(seed.CubeName, '')
      AND ISNULL(policy.GuardrailType, '') = ISNULL(seed.GuardrailType, '')
      AND policy.NotificationType = seed.NotificationType
      AND policy.RecipientRole = seed.RecipientRole
      AND policy.Recipients = seed.Recipients
);
GO

SELECT
    CubeName,
    CubeTableName,
    PolicyGroup,
    RefreshWave,
    RefreshPriority,
    TableOwnerRecipients,
    IsPolicyEnabled,
    ModifiedBy,
    ModifiedAtUtc
FROM etl.datawarehouseandcubemapping
WHERE CubeName IN ('DAModel', 'MM_CubeModel', 'VN_CubeModel', 'PROD_DataAnalyticsModel', 'NEW_CubeModel')
ORDER BY CubeName, CubeTableName, Partition;
GO

SELECT
    EnvironmentName,
    PolicyGroup,
    CubeName,
    GuardrailType,
    NotificationType,
    RecipientRole,
    Recipients,
    Severity,
    SortOrder
FROM etl.cuberefreshnotificationpolicy
ORDER BY EnvironmentName, PolicyGroup, NotificationType, SortOrder;
GO
