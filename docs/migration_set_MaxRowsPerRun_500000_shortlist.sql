-- Set an initial MaxRowsPerRun guardrail for the agreed shortlist
-- of fact / partition-heavy mappings. Safe to run on prod, UAT, and new:
-- rows that do not exist in the current database are ignored.

UPDATE etl.datawarehouseandcubemapping
SET
    MaxRowsPerRun = 500000,
    PolicyGroup = COALESCE(NULLIF(LTRIM(RTRIM(PolicyGroup)), ''), 'heavy-fact'),
    GuardrailType = COALESCE(NULLIF(LTRIM(RTRIM(GuardrailType)), ''), 'MaxRowsPerRun'),
    TableOwnerRecipients = COALESCE(
        NULLIF(LTRIM(RTRIM(TableOwnerRecipients)), ''),
        NULLIF(LTRIM(RTRIM(OwnerEmails)), ''),
        'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com'
    ),
    OwnerEmails = COALESCE(NULLIF(LTRIM(RTRIM(OwnerEmails)), ''), 'Anton.Tuan@deheus.com; Gina.Hai@deheus.com; mina.my@deheus.com'),
    FixGuide = COALESCE(
        NULLIF(LTRIM(RTRIM(FixGuide)), ''),
        'This table exceeded the per-run volume threshold. Refresh only the active partitions, reduce the lookback window, review the source query for large scans, and rerun after the load size is back within limit.'
    ),
    RequirePartition = CASE
        WHEN NULLIF(LTRIM(RTRIM(ISNULL(Partition, ''))), '') IS NOT NULL THEN 1
        ELSE RequirePartition
    END
WHERE
    (CubeName = 'DAModel' AND CubeTableName IN (
        'DWH vw_fInventory',
        'DWH vw_fProductionTransaction',
        'DWH vw_fProductionVolume',
        'DWH vw_fSalesNAV'
    ))
    OR (CubeName = 'MM_CubeModel' AND CubeTableName IN (
        'MMWH vw_wfSales',
        'MMWH vw_wfSalesNAV',
        'MMWH vw_wfProductionVolumeForBulk'
    ))
    OR (CubeName = 'PROD_DataAnalyticsModel' AND CubeTableName IN (
        'AR Aging Detail',
        'DWH vw_fSalesNAV'
    ))
    OR (CubeName = 'NEW_CubeModel' AND CubeTableName IN (
        'DWH vw_fSalesNAV_Consol',
        'DWH vw_fSalesBudgetNAV_Consol'
    ));
GO

SELECT
    CubeName,
    CubeTableName,
    Partition,
    PolicyGroup,
    RequirePartition,
    GuardrailType,
    TableOwnerRecipients,
    OwnerEmails,
    MaxRowsPerRun
FROM etl.datawarehouseandcubemapping
WHERE MaxRowsPerRun IS NOT NULL
ORDER BY CubeName, CubeTableName, Partition;
GO
