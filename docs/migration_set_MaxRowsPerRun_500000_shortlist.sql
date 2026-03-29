-- Set an initial MaxRowsPerRun guardrail for the agreed shortlist
-- of fact / partition-heavy mappings. Safe to run on prod, UAT, and new:
-- rows that do not exist in the current database are ignored.

UPDATE etl.datawarehouseandcubemapping
SET MaxRowsPerRun = 500000
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
    MaxRowsPerRun
FROM etl.datawarehouseandcubemapping
WHERE MaxRowsPerRun IS NOT NULL
ORDER BY CubeName, CubeTableName, Partition;
GO
