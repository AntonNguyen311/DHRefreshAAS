-- Add RefreshType column to etl.datawarehouseandcubemapping
-- Values: 'Full'     = data + calculate per partition (safe, no report errors during refresh)
--         'DataOnly' = load data only, calculate once at the end (faster for large tables,
--                      but reports may show 'needs to be recalculated' until Calculate completes)
-- Default: 'Full' (safe — manually set large/slow tables to 'DataOnly' for performance)

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
    AND name = 'RefreshType'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD RefreshType NVARCHAR(20) NOT NULL DEFAULT 'Full';

    PRINT 'Column RefreshType added to etl.datawarehouseandcubemapping (default: Full)';
END
ELSE
BEGIN
    PRINT 'Column RefreshType already exists';
END
GO

-- Verify
SELECT CubeTableName, Partition, RefreshType, CubeName
FROM etl.datawarehouseandcubemapping
ORDER BY RefreshType, CubeTableName;
GO

-- To set specific large tables to DataOnly for better performance, run:
-- UPDATE etl.datawarehouseandcubemapping SET RefreshType = 'DataOnly' WHERE CubeTableName IN ('fSalesNAV', 'fInventory', ...);
