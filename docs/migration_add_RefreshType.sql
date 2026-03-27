-- Add RefreshType column to etl.datawarehouseandcubemapping
-- Values: 'Full' = data + calculate per partition (consistent immediately, slower)
--         'DataOnly' = load data only, calculate once at the end (faster for large tables)
-- Default: 'DataOnly' (backward compatible — same behavior as before)

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('etl.datawarehouseandcubemapping')
    AND name = 'RefreshType'
)
BEGIN
    ALTER TABLE etl.datawarehouseandcubemapping
    ADD RefreshType NVARCHAR(20) NOT NULL DEFAULT 'DataOnly';

    PRINT 'Column RefreshType added to etl.datawarehouseandcubemapping';
END
ELSE
BEGIN
    PRINT 'Column RefreshType already exists';
END
GO

-- Set dimension tables to 'Full' (small tables, need immediate consistency)
-- Adjust the WHERE clause based on your actual dimension table naming convention
UPDATE etl.datawarehouseandcubemapping
SET RefreshType = 'Full'
WHERE CubeTableName LIKE 'd%'  -- dimension tables typically start with 'd'
  AND RefreshType = 'DataOnly';

PRINT 'Updated dimension tables to Full refresh type';
GO

-- Verify
SELECT CubeTableName, Partition, RefreshType, CubeName
FROM etl.datawarehouseandcubemapping
ORDER BY RefreshType, CubeTableName;
GO
