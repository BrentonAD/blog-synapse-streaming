------------------------------------------------------------------------------------------------------------
-- Examples of how to query Delta lake with On-Demand Azure Synapse SQL Pools
------------------------------------------------------------------------------------------------------------

-- Query Delta Bronze
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/iot/bronze/turbine_raw/',
    FORMAT = 'delta'
) as rows;

-- Query Delta Silver
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/iot/silver/turbine_agg/',
    FORMAT = 'delta'
) as rows;

-- Query Delta Gold
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/iot/gold/turbine_enriched/',
    FORMAT = 'delta'
) as rows

-- Query Raw Parquet in Gold
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/iot/silver/turbine_agg/**',
    FORMAT = 'parquet'
) as rows;
