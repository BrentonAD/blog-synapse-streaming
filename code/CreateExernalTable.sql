------------------------------------------------------------------------------------------------------------
-- Create an external table on an On-Demand Azure Synapse SQL Pool to query like ordinary Azure Database
------------------------------------------------------------------------------------------------------------

USE dw_gold

CREATE EXTERNAL DATA SOURCE IotDeltaLake  
WITH
(    
    LOCATION = 'abfss://iot@<storage-account>.blob.core.windows.net'
) 
GO
CREATE EXTERNAL FILE FORMAT DeltaLakeFormat WITH (  FORMAT_TYPE = DELTA );
GO

CREATE EXTERNAL TABLE turbine_enriched (
    [date] DATETIME,
    [deviceid] VARCHAR(MAX),
    [windowStart] DATETIME,
    [windowEnd] DATETIME,
    [rpm] FLOAT,
    [angle] FLOAT,
    [temperature] FLOAT,
    [humidity] FLOAT,
    [windspeed] FLOAT,
    [winddirection] VARCHAR(4)
) WITH (
        LOCATION = 'gold/turbine_enriched', --> the root folder containing the Delta Lake files
        data_source = IotDeltaLake,
        FILE_FORMAT = DeltaLakeFormat
);