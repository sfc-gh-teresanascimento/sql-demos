/**
 * This demo illustrates how to continuously load data in micro batches into Snowflake 
 * with Snowpipe auto-ingest. 
 * We will use Azure Event Grid to send event notifications to Snowflake to trigger 
 * data ingestion automatically. 
 * 
 */

 /* Step 1: Setup your Azure blob storage
  * Instructions: Check Microsoft documentation to create a Storage account and a blob storage container
  * https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create
  * https://learn.microsoft.com/en-us/azure/storage/blobs/blob-containers-portal
  */

 /* Step 2: Create a Snowflake external stage connected to your Azure blob storage container
  */ 
use role ACCOUNTADMIN;

-- Update the variables below with your Azure account details details
SET TENANT_ID = 'xxxx-xxxx-xxxx'; -- Find this at Azure Portal -> Microsoft Entra ID -> Properties
SET CONTAINER_PATH = '/<container>/<path>/'; 

-- Create a storage integration
CREATE OR REPLACE STORAGE INTEGRATION AZUREBLOB
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = $TENANT_ID
  STORAGE_ALLOWED_LOCATIONS = ($CONTAINER_PATH);

  DESCRIBE STORAGE INTEGRATION AZUREBLOB; -- Collect AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME 

-- Grant Snowflake access to storage locations
-- 1) Navigate to AZURE_CONSENT_URL collected previous and select 'Accept'
-- 2) In the Azure Portal -> Storage Accounts -> Your Storage Account -> Access Control (IAM) -> 
--    -> Add -> Add Role Assignment
--     - Tab Role: select 'Storage Blob Data Reader'
--     - Tab Members: Select Members ->  Search by AZURE_MULTI_TENANT_APP_NAME (use only the string before the underscore)
--     - Review and Assign

-- Create a database and a schema to use in this demo
create or replace database DEMO_SNOWPIPE;
create or replace schema DEV;

-- Create a file format that represents the files in the Azure blob storage container
create or replace file format FILE_JSON
  type = 'JSON'
  strip_outer_array = true;

-- Create an external stage
CREATE OR REPLACE STAGE AZURESTAGE
  STORAGE_INTEGRATION = AZUREBLOB
  URL = $CONTAINER_PATH
  FILE_FORMAT = FILE_JSON;

LIST @AZURESTAGE;

 /* Step 3: Setup Azure Event Grid to send storage events
  * Instructions: Check this documentation on how to configure an Event Grid subscription in the Azure Portal
  * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-azure#configuring-automation-with-azure-event-grid
  */ 
  

 /* Step 4: Create a Snowflake notification integration and grant Snowflake access to the storage queue in Azure
  */ 

-- Update the variables below with your Azure Storage Queue URL
SET QUEUE_URL = 'xxxx-xxxx-xxxx'; -- Find this at Azure Portal -> Storage account -> Queue service -> Queues

-- Create the notification integration in Snowflake
CREATE NOTIFICATION INTEGRATION AZEVENTGRID
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = $QUEUE_URL
  AZURE_TENANT_ID = $TENANT_ID;

DESCRIBE NOTIFICATION INTEGRATION AZEVENTGRID; -- Collect AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME 

-- Grant Snowflake access to storage locations
-- 1) Navigate to AZURE_CONSENT_URL collected previous and select 'Accept'
-- 2) In the Azure Portal -> Storage Accounts -> Your Storage Queue -> Queues -> Your Queue 
--    -> Access Control (IAM) -> Add -> Add Role Assignment
--     - Tab Role: select 'Storage Queue Data Contributor'
--     - Tab Members: Select Members ->  Search by AZURE_MULTI_TENANT_APP_NAME (use only the string before the underscore)
--     - Review and Assign

/* Step 5: Create a Snowpipe with auto-ingest enabled
 */

-- Create a table to store ingested data from files. This is an illustrative example.
CREATE OR REPLACE TABLE SAMPLE_TABLE
    (
        detail_id NUMBER(38,0),
        id NUMBER(38,0),
        item_id NUMBER(38,0),
        quantity NUMBER(5,0),
        price NUMBER(38,6),
    );

-- Create a Snowpipe with auto-ingest enabled and referencing the Event Grid notification integration.
-- Use the COPY command to copy data from the files into a Snowflake table. 
-- Snowflake natively supports JSON. JSON first-level attributes can be accessed by adding a colon ":" to the referenced JSON. 
-- This is an illustrative example
create or replace pipe JSON_SNOWPIPE
  auto_ingest = true
  integration = 'AZEVENTGRID'
  as
  COPY INTO SAMPLE_TABLE
  FROM 
    (SELECT 
        $1:"DETAIL_ID"::NUMBER(38,0) AS detail_id,
        $1:"ID"::NUMBER(38,0) AS id,
        $1:"ITEM_ID"::NUMBER(38,0) AS item_id,
        $1:"QUANTITY"::NUMBER(5,0) AS quantity,
        $1:"PRICE"::NUMBER(38,6) AS price,
    FROM @AZURESTAGE/
    (FILE_FORMAT => FILE_JSON) 
    );

-- Refresh the pipe to load any historical files already in the stage
ALTER PIPE JSON_SNOWPIPE REFRESH;

-- You will need a warehouse for the steps below
create or replace warehouse DEMO_INGESTION_WH with 
	warehouse_size = 'XXLARGE' 
    warehouse_type = 'STANDARD'
    auto_resume = true 
    auto_suspend = 600   
    max_cluster_count = 2
    min_cluster_count = 1;
use warehouse DEMO_INGESTION_WH;

-- Check the status of the Snowpipe
SELECT SYSTEM$PIPE_STATUS('JSON_SNOWPIPE');

-- Check the the records loaded in the table
select count(*) from ORDER_DETAIL;

-- Now try to add a new file to the Azure blob storage container and Snowpipe will automatically load the file
-- and save the records in the table. 

