/**
 * This demo illustrates how to Queue Notifications in Snowflake to send events continuously load data in micro batches into Snowflake 
 * to a topic in AWS SNS.
 * 
 */

 /* Step 1: Create an AWS SNS topic, IAM role and policy
  * Instructions: Follow AWS docs to create an SNS topic. Create as Standard Topic (not FIFO)
  * https://docs.aws.amazon.com/sns/latest/dg/sns-create-topic.html
  * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-errors-sns#step-2-creating-the-iam-policy
  * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-errors-sns#step-3-creating-the-aws-iam-role 
  * 
  * 
  */ 

-- Capture the SNS topic ARN and the role topic ARN in the previous steps
SET SNS_ARN = 'XXXX';
SET ROLE_ARN = 'XXXX';

 /* Step 2: Create a Snowflake notification integration
  */   
use role ACCOUNTADMIN;
CREATE NOTIFICATION INTEGRATION AWSSNSTOPIC
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = $SNS_ARN
  AWS_SNS_ROLE_ARN = $ROLE_ARN;

DESC NOTIFICATION INTEGRATION AWSSNSTOPIC; -- Collect property values: SF_AWS_IAM_USER_ARN and SF_AWS_EXTERNAL_ID
-- Grant Snowflake access to the SNS topic:
--   1) Go to the AWS management console -> IAM -> Roles -> role you created on Step 1.
--   2) Select the 'Trust relationships' tab and then 'Edit Trust Policy'
--   3) Update the policy (in JSON) with:
--     - SF_AWS_IAM_USER_ARN in Principal.AWS
--     - SF_AWS_EXTERNAL_ID in sts:ExternalId
--   4) Then update the policy.

 /* Step 3: Use the Stored Procedure SEND_SNOWFLAKE_NOTIFICATION to send your notification to the AWS SNS topic previously created
  */  

-- You will need a warehouse for the steps below
create or replace warehouse DEMO_ALERTS_WH with 
	warehouse_size = 'XSMALL' 
  warehouse_type = 'STANDARD'
  auto_resume = true 
  auto_suspend = 600;   

-- A few examples of how to send a notification
call SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
  '{ "text/html": "<p>A notification</p>" }', 
  SNOWFLAKE.NOTIFICATION.INTEGRATION('AWSSNSTOPIC'));
call SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(
  '[
    {
      "alertname": "test.alert",
      "instance": "a.example",
      "project": "test.project",
      "service": "test.service"
    }
  ]') ,
  SNOWFLAKE.NOTIFICATION.INTEGRATION('AWSSNSTOPIC'));
call SYSTEM$SEND_SNOWFLAKE_NOTIFICATION('{ "text/plain": "Plain text notification" }', 
  SNOWFLAKE.NOTIFICATION.INTEGRATION('AWSSNSTOPIC'));

 /* Step 4: Use Snowflake Alerts to trigger a notification based on a condition.
  * The condition will be based on values stored in a Snowflake table.
  */   

-- Start by creating a database, schema and a sample table
create or replace database DEMO_ALERTS;
create or replace schema DEV;
create or replace table MONITORED_VALUES (
  id INTEGER,
  value INTEGER,
  record_timestamp TIMESTAMP
);

-- Create the alert referencing the condition to be satisfied (based on values in a table)
--   and the action to be executed once the condition is met.
CREATE OR REPLACE ALERT MONITORED_ALERT
  WAREHOUSE = DEMO_ALERTS_WH
  SCHEDULE = '1 minute'
  IF( EXISTS(
    SELECT ID FROM MONITORED_VALUES WHERE value>100
      AND record_timestamp BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
       AND SNOWFLAKE.ALERT.SCHEDULED_TIME())) 
  THEN
    call SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
      '{ "text/plain": "Monitored values over 100" }', 
      SNOWFLAKE.NOTIFICATION.INTEGRATION('AWSSNSTOPIC'));

-- When alerts are created, they are suspended. You need to resume then manually.
ALTER ALERT MONITORED_ALERT RESUME;

-- Here's how to check alert executions
SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START
      =>dateadd('hour',-1,current_timestamp()))) -- within the last hour
ORDER BY SCHEDULED_TIME DESC;

-- Now insert data into the table that will trigger the alert
insert into MONITORED_VALUES values (1, 1);
insert into MONITORED_VALUES values (2, 101);

-- Check the alert execution history again. Query will return one row per minute, as per the alert schedule.
-- The column 'STATE' will show whether the condition was not met or if the alert was triggered. 
