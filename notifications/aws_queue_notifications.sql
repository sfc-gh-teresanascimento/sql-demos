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
-- These are illustrative examples
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


