use role accountadmin;

-- create warehouse
create or replace warehouse CAPSTONE_WH with 
	warehouse_size = 'XSMALL' 
    warehouse_type = 'STANDARD'
    auto_resume = true 
    auto_suspend = 120
    MAX_CLUSTER_COUNT = 1
    MIN_CLUSTER_COUNT = 1;
use warehouse capstone_wh;

-- create database and schema
create or replace database capstone;
use database capstone;
create or replace schema dev;
use schema dev;

/* load first dataset: ADS-B
*/

--setup the AWS bucket before running the next queries.
--follow: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
CREATE or replace STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/capstone-tnascimento-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://capstone-tnascimento/');

--run the query below to copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID 
--into your AWS role trust policy (IAM -> roles -> <your-role> -> Trust relationships -> Edit policy)
  DESCRIBE INTEGRATION S3_INTEGRATION;

--create stages to load ads-b from s3
create or replace stage adsb_kbfi_stage
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://capstone-tnascimento/kbfi-0001/'
  file_format=(type=json)
  directory = (enable=true auto_refresh=true);
list @adsb_kbfi_stage;
  
CREATE or replace STAGE ADSB_KAPA_stage
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://capstone-tnascimento/kapa-0001/'
  FILE_FORMAT =(type=json)
  directory = (enable=true auto_refresh=true);
list @ADSB_KAPA_stage;

-- size up warehouse
alter warehouse capstone_wh set warehouse_size=xxlarge;

--create stage for kbfi raw data with copy command (option 1)
create or replace table adsb_kbfi_raw (filename varchar, file_row_number number, record_dt date, v variant);
copy into adsb_kbfi_raw from (
	select 
    	metadata$filename, 
        metadata$file_row_number, 
        to_date(substr(metadata$filename, 11, 10), 'YYYY/MM/DD'),
        parse_json($1) 
    from @adsb_kbfi_stage);
select count(*) from adsb_kbfi_raw;
select * from adsb_kbfi_raw limit 10;


--create stage for kbfi raw data as external table (option 2)
create or replace external table ext_adsb_kbfi_raw(
  filename varchar as (metadata$filename), 
  file_row_number number as (metadata$file_row_number), 
  record_dt date as to_date(substr(metadata$filename, 11, 10), 'YYYY/MM/DD')
) partition by (record_dt)
	location = @adsb_kbfi_stage
    file_format = (type=json)
	auto_refresh = true;
select * from ext_adsb_kbfi_raw limit 10;
    
--create stage for kapa raw data with copy command (option 1)
create or replace table adsb_kapa_raw (filename varchar, file_row_number number, record_dt date, v variant);
copy into adsb_kapa_raw from (
	select 
        metadata$filename, 
        metadata$file_row_number, 
        to_date(
            substr(metadata$filename, 16, 5) ||  substr(metadata$filename, 27, 3) || substr(metadata$filename, 34, 2),
            'YYYY/MM/DD'
    		),
        parse_json($1) 
    from @adsb_kapa_stage);
select count(*) from adsb_kapa_raw;
select * from adsb_kapa_raw limit 10;

-- --create stage for kapa raw data as external table (option 2)
create or replace external table ext_adsb_kapa_raw(
  filename varchar as (metadata$filename), 
  file_row_number number as (metadata$file_row_number), 
  record_dt date as 
  	to_date(
		substr(metadata$filename, 16, 5) ||  substr(metadata$filename, 27, 3) || substr(metadata$filename, 34, 2),
    	'YYYY/MM/DD'
	)
) partition by (record_dt)
	location = @adsb_kapa_stage
    file_format = (type=json)
	auto_refresh = true;
select * from ext_adsb_kapa_raw limit 10;

--create snowpipe to ingest kapa data continuosly with copy command (possible with external table?)
create or replace pipe adsb_kapa_pipe auto_ingest=true as
	copy into adsb_kapa_raw
	from (select 
    	metadata$filename, 
        metadata$file_row_number, 
        to_date(metadata$FILE_LAST_MODIFIED), 
        parse_json($1) from @adsb_kapa_stage);
show pipes;
select system$pipe_status('adsb_kapa_pipe');

--create stream to track changes in kapa raw data
create or replace stream adsb_kapa_stream on table adsb_kapa_raw;
show streams;
select count(*) from adsb_kapa_stream;
    
/* load second dataset: FAA
*/

--create csv format to load FAA data
CREATE OR replace FILE FORMAT csv_format
	type = csv
	field_delimiter = ','
	skip_header = 1
	trim_space = true
	error_on_column_count_mismatch = false
	empty_field_as_null = true
	compression = gzip;

--create stage for FAA data
create or replace stage faa_stage
	file_format = (type = csv);
list @faa_stage;
    
/*

1) manually download locally new FAA data

2) login into snowsql

snowsql -a bi50110.eu-west-2.aws -u TNASCIMENTO

3) inside snowsql:

use DATABASE CAPSTONE;
use SCHEMA dev;
put file://~/Downloads/ReleasableAircraft/MASTER.txt @faa_stage;
put file://~/Downloads/ReleasableAircraft/ENGINE.txt @faa_stage;
put file://~/Downloads/ReleasableAircraft/ACFTREF.txt @faa_stage;

*/
list @faa_stage;

--load faa data from stages into tables
--load acftref data
create or replace table faa_acftref_raw(
    code string,
    mfr string,
    model string,
    type_acft string,
    type_eng string,
    ac_cat string,
    build_cert_ind number,
    no_eng number,
    no_seats number,
    ac_weight string,
    speed string,
    tc_data_sheet string,
    tc_data_holder string);

copy into faa_acftref_raw
	from @faa_stage/ACFTREF.txt.gz
	file_format = csv_format;

select * from faa_acftref_raw limit 10;    

--load engine data
create or replace table faa_engine_raw(
    code string,
    mfr string,
    model string,
    type number,
    horsepower number,
	thrust number);
    
copy into faa_engine_raw
    from @faa_stage/ENGINE.txt.gz
	file_format = csv_format;

select * from faa_engine_raw limit 10;

--load master data
create or replace table faa_master_raw
    (n_number string,
    serial_number string,
    mfr_mdl_code string,
    eng_mfr_mdl number,
    year_mfr string,
    type_registrant number,
    name string,
    street string,
    street2 string,
    city string,
    state string,
    zip_code string,
    region string,
    county number,
    country string,
    last_action_date date,
    cert_issue_date date,
    certification string,
    aircraft_type string,
    engine_type number,
    status_code string,
    mode_s_code number,
    fract_owner string,
    air_worth_date date,
    other_names_1 string,
    other_names_2 string,
    other_names_3 string,
    other_names_4 string,
    other_names_5 string,
    expiration_date date,
    unique_id number,
    kit_mfr string,
    kit_model string,
    mode_s_hex_code string);

copy into faa_master_raw
    from @faa_stage/MASTER.txt.gz
    file_format = csv_format;
    
select * from faa_master_raw limit 5000;

--master load check!! this query should produce no results
select mode_s_hex_code
, len(mode_s_hex_code)
, len(trim(mode_s_hex_code, ' '))
from faa_master_raw md
where len(mode_s_hex_code) != len(trim(mode_s_hex_code, ' '))
; 

/* load third dataset: weather

	search marketplace for: Weather Source LLC: frostbyte
	database name: weather_source
*/

select * from weather_source.onpoint_id.postal_codes limit 100;
--check weather at snowflake office in march
select
    postal_code,
    country,
    date_valid_std,
    min_temperature_air_2m_f,
    avg_temperature_air_2m_f,
    max_temperature_air_2m_f
from
    weather_source.onpoint_id.history_day
where
    postal_code = 'EC1V 9NR' and
    country = 'GB' and
    date_valid_std between date_from_parts(year(current_date)-1,3,1) and date_from_parts(year(current_date)-1,3,31)
order by
    date_valid_std;

-- downsize warehouse
alter warehouse capstone_wh set warehouse_size=xsmall;