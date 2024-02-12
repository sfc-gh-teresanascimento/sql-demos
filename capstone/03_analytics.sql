use role accountadmin;
use warehouse capstone_wh;
use database capstone;
use schema dev;

--create view on top of external table kapa
create or replace view ADS_B_KAPA_EXT_VW as
select 
	filename,
    file_row_number,
    record_dt,
    value
from EXT_ADSB_KAPA_RAW;

--create view on top of external table kbfi
create or replace view ADS_B_KBFI_EXT_VW as
select 
	filename,
    file_row_number,
    record_dt,
    value
from EXT_ADSB_KBFI_RAW;

--clear warehouse and result set cache
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
alter warehouse capstone_wh suspend;
alter warehouse capstone_wh resume;
alter warehouse capstone_wh set warehouse_size=xsmall;

--queries below should produce results in less than 30s
-- select *
-- 	from ads_b_kapa_ext_vw where record_dt = '2021-12-07';
select *
	from ADS_B_KBFI_EXT_VW where record_dt = '2021-03-17';

alter warehouse capstone_wh set warehouse_size=xxlarge;
    
--create view on stage tables raw - kbfi and kapa
create or replace view stage_ads_b_kbfi_vw as
    select 
    	filename,
        file_row_number,
        record_dt,
        v
    from ADSB_KBFI_RAW;

create or replace view stage_ads_b_kapa_vw as
    select 
    	filename,
        file_row_number,
        record_dt,
        v:clock::datetime as RECORDED_TS,
        v
    from ADSB_KAPA_RAW;

alter warehouse capstone_wh suspend;
alter warehouse capstone_wh resume;
alter warehouse capstone_wh set warehouse_size=xsmall;

-- these queries will take longer then the ones querying external tables
-- use limit 
SELECT * from stage_ads_b_kapa_vw limit 100000000;
SELECT * from stage_ads_b_kapa_vw limit 1000;


create or replace view aircraft_flight_vw as
	select 
    	a.*, master.*, 
        ref.code,
        ref.mfr,
        ref.model,
        ref.type_eng,
        ref.ac_weight,
        ref.no_seats,
        ref.no_eng,
        e.horsepower,
        e.thrust
 	from adsb a
    left outer join faa_master_raw master on a.aircraft_hex = master.mode_s_hex_code
    left outer join faa_acftref_raw ref on master.mfr_mdl_code = ref.code
    left outer join faa_engine_raw e on master.eng_mfr_mdl = e.code;

select * from aircraft_flight_vw limit 10;

--Who owns aircraft with ident N350XX ?
--Who is the plane manufacturer and model number?
select 
    aircraft_flight, 
    name, 
    mfr,
    model,
    record_ts,
    filename,
    file_row_number
from aircraft_flight_vw where AIRCRAFT_FLIGHT = 'N350XX';

--Who owns aircraft with HEXID A0AEFD ?
--Who is the plane manufacturer and model number?
select 
    aircraft_flight, 
    name, 
    mfr,
    model,
    record_ts,
    filename,
    file_row_number
from aircraft_flight_vw where AIRCRAFT_HEX = 'A0AEFD';

--What is the manufacturer and model number of the plan owned by 007 ENTERPRISES LLC?
select 
    name, 
    mfr,
    model,
    record_ts,
    filename,
    file_row_number
from aircraft_flight_vw where name = '007 ENTERPRISES LLC';

--create audit user and warehouse to run queries on views
create or replace warehouse CAPSTONE_AUDIT_WH with 
	warehouse_size = 'XSMALL' 
    warehouse_type = 'STANDARD'
    auto_resume = true 
    auto_suspend = 120
    MAX_CLUSTER_COUNT = 2
    MIN_CLUSTER_COUNT = 1;

use role accountadmin;
create or replace role audit_role;
grant all on warehouse CAPSTONE_AUDIT_WH to role audit_role;
grant usage on database capstone to role audit_role;
grant usage on schema capstone.dev to role audit_role;
grant select on all views in database capstone to role audit_role;

grant role audit_role to user TNASCIMENTO;
use role audit_role;
use warehouse capstone_audit_wh;
alter warehouse capstone_audit_wh set warehouse_size=xsmall;

use database capstone;
use schema dev;
show databases;
show schemas;

--this query produce no results
show tables;

--audit role doesn't have priviledges to query adsb
select * from capstone.dev.adsb;

--audit role should be able to query all views below
select * from capstone.dev.ADS_B_KAPA_EXT_VW limit 10;
select * from capstone.dev.ADS_B_KBFI_EXT_VW limit 10;
select * from capstone.dev.stage_ads_b_kapa_vw limit 10;
select * from capstone.dev.stage_ads_b_kbfi_vw limit 10;
select * from capstone.dev.aircraft_flight_vw limit 10;

--create audit user with audit role
use role accountadmin;
create or replace user AUDIT_USER password='Snowflake1!';
grant role audit_role to user AUDIT_USER;
    