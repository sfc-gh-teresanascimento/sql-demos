use role accountadmin;
use warehouse capstone_wh;
use database capstone;
use schema dev;

alter warehouse capstone_wh set warehouse_size=xxlarge;

--create conformed adsb table from kapa data
create or replace table
	adsb CLUSTER BY (RECORD_TS) 
as select
	v:gs::integer as AIRCRAFT_GS,
	v:heading::integer as AIRCRAFT_NAV_HEADING,
    v:baro_alt::integer as AIRCRAFT_ALT_BARO,
	v:squawk::integer as AIRCRAFT_SQUAWK,
    v:gps_alt::integer as AIRCRAFT_ALT_GEOM,
	v:lon::integer as AIRCRAFT_LON,
	v:ident::string as AIRCRAFT_FLIGHT,
    v:hexid::string as AIRCRAFT_HEX,
    v:clock::datetime as RECORD_TS,
    v:lat:integer as AIRCRAFT_LAT,
    v:air_ground::string as AIR_GROUND,
    FILENAME,
    FILE_ROW_NUMBER
from
	adsb_kapa_raw;

--insert kbfi data into conformed adsb
insert into adsb
  select
    a.value:gs::integer as AIRCRAFT_GS,
    a.value:true_heading::integer as AIRCRAFT_NAV_HEADING,
	to_number(case when a.value:alt_baro='ground' then 0 else a.value:alt_baro END) as AIRCRAFT_ALT_BARO,
    a.value:squawk::integer as AIRCRAFT_SQUAWK,
    a.value:alt_geom::integer as AIRCRAFT_ALT_GEOM,
	a.value:lon::integer as AIRCRAFT_LON,
    a.value:flight::string as AIRCRAFT_FLIGHT,
	a.value:hex::string as AIRCRAFT_HEX,
	v:now::datetime AS RECORD_TS,
	a.value:lat::integer as AIRCRAFT_LAT,
	(case when a.value:alt_baro = 'ground' THEN 'G' WHEN a.value:alt_baro != 0 THEN 'A' ELSE Null END) as AIR_GROUND,
    FILENAME,
    FILE_ROW_NUMBER
  from
	adsb_kbfi_raw,
    lateral flatten(input => adsb_kbfi_raw.v, path => 'aircraft') a;

--create task to insert data from kapa_stream into adsb
create or replace task kapa_task 
    user_task_managed_initial_warehouse_size = small
    schedule = '3 minutes'
    when system$stream_has_data('kapa_stream')
    as insert into adsb
    select
    	v:gs::integer as AIRCRAFT_GS,
    	v:heading::integer as AIRCRAFT_NAV_HEADING,
        v:baro_alt::integer as AIRCRAFT_ALT_BARO,
    	v:squawk::integer as AIRCRAFT_SQUAWK,
        v:gps_alt::integer as AIRCRAFT_ALT_GEOM,
    	v:lon::integer as AIRCRAFT_LON,
    	v:ident::string as AIRCRAFT_FLIGHT,
        v:hexid::string as AIRCRAFT_HEX,
        v:clock::datetime as RECORD_TS,
        v:lat:integer as AIRCRAFT_LAT,
        v:air_ground::string as AIR_GROUND,
        FILENAME,
        FILE_ROW_NUMBER
    from
    	adsb_kapa_raw;

show tasks;
ALTER TASK kapa_task RESUME;
select count(*) from adsb;




