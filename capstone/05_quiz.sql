use role accountadmin;
use warehouse capstone_wh;
use database capstone;
use schema dev;

alter warehouse capstone_wh set warehouse_size=xxlarge;

-- Assuming capstone_stage is the stage pointing to the top of your S3 Bucket.  How many files are in this location?
-- capstone_stage/kbfi-0001/2021/03/17/
SELECT * FROM DIRECTORY( @adsb_kbfi_stage ) limit 10;
SELECT count(distinct(relative_path)) 
FROM DIRECTORY( @adsb_kbfi_stage ) 
	where startswith(relative_path, '2021/03/17/');

--Assuming capstone_stage is the stage pointing to the top of your S3 Bucket.  How many files are in this location?
--capstone_stage/kapa-0001/year=2021/month=12/day=07/
select * from directory (@adsb_kapa_stage) limit 10;
select count(distinct(relative_path))
from directory (@adsb_kapa_stage) 
	where startswith(relative_path, 'year=2021/month=12/day=07/');

--One of the requirements was to store the file name and the row number from the source file in the data.  Answer the following questions based on this requirement. What is the highest row number in the files for the kbfi data?
select max(file_row_number) from adsb_kbfi_raw;

--One of the requirements was to store the file name and the row number from the source file in the data. Answer the following questions based on this requirement. What is the distinct count of file names loaded in the files for the kapa data where file_name like '%/year=2022/%'?   (Assume file_name is what you called the file name loaded in the table.)
select count (distinct (filename)) from adsb_kapa_raw where filename like '%/year=2022/%';

--Answer the following question based on the data in the combined view that includes the flight data and the FAA data (master, reference file, engine).  
--Who owns aircraft with ident N350XX ?
select 
    aircraft_flight, 
    name, 
    mfr,
    model,
    record_ts,
    filename,
    file_row_number
from aircraft_flight_vw where AIRCRAFT_FLIGHT = 'N350XX';

--Answer the following question based on the data in the combined view that includes the flight data and the FAA data (master, reference file, engine).   
--Who is the plane manufacturer and model number for aircraft with ident N350XX?

--(same query as previous)

--Answer the following question based on the data in the combined view that includes the flight data and the FAA data (master, reference file, engine).   
--What is the manufacturer and model number of the plan owned by 007 ENTERPRISES LLC?
select 
    name, 
    mfr,
    model,
    record_ts,
    filename,
    file_row_number
from aircraft_flight_vw where name = '007 ENTERPRISES LLC';

--Answer the following question based on the data in the combined view that includes the flight data and the FAA data (master, reference file, engine).  
--How many planes (HEXID) did AMAZON.COM SERVICES LLC fly in 2021 according to the data?
select count(distinct(aircraft_hex)) from aircraft_flight_vw 
where name = 'AMAZON.COM SERVICES LLC' and date_part('year', record_ts) = 2021;

--Answer the following question based on the data in the combined view that includes the flight data and the FAA data (master, reference file, engine).  
--How many planes (HEXID) did FEDERAL EXPRESS CORPORATION fly in 2022 according to the data?
select count(distinct(aircraft_hex)) from aircraft_flight_vw 
where name = 'FEDERAL EXPRESS CORPORATION' and date_part('year', record_ts) = 2022;

--Answer the following question based on the data in the combined view that includes the flight data and the FAA data (master, reference file, engine).  
--How many owners (think name like FEDERAL EXPRESS CORPORATION) fly on 2020-12-25 according to the data?
select count(distinct(name)) from aircraft_flight_vw where datediff('day', record_ts, '2020-12-25') = 0;
