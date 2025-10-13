-------------------------Database design & ETL----------------------
---- Data Cleaning  - Demography-----
--Staging for demography --
Create table demography_staging as (
	Select * from demography
);

Select * from demography_staging;

---Altering the Datatype---
Alter table demography_staging
	alter column patientid type integer using patientid::integer,
	alter column hba1c type numeric using hba1c::numeric;

ALTER TABLE demography_staging
ADD CONSTRAINT pk_patientid PRIMARY KEY (patientid);

----Checking for Duplicate rows in demography_staging table---

SELECT patientid, COUNT(*) AS count
FROM demography_staging
GROUP BY patientid
HAVING COUNT(*) > 1; /* No duplicate rows */

-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
--Staging for dexcom --
Create table dexcom_staging as (
	Select * from dexcom
);

Select * from dexcom_staging;

--Dropping the columns with only null values---
Alter table dexcom_staging
	drop column patientinfo,
	drop column deviceinfo,
	drop column insulinvalue_u,
	drop column carbvaluegrams,
	drop column glucoserateofchangemgdlmin;
	
---Column name changes---
alter table dexcom_staging rename column timeof to date_time;
alter table dexcom_staging rename column glucosevaluemgdl to glucosevalue;
alter table dexcom_staging rename column transmittertimelonginteger to transmittertime;

Select * from dexcom_staging;

---Altering the Datatype---
Alter table dexcom_staging
	alter column indexval type integer using indexval::integer,
	alter column patientid type integer using patientid::integer,
	alter column date_time type timestamp using date_time::timestamp,
	alter column glucosevalue type integer using glucosevalue::integer,
	alter column duration type time using duration::time,
	alter column transmittertime type integer using transmittertime::integer;
	
Select * from dexcom_staging;	

---Adding Constraints--
ALTER TABLE dexcom_staging
ADD CONSTRAINT fk_dexcom_patientid FOREIGN KEY (patientid)
REFERENCES demography_staging(patientid);


----Checking for Duplicate rows in dexcom_staging table---

SELECT patientid, date_time, COUNT(*) AS count
FROM dexcom_staging
GROUP BY patientid, date_time
HAVING COUNT(*) > 1;   /* No duplicate rows */

-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
---Staging for eda ---
Create table eda_staging as (
	Select * from eda
);

Select * from eda_staging;


---Column name changes---
alter table eda_staging rename column timeof to date_time;

---Altering the Datatype---
Alter table eda_staging
	alter column patientid type integer using patientid::integer,
	alter column date_time type timestamp(3) using date_time:: timestamp(3),
	alter column eda type double precision using eda::double precision;-- it took long excution time so changed to float--
----Changing Datatype-----
Alter table eda_staging alter column eda type float using eda::float;

---Adding Constraints--
ALTER TABLE eda_staging
ADD CONSTRAINT fk_eda_patientid FOREIGN KEY (patientid)
REFERENCES demography_staging(patientid);


----Checking for Duplicate rows in eda_staging table---

SELECT date_time, eda, patientid, COUNT(*) AS count
FROM eda_staging
GROUP BY date_time, eda, patientid
HAVING COUNT(*) > 1;   /* No duplicate rows */


-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
--Staging for foodlog ---
Create table foodlog_staging as (
	Select * from foodlog
);

Select * from foodlog_staging;

---Dropping the columns---
Alter table foodlog_staging
	drop column dateof,
	drop column timeof,
	drop column time_of_day;

---Column name changes---
Alter table foodlog_staging rename column amount to quantity;
	
---Altering the Datatype---
Alter table foodlog_staging
	alter column patientid type integer using patientid::integer,
	alter column time_begin type timestamp using time_begin::timestamp,
	alter column time_end type time using time_end::time,
	alter column quantity type varchar using quantity::varchar,
	alter column calorie type numeric using calorie::numeric,
	alter column total_carb type numeric using total_carb::numeric,
	alter column sugar type numeric using sugar::numeric,
	alter column protein type numeric using protein::numeric,
	alter column dietary_fiber type numeric using dietary_fiber::numeric,
	alter column total_fat type numeric using total_fat::numeric;
	
---Adding Constraints--
ALTER TABLE foodlog_staging
ADD CONSTRAINT fk_foodlog_patientid FOREIGN KEY (patientid)
REFERENCES demography_staging(patientid);


----Checking for Duplicate rows in foodlog_staging table---

SELECT patientid, time_begin, logged_food ,COUNT(*) AS count
FROM foodlog_staging
GROUP BY patientid,time_begin,  logged_food
HAVING COUNT(*) > 1;
/* one row with patientid = 15, time_begin ='2020-02-21', logged_food = coffee+skim milk is having duplicate entire row */

-----Removing duplpicate row-----

with ranked_foodlog as (
  select ctid,
         row_number() over (
           partition by patientid, time_begin, logged_food
           order by time_begin
         ) as row_num
  from foodlog_staging
)
delete from foodlog_staging
where ctid in (
  select ctid
  from ranked_foodlog
  where row_num > 1
  limit 1
);  


-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
--Staging for hr ---
Create table hr_staging as (
	Select * from hr
);

Select * from hr_staging;
---Column name changes---
Alter table hr_staging rename column timeof to date_time;
	
---Altering the Datatype---
Alter table hr_staging
	alter column patientid type integer using patientid::integer,
	alter column date_time type timestamp using date_time::timestamp,
	alter column hr type float using hr::float;
	
---Adding Constraints--
ALTER TABLE hr_staging
ADD CONSTRAINT fk_hr_patientid FOREIGN KEY (patientid)
REFERENCES demography_staging(patientid);


----Checking for Duplicate rows in hr_staging table---

SELECT patientid, date_time, hr, COUNT(*) AS count
FROM hr_staging
GROUP BY patientid,date_time, hr
HAVING COUNT(*) > 1; /* Has 69231 rows duplicates detected */


-----Removing Duplicates-------
with ranked_hr as (
  select ctid,
         row_number() over (
           partition by patientid, date_time, hr
           order by date_time
         ) as row_num
  from hr_staging
)
delete from hr_staging
where ctid in (
  select ctid
  from ranked_hr
  where row_num > 1
);


-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
--Staging for ibi ---
Create table ibi_staging as (
	Select * from ibi
);

Select * from ibi_staging;
---Column name changes---
Alter table ibi_staging rename column timeof to date_time;
	
---Altering the Datatype---
Alter table ibi_staging
	alter column patientid type integer using patientid::integer,
	alter column date_time type timestamp(6) using date_time::timestamp(6),
	alter column ibi type float using ibi::float;
	
---Adding Constraints--
ALTER TABLE ibi_staging
ADD CONSTRAINT fk_ibi_patientid FOREIGN KEY (patientid)
REFERENCES demography_staging(patientid);


----Checking for Duplicate rows in ibi_staging table---

SELECT patientid, date_time, ibi, COUNT(*) AS count
FROM ibi_staging
GROUP BY patientid,date_time, ibi
HAVING COUNT(*) > 1;  /* No duplicate rows */


-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
--Staging for temp ---
Create table temp_staging as (
	Select * from temperature
);

Select * from temp_staging;
---Column name changes---
alter table temp_staging rename column timeof to date_time;
alter table temp_staging rename column temparature to temperature;	
---Altering the Datatype---
Alter table temp_staging
	alter column patientid type integer using patientid::integer,
	alter column date_time type timestamp(3) using date_time::timestamp(3),
	alter column temperature type float using temperature::float;
	
---Adding Constraints--
ALTER TABLE temp_staging
ADD CONSTRAINT fk_temp_patientid FOREIGN KEY (patientid)
REFERENCES demography_staging(patientid);


----Checking for Duplicate rows in temp_staging table---
SELECT patientid, date_time, temperature, COUNT(*) AS count
FROM temp_staging
GROUP BY patientid,date_time, temperature
HAVING COUNT(*) > 1; /* Only patientid 15 has 192 dupicate rows on '2020-07-24' */

-----Removing Duplicates----------
with ranked_temp as (
  select ctid,
         row_number() over (
           partition by patientid, date_time, temperature
           order by date_time
         ) as row_num
  from temp_staging
)
delete from temp_staging
where ctid in (
  select ctid
  from ranked_temp
  where row_num > 1
);


-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
----Dropping the Original Tables----
Drop table demography;
Drop table dexcom;
Drop table eda;
Drop table foodlog;
Drop table hr;
Drop table ibi;
Drop table temperature;


------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
--- Renaming the table name-----
ALTER TABLE demography_staging RENAME TO demography;
ALTER TABLE dexcom_staging RENAME TO dexcom;
ALTER TABLE eda_staging RENAME TO eda;
ALTER TABLE foodlog_staging RENAME TO foodlog;
ALTER TABLE hr_staging RENAME TO heartrate;
ALTER TABLE ibi_staging RENAME TO  ibinterval;
ALTER TABLE temp_staging RENAME TO temperature;

