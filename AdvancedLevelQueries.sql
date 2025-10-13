---------------------------Advanced Level------------------------

--Q1. Create a trigger to alert when a patients fasting glucose level is excedding the threshold------
/* Create summary table to store daily fasting glucose averages */
Create Table daily_fasting_summary (
  patientid Integer,
  fasting_day Date,
  avg_fasting_glucose Numeric,
  warning Text
);

/* Create trigger to raise warning if threshold is exceeded */
Create Or Replace Function check_fasting_threshold()
Returns Trigger As $$
Begin
  If New.avg_fasting_glucose > 140 Then
    Raise Warning 'Patient % has high fasting glucose on %',New.patientid, New.avg_fasting_glucose;
  End If;
  Return New;
End;
$$ Language plpgsql;

/* trigger Function */
Create Trigger trigger_fasting_warning
Before Insert On daily_fasting_summary
For Each Row
Execute Function check_fasting_threshold();

/* Insert action */
Insert into daily_fasting_summary (patientid, fasting_day, avg_fasting_glucose)
values (1, '2025-10-03', 145.2);

Select * from daily_fasting_summary;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q2. Create a stored Procedure to alert if a high risk food is logged------
Create Or Replace Procedure check_logged_food_risk(p_patientid Integer, p_date Date)
Language plpgsql As $$
Declare
  v_food Text;
  v_risk_item Text;
  high_risk_foods Text[] := Array[ /* := is assignment operatoe used inside declare block */
    'cake', 'soda', 'white rice', 'ice cream', 'sweet', 'pastry', 'milkshake', 'chocolate', 'donut'];
Begin
  -- Loop through each logged food entry for the patient on the given date
  For v_food In
    Select logged_food
    From foodlog
    Where patientid = p_patientid
      And Date(time_begin) = p_date
      And logged_food Is Not Null
  Loop
    -- Loop through each high-risk item and check for match
    For v_risk_item In Select Unnest(high_risk_foods)/* Unnest function used to bresk the array into single item */
    Loop
      If Lower(v_food) Like '%' || Lower(v_risk_item) || '%' Then
        Raise Notice 'High-risk food logged by patient % on %: %',p_patientid, p_date, v_food;
        Exit; -- Stop checking once a match is found
      End If;
    End Loop;
  End Loop;
End;
$$;


---Inserting values into the existing foodlog table to check the stored procedure----
/* Inserting patient into demography first, so that we can enter values in food log as 
foodlog references primary key of demography in our database design */
Insert into demography (patientid, gender, hba1c)
values (17, 'MALE', 5.6);
       
/* Now inserting vlaues into foodlog to check the procedure behavior */	   
Insert into foodlog (patientid, time_begin, logged_food)
values (17, '2025-10-03 08:30', 'white rice'),
       (17, '2025-10-03 12:45', 'grilled chicken'),
       (17, '2025-10-03 19:00', 'ice cream');
/* Calling the  Procedure */
Call check_logged_food_risk(17, '2025-10-03');


--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------


--Q3. What is the average heart rate and average glucose of patients per hour? Raise a flag when they have out of range values.
DROP FUNCTION IF EXISTS get_hourly_hr_glucose(integer);

CREATE INDEX idx_heartrate_hourly ON heartrate(patientid, date_trunc('hour', date_time));
CREATE INDEX idx_dexcom_hourly ON dexcom(patientid, date_trunc('hour', date_time));

---Ceating user defined function to calculate hourly hr, glucose with clinical flag
CREATE OR REPLACE FUNCTION get_hourly_hr_glucose(p_patientid INTEGER DEFAULT NULL)
RETURNS TABLE (
  new_patientid INTEGER,
  hour_bucket TIMESTAMP,
  avg_hr NUMERIC,
  avg_glucose NUMERIC,
  clinical_flag TEXT
) AS $$
BEGIN
  RETURN QUERY
  WITH hr_hourly AS (
    SELECT
      patientid,
      date_trunc('hour', date_time) AS hour_bucket,
      ROUND(AVG(hr)::numeric,2) AS avg_hr
    FROM heartrate
    WHERE p_patientid IS NULL OR patientid = p_patientid
    GROUP BY patientid, hour_bucket
  ),
  glucose_hourly AS (
    SELECT
      patientid,
      date_trunc('hour', date_time) AS hour_bucket,
      ROUND(AVG(glucosevalue)::numeric,2) AS avg_glucose
    FROM dexcom
    WHERE p_patientid IS NULL OR patientid = p_patientid
    GROUP BY patientid, hour_bucket
  )
  SELECT
    COALESCE(h.patientid, g.patientid) AS new_patientid,
    COALESCE(h.hour_bucket, g.hour_bucket) AS hour_bucket,
    h.avg_hr,
    g.avg_glucose,
    CASE 
      WHEN (h.avg_hr IS NULL AND g.avg_glucose IS NULL) THEN 'No Data'
      WHEN h.avg_hr < 60 THEN 'Bradycardia (Low HR)'
      WHEN h.avg_hr > 100 THEN 'Tachycardia (High HR)'
      WHEN g.avg_glucose < 70 THEN 'Hypoglycemia (Low Glucose)'
      WHEN g.avg_glucose > 180 THEN 'Hyperglycemia (High Glucose)'
      ELSE 'Normal Range'
    END AS clinical_flag
  FROM hr_hourly h
  FULL JOIN glucose_hourly g
    ON h.patientid = g.patientid
   AND h.hour_bucket = g.hour_bucket
  ORDER BY new_patientid, hour_bucket;
END;
$$ LANGUAGE plpgsql;

--For all patient without passing parameter
select * from get_hourly_hr_glucose();
---To view one singlepatient by passing id as a parameter
select * from get_hourly_hr_glucose(3);

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q4. Which patients show the most variability in heart rate and glucose levels?

--Creating function
CREATE OR REPLACE FUNCTION get_patient_variability()
RETURNS TABLE (
  pv_patientid INTEGER,
  hr_stddev NUMERIC,
  glucose_stddev NUMERIC,
  hr_cv NUMERIC,
  glucose_cv NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    new_patientid,
    ROUND(STDDEV(avg_hr)::NUMERIC,2),
    ROUND(STDDEV(avg_glucose),2),
    ROUND((STDDEV(avg_hr) / AVG(avg_hr))::NUMERIC, 2),
    ROUND(STDDEV(avg_glucose) / AVG(avg_glucose), 2)
  FROM get_hourly_hr_glucose()
  GROUP BY new_patientid;
END;
$$ LANGUAGE plpgsql;

--Viewing the result for heartrate
SELECT * FROM get_patient_variability()
ORDER BY hr_stddev DESC;

--Viewing the result for glucose
SELECT * FROM get_patient_variability()
ORDER BY glucose_stddev DESC;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q5. Patient vs Status breakdown—who’s showing recurring stress using crosstab function(Pivotting)

--Enabling the table function extension to avial the crosstab function for pivotting. 
--Because postgresql doesn't have any builtin function to pitvot the table explicitly.
create extension if not exists tablefunc;

--Now we are going to create a Source query and category query to use crosstab()
/*  Purpose:
The source query provides the raw data that will be pivoted. It must return three columns in this exact order:
- Row identifier → the entity you want to pivot by (e.g., patientid)
- Category → the values that will become column headers (e.g., 'Normal', 'Stress Spike')
- Value → the metric to fill into the pivoted cells (e.g., count(*) or hour_count)


This query says: “For each patient, count how many hours they spent in each status.”


2. Category Query: What columns to create
Purpose:
The category query defines which values from the second column of the source query should become columns in the final pivoted table.
Example:
values ('Normal'), ('Stress Spike')


This tells crosstab() to create two columns: one for 'Normal' and one for 'Stress Spike'.  

Syntax:
select * from crosstab(
  $$ <source_query> $$,
  $$ <category_query> $$
) as <alias>(<row_id_type>, <column1_type>, <column2_type>, ...)
*/

/*
-- Crosstab query: pivot patientid vs status
select * from crosstab(
  $$
  with hr_hourly as (
    select 
      patientid,
      date_trunc('hour', date_time) as hour_start,
      avg(hr) as avg_hr
    from heartrate
    group by patientid, date_trunc('hour', date_time)
  ),
  summary as (
    select 
      dx.patientid,
      case
        when avg(dx.glucosevalue) > 180 and avg(h.avg_hr) > 100 then 'Stress Spike'
        else 'Normal'
      end as status
    from dexcom dx
    left join hr_hourly h
      on dx.patientid = h.patientid
      and dx.date_time >= h.hour_start
      and dx.date_time < h.hour_start + interval '1 hour'
    group by dx.patientid, date_trunc('day', dx.date_time), extract(hour from dx.date_time)
  )
  select patientid, status, count(*) as hour_count
  from summary
  group by patientid, status
  order by patientid, status
  $$,
  $$values ('Normal'), ('Stress Spike')$$
) as ct(patientid text, normal_hours int, stress_spike_hours int); */
--No such stress spikes hours in the dataset.


-- Use crosstab() to pivot recurring stress spike counts per patient-hour
select * from crosstab(
  $$

  --Aggregate heart rate data into hourly averages
  with hr_hourly as (
    select 
      patientid,
      date_trunc('hour', date_time) as hour_start,
      avg(hr) as avg_hr
    from heartrate
    group by patientid, date_trunc('hour', date_time)
  ),

  --Classify each glucose+HR hour as 'Stress Spike' or 'Normal'
  stress_flags as (
    select 
      dx.patientid,
      date_trunc('day', dx.date_time)::date as day, -- Extract date
      extract(hour from dx.date_time)::int as hour, -- Extract hour
      case
        when avg(dx.glucosevalue) > 180 and avg(h.avg_hr) > 100 then 'Stress Spike'
        else 'Normal'
      end as status
    from dexcom dx
    left join hr_hourly h
      on dx.patientid = h.patientid
      and dx.date_time >= h.hour_start
      and dx.date_time < h.hour_start + interval '1 hour'
    group by dx.patientid, date_trunc('day', dx.date_time), extract(hour from dx.date_time)
  ),

  --Count how many distinct days each patient had a 'Stress Spike' at each hour
  recurring as (
    select 
      patientid,
      hour,
      status,
      count(distinct day) as recurring_count
    from stress_flags
    where status = 'Stress Spike'
    group by patientid, hour, status
  )

  --Prepare source query for crosstab
  -- Combine patientid and hour into a single row identifier
  select 
    patientid || '-' || hour as patient_hour, -- Row ID for crosstab
    status,                                   -- Category to pivot
    recurring_count                           -- Value to fill
  from recurring
  order by patient_hour, status

  $$,

  --Define fixed categories to pivot (only 'Stress Spike' in this case)
  $$values ('Stress Spike')$$

) as ct(
  patient_hour text,          -- Row ID: patientid-hour combo
  stress_spike_days int       -- Number of days with stress spike at that hour
);

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q6. How to find the average value of the three physiological signals EDA, Heart rate and 
------Temperature per hour for the three different glucose ranges?
CREATE EXTENSION IF NOT EXISTS tablefunc;
DROP TABLE IF EXISTS joined_signals_hourly;

--Joining and Filtering data's from multiple tables
CREATE TEMP TABLE joined_signals_hourly AS
WITH
  hr_hourly AS (
    SELECT
      patientid,
      date_trunc('hour', date_time) AS hour_bin,
      AVG(hr) AS avg_hr
    FROM heartrate
    GROUP BY patientid, hour_bin
  ),
  eda_hourly AS (
    SELECT
      patientid,
      date_trunc('hour', date_time) AS hour_bin,
      AVG(eda) AS avg_eda
    FROM eda
    GROUP BY patientid, hour_bin
  ),
  temp_hourly AS (
    SELECT
      patientid,
      date_trunc('hour', date_time) AS hour_bin,
      AVG(temperature) AS avg_temp
    FROM temperature
    GROUP BY patientid, hour_bin
  ),
  glucose_hourly AS (
    SELECT
      patientid,
      date_trunc('hour', date_time) AS hour_bin,
      AVG(glucosevalue) AS avg_glucose
    FROM dexcom
    GROUP BY patientid, hour_bin
  )
SELECT 
  g.patientid,
  g.hour_bin,
  g.avg_glucose,
  h.avg_hr,
  e.avg_eda,
  t.avg_temp,
  CASE 
      WHEN g.avg_glucose < 70 THEN 'Low'
      WHEN g.avg_glucose BETWEEN 70 AND 140 THEN 'Normal'
      ELSE 'High'
  END AS glucose_range
FROM glucose_hourly g
LEFT JOIN hr_hourly h ON g.patientid = h.patientid AND g.hour_bin = h.hour_bin
LEFT JOIN eda_hourly e ON g.patientid = e.patientid AND g.hour_bin = e.hour_bin
LEFT JOIN temp_hourly t ON g.patientid = t.patientid AND g.hour_bin = t.hour_bin;

--Pivotting
SELECT *
FROM crosstab(
  $$
  SELECT
      glucose_range,
      'EDA' AS metric,
      ROUND((AVG(avg_eda))::NUMERIC,2) AS mean_value
  FROM joined_signals_hourly
  GROUP BY glucose_range
  UNION ALL
  SELECT glucose_range, 'HR', ROUND((AVG(avg_hr))::NUMERIC,2)
  FROM joined_signals_hourly
  GROUP BY glucose_range
  UNION ALL
  SELECT glucose_range, 'Temp', ROUND((AVG(avg_temp))::NUMERIC,2)
  FROM joined_signals_hourly
  GROUP BY glucose_range
  ORDER BY 1,2
  $$,
  $$ VALUES ('EDA'), ('HR'), ('Temp') $$
) AS ct(glucose_range TEXT, eda NUMERIC, hr NUMERIC, temp NUMERIC);

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q7. Using Grouping sets function to check eda and 
-----temperature spike with glucose level in hourly basis for all the patients.

--Hourly temperature averages
with temp_hourly as (
  select
    patientid,
    date_trunc('hour', date_time) as hour_start,
    round(avg(temperature)::numeric, 2) as avg_temp
  from temperature
  group by patientid, date_trunc('hour', date_time)
),

--Hourly EDA averages
eda_hourly as (
  select
    patientid,
    date_trunc('hour', date_time) as hour_start,
    round(avg(eda)::numeric, 2) as avg_eda
  from eda
  group by patientid, date_trunc('hour', date_time)
),

--Join dexcom with temp and eda to classify spikes
multi_summary as (
  select
    dx.patientid,
    date_trunc('day', dx.date_time)::date as day,
    extract(hour from dx.date_time)::int as hour,
    round(avg(dx.glucosevalue)::numeric, 2) as avg_glucose,
    round(avg(t.avg_temp)::numeric, 2) as avg_temp,
    round(avg(e.avg_eda)::numeric, 2) as avg_eda,
    case
      when avg(dx.glucosevalue) > 180 and avg(t.avg_temp) > 99 and avg(e.avg_eda) > 0.5 then 'Stress Spike'
      when avg(dx.glucosevalue) > 180 then 'Glucose Spike'
      when avg(t.avg_temp) > 99 then 'Temp Spike'
      when avg(e.avg_eda) > 0.5 then 'EDA Spike'
      else 'Normal'
    end as status
  from dexcom dx
  left join temp_hourly t
    on dx.patientid = t.patientid
    and dx.date_time >= t.hour_start
    and dx.date_time < t.hour_start + interval '1 hour'
  left join eda_hourly e
    on dx.patientid = e.patientid
    and dx.date_time >= e.hour_start
    and dx.date_time < e.hour_start + interval '1 hour'
  group by dx.patientid, date_trunc('day', dx.date_time), extract(hour from dx.date_time)
)

--GROUPING SETS summary with all three metrics
select
  patientid,
  hour,
  round(avg(avg_glucose)::numeric, 2) as avg_glucose,
  round(avg(avg_temp)::numeric, 2) as avg_temp,
  round(avg(avg_eda)::numeric, 2) as avg_eda,
  count(*) filter (where status = 'Stress Spike') as stress_spike_count,
  count(*) filter (where status = 'Glucose Spike') as glucose_spike_count,
  count(*) filter (where status = 'Temp Spike') as temp_spike_count,
  count(*) filter (where status = 'EDA Spike') as eda_spike_count
from multi_summary
group by grouping sets (
  (patientid, hour),
  (patientid),
  (hour)
)
order by patientid, hour;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q8. Using Recursive CTE - For each patient, how many consecutive hours of elevated
------glucose levels occurred after high-calorie meals, and what was 
------the average glucose during those spike chains?

-- Anchor part : first hour after meal
with recursive postmeal_chain as (
   select
    f.patientid,
    f.time_begin,
    date_trunc('hour', f.time_begin + interval '1 hour') as spike_hour,
    1 as hour_offset,
    dx.glucosevalue
  from foodlog f
  join dexcom dx
    on f.patientid = dx.patientid
    and dx.date_time >= f.time_begin
    and dx.date_time < f.time_begin + interval '1 hour'
  where f.calorie > 300 and dx.glucosevalue > 180

  union all

  -- Recursive part: next hour continues spike
  select
    pc.patientid,
    pc.time_begin,
    date_trunc('hour', pc.spike_hour + interval '1 hour') as spike_hour,
    pc.hour_offset + 1,
    dx.glucosevalue
  from postmeal_chain pc
  join dexcom dx
    on pc.patientid = dx.patientid
    and dx.date_time >= pc.spike_hour + interval '1 hour'
    and dx.date_time < pc.spike_hour + interval '2 hour'
  where dx.glucosevalue > 180
)
select
  patientid,
  time_begin::date as day,
  max(hour_offset) as spike_duration,
  round(avg(glucosevalue), 2) as avg_spike_glucose
from postmeal_chain
group by patientid, time_begin::date
order by patientid, day;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q9. How to calculate the HRV metrics (RMSSD, SDNN, and pNN50) from the IBI reading after glucose crash?

CREATE OR REPLACE FUNCTION get_hrv_after_glucose_crash(
    patient_id INTEGER,
    hours_forward INTEGER DEFAULT 1
)
RETURNS TABLE (
    glucose_crash_time TIMESTAMPTZ,
    glucosevalue NUMERIC,
    rmssd NUMERIC,
    sdnn NUMERIC,
    pnn50 NUMERIC
)
LANGUAGE sql
AS $$
WITH crashes AS (
    SELECT date_time AS glucose_crash_time, glucosevalue
    FROM dexcom
    WHERE glucosevalue < 70
      AND patientid = patient_id
),
ibi_window AS (
    SELECT 
        c.glucose_crash_time,
        c.glucosevalue,
        i.date_time,
        i.ibi
    FROM crashes c
    JOIN ibinterval i ON i.patientid = patient_id
                     AND i.date_time BETWEEN c.glucose_crash_time AND c.glucose_crash_time + interval '1 hour' * hours_forward
),
ibi_diff AS (
    SELECT 
        glucose_crash_time,
        glucosevalue,
        ibi,
        ibi - LAG(ibi) OVER (PARTITION BY glucose_crash_time ORDER BY date_time) AS ibi_diff
    FROM ibi_window
)
SELECT 
    glucose_crash_time,
    glucosevalue,
    ROUND(SQRT(AVG(POWER(ibi_diff, 2)))::NUMERIC, 3) AS rmssd,
    ROUND(STDDEV(ibi)::NUMERIC, 3) AS sdnn,
    ROUND(100.0 * SUM(CASE WHEN ABS(ibi_diff) > 0.05 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(ibi_diff), 0), 2) AS pnn50
FROM ibi_diff
GROUP BY glucose_crash_time, glucosevalue
ORDER BY glucose_crash_time DESC;
$$;

--Calling the function
SELECT * FROM get_hrv_after_glucose_crash(1);

/*--PatientID 1, 8 & 16 alone returns values. Because other PatientIDs with glucose crash 3,4,7,10 & 15 
Don't have enough ibi readings to compute HRV.*/


--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q10. Using a Cursor-driven Procedure, Loop through each patient’s meal 
------and check whether any nutrients exceed its threshold or not and 
------view the glucose spike if it’s exceeded.

-- Create a stored procedure that returns a cursor named 'alert_cursor'
create or replace procedure get_nutrient_alerts(inout alert_cursor refcursor)
language plpgsql
as $$
begin

  -- Open the cursor to return a result set of nutrient overloads with glucose spike info
  open alert_cursor for

    -- Main SELECT: aggregates nutrient intake per patient per hour
    select
      f.patientid,                          -- Patient identifier
      f.hour_start,                         -- Hourly window (truncated from meal time)
      sum(f.total_carb) as total_carb,      -- Total carbs consumed in that hour
      sum(f.dietary_fiber) as dietary_fiber,-- Total fiber consumed in that hour
      sum(f.sugar) as sugar,                -- Total sugar consumed in that hour
      sum(f.protein) as protein,            -- Total protein consumed in that hour
      sum(f.total_fat) as total_fat,        -- Total fat consumed in that hour

      -- Max glucose value recorded in that hour
      max(dx.glucosevalue) as max_glucose,

      -- Flag if glucose spike occurred (>180 mg/dL)
      case
        when max(dx.glucosevalue) > 180 then true
        else false
      end as glucose_spike

    -- Subquery: normalize foodlog timestamps to hourly blocks
    from (
      select
        patientid,
        date_trunc('hour', time_begin) as hour_start, -- Round meal time to the hour
        total_carb,
        dietary_fiber,
        sugar,
        protein,
        total_fat
      from foodlog
    ) f

    -- Join with Dexcom readings to check glucose values in the same hour
    left join dexcom dx
      on f.patientid = dx.patientid
      and dx.date_time >= f.hour_start
      and dx.date_time < f.hour_start + interval '1 hour'

    -- Group by patient and hour to aggregate nutrient intake and glucose
    group by f.patientid, f.hour_start

    -- Only return rows where at least one nutrient exceeds its threshold
    having sum(f.total_carb) > 60
        or sum(f.dietary_fiber) > 25
        or sum(f.sugar) > 30
        or sum(f.protein) > 40
        or sum(f.total_fat) > 20;

end;
$$;

--To view the procedure result, we have to execute these 4 lines one after the other in a single transaction.
begin;
call get_nutrient_alerts('alert_cursor');
fetch all from alert_cursor;
commit;

--To rollback
rollback;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q11. Using Range Partitioning, we are partitioning the Foodlog table into three different available years.(2019,2020 and 2025)

--To view patient count in three different years:
select extract(year from time_begin) as year, count(distinct patientid) as patient_count
from foodlog where time_begin is not null group by year;

-------Range Partition----------
-- Creating the Parent table for the partition 
--*****Column names, datatype and column order should be same as the existing table for Partition *****  
create table foodlog_partitioned (
  time_begin timestamp,
  time_end time,
  logged_food text,
  quantity varchar,
  unit text,
  searched_food text,
  calorie numeric,
  total_carb numeric,
  dietary_fiber numeric,
  sugar numeric,
  protein numeric,
  total_fat numeric,
  patientid int
) partition by range (time_begin);


-- Year-based partitions
create table foodlog_2019 partition of foodlog_partitioned
  for values from ('2019-01-01') to ('2020-01-01');

create table foodlog_2020 partition of foodlog_partitioned
  for values from ('2020-01-01') to ('2021-01-01');

create table foodlog_2025 partition of foodlog_partitioned
  for values from ('2025-01-01') to ('2026-01-01');

-- Default partition for nulls
create table foodlog_nulls partition of foodlog_partitioned default;

--Copy data from the existing foodlog to foodlog_partitioned
insert into foodlog_partitioned
select * from foodlog;

---Validating the Partition Distribution 
-- Row count per partition
select '2019' as year, count(*) from foodlog_2019
union all
select '2020', count(*) from foodlog_2020
union all
select '2025', count(*) from foodlog_2025
union all
select 'nulls', count(*) from foodlog_nulls;

--To view the table list in the postgres  information schema:
Select table_name from information_schema.tables
where table_name like 'foodlog_%';


--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q12. Using After insert trigger, how can we automatically flag a patient’s temperature 
------as unstable if their daily variability exceeds 0.5°C ?

--Creating the table 
create table temp_variability_alert (
    patientid integer,
    temperature numeric,
    date_time timestamp,
    status text
);


--Creating the Trigger Function
create or replace function flag_hourly_temperature_sd()
returns trigger as $$
declare
    temp_hour_start timestamp;
    temp_hour_end timestamp;
    temp_sd numeric;
begin
    temp_hour_start := date_trunc('hour', new.date_time);
    temp_hour_end := temp_hour_start + interval '1 hour';

    select stddev_pop(temperature)
    into temp_sd
    from temp_variability_alert
    where patientid = new.patientid
      and date_time >= temp_hour_start
      and date_time < temp_hour_end;

    if temp_sd > 0.5 then
        update temp_variability_alert
        set status = 'unstable'
        where ctid = new.ctid;
    else
        update temp_variability_alert
        set status = 'stable'
        where ctid = new.ctid;
    end if;

    return null;
end;
$$ language plpgsql;


--Creating the trigger
create trigger trg_flag_hourly_temperature_sd
after insert on temp_variability_alert
for each row
execute function flag_hourly_temperature_sd();

--Inserting values to the demography and temperature tables to check the trigger
insert into demography (patientid, gender, hba1c)
values(19, 'FEMALE', 6.3);


insert into temp_variability_alert (patientid, temperature, date_time)
values
(19, 36.8, '2025-10-07 08:05:00'),
(19, 37.0, '2025-10-07 08:15:00'),
(19, 37.4, '2025-10-07 08:35:00'),
(19, 36.9, '2025-10-07 08:45:00'),
(19, 37.6, '2025-10-07 08:55:00');

---To view the change when new patienid is added with respective values
Select * from temp_variability_alert;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q13: Find the food intake chain of each patient before glucose hike.
WITH RECURSIVE food_chain AS (
    -- Anchor: meals within 2 hours before a glucose spike
    SELECT DISTINCT ON (f.time_begin)
			d.patientid, date_trunc('minute', d.date_time) AS spike_time,
           f.time_begin AS food_time,
           f.logged_food, f.calorie, f.sugar, f.dietary_fiber,
           1 AS depth,
           ARRAY[f.time_begin] AS path
    FROM dexcom d
    JOIN foodlog f ON f.patientid = d.patientid
    WHERE d.glucosevalue > 140
      AND f.time_begin < d.date_time
      AND f.time_begin >= d.date_time - interval '2 hours'

    UNION ALL

    -- Recursive step: earlier meals within 2 hours of previous meal
    SELECT fc.patientid, fc.spike_time,
           f.time_begin AS food_time,
           f.logged_food, f.calorie, f.sugar, f.dietary_fiber,
           fc.depth + 1,
           fc.path || f.time_begin
    FROM food_chain fc
    JOIN foodlog f ON f.patientid = fc.patientid
    WHERE f.time_begin < fc.food_time
      AND f.time_begin >= fc.food_time - interval '2 hours'
      AND NOT f.time_begin = ANY(fc.path)
),

-- Bin dexcom readings to reduce volume
binned_glucose AS (
    SELECT patientid,
           date_trunc('minute', date_time) AS glucose_minute,
           AVG(glucosevalue) AS avg_glucose
    FROM dexcom
    GROUP BY patientid, date_trunc('minute', date_time)
),

-- Identify glucose spikes
glucose_spikes AS (
    SELECT patientid, glucose_minute AS spike_time
    FROM binned_glucose
    WHERE avg_glucose > 140
)

-- Final output
SELECT patientid, spike_time, food_time, logged_food,
       ROUND(calorie, 2) AS calorie,
       ROUND(sugar, 2) AS sugar,
       ROUND(dietary_fiber, 2) AS dietary_fiber,
       depth
FROM food_chain
ORDER BY patientid, spike_time, depth DESC;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------


--Q14. Creating Meaterialized view for sensor summary with all the biomarkers

/* In the datset dexcom has every 5mins readings, heartrate has every seconds readings, 
temperature and eda has every.25 seconds readings. So To optimize the performance bottleneck,
1. We are writing CTE's to calculate the time as hour adn calculating the mean and stddev of respective values.
2. joining all the hourly cte's in a materialized view to create summary
*/


create materialized view hourly_sensor_summary as
with dexcom_hourly as ( 
    select patientid,
           date_trunc('hour', date_time) as hour,
           round(avg(glucosevalue)::Numeric, 2) as avg_glucose,
           round(stddev_pop(glucosevalue)::Numeric,2) as sd_glucose
    from dexcom
    group by patientid, date_trunc('hour', date_time)
),
hr_hourly as (
    select patientid,
           date_trunc('hour', date_time) as hour,
           round(avg(hr)::Numeric,2) as avg_hr,
           round(stddev_pop(hr)::Numeric,2) as sd_hr
    from heartrate
    group by patientid, date_trunc('hour', date_time)
),
temp_hourly as (
    select patientid,
           date_trunc('hour', date_time) as hour,
           round(avg(temperature)::Numeric,2) as avg_temp,
           round(stddev_pop(temperature)::Numeric,2) as sd_temp
    from temperature
    group by patientid, date_trunc('hour', date_time)
),
eda_hourly as (
    select patientid,
           date_trunc('hour', date_time) as hour,
           round(avg(eda)::Numeric,2) as avg_eda,
           round(stddev_pop(eda)::Numeric,2) as sd_eda
    from eda
    group by patientid, date_trunc('hour', date_time)
)
select
    d.patientid,
    d.hour,
    d.avg_glucose,
    d.sd_glucose,
    hr.avg_hr,
    hr.sd_hr,
    temp.avg_temp,
    temp.sd_temp,
    eda.avg_eda,
    eda.sd_eda
from dexcom_hourly d
left join hr_hourly hr on d.patientid = hr.patientid and d.hour = hr.hour
left join temp_hourly temp on d.patientid = temp.patientid and d.hour = temp.hour
left join eda_hourly eda on d.patientid = eda.patientid and d.hour = eda.hour;

--To view the Materialized view:
select * from hourly_sensor_summary
order by patientid, hour;

--To refresh Materialized View
/* New data has been inserted, updated, or 
deleted in the underlying tables (e.g., new Dexcom or Empatica readings)
*/
refresh materialized view hourly_sensor_summary;

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--Q15. Using Cursor,Create a Stored Procedure to see Stress-Triggered Glucose Spikes
-- Identify hours where glucose rises sharply while HR spikes.

--As calling the procedure withput creating index is having performance bottleneck with long execution time. 
--So we are creating index on patientid and date_time columns. Heartrate table already having the index for those columns.

--Creating Indexes
create index idx_heartrate_patient_time on heartrate(patientid, date_time); /* already created */

--Creating the procedure
create or replace procedure summarize_hourly_glucose_hr(inout ref refcursor)
language plpgsql
as $$
begin
  open ref for

  --Pre-aggregate heartrate data by patient and hour
  --This reduces join volume and speeds up execution
  with hr_hourly as (
    select
      patientid,
      date_trunc('hour', date_time) as hour_start,
      avg(hr) as avg_hr
    from heartrate
    group by patientid, date_trunc('hour', date_time)
  )

  -- Join dexcom (glucose every 5 mins) with hourly HR summaries
  -- Use range-based join to preserve index usage and align glucose with HR
  select
    dx.patientid,
    date_trunc('day', dx.date_time)::date as day,
    extract(hour from dx.date_time)::int as hour,
    round(avg(dx.glucosevalue)::Numeric,2) as avg_glucose,
    round(avg(h.avg_hr)::Numeric,2) as avg_hr,

    --Flag stress spikes based on glucose and HR thresholds
    case
      when avg(dx.glucosevalue) > 180 and avg(h.avg_hr) > 100 then 'Stress Spike'
      else 'Normal'
    end as status

  from dexcom dx
  left join hr_hourly h
    on dx.patientid = h.patientid
    and dx.date_time >= h.hour_start
    and dx.date_time < h.hour_start + interval '1 hour'

  --Group by patient, day, and hour for hourly summaries
  group by dx.patientid, date_trunc('day', dx.date_time), extract(hour from dx.date_time)

  --Order results for dashboard or classroom narration
  order by dx.patientid, day, hour;

end;
$$;

--To view the cursor, executing these lines one by one in a same transaction--
begin;
call summarize_hourly_glucose_hr('mycursor');
fetch all from mycursor;
commit;

--To rollback the transaction
rollback;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

















