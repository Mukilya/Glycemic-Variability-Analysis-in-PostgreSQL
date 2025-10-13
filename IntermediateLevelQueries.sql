------------Intermediate Level---------------------------------------

--Q1. Calculate the daily average glucose per patient 
With daily_avg as (                         
  Select 
    patientid,
    date(date_time) as day,
    round(avg(glucosevalue), 1) as avg_glucose
  from dexcom
  group by patientid, date(date_time)
  order by patientid, date(date_time)
)
Select * from daily_avg;


--Q2. Calculate the daily average glucose per day per patient and flag highest day 
With flag_high_avg as (                        
  Select 
    patientid,
    date(date_time) as day,
    round(avg(glucosevalue), 1) as avg_glucose,
	case when round(avg(glucosevalue),2) > 140 then 1 else 0 end as high_flag
  from dexcom
  group by patientid, date(date_time)
  )
Select patientid,count(*) as total_days,
count(*) filter (where high_flag = 1) as high_glucose_days
from flag_high_avg
group by patientid
order by high_glucose_days desc;


--Q3.Count the Missing transmission time for  each the patient
With missing_transmissions as (
  Select patientid, date_time, count(*) as missing_count
  from dexcom
  where transmittertime is null
  group by patientid, date_time
)
Select * from missing_transmissions
order by missing_count desc;


--Q4.Summarize the patients daily Nutrient summary
With daily_nutrient_summary as (
  Select 
    patientid,
    Date(time_begin) as day,
    sum(calorie) as total_calories,
    sum(sugar) as total_sugar,
    sum(protein) as total_protein,
	sum(total_carb) as total_carbs,
	sum(total_fat) as total_fats,
	sum(dietary_fiber) as total_fiber
  from foodlog
  group by patientid, Date(time_begin)
)
Select * from daily_nutrient_summary
order by patientid, day;

--Q5.Time difference between consecutive glucose readings---

select patientid, date_time,
       date_time - lag(date_time) over (partition by patientid order by date_time) as diff_time
from dexcom;


--Q6. Identifying  Daily Average Fasting Glucose Level for all patients
With fasting_candidates As (  /* Identifying fasting window time */
  Select d.patientid, d.date_time, d.glucosevalue
  From dexcom d
  Where Extract(Hour From d.date_time) Between 4 And 7
),
recent_food As (  /* Selcting values from foodlog */
  Select patientid, time_begin
  From foodlog
),
fasting_glucose As ( /* Joining foodlog vlaues with fasting time window */
  Select fc.*
  From fasting_candidates fc
  Left Join recent_food rf On fc.patientid = rf.patientid
    And rf.time_begin Between fc.date_time - Interval '8 hours' And fc.date_time
  Where rf.time_begin Is Null
)
Select
  patientid,
  Date(date_time) As fasting_day,
  Round(Avg(glucosevalue), 2) As avg_fasting_glucose
From fasting_glucose
Group By patientid, Date(date_time)
Order By patientid, fasting_day;



--Q7. Calculating rolling average of glucose for all the patients
with daily_glucose as (              /* Calculating daily average */
  select patientid, date(date_time) as day, 
  round(avg(glucosevalue), 2) as daily_avg
  from dexcom
  group by patientid, date(date_time)
)
select patientid,day,daily_avg,  /* calculating rolling average */
round(avg(daily_avg) over (partition by patientid order by day rows between 2 preceding and current row), 2) as rolling_avg_glucose
from daily_glucose
order by patientid, day;

--Q8. Creating range views for the glucose average level for the whole time window
select patientid, count(distinct(extract(day from date_time)))as Total_days,
round(avg(glucosevalue),2)as avg_glucose,
case 
    when round(avg(glucosevalue),2) < 70 then 'Hypoglycemia'
    when round(avg(glucosevalue),2) between 70 and 140 then 'Normal'
    else 'Hyperglycemia'
end as glucose_range
from dexcom
group by patientid;



--Q9.Finding how many days consecutive days the patient glucose level is exceeding the threshold level
With daily_avg as ( /* calculating daily average per patient  */                        
  Select 
    patientid,
    date(date_time) as day,
    round(avg(glucosevalue), 1) as avg_glucose
  from dexcom
  group by patientid, date(date_time)
),
flagged_days as ( /* flagging who is having higher the threshol 140 */ 
  Select 
    patientid,
    day,
    avg_glucose,
    case when avg_glucose > 140 then 1 else 0 end as is_high
  from daily_avg
),
sequenced as ( /* finding the steak break  */ 
  Select *,
    case when is_high = 1 and lag(day) over (partition by patientid order by day) = day - INTERVAL '1 day'
      then 0 else 1
    end as streak_break
  from flagged_days
),
grouped as ( /* Counting the streak */ 
  Select *,
    sum(streak_break) over (partition by patientid order by day) as streak_group
  from sequenced
)
Select patientid, streak_group, count(*) as consecutive_high_days
from grouped
where is_high = 1
group by patientid, streak_group
having count(*) >= 2
order by patientid, consecutive_high_days desc;



--Q10. Identifying  the daily Meal Window wise glucose level for all Patients

With meal_windows as (           /* calculating Meal window*/
  Select patientid,time_begin,logged_food,
    time_begin - INTERVAL '30 minutes' as pre_meal_start,
    time_begin + INTERVAL '1 hour' as post_meal_start,
    time_begin + INTERVAL '2 hours' as post_meal_end
  from foodlog
),
get_glucose as(            /* Mapping glucose vlaue for the mael window types */
 Select mw.patientid,mw.logged_food,
  mw.time_begin as meal_time,
  dx.date_time as glucose_time,
  dx.glucosevalue,
  Case 
    when dx.date_time between mw.pre_meal_start and mw.time_begin then 'Pre-Meal'
    when dx.date_time between mw.post_meal_start and mw.post_meal_end then 'Post-Meal'
    else 'Outside Meal Window'
  end as glucose_context
from meal_windows mw
join dexcom dx on mw.patientid = dx.patientid
where dx.date_time between mw.pre_meal_start and mw.post_meal_end
)
Select
  patientid,
  DATE(meal_time) as meal_day,
  glucose_context,
  ROUND(avg(glucosevalue), 1) as avg_glucose
from get_glucose
group by patientid, DATE(meal_time), glucose_context
order by patientid, meal_day, glucose_context;


--Q11. Creating range view for the heartrate level for the whole timewindow for all pateints

/* As heartrate column has 9384435(9Million)rows, the query execution is taking too long 
while performing join, aggrgation and filtering with respective to time series column. 
So inorder to improve the perfprmance and query execution time, we are creating index here. */

--Index for both patientid and datetime--
create index idx_heartrate_patient_time on heartrate(patientid, date_time); (--index creation took: 4sec 305msec)
/*After creating index also it is taking beyond 2mins and running still so cancelled query
with get_heartrate as(  
	select dx.patientid, 
	count(distinct(extract(day from dx.date_time)))as Total_days,
	round(avg(dx.glucosevalue),2)as avg_glucose, avg(h.hr)as Avg_hr
	from dexcom dx
	join heartrate h on dx.patientid = h.patientid and date(dx.date_time) = date(h.date_time)
	group by dx.patientid
)
select patientid, Total_days,avg_glucose, Avg_hr,
case 
	when Avg_hr< 60 then 'Bradycardia'
    when Avg_hr between 60 and 100 then 'Normal'
    else 'Tachycardia'
  end as hr_status
from get_heartrate
order by patientid;
Select * from heartrate; */

/* The reason here is in dexcom table reading is recorded every 5 mins, 
but in heartrate it is every second(Empatica E4). So we need to bound 
the heartrate timeseries column into hour
*/

-- aggregate heart rate by patient and day
with hr_daily as (
  select 
    patientid,
    date(date_time) as day,
    round(avg(hr)::Numeric, 2) as avg_hr
  from heartrate
  group by patientid, date(date_time)
),

--aggregate glucose by patient and day
glucose_daily as (
  select 
    patientid,
    date(date_time) as day,
    round(avg(glucosevalue), 2) as avg_glucose
  from dexcom
  group by patientid, date(date_time)
),
--join daily summaries and classify heart rate
get_heartrate as (
  select 
    g.patientid,
    count(distinct g.day) as total_days,
    avg(g.avg_glucose) as avg_glucose,
    avg(h.avg_hr) as avg_hr
  from glucose_daily g
  join hr_daily h 
    on g.patientid = h.patientid 
   and g.day = h.day
  group by g.patientid
)
select 
  patientid,
  total_days,
  round(avg_glucose, 2) as avg_glucose,
  round(avg_hr, 2) as avg_hr,
  case 
    when avg_hr < 60 then 'bradycardia'
    when avg_hr between 60 and 100 then 'normal'
    else 'tachycardia'
  end as hr_status
from get_heartrate
order by patientid;

/* Finally we could acheive it in 684msec */



--Q12: Is there any relation between glucose change after meals and the nutrition value of the meals?
WITH ranked_meals AS (
    SELECT *,
		   -- Rank meals from lowest to highest nutrition
           ROW_NUMBER() OVER (
               PARTITION BY patientid
               ORDER BY (protein + dietary_fiber) ASC NULLS LAST
           ) AS rank_low,
		   -- Rank meals from highest to lowest nutrition
           ROW_NUMBER() OVER (
               PARTITION BY patientid
               ORDER BY (protein + dietary_fiber) DESC NULLS LAST
           ) AS rank_high
    FROM foodlog
    WHERE protein IS NOT NULL AND dietary_fiber IS NOT NULL
),
--Select the highest and lowest nutrition meals per patient
selected_meals AS (
    SELECT patientid, time_begin AS meal_time,
           protein, dietary_fiber,
		   -- Label each meal as either highest or lowest nutrition
           CASE 
               WHEN rank_low = 1 THEN 'lowest_nutrition'
               WHEN rank_high = 1 THEN 'highest_nutrition'
           END AS meal_type
    FROM ranked_meals
	-- Only keep meals that are either highest or lowest ranked
    WHERE rank_low = 1 OR rank_high = 1
),

binned_glucose AS (
    SELECT patientid,
           date_trunc('minute', date_time) AS glucose_minute,
           AVG(glucosevalue) AS avg_glucose
    FROM dexcom
    GROUP BY patientid, date_trunc('minute', date_time)
),

meal_glucose_window AS (
    SELECT m.patientid, m.meal_time, m.meal_type,
           m.protein, m.dietary_fiber,
           g_pre.avg_glucose AS pre_meal_glucose,
           g_post.avg_glucose AS post_meal_glucose
    FROM selected_meals m
	-- Get the most recent glucose reading before the meal
    LEFT JOIN LATERAL (
        SELECT avg_glucose
        FROM binned_glucose g
        WHERE g.patientid = m.patientid
          AND g.glucose_minute < m.meal_time
        ORDER BY g.glucose_minute DESC
        LIMIT 1
    ) g_pre ON true
	-- Get the earliest glucose reading within 2 hours after the meal
    LEFT JOIN LATERAL (
        SELECT avg_glucose
        FROM binned_glucose g
        WHERE g.patientid = m.patientid
          AND g.glucose_minute BETWEEN m.meal_time AND m.meal_time + interval '2 hours'
        ORDER BY g.glucose_minute ASC
        LIMIT 1
    ) g_post ON true
)

SELECT patientid, meal_type, meal_time,
       ROUND(pre_meal_glucose, 2) AS pre_meal_glucose,
       ROUND(post_meal_glucose, 2) AS post_meal_glucose,
       ROUND(post_meal_glucose - pre_meal_glucose, 2) AS glucose_change,
       ROUND(protein, 2) AS protein,
       ROUND(dietary_fiber, 2) AS dietary_fiber
FROM meal_glucose_window
WHERE pre_meal_glucose IS NOT NULL AND post_meal_glucose IS NOT NULL
ORDER BY patientid, meal_type DESC;


/*Q13: For each patient-day, what is the correlation between HR and glucose?
*/

select 
	d.patientid, 
	DATE_TRUNC('day', d.date_time) as day,
	corr(d.glucosevalue, h.hr) as glucose_hr_corr 
from
	dexcom d 
join 
	heartrate h on d.patientid = h.patientid and 
	DATE_TRUNC('minute', d.date_time) = DATE_TRUNC('minute', h.date_time)
group by d.patientid, day;

/*Q14: Assign each patient’s EDA into quartiles (low, med-low, med-high, high).*/

select patientid, date_time, eda,
       case ntile(4) over (partition by patientid order by eda)
            when 1 then 'Low'
            when 2 then 'Med-Low'
            when 3 then 'Med-High'
            else 'High' end as stress_bucket
from eda;

/*Q15: If glucose <70, flag recovery as “Fast” (<30 min) or “Slow” (>30 min)*/

with hypo_events as (
 	select patientid, date_time,
         lead(date_time) over (partition by patientid order by date_time) as next_time,
         lead(glucosevalue) over (partition by patientid order by date_time) as next_glucose from dexcom where glucosevalue < 70)
	select patientid, date_time,
       case when next_glucose >= 90 and (next_time - date_time) < INTERVAL '30 minutes'
            then 'Fast Recovery'
            else 'Slow Recovery' end as recovery_flag
from hypo_events;

/*Q16: Assign variability label: glucose STDDEV <20 → Low, 20–40 → Medium, >40 → High.
*/

with daily_var as (
  select 
  	patientid, 
  	DATE(date_time) as day, 
  	STDDEV(glucosevalue) as gv 
  from 
  	dexcom 
  group by patientid, day
  )
select 
  patientid,
  day, 
  round(gv, 2) as GV,
  case 
  	when gv < 20 then 'Low'
    when gv <= 40 then 'Medium'
    else 'High' end as variability_level
from daily_var;

--Q17: Which patients have the highest average calorie intake over any 3-day window
SELECT patientid, time_begin, avg_calories
FROM (
    SELECT 
        patientid,
        time_begin,
        ROUND(AVG(calorie) OVER (
            PARTITION BY patientid 
            ORDER BY time_begin 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS avg_calories
    FROM foodlog
    WHERE calorie IS NOT NULL
) sub
ORDER BY avg_calories DESC
LIMIT 10;


--Q18: Rank foods by frequency of logging within each patient using window functions
SELECT 
    patientid,
    logged_food,
    COUNT(*) AS log_count,
    RANK() OVER (
        PARTITION BY patientid 
        ORDER BY COUNT(*) DESC
    ) AS food_rank
FROM foodlog
GROUP BY patientid, logged_food
ORDER BY patientid, food_rank;

/*Q19: Detect more than 1 hour gaps in glucose logging per patient using LAG() and timestamp difference?
*/
WITH gap_check AS (
    SELECT 
        patientid,
        date_time,
		-- Get the previous glucose reading timestamp for each patient
        LAG(date_time) OVER (
            PARTITION BY patientid 
            ORDER BY date_time
        ) AS previous_time,
		-- Calculate the time gap in minutes between current and previous readings
        ROUND(EXTRACT(EPOCH FROM date_time - LAG(date_time) OVER (
            PARTITION BY patientid 
            ORDER BY date_time
        )) / 60, 2) AS gap_minutes
    FROM dexcom
    WHERE glucosevalue IS NOT NULL
)
--Select rows where the time gap between readings exceeds 60 minutes
SELECT *
FROM gap_check
WHERE gap_minutes > 60;


/*Q20: How can we track short-term temperature trends for each patient using a rolling average?*/

SELECT 
    patientid,
    date_time,
    temperature,
    ROUND(
        AVG(temperature) OVER (
            PARTITION BY patientid
            ORDER BY date_time
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        )::numeric, 2
    ) AS rolling_avg
FROM temperature;

/*Q21:Identify abnormal heart rate spikes per patient using threshold filtering?*/

SELECT 
    patientid,
    date_time,
    hr,
	-- Get the previous heart rate reading for the same patient
    LAG(hr) OVER (
        PARTITION BY patientid
        ORDER BY date_time
    ) AS previous_hr,
	-- Calculate the difference between current and previous heart rate
    ROUND(
        (hr - LAG(hr) OVER (
            PARTITION BY patientid
            ORDER BY date_time
        ))::numeric, 2
    ) AS spike_diff
FROM heartrate
-- Only include readings where heart rate exceeds 120 bpm
WHERE hr > 120
ORDER BY patientid, date_time;


/*Q22:: Are patients with higher HbA1c values showing stronger glucose spikes after breakfast? */
--Identify breakfast times (5am to 10am) from the food log
WITH breakfast_times AS (
    SELECT 
        patientid,
        time_begin AS breakfast_time
    FROM 
        foodlog
    WHERE 
        EXTRACT(HOUR FROM time_begin) BETWEEN 5 AND 10
),
--Calculate glucose spikes within 2 hours after breakfast
glucose_spikes AS (
    SELECT 
        dx.patientid,
        bt.breakfast_time AS timeofbreakfast,
		-- Compute glucose spike as the difference between max and min glucose values
        MAX(dx.glucosevalue) - MIN(dx.glucosevalue) AS glucose_spike
    FROM 
        dexcom dx
    JOIN 
        breakfast_times bt ON dx.patientid = bt.patientid
    WHERE 
		-- Only include glucose readings within 2 hours after breakfast
        dx.date_time > bt.breakfast_time
        AND dx.date_time <= bt.breakfast_time + INTERVAL '2 hours'
    GROUP BY 
        dx.patientid, bt.breakfast_time
)
SELECT 
    gs.patientid,
    d.hba1c,
	-- Categorize HbA1c into diabetes status
	CASE 
        WHEN d.hba1c < 5.7 THEN 'Non-diabetic'
        WHEN d.hba1c >= 5.7 AND d.hba1c < 6.5 THEN 'Pre-diabetic'
        ELSE 'Diabetic'
    END AS hba1c_status,
    gs.glucose_spike,
	(gs.timeofbreakfast)::time
FROM 
    glucose_spikes gs
JOIN 
    demography d ON gs.patientid = d.patientid
ORDER BY 
     gs.glucose_spike DESC;


/*Q23:How to find Count of Gaps per Patient (Glucose Example)  
that suggests device syncing issues?*/

WITH gaps AS (
  SELECT 
    patientid,
    date_time,
	-- Retrieve the timestamp of the previous reading for the same patient
    LAG(date_time) OVER (PARTITION BY patientid ORDER BY date_time) AS prev_time
  FROM dexcom
)
--Count how many gaps exceed 10 minutes for each patient
SELECT 
  patientid,
  COUNT(*) AS gap_count
FROM gaps
-- Only consider rows where a previous reading exists
WHERE prev_time IS NOT NULL
-- Filter for gaps longer than 10 minutes
  AND date_time - prev_time > INTERVAL '10 minutes'
GROUP BY patientid
ORDER BY gap_count DESC;


/*Q24:Compare Daily IBI Variability with Heart Rate*/
-- Compute daily IBI statistics for each patient
WITH daily_ibi AS (
    SELECT
        patientid,
        DATE_TRUNC('day', date_time) AS day,
        ROUND(AVG(ibi)::numeric, 3) AS mean_ibi,
        ROUND(STDDEV_SAMP(ibi)::numeric, 3) AS sd_ibi
    FROM ibinterval
    WHERE ibi IS NOT NULL
    GROUP BY patientid, DATE_TRUNC('day', date_time)
),
-- Compute daily heart rate statistics for each patient
daily_hr AS (
    SELECT
        patientid,
        DATE_TRUNC('day', date_time) AS day,
        ROUND(AVG(hr)::numeric, 2) AS mean_hr,
        ROUND(STDDEV_SAMP(hr)::numeric, 2) AS sd_hr
    FROM heartrate
    WHERE hr IS NOT NULL
    GROUP BY patientid, DATE_TRUNC('day', date_time)
)
-- Join daily IBI and HR stats, and compute correlation between mean IBI and mean HR
SELECT
    i.patientid,
    i.day,
    i.mean_ibi,
    i.sd_ibi,
    h.mean_hr,
    h.sd_hr,
    ROUND(
        CORR(i.mean_ibi::double precision, h.mean_hr::double precision)
        OVER (PARTITION BY i.patientid)::numeric,
        3
    ) AS ibi_hr_corr
FROM daily_ibi i
JOIN daily_hr h
  ON i.patientid = h.patientid AND i.day = h.day
ORDER BY i.patientid, i.day;

/*Q25:Join eda and ibi on patientid and nearest timestamp (within 1 minute)
to find the average paired eda–ibi correlation per patient?*/

CREATE INDEX idx_eda_pid_time ON eda(patientid, date_time);
CREATE INDEX idx_ibinterval_pid_time ON ibinterval(patientid, date_time);

WITH eda_min AS (
  SELECT patientid,
         date_trunc('minute', date_time) AS minute_bin,
         AVG(eda) AS eda
  FROM eda
  GROUP BY patientid, minute_bin
),
ibi_min AS (
  SELECT patientid,
         date_trunc('minute', date_time) AS minute_bin,
         AVG(ibi) AS ibi
  FROM ibinterval
  GROUP BY patientid, minute_bin
)
SELECT
  e.patientid,
  corr(e.eda, i.ibi) AS corr_eda_ibi
FROM eda_min e
JOIN ibi_min i
  ON e.patientid = i.patientid
 AND e.minute_bin = i.minute_bin
GROUP BY e.patientid
ORDER BY e.patientid;

/*Q26: Using both tables, calculate for each patient the Pearson correlation
between eda and ibi values?*/
--Aggregate EDA (Electrodermal Activity) readings per minute per patient
WITH eda_min AS (
  SELECT
      patientid,
      date_trunc('minute', date_time) AS minute_bin,-- Round timestamp to the nearest minute
      AVG(eda) AS avg_eda
  FROM eda
  GROUP BY patientid, minute_bin
),
-- Aggregate IBI (Inter-Beat Interval) readings per minute per patient
ibi_min AS (
  SELECT
      patientid,
      date_trunc('minute', date_time) AS minute_bin,-- Round timestamp to the nearest minute
      AVG(ibi) AS avg_ibi
  FROM ibinterval
  GROUP BY patientid, minute_bin
)
-- Compute per-patient correlation
SELECT
    e.patientid,
    corr(e.avg_eda, i.avg_ibi) AS corr_eda_ibi,
    COUNT(*) AS pair_count
FROM eda_min e
JOIN ibi_min i
  ON e.patientid = i.patientid
 AND e.minute_bin = i.minute_bin
GROUP BY e.patientid
ORDER BY e.patientid;


/*Q27:Create a ranked list of patients based on their overall mean EDA, 
with the highest mean receiving rank 1?*/
WITH avg_eda AS (
  SELECT patientid, AVG(eda) AS mean_eda
  FROM eda
  GROUP BY patientid
)
SELECT
  patientid,
  ROUND(mean_eda::numeric, 3) AS mean_eda,
  -- Rank patients from highest to lowest average EDA
  RANK() OVER (ORDER BY mean_eda DESC) AS eda_rank 
FROM avg_eda
ORDER BY eda_rank;

/*Q28: Determine the maximum and minimum eda recorded for each patient within each week*/
SELECT
    patientid,
	-- Truncate the timestamp to the start of the week
    DATE_TRUNC('week', date_time) AS week_start,
    ROUND(MAX(eda)::numeric, 2) AS max_eda,
    ROUND(MIN(eda)::numeric, 2) AS min_eda
FROM eda
GROUP BY patientid, week_start
ORDER BY patientid, week_start;

/*Q29: For each patient, calculate the daily average and maximum heart rate
using date_trunc and compare?*/

WITH daily AS (
  SELECT
      patientid,
      date_trunc('day', date_time) AS day,
      AVG(hr)  AS avg_hr,
      MAX(hr)  AS max_hr
  FROM heartrate
  GROUP BY patientid, day
)
SELECT
    patientid,
    day,
    ROUND(avg_hr::numeric, 2) AS avg_hr,
    ROUND(max_hr::numeric, 2) AS max_hr,
    ROUND((max_hr - avg_hr)::numeric, 2) AS max_minus_avg  -- simple comparison
FROM daily
ORDER BY patientid, day;

/*Q30: For each patient, find the daily average temp and 
also find the three highest temperatures they had during this period.
)*/
WITH daily_temp AS (
  SELECT
      patientid,
      DATE_TRUNC('day', date_time) AS day,
      AVG(temperature) AS avg_temp
  FROM temperature
  GROUP BY patientid, day
),
ranked AS (
  SELECT
      patientid,
      day,
      ROUND(avg_temp::numeric, 2) AS avg_temp,
      RANK() OVER (PARTITION BY patientid ORDER BY avg_temp DESC) AS temp_rank
  FROM daily_temp
)
SELECT *
FROM ranked
WHERE temp_rank <= 3
ORDER BY patientid, temp_rank;

/*Q31:Categorize glucose as Hypo (<70), Normal (70–180), or Hyper (>180). 
Count events per patient per day.*/

select patientid,
       DATE(date_time) as day,
	   -- Count the number of readings in the 
	   --Hypo zone (< 70 mg/dL),Normal zone (70–180 mg/dL)and Hyper zone (> 180 mg/dL)
       count(*) filter (where zone = 'Hypo') as hypo_count,
       count(*) filter (where zone = 'Normal') as normal_count,
       count(*) filter (where zone = 'Hyper') as hyper_count
--subquery to assign zone labels to each glucose reading
from ( select patientid, date_time,
         case when glucosevalue < 70 then 'Hypo'
              when glucosevalue between 70 and 180 then 'Normal'
              else 'Hyper' end as zone
  from dexcom) t group by patientid, day;


 