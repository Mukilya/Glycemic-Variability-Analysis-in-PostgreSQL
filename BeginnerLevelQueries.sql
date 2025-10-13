---Q1: Which patients fall into normal, prediabetic, or diabetic HbA1c ranges—and how do these distributions vary by gender?---

SELECT
    gender,
    CASE 
        WHEN hba1c < 5.7 THEN 'Normal'
        WHEN hba1c BETWEEN 5.7 AND 6.4 THEN 'Prediabetic'
        WHEN hba1c >= 6.5 THEN 'Diabetic'
        ELSE 'Unknown'
    END AS hba1c_status,
    COUNT(*) AS category_count,
    COUNT(*) OVER (PARTITION BY gender) AS total_by_gender,
    ROUND(100.0 * COUNT(*) / COUNT(*) OVER (PARTITION BY gender), 2) AS percent_within_gender
FROM 
    demography
WHERE 
    hba1c IS NOT NULL
GROUP BY 
    gender, hba1c_status
ORDER BY 
    gender, hba1c_status;

----Q2: What foods are frequently searched but rarely logged by patients?----

SELECT
    searched_food,
    COUNT(searched_food) AS search_count,
    COUNT(logged_food) FILTER (WHERE logged_food = searched_food) AS log_count,
    COUNT(searched_food) - COUNT(logged_food) FILTER (WHERE logged_food = searched_food) AS search_minus_log
FROM
    public.foodlog
WHERE
    searched_food IS NOT NULL
GROUP BY
    searched_food
HAVING
    COUNT(searched_food) > COUNT(logged_food) FILTER (WHERE logged_food = searched_food)
ORDER BY
    search_minus_log DESC;


----Q3: Are there patterns where patients search for high-calorie foods but log healthier options?----
SELECT
    patientid,
    searched_food,
    logged_food,
    calorie,
    CASE 
        WHEN calorie < 200 THEN 'Low Calorie'
        WHEN calorie BETWEEN 200 AND 500 THEN 'Moderate Calorie'
        WHEN calorie > 500 THEN 'High Calorie'
        ELSE 'Unknown'
    END AS logged_food_category
FROM 
    public.foodlog
WHERE 
    searched_food IS NOT NULL
    AND logged_food IS NOT NULL
    AND searched_food <> logged_food
ORDER BY 
    patientid, searched_food;


----Q4: Which patients consistently log foods they search for—and which ones don’t?----

SELECT
    patientid,
    COUNT(*) AS total_entries,
    COUNT(*) FILTER (WHERE logged_food = searched_food) AS matched_entries,
    ROUND(
        COUNT(*) FILTER (WHERE logged_food = searched_food)::NUMERIC / COUNT(*) * 100, 
        2
    ) AS match_percentage
FROM 
    public.foodlog
WHERE 
    searched_food IS NOT NULL 
    AND logged_food IS NOT NULL
GROUP BY 
    patientid
ORDER BY 
    match_percentage DESC;


----Q5: Is there a time-of-day pattern in food searches vs. actual logging?----

SELECT
    EXTRACT(HOUR FROM time_begin) AS search_hour,
    EXTRACT(HOUR FROM time_end) AS log_hour,
    COUNT(*) AS entry_count
FROM 
    public.foodlog
WHERE 
    searched_food IS NOT NULL 
    AND logged_food IS NOT NULL
GROUP BY 
    EXTRACT(HOUR FROM time_begin),
    EXTRACT(HOUR FROM time_end)
ORDER BY 
    search_hour, log_hour;


----Q6: Are searched foods generally higher in sugar or fat than logged foods?----
SELECT
    ROUND(AVG(sugar) FILTER (WHERE searched_food IS NOT NULL), 2) AS avg_searched_sugar,
    ROUND(AVG(sugar) FILTER (WHERE logged_food IS NOT NULL), 2) AS avg_logged_sugar,
    ROUND(AVG(total_fat) FILTER (WHERE searched_food IS NOT NULL), 2) AS avg_searched_fat,
    ROUND(AVG(total_fat) FILTER (WHERE logged_food IS NOT NULL), 2) AS avg_logged_fat
FROM 
    public.foodlog
WHERE 
    searched_food IS NOT NULL 
    AND logged_food IS NOT NULL;

----Q7: Do patients who search for certain food types (e.g., snacks) tend to log different categories?----
SELECT
    patientid,
    searched_food,
    logged_food,
    CASE 
        WHEN LOWER(searched_food) LIKE '%chip%' OR LOWER(searched_food) LIKE '%cookie%' OR LOWER(searched_food) LIKE '%cake%' THEN 'Snack'
        WHEN LOWER(searched_food) LIKE '%apple%' OR LOWER(searched_food) LIKE '%banana%' OR LOWER(searched_food) LIKE '%orange%' THEN 'Fruit'
        ELSE 'Other'
    END AS searched_category,
    CASE 
        WHEN LOWER(logged_food) LIKE '%chip%' OR LOWER(logged_food) LIKE '%cookie%' OR LOWER(logged_food) LIKE '%cake%' THEN 'Snack'
        WHEN LOWER(logged_food) LIKE '%apple%' OR LOWER(logged_food) LIKE '%banana%' OR LOWER(logged_food) LIKE '%orange%' THEN 'Fruit'
        ELSE 'Other'
    END AS logged_category
FROM 
    public.foodlog
WHERE 
    searched_food IS NOT NULL 
    AND logged_food IS NOT NULL
    AND searched_food <> logged_food
ORDER BY 
    patientid;

----Q8: What are the top 10 most searched foods across all patients?----

SELECT
    searched_food,
    COUNT(*) AS search_count
FROM 
    public.foodlog
WHERE 
    searched_food IS NOT NULL
GROUP BY 
    searched_food
ORDER BY 
    search_count DESC
LIMIT 10;

----Q9: What are the most frequently logged foods—and how do they compare to search trends?----
SELECT
    food,
    SUM(search_count) AS total_searches,
    SUM(log_count) AS total_logs
FROM (
    SELECT
        searched_food AS food,
        COUNT(*) AS search_count,
        0 AS log_count
    FROM public.foodlog
    WHERE searched_food IS NOT NULL
    GROUP BY searched_food

    UNION ALL

    SELECT
        logged_food AS food,
        0 AS search_count,
        COUNT(*) AS log_count
    FROM public.foodlog
    WHERE logged_food IS NOT NULL
    GROUP BY logged_food
) AS combined
GROUP BY food
ORDER BY total_logs DESC
LIMIT 10;

----Q10: Are there foods that are searched by many but logged by few?----
SELECT
    food,
    SUM(search_count) AS total_searches,
    SUM(log_count) AS total_logs,
    SUM(search_count) - SUM(log_count) AS search_minus_log
FROM (
    SELECT
        searched_food AS food,
        COUNT(*) AS search_count,
        0 AS log_count
    FROM public.foodlog
    WHERE searched_food IS NOT NULL
    GROUP BY searched_food

    UNION ALL

    SELECT
        logged_food AS food,
        0 AS search_count,
        COUNT(*) AS log_count
    FROM public.foodlog
    WHERE logged_food IS NOT NULL
    GROUP BY logged_food
) AS combined
GROUP BY food
HAVING SUM(search_count) > SUM(log_count)
ORDER BY search_minus_log DESC
LIMIT 10;

----Q11: How do post-breakfast glucose levels differ between male and female prediabetic patients?----
SELECT 
    d.gender,
	--d.patientid,
    AVG(dx.glucosevalue) AS avg_post_breakfast_glucose
FROM 
    dexcom dx
JOIN 
    foodlog fl ON dx.patientid = fl.patientid
JOIN 
    demography d ON dx.patientid = d.patientid AND d.hba1c BETWEEN 5.7 and 6.4
WHERE 
    EXTRACT(HOUR FROM fl.time_begin) BETWEEN 5 AND 10
    AND dx.date_time > fl.time_begin
    AND dx.date_time <= fl.time_begin + INTERVAL '2 hours'
GROUP BY 
    d.gender;


----Q12: What are the items each patient ate for breakfast daily?----
SELECT 
    patientid,
    DATE(time_begin) AS breakfast_date,
    STRING_AGG(logged_food, ', ') AS breakfast_items
FROM 
    foodlog
WHERE 
    EXTRACT(HOUR FROM time_begin) BETWEEN 5 AND 10
GROUP BY 
    patientid, DATE(time_begin)
ORDER BY 
    patientid, breakfast_date;

----Q13:  What is the average HbA1c value across all patients?----

SELECT AVG(hba1c) AS average_hba1c
FROM demography;

----Q14: How many Patients belong to each gender category?----

SELECT gender, COUNT(*) AS patient_count
FROM demography
GROUP BY gender;

----Q15: Which patients have HbA1c values above 6.0 and what are their gneders?----
SELECT patientid, gender, hba1c
FROM demography
WHERE hba1c > 6.0;


----Q16: What is the minimum and maximum HbA1c recorded?-----

SELECT
  MIN(hba1c) AS min_hba1c,
  MAX(hba1c) AS max_hba1c
FROM demography;

----Q17: What is the average glucose value for each patient along with their gender?----

SELECT
  d.patientid,
  d.gender,
  AVG(x.glucosevalue) AS avg_glucose
FROM demography d
JOIN dexcom x ON d.patientid = x.patientid
GROUP BY d.patientid, d.gender
ORDER BY avg_glucose DESC;

----Q18: Which patient had the highest average glucose value?----

SELECT d.patientid, d.gender, AVG(x.glucosevalue) AS avg_glucose
FROM demography d
JOIN dexcom x ON d.patientid = x.patientid
GROUP BY d.patientid, d.gender
ORDER BY avg_glucose DESC
LIMIT 1;


----Q19: Show total sugar intake per patient, ordered by highest ----

SELECT f.patientid, SUM(f.sugar) AS total_sugar
FROM foodlog f
GROUP BY f.patientid
ORDER BY total_sugar DESC
limit 5;

----Q20: Which patients had temperature readings above 37.5°C?----

SELECT t.patientid, t.temperature
FROM temperature t
WHERE t.temperature > 37.5
ORDER BY t.patientid, t.temperature DESC;

-----Q21: List patients with both heart rate and glucose data available?----

SELECT d.patientid,
       d.gender,
       h.date_time,
       h.hr,
       dx.glucosevalue
FROM demography d
JOIN heartrate h ON d.patientid = h.patientid
JOIN dexcom dx ON d.patientid = dx.patientid
AND DATE_TRUNC('minute', h.date_time) = DATE_TRUNC('minute', dx.date_time)
ORDER BY d.patientid, h.date_time;

----Q22: Show average IBI per patient----

SELECT
    i.patientid,
    ROUND(AVG(i.ibi)::numeric, 2) AS avg_ibi
FROM ibinterval i
GROUP BY i.patientid;


----Q23: List patients with more than 5 food entries and average glucose above 150?----

SELECT f.patientid
FROM foodlog f
JOIN dexcom x ON f.patientid = x.patientid
GROUP BY f.patientid
HAVING COUNT(f.*) > 5 AND AVG(x.glucosevalue) > 150;

----Q24: Which Dexcom event types are most frequent across patients?----

SELECT eventtype,
       COUNT(*) AS event_count
FROM dexcom
WHERE eventtype IS NOT NULL
GROUP BY eventtype
ORDER BY event_count DESC;

----Q25: Categorize devices by alert frequency (using CASE)----

SELECT sourcedeviceid,
       COUNT(*) FILTER (WHERE eventtype ILIKE 'alert%') AS alert_count,
       CASE
           WHEN COUNT(*) FILTER (WHERE eventtype ILIKE 'alert%') > 500 THEN 'High Alert Device'
           WHEN COUNT(*) FILTER (WHERE eventtype ILIKE 'alert%') BETWEEN 100 AND 500 THEN 'Medium Alert Device'
           ELSE 'Low Alert Device'
       END AS alert_category
FROM dexcom
GROUP BY sourcedeviceid
ORDER BY alert_count DESC;

----Q26: Classify glucose by morning (6–12), afternoon (12–18), evening (18–24), night (0–6)?----

select patientid,
       case when extract(hour from date_time) between 6 and 11 then 'Morning'
            when extract(hour from date_time) between 12 and 17 then 'Afternoon'
            when extract(hour from date_time) between 18 and 23 then 'Evening'
            else 'Night' end as day_period, avg(glucosevalue) as avg_glucose
from dexcom
group by patientid, day_period order by patientid asc;


----Q27: Label meals based on nutrients?----
select patientid, logged_food,
       case when total_carb > protein * 2 then 'Carb-heavy'
            when protein > total_carb * 2 then 'Protein-heavy'
            else 'Balanced' end as meal_type
from foodlog;


----Q28:Split HR into zones: Rest (<60), Moderate (60–100), High (>100). Compute % time in each zone?----

select patientid,
       round(100.0 * sum(case when hr < 60 then 1 else 0 end) / count(*), 2) as pct_rest,
       round(100.0 * sum(case when hr between 60 and 100 then 1 else 0 end) / count(*), 2) as pct_moderate,
       round(100.0 * sum(case when hr > 100 then 1 else 0 end) / count(*), 2) as pct_high from heartrate group by patientid;
----Q29: Label temperature as Normal (<37.5), Low-grade (37.5–38), High (>38)?----
select patientid, date_time,
       case when temperature < 37.5 then 'Normal'
            when temperature < 38 then 'Low-grade'
            else 'High' end as fever_level
from temperature;

















