WITH joined AS (
  /* 1) Join catboost_enriched_results cer to results_entries re 
        on (course_cd, race_date, race_number, axciskey or saddle_cloth_number).
     We’ll keep the predicted "rank" and the actual finishing position. */
  SELECT
    cer.model_key,
    cer.course_cd,
    cer.race_date,
    cer.race_number,
    cer.horse_id,
    cer."rank"            AS predicted_rank,
    re.official_fin       AS actual_fin
  FROM catboost_enriched_results_a cer
  JOIN results_entries re 
       ON cer.course_cd           = re.course_cd
      AND cer.race_date           = re.race_date
      AND cer.race_number         = re.race_number
      AND cer.axciskey = re.axciskey    -- or cer.axciskey = re.axciskey if that’s consistent
),
actual_top4 AS (
  /* 2) The 4 *actually finishing* in 1..4 for each (model_key, race)
        We store the horse_id plus official_fin so we can check the order. */
  SELECT
    j.model_key,
    j.course_cd,
    j.race_date,
    j.race_number,
    j.horse_id,
    j.actual_fin
  FROM joined j
  WHERE j.actual_fin BETWEEN 1 AND 4
),
predicted_top4 AS (
  /* 3) The 4 *predicted* in 1..4 for each (model_key, race).
        We keep the horse plus predicted_rank so we can check order. */
  SELECT
    j.model_key,
    j.course_cd,
    j.race_date,
    j.race_number,
    j.horse_id,
    j.predicted_rank
  FROM joined j
  WHERE j.predicted_rank BETWEEN 1 AND 4
),
races AS (
  /*
    4) We'll do a full join of actual_top4 and predicted_top4 
       on (model_key, course_cd, race_date, race_number, horse_id).
       This lets us see for each race:
         - Which horses are top-4 actual only,
         - Which are top-4 predicted only,
         - Which are both (and their official_fin + predicted_rank).
  */
  SELECT
    COALESCE(a.model_key, p.model_key)      AS model_key,
    COALESCE(a.course_cd, p.course_cd)      AS course_cd,
    COALESCE(a.race_date, p.race_date)      AS race_date,
    COALESCE(a.race_number, p.race_number)  AS race_number,
    COALESCE(a.horse_id, p.horse_id)        AS horse_id,
    a.actual_fin,
    p.predicted_rank
  FROM actual_top4 a
  FULL JOIN predicted_top4 p
    ON a.model_key    = p.model_key
   AND a.course_cd    = p.course_cd
   AND a.race_date    = p.race_date
   AND a.race_number  = p.race_number
   AND a.horse_id     = p.horse_id
),
stats_per_race AS (
  /*
    5) For each race + model, we check:
       - predicted_count = how many horses the model predicted in top-4 
       - actual_count    = how many were truly top-4
       - intersection_4  = 1 if sets match in ANY order (i.e. predicted_count=4, actual_count=4, same horses).
         We do that by counting how many have BOTH predicted_rank and actual_fin non-null, 
         and confirm it's 4 while both sets are of size 4.
       - exact_4         = 1 if the top-4 is in EXACT order. That means for each horse in top-4, we want:
            predicted_rank = actual_fin. We also need 4 total matches.
  */
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    COUNT(*) FILTER (WHERE predicted_rank IS NOT NULL) AS predicted_count,
    COUNT(*) FILTER (WHERE actual_fin IS NOT NULL)     AS actual_count,
    COUNT(*) FILTER (
      WHERE predicted_rank IS NOT NULL
        AND actual_fin IS NOT NULL
    ) AS overlap_count,
    -- For EXACT matching, we want each horse with predicted_rank= actual_fin.
    -- If all 4 matched in that manner => exact_4=1
    SUM(
      CASE 
        WHEN predicted_rank IS NOT NULL
         AND actual_fin IS NOT NULL
         AND predicted_rank = actual_fin 
         AND predicted_rank BETWEEN 1 AND 4
        THEN 1 ELSE 0 
      END
    ) AS matched_positions
  FROM races
  GROUP BY model_key, course_cd, race_date, race_number
),
race_flags AS (
  /*
    6) Now define two flags per race:
       any_order_flag = 1 if (actual_count=4, predicted_count=4, overlap_count=4).
       exact_order_flag= 1 if any_order_flag=1 plus matched_positions=4
         (meaning we got all 4 horses the same AND each predicted_rank=actual_fin).
  */
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    CASE
      WHEN actual_count=4 AND predicted_count=4 AND overlap_count=4
      THEN 1 ELSE 0
    END AS any_order_flag,
    CASE
      -- we only define exact=1 if the same 4 horses AND all positions match
      WHEN actual_count=4
       AND predicted_count=4
       AND overlap_count=4
       AND matched_positions=4
      THEN 1 ELSE 0
    END AS exact_order_flag
  FROM stats_per_race
),
summary AS (
  /*
    7) Summarize by model_key:
       total_races = distinct (course_cd, race_date, race_number) combos
       sum(any_order_flag), sum(exact_order_flag).
  */
  SELECT
    model_key,
    COUNT(DISTINCT (course_cd, race_date, race_number)) AS total_races,
    SUM(any_order_flag) AS any_order_count,
    SUM(exact_order_flag) AS exact_order_count
  FROM race_flags
  GROUP BY model_key
)
SELECT
  model_key,
  total_races,
  any_order_count,
  ROUND((any_order_count::numeric / total_races)*100, 2) AS any_order_percent,
  exact_order_count,
  ROUND((exact_order_count::numeric / total_races)*100, 2) AS exact_order_percent
FROM summary
ORDER BY any_order_percent desc, model_key;


WITH joined AS (
  /* 1) Join catboost_enriched_results cer to results_entries re 
        on (course_cd, race_date, race_number, axciskey or saddle_cloth_number).
     We’ll keep the predicted "rank" and the actual finishing position. */
  SELECT
    cer.model_key,
    cer.course_cd,
    cer.race_date,
    cer.race_number,
    cer.horse_id,
    cer."rank"            AS predicted_rank,
    re.official_fin       AS actual_fin
  FROM catboost_enriched_results_a cer
  JOIN results_entries re 
       ON cer.course_cd           = re.course_cd
      AND cer.race_date           = re.race_date
      AND cer.race_number         = re.race_number
      AND cer.axciskey = re.axciskey    -- or cer.axciskey = re.axciskey if that’s consistent
),
actual_top4 AS (
  /* 2) The 4 *actually finishing* in 1..4 for each (model_key, race)
        We store the horse_id plus official_fin so we can check the order. */
  SELECT
    j.model_key,
    j.course_cd,
    j.race_date,
    j.race_number,
    j.horse_id,
    j.actual_fin
  FROM joined j
  WHERE j.actual_fin BETWEEN 1 AND 4
),
predicted_top4 AS (
  /* 3) The 4 *predicted* in 1..4 for each (model_key, race).
        We keep the horse plus predicted_rank so we can check order. */
  SELECT
    j.model_key,
    j.course_cd,
    j.race_date,
    j.race_number,
    j.horse_id,
    j.predicted_rank
  FROM joined j
  WHERE j.predicted_rank BETWEEN 1 AND 4
),
races AS (
  /*
    4) We'll do a full join of actual_top4 and predicted_top4 
       on (model_key, course_cd, race_date, race_number, horse_id).
       This lets us see for each race:
         - Which horses are top-4 actual only,
         - Which are top-4 predicted only,
         - Which are both (and their official_fin + predicted_rank).
  */
  SELECT
    COALESCE(a.model_key, p.model_key)      AS model_key,
    COALESCE(a.course_cd, p.course_cd)      AS course_cd,
    COALESCE(a.race_date, p.race_date)      AS race_date,
    COALESCE(a.race_number, p.race_number)  AS race_number,
    COALESCE(a.horse_id, p.horse_id)        AS horse_id,
    a.actual_fin,
    p.predicted_rank
  FROM actual_top4 a
  FULL JOIN predicted_top4 p
    ON a.model_key    = p.model_key
   AND a.course_cd    = p.course_cd
   AND a.race_date    = p.race_date
   AND a.race_number  = p.race_number
   AND a.horse_id     = p.horse_id
),
stats_per_race AS (
  /*
    5) For each race + model, we check:
       - predicted_count = how many horses the model predicted in top-4 
       - actual_count    = how many were truly top-4
       - intersection_4  = 1 if sets match in ANY order (i.e. predicted_count=4, actual_count=4, same horses).
         We do that by counting how many have BOTH predicted_rank and actual_fin non-null, 
         and confirm it's 4 while both sets are of size 4.
       - exact_4         = 1 if the top-4 is in EXACT order. That means for each horse in top-4, we want:
            predicted_rank = actual_fin. We also need 4 total matches.
  */
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    COUNT(*) FILTER (WHERE predicted_rank IS NOT NULL) AS predicted_count,
    COUNT(*) FILTER (WHERE actual_fin IS NOT NULL)     AS actual_count,
    COUNT(*) FILTER (
      WHERE predicted_rank IS NOT NULL
        AND actual_fin IS NOT NULL
    ) AS overlap_count,
    -- For EXACT matching, we want each horse with predicted_rank= actual_fin.
    -- If all 4 matched in that manner => exact_4=1
    SUM(
      CASE 
        WHEN predicted_rank IS NOT NULL
         AND actual_fin IS NOT NULL
         AND predicted_rank = actual_fin 
         AND predicted_rank BETWEEN 1 AND 4
        THEN 1 ELSE 0 
      END
    ) AS matched_positions
  FROM races
  GROUP BY model_key, course_cd, race_date, race_number
),
race_flags AS (
  /*
    6) Now define two flags per race:
       any_order_flag = 1 if (actual_count=4, predicted_count=4, overlap_count=4).
       exact_order_flag= 1 if any_order_flag=1 plus matched_positions=4
         (meaning we got all 4 horses the same AND each predicted_rank=actual_fin).
  */
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    CASE
      WHEN actual_count=4 AND predicted_count=4 AND overlap_count=4
      THEN 1 ELSE 0
    END AS any_order_flag,
    CASE
      -- we only define exact=1 if the same 4 horses AND all positions match
      WHEN actual_count=4
       AND predicted_count=4
       AND overlap_count=4
       AND matched_positions=4
      THEN 1 ELSE 0
    END AS exact_order_flag
  FROM stats_per_race
),
summary AS (
  /*
    7) Summarize by model_key:
       total_races = distinct (course_cd, race_date, race_number) combos
       sum(any_order_flag), sum(exact_order_flag).
  */
  SELECT
    model_key,
    COUNT(DISTINCT (course_cd, race_date, race_number)) AS total_races,
    SUM(any_order_flag) AS any_order_count,
    SUM(exact_order_flag) AS exact_order_count
  FROM race_flags
  GROUP BY model_key
)
SELECT
  model_key,
  total_races,
  any_order_count,
  ROUND((any_order_count::numeric / total_races)*100, 2) AS any_order_percent,
  exact_order_count,
  ROUND((exact_order_count::numeric / total_races)*100, 2) AS exact_order_percent
FROM summary
ORDER BY any_order_percent desc, model_key;













select count(*)
from (
select cer.course_cd , cer.race_date , cer.race_number , re.horse_name , count(re.official_fin) , count(cer."rank") , count(cer.prediction) , count(cer.true_label), cer.model_key 
from catboost_enriched_results cer 
join results_entries re on cer.course_cd = re.course_cd 
	and cer.race_date = re.race_date 
	and cer.race_number = re. race_number 
	and cer.axciskey = re.axciskey 
where re.official_fin = cer."rank" 
and re.official_fin = 1
group by cer.course_cd , cer.race_date , cer.race_number , re.horse_name , cer.model_key )

select race_id,  cer.course_cd , cer.race_date , cer.race_number , re.horse_name , count(re.official_fin) , count(cer."rank") , count(cer.prediction) , count(cer.true_label), cer.model_key 
from catboost_enriched_results cer 
join results_entries re on cer.course_cd = re.course_cd 
	and cer.race_date = re.race_date 
	and cer.race_number = re. race_number 
	and cer.axciskey = re.axciskey 
where re.official_fin = cer."rank" 
and re.official_fin in (1, 2, 3, 4)
and cer.race_id = 'AQU_2022-03-19_9.0'
group by race_id , cer.course_cd , cer.race_date , cer.race_number , re.horse_name , cer.model_key

