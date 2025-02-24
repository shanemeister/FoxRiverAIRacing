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

delete from catboost_enriched_results where 1=1;


-- Breakdown on the number of any_order_top4 and exact_order_top_4
WITH predicted_top4 AS (
    SELECT
        model_key,
        course_cd,
        race_date,
        race_number,
        /* Gather the top-4 predicted horses in the exact order of rank */
        array_agg(horse_id ORDER BY "rank") AS predicted_arr
    FROM catboost_enriched_results
    WHERE "rank" BETWEEN 1 AND 4
    GROUP BY 1,2,3,4
),
actual_top4 AS (
    select
        model_key,
        course_cd,
        race_date,
        race_number,
        /* Gather the actual top-4 finishers in the exact order of official_fin */
        array_agg(horse_id ORDER BY official_fin) AS actual_arr
    FROM catboost_enriched_results
    WHERE official_fin BETWEEN 1 AND 4
    GROUP BY 1,2,3,4
),
joined AS (
    SELECT
        p.model_key,
        p.course_cd,
        p.race_date,
        p.race_number,
        p.predicted_arr,
        a.actual_arr
    FROM predicted_top4 p
    JOIN actual_top4 a
      ON p.model_key = a.model_key
     AND p.course_cd = a.course_cd
     AND p.race_date = a.race_date
     AND p.race_number = a.race_number
)
SELECT
    model_key,
    COUNT(*) AS total_races,
    SUM(
      CASE 
        WHEN (
           SELECT array_agg(x ORDER BY x)
           FROM unnest(predicted_arr) x
          ) = (
           SELECT array_agg(x ORDER BY x)
           FROM unnest(actual_arr) x
          )
          AND predicted_arr <> actual_arr
        THEN 1 ELSE 0 
      END
   ) AS any_order_only_count,
    SUM(
      CASE 
        WHEN predicted_arr = actual_arr
        THEN 1 ELSE 0 
      END
    ) AS exact_order_count,
    ROUND(
      100.0 * 
      SUM(
        CASE
          WHEN (
              SELECT array_agg(x ORDER BY x)
              FROM unnest(predicted_arr) x
            ) = (
              SELECT array_agg(x ORDER BY x)
              FROM unnest(actual_arr) x
            )
            AND predicted_arr <> actual_arr
          THEN 1 ELSE 0
        END
      )::numeric / COUNT(*),
      2
    ) AS any_order_only_percent,
    ROUND(
      100.0 *
      SUM(
        CASE 
          WHEN predicted_arr = actual_arr 
          THEN 1 ELSE 0 
        END
      )::numeric / COUNT(*),
      2
    ) AS exact_order_percent,
    ROUND(
      (
        /* any_order_only_count + exact_order_count */
        (
          SUM(
            CASE 
              WHEN (
                SELECT array_agg(x ORDER BY x)
                FROM unnest(predicted_arr) x
              ) = (
                SELECT array_agg(x ORDER BY x)
                FROM unnest(actual_arr) x
              )
              THEN 1 ELSE 0
            END
          )::numeric
        ) 
        / COUNT(*)
      ) * 100.0
      , 2
    ) AS total_top4_percent
FROM joined
--where race_date > '2024-01-01'
--and race_date > '2023-12-31'
GROUP BY model_key
ORDER BY total_top4_percent desc;

-- Print model and total_top4_percent

WITH predicted_top4 AS (
    SELECT
        model_key,
        course_cd,
        race_date,
        race_number,
        /* Gather the top-4 predicted horses in the exact order of rank */
        array_agg(horse_id ORDER BY "rank") AS predicted_arr
    FROM catboost_enriched_results
    WHERE "rank" BETWEEN 1 AND 4
    GROUP BY 1,2,3,4
),
actual_top4 AS (
    select
        model_key,
        course_cd,
        race_date,
        race_number,
        /* Gather the actual top-4 finishers in the exact order of official_fin */
        array_agg(horse_id ORDER BY official_fin) AS actual_arr
    FROM catboost_enriched_results
    WHERE official_fin BETWEEN 1 AND 4
    GROUP BY 1,2,3,4
),
joined AS (
    SELECT
        p.model_key,
        p.course_cd,
        p.race_date,
        p.race_number,
        p.predicted_arr,
        a.actual_arr
    FROM predicted_top4 p
    JOIN actual_top4 a
      ON p.model_key = a.model_key
     AND p.course_cd = a.course_cd
     AND p.race_date = a.race_date
     AND p.race_number = a.race_number
)
SELECT
    model_key,
    ROUND(
      (
        /* any_order_only_count + exact_order_count */
        (
          SUM(
            CASE 
              WHEN (
                SELECT array_agg(x ORDER BY x)
                FROM unnest(predicted_arr) x
              ) = (
                SELECT array_agg(x ORDER BY x)
                FROM unnest(actual_arr) x
              )
              THEN 1 ELSE 0
            END
          )::numeric
        ) 
        / COUNT(*)
      ) * 100.0
      , 2
    ) AS total_top4_percent
FROM joined
--where race_date > '2024-01-01'
--and race_date > '2023-12-31'
GROUP BY model_key
ORDER BY total_top4_percent desc;

-- Number of races and the number of horses in each race
WITH race_size AS (
  SELECT
    course_cd,
    race_date,
    race_number,
    COUNT(DISTINCT horse_id) AS num_horses
  FROM "catboost_enriched_results-2025-02-18b" cerb 
  GROUP BY
    course_cd,
    race_date,
    race_number
)
SELECT
  num_horses,
  COUNT(*) AS race_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percent_of_races
FROM race_size
GROUP BY num_horses
ORDER BY num_horses;

-- Tells the whole story and combines the two queries above:
WITH race_size AS (
  SELECT
    course_cd,
    race_date,
    race_number,
    COUNT(DISTINCT horse_id) AS num_horses
  FROM catboost_enriched_results
  GROUP BY
    course_cd,
    race_date,
    race_number
),
predicted_top4 AS (
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    array_agg(horse_id ORDER BY "rank") AS predicted_arr
  FROM catboost_enriched_results
  WHERE "rank" BETWEEN 1 AND 4
  GROUP BY 1,2,3,4
),
actual_top4 AS (
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    array_agg(horse_id ORDER BY official_fin) AS actual_arr
  FROM catboost_enriched_results
  WHERE official_fin BETWEEN 1 AND 4
  GROUP BY 1,2,3,4
),
joined AS (
  SELECT
    p.model_key,
    p.course_cd,
    p.race_date,
    p.race_number,
    r.num_horses,            -- Field size
    p.predicted_arr,
    a.actual_arr
  FROM predicted_top4 p
  JOIN actual_top4 a
    ON p.model_key = a.model_key
   AND p.course_cd = a.course_cd
   AND p.race_date = a.race_date
   AND p.race_number = a.race_number
  JOIN race_size r
    ON p.course_cd = r.course_cd
   AND p.race_date = r.race_date
   AND p.race_number = r.race_number
)
SELECT
    model_key,
    num_horses,
    COUNT(*) AS total_races,
    /*
     * any_order_only_count:
     * same set of 4 horses ignoring order,
     * but not identical arrays (meaning the exact order differs)
     */
    SUM(
      CASE 
        WHEN (
          SELECT array_agg(x ORDER BY x)
          FROM unnest(predicted_arr) x
        ) = (
          SELECT array_agg(x ORDER BY x)
          FROM unnest(actual_arr) x
        )
        AND predicted_arr <> actual_arr
      THEN 1 ELSE 0 
      END
    ) AS any_order_only_count,
    /*
     * exact_order_count:
     * the 4 horses match in the same exact order (rank-by-rank).
     */
    SUM(
      CASE 
        WHEN predicted_arr = actual_arr
        THEN 1 ELSE 0 
      END
    ) AS exact_order_count,
    /*
     * Percentages of each relative to total_races
     */
    ROUND(
      100.0 * 
      SUM(
        CASE
          WHEN (
            SELECT array_agg(x ORDER BY x)
            FROM unnest(predicted_arr) x
          ) = (
            SELECT array_agg(x ORDER BY x)
            FROM unnest(actual_arr) x
          )
          AND predicted_arr <> actual_arr
        THEN 1 ELSE 0
        END
      )::numeric / COUNT(*),
      2
    ) AS any_order_only_percent,
    ROUND(
      100.0 *
      SUM(
        CASE 
          WHEN predicted_arr = actual_arr 
          THEN 1 ELSE 0 
        END
      )::numeric / COUNT(*),
      2
    ) AS exact_order_percent,
    /*
     * total_top4_percent = any_order_only_percent + exact_order_percent
     */
    ROUND(
      (
        SUM(
          CASE
            WHEN (
              SELECT array_agg(x ORDER BY x)
              FROM unnest(predicted_arr) x
            ) = (
              SELECT array_agg(x ORDER BY x)
              FROM unnest(actual_arr) x
            )
            THEN 1 ELSE 0
          END
        )::numeric
      / COUNT(*)
      ) * 100.0
      , 2
    ) AS total_top4_percent
FROM joined
--where model_key = 'YetiRank:top=1_NDCG:top=1_20250217_230747'
GROUP BY model_key, num_horses
ORDER BY model_key, num_horses;

-- Filter by number of horses in the race and get the race info to verify:

WITH race_size AS (
  SELECT
    course_cd,
    race_date,
    race_number,
    COUNT(DISTINCT horse_id) AS num_horses
  FROM catboost_enriched_results
  GROUP BY course_cd, race_date, race_number
),
predicted_top4 AS (
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    array_agg(horse_id ORDER BY "rank") AS predicted_arr
  FROM catboost_enriched_results
  WHERE "rank" BETWEEN 1 AND 4
  GROUP BY model_key, course_cd, race_date, race_number
),
actual_top4 AS (
  SELECT
    model_key,
    course_cd,
    race_date,
    race_number,
    array_agg(horse_id ORDER BY official_fin) AS actual_arr
  FROM catboost_enriched_results
  WHERE official_fin BETWEEN 1 AND 4
  GROUP BY model_key, course_cd, race_date, race_number
),
joined AS (
  SELECT
    p.model_key,
    p.course_cd,
    p.race_date,
    p.race_number,
    r.num_horses,
    p.predicted_arr,
    a.actual_arr
  FROM predicted_top4 p
  JOIN actual_top4 a
    ON p.model_key = a.model_key
   AND p.course_cd = a.course_cd
   AND p.race_date = a.race_date
   AND p.race_number = a.race_number
  JOIN race_size r
    ON p.course_cd = r.course_cd
   AND p.race_date = r.race_date
   AND p.race_number = r.race_number
)
SELECT
    j.model_key,
    j.course_cd,
    j.race_date,
    j.race_number,
    j.num_horses,
    /* 
       any_order_flag = 1 if top-4 sets match ignoring order,
                        but not identical array
    */
    CASE 
      WHEN 
         (SELECT array_agg(x ORDER BY x) FROM unnest(j.predicted_arr) x)
         =
         (SELECT array_agg(x ORDER BY x) FROM unnest(j.actual_arr) x)
         AND j.predicted_arr <> j.actual_arr
      THEN 1 ELSE 0
    END AS any_order_flag,
    /*
       exact_order_flag = 1 if top-4 is exactly identical
    */
    CASE 
      WHEN j.predicted_arr = j.actual_arr 
      THEN 1 ELSE 0 
    END AS exact_order_flag
FROM joined j
WHERE j.num_horses IN (7, 8, 9, 10)       -- or >= 17, or whatever filter you need
ORDER BY 
    j.model_key,
    j.num_horses,
    j.course_cd,
    j.race_date,
    j.race_number;
    
   -- Breakdown on which model is best at picking WPS/4th
   WITH rank_counts AS (
  SELECT
    model_key,
    -- total times each rank was predicted
    SUM(CASE WHEN "rank" = 1 THEN 1 ELSE 0 END) AS total_pred_1,
    SUM(CASE WHEN "rank" = 2 THEN 1 ELSE 0 END) AS total_pred_2,
    SUM(CASE WHEN "rank" = 3 THEN 1 ELSE 0 END) AS total_pred_3,
    SUM(CASE WHEN "rank" = 4 THEN 1 ELSE 0 END) AS total_pred_4,
    -- how many times the predicted rank matched actual finishing position
    SUM(CASE WHEN "rank" = 1 AND official_fin = 1 THEN 1 ELSE 0 END) AS correct_1,
    SUM(CASE WHEN "rank" = 2 AND official_fin = 2 THEN 1 ELSE 0 END) AS correct_2,
    SUM(CASE WHEN "rank" = 3 AND official_fin = 3 THEN 1 ELSE 0 END) AS correct_3,
    SUM(CASE WHEN "rank" = 4 AND official_fin = 4 THEN 1 ELSE 0 END) AS correct_4
  FROM catboost_enriched_results
  GROUP BY model_key
)
SELECT
  model_key,
  /* First place */
  correct_1                  AS correct_pred_first,
  total_pred_1               AS total_pred_first,
  ROUND(
    100.0 * correct_1 / NULLIF(total_pred_1, 0),
    2
  )                          AS pct_correct_first,
  /* Second place */
  correct_2                  AS correct_pred_second,
  total_pred_2               AS total_pred_second,
  ROUND(
    100.0 * correct_2 / NULLIF(total_pred_2, 0),
    2
  )                          AS pct_correct_second,
  /* Third place */
  correct_3                  AS correct_pred_third,
  total_pred_3               AS total_pred_third,
  ROUND(
    100.0 * correct_3 / NULLIF(total_pred_3, 0),
    2
  )                          AS pct_correct_third,
  /* Fourth place */
  correct_4                  AS correct_pred_fourth,
  total_pred_4               AS total_pred_fourth,
  ROUND(
    100.0 * correct_4 / NULLIF(total_pred_4, 0),
    2
  )                          AS pct_correct_fourth
FROM rank_counts
ORDER BY pct_correct_fourth desc;
   