WITH top4 AS (
  SELECT
    model_key,
    group_id,
    rank::int AS rank,  -- cast to INT
    re.official_fin::int as actual_finish -- also ensure same INT
  FROM ensemble_average_results ear
  JOIN results_entries re ON ear.course_cd = re.course_cd
   AND ear.race_date = re.race_date
   AND ear.race_number = re.race_number
   AND ear.axciskey = re.axciskey
  WHERE ear.rank <= 4
  UNION ALL
  SELECT
    'ensemble' AS model_key,
    group_id,
    ensemble_rank::int as rank,  -- also cast to int
    re.official_fin::int as actual_finish
  FROM ensemble_average_results ear
  JOIN results_entries re ON ear.course_cd = re.course_cd
   AND ear.race_date = re.race_date
   AND ear.race_number = re.race_number
   AND ear.axciskey = re.axciskey
  WHERE ear.ensemble_rank <= 4
),
race_agg AS (
  SELECT
    model_key,
    group_id,
    /* build int[] arrays to avoid mismatch */
    ARRAY_AGG(rank ORDER BY rank)::int[]            AS predicted_ranks,
    ARRAY_AGG(actual_finish ORDER BY rank)::int[]   AS actual_finishes
  FROM top4
  GROUP BY model_key, group_id
)
SELECT
  model_key,
  COUNT(*) AS total_races,
  SUM(CASE WHEN predicted_ranks = actual_finishes THEN 1 ELSE 0 END) AS correct_top_4_ordered,
  ROUND(
    100.0 * SUM(CASE WHEN predicted_ranks = actual_finishes THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS ordered_top_4_accuracy,
  /* check sorted arrays for unordered match */
  SUM(
    CASE WHEN
      (SELECT ARRAY_AGG(x ORDER BY x) FROM unnest(predicted_ranks) x)
        =
      (SELECT ARRAY_AGG(y ORDER BY y) FROM unnest(actual_finishes) y)
      THEN 1 ELSE 0 END
  ) AS correct_top_4_unordered,
  ROUND(
    100.0 * SUM(
      CASE WHEN
        (SELECT ARRAY_AGG(x ORDER BY x) FROM unnest(predicted_ranks) x)
          =
        (SELECT ARRAY_AGG(y ORDER BY y) FROM unnest(actual_finishes) y)
        THEN 1 ELSE 0 END
    ) / COUNT(*),
    2
  ) AS unordered_top_4_accuracy
FROM race_agg
GROUP BY model_key
ORDER BY unordered_top_4_accuracy DESC;

select ear.track_name , ear.race_date ,ear.race_number, ear.horse_name , re.official_fin, ear."rank" , ear.ensemble_rank , ear.true_label , ear.prediction, ear.ensemble_score, ear.model_key  
from ensemble_average_results ear 
join results_entries re on ear.course_cd = re.course_cd 
	and ear.race_date = re.race_date 
	and ear.race_number = re.race_number 
	and ear.axciskey = re.axciskey 
where ear.course_cd = 'TOP'
and ear.race_date = '2025-01-18'
and ear.race_number = 1
order by re.official_fin, race_date desc  ;

