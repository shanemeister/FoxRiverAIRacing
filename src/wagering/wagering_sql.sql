 WITH matched AS (          -- ❶ every horse whose rank == finish
    SELECT
        p.race_key,
        UPPER(TRIM(r.course_cd))           AS course_cd,
        r.race_date,
        r.race_number,
        re.official_fin,
        CAST(r.morn_odds AS float8)        AS morn_odds,
        p.top_3_score                      AS raw_score,
        p.top_3_rank                       AS rank,
        p.calibrated_prob                  AS score,
        p.max_prob,
        p.second_prob,
        p.prob_gap,
        p.std_prob,
        p.leader_gap,
        p.trailing_gap,
        p.edge,
        p.kelly
    FROM races r2
    JOIN runners r
      ON r2.course_cd   = r.course_cd
     AND r2.race_date   = r.race_date
     AND r2.race_number = r.race_number
    JOIN horse h
      ON r.axciskey = h.axciskey
    LEFT JOIN results_entries re
      ON r.course_cd           = re.course_cd
     AND r.race_date           = re.race_date
     AND r.race_number         = re.race_number
     AND r.saddle_cloth_number = re.program_num
    LEFT JOIN predictions_20250510_104930_1_calibrated p
      ON r.course_cd           = p.course_cd
     AND r.race_date           = p.race_date
     AND r.race_number         = p.race_number
     AND r.saddle_cloth_number = p.saddle_cloth_number
    WHERE p.top_3_rank IS NOT NULL
      AND r.morn_odds  IS NOT NULL
      AND re.official_fin IS NOT NULL
          -- rank matches finish
      AND re.official_fin = p.top_3_rank
          -- keep only placings you care about
      AND re.official_fin IN (1,2,3)
          -- date / track filters …
      AND r.race_date >= CURRENT_DATE - INTERVAL '1 MONTH'
      AND r.course_cd IN (
            'CTD','CTM','ASD','TWO','TGG','TSA','DEL','TGP','TAM','PRM',
            'HAW','HOO','IND','TCD','ELP','TKD','KEE','TTP','LAD','CNL',
            'LRL','PIM','CBY','CLS','MED','MTH','AQU','BEL','SAR','MVR',
            'TDN','PEN','PRX','HOU','TLS','TOP','DMR'
          )
)
-- ❷ keep only races that have *exactly* N matching horses
SELECT m.*
FROM   matched m
JOIN  ( SELECT race_key           -- change N here ➜
               FROM matched
               GROUP BY race_key
               HAVING COUNT(*) = 3   -- =2 now; use 3 or 4 later
      ) AS ok
  ON m.race_key = ok.race_key
ORDER BY m.race_key, m.official_fin;



SELECT
    p.race_key,
    p.official_fin,
    p.top_3_rank ,
    p.calibrated_prob,
    p.leader_gap,
    p.trailing_gap
FROM  predictions_20250510_104930_1_calibrated  AS p
WHERE p.race_key IN ('AQU_2025-04-27_6.0')
and p.official_fin in (1,2,3)
ORDER BY 1, p.calibrated_prob DESC;

