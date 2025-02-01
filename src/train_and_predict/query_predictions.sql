-- Rank by a Single Score (e.g. score_A)
SELECT
    race_id,
    horse_id,
    horse_name,
    "score_A",
    RANK() OVER (
       PARTITION BY race_id
       ORDER BY "score_A" DESC
    ) AS rank_a
FROM predictions_2025_02_01_1
ORDER BY race_id, rank_a;


-- 2) Rank by Multiple Scores Separately
SELECT
    race_id,
    horse_id,
    horse_name,
    "score_A",
    RANK() OVER (
       PARTITION BY race_id
       ORDER BY "score_A" DESC
    ) AS rank_a,
    "score_B",
    RANK() OVER (
       PARTITION BY race_id
       ORDER BY "score_B" DESC
    ) AS rank_b,
    "score_C",
    RANK() OVER (
       PARTITION BY race_id
       ORDER BY "score_C" DESC
    ) AS rank_c
    -- etc. for however many scores
FROM predictions_2025_02_01_1
ORDER BY race_id, rank_a;



-- 3) Rank by Average (or Sum) of Multiple Scores
SELECT
    race_id,
    horse_id,
    horse_name,
    (
       ("score_A" + "score_B" + "score_C" + "score_D" + 
        "score_E" + "score_F" + "score_G" + "score_H" +
        "score_I" + "score_J" + "score_K" + "score_L" +
        "score_M" + "score_N" + "score_O" + "score_P")
       / 16.0
    ) AS avg_score,
    RANK() OVER (
       PARTITION BY race_id
       ORDER BY 
         (
           ("score_A" + "score_B" + "score_C" + "score_D" + 
            "score_E" + "score_F" + "score_G" + "score_H" +
            "score_I" + "score_J" + "score_K" + "score_L" +
            "score_M" + "score_N" + "score_O" + "score_P")
           / 16.0
         ) DESC
    ) AS avg_rank
FROM predictions_2025_02_01_1
ORDER BY race_id, avg_rank;


--RANK() vs. ROW_NUMBER() vs. DENSE_RANK():
--	•	RANK() → If two horses tie for 1st, they both get rank 1, next horse gets rank 3 (skipping rank 2).
--	•	ROW_NUMBER() → No ties. Each horse has a unique rank within that race.
--	•	DENSE_RANK() → If two horses tie for 1st, they both get rank 1, next horse gets rank 2 (no skip).

SELECT *
FROM (
    SELECT 
       race_id,
       horse_id,
       horse_name,
       "score_A",
       RANK() OVER (PARTITION BY race_id ORDER BY "score_A" DESC) AS rank_a
    FROM predictions_2025_02_01_1
) sub
WHERE rank_a <= 3
ORDER BY race_id, rank_a;

