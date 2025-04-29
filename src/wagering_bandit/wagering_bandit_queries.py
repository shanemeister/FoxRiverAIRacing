# Define SQL queries without trailing semicolons
def wager_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "race_predictions": """
            WITH base AS (
                    SELECT
                        UPPER(TRIM(r.course_cd))            AS course_cd,
                        r2.race_date,
                        r2.race_number,
                        UPPER(TRIM(r.saddle_cloth_number))   AS saddle_cloth_number,
                        h.horse_id,
                        re.official_fin,
                        CAST(r.morn_odds AS float8)        AS morn_odds,
                        r.avg_purse_val_calc,
                        re.dollar_odds,
                        r2.race_type,
                        r2.trk_cond       AS track_condition,
                        r2.surface,
                        r2.distance_meters,
                        p.score           AS raw_score,
                        p.rank            AS rank,
                        p.group_id        AS group_id,
                        p.calibrated_prob AS score,
                        p.track_name,
                        p.race_key
                    FROM races r2
                    JOIN runners r
                        ON r2.course_cd   = r.course_cd
                    AND r2.race_date   = r.race_date
                    AND r2.race_number = r.race_number
                    JOIN horse h
                        ON r.axciskey     = h.axciskey
                    LEFT JOIN results_entries re
                        ON r.course_cd     = re.course_cd
                    AND r.race_date     = re.race_date
                    AND r.race_number   = re.race_number
                    AND r.saddle_cloth_number = re.program_num
                    LEFT JOIN predictions_20250426_151421_1_calibrated p
                        ON r.course_cd           = p.course_cd
                    AND r.race_date           = p.race_date
                    AND r.race_number         = p.race_number
                    AND r.saddle_cloth_number = p.saddle_cloth_number
                    WHERE p.rank IS NOT NULL
                    )
                    SELECT
                    *,
                    COUNT(*) OVER (
                        PARTITION BY course_cd, race_date, race_number
                    ) AS field_size
                    FROM base
        """,
        "wagers": """
            SELECT ew.course_cd , ew.race_date , ew.race_number , ew.wager_id, ew.num_tickets, ew.wager_type, ew.winners, ew.payoff, ew.pool_total, ew.post_time 
            FROM exotic_wagers ew 
            WHERE ew.wager_type is not null
            AND ew.race_date is not null
        """
    }
    return queries