# Define SQL queries without trailing semicolons
def sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
           "results": """
                    SELECT REGEXP_REPLACE(TRIM(UPPER(r.course_cd)), '\s+$', '') AS course_cd,
                        r.race_date,
                        r.race_number,
                        REGEXP_REPLACE(TRIM(UPPER(r2.saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number,
                        h.horse_id,
                        h.horse_name,
                        re.official_fin,
                        r.purse,
                        r.wps_pool,
                        r2.weight,
                        h.foal_date AS date_of_birth,
                        TRIM(h.sex) AS sex,
                        r2.post_position AS start_position,
                        TRIM(r2.equip) AS equip,
                        r2.claimprice,
                        TRIM(r.surface) AS surface,
                        ts.surface_type_description,
                        TRIM(r.trk_cond) AS trk_cond,
                        tc.description AS trk_cond_desc,
                        TRIM(r.weather) AS weather,
                        r.distance,
                        r.dist_unit,
                        r2.power,
                        TRIM(r2.med) AS med,
                        r2.morn_odds,
                        r2.avgspd,
                        r2.jock_key,
                        r2.train_key,
                        TRIM(r.race_type) AS race_type,
                        r.class_rating,
                        r2.net_sentiment,
                        TRIM(r.stk_clm_md) AS stk_clm_md,
                        TRIM(r2.turf_mud_mark) AS turf_mud_mark,
                        r2.avg_spd_sd,
                        r2.ave_cl_sd,
                        r2.hi_spd_sd,
                        r2.pstyerl,
                        -- ALL_RACES cumulative stats (prefix with all_ for clarity)
                        has_all.starts AS all_starts,
                        has_all.win AS all_win,
                        has_all.place AS all_place,
                        has_all.show AS all_show,
                        has_all.fourth AS all_fourth,
                        has_all.earnings AS all_earnings,
                        -- Condition-specific cumulative stats (prefix with cond_ for clarity)
                        has_cond.starts AS cond_starts,
                        has_cond.win AS cond_win,
                        has_cond.place AS cond_place,
                        has_cond.show AS cond_show,
                        has_cond.fourth AS cond_fourth,
                        has_cond.earnings AS cond_earnings
                    FROM races r
                    LEFT JOIN v_runners r2 ON r.course_cd = r2.course_cd
                    AND r.race_date = r2.race_date
                    AND r.race_number = r2.race_number
                    JOIN v_results_entries re ON r2.course_cd = re.course_cd
                    AND r2.race_date = re.race_date
                    AND r2.race_number = re.race_number
                    AND r2.saddle_cloth_number = re.program_num
                    and r2.axciskey = re.axciskey 
                    LEFT JOIN jockey j ON r2.jock_key = j.jock_key
                    LEFT JOIN trainer t ON r2.train_key = t.train_key
                    LEFT JOIN track_conditions tc ON r.trk_cond = tc.code
                    LEFT JOIN track_surface ts ON r.surface = ts.surface_type_code
                    JOIN horse h ON re.axciskey = h.axciskey
                    -- Join ALL_RACES stats
                    JOIN horse_accum_stats has_all ON has_all.axciskey = re.axciskey
                    AND has_all.as_of_date = r.race_date
                    AND has_all.stat_type = 'ALL_RACES'
                    -- Join condition-specific stats
                    LEFT JOIN horse_accum_stats has_cond ON has_cond.axciskey = re.axciskey
                    AND has_cond.as_of_date = r.race_date
                    AND has_cond.stat_type = CASE
                        WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance < 700 THEN 'MUDDY_SPRNT'
                        WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance >= 700 THEN 'MUDDY_RTE'
                        WHEN r.surface = 'D' AND r.distance <= 700 THEN 'DIRT_SPRNT'
                        WHEN r.surface = 'D' AND r.distance > 700 THEN 'DIRT_RTE'
                        WHEN r.surface = 'T' AND r.distance <= 700 THEN 'TURF_SPRNT'
                        WHEN r.surface = 'T' AND r.distance > 700 THEN 'TURF_RTE'
                        WHEN r.surface = 'A' AND r.distance <= 700 THEN 'ALL_WEATHER_SPRNT'
                        WHEN r.surface = 'A' AND r.distance > 700 THEN 'ALL_WEATHER_RTE'
                        WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance <= 700 THEN 'ALLOWANCE_SPRNT'
                        WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance > 700 THEN 'ALLOWANCE_RTE'
                        WHEN r.surface = 'D' AND r.race_type = 'Claiming' AND r.distance <= 700 THEN 'CLAIMING_SPRNT'
                        WHEN r.surface = 'D' AND r.race_type = 'Claiming' AND r.distance > 700 THEN 'CLAIMING_RTE'
                        WHEN r.surface = 'D' AND r.race_type = 'Stakes' AND r.distance <= 700 THEN 'STAKES_SPRNT'
                        WHEN r.surface = 'D' AND r.race_type = 'Stakes' AND r.distance > 700 THEN 'STAKES_RTE'
                        ELSE NULL
                        END
                    WHERE r2.breed_type = 'TB'
                    AND re.official_fin IS NOT null
        """,
        "gpspoint": """
            SELECT REGEXP_REPLACE(TRIM(UPPER(r.course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(r2.saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, time_stamp, 
                longitude, latitude, speed, progress, stride_frequency, post_time, location
            FROM v_gpspoint
        """,
        "sectionals": """
            SELECT REGEXP_REPLACE(TRIM(UPPER(r.course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(r2.saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, gate_name, gate_numeric,
                length_to_finish, sectional_time, running_time, distance_back, distance_ran,
                number_of_strides, post_time
            FROM v_sectionals
        """,
        "routes": """
            SELECT course_cd,line_type, track_name, line_name, coordinates
            FROM routes
        """
    }
    return queries