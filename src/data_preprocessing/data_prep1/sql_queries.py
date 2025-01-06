# Define SQL queries without trailing semicolons
def sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
           "results": """
            SELECT 
                /* Original columns */
                REGEXP_REPLACE(TRIM(UPPER(r.course_cd)), '\s+$', '') AS course_cd,
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
                TRIM(r2.equip) AS equip,
                r2.claimprice,
                TRIM(r.surface) AS surface,
                TRIM(r.trk_cond) AS trk_cond,
                TRIM(r.weather) AS weather,
                r.distance,
                r.dist_unit,
                r2.power,
                TRIM(r2.med) AS med,
                r2.morn_odds,
                r2.avgspd,
                TRIM(r.race_type) AS race_type,
                r.class_rating,
                r.todays_cls,
                r2.net_sentiment,
                TRIM(r.stk_clm_md) AS stk_clm_md,
                TRIM(r2.turf_mud_mark) AS turf_mud_mark,
                r2.avg_spd_sd,
                r2.ave_cl_sd,
                r2.hi_spd_sd,
                r2.pstyerl,
                -- Horse cumulative stats (ALL_RACES, prefix "all_")
                has_all.starts       AS all_starts,
                has_all.win          AS all_win,
                has_all.place        AS all_place,
                has_all.show         AS all_show,
                has_all.fourth       AS all_fourth,
                has_all.earnings     AS all_earnings,
                -- Condition-specific cumulative stats
                has_cond.starts      AS cond_starts,
                has_cond.win         AS cond_win,
                has_cond.place       AS cond_place,
                has_cond.show        AS cond_show,
                has_cond.fourth      AS cond_fourth,
                has_cond.earnings    AS cond_earnings,
                /* ---------------------------
                Example: horse_recent_form
                e.g. columns: avg_fin_3, avg_fin_5, speed_improvement, days_off, etc.
                Join on (h.horse_id and r.race_date).
                --------------------------- */
                hrf.avg_fin_3,
                hrf.avg_beaten_3,
                hrf.avg_speed_3,
                hrf.avg_fin_5,
                hrf.avg_beaten_5,
                hrf.avg_speed_5,
                hrf.speed_improvement,
                hrf.days_off,
                hrf.layoff_cat,
                -- etc. as needed
                /* ---------------------------
                sectionals_aggregated
                e.g.: avgtime_gate1, dist_bk_gate4, total_distance_ran, running_time
                join keys: same course_cd, race_date, race_number, saddle_cloth_number
                --------------------------- */
                sa.avgtime_gate1,
                sa.avgtime_gate2,
                sa.avgtime_gate3,
                sa.avgtime_gate4,
                sa.dist_bk_gate4   AS sa_dist_bk_gate4,
                sa.total_distance_ran,
                sa.running_time,
                /* ---------------------------
                gps_aggregated
                e.g.: speed_q1..q4, total_dist_covered, avg_acceleration, net_progress_gain
                join keys: same as above
                --------------------------- */
                ga.speed_q1,
                ga.speed_q2,
                ga.speed_q3,
                ga.speed_q4,
                ga.total_dist_covered,
                ga.avg_acceleration,
                ga.net_progress_gain,
                ga.avg_stride_length AS gps_avg_stride_length,
                /* ---------------------------
                Jockey accum stats (all_races_j)
                Typically: join on j.jock_key & r.race_date
                Grab only win_percentage / itm_percentage
                --------------------------- */
                jast_j.win_percentage AS jock_win_percent,
                jast_j.itm_percentage AS jock_itm_percent,
                /* ---------------------------
                Trainer accum stats (all_races_t)
                Typically: join on t.train_key & r.race_date
                --------------------------- */
                tast_t.win_percentage  AS trainer_win_percent,
                tast_t.itm_percentage  AS trainer_itm_percent,
                /* ---------------------------
                Jockey-Trainer accum stats
                e.g. trainer_jockey_stats, also by (jock_key, train_key, r.race_date)
                --------------------------- */
                tjstat.win_percentage  AS jt_win_percent,
                tjstat.itm_percentage  AS jt_itm_percent,
                /* ---------------------------
                Jockey accum by track, etc.
                Example for jock_accum_stats_by_track
                We join on j.jock_key, r.race_date, r.course_cd
                --------------------------- */
                jtrack.win_percentage  AS jock_win_track,
                jtrack.itm_percentage  AS jock_itm_track,
                /* similarly trainer_accum_stats_by_track, jt_accum_stats_by_track, etc. */
                ttrack.win_percentage  AS trainer_win_track,
                ttrack.itm_percentage  AS trainer_itm_track,
                jttrack.win_percentage AS jt_win_track,
                jttrack.itm_percentage AS jt_itm_track
            FROM races r
            /* existing joins for runners, results_entries, horse, etc. */
            LEFT JOIN runners r2 
                ON r.course_cd = r2.course_cd
                AND r.race_date = r2.race_date
                AND r.race_number = r2.race_number
            JOIN results_entries re
                ON r2.course_cd = re.course_cd
                AND r2.race_date = re.race_date
                AND r2.race_number = re.race_number
                AND r2.saddle_cloth_number = re.program_num
                AND r2.axciskey = re.axciskey
            LEFT JOIN horse h
                ON re.axciskey = h.axciskey
            /* horse accum stats all_races */
            JOIN horse_accum_stats has_all
                ON has_all.axciskey = re.axciskey
                AND has_all.as_of_date = r.race_date
                AND has_all.stat_type = 'ALL_RACES'
            /* horse accum stats condition-based */
            LEFT JOIN horse_accum_stats has_cond
                ON has_cond.axciskey = re.axciskey
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
                    WHEN r.surface = 'D' AND r.race_type = 'Stakes'   AND r.distance <= 700 THEN 'STAKES_SPRNT'
                    WHEN r.surface = 'D' AND r.race_type = 'Stakes'   AND r.distance > 700 THEN 'STAKES_RTE'
                    ELSE NULL END
            /* ========== Additional Joins ========== */
            /* horse_recent_form: match on horse + race_date */
            LEFT JOIN horse_recent_form hrf
            ON h.horse_id = hrf.horse_id
            AND r2.race_date = hrf.race_date
            and r2.course_cd = hrf.course_cd
            /* sectionals_aggregated: match on race keys */
            JOIN sectionals_aggregated sa
            ON r2.course_cd = sa.course_cd
            AND r2.race_date = sa.race_date
            AND r2.race_number = sa.race_number
            AND r2.saddle_cloth_number = sa.saddle_cloth_number
            /* gps_aggregated: match on race keys */
            JOIN gps_aggregated ga
            ON sa.course_cd = ga.course_cd
            AND sa.race_date = ga.race_date
            AND sa.race_number = ga.race_number
            AND sa.saddle_cloth_number = ga.saddle_cloth_number
            /* Jockey & Trainer tables (if needed) */
            LEFT JOIN jockey j
            ON r2.jock_key = j.jock_key
            LEFT JOIN trainer t
            ON r2.train_key = t.train_key
            /* Jockey accum stats (all races) => just pulling partial columns here as example */
            LEFT JOIN jock_accum_stats jast_j
            ON jast_j.jock_key = j.jock_key
            AND jast_j.as_of_date = r2.race_date
            AND jast_j.stat_type = 'ALL_RACES_J'
            /* Trainer accum stats (all races) */
            LEFT JOIN trainer_accum_stats tast_t
            ON tast_t.train_key = t.train_key
            AND tast_t.as_of_date = r2.race_date
            AND tast_t.stat_type = 'ALL_RACES_T'
            /* Jockey-Trainer stats */
            LEFT JOIN trainer_jockey_stats tjstat
            ON tjstat.train_key = t.train_key
            AND tjstat.jock_key  = j.jock_key
            AND tjstat.as_of_date= r2.race_date
            AND tjstat.stat_type = 'ALL_RACES_JT'
            /* Jockey accum stats by track */
            LEFT JOIN jock_accum_stats_by_track jtrack
            ON jtrack.jock_key = j.jock_key
            AND jtrack.course_cd = r2.course_cd
            AND jtrack.as_of_date= r2.race_date
            AND jtrack.stat_type = 'ALL_RACES_J_TRACK'
            /* Trainer accum stats by track */
            LEFT JOIN trainer_accum_stats_by_track ttrack
            ON ttrack.train_key = t.train_key
            AND ttrack.course_cd = r2.course_cd
            AND ttrack.as_of_date= r2.race_date
            AND ttrack.stat_type = 'ALL_RACES_T_TRACK'
            /* Jockey-Trainer by track */
            LEFT JOIN jt_accum_stats_by_track jttrack
            ON jttrack.jock_key = j.jock_key
            AND jttrack.train_key = t.train_key
            AND jttrack.course_cd = r2.course_cd
            AND jttrack.as_of_date= r2.race_date
            AND jttrack.stat_type = 'ALL_RACES_JT_TRACK'
            WHERE
                r2.breed_type = 'TB'
                AND re.official_fin IS NOT NULL
    """
        # "results_only": """
        #             SELECT REGEXP_REPLACE(TRIM(UPPER(r.course_cd)), '\s+$', '') AS course_cd,
        #                 r.race_date,
        #                 r.race_number,
        #                 REGEXP_REPLACE(TRIM(UPPER(r2.saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number,
        #                 h.horse_id,
        #                 h.horse_name,
        #                 re.official_fin,
        #                 r.purse,
        #                 r.wps_pool,
        #                 r2.weight,
        #                 h.foal_date AS date_of_birth,
        #                 TRIM(h.sex) AS sex,
        #                 r2.post_position AS start_position,
        #                 TRIM(r2.equip) AS equip,
        #                 r2.claimprice,
        #                 TRIM(r.surface) AS surface,
        #                 ts.surface_type_description,
        #                 TRIM(r.trk_cond) AS trk_cond,
        #                 tc.description AS trk_cond_desc,
        #                 TRIM(r.weather) AS weather,
        #                 r.distance,
        #                 r.dist_unit,
        #                 r2.power,
        #                 TRIM(r2.med) AS med,
        #                 r2.morn_odds,
        #                 r2.avgspd,
        #                 r2.jock_key,
        #                 r2.train_key,
        #                 TRIM(r.race_type) AS race_type,
        #                 r.class_rating,
        #                 r2.net_sentiment,
        #                 TRIM(r.stk_clm_md) AS stk_clm_md,
        #                 TRIM(r2.turf_mud_mark) AS turf_mud_mark,
        #                 r2.avg_spd_sd,
        #                 r2.ave_cl_sd,
        #                 r2.hi_spd_sd,
        #                 r2.pstyerl,
        #                 -- ALL_RACES cumulative stats (prefix with all_ for clarity)
        #                 has_all.starts AS all_starts,
        #                 has_all.win AS all_win,
        #                 has_all.place AS all_place,
        #                 has_all.show AS all_show,
        #                 has_all.fourth AS all_fourth,
        #                 has_all.earnings AS all_earnings,
        #                 -- Condition-specific cumulative stats (prefix with cond_ for clarity)
        #                 has_cond.starts AS cond_starts,
        #                 has_cond.win AS cond_win,
        #                 has_cond.place AS cond_place,
        #                 has_cond.show AS cond_show,
        #                 has_cond.fourth AS cond_fourth,
        #                 has_cond.earnings AS cond_earnings
        #             FROM races r
        #             LEFT JOIN runners r2 ON r.course_cd = r2.course_cd
        #             AND r.race_date = r2.race_date
        #             AND r.race_number = r2.race_number
        #             JOIN results_entries re ON r2.course_cd = re.course_cd
        #             AND r2.race_date = re.race_date
        #             AND r2.race_number = re.race_number
        #             AND r2.saddle_cloth_number = re.program_num
        #             and r2.axciskey = re.axciskey 
        #             LEFT JOIN jockey j ON r2.jock_key = j.jock_key
        #             LEFT JOIN trainer t ON r2.train_key = t.train_key
        #             LEFT JOIN track_conditions tc ON r.trk_cond = tc.code
        #             LEFT JOIN track_surface ts ON r.surface = ts.surface_type_code
        #             JOIN horse h ON re.axciskey = h.axciskey
        #             -- Join ALL_RACES stats
        #             JOIN horse_accum_stats has_all ON has_all.axciskey = re.axciskey
        #             AND has_all.as_of_date = r.race_date
        #             AND has_all.stat_type = 'ALL_RACES'
        #             -- Join condition-specific stats
        #             LEFT JOIN horse_accum_stats has_cond ON has_cond.axciskey = re.axciskey
        #             AND has_cond.as_of_date = r.race_date
        #             AND has_cond.stat_type = CASE
        #                 WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance < 700 THEN 'MUDDY_SPRNT'
        #                 WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance >= 700 THEN 'MUDDY_RTE'
        #                 WHEN r.surface = 'D' AND r.distance <= 700 THEN 'DIRT_SPRNT'
        #                 WHEN r.surface = 'D' AND r.distance > 700 THEN 'DIRT_RTE'
        #                 WHEN r.surface = 'T' AND r.distance <= 700 THEN 'TURF_SPRNT'
        #                 WHEN r.surface = 'T' AND r.distance > 700 THEN 'TURF_RTE'
        #                 WHEN r.surface = 'A' AND r.distance <= 700 THEN 'ALL_WEATHER_SPRNT'
        #                 WHEN r.surface = 'A' AND r.distance > 700 THEN 'ALL_WEATHER_RTE'
        #                 WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance <= 700 THEN 'ALLOWANCE_SPRNT'
        #                 WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance > 700 THEN 'ALLOWANCE_RTE'
        #                 WHEN r.surface = 'D' AND r.race_type = 'Claiming' AND r.distance <= 700 THEN 'CLAIMING_SPRNT'
        #                 WHEN r.surface = 'D' AND r.race_type = 'Claiming' AND r.distance > 700 THEN 'CLAIMING_RTE'
        #                 WHEN r.surface = 'D' AND r.race_type = 'Stakes' AND r.distance <= 700 THEN 'STAKES_SPRNT'
        #                 WHEN r.surface = 'D' AND r.race_type = 'Stakes' AND r.distance > 700 THEN 'STAKES_RTE'
        #                 ELSE NULL
        #                 END
        #             WHERE r2.breed_type = 'TB'
        #             AND re.official_fin IS NOT null
        # """
        # "gpspoint": """
        #     SELECT REGEXP_REPLACE(TRIM(UPPER(course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, time_stamp, 
        #         longitude, latitude, speed, progress, stride_frequency, post_time, location
        #     FROM v_gpspoint
        # """,
        # "sectionals": """
        #     SELECT REGEXP_REPLACE(TRIM(UPPER(course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, gate_name, gate_numeric,
        #         length_to_finish, sectional_time, running_time, distance_back, distance_ran,
        #         number_of_strides, post_time
        #     FROM v_sectionals
        # """,
        # "routes": """
        #     SELECT course_cd,line_type, track_name, line_name, coordinates
        #     FROM routes
        # """
    }
    return queries