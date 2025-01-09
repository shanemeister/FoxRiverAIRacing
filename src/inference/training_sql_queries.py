# Define SQL queries without trailing semicolons
def sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
           "training_data": 
            """
SELECT 
            UPPER(TRIM(r.course_cd)) AS course_cd,
            r.race_date,
            r.race_number,
            UPPER(TRIM(r2.saddle_cloth_number)) AS saddle_cloth_number,
            h.horse_id,
            h.horse_name,
            re.official_fin,
            r.purse,
            r2.weight,
            h.foal_date AS date_of_birth,
            TRIM(h.sex)   AS sex,
            TRIM(r2.equip) AS equip,
            r2.claimprice,
            TRIM(r.surface)  AS surface,
            TRIM(r.trk_cond) AS trk_cond,
            TRIM(r.rr_weather)  AS weather,
            r.distance,
            r.dist_unit,
            r2.power,
            TRIM(r2.med)    AS med,
            r2.morn_odds,
            r2.avgspd,
            TRIM(r.race_type) AS race_type,
            r.class_rating,
            r2.net_sentiment,
            TRIM(r.stk_clm_md)     AS stk_clm_md,
            TRIM(r2.turf_mud_mark) AS turf_mud_mark,
            r2.avg_spd_sd,
            r2.ave_cl_sd,
            r2.hi_spd_sd,
            r2.pstyerl,
            has_all.starts       AS all_starts,
            has_all.win          AS all_win,
            has_all.place        AS all_place,
            has_all.show         AS all_show,
            has_all.fourth       AS all_fourth,
            has_all.earnings     AS all_earnings,
            has_cond.starts      AS cond_starts,
            has_cond.win         AS cond_win,
            has_cond.place       AS cond_place,
            has_cond.show        AS cond_show,
            has_cond.fourth      AS cond_fourth,
            has_cond.earnings    AS cond_earnings,
            hrf.avg_fin_3,
            hrf.avg_beaten_3,
            hrf.avg_speed_3,
            hrf.avg_fin_5,
            hrf.avg_beaten_5,
            hrf.avg_speed_5,
            hrf.speed_improvement,
            hrf.days_off,
            hrf.layoff_cat,
            sa.avgtime_gate1,
            sa.avgtime_gate2,
            sa.avgtime_gate3,
            sa.avgtime_gate4,
            sa.dist_bk_gate4     AS sa_dist_bk_gate4,
            sa.total_distance_ran,
            sa.running_time,
            ga.speed_q1,
            ga.speed_q2,
            ga.speed_q3,
            ga.speed_q4,
            ga.total_dist_covered,
            ga.avg_acceleration,
            ga.net_progress_gain,
            ga.avg_stride_length AS gps_avg_stride_length,
            jast_j.win_percentage AS jock_win_percent,
            jast_j.itm_percentage AS jock_itm_percent,
            tast_t.win_percentage  AS trainer_win_percent,
            tast_t.itm_percentage  AS trainer_itm_percent,
            tjstat.win_percentage  AS jt_win_percent,
            tjstat.itm_percentage  AS jt_itm_percent,
            jtrack.win_percentage  AS jock_win_track,
            jtrack.itm_percentage  AS jock_itm_track,
            ttrack.win_percentage  AS trainer_win_track,
            ttrack.itm_percentage  AS trainer_itm_track,
            jttrack.win_percentage AS jt_win_track,
            jttrack.itm_percentage AS jt_itm_track
        FROM races r
        LEFT JOIN runners r2
            ON r.course_cd  = r2.course_cd
            AND r.race_date  = r2.race_date
            AND r.race_number= r2.race_number
        LEFT JOIN results_entries re
            ON r2.course_cd = re.course_cd
            AND r2.race_date = re.race_date
            AND r2.race_number= re.race_number
            AND r2.saddle_cloth_number = re.program_num
            AND r2.axciskey = re.axciskey
        LEFT JOIN horse h
            ON r2.axciskey = h.axciskey
        LEFT JOIN horse_accum_stats has_all
				  ON has_all.axciskey  = r2.axciskey
				  AND has_all.stat_type = 'ALL_RACES'
				  AND has_all.as_of_date = (
				      SELECT MAX(a2.as_of_date)
				      FROM horse_accum_stats a2
				      WHERE a2.axciskey   = r2.axciskey
				        AND a2.stat_type  = 'ALL_RACES'
				        AND a2.as_of_date <= r.race_date)
		LEFT JOIN horse_accum_stats has_cond
		    ON has_cond.axciskey = r2.axciskey
		    /* Use exactly the same CASE expression for the stat_type: */
		    AND has_cond.stat_type = CASE
		        WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance < 700  THEN 'MUDDY_SPRNT'
		        WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance >= 700 THEN 'MUDDY_RTE'
		        WHEN r.surface = 'D' AND r.distance <= 700 THEN 'DIRT_SPRNT'
		        WHEN r.surface = 'D' AND r.distance > 700  THEN 'DIRT_RTE'
		        WHEN r.surface = 'T' AND r.distance <= 700 THEN 'TURF_SPRNT'
		        WHEN r.surface = 'T' AND r.distance > 700  THEN 'TURF_RTE'
		        WHEN r.surface = 'A' AND r.distance <= 700 THEN 'ALL_WEATHER_SPRNT'
		        WHEN r.surface = 'A' AND r.distance > 700  THEN 'ALL_WEATHER_RTE'
		        WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance <= 700 THEN 'ALLOWANCE_SPRNT'
		        WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance > 700  THEN 'ALLOWANCE_RTE'
		        WHEN r.surface = 'D' AND r.race_type = 'Claiming'  AND r.distance <= 700 THEN 'CLAIMING_SPRNT'
		        WHEN r.surface = 'D' AND r.race_type = 'Claiming'  AND r.distance > 700  THEN 'CLAIMING_RTE'
		        WHEN r.surface = 'D' AND r.race_type = 'Stakes'    AND r.distance <= 700 THEN 'STAKES_SPRNT'
		        WHEN r.surface = 'D' AND r.race_type = 'Stakes'    AND r.distance > 700  THEN 'STAKES_RTE'
		        ELSE NULL
		    END
		    AND has_cond.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM horse_accum_stats a2
		        WHERE a2.axciskey  = r2.axciskey
		          AND a2.stat_type = CASE
		              WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance < 700  THEN 'MUDDY_SPRNT'
		              WHEN r.surface = 'D' AND r.trk_cond = 'Muddy' AND r.distance >= 700 THEN 'MUDDY_RTE'
		              WHEN r.surface = 'D' AND r.distance <= 700 THEN 'DIRT_SPRNT'
		              WHEN r.surface = 'D' AND r.distance > 700  THEN 'DIRT_RTE'
		              WHEN r.surface = 'T' AND r.distance <= 700 THEN 'TURF_SPRNT'
		              WHEN r.surface = 'T' AND r.distance > 700  THEN 'TURF_RTE'
		              WHEN r.surface = 'A' AND r.distance <= 700 THEN 'ALL_WEATHER_SPRNT'
		              WHEN r.surface = 'A' AND r.distance > 700  THEN 'ALL_WEATHER_RTE'
		              WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance <= 700 THEN 'ALLOWANCE_SPRNT'
		              WHEN r.surface = 'D' AND r.race_type = 'Allowance' AND r.distance > 700  THEN 'ALLOWANCE_RTE'
		              WHEN r.surface = 'D' AND r.race_type = 'Claiming'  AND r.distance <= 700 THEN 'CLAIMING_SPRNT'
		              WHEN r.surface = 'D' AND r.race_type = 'Claiming'  AND r.distance > 700  THEN 'CLAIMING_RTE'
		              WHEN r.surface = 'D' AND r.race_type = 'Stakes'    AND r.distance <= 700 THEN 'STAKES_SPRNT'
		              WHEN r.surface = 'D' AND r.race_type = 'Stakes'    AND r.distance > 700  THEN 'STAKES_RTE'
		              ELSE NULL
          END
          AND a2.as_of_date <= r.race_date)        
          LEFT JOIN horse_recent_form hrf
            ON h.horse_id = hrf.horse_id
            AND r.race_date= hrf.race_date
            AND r.course_cd= hrf.course_cd
            AND r2.saddle_cloth_number = hrf.saddle_cloth_number
            AND r2.race_number = hrf.race_number
        LEFT JOIN sectionals_aggregated sa
            ON r2.course_cd           = sa.course_cd
            AND r2.race_date           = sa.race_date
            AND r2.race_number         = sa.race_number
            AND r2.saddle_cloth_number = sa.saddle_cloth_number
        LEFT JOIN gps_aggregated ga
            ON r2.course_cd           = ga.course_cd
            AND r2.race_date           = ga.race_date
            AND r2.race_number         = ga.race_number
            AND r2.saddle_cloth_number = ga.saddle_cloth_number
        LEFT JOIN jockey j
            ON r2.jock_key = j.jock_key
        LEFT JOIN trainer t
            ON r2.train_key = t.train_key
        LEFT JOIN jock_accum_stats jast_j
		    ON j.jock_key     = jast_j.jock_key
		    AND jast_j.stat_type = 'ALL_RACES_J'
		    AND jast_j.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM jock_accum_stats a2
		        WHERE a2.jock_key   = j.jock_key
		          AND a2.stat_type  = 'ALL_RACES_J'
		          AND a2.as_of_date <= r.race_date
		    )
        LEFT JOIN trainer_accum_stats tast_t
		    ON t.train_key       = tast_t.train_key
		    AND tast_t.stat_type = 'ALL_RACES_T'
		    AND tast_t.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM trainer_accum_stats a2
		        WHERE a2.train_key  = t.train_key
		          AND a2.stat_type  = 'ALL_RACES_T'
		          AND a2.as_of_date <= r.race_date
		    )
        LEFT JOIN trainer_jockey_stats tjstat
		    ON t.train_key       = tjstat.train_key
		    AND j.jock_key       = tjstat.jock_key
		    AND tjstat.stat_type = 'ALL_RACES_JT'
		    AND tjstat.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM trainer_jockey_stats a2
		        WHERE a2.train_key = t.train_key
		          AND a2.jock_key  = j.jock_key
		          AND a2.stat_type = 'ALL_RACES_JT'
		          AND a2.as_of_date <= r.race_date
		    )
        LEFT JOIN jock_accum_stats_by_track jtrack
		    ON j.jock_key         = jtrack.jock_key
		    AND r2.course_cd      = jtrack.course_cd
		    AND jtrack.stat_type  = 'ALL_RACES_J_TRACK'
		    AND jtrack.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM jock_accum_stats_by_track a2
		        WHERE a2.jock_key   = j.jock_key
		          AND a2.course_cd  = r2.course_cd
		          AND a2.stat_type  = 'ALL_RACES_J_TRACK'
		          AND a2.as_of_date <= r.race_date
		    )
		LEFT JOIN trainer_accum_stats_by_track ttrack
		    ON t.train_key        = ttrack.train_key
		    AND r2.course_cd      = ttrack.course_cd
		    AND ttrack.stat_type  = 'ALL_RACES_T_TRACK'
		    AND ttrack.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM trainer_accum_stats_by_track a2
		        WHERE a2.train_key = t.train_key
		          AND a2.course_cd = r2.course_cd
		          AND a2.stat_type= 'ALL_RACES_T_TRACK'
		          AND a2.as_of_date <= r.race_date
		    )
		LEFT JOIN jt_accum_stats_by_track jttrack
		    ON j.jock_key         = jttrack.jock_key
		    AND t.train_key       = jttrack.train_key
		    AND r2.course_cd      = jttrack.course_cd
		    AND jttrack.stat_type = 'ALL_RACES_JT_TRACK'
		    AND jttrack.as_of_date = (
		        SELECT MAX(a2.as_of_date)
		        FROM jt_accum_stats_by_track a2
		        WHERE a2.jock_key   = j.jock_key
		          AND a2.train_key  = t.train_key
		          AND a2.course_cd  = r2.course_cd
		          AND a2.stat_type  = 'ALL_RACES_JT_TRACK'
		          AND a2.as_of_date <= r.race_date
		    )
        WHERE re.official_fin is not null
            AND r2.breed_type = 'TB'
        """
    }
    return queries