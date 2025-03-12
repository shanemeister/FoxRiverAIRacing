# Define SQL queries without trailing semicolons
def sql_queries():
    queries = {
        "training_data": 
        """
        SELECT
                r2.axciskey AS axciskey,
                UPPER(TRIM(r.course_cd)) AS course_cd,
                r.race_date AS race_date,
                r.race_number AS race_number,
                r.post_time AS post_time,
                UPPER(TRIM(r2.saddle_cloth_number)) AS saddle_cloth_number,
                h.horse_id AS horse_id,
                h.horse_name AS horse_name,
                re.official_fin AS official_fin,
                r.rr_par_time AS par_time,
                sa.running_time_prev as running_time,
                sa.total_distance_ran_prev as total_distance_ran,
                sa.avgtime_gate1_prev as avgtime_gate1,
                sa.avgtime_gate2_prev as avgtime_gate2,
                sa.avgtime_gate3_prev as avgtime_gate3,
                sa.avgtime_gate4_prev as avgtime_gate4,
                sa.dist_bk_gate1_prev as dist_bk_gate1,
                sa.dist_bk_gate2_prev as dist_bk_gate2,
                sa.dist_bk_gate3_prev as dist_bk_gate3,
                sa.dist_bk_gate4_prev as dist_bk_gate4,
                r2.prev_speed_rating AS prev_speed_rating,
                r2.previous_class AS previous_class,
                r.purse AS purse,
                r2.weight AS weight,
                h.foal_date AS date_of_birth,
                TRIM(h.sex) AS sex,
                TRIM(r2.equip) AS equip,
                r2.claimprice AS claimprice,
                TRIM(r.surface) AS surface,
                r.distance_meters AS distance_meters,
                r.class_rating AS class_rating,
                r2.previous_distance AS previous_distance,
                r2.previous_surface AS previous_surface,
                r2.off_finish_last_race AS off_finish_last_race,
                r2.power AS power,
                tc.code AS trk_cond,
                TRIM(r2.med) AS med,
                r2.morn_odds AS morn_odds,
                r2.avgspd AS avgspd,
                r2.race_count AS starts,
                TRIM(r.race_type) AS race_type,
                r2.net_sentiment AS net_sentiment,
                TRIM(r.stk_clm_md) AS stk_clm_md,
                TRIM(r2.turf_mud_mark) AS turf_mud_mark,
                r2.avg_spd_sd AS avg_spd_sd,
                r2.ave_cl_sd AS ave_cl_sd,
                r2.hi_spd_sd AS hi_spd_sd,
                r2.pstyerl AS pstyerl,
                has_all.starts AS all_starts,
                has_all.win AS all_win,
                has_all.place AS all_place,
                has_all.show AS all_show,
                has_all.fourth AS all_fourth,
                has_all.earnings AS all_earnings,
                COALESCE(has_all.itm_percentage,0) AS horse_itm_percentage,
                has_cond.starts AS cond_starts,
                has_cond.win AS cond_win,
                has_cond.place AS cond_place,
                has_cond.show AS cond_show,
                has_cond.fourth AS cond_fourth,
                has_cond.earnings AS cond_earnings,
                jast_j.win_percentage AS jock_win_percent,
                jast_j.itm_percentage AS jock_itm_percent,
                tast_t.win_percentage AS trainer_win_percent,
                tast_t.itm_percentage AS trainer_itm_percent,
                tjstat.win_percentage AS jt_win_percent,
                tjstat.itm_percentage AS jt_itm_percent,
                jtrack.win_percentage AS jock_win_track,
                jtrack.itm_percentage AS jock_itm_track,
                ttrack.win_percentage AS trainer_win_track,
                ttrack.itm_percentage AS trainer_itm_track,
                jttrack.win_percentage AS jt_win_track,
                jttrack.itm_percentage AS jt_itm_track,
                CASE WHEN s.starts=0 THEN 0 ELSE (s.wins+s.places+s.shows)/s.starts END AS sire_itm_percentage,
                s.roi AS sire_roi,
                CASE WHEN d.starts=0 THEN 0 ELSE (d.wins+d.places+d.shows)/d.starts END AS dam_itm_percentage,
                d.roi AS dam_roi,
                hrf.total_races_5 AS total_races_5,
                hrf.avg_fin_5 AS avg_fin_5,
                hrf.avg_speed_5 AS avg_speed_5,
                hrf.best_speed AS best_speed,
                hrf.avg_beaten_len_5 AS avg_beaten_len_5,
                hrf.first_race_date_5 AS first_race_date_5,
                hrf.most_recent_race_5 AS most_recent_race_5,
                hrf.avg_dist_bk_gate1_5 AS avg_dist_bk_gate1_5,
                hrf.avg_dist_bk_gate2_5 AS avg_dist_bk_gate2_5,
                hrf.avg_dist_bk_gate3_5 AS avg_dist_bk_gate3_5,
                hrf.avg_dist_bk_gate4_5 AS avg_dist_bk_gate4_5,
                hrf.avg_speed_fullrace_5 AS avg_speed_fullrace_5,
                hrf.avg_stride_length_5 AS avg_stride_length_5,
                hrf.avg_strfreq_q1_5 AS avg_strfreq_q1_5,
                hrf.avg_strfreq_q2_5 AS avg_strfreq_q2_5,
                hrf.avg_strfreq_q3_5 AS avg_strfreq_q3_5,
                hrf.avg_strfreq_q4_5 AS avg_strfreq_q4_5,
                hrf.prev_speed AS prev_speed,  -- previous speed rating
                hrf.speed_improvement AS speed_improvement,
                hrf.prev_race_date AS prev_race_date,
                hrf.days_off AS days_off,
                hrf.layoff_cat AS layoff_cat,
                hrf.avg_workout_rank_3 AS avg_workout_rank_3,
                hrf.count_workouts_3 AS count_workouts_3,
                r2.race_count AS race_count,
                c.track_name AS track_name,
                CASE 
                    WHEN (hrf.avg_speed_5 IS NOT NULL 
                        AND hrf.best_speed IS NOT NULL 
                        AND hrf.avg_beaten_len_5 IS NOT NULL 
                        AND hrf.first_race_date_5 IS NOT NULL 
                        AND hrf.most_recent_race_5 IS NOT NULL 
                        AND hrf.avg_dist_bk_gate1_5 IS NOT NULL 
                        AND hrf.avg_dist_bk_gate2_5 IS NOT NULL 
                        AND hrf.avg_dist_bk_gate3_5 IS NOT NULL 
                        AND hrf.avg_dist_bk_gate4_5 IS NOT NULL 
                        AND hrf.avg_speed_fullrace_5 IS NOT NULL 
                        AND hrf.avg_stride_length_5 IS NOT NULL 
                        AND hrf.avg_strfreq_q1_5 IS NOT NULL 
                        AND hrf.avg_strfreq_q2_5 IS NOT NULL 
                        AND hrf.avg_strfreq_q3_5 IS NOT NULL 
                        AND hrf.avg_strfreq_q4_5 IS NOT NULL 
                        AND hrf.prev_speed IS NOT NULL 
                        AND hrf.speed_improvement IS NOT NULL) 
                    THEN 1 ELSE 0 END AS has_gps,
                CASE 
                    WHEN r.race_date < CURRENT_DATE THEN 'historical'
                ELSE 'future'
                END AS data_flag
            FROM races r
            JOIN runners r2 
                ON r.course_cd = r2.course_cd 
                AND r.race_date = r2.race_date 
                AND r.race_number = r2.race_number
            LEFT JOIN results_entries re 
                ON r2.course_cd = re.course_cd 
                AND r2.race_date = re.race_date 
                AND r2.race_number = re.race_number 
                AND r2.saddle_cloth_number = re.program_num 
                AND r2.axciskey = re.axciskey
            JOIN horse h 
                ON r2.axciskey = h.axciskey
            LEFT JOIN LATERAL(
                        SELECT h2.* 
                        FROM horse_form_agg h2 
                        WHERE h2.horse_id = h.horse_id 
                        AND CAST(h2.as_of_date AS date) <= CAST(r.race_date AS date)
                        ORDER BY CAST(h2.as_of_date AS date) DESC 
                        LIMIT 1
                ) hrf ON true
            LEFT JOIN stat_sire s ON h.axciskey=s.axciskey 
                AND s.type='LIFETIME'
            LEFT JOIN stat_dam d ON h.axciskey=d.axciskey 
                AND d.type='LIFETIME'            
            LEFT JOIN LATERAL (
                                SELECT s.*
                                FROM sectionals_aggregated_locf s
                                WHERE s.horse_id = h.horse_id
                                    AND s.as_of_date <= r2.race_date
                                ORDER BY s.as_of_date DESC
                                LIMIT 1
                                ) sa ON TRUE
            LEFT JOIN horse_accum_stats has_all ON has_all.axciskey=r2.axciskey 
                AND has_all.stat_type='ALL_RACES' 
                AND has_all.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM horse_accum_stats a2 
                                        WHERE a2.axciskey=r2.axciskey 
                                        AND a2.stat_type='ALL_RACES'
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN horse_accum_stats has_cond ON has_cond.axciskey=r2.axciskey 
                AND has_cond.stat_type=CASE WHEN r.surface='D' 
                AND r.trk_cond='MY' 
                AND r.distance_meters<=1409 THEN 'MUDDY_SPRNT' 
                        WHEN r.surface='D' AND r.trk_cond='MY' 
                        AND r.distance_meters>=1409 THEN 'MUDDY_RTE' 
                        WHEN r.surface='D' AND r.distance_meters<=1409 THEN 'DIRT_SPRNT' 
                        WHEN r.surface='D' AND r.distance_meters>1409 THEN 'DIRT_RTE' 
                        WHEN r.surface='T' AND r.distance_meters<=1409 THEN 'TURF_SPRNT' 
                        WHEN r.surface='T' AND r.distance_meters>1409 THEN 'TURF_RTE' 
                        WHEN r.surface='A' AND r.distance_meters<=1409 THEN 'ALL_WEATHER_SPRNT' 
                        WHEN r.surface='A' AND r.distance_meters>1409 THEN 'ALL_WEATHER_RTE' 
                        WHEN r.surface='D' AND r.race_type='Allowance' AND r.distance_meters<=1409 THEN 'ALLOWANCE_SPRNT'
                        WHEN r.surface='D' AND r.race_type='Allowance' AND r.distance_meters>1409 THEN 'ALLOWANCE_RTE' 
                        WHEN r.surface='D' AND r.race_type='Claiming' AND r.distance_meters<=1409 THEN 'CLAIMING_SPRNT' 
                        WHEN r.surface='D' AND r.race_type='Claiming' AND r.distance_meters>1409 THEN 'CLAIMING_RTE' 
                        WHEN r.surface='D' AND r.race_type='Stakes' AND r.distance_meters<=1409 THEN 'STAKES_SPRNT' 
                        WHEN r.surface='D' AND r.race_type='Stakes' AND r.distance_meters>1409 THEN 'STAKES_RTE' 
                        ELSE NULL END AND has_cond.as_of_date=(SELECT MAX(a2.as_of_date) 
                                                                FROM horse_accum_stats a2 
                                                                WHERE a2.axciskey=r2.axciskey 
                                                                AND a2.stat_type=CASE WHEN r.surface='D' 
                                                                AND r.trk_cond='MY' 
                                                                AND r.distance_meters<=1409 THEN 'MUDDY_SPRNT' 
                                                                WHEN r.surface='D' AND r.trk_cond='MY' 
                                                                AND r.distance_meters>=1409 THEN 'MUDDY_RTE' 
                                                                WHEN r.surface='D' AND r.distance_meters<=1409 
                                                                THEN 'DIRT_SPRNT' WHEN r.surface='D' 
                                                                AND r.distance_meters>1409 THEN 'DIRT_RTE' 
                                                                WHEN r.surface='T' AND r.distance_meters<=1409 
                                                                THEN 'TURF_SPRNT' WHEN r.surface='T' 
                                                                AND r.distance_meters>1409 THEN 'TURF_RTE' 
                                                                WHEN r.surface='A' AND r.distance_meters<=1409 
                                                                THEN 'ALL_WEATHER_SPRNT' WHEN r.surface='A' 
                                                                AND r.distance_meters>1409 THEN 'ALL_WEATHER_RTE' 
                                                                WHEN r.surface='D' AND r.race_type='Allowance' 
                                                                AND r.distance_meters<=1409 THEN 'ALLOWANCE_SPRNT' 
                                                                WHEN r.surface='D' AND r.race_type='Allowance' 
                                                                AND r.distance_meters>1409 THEN 'ALLOWANCE_RTE' 
                                                                WHEN r.surface='D' AND r.race_type='Claiming' 
                                                                AND r.distance_meters<=1409 THEN 'CLAIMING_SPRNT' 
                                                                WHEN r.surface='D' AND r.race_type='Claiming' 
                                                                AND r.distance_meters>1409 THEN 'CLAIMING_RTE' 
                                                                WHEN r.surface='D' AND r.race_type='Stakes' 
                                                                AND r.distance_meters<=1409 THEN 'STAKES_SPRNT' 
                                                                WHEN r.surface='D' AND r.race_type='Stakes' 
                                                                AND r.distance_meters>1409 THEN 'STAKES_RTE' 
                                                                ELSE NULL END AND a2.as_of_date<=r.race_date)  
            LEFT JOIN jockey j ON r2.jock_key=j.jock_key
            LEFT JOIN trainer t ON r2.train_key=t.train_key
            LEFT JOIN jock_accum_stats jast_j ON j.jock_key=jast_j.jock_key 
                AND jast_j.stat_type='ALL_RACES_J' 
                AND jast_j.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM jock_accum_stats a2 
                                        WHERE a2.jock_key=j.jock_key 
                                        AND a2.stat_type='ALL_RACES_J'
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN trainer_accum_stats tast_t ON t.train_key=tast_t.train_key 
                AND tast_t.stat_type='ALL_RACES_T' 
                AND tast_t.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM trainer_accum_stats a2 
                                        WHERE a2.train_key=t.train_key 
                                        AND a2.stat_type='ALL_RACES_T' 
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN trainer_jockey_stats tjstat ON t.train_key=tjstat.train_key 
                AND j.jock_key=tjstat.jock_key AND tjstat.stat_type='ALL_RACES_JT' 
                AND tjstat.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM trainer_jockey_stats a2 
                                        WHERE a2.train_key=t.train_key 
                                        AND a2.jock_key=j.jock_key 
                                        AND a2.stat_type='ALL_RACES_JT' 
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN jock_accum_stats_by_track jtrack ON j.jock_key=jtrack.jock_key 
                AND r2.course_cd=jtrack.course_cd 
                AND jtrack.stat_type='ALL_RACES_J_TRACK' 
                AND jtrack.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM jock_accum_stats_by_track a2 
                                        WHERE a2.jock_key=j.jock_key 
                                        AND a2.course_cd=r2.course_cd 
                                        AND a2.stat_type='ALL_RACES_J_TRACK' 
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN trainer_accum_stats_by_track ttrack ON t.train_key=ttrack.train_key 
                AND r2.course_cd=ttrack.course_cd 
                AND ttrack.stat_type='ALL_RACES_T_TRACK' 
                AND ttrack.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM trainer_accum_stats_by_track a2 
                                        WHERE a2.train_key=t.train_key 
                                        AND a2.course_cd=r2.course_cd 
                                        AND a2.stat_type='ALL_RACES_T_TRACK' 
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN jt_accum_stats_by_track jttrack ON j.jock_key=jttrack.jock_key 
                AND t.train_key=jttrack.train_key 
                AND r2.course_cd=jttrack.course_cd 
                AND jttrack.stat_type='ALL_RACES_JT_TRACK' 
                AND jttrack.as_of_date=(SELECT MAX(a2.as_of_date) 
                                        FROM jt_accum_stats_by_track a2 
                                        WHERE a2.jock_key=j.jock_key 
                                        AND a2.train_key=t.train_key 
                                        AND a2.course_cd=r2.course_cd 
                                        AND a2.stat_type='ALL_RACES_JT_TRACK' 
                                        AND a2.as_of_date<=r.race_date)
            LEFT JOIN track_conditions tc ON r.trk_cond=tc.code
            JOIN course c ON r.course_cd=c.course_cd
            WHERE r2.breed_type='TB'
            AND c.course_cd <> 'CMR'
        """
    }
    return queries
