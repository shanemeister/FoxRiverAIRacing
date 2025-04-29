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
                CONCAT(r.course_cd, '_', r.race_date, '_', r.race_number) AS race_id,
                r.post_time AS post_time,
                UPPER(TRIM(r2.saddle_cloth_number)) AS saddle_cloth_number,
                h.horse_id AS horse_id,
                h.horse_name AS horse_name,
                re.official_fin AS official_fin, -- official finish position, use as target possibly
                r.rr_par_time AS par_time,  -- par_time for a race track pre-computed
                r2.post_position AS post_position,
                r2.avg_purse_val_calc AS avg_purse_val,       -- av_pur_val Average Purse Value Calculation
                -- ============================
                -- HORSE-RELATED STATS
                -- ============================
                r2.prev_speed_rating AS prev_speed_rating,   -- (Horse) previous speed rating
                r2.previous_class AS previous_class,         -- (Horse) previous class level
                r2.weight AS weight,                         -- (Horse) carried weight
                h.foal_date AS date_of_birth,                -- (Horse) DOB
                TRIM(h.sex) AS sex,                          -- (Horse) sex/gender
                TRIM(r2.equip) AS equip,                     -- (Horse) equipment (blinkers, etc.)
                r2.claimprice AS claimprice,                 -- (Horse) claiming price if applicable
                r2.previous_distance AS previous_distance,   -- (Horse) distance of last race
                r2.previous_surface AS previous_surface,     -- (Horse) official finish last race
                r2.prev_official_fin AS prev_official_fin, -- (Horse) how far off the finish last race
                r2.power AS power,                           -- (Horse) power rating
                TRIM(r2.med) AS med,                         -- (Horse) medication usage
                r2.avgspd AS avgspd,                         -- (Horse) average speed figure
                r2.race_count AS starts,                     -- (Horse) total starts (shortcut alias)
                r2.avg_spd_sd AS avg_spd_sd,                 -- (Horse) standard deviation of avg speeds
                r2.ave_cl_sd AS ave_cl_sd,                   -- (Horse) standard deviation of avg class
                r2.hi_spd_sd AS hi_spd_sd,                   -- (Horse) standard deviation of highest speed
                r2.pstyerl AS pstyerl,                       -- (Horse) previous year stats (?)
                -- =============================
                -- End of Sectionals LSTM
                -- =============================
                r.purse AS purse,                            -- (Race) purse amount
                TRIM(r.surface) AS surface,                  -- (Race) current surface
                r.distance_meters AS distance_meters,        -- (Race) current race distance
                r.class_rating AS class_rating,              -- (Race) class rating
                tc.code AS trk_cond,                         -- (Race) track condition code
                r2.morn_odds AS morn_odds,                   -- (Race) morning-line odds (horse+race specific)
                TRIM(r.race_type) AS race_type,              -- (Race) type (stakes, allowance, etc.)
                TRIM(r.stk_clm_md) AS stk_clm_md,            -- (Race) whether stakes, claiming, maiden, etc.
                TRIM(r2.turf_mud_mark) AS turf_mud_mark,     -- (Race) special mark for turf or mud conditions
                -- CASE 
                --    WHEN r.race_date < CURRENT_DATE THEN 'historical'
                --    ELSE 'future'
                -- END AS data_flag,                            -- (Race) indicates if race is past or upcoming
                -- ============================
                -- JOCKEY / TRAINER STATS
                -- ============================
                jast_j.win_percentage AS jock_win_percent,    -- (Jockey) overall win %
                jast_j.itm_percentage AS jock_itm_percent,    -- (Jockey) in-the-money %
                tast_t.win_percentage AS trainer_win_percent, -- (Trainer) overall win %
                tast_t.itm_percentage AS trainer_itm_percent, -- (Trainer) in-the-money %
                tjstat.win_percentage AS jt_win_percent,      -- (Jockey+Trainer) combined win %
                tjstat.itm_percentage AS jt_itm_percent,      -- (Jockey+Trainer) combined ITM %
                jtrack.win_percentage AS jock_win_track,      -- (Jockey) track-specific win %
                jtrack.itm_percentage AS jock_itm_track,      -- (Jockey) track-specific ITM %
                ttrack.win_percentage AS trainer_win_track,   -- (Trainer) track-specific win %
                ttrack.itm_percentage AS trainer_itm_track,   -- (Trainer) track-specific ITM %
                jttrack.win_percentage AS jt_win_track,       -- (Jockey+Trainer) track-specific win %
                jttrack.itm_percentage AS jt_itm_track,       -- (Jockey+Trainer) track-specific ITM %               
                -- ============================
                -- SIRE/DAM (PEDIGREE) STATS
                -- ============================
                CASE WHEN s.starts=0 THEN 0 
                     ELSE (s.wins+s.places+s.shows)/s.starts 
                END AS sire_itm_percentage,   -- (Sire) in-the-money %
                s.roi AS sire_roi,            -- (Sire) return on investment
                CASE WHEN d.starts=0 THEN 0 
                     ELSE (d.wins+d.places+d.shows)/d.starts 
                END AS dam_itm_percentage,    -- (Dam) in-the-money %
                d.roi AS dam_roi,            -- (Dam) return on investment
                -- ============================
                -- HORSE AGGREGATE STATS (All / Condition)
                -- ============================
                has_all.starts AS all_starts,     -- (Horse) total career starts
                has_all.win AS all_win,           -- (Horse) total career wins
                has_all.place AS all_place,       -- (Horse) total career places
                has_all.show AS all_show,         -- (Horse) total career shows
                has_all.fourth AS all_fourth,     -- (Horse) total 4th place finishes
                has_all.earnings AS all_earnings, -- (Horse) total earnings
                COALESCE(has_all.itm_percentage,0) AS horse_itm_percentage, -- (Horse) overall ITM %
                -- ---------------------------
                has_cond.starts AS cond_starts,   -- (Horse) starts under certain condition
                has_cond.win AS cond_win,         -- (Horse) wins under certain condition
                has_cond.place AS cond_place,     -- (Horse) places under certain condition
                has_cond.show AS cond_show,       -- (Horse) shows under certain condition
                has_cond.fourth AS cond_fourth,   -- (Horse) 4th place under condition
                has_cond.earnings AS cond_earnings, -- (Horse) earnings under condition             
                -- ============================
                -- NET SENTIMENT (MISC)
                -- ============================
                r2.net_sentiment AS net_sentiment, -- (Misc) sentiment analysis (?)
                -- ============================
                -- 5-RACE FORM / HISTORICAL FORM / NOT INCLUDING SECTIONALS OR GPS
                -- ============================
                hrf.total_races_5 AS total_races_5,          -- (5-Race) # races considered
                hrf.avg_fin_5 AS avg_fin_5,                  -- (5-Race) avg finishing position
                hrf.best_speed AS best_speed,                 -- (5-Race) best speed figure among last 5
                hrf.first_race_date_5 AS first_race_date_5,   -- (5-Race) earliest race date in last 5
                hrf.most_recent_race_5 AS most_recent_race_5, -- (5-Race) date of the most recent race
                hrf.prev_speed AS prev_speed,                     -- (5-Race) prior speed rating
                hrf.speed_improvement AS speed_improvement,       -- (5-Race) improvement vs. prior race
                hrf.prev_race_date AS prev_race_date,             -- (5-Race) date of previous race
                hrf.days_off AS days_off,                         -- (5-Race) days since last race
                hrf.layoff_cat AS layoff_cat,                     -- (5-Race) category of layoff
                hrf.avg_workout_rank_3 AS avg_workout_rank_3,     -- (5-Race) average workout rank
                hrf.count_workouts_3 AS count_workouts_3,         -- (5-Race) # of workouts in that period               
                -- ============================
                -- TRACK NAME
                -- ============================
                c.track_name AS track_name   -- (Could be race-level or general info)
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
            AND c.track_name IS NOT NULL
            AND re.official_fin IS NOT NULL
        """
    }
    return queries
