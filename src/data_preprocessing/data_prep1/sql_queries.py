# Define SQL queries without trailing semicolons
def sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "sectional_results": """
                SELECT h.horse_id, vre.official_fin, s.course_cd, s.race_date, s.race_number, s.saddle_cloth_number, s.gate_name , s.gate_numeric , s.length_to_finish , s.sectional_time , s.running_time , s.distance_back , s.distance_ran , s.number_of_strides ,vre.post_pos,
                        vre.speed_rating, vr.turf_mud_mark , vr.weight , vr.morn_odds , vr.avgspd , r.surface , r.trk_cond , r.class_rating ,  r.weather , r.wps_pool ,  r.stk_clm_md , r.todays_cls , vr.net_sentiment  
                    FROM sectionals s 
                    left join v_results_entries vre on s.course_cd = vre.course_cd 
                        and s.race_date = vre.race_date 
                        and s.race_number = vre.race_number 
                        and s.saddle_cloth_number = vre.program_num 
                    left JOIN v_runners vr 
                        ON vre.course_cd = vr.course_cd
                        AND vre.race_date = vr.race_date
                        AND vre.race_number = vr.race_number
                        AND vre.program_num = vr.saddle_cloth_number
                    join v_races r on vre.course_cd = r.course_cd
                        AND vre.race_date = r.race_date
                        AND vre.race_number = r.race_number
                    JOIN horse h 
                        ON vre.axciskey = h.axciskey
                    WHERE vre.breed = 'TB'
           """,
           "results": """
                    select h.horse_id , re.course_cd , re.race_date , re.race_number , re.program_num  as saddle_cloth_number, re.official_fin , re2.split_num, re2.earnings , r.purse , r.wps_pool , re.dollar_odds ,
                        re.weight , h.foal_date as date_of_birth, h.sex, re.horse_name , re.start_position , re.equip ,re.claim_price , r.surface , ts.surface_type_description , r.trk_cond , tc.description as trk_cond_desc , r.weather , r.distance , r.dist_unit ,
                        r2.morn_odds as derived_favorite, r.race_type , r.class_rating , re.speed_rating, vs.total_race_time, vs.total_strides, vs.avg_stride_length, r2.net_sentiment , r.stk_clm_md , r2.turf_mud_mark, j.jock_id, j.jock_name, t.train_id, t.train_name
                    from v_races r
                    left join v_results_entries re on r.course_cd = re.course_cd 
                        and r.race_date = re.race_date 
                        and r.race_number = re.race_number
                    left join v_runners r2 on re.course_cd = r2.course_cd 
                        and re.race_date = r2.race_date 
                        and re.race_number = r2.race_number
                        and re.program_num = r2.saddle_cloth_number 
                    left join v_sectionals_agg vs ON re.course_cd = vs.course_cd 
                        and re.race_date = vs.race_date 
                        and re.race_number = vs.race_number
                        and re.program_num = vs.saddle_cloth_number
                    left join jockey j on re.jock_key = j.jock_key 
                    left join trainer t on re.train_key = t.train_key
                    left join track_conditions tc on r.trk_cond = tc.code 
                    left join track_surface ts on r.surface = ts.surface_type_code 
                    left join horse h on re.axciskey = h.axciskey 
                    left join v_results_earnings re2 on re.course_cd = re2.course_cd 
                        and re.race_date = re2.race_date 
                        and re.race_number = re2.race_number 
                        and re.official_fin = re2.split_num 
                    where re.official_fin > 0
                    and re.breed = 'TB'
                    order by re.official_fin
                    """,
        "gpspoint": """
            SELECT course_cd, race_date, race_number, saddle_cloth_number, time_stamp, 
                longitude, latitude, speed, progress, stride_frequency, post_time, location
            FROM v_gpspoint
        """
    }
    return queries