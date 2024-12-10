# Define SQL queries without trailing semicolons
def sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "results": """
            SELECT h.horse_id, vre.official_fin, vre.course_cd, vre.race_date, vre.race_number, vre.program_num AS saddle_cloth_number, vre.post_pos,
                vre.speed_rating, vr.turf_mud_mark , vr.weight , vr.morn_odds , vr.avgspd , r.surface , r.trk_cond , r.class_rating ,  r.weather , r.wps_pool ,  r.stk_clm_md , r.todays_cls , vr.net_sentiment  
            FROM v_results_entries vre
            JOIN v_runners vr 
                ON vre.course_cd = vr.course_cd
                AND vre.race_date = vr.race_date
                AND vre.race_number = vr.race_number
                AND vre.program_num = vr.saddle_cloth_number
            join races r on vre.course_cd = r.course_cd
                AND vre.race_date = r.race_date
                AND vre.race_number = r.race_number
            JOIN horse h 
                ON vre.axciskey = h.axciskey
            WHERE vre.breed = 'TB'
            GROUP BY h.horse_id, vre.official_fin, vre.course_cd, vre.race_date, vre.race_number, vre.program_num, vre.post_pos,
                vre.speed_rating, vr.turf_mud_mark , vr.weight , vr.morn_odds , vr.avgspd , r.surface , r.trk_cond , r.class_rating ,  r.weather , r.wps_pool ,  r.stk_clm_md , r.todays_cls , vr.net_sentiment 
        """,
        "sectionals": """
            SELECT course_cd, race_date, race_number, saddle_cloth_number, gate_name, 
                gate_numeric, length_to_finish, sectional_time, running_time, 
                distance_back, distance_ran, number_of_strides
            FROM v_sectionals
        """,
        "gpspoint": """
            SELECT course_cd, race_date, race_number, saddle_cloth_number, time_stamp, 
                longitude, latitude, speed, progress, stride_frequency, post_time, location
            FROM v_gpspoint
        """
    }
    return queries