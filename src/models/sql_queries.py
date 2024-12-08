# Define SQL queries without trailing semicolons
def sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "results": """
            SELECT vre.course_cd, vre.race_date, vre.race_number, vre.program_num AS saddle_cloth_number, vre.post_pos,
                h.horse_id, vre.official_fin, vre.finish_time, vre.speed_rating, vr.todays_cls,
                vr.previous_surface, vr.previous_class, vr.net_sentiment
            FROM v_results_entries vre
            JOIN v_runners vr 
                ON vre.course_cd = vr.course_cd
                AND vre.race_date = vr.race_date
                AND vre.race_number = vr.race_number
                AND vre.program_num = vr.saddle_cloth_number
            JOIN horse h 
                ON vre.axciskey = h.axciskey
            WHERE vre.breed = 'TB'
            GROUP BY vre.course_cd, vre.race_date, vre.race_number, vre.program_num, vre.post_pos,
                    h.horse_id, vre.official_fin, vre.finish_time, vre.speed_rating, vr.todays_cls,
                    vr.previous_surface, vr.previous_class, vr.net_sentiment
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