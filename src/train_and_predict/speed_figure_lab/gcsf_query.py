# Define SQL queries without trailing semicolons
def gcsp_sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "gpspoint": """
            SELECT h.horse_id, g.course_cd, g.race_date, g.race_number, REGEXP_REPLACE(TRIM(UPPER(g.saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number , 
            g."time_stamp", g.speed, g.progress, g.stride_frequency, r.distance_meters as official_distance
            FROM races r 
            JOIN runners r2 on r.course_cd = r2.course_cd 
                and r.race_date = r2.race_date 
                and r.race_number = r2.race_number      
            JOIN gpspoint g on r2.course_cd = g.course_cd 
                and r2.race_date = g.race_date 
                and r2.race_number = g.race_number 
                and r2.saddle_cloth_number = g.saddle_cloth_number
            JOIN horse h on r2.axciskey = h.axciskey
            WHERE g.stride_frequency is not null 
            GROUP BY h.horse_id, g.course_cd , g.race_date , g.race_number , g.saddle_cloth_number , g."time_stamp", r.distance_meters 
            ORDER BY  g.course_cd , g.race_date , g.race_number , g.saddle_cloth_number , h.horse_id, g."time_stamp"
        """,
        "sectionals": """
                SELECT h.horse_id, s.course_cd, s.race_date, s.race_number,  REGEXP_REPLACE(TRIM(UPPER(s.saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, s.gate_name , s.sectional_time , s.length_to_finish , 
                s.running_time , s.distance_back , s.distance_ran , s.number_of_strides, r.distance_meters as official_distance 
                FROM races r 
                JOIN runners r2 on r.course_cd = r2.course_cd 
                    and r.race_date = r2.race_date 
                    and r.race_number = r2.race_number 
                JOIN sectionals s on r2.course_cd = s.course_cd 
                    and r2.race_date = s.race_date 
                    and r2.race_number = s.race_number 
                    and r2.saddle_cloth_number = s.saddle_cloth_number 
                JOIN horse h on r2.axciskey = h.axciskey
                GROUP BY h.horse_id, s.course_cd , s.race_date , s.race_number , s.saddle_cloth_number , s.gate_numeric , s.gate_name, r.distance_meters
                ORDER BY s.course_cd , s.race_date , s.race_number , s.saddle_cloth_number , h.horse_id, s.gate_numeric
        """
    }
    return queries