# Define SQL queries without trailing semicolons
def tpd_sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "gpspoint": """
            SELECT REGEXP_REPLACE(TRIM(UPPER(course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, time_stamp, 
                longitude, latitude, speed, progress, stride_frequency, post_time, location
            FROM gpspoint
        """,
        "sectionals": """
            SELECT REGEXP_REPLACE(TRIM(UPPER(course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, gate_name, gate_numeric,
                length_to_finish, sectional_time, running_time, distance_back, distance_ran,
                number_of_strides, post_time
            FROM sectionals
        """
    }
    return queries