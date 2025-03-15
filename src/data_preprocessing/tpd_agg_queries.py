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
        """,
        "runners": """
            SELECT REGEXP_REPLACE(TRIM(UPPER(course_cd)), '\s+$', '') AS course_cd, race_date, race_number, REGEXP_REPLACE(TRIM(UPPER(saddle_cloth_number)), '\s+$', '') AS saddle_cloth_number, 
                country, axciskey, post_position, todays_cls, owner_name, turf_mud_mark, avg_purse_val_calc, weight, wght_shift, cldate, price, bought_fr, power, med, equip, morn_odds, breeder, 
                ae_flag, power_symb, horse_comm, breed_type, lst_salena, lst_salepr, lst_saleda, claimprice, avgspd, avgcls, apprweight, jock_key, train_key, post_time, previous_surface, 
                previous_class, net_sentiment, avg_spd_sd, ave_cl_sd,hi_spd_sd, pstyerl, previous_distance, off_finish_last_race, race_count, prev_speed_rating, gps_rawspeed, gcsf_speed_figure
            FROM runners
        """,
        "horse": """
            SELECT axciskey, horse_name, foal_date, sex, wh_foaled, color, horse_id
            FROM horse
        """
    }
    return queries