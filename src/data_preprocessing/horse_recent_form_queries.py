# Define SQL queries without trailing semicolons
def form_sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "races": """
            select r.course_cd, r.race_date, r.race_number, re.program_num as saddle_cloth_number, h.horse_id, r.todays_cls , re.official_fin , re.speed_rating , sa.running_time , sa.dist_bk_gate1, sa.dist_bk_gate2, sa.dist_bk_gate3, sa.dist_bk_gate4 , ga.avg_speed_fullrace , ga.avg_stride_length, ga.strfreq_q1 , ga.strfreq_q2 , ga.strfreq_q3 , ga.strfreq_q4 
            from races r 
            join results_entries re on r.course_cd = re.course_cd 
                and r.race_date = re.race_date 
                and r.race_number = re.race_number 
            join sectionals_aggregated sa on re.course_cd = sa.course_cd 
                and re.race_date = sa.race_date 
                and re.race_number = sa.race_number 
                and re.program_num = sa.saddle_cloth_number 
            join gps_aggregated ga on sa.course_cd = ga.course_cd 
                and sa.race_date = ga.race_date 
                and sa.race_number = ga.race_number 
                and sa.saddle_cloth_number = ga.saddle_cloth_number 
            join horse h on re.axciskey = h.axciskey 
            where re.breed = 'TB'
        """,
        "workouts": """
            SELECT w.course_cd, w.race_date, w.race_number, w.saddle_cloth_number, h.horse_id, w.worknum,days_back, w.worktext, w.ranking, w.rank_group
            FROM workoutdata w
            JOIN results_entries re ON w.course_cd = re.course_cd
                and w.race_date = re.race_date
                and w.race_number = re.race_number
                and w.saddle_cloth_number = re.program_num
            join horse h on re.axciskey = h.axciskey
            WHERE re.breed = 'TB'  
        """
    }
    return queries