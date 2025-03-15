# Define SQL queries without trailing semicolons
def form_sql_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "races": """
            select r.course_cd, r.race_date, r.race_number, r2.saddle_cloth_number, h.horse_id, r2.todays_cls , re.official_fin , re.speed_rating , sa.running_time , sa.dist_bk_gate1, sa.dist_bk_gate2, sa.dist_bk_gate3, sa.dist_bk_gate4 , ga.avg_speed_fullrace , ga.avg_stride_length, ga.strfreq_q1 , ga.strfreq_q2 , ga.strfreq_q3 , ga.strfreq_q4 
            from races r 
            join runners r2 on r.course_cd = r2.course_cd
                and r.race_date = r2.race_date
                and r.race_number = r2.race_number
            join results_entries re on r2.course_cd = re.course_cd 
                and r2.race_date = re.race_date 
                and r2.race_number = re.race_number
                and r2.saddle_cloth_number = re.program_num 
            left join sectionals_aggregated sa on r2.course_cd = sa.course_cd 
                and r2.race_date = sa.race_date 
                and r2.race_number = sa.race_number 
                and r2.saddle_cloth_number = sa.saddle_cloth_number 
            left join gps_aggregated ga on sa.course_cd = ga.course_cd 
                and sa.race_date = ga.race_date 
                and sa.race_number = ga.race_number 
                and sa.saddle_cloth_number = ga.saddle_cloth_number 
            join horse h on r2.axciskey = h.axciskey 
            where r2.breed_type = 'TB'
            --and re.official_fin is not null
        """,
        "workouts": """
            SELECT w.course_cd, w.race_date, w.race_number, w.saddle_cloth_number, h.horse_id, w.worknum,days_back, w.worktext, w.ranking, w.rank_group
            FROM workoutdata w
            JOIN runners r ON w.course_cd = r.course_cd
                and w.race_date = r.race_date
                and w.race_number = r.race_number
                and w.saddle_cloth_number = r.saddle_cloth_number
            join horse h on r.axciskey = h.axciskey
            WHERE r.breed_type = 'TB'  
        """
    }
    return queries