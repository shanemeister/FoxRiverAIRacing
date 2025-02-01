# Define SQL queries without trailing semicolons
def full_query_df():
    # Define SQL queries without trailing semicolons
    queries = {
        "speed_figure": """
            SELECT
                re.official_fin, 
                r.course_cd,
                c.track_name,
                r.race_date,
                r.race_number,
                h.axciskey, 
                h.horse_id, 
                h.horse_name, 
                r2.saddle_cloth_number, 
                sa.running_time - r.rr_win_time AS time_behind, 
                sa.running_time - r.rr_par_time AS pace_delta_time, 
                re.speed_rating, 
                r2.prev_speed_rating,
                ROUND(r.distance_meters) as distance_meters, 
                ROUND(r2.previous_distance), 
                r.class_rating,                
                r2.previous_class,
                r.surface,
                r2.previous_surface, 
                r2.power,                  
                r2.off_finish_last_race, 
                r2.race_count, 
                r.trk_cond
            FROM races r
            JOIN runners r2 ON r.course_cd = r2.course_cd 
                AND r.race_date = r2.race_date 
                AND r.race_number = r2.race_number 
            left JOIN results_entries re ON r2.course_cd = re.course_cd 
                AND r2.race_date = re.race_date 
                AND r2.race_number = re.race_number
                AND r2.saddle_cloth_number = re.program_num
            JOIN sectionals_aggregated sa ON r2.course_cd = sa.course_cd 
                AND r2.race_date = sa.race_date 
                AND r2.race_number = sa.race_number
                AND r2.saddle_cloth_number = sa.saddle_cloth_number
            JOIN horse h ON r2.axciskey = h.axciskey 
            join course c on r.course_cd = c.course_cd
            where re.official_fin is not null 
		        AND c.course_cd IN (
		        'CNL','SAR','PIM','TSA','BEL','MVR','TWO','CLS','KEE','TAM','TTP','TKD', 
		        'ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP','TGG','CBY','LRL', 
		        'TED','IND','CTD','ASD','TCD','LAD','TOP')
        	ORDER BY c.course_cd, r.race_date, r.race_number, r2.saddle_cloth_number
        """
    }
    return queries