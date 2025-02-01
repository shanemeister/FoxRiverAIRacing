# Define SQL queries without trailing semicolons
def full_query_df():
    # Define SQL queries without trailing semicolons
    queries = {
        "speed_figure": """
        WITH past_races AS (
            SELECT
                r.course_cd,
                r.race_date,
                r.race_number,
                h.axciskey, 
                h.horse_id, 
                h.horse_name, 
                r2.saddle_cloth_number, 
                ROUND(r.distance_meters) as distance_meters, 
                re.official_fin, 
                sa.running_time - r.rr_win_time AS time_behind, 
                sa.running_time - r.rr_par_time AS pace_delta_time, 
                re.speed_rating, 
                r.class_rating, 
                r2.power, 
                r2.previous_class,
                r.surface,
                r2.previous_surface, 
                r2.previous_distance, 
                r2.prev_speed_rating, 
                r2.off_finish_last_race, 
                r2.race_count, 
                r.trk_cond
            FROM races r
            JOIN runners r2 ON r.course_cd = r2.course_cd 
                AND r.race_date = r2.race_date 
                AND r.race_number = r2.race_number 
            JOIN results_entries re ON r2.course_cd = re.course_cd 
                AND r2.race_date = re.race_date 
                AND r2.race_number = re.race_number
                AND r2.saddle_cloth_number = re.program_num
            JOIN sectionals_aggregated sa ON r2.course_cd = sa.course_cd 
                AND r2.race_date = sa.race_date 
                AND r2.race_number = sa.race_number
                AND r2.saddle_cloth_number = sa.saddle_cloth_number
            JOIN horse h ON r2.axciskey = h.axciskey 
            WHERE r.race_date < CURRENT_DATE
            ORDER BY r.race_date
        ),
        future_races AS (
            SELECT 
                r.course_cd, 
                r.race_date, 
                r.race_number, 
                h.axciskey, 
                c.track_name, 
                h.horse_id, 
                h.horse_name, 
                r2.saddle_cloth_number,
                r.surface
            FROM races r
            JOIN runners r2 ON r.course_cd = r2.course_cd 
                AND r.race_date = r2.race_date 
                AND r.race_number = r2.race_number 
            JOIN horse h ON r2.axciskey = h.axciskey
            JOIN course c ON r.course_cd = c.course_cd
            WHERE r.race_date >= CURRENT_DATE
            order by r.race_date desc 
        )
        SELECT 
            fr.course_cd, 
            fr.track_name, 
            fr.race_date         AS current_race_date,
            fr.race_number,
            fr.axciskey,
            fr.horse_id, 
            fr.horse_name, 
            fr.saddle_cloth_number,
            pr.time_behind, 
            pr.pace_delta_time, 
            pr.speed_rating,
            pr.prev_speed_rating,
            ROUND(pr.previous_distance),
            ROUND(pr.distance_meters), 
            pr.class_rating, 
            pr.previous_class, 
            pr.surface,
            pr.previous_surface, 
            pr.power,
            pr.off_finish_last_race, 
            pr.race_count, 
            pr.trk_cond
        FROM future_races fr
        LEFT JOIN LATERAL (
            SELECT p.*
            FROM past_races p
            WHERE p.axciskey   = fr.axciskey
            -- We only want races strictly in the past relative to the future race:
            AND p.race_date < fr.race_date
            ORDER BY p.race_date DESC
            LIMIT 1
        ) pr ON true
        WHERE fr.course_cd IN (
        'CNL','SAR','PIM','TSA','BEL','MVR','TWO','CLS','KEE','TAM','TTP','TKD', 
        'ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP','TGG','CBY','LRL', 
        'TED','IND','CTD','ASD','TCD','LAD','TOP'
        )
        ORDER BY fr.course_cd, fr.race_date, fr.race_number, fr.saddle_cloth_number
        """
    }
    return queries