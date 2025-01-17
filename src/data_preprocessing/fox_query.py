# Define SQL queries without trailing semicolons
def full_query_df():
    # Define SQL queries without trailing semicolons
    queries = {
        "speed_figure": """
    SELECT 
        c.course_cd,
        c.track_name,
        r.race_date,
        r.race_number,
        h.horse_id,
        r.distance_meters,
        re.official_fin, 
        ROUND(sa.running_time - r.rr_win_time) AS time_behind,
        sa.running_time - r.rr_par_time AS pace_delta_time, 
        re.speed_rating, 
        r.class_rating, 
        r2.previous_class,  
        r2.power,
        has.starts, 
        has.itm_percentage AS horse_itm_percentage, 
        tc.description AS track_conditions 
    FROM 
        v_races r
    JOIN 
        results_entries re ON r.course_cd = re.course_cd 
        AND r.race_date = re.race_date
        AND r.race_number = re.race_number
    JOIN 
        runners r2 ON re.course_cd = r2.course_cd 
        AND re.race_date = r2.race_date 
        AND re.race_number = r2.race_number 
        AND re.program_num = r2.saddle_cloth_number
    JOIN 
        sectionals_aggregated sa ON re.course_cd = sa.course_cd 
        AND re.race_date = sa.race_date 
        AND re.race_number = sa.race_number 
        AND re.program_num = sa.saddle_cloth_number
    JOIN 
        horse h ON re.axciskey = h.axciskey 
    JOIN 
        horse_accum_stats has ON h.axciskey = has.axciskey
        AND r.race_date = has.as_of_date 
        AND has.stat_type = 'ALL_RACES'
    JOIN 
        track_conditions tc ON r.trk_cond = tc.code
    JOIN 
        course c ON r.course_cd = c.course_cd 
    WHERE 
        re.breed = 'TB'
        AND r.rr_par_time <> 0
        AND r.rr_par_time IS NOT NULL
        AND r.course_cd IN ('CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
                            'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
                            'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP')
        """
    }
    return queries