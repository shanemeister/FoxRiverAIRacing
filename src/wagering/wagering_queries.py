# Define SQL queries without trailing semicolons
def wager_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "races": """
            SELECT UPPER(TRIM(r.course_cd)) AS course_cd, r.race_date ,r.race_number, UPPER(TRIM(r.saddle_cloth_number)) AS saddle_cloth_number, h.horse_id, re.official_fin , CAST(r.morn_odds AS float8) AS morn_odds , r.avg_purse_val_calc , re.dollar_odds , r2.race_type,
                    r2.trk_cond as track_condition , r2.surface , r2.distance_meters ,p.calibrated_prob as score, p."rank"
            FROM races r2 
            JOIN runners r on r2.course_cd = r.course_cd 
                AND r2.race_date = r.race_date 
                AND r2.race_number = r.race_number 
            JOIN horse h on r.axciskey = h.axciskey 
            LEFT JOIN results_entries re on r.course_cd = re.course_cd 
                AND r.race_date = re.race_date
                AND r.race_number = re.race_number
                AND r.saddle_cloth_number = re.program_num
            left join predictions_20250426_151421_1_calibrated p on r.course_cd = p.course_cd
            	and r.race_date = p.race_date
            	and r.race_number = p.race_number
            	and r.saddle_cloth_number = p.saddle_cloth_number 
            WHERE p."rank" is not null
            AND r.race_date >= '2024-07-01' -- CURRENT_DATE - INTERVAL '6 MONTHS' -- >= '2024-06-30'
            AND r.course_cd in('CNL','SAR','PIM','TSA','BEL','MVR','TWO','KEE','TAM',
                            'TTP','TKD','ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP',
                            'TGG','CBY','LRL','TED','IND','TCD','TOP')
        """,
        "wagers": """
            SELECT ew.course_cd , ew.race_date , ew.race_number , ew.wager_id, ew.num_tickets, ew.wager_type, ew.winners, ew.payoff, ew.pool_total, ew.post_time 
            FROM exotic_wagers ew 
            WHERE ew.wager_type is not null
            AND ew.course_cd in('CNL','SAR','PIM','TSA','BEL','MVR','TWO','KEE','TAM',
                            'TTP','TKD','ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP',
                            'TGG','CBY','LRL','TED','IND','TCD','TOP')
        """
    }
    return queries