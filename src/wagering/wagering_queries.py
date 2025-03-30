# Define SQL queries without trailing semicolons
def wager_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "races": """
            SELECT UPPER(TRIM(r.course_cd)) AS course_cd, r.race_date ,r.race_number, UPPER(TRIM(r.saddle_cloth_number)) AS saddle_cloth_number, h.horse_id, re.official_fin , r.morn_odds , r.avg_purse_val_calc , re.dollar_odds , r2.race_type,
                    r2.trk_cond , r2.surface , r2.distance_meters ,cer.prediction, cer."rank"
            FROM races r2 
            JOIN runners r on r2.course_cd = r.course_cd 
                AND r2.race_date = r.race_date 
                AND r2.race_number = r.race_number 
            JOIN horse h on r.axciskey = h.axciskey 
            LEFT JOIN results_entries re on r.course_cd = re.course_cd 
                AND r.race_date = re.race_date
                AND r.race_number = re.race_number
                AND r.saddle_cloth_number = re.program_num
            left join catboost_enriched_results cer on r.course_cd = cer.course_cd
            	and r.race_date = cer.race_date
            	and r.race_number = cer.race_number
            	and r.saddle_cloth_number = cer.saddle_cloth_number 
            WHERE cer.model_key = 'YetiRank:top=1_NDCG:top=3_20250329_153033'
            and r.course_cd in('CNL','SAR','PIM','TSA','BEL','MVR','TWO','KEE','TAM',
                            'TTP','TKD','ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP',
                            'TGG','CBY','LRL','TED','IND','TCD','TOP')
            --and re.official_fin = 1
            --and cer.rank >= 2
            and cer."rank" is not null
            AND r.race_date >= '2024-06-30'
        """,
        "wagers": """
            SELECT ew.course_cd , ew.race_date , ew.race_number , ew.wager_id, ew.num_tickets, ew.wager_type, ew.winners, ew.payoff, ew.pool_total, ew.post_time 
            FROM exotic_wagers ew 
        """
    }
    return queries