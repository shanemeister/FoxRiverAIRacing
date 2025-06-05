# Define SQL queries without trailing semicolons
def wager_queries():
    # Define SQL queries without trailing semicolons
    queries = {
        "races": """
            SELECT UPPER(TRIM(r.course_cd)) AS course_cd, r.race_date ,r.race_number, UPPER(TRIM(r.saddle_cloth_number)) AS saddle_cloth_number, p.race_key as race_key, 
            h.horse_id, re.official_fin , CAST(r.morn_odds AS float8) AS morn_odds , r.avg_purse_val_calc , re.dollar_odds , r2.race_type,
                    r2.trk_cond as track_condition , r2.surface , r2.distance_meters ,p.top_3_score AS raw_score,top_3_rank as "rank",
                    p.calibrated_prob as score, p.sec_score as sec_score, p.calibrated_logit as skill, p.fav_morn_odds as fav_morn_odds, p.avg_morn_odds as avg_morn_odds,
                    p.max_prob as max_prob, p.second_prob as second_prob, p.prob_gap as prob_gap, p.std_prob as std_prob, p.leader_gap as leader_gap,
                    p.trailing_gap as trailing_gap, p.edge as edge, p.kelly as kelly
            FROM races r2 
            JOIN runners r on r2.course_cd = r.course_cd 
                AND r2.race_date = r.race_date 
                AND r2.race_number = r.race_number 
            JOIN horse h on r.axciskey = h.axciskey 
            LEFT JOIN results_entries re on r.course_cd = re.course_cd 
                AND r.race_date = re.race_date
                AND r.race_number = re.race_number
                AND r.saddle_cloth_number = re.program_num
            left join predictions_20250510_104930_1_calibrated p on r.course_cd = p.course_cd
            	and r.race_date = p.race_date
            	and r.race_number = p.race_number
            	and r.saddle_cloth_number = p.saddle_cloth_number 
            WHERE p.top_3_rank is not null
            AND r.morn_odds is not null
            -- AND r.morn_odds > 0
            AND r.race_date >= CURRENT_DATE - INTERVAL '6 MONTH' -- >= '2024-06-30'
            AND r.course_cd in('CTM','ASD','TWO','TGG','TSA','DEL','TGP','TAM','IND','TCD',
                                'ELP','TKD','KEE','TTP','LRL','PIM','CLS','MTH','AQU','BEL',
                                'SAR','TDN','PRX','TOP','DMR')                               
        """,
        "wagers": """
            SELECT ew.course_cd , ew.race_date , ew.race_number , ew.wager_id, ew.num_tickets, ew.wager_type, ew.winners, ew.payoff, ew.pool_total, ew.post_time 
            FROM exotic_wagers ew 
            WHERE ew.wager_type is not null
            AND ew.course_cd in('CTM','ASD','TWO','TGG','TSA','DEL','TGP','TAM','IND','TCD',
                                'ELP','TKD','KEE','TTP','LRL','PIM','CLS','MTH','AQU','BEL',
                                'SAR','TDN','PRX','TOP','DMR')
        """
    }
    return queries
