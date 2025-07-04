# Define SQL queries without trailing semicolons
def horse_embedding_queries():
    queries = {
        "horse_embedding": 
        """
        SELECT course_cd,
            race_date,
            race_number,
            group_id,
            class_rating,
            horse_id,
            race_id,
            axciskey,
            post_time,
            saddle_cloth_number,
            horse_name,
            official_fin,
            dist_bk_gate4_target,
            running_time_target,
            par_time,
            post_position,
            avg_purse_val,
            running_time,
            total_distance_ran,
            avgtime_gate1,
            avgtime_gate2,
            avgtime_gate3,
            avgtime_gate4,
            dist_bk_gate1,
            dist_bk_gate2,
            dist_bk_gate3,
            dist_bk_gate4,
            speed_q1,
            speed_q2,
            speed_q3,
            speed_q4,
            speed_var,
            avg_speed_fullrace,
            accel_q1,
            accel_q2,
            accel_q3,
            accel_q4,
            avg_acceleration,
            max_acceleration,
            jerk_q1,
            jerk_q2,
            jerk_q3,
            jerk_q4,
            avg_jerk,
            max_jerk,
            dist_q1,
            dist_q2,
            dist_q3,
            dist_q4,
            total_dist_covered,
            strfreq_q1,
            strfreq_q2,
            strfreq_q3,
            strfreq_q4,
            avg_stride_length,
            net_progress_gain,
            prev_speed_rating,
            previous_class,
            weight,
            date_of_birth,
            sex,
            equip,
            claimprice,
            previous_distance,
            previous_surface,
            prev_official_fin,
            power,
            med,
            avgspd,
            starts,
            avg_spd_sd,
            ave_cl_sd,
            hi_spd_sd,
            pstyerl,
            sec_score,
            sec_dim1,
            sec_dim2,
            sec_dim3,
            sec_dim4,
            sec_dim5,
            sec_dim6,
            sec_dim7,
            sec_dim8,
            sec_dim9,
            sec_dim10,
            sec_dim11,
            sec_dim12,
            sec_dim13,
            sec_dim14,
            sec_dim15,
            sec_dim16,
            purse,
            surface,
            distance_meters,
            trk_cond,
            morn_odds,
            race_type,
            stk_clm_md,
            turf_mud_mark,
            jock_win_percent,
            jock_itm_percent,
            trainer_win_percent,
            trainer_itm_percent,
            jt_win_percent,
            jt_itm_percent,
            jock_win_track,
            jock_itm_track,
            trainer_win_track,
            trainer_itm_track,
            jt_win_track,
            jt_itm_track,
            sire_itm_percentage,
            sire_roi,
            dam_itm_percentage,
            dam_roi,
            all_starts,
            all_win,
            all_place,
            all_show,
            all_fourth,
            all_earnings,
            horse_itm_percentage,
            cond_starts,
            cond_win,
            cond_place,
            cond_show,
            cond_fourth,
            cond_earnings,
            net_sentiment,  
            total_races_5,
            avg_fin_5,
            avg_speed_5,
            best_speed,
            avg_beaten_len_5,
            first_race_date_5,
            most_recent_race_5,
            avg_dist_bk_gate1_5,
            avg_dist_bk_gate2_5,
            avg_dist_bk_gate3_5,
            avg_dist_bk_gate4_5,
            avg_speed_fullrace_5,
            avg_stride_length_5,
            avg_strfreq_q1_5,
            avg_strfreq_q2_5,
            avg_strfreq_q3_5,
            avg_strfreq_q4_5,
            prev_speed,
            speed_improvement, 
            prev_race_date,
            days_off,
            layoff_cat,
            avg_workout_rank_3,
            count_workouts_3,
            track_name,
            has_gps,
            age_at_race_day,
            relevance,
            top4_label,
            class_offset,
            class_multiplier,
            official_distance,
            base_speed,
            dist_penalty,
            horse_mean_rps,
            horse_std_rps,
            global_speed_score_iq,
            race_count_agg,
            race_avg_speed_agg,
            race_std_speed_agg,
            race_avg_relevance_agg,
            race_std_relevance_agg,
            race_class_count_agg,
            race_class_avg_speed_agg,
            race_class_min_speed_agg,
            race_class_max_speed_agg,
            embed_0,
            embed_1,
            embed_2,
            embed_3,
            embed_4,
            embed_5,
            embed_6,
            embed_7,
            embed_8
        FROM horse_embedding_final        
        WHERE course_cd in('CNL','SAR','PIM','TSA','BEL','MVR','TWO','KEE','TAM',
                            'TTP','TKD','ELP','PEN','HOU','DMR','TLS','AQU','MTH','TGP',
                            'TGG','CBY','LRL','TED','IND','TCD','TOP')
        """
    }
    return queries
