from catboost import CatBoostRegressor
import pandas as pd
import logging
from sklearn.preprocessing import LabelEncoder
from catboost import CatBoostRegressor, Pool
from src.data_preprocessing.data_prep1.data_utils import save_predictions

def make_cat_predictions(pred_data: pd.DataFrame, final_model: CatBoostRegressor) -> pd.DataFrame:
    """
    Make predictions using the CatBoost model.

    Args:
    data: pd.DataFrame - The input data to make predictions on.
    model: CatBoostRegressor - The CatBoost model to use for predictions.

    Returns:
    pd.DataFrame - The predictions made by the model.
    """
   # Set race_id
   # Create race_id for grouping
    pred_data["race_id"] = (
        pred_data["course_cd"].astype(str) + "_" +
        pred_data["race_date"].astype(str) + "_" +
        pred_data["race_number"].astype(str)
    )
    
    # Group and sort data by race_id and group_id
    # Generate unique numeric group_id from race_id
    pred_data["group_id"] = pred_data["race_id"].astype("category").cat.codes

    # Sort by race_id for consistency
    pred_data = pred_data.sort_values("group_id", ascending=True)
    pred_data.reset_index(drop=True, inplace=True)
    
    # Drop Non-numeric Features
    unused_columns = [
    # columns you do NOT use in features or group_id
    "race_date", "horse_name", "date_of_birth", "saddle_cloth_number"
    ]
    cols_to_drop = [col for col in unused_columns if col in pred_data.columns]

    pred_data.drop(columns=cols_to_drop, inplace=True)
    logging.info(f"After dropping unused cols, shape: {pred_data.shape}")
    
    logging.info("Convert DataTime columns to Numerical Values")
    # Convert datetime columns to numerical
    pred_data["first_race_date_5"] = pd.to_datetime(pred_data["first_race_date_5"])
    pred_data["most_recent_race_5"] = pd.to_datetime(pred_data["most_recent_race_5"])
    pred_data["prev_race_date"] = pd.to_datetime(pred_data["prev_race_date"])

    # Calculate numeric date features
    pred_data["first_race_date_5_numeric"] = (pred_data["first_race_date_5"] - pd.Timestamp("1970-01-01")).dt.days
    pred_data["most_recent_race_5_numeric"] = (pred_data["most_recent_race_5"] - pd.Timestamp("1970-01-01")).dt.days
    pred_data["prev_race_date_numeric"] = (pred_data["prev_race_date"] - pd.Timestamp("1970-01-01")).dt.days

    # Drop original datetime columns
    pred_data.drop(columns=["first_race_date_5", "most_recent_race_5", "prev_race_date"], inplace=True)
    
    # Assigned Numerical Features
    # Define features
    features = [
        'horse_id', 'course_cd', 'sex', 'equip', 'surface', 'med',
        'race_type', 'stk_clm_md', 'turf_mud_mark', 'layoff_cat',
        'race_number', 'purse', 'weight', 'claimprice', 'power', 'morn_odds',
        'avgspd', 'class_rating', 'net_sentiment', 'avg_spd_sd', 'ave_cl_sd',
        'hi_spd_sd', 'pstyerl', 'all_starts', 'all_win', 'all_place',
        'all_show', 'all_fourth', 'all_earnings', 'cond_starts', 'cond_win',
        'cond_place', 'cond_show', 'cond_fourth', 'cond_earnings',
        'avg_speed_5', 'best_speed', 'avg_beaten_len_5', 'avg_dist_bk_gate1_5',
        'avg_dist_bk_gate2_5', 'avg_dist_bk_gate3_5', 'avg_dist_bk_gate4_5',
        'avg_speed_fullrace_5', 'avg_stride_length_5', 'avg_strfreq_q1_5',
        'avg_strfreq_q2_5', 'avg_strfreq_q3_5', 'avg_strfreq_q4_5',
        'prev_speed', 'speed_improvement', 'days_off', 'avg_workout_rank_3',
        'jock_win_percent', 'jock_itm_percent', 'trainer_win_percent',
        'trainer_itm_percent', 'jt_win_percent', 'jt_itm_percent',
        'jock_win_track', 'jock_itm_track', 'trainer_win_track',
        'trainer_itm_track', 'jt_win_track', 'jt_itm_track', 'age_at_race_day',
        'distance_meters', 'count_workouts_3'
    ]
    
    logging.info ("Set the Category Columns with Label Encoder")
    
    # Keep original horse_id for identification
    pred_data["horse_id_original"] = pred_data["horse_id"]

    # Encode categorical columns
    cat_cols = [
        "horse_id", "course_cd", "sex", "equip", "surface", "med",  
        "race_type", "stk_clm_md", "turf_mud_mark", "layoff_cat"
    ]
    for c in cat_cols:
        lbl = LabelEncoder()
        pred_data[c] = lbl.fit_transform(pred_data[c].astype(str))

    # Create the Prediction Pool
    logging.info("Create the Prediction Pool")
    # Create the prediction pool
    prediction_pool = Pool(
        data=pred_data[features],
        group_id=pred_data["group_id"],  # Ensure group_id is present for ranking
        cat_features=cat_cols  # Include categorical feature names
    )
    
    logging.info(f"Prediction Pool has rows: {prediction_pool.num_row()}")
    
    # Loading the saved model
        
    # Generate predictions
    predictions = final_model.predict(prediction_pool)

    logging.info("Predictions generated: %s", predictions)
    
    # Attach predictions to the original dataset
    pred_data["predicted_score"] = predictions

    # Rank horses within each race based on their predicted scores
    pred_data["predicted_rank"] = pred_data.groupby("race_id")["predicted_score"].rank(
        method="first", ascending=False
    )
    
    subset_df = pred_data[["race_id", "horse_id_original", "predicted_score"]]
    subset_df = subset_df.sort_values(by=["race_id", "predicted_score"], ascending=[True, False])

    # Rename predicted_score -> cat_score for clarity
    subset_df.rename(columns={"predicted_score": "cat_score"}, inplace=True)

    # Example usage for CAT model
    save_predictions(subset_df, 'cat')
    
