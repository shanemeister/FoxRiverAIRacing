from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, mean as spark_mean
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline

def prep_data(df: DataFrame) -> DataFrame:
    # Drop unwanted high-cardinality columns that don't add predictive value:
    # Example: time_stamp, time_stamp_ms, sec_time_stamp, location, race_date (if not used)
    # Adjust based on domain knowledge.
    drop_cols = ["time_stamp", "time_stamp_ms", "race_date", "saddle_cloth_number", ]
    df = df.drop(*drop_cols)
    
    # Categorical columns with low cardinality
    # Decide which to one-hot encode and which to just index.
    categorical_cols = ["course_cd", post_pos", 
                        "turf_mud_mark", "surface", "trk_cond", "weather", "stk_clm_md"]
    
    # Numeric columns
    # Example numeric cols:
    # speed, progress, stride_frequency, post_time (convert to numeric?), speed_rating,
    # avgspd, class_rating, wps_pool, todays_cls, net_sentiment, weight, morn_odds
    # Some might be temporal or numeric. If 'post_time' is a timestamp, consider extracting hours/minutes as numeric.
    
    # For simplicity, let's assume these numeric columns need scaling/imputation:
    numeric_cols = ["speed", "progress", "stride_frequency", "speed_rating", "avgspd", 
                    "class_rating", "wps_pool", "todays_cls", "net_sentiment", "weight", "morn_odds"]
    
    # Impute missing numeric values with mean
    # A simple approach: for each numeric column, fill nulls with global mean.
    for num_col in numeric_cols:
        mean_val = df.select(spark_mean(col(num_col)).alias('mean_val')).collect()[0]['mean_val']
        df = df.withColumn(num_col, when(col(num_col).isNull(), mean_val).otherwise(col(num_col)))
    
    # For categorical columns, fill nulls with a placeholder "unknown"
    for cat_col in categorical_cols:
        df = df.withColumn(cat_col, when(col(cat_col).isNull(), "unknown").otherwise(col(cat_col)))
    
    # Horse embedding: assuming horse_id_index already exists as integer
    # If not, create it:
    # If we have horse_id as string, we can index it:
    # horse_id_indexer = StringIndexer(inputCol="horse_id", outputCol="horse_id_index", handleInvalid="keep")
    # If it's already indexed, skip this step.
    
    # StringIndexers for categorical columns
    #indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols if c != "horse_id"]
    
    # One-hot encode selected features (maybe surface, turf_mud_mark)
    ohe_cols = ["turf_mud_mark", "surface", "trk_cond", "weather", "stk_clm_md"]
    encoders = [OneHotEncoder(inputCols=[f"{c}_index"], outputCols=[f"{c}_onehot"]) for c in ohe_cols]
    
    # Assemble numeric features for scaling
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="numeric_features")
    scaler = MinMaxScaler(inputCol="numeric_features", outputCol="scaled_numeric_features")
    
    # Final feature vector assembly
    # Combine horse_id_index, scaled numeric features, and one-hot encoded categorical
    final_input = ["horse_id_index", "scaled_numeric_features"] + [f"{c}_onehot" for c in ohe_cols]
    
    # If we want to include other indexed categorical columns that we didn't one-hot encode
    # (like course_cd_index) directly, add them to final_input as well.
    low_card_index_only = [c for c in categorical_cols if c not in ohe_cols and c != "horse_id"]
    final_input += [f"{c}_index" for c in low_card_index_only]
    
    final_assembler = VectorAssembler(inputCols=final_input, outputCol="features")
    
    # Build pipeline
    pipeline_stages = indexers + encoders + [assembler, scaler, final_assembler]
    pipeline = Pipeline(stages=pipeline_stages)
    
    model = pipeline.fit(df)
    df_prepared = model.transform(df)
    
    # df_prepared now has a 'features' column ready for modeling.
    # Drop intermediate columns if desired
    drop_intermediate = ["numeric_features", "scaled_numeric_features"] + \
                        [f"{c}_index" for c in categorical_cols if c != "horse_id"] + \
                        [f"{c}_onehot" for c in ohe_cols]
    df_prepared = df_prepared.drop(*drop_intermediate)
    
    return df_prepared