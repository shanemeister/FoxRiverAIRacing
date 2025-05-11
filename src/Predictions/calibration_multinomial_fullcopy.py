from pyspark.sql.functions import when, col

#!/usr/bin/env python3
"""
calibrate_multinomial_fullcopy.py
---------------------------------
T-calibration that keeps *all* original prediction columns.
"""
import os, sys, logging, configparser
from datetime import datetime
import psycopg2, pandas as pd, numpy as np
from psycopg2 import pool
from scipy.optimize import minimize
from src.data_preprocessing.data_prep1.data_utils import initialize_environment
from pyspark.sql import functions as F            # only needed for writes
# -------------------------------------------------------------------------
# 0 ) helpers – logging & DB pool
# -------------------------------------------------------------------------
def setup_logging(fname):
    log_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs'
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, fname)
    with open(path, "w"):                        # truncate
        pass
    logging.basicConfig(
        filename=path,
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def read_cfg(p="~/myCode/horse-racing/FoxRiverAIRacing/config.ini"):
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser(p))
    return cfg

def db_pool(cfg):
    kw = dict(
        user=cfg["database"]["user"],
        host=cfg["database"]["host"],
        port=cfg["database"]["port"],
        database=cfg["database"]["dbname"],
        password=cfg["database"].get("password", "")
    )
    return pool.SimpleConnectionPool(1, 6, **kw)

# --- put near the top of the file, after EPS helper -----------------
def add_race_level_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich *df* (one row per horse) with:
        fav_morn_odds, avg_morn_odds, max_prob, second_prob,
        prob_gap (=max-second), std_prob,
        leader_gap, trailing_gap  (per horse), edge, kelly
    Assumes columns  [course_cd, race_date, race_number,
                      calibrated_prob, morn_odds]  already exist.
    """

    def enrich_one_race(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()

        # ---------- race-level aggregates ----------
        g["fav_morn_odds"] = g["morn_odds"].min()
        g["avg_morn_odds"] = g["morn_odds"].mean()

        probs_sorted = np.sort(g["calibrated_prob"].values)[::-1]
        max_p, second_p = probs_sorted[0], probs_sorted[1] if len(probs_sorted) > 1 else 0.0
        g["max_prob"]    = max_p
        g["second_prob"] = second_p
        g["prob_gap"]    = max_p - second_p
        g["std_prob"]    = g["calibrated_prob"].std(ddof=0)

        # ---------- per-horse gaps ----------
        # keep a copy of the current labels to restore later
        orig_idx = g.index

        # 1) sort by prob so neighbours are adjacent
        g_prob = g.sort_values("calibrated_prob", ascending=False)

        # 2) leader gap: distance to the favourite’s probability
        top_prob = g_prob.iloc[0]["calibrated_prob"]
        g_prob["leader_gap"] = top_prob - g_prob["calibrated_prob"]

        # 3) trailing gap: distance to the next horse in this ordering
        g_prob["trailing_gap"] = (
            g_prob["calibrated_prob"] - g_prob["calibrated_prob"].shift(-1)
        )

        # 4) the *true* last-probability horse gets sentinel –1.0
        worst_idx = g_prob["calibrated_prob"].idxmin()    # label, not position
        g_prob.loc[worst_idx, "trailing_gap"] = -1.0

        # 5) put rows back to their original file order
        g = g_prob.reindex(orig_idx)

        return g

    df = (
        df.groupby(["course_cd", "race_date", "race_number"], sort=False)
          .apply(enrich_one_race)
          .reset_index(drop=True)
    )

    # ---------- edge & Kelly (allow negatives) ----------
    p_ml = df["morn_odds"].clip(upper=0.999)        # stay in (0,1)
    b    = (1.0 / p_ml) - 1.0
    df["edge"]  = df["calibrated_prob"] - p_ml
    df["kelly"] = (df["calibrated_prob"] * b - (1.0 - df["calibrated_prob"])) / b
    # no clip(lower=0)  → negative Kelly preserved
    return df

# -------------------------------------------------------------------------
# 1 ) temperature scaling utilities
# -------------------------------------------------------------------------
EPS = 1e-12
def softmax(scores, T):
    s = np.clip(scores, -50, 50)
    s -= s.max()
    e = np.exp(s / T)
    return e / (e.sum() + EPS)

def nll(T_arr, grp_arrays):
    """ Negative log-likelihood of winners over all races. """
    T = T_arr[0]
    loss = 0.0
    for scores, winners in grp_arrays:
        p = softmax(scores, T)
        loss += -np.log(p[winners == 1] + EPS).sum()
    return loss

def fit_T(df_cal):
    grp_arrays = [
        (g["score"].values, g["winner"].values)
        for _, g in df_cal.groupby("race_key", sort=False)
    ]
    res = minimize(
        fun=nll,
        x0=np.array([1.0]),
        args=(grp_arrays,),
        bounds=[(0.001, 100.0)],
        method="L-BFGS-B"
    )
    T = float(res.x[0]) if res.success else 1.0
    logging.info(f"Fitted temperature T = {T:.4f}")
    return T

def set_nan_to_null_postgres(pool_, dst_table):
    update_sql = f"""
        UPDATE {dst_table}
        SET official_fin = NULL
        WHERE official_fin IS NOT NULL AND official_fin::float::text = 'NaN';
    """
    update_sql2 = f"""
        UPDATE {dst_table}
        SET top_3_rank = NULL
        WHERE top_3_rank IS NOT NULL AND top_3_rank::float::text = 'NaN';
    """
    with pool_.getconn() as conn, conn.cursor() as cur:
        cur.execute(update_sql)
        cur.execute(update_sql2)
        conn.commit()

# -------------------------------------------------------------------------
# 2 ) main
# -------------------------------------------------------------------------
def main():
    setup_logging("calibrate_fullcopy.log")
    spark, jdbc_url, jdbc_props, *_ = initialize_environment()
    cfg   = read_cfg()
    pool_ = db_pool(cfg)

    ## ------------------------------------------------------------------ ##
    ## A) names & simple meta
    ## ------------------------------------------------------------------ ##
    src_table = "predictions_20250510_104930_1"          # <-- change only here
    dst_table = f"{src_table}_calibrated"

    ## ------------------------------------------------------------------ ##
    ## B) pull *only* minimal columns for T-fit (faster)
    ## ------------------------------------------------------------------ ##
    fit_sql = f"""
        SELECT course_cd, race_date, race_number,
               top_3_score                AS score,
               CASE WHEN official_fin = 1 THEN 1 ELSE 0 END AS winner
        FROM   {src_table}
        WHERE  official_fin IS NOT NULL
          AND  official_fin > 0
          AND  top_3_score IS NOT NULL
    """
    with pool_.getconn() as conn, conn.cursor() as cur:
        cur.execute(fit_sql)
        df_cal = pd.DataFrame(cur.fetchall(), columns=[
            "course_cd","race_date","race_number","score","winner"
        ])

    if df_cal.empty:
        logging.error("Calibration query returned zero rows. Abort.")
        sys.exit(1)

    df_cal["race_key"] = (
        df_cal["course_cd"].astype(str) + "_" +
        df_cal["race_date"].astype(str) + "_" +
        df_cal["race_number"].astype(str)
    )

    T_best = fit_T(df_cal)

    ## ------------------------------------------------------------------ ##
    ## C) fetch *all* columns, compute calibrated prob + logit
    ## ------------------------------------------------------------------ ##
    full_sql = f"SELECT * FROM {src_table}"
    with pool_.getconn() as conn, conn.cursor() as cur:
        cur.execute(full_sql)
        cols = [d.name for d in cur.description]
        full_df = pd.DataFrame(cur.fetchall(), columns=cols)

    # ✱ 1.  convert weird / string NaNs in official_fin -> real NaN  → SQL NULL
    full_df["official_fin"] = full_df["official_fin"].where(~full_df["official_fin"].isna(), None)

    # ✱ 2.  same for top_3_rank (leave rows intact, just normalise)
    full_df["top_3_rank"] = full_df["top_3_rank"].where(~full_df["top_3_rank"].isna(), None)

    # build race_key again
    full_df["race_key"] = (
        full_df["course_cd"].astype(str) + "_" +
        full_df["race_date"].astype(str) + "_" +
        full_df["race_number"].astype(str)
    )

    # choose whichever raw-score column you want to turn into a probability
    # here we use `top_3_score`
    if "top_3_score" not in full_df.columns:
        raise KeyError("Column 'top_3_score' not found in predictions table.")

    def calibrate_group(s):
        p = softmax(s.values, T_best)
        return pd.Series({
            "calibrated_prob" : p,
            "calibrated_logit": np.log(p / (1 - p + EPS))
        })

    probs = (
        full_df.groupby("race_key")["top_3_score"]
               .apply(lambda s: calibrate_group(s)["calibrated_prob"])
               .explode()
               .astype(float)
               .values
    )
    logits = (
        full_df.groupby("race_key")["top_3_score"]
               .apply(lambda s: calibrate_group(s)["calibrated_logit"])
               .explode()
               .astype(float)
               .values
    )

    # ------------------------------------------------------------------ #
    # C)  calibrated_prob  &  calibrated_logit  (index-safe)
    # ------------------------------------------------------------------ #
    def softmax_vec(scores: np.ndarray, T: float) -> np.ndarray:
        s = np.clip(scores, -50, 50)
        s -= s.max()
        e = np.exp(s / T)
        return e / (e.sum() + EPS)

    full_df["calibrated_prob"] = (
        full_df.groupby("race_key")["top_3_score"]
            .transform(lambda s: softmax_vec(s.values, T_best))
    )

    full_df["calibrated_logit"] = np.log(
        full_df["calibrated_prob"] / (1.0 - full_df["calibrated_prob"] + EPS)
    )    
    ## ------------------------------------------------------------------ ##
    ## D) add the new race-level & per-horse features
    ## ------------------------------------------------------------------ ##
    full_df = add_race_level_features(full_df)
    ## ------------------------------------------------------------------ ##
    ## E) write back — overwrite if table exists
    ## ------------------------------------------------------------------ ##
    spark_df = spark.createDataFrame(full_df)
    # Replace NaN with None (NULL) in official_fin
    spark_df = spark_df.withColumn(
        "official_fin",
        when(col("official_fin").isNull() | (col("official_fin") != col("official_fin")), None).otherwise(col("official_fin"))
    )

    # Replace NaN with None (NULL) in official_fin
    spark_df = spark_df.withColumn(
        "top_3_rank",
        when(col("top_3_rank").isNull() | (col("top_3_rank") != col("top_3_rank")), None).otherwise(col("top_3_rank"))
    )

    (
        spark_df.write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", dst_table)
                .option("user", jdbc_props["user"])
                .option("driver", jdbc_props["driver"])
                .mode("overwrite")
                .save()
    )
    set_nan_to_null_postgres(pool_, dst_table)
    logging.info(f"✓ wrote {len(full_df):,} rows to {dst_table}")
    spark.stop()

if __name__ == "__main__":
    main()