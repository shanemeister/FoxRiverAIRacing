def recommend_exacta(preds_df, top_n=2):
    """
    Given one race’s DataFrame of horses & scores,
    return the saddle_cloth_numbers of your top_n to play Exacta.
    """
    top = preds_df.sort_values("score", ascending=False).head(top_n)
    return list(top["saddle_cloth_number"].astype(str))

# stub out daily_double, pick3, etc. same pattern