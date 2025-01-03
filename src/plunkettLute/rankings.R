library(arrow)
library(dplyr)
library(PlackettLuce)
library(psychotools)

# ----------------------------------------------------------------------------
# 1) Load Data with Arrow and Collect into memory
# ----------------------------------------------------------------------------
parquet_dir <- "/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/parquet/results_only_clean.parquet"
df_arrow <- arrow::open_dataset(parquet_dir)
df_r <- df_arrow %>% collect()  # Pull into an in-memory tibble/data.frame

filtered_ds_2024 <- df_r %>%
  filter(
    race_date >= "2024-01-01",
    race_date < "2024-06-01"
  )
# Debug: Check columns
cat("Columns in filtered_ds_2024:\n")
print(colnames(filtered_ds_2024))

# ----------------------------------------------------------------------------
# 2) Basic checks
# ----------------------------------------------------------------------------
needed_cols <- c("course_cd", "race_date", "race_number", "official_fin", "horse_id")
missing_cols <- setdiff(needed_cols, colnames(filtered_ds_2024))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse=", "))
}

# Convert horse_id to numeric, if needed
if (!is.numeric(filtered_ds_2024$horse_id)) {
  filtered_ds_2024$horse_id <- as.numeric(filtered_ds_2024$horse_id)
}

# official_fin should be numeric
if (!is.numeric(filtered_ds_2024$official_fin)) {
  filtered_ds_2024$official_fin <- as.numeric(filtered_ds_2024$official_fin)
}

# ----------------------------------------------------------------------------
# 3) Create a "race_id" + rank horses within each race
# ----------------------------------------------------------------------------
ranked_data <- filtered_ds_2024 %>%
  mutate(race_id = paste(course_cd, race_date, race_number, sep="_")) %>%
  group_by(race_id) %>%
  arrange(official_fin, .by_group=TRUE) %>%
  mutate(rank = row_number()) %>%
  ungroup()

# Optional: Exclude any single-horse races if not meaningful
race_counts <- ranked_data %>%
  group_by(race_id) %>%
  summarise(n_horses = n()) %>%
  ungroup()

valid_races <- race_counts %>%
  filter(n_horses > 1) %>%  # skip 1-horse races
  pull(race_id)

ranked_data <- ranked_data %>% filter(race_id %in% valid_races)

cat("\nPreview of 'ranked_data':\n")
print(head(ranked_data, 10))

# ----------------------------------------------------------------------------
# 4) Build a row per race: horse IDs in finishing order
# ----------------------------------------------------------------------------
pl_wide <- ranked_data %>%
  group_by(race_id) %>%
  summarize(
    # horse_list in order from 1st place to last place
    horse_list = list(horse_id),
    n_horses   = n()
  ) %>%
  ungroup()

# maximum race size
max_len <- max(pl_wide$n_horses)

cat("\nLongest race has", max_len, "horses.\n")

# Helper to pad horse-list with NA
pad_to_length <- function(x, final_len) {
  out <- c(x, rep(NA_real_, final_len - length(x)))
  # Ideally, all x are > 0 (no zero or negative).
  out
}

rank_matrix <- do.call(
  rbind,
  lapply(pl_wide$horse_list, pad_to_length, final_len = max_len)
)

ranked_obj <- as.rankings(
  x     = rank_matrix,
  input = "ordering"
)

cat("\nHead of rank_matrix:\n")
print(head(rank_matrix, 6))

# Sometimes entire row can be NA if there's an edge case. Let's remove them.
# row i is for race i
all_na_rows <- apply(rank_matrix, 1, function(row) all(is.na(row)))
if (any(all_na_rows)) {
  cat("Dropping", sum(all_na_rows), "rows that are entirely NA.\n")
  rank_matrix <- rank_matrix[!all_na_rows, , drop=FALSE]
  pl_wide <- pl_wide[!all_na_rows, ]
}

# ----------------------------------------------------------------------------
# 5) Fit the Plackett-Luce model using the ranked_obj
# ----------------------------------------------------------------------------
pl_model <- PlackettLuce(ranked_obj)
cat("\nPlackett-Luce Model Summary:\n")
print(summary(pl_model))

# ----------------------------------------------------------------------------
# 6) Predict probabilities
# ----------------------------------------------------------------------------
# For each race (group), get the item-level probabilities of each item “winning.”
predictions <- predict(pl_model, type="probabilities")

cat("\nPredictions (head):\n")
print(head(predictions))

# ----------------------------------------------------------------------------
# 7) Save
# ----------------------------------------------------------------------------
write.csv(predictions, "pl_model_predictions.csv", row.names=FALSE)
cat("\nSaved pl_model_predictions.csv\n")