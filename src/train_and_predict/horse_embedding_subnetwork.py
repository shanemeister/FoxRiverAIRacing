import logging
import datetime
import os
import optuna
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, Callback
from tensorflow.keras.regularizers import l2
from optuna.integration import TFKerasPruningCallback
from sklearn.model_selection import train_test_split
import scipy.stats as stats
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Suppose you have a callback for spearman ranking:
class RankingMetricCallback(Callback):
    def __init__(self, val_data):
        """
        val_data: (val_numeric, val_horse_stats, val_horse_id, val_race_numeric, val_cat_inputs, val_y)
                  or some structure that includes all validation features
        """
        super().__init__()
        self.val_data = val_data

    def on_epoch_end(self, epoch, logs=None):
        preds = self.model.predict(self.val_data["model_inputs"])
        corr, _ = stats.spearmanr(self.val_data["y_val"], preds.flatten())
        logs = logs or {}
        logs["val_spearman"] = corr
        print(f" - val_spearman: {corr:.4f}")


def build_horse_embedding_model(num_horses,
                                num_horse_stats,
                                race_num_features,
                                cat_vocabs,
                                horse_embedding_dim=8,
                                hidden_units=64,
                                n_hidden_layers=2,
                                activation="relu",
                                dropout_rate=0.0,
                                l2_reg=1e-4,
                                learning_rate=1e-3):
    """
    Builds a Keras model that:
      1) Learns a horse-level embedding from (horse_id + numeric horse-level stats),
      2) Combines that with race-level numeric features (like race_class_avg_speed, etc.),
      3) Also uses embeddings for any categorical race features (like course_cd, trk_cond),
      4) Produces a single numeric output (like relevance).
    """

    # ---------- HORSE SUB-NETWORK ----------
    # (A) horse_id input + embedding
    horse_id_input = keras.Input(shape=(), name="horse_id_input", dtype=tf.int32)
    horse_id_emb_layer = layers.Embedding(input_dim=num_horses + 1,
                                          output_dim=horse_embedding_dim,
                                          name="horse_id_embedding")
    horse_id_emb = layers.Flatten()(horse_id_emb_layer(horse_id_input))

    # (B) numeric horse-level stats
    horse_stats_input = keras.Input(shape=(num_horse_stats,), name="horse_stats_input")
    x_horse_stats = layers.Dense(hidden_units, activation=activation, kernel_regularizer=l2(l2_reg))(horse_stats_input)
    for _ in range(n_hidden_layers - 1):
        x_horse_stats = layers.Dense(hidden_units, activation=activation, kernel_regularizer=l2(l2_reg))(x_horse_stats)
        if dropout_rate > 0:
            x_horse_stats = layers.Dropout(dropout_rate)(x_horse_stats)

    # (C) combine them into a single "horse embedding"
    combined_horse = layers.Concatenate()([horse_id_emb, x_horse_stats])
    horse_embedding_out = layers.Dense(horse_embedding_dim, activation="linear", name="horse_embedding_out")(combined_horse)
    # This yields a vector of dimension "horse_embedding_dim" that encodes both ID and numeric stats.

    # ---------- RACE/CONTEXT SUB-NETWORK ----------
    # Suppose you have race-level numeric features (like race_class_avg_speed, etc.)
    race_numeric_input = keras.Input(shape=(race_num_features,), name="race_numeric_input")
    x_race = race_numeric_input
    # If desired, you can do a small MLP on these race numeric features:
    x_race = layers.Dense(hidden_units, activation=activation, kernel_regularizer=l2(l2_reg))(x_race)
    if dropout_rate > 0:
        x_race = layers.Dropout(dropout_rate)(x_race)

    # ---------- CATEGORICAL RACE FEATURES (course_cd, trk_cond, etc.) ----------
    cat_inputs = {}
    cat_embeddings = []
    for cat_name, vocab_size in cat_vocabs.items():
        cat_in = keras.Input(shape=(), name=f"{cat_name}_input", dtype=tf.string)
        cat_inputs[cat_name] = cat_in

        lookup = layers.StringLookup(output_mode="int", vocabulary=vocab_size, name=f"{cat_name}_lookup")
        cat_indices = lookup(cat_in)
        # Embedding dimension can be smaller for these race cat features:
        cat_embed_layer = layers.Embedding(input_dim=len(vocab_size) + 1, output_dim=4, name=f"{cat_name}_embed")
        cat_embedded = layers.Flatten()(cat_embed_layer(cat_indices))
        cat_embeddings.append(cat_embedded)

    # ---------- COMBINE EVERYTHING ----------
    # Combine horse_embedding_out, race numeric, and cat embeddings
    combined_all = layers.Concatenate(name="combined_all")([horse_embedding_out, x_race] + cat_embeddings)
    # final dense
    final_output = layers.Dense(1, activation="linear", name="output")(combined_all)

    model = keras.Model(
        inputs=[horse_id_input, horse_stats_input, race_numeric_input] + list(cat_inputs.values()),
        outputs=final_output
    )

    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
        loss="mse",
        metrics=["mae"]
    )

    return model


def example_training_pipeline():
    """
    Illustrates how you might use the "build_horse_embedding_model" in a pipeline
    that:

    1) Splits data into (horse_id, horse_stats, race_numeric, cat_features) for train/val,
    2) Trains with MSE loss,
    3) Extracts the final horse embedding for each horse_id.
    """

    # (A) Suppose you have loaded your data into a Pandas DF "historical_pdf"
    # with columns: horse_id, numeric stats about the horse, race-level numeric, cat columns, and y= relevance.

    # Just placeholders to show the concept:
    horse_id_array = np.random.randint(1, 1000, size=(5000,))  # 5000 samples
    # e.g. 5 horse-level numeric stats:
    horse_stats_array = np.random.rand(5000, 5)
    # race numeric features, e.g. 6 columns (like race_class_avg_speed, etc.)
    race_numeric_array = np.random.rand(5000, 6)
    # cat features:
    cat_cd_array = np.array(["CD1", "CD2", "CD3"], dtype=object)
    track_cond_array = np.array(["fast", "muddy", "sloppy"], dtype=object)

    course_cd_array = np.random.choice(cat_cd_array, size=(5000,))
    trk_cond_array = np.random.choice(track_cond_array, size=(5000,))

    y_relevance = np.random.rand(5000)  # some float in 0..1

    # (B) We'll do a train/val split
    all_indices = np.arange(5000)
    train_inds, val_inds = train_test_split(all_indices, test_size=0.2, random_state=42)

    # Extract subsets:
    X_train_horseid = horse_id_array[train_inds]
    X_val_horseid   = horse_id_array[val_inds]

    X_train_horse_stats = horse_stats_array[train_inds]
    X_val_horse_stats   = horse_stats_array[val_inds]

    X_train_race_num = race_numeric_array[train_inds]
    X_val_race_num   = race_numeric_array[val_inds]

    X_train_course_cd = course_cd_array[train_inds]
    X_val_course_cd   = course_cd_array[val_inds]

    X_train_trk_cond = trk_cond_array[train_inds]
    X_val_trk_cond   = trk_cond_array[val_inds]

    y_train = y_relevance[train_inds]
    y_val   = y_relevance[val_inds]

    # (C) Build the model
    # Suppose we have 1000 horses total
    num_horses = 1000
    num_horse_stats = 5
    race_num_features = 6

    # For cat vocabs, we want the actual unique strings used by StringLookup
    cat_vocabs = {
        "course_cd": np.unique(cat_cd_array).tolist(),  # e.g. ["CD1", "CD2", "CD3"]
        "trk_cond": np.unique(track_cond_array).tolist() # e.g. ["fast", "muddy", "sloppy"]
    }

    model = build_horse_embedding_model(
        num_horses=num_horses,
        num_horse_stats=num_horse_stats,
        race_num_features=race_num_features,
        cat_vocabs=cat_vocabs,      # pass the mapping
        horse_embedding_dim=8,      # dimension for the final horse embedding
        hidden_units=64,
        n_hidden_layers=2,
        activation="relu",
        dropout_rate=0.2,
        l2_reg=1e-4,
        learning_rate=1e-3
    )

    # (D) Prepare train/val inputs (matching model input names)
    train_inputs = {
        "horse_id_input": X_train_horseid,
        "horse_stats_input": X_train_horse_stats,
        "race_numeric_input": X_train_race_num,
        "course_cd_input": X_train_course_cd,
        "trk_cond_input": X_train_trk_cond
    }
    val_inputs = {
        "horse_id_input": X_val_horseid,
        "horse_stats_input": X_val_horse_stats,
        "race_numeric_input": X_val_race_num,
        "course_cd_input": X_val_course_cd,
        "trk_cond_input": X_val_trk_cond
    }

    # (E) Fit the model
    early_stop = EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True)
    model.fit(
        train_inputs,
        y_train,
        validation_data=(val_inputs, y_val),
        epochs=50,
        batch_size=256,
        callbacks=[early_stop],
        verbose=1
    )

    # Evaluate final:
    val_loss, val_mae = model.evaluate(val_inputs, y_val, verbose=0)
    print(f"Final val_loss={val_loss:.4f}, val_mae={val_mae:.4f}")

    # (F) Extract the final horse embedding
    # We want the weights from the "horse_embedding_out" layer or see how we store them.
    # Actually, in this example, "horse_embedding_out" is an intermediate layer's output
    # so we can build a sub-model or override train_step.

    # Alternatively, if we just want to do a forward pass: For each horse_id, we feed in
    # zero or mean data for the numeric inputs to get that "horse embedding out."

    # For demonstration, let's build a "horse submodel" that outputs horse_embedding_out
    submodel = keras.Model(
        inputs=[model.get_layer("horse_id_input").input,
                model.get_layer("horse_stats_input").input],
        outputs=model.get_layer("horse_embedding_out").output
    )

    # Suppose we want to get the horse embedding for each row in the entire dataset:
    full_inputs = {
        "horse_id_input": horse_id_array,
        "horse_stats_input": horse_stats_array
    }
    horse_embeddings = submodel.predict(full_inputs)  # shape (5000, horse_embedding_dim)

    # Then we'd typically reduce to a unique embedding per horse_id by grouping, or pick the last row for each horse, etc.

    print("Sample of horse_embeddings shape:", horse_embeddings.shape)
    # (5000, 8) if horse_embedding_dim=8

    # You could group by horse_id to get a single representative embedding (like mean).
    # But often you'd do this in training if your objective is to produce exactly one embedding per horse.

    return model


def main():
    model = example_training_pipeline()
    # Then proceed with final steps, e.g. union future data, etc.

if __name__ == "__main__":
    main()