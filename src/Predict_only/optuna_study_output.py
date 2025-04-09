import optuna

storage = "sqlite:///optuna_study.db"
study_name = "catboost_optuna_study"

# Load the existing study
study = optuna.load_study(study_name=study_name, storage=storage)

# Print out each trial's results
for t in study.trials:
    print(f"Trial #{t.number}, Value: {t.value}, Params: {t.params}")

# Or get just the best trial
best_trial = study.best_trial
print("Best trial value:", best_trial.value)
print("Best trial params:", best_trial.params)

# Convert all trials to a Pandas DataFrame
df = study.trials_dataframe()
print(df.head())
