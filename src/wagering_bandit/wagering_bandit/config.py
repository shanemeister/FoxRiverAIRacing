import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_URL = os.getenv("DB_URL")  # e.g. "postgresql://..."
    BANDIT_ARMS        = ["no_bet","exacta","daily_double","pick3","pick4"]
    BANDIT_POLICY      = "EpsilonGreedy"   # e.g. "EpsilonGreedy", "Softmax", "UCB1", etc.
    BANDIT_EPSILON     = 0.1
    BANDIT_NEIGHBOR    = "KNearest"        # e.g. "KNearest", "Clusters", "Radius", ...
    BANDIT_NEIGHBOR_K  = 5                 # used if 'KNearest'
    BANDIT_NUM_CLUSTERS= 10                # used if 'Clusters'
    # Available NeighborhoodPolicy options: ['Clusters', 'KNearest', 'LSHNearest', 'Radius', 'TreeBandit']
settings = Settings()