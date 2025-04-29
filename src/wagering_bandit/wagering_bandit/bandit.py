import logging
import time
from mabwiser.mab import MAB, LearningPolicy, NeighborhoodPolicy
from .config import settings

def get_learning_policy(policy_name: str, epsilon: float = 0.1):
    """
    Convert the string 'EpsilonGreedy' or 'Softmax', etc. into the right
    MABWiser LearningPolicy object, optionally passing relevant parameters.
    """
    if policy_name == "EpsilonGreedy":
        return LearningPolicy.EpsilonGreedy(epsilon=epsilon)
    elif policy_name == "Softmax":
        return LearningPolicy.Softmax()  # add temperature if desired
    elif policy_name == "UCB1":
        return LearningPolicy.UCB1()
    else:
        logging.warning(f"Unknown learning policy: {policy_name}. Defaulting to EpsilonGreedy.")
        return LearningPolicy.EpsilonGreedy(epsilon=0.1)

def get_neighborhood_policy(neighbor_name: str, k=5, n_clusters=10):
    """
    Convert the string 'KNearest', 'Clusters', etc. into the right 
    MABWiser NeighborhoodPolicy object, passing relevant parameters.
    """
    if neighbor_name == "KNearest":
        return NeighborhoodPolicy.KNearest(k=k)
    elif neighbor_name == "Clusters":
        return NeighborhoodPolicy.Clusters(n_clusters=n_clusters)
    elif neighbor_name == "Radius":
        return NeighborhoodPolicy.Radius(radius=1.0)
    elif neighbor_name == "TreeBandit":
        return NeighborhoodPolicy.TreeBandit()
    else:
        logging.warning(f"Unknown neighbor policy: {neighbor_name}. No neighbor policy used.")
        return None  # means no neighborhood policy

class ContextualBandit:
    def __init__(self, arms=None):
        arms = arms or settings.BANDIT_ARMS

        learning_policy_obj = get_learning_policy(
            settings.BANDIT_POLICY, 
            epsilon=settings.BANDIT_EPSILON
        )

        neighborhood_policy_obj = get_neighborhood_policy(
            neighbor_name=settings.BANDIT_NEIGHBOR,
            k=settings.BANDIT_NEIGHBOR_K,
            n_clusters=settings.BANDIT_NUM_CLUSTERS
        )

        self.mab = MAB(
            arms=arms,
            learning_policy=learning_policy_obj,
            neighborhood_policy=neighborhood_policy_obj
        )

    def train(self, contexts, decisions, rewards):
        """
        contexts : array-like, shape (n_samples, n_features)
        decisions: array-like, shape (n_samples,)   ← which arm was pulled
        rewards  : array-like, shape (n_samples,)   ← observed payoff
        """
        logging.info(
            f"Starting bandit.fit on {len(decisions)} samples, "
            f"{contexts.shape[1]} features…"
        )
        start = time.time()

        # Train the MAB
        self.mab.fit(decisions=decisions, rewards=rewards, contexts=contexts)

        elapsed = time.time() - start
        logging.info(f"…done training bandit in {elapsed:.1f}s")


    def recommend(self, contexts):
        """
        Returns:
          best_arm_for_each_row: np.array or list of best arms
          exp_value_for_each_row: list of floats (the chosen arm's expected reward)
        """
        # 1) best arm for each row
        best_arms = self.mab.predict(contexts)

        # 2) dictionary of expected rewards for *all* arms in each row
        #    e.g. [ {armA: rA, armB: rB, ...}, {armA: rA2, ...}, ... ]
        all_exps = self.mab.predict_expectations(contexts)

        # 3) Retrieve just the best arm's expected reward (numeric float)
        best_arm_exps = []
        for i, chosen_arm in enumerate(best_arms):
            # all_exps[i] is a dict: { "no_bet": 0.0, "exacta": 1.23, ... }
            best_arm_exps.append(all_exps[i][chosen_arm])

        return best_arms, best_arm_exps

    def save(self, path):
        import pickle
        with open(path, "wb") as f:
            pickle.dump(self.mab, f)

    def load(self, path):
        import pickle
        with open(path, "rb") as f:
            self.mab = pickle.load(f)