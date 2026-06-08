import random
from datetime import datetime, timedelta, timezone

import numpy as np

from simulator.config import Config


class TransactionGenerator:
    """Generates synthetic financial transactions with optional fraud labels.

    Follows the Fraud Detection Handbook simulation approach:
    - Each customer has a set of terminals they use
    - Each terminal has a base risk profile
    - Fraud is injected based on scenario probabilities
    """

    def __init__(self, config: Config, seed: int = 42):
        self.config = config
        self.rng = np.random.default_rng(seed)
        self.py_rng = random.Random(seed)

        self._customer_profiles = self._generate_customer_profiles()
        self._terminal_profiles = self._generate_terminal_profiles()
        self._customer_terminals = self._assign_customer_terminals()

    def _generate_customer_profiles(self) -> dict:
        """Generate per-customer spending profiles."""
        profiles = {}
        for cid in range(self.config.num_customers):
            profiles[cid] = {
                "mean_amount": float(self.rng.lognormal(mean=3.5, sigma=1.0)),
                "std_amount": float(self.rng.uniform(10, 200)),
                "tx_frequency_hours": float(self.rng.uniform(1, 72)),
            }
        return profiles

    def _generate_terminal_profiles(self) -> dict:
        """Generate per-terminal risk profiles."""
        profiles = {}
        for tid in range(self.config.num_terminals):
            profiles[tid] = {
                "base_fraud_rate": float(self.rng.beta(1, 1000)),
                "mean_amount": float(self.rng.lognormal(mean=3.5, sigma=1.2)),
            }
        return profiles

    def _assign_customer_terminals(self) -> dict:
        """Assign each customer 1-10 terminals they regularly use."""
        terminals = {}
        for cid in range(self.config.num_customers):
            n = self.py_rng.randint(1, 10)
            terminals[cid] = self.py_rng.choices(
                range(self.config.num_terminals), k=n
            )
        return terminals

    def _select_fraud_scenario(self) -> int:
        """Select fraud scenario based on configured weights."""
        return self.py_rng.choices(
            range(len(self.config.fraud_scenario_weights)),
            weights=self.config.fraud_scenario_weights,
            k=1,
        )[0]

    def generate_batch(
        self,
        batch_size: int,
        start_time: datetime,
    ) -> list[dict]:
        """Generate a batch of transactions (some fraudulent, some legitimate)."""
        transactions = []
        for _ in range(batch_size):
            tx = self._generate_single_transaction(start_time)
            transactions.append(tx)
        return transactions

    def _generate_single_transaction(self, base_time: datetime) -> dict:
        """Generate a single transaction with timestamp offset for spacing."""
        tx_id = self.rng.integers(1, 2**31)
        customer_id = int(self.rng.integers(0, self.config.num_customers))

        terminals = self._customer_terminals[customer_id]
        terminal_id = self.py_rng.choice(terminals)

        # Amount with noise around customer profile
        profile = self._customer_profiles[customer_id]
        amount = max(0.01, float(self.rng.normal(
            profile["mean_amount"], profile["std_amount"]
        )))

        # Determine fraud scenario
        scenario = self._select_fraud_scenario()
        is_fraud = 1 if scenario > 0 else 0

        # Fraudulent transactions have inflated amounts
        if is_fraud:
            amount *= self.rng.uniform(2.0, 10.0)

        # Timestamp within the batch window
        offset_ms = int(self.rng.uniform(0, 999))
        tx_datetime = base_time + timedelta(milliseconds=offset_ms)

        # Elapsed time from simulation start (days since epoch for demo)
        sim_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
        elapsed = tx_datetime - sim_start
        tx_time_seconds = int(elapsed.total_seconds())
        tx_time_days = tx_time_seconds // 86400

        return {
            "TRANSACTION_ID": int(tx_id),
            "TX_DATETIME": tx_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "CUSTOMER_ID": customer_id,
            "TERMINAL_ID": terminal_id,
            "TX_AMOUNT": round(amount, 2),
            "TX_TIME_SECONDS": tx_time_seconds,
            "TX_TIME_DAYS": tx_time_days,
            "_fraud_label": {
                "TX_FRAUD": is_fraud,
                "TX_FRAUD_SCENARIO": scenario,
            },
        }
