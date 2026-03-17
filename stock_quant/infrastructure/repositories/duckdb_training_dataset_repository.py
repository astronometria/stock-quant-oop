from __future__ import annotations


class DuckDbTrainingDatasetRepository:

    def __init__(self, con):

        self.con = con


    def build_dataset(self):

        self.con.execute(
            """
            INSERT INTO training_dataset_daily
            SELECT
                p.symbol,
                p.price_date,
                p.close,
                f.*,
                s.*
            FROM price_history p
            LEFT JOIN fundamental_features_daily f
              ON p.symbol = f.symbol
             AND f.available_at <= p.price_date
            LEFT JOIN short_features_daily s
              ON p.symbol = s.symbol
             AND s.available_at <= p.price_date
            JOIN market_universe u
              ON p.symbol = u.symbol
             AND u.include_in_universe = TRUE
            """
        )

        return self.con.execute(
            "SELECT COUNT(*) FROM training_dataset_daily"
        ).fetchone()[0]
