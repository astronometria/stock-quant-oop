from __future__ import annotations

from typing import Any


class DuckDbTrainingDatasetRepository:
    """
    Point-in-time training dataset repository.

    Critical anti-bias rules
    ------------------------
    - never read price_latest
    - fundamentals join on available_at <= price_date
    - short features join on available_at <= price_date
    - for each (symbol, price_date), take the latest available upstream row
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    def ensure_table(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS training_dataset_daily (
                symbol VARCHAR,
                price_date DATE,
                close DOUBLE,
                fundamental_available_at TIMESTAMP,
                short_available_at TIMESTAMP,
                fundamentals_payload_json VARCHAR,
                short_payload_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def truncate(self) -> None:
        self.con.execute("DELETE FROM training_dataset_daily")

    def build_dataset(self) -> int:
        self.ensure_table()
        self.truncate()

        self.con.execute(
            """
            INSERT INTO training_dataset_daily (
                symbol,
                price_date,
                close,
                fundamental_available_at,
                short_available_at,
                fundamentals_payload_json,
                short_payload_json,
                created_at
            )
            WITH price_base AS (
                SELECT
                    p.symbol,
                    p.price_date,
                    p.close
                FROM price_history p
                INNER JOIN market_universe u
                    ON UPPER(TRIM(p.symbol)) = UPPER(TRIM(u.symbol))
                   AND u.include_in_universe = TRUE
            ),
            fundamentals_ranked AS (
                SELECT
                    p.symbol,
                    p.price_date,
                    f.available_at,
                    to_json(struct_pack(*COLUMNS(*))) AS payload_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.symbol, p.price_date
                        ORDER BY f.available_at DESC NULLS LAST
                    ) AS rn
                FROM price_base p
                LEFT JOIN fundamental_features_daily f
                    ON UPPER(TRIM(p.symbol)) = UPPER(TRIM(f.symbol))
                   AND f.available_at IS NOT NULL
                   AND CAST(f.available_at AS DATE) <= p.price_date
            ),
            short_ranked AS (
                SELECT
                    p.symbol,
                    p.price_date,
                    COALESCE(
                        s.available_at,
                        s.max_source_available_at
                    ) AS effective_available_at,
                    to_json(struct_pack(*COLUMNS(*))) AS payload_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.symbol, p.price_date
                        ORDER BY COALESCE(s.available_at, s.max_source_available_at) DESC NULLS LAST
                    ) AS rn
                FROM price_base p
                LEFT JOIN short_features_daily s
                    ON UPPER(TRIM(p.symbol)) = UPPER(TRIM(s.symbol))
                   AND COALESCE(s.available_at, s.max_source_available_at) IS NOT NULL
                   AND CAST(COALESCE(s.available_at, s.max_source_available_at) AS DATE) <= p.price_date
            )
            SELECT
                p.symbol,
                p.price_date,
                p.close,
                fr.available_at AS fundamental_available_at,
                sr.effective_available_at AS short_available_at,
                fr.payload_json AS fundamentals_payload_json,
                sr.payload_json AS short_payload_json,
                CURRENT_TIMESTAMP
            FROM price_base p
            LEFT JOIN fundamentals_ranked fr
                ON p.symbol = fr.symbol
               AND p.price_date = fr.price_date
               AND fr.rn = 1
            LEFT JOIN short_ranked sr
                ON p.symbol = sr.symbol
               AND p.price_date = sr.price_date
               AND sr.rn = 1
            """
        )

        row = self.con.execute(
            "SELECT COUNT(*) FROM training_dataset_daily"
        ).fetchone()
        return int(row[0]) if row else 0
