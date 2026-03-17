from __future__ import annotations


class DuckDbFundamentalsRepository:

    """
    Repository fundamentals.

    Règle anti-biais :

    Toute sélection pour features doit filtrer sur available_at.
    """

    def __init__(self, con):

        self.con = con


    def load_sec_fact_normalized_rows(self):

        return self.con.execute(
            """
            SELECT *
            FROM sec_fact_normalized
            WHERE available_at IS NOT NULL
            """
        ).fetchall()


    def insert_snapshots(self, rows):

        if not rows:
            return 0

        self.con.executemany(
            """
            INSERT INTO fundamental_snapshot_quarterly
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        return len(rows)


    def build_daily_features(self):

        self.con.execute(
            """
            INSERT INTO fundamental_features_daily
            SELECT *
            FROM fundamental_snapshot_quarterly
            """
        )

        return self.con.execute(
            "SELECT COUNT(*) FROM fundamental_features_daily"
        ).fetchone()[0]
