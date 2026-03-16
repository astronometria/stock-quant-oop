from __future__ import annotations

# =============================================================================
# duckdb_fundamentals_repository.py
# -----------------------------------------------------------------------------
# Repository DuckDB pour:
# - lire les données SEC normalisées nécessaires au build fundamentals
# - écrire les snapshots quarterly / annual / TTM
# - écrire les features daily
#
# Correctifs importants de cette version:
# 1) charge available_at depuis sec_fact_normalized quand présent
#    -> permet de préserver la logique point-in-time / anti look-ahead
#
# 2) upsert de fundamental_features_daily dédupliqué par:
#       (company_id, as_of_date, period_type)
#    et non plus seulement (company_id, as_of_date)
#    -> évite d'écraser QUARTERLY / ANNUAL / TTM le même jour
#
# 3) code plus défensif et plus commenté pour faciliter la maintenance
# =============================================================================

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.fundamental import FundamentalFeatureDaily, FundamentalSnapshot
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbFundamentalsRepository:
    """
    Repository spécialisé pour la chaîne fundamentals.

    Notes de design:
    - suppose une connexion active via DuckDbUnitOfWork
    - fait du staging temporaire avant delete/insert
    - garde une logique simple, déterministe et idempotente
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        """
        Expose la connexion active.

        On fail fast si le repository est appelé hors contexte UoW actif.
        """
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    # =========================================================================
    # Lectures SEC sources pour le pipeline fundamentals
    # =========================================================================
    def load_sec_filing_rows(self) -> list[dict[str, Any]]:
        """
        Charge les filings SEC déjà normalisés.

        Champs importants:
        - accepted_at
        - available_at

        available_at est la date de disponibilité réelle exploitée plus loin
        pour éviter de dater les features à period_end_date.
        """
        try:
            rows = self.con.execute(
                """
                SELECT
                    filing_id,
                    company_id,
                    cik,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    available_at
                FROM sec_filing
                ORDER BY cik, filing_date, accepted_at
                """
            ).fetchall()

            return [
                {
                    "filing_id": row[0],
                    "company_id": row[1],
                    "cik": row[2],
                    "form_type": row[3],
                    "filing_date": row[4],
                    "accepted_at": row[5],
                    "accession_number": row[6],
                    "available_at": row[7],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load sec_filing rows: {exc}") from exc

    def load_sec_fact_normalized_rows(self) -> list[dict[str, Any]]:
        """
        Charge les faits SEC normalisés.

        Correctif clé:
        - remonte available_at quand la colonne existe dans sec_fact_normalized

        Pourquoi:
        - le schéma SEC du projet stocke available_at
        - même si le service fundamentals retombe surtout sur available_at du filing,
          conserver available_at côté facts rend le flux plus cohérent et
          facilite les évolutions futures
        """
        try:
            # -----------------------------------------------------------------
            # On sonde le schéma avant la requête principale pour rester
            # compatible avec des DB créées avant l'ajout éventuel de colonnes.
            # -----------------------------------------------------------------
            schema_rows = self.con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main'
                  AND table_name = 'sec_fact_normalized'
                ORDER BY ordinal_position
                """
            ).fetchall()

            columns = {str(row[0]).lower() for row in schema_rows}
            has_available_at = "available_at" in columns

            if has_available_at:
                rows = self.con.execute(
                    """
                    SELECT
                        filing_id,
                        company_id,
                        cik,
                        taxonomy,
                        concept,
                        period_end_date,
                        unit,
                        value_text,
                        value_numeric,
                        source_name,
                        available_at
                    FROM sec_fact_normalized
                    ORDER BY cik, period_end_date, concept
                    """
                ).fetchall()

                return [
                    {
                        "filing_id": row[0],
                        "company_id": row[1],
                        "cik": row[2],
                        "taxonomy": row[3],
                        "concept": row[4],
                        "period_end_date": row[5],
                        "unit": row[6],
                        "value_text": row[7],
                        "value_numeric": row[8],
                        "source_name": row[9],
                        "available_at": row[10],
                    }
                    for row in rows
                ]

            # -----------------------------------------------------------------
            # Fallback défensif pour anciennes DB / anciens schémas.
            # -----------------------------------------------------------------
            rows = self.con.execute(
                """
                SELECT
                    filing_id,
                    company_id,
                    cik,
                    taxonomy,
                    concept,
                    period_end_date,
                    unit,
                    value_text,
                    value_numeric,
                    source_name
                FROM sec_fact_normalized
                ORDER BY cik, period_end_date, concept
                """
            ).fetchall()

            return [
                {
                    "filing_id": row[0],
                    "company_id": row[1],
                    "cik": row[2],
                    "taxonomy": row[3],
                    "concept": row[4],
                    "period_end_date": row[5],
                    "unit": row[6],
                    "value_text": row[7],
                    "value_numeric": row[8],
                    "source_name": row[9],
                    "available_at": None,
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load sec_fact_normalized rows: {exc}") from exc

    # =========================================================================
    # API de remplacement compatible avec le pipeline actuel
    # =========================================================================
    def replace_fundamental_snapshot_quarterly(self, rows: list[FundamentalSnapshot]) -> int:
        return self.upsert_fundamental_snapshot_quarterly(rows)

    def replace_fundamental_snapshot_annual(self, rows: list[FundamentalSnapshot]) -> int:
        return self.upsert_fundamental_snapshot_annual(rows)

    def replace_fundamental_ttm(self, rows: list[FundamentalSnapshot]) -> int:
        return self.upsert_fundamental_ttm(rows)

    def replace_fundamental_features_daily(self, rows: list[FundamentalFeatureDaily]) -> int:
        return self.upsert_fundamental_features_daily(rows)

    # =========================================================================
    # Upserts snapshots
    # =========================================================================
    def upsert_fundamental_snapshot_quarterly(self, rows: list[FundamentalSnapshot]) -> int:
        return self._upsert_snapshot_table("fundamental_snapshot_quarterly", rows)

    def upsert_fundamental_snapshot_annual(self, rows: list[FundamentalSnapshot]) -> int:
        return self._upsert_snapshot_table("fundamental_snapshot_annual", rows)

    def upsert_fundamental_ttm(self, rows: list[FundamentalSnapshot]) -> int:
        return self._upsert_snapshot_table("fundamental_ttm", rows)

    def _upsert_snapshot_table(self, table_name: str, rows: list[FundamentalSnapshot]) -> int:
        """
        Upsert générique pour les tables snapshot.

        Clé métier retenue dans chaque table:
        - company_id
        - period_end_date

        Comme quarterly / annual / TTM vivent dans trois tables distinctes,
        cette clé suffit ici.
        """
        try:
            if not rows:
                return 0

            payload = [
                (
                    row.company_id,
                    row.cik,
                    row.period_type,
                    row.period_end_date,
                    row.available_at,
                    row.revenue,
                    row.net_income,
                    row.assets,
                    row.liabilities,
                    row.equity,
                    row.operating_cash_flow,
                    row.shares_outstanding,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
                if row.company_id and row.period_end_date is not None
            ]

            if not payload:
                return 0

            stage_table = f"tmp_{table_name}_stage"

            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    company_id VARCHAR,
                    cik VARCHAR,
                    period_type VARCHAR,
                    period_end_date DATE,
                    available_at TIMESTAMP,
                    revenue DOUBLE,
                    net_income DOUBLE,
                    assets DOUBLE,
                    liabilities DOUBLE,
                    equity DOUBLE,
                    operating_cash_flow DOUBLE,
                    shares_outstanding DOUBLE,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )

            try:
                self.con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        company_id,
                        cik,
                        period_type,
                        period_end_date,
                        available_at,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                # -------------------------------------------------------------
                # Delete ciblé seulement sur les clés métier réellement visées.
                # -------------------------------------------------------------
                self.con.execute(
                    f"""
                    DELETE FROM {table_name} AS target
                    USING {stage_table} AS stage
                    WHERE target.company_id = stage.company_id
                      AND target.period_end_date = stage.period_end_date
                    """
                )

                # -------------------------------------------------------------
                # Insert déterministe: si plusieurs lignes existent dans le stage
                # pour la même clé, on garde:
                # 1) available_at la plus récente
                # 2) created_at la plus récente
                # -------------------------------------------------------------
                self.con.execute(
                    f"""
                    INSERT INTO {table_name} (
                        company_id,
                        cik,
                        period_type,
                        period_end_date,
                        available_at,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        source_name,
                        created_at
                    )
                    SELECT
                        company_id,
                        cik,
                        period_type,
                        period_end_date,
                        available_at,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        source_name,
                        created_at
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY company_id, period_end_date
                                ORDER BY available_at DESC NULLS LAST,
                                         created_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert {table_name}: {exc}") from exc

    # =========================================================================
    # Upsert features daily
    # =========================================================================
    def upsert_fundamental_features_daily(self, rows: list[FundamentalFeatureDaily]) -> int:
        """
        Upsert des features daily.

        Correctif clé:
        - déduplication par (company_id, as_of_date, period_type)

        Pourquoi:
        - le pipeline génère des lignes QUARTERLY / ANNUAL / TTM
        - elles peuvent partager le même as_of_date
        - il faut conserver les 3, pas en écraser 2
        """
        try:
            if not rows:
                return 0

            payload = [
                (
                    row.company_id,
                    row.as_of_date,
                    row.period_type,
                    row.revenue,
                    row.net_income,
                    row.assets,
                    row.liabilities,
                    row.equity,
                    row.operating_cash_flow,
                    row.shares_outstanding,
                    row.net_margin,
                    row.debt_to_equity,
                    row.return_on_assets,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
                if row.company_id and row.as_of_date is not None and row.period_type
            ]

            if not payload:
                return 0

            self.con.execute(
                """
                CREATE TEMP TABLE tmp_fundamental_features_daily_stage (
                    company_id VARCHAR,
                    as_of_date DATE,
                    period_type VARCHAR,
                    revenue DOUBLE,
                    net_income DOUBLE,
                    assets DOUBLE,
                    liabilities DOUBLE,
                    equity DOUBLE,
                    operating_cash_flow DOUBLE,
                    shares_outstanding DOUBLE,
                    net_margin DOUBLE,
                    debt_to_equity DOUBLE,
                    return_on_assets DOUBLE,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )

            try:
                self.con.executemany(
                    """
                    INSERT INTO tmp_fundamental_features_daily_stage (
                        company_id,
                        as_of_date,
                        period_type,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        net_margin,
                        debt_to_equity,
                        return_on_assets,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                # -------------------------------------------------------------
                # Delete ciblé avec period_type inclus.
                # -------------------------------------------------------------
                self.con.execute(
                    """
                    DELETE FROM fundamental_features_daily AS target
                    USING tmp_fundamental_features_daily_stage AS stage
                    WHERE target.company_id = stage.company_id
                      AND target.as_of_date = stage.as_of_date
                      AND target.period_type = stage.period_type
                    """
                )

                # -------------------------------------------------------------
                # Insert déterministe par clé complète.
                # -------------------------------------------------------------
                self.con.execute(
                    """
                    INSERT INTO fundamental_features_daily (
                        company_id,
                        as_of_date,
                        period_type,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        net_margin,
                        debt_to_equity,
                        return_on_assets,
                        source_name,
                        created_at
                    )
                    SELECT
                        company_id,
                        as_of_date,
                        period_type,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        net_margin,
                        debt_to_equity,
                        return_on_assets,
                        source_name,
                        created_at
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY company_id, as_of_date, period_type
                                ORDER BY created_at DESC NULLS LAST
                            ) AS rn
                        FROM tmp_fundamental_features_daily_stage
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute("DROP TABLE IF EXISTS tmp_fundamental_features_daily_stage")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert fundamental_features_daily: {exc}") from exc
