from __future__ import annotations

"""
DuckDB repository for research-grade signatures and manifests.

Objectif
--------
Fournir une API très mince pour:
- écrire / relire les signatures de contenu utilisées par les skips
- écrire des manifests de snapshot research-grade
- garder une couche de persistance testable

Philosophie
-----------
- Python mince
- SQL-first
- beaucoup de commentaires pour faciliter la maintenance
"""

from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb

from stock_quant.infrastructure.db.research_manifest_schema import (
    ResearchManifestSchemaManager,
)


@dataclass(frozen=True)
class ResearchBuildSignature:
    pipeline_name: str
    signature_scope: str
    source_name: str
    signature_hash: str
    row_count: int | None = None
    min_business_date: date | None = None
    max_business_date: date | None = None
    notes: str | None = None


@dataclass(frozen=True)
class ResearchDatasetManifest:
    snapshot_id: str
    dataset_name: str
    git_commit: str | None = None
    parameters_json: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    created_by_pipeline: str | None = None
    source_count: int | None = None
    total_row_count: int | None = None
    status: str = "completed"
    notes: str | None = None


@dataclass(frozen=True)
class ResearchDatasetInputSignature:
    snapshot_id: str
    dataset_name: str
    source_name: str
    signature_hash: str
    row_count: int | None = None
    min_business_date: date | None = None
    max_business_date: date | None = None


class DuckDbResearchManifestRepository:
    """
    Repository mince pour métadonnées research-grade.

    Important
    ---------
    Ce repository ne fait pas les calculs de signature lui-même.
    Il:
    - persiste les résultats
    - relit le dernier état connu
    """

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.schema_manager = ResearchManifestSchemaManager()

    def ensure_tables(self) -> None:
        """
        S'assure que les tables nécessaires existent.
        """
        self.schema_manager.ensure_all(self.con)

    def insert_build_signature(self, item: ResearchBuildSignature) -> None:
        """
        Ajoute une nouvelle signature calculée.

        On choisit volontairement une stratégie append-only:
        - plus simple à auditer
        - permet de voir l'historique des signatures
        """
        self.con.execute(
            """
            INSERT INTO research_build_signature (
                pipeline_name,
                signature_scope,
                source_name,
                signature_hash,
                row_count,
                min_business_date,
                max_business_date,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                item.pipeline_name,
                item.signature_scope,
                item.source_name,
                item.signature_hash,
                item.row_count,
                item.min_business_date,
                item.max_business_date,
                item.notes,
            ],
        )

    def get_latest_build_signature(
        self,
        pipeline_name: str,
        signature_scope: str,
        source_name: str,
    ) -> dict[str, Any] | None:
        """
        Relit la dernière signature connue pour une source précise.
        """
        row = self.con.execute(
            """
            SELECT
                pipeline_name,
                signature_scope,
                source_name,
                signature_hash,
                row_count,
                min_business_date,
                max_business_date,
                computed_at,
                notes
            FROM research_build_signature
            WHERE pipeline_name = ?
              AND signature_scope = ?
              AND source_name = ?
            ORDER BY computed_at DESC
            LIMIT 1
            """,
            [pipeline_name, signature_scope, source_name],
        ).fetchone()

        if row is None:
            return None

        return {
            "pipeline_name": row[0],
            "signature_scope": row[1],
            "source_name": row[2],
            "signature_hash": row[3],
            "row_count": row[4],
            "min_business_date": row[5],
            "max_business_date": row[6],
            "computed_at": row[7],
            "notes": row[8],
        }

    def insert_dataset_manifest(self, item: ResearchDatasetManifest) -> None:
        """
        Insère un manifest principal de snapshot.
        """
        self.con.execute(
            """
            INSERT INTO research_dataset_manifest (
                snapshot_id,
                dataset_name,
                git_commit,
                parameters_json,
                start_date,
                end_date,
                created_by_pipeline,
                source_count,
                total_row_count,
                status,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                item.snapshot_id,
                item.dataset_name,
                item.git_commit,
                item.parameters_json,
                item.start_date,
                item.end_date,
                item.created_by_pipeline,
                item.source_count,
                item.total_row_count,
                item.status,
                item.notes,
            ],
        )

    def insert_dataset_input_signature(
        self,
        item: ResearchDatasetInputSignature,
    ) -> None:
        """
        Insère une signature d'entrée liée à un snapshot.
        """
        self.con.execute(
            """
            INSERT INTO research_dataset_input_signature (
                snapshot_id,
                dataset_name,
                source_name,
                signature_hash,
                row_count,
                min_business_date,
                max_business_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                item.snapshot_id,
                item.dataset_name,
                item.source_name,
                item.signature_hash,
                item.row_count,
                item.min_business_date,
                item.max_business_date,
            ],
        )

    def get_dataset_manifest(
        self,
        snapshot_id: str,
    ) -> dict[str, Any] | None:
        """
        Relit un manifest principal par snapshot_id.
        """
        row = self.con.execute(
            """
            SELECT
                snapshot_id,
                dataset_name,
                git_commit,
                parameters_json,
                start_date,
                end_date,
                created_at,
                created_by_pipeline,
                source_count,
                total_row_count,
                status,
                notes
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [snapshot_id],
        ).fetchone()

        if row is None:
            return None

        return {
            "snapshot_id": row[0],
            "dataset_name": row[1],
            "git_commit": row[2],
            "parameters_json": row[3],
            "start_date": row[4],
            "end_date": row[5],
            "created_at": row[6],
            "created_by_pipeline": row[7],
            "source_count": row[8],
            "total_row_count": row[9],
            "status": row[10],
            "notes": row[11],
        }

    def list_dataset_input_signatures(
        self,
        snapshot_id: str,
    ) -> list[dict[str, Any]]:
        """
        Liste les signatures d'entrée liées à un snapshot.
        """
        rows = self.con.execute(
            """
            SELECT
                snapshot_id,
                dataset_name,
                source_name,
                signature_hash,
                row_count,
                min_business_date,
                max_business_date,
                recorded_at
            FROM research_dataset_input_signature
            WHERE snapshot_id = ?
            ORDER BY source_name
            """,
            [snapshot_id],
        ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "snapshot_id": row[0],
                    "dataset_name": row[1],
                    "source_name": row[2],
                    "signature_hash": row[3],
                    "row_count": row[4],
                    "min_business_date": row[5],
                    "max_business_date": row[6],
                    "recorded_at": row[7],
                }
            )
        return out
