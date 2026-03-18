from __future__ import annotations

"""
Research-grade manifest schema foundation.

Objectif
--------
Créer les tables minimales nécessaires pour rendre les pipelines
de recherche plus défendables scientifiquement:

- signatures de contenu par source canonique
- manifests de snapshots de dataset research-grade

Principes
---------
- SQL-first: le schéma vit ici, en DDL DuckDB clair et versionnable
- idempotent: relancer l'initialisation ne doit rien casser
- compatible: on évite toute dépendance forte à une infra exotique
- commenté: ce fichier sert aussi de documentation vivante
"""

from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class ResearchManifestSchemaManager:
    """
    Initialise les tables de métadonnées research-grade.

    Cette classe suit le style déjà utilisé dans le repo:
    - manager de schéma simple
    - une méthode publique `ensure_all`
    - helpers privés par table / index
    """

    def ensure_all(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        Point d'entrée principal.

        Important:
        - l'ordre de création est stable
        - la méthode est idempotente
        """
        self._create_research_build_signature(con)
        self._create_research_dataset_manifest(con)
        self._create_research_dataset_input_signature(con)

    def _create_research_build_signature(
        self,
        con: duckdb.DuckDBPyConnection,
    ) -> None:
        """
        Stocke la signature calculée d'un ensemble de sources.

        Usage visé
        ----------
        Exemple:
        - pipeline_name = "build_short_features"
        - signature_scope = "daily_pipeline_skip_guard"
        - source_name = "daily_short_volume_history"
        - signature_hash = "...hash SQL..."
        - row_count / min_date / max_date pour audit lisible

        Pourquoi une table dédiée?
        --------------------------
        On veut distinguer:
        - la signature d'une source
        - le manifest d'un snapshot complet
        """
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_build_signature (
                pipeline_name VARCHAR NOT NULL,
                signature_scope VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,

                -- hash calculé à partir du contenu ou d'agrégats robustes
                signature_hash VARCHAR NOT NULL,

                -- métriques d'audit lisibles par humain
                row_count BIGINT,
                min_business_date DATE,
                max_business_date DATE,

                -- traçabilité opérationnelle
                computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                -- note libre éventuelle pour expliquer la signature
                notes VARCHAR
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_build_signature_lookup
            ON research_build_signature(
                pipeline_name,
                signature_scope,
                source_name,
                computed_at
            )
            """
        )

    def _create_research_dataset_manifest(
        self,
        con: duckdb.DuckDBPyConnection,
    ) -> None:
        """
        Table principale des snapshots de dataset de recherche.

        Grain
        -----
        1 ligne = 1 snapshot logique de dataset research-grade.
        """
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_dataset_manifest (
                snapshot_id VARCHAR NOT NULL,
                dataset_name VARCHAR NOT NULL,

                -- commit git du code qui a produit le snapshot
                git_commit VARCHAR,

                -- paramétrage / contexte de build stocké en JSON texte
                parameters_json VARCHAR,

                -- période logique couverte par le snapshot
                start_date DATE,
                end_date DATE,

                -- informations de traçabilité
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by_pipeline VARCHAR,

                -- résumé de qualité / volumétrie
                source_count BIGINT,
                total_row_count BIGINT,

                -- statut logique du snapshot
                status VARCHAR NOT NULL DEFAULT 'completed',

                -- identifiant humain ou libre
                notes VARCHAR
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_dataset_manifest_snapshot
            ON research_dataset_manifest(snapshot_id)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_dataset_manifest_dataset
            ON research_dataset_manifest(dataset_name, created_at)
            """
        )

    def _create_research_dataset_input_signature(
        self,
        con: duckdb.DuckDBPyConnection,
    ) -> None:
        """
        Détail des signatures d'entrée utilisées par un snapshot.

        Grain
        -----
        1 ligne = 1 source d'entrée pour 1 snapshot.

        Exemple
        -------
        snapshot_id = research_snapshot_2026_03_18_short_features
        source_name = daily_short_volume_history
        signature_hash = ...
        """
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_dataset_input_signature (
                snapshot_id VARCHAR NOT NULL,
                dataset_name VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,

                signature_hash VARCHAR NOT NULL,
                row_count BIGINT,
                min_business_date DATE,
                max_business_date DATE,

                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_dataset_input_signature_snapshot
            ON research_dataset_input_signature(snapshot_id, source_name)
            """
        )
