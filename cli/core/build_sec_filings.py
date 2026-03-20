#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository
from stock_quant.pipelines.build_sec_filings_pipeline import BuildSecFilingsPipeline
from stock_quant.shared.exceptions import PipelineError, RepositoryError


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes:
    - --db-path permet de cibler explicitement la base DuckDB.
    - --verbose active quelques prints de diagnostic simples.
    """
    parser = argparse.ArgumentParser(
        description="Build normalized SEC filing tables incrementally."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def _require_active_connection(uow: DuckDbUnitOfWork):
    """
    Retourne la connexion DuckDB active portée par le UnitOfWork.

    Pourquoi cette fonction existe:
    - dans ce codebase, les repositories DuckDB consomment une connexion brute
      via `.execute()` / `.executemany()`
    - `DuckDbUnitOfWork` n'expose pas directement `.execute()`
    - il expose la connexion active via `uow.connection`
    - le bug actuel venait du fait que le repository SEC recevait `uow`
      au lieu de `uow.connection`

    On centralise donc ici la vérification pour éviter un autre bug du même type.
    """
    con = uow.connection
    if con is None:
        raise RepositoryError("DuckDbUnitOfWork has no active connection")
    return con


def main() -> int:
    """
    Point d'entrée principal.

    Déroulement:
    1. charge la configuration
    2. initialise les schémas master-data et SEC
    3. ouvre une nouvelle transaction/connexion pour exécuter le pipeline SEC
    4. injecte explicitement la connexion DuckDB au repository SEC
    5. imprime le résumé JSON du pipeline
    """
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_sec_filings] project_root={config.project_root}")
        print(f"[build_sec_filings] db_path={config.db_path}")

    session_factory = DuckDbSessionFactory(config.db_path)

    # ------------------------------------------------------------------
    # Pass 1a: s'assurer que le schéma master-data existe
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        MasterDataSchemaManager(uow).initialize()

    # ------------------------------------------------------------------
    # Pass 1b: s'assurer que le schéma SEC existe
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        SecSchemaManager(uow).initialize()

    # ------------------------------------------------------------------
    # Pass 2: exécuter le pipeline SEC sur une connexion fraîche
    #
    # IMPORTANT:
    # DuckDbSecRepository attend une connexion DuckDB brute,
    # pas le DuckDbUnitOfWork lui-même.
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        con = _require_active_connection(uow)
        repository = DuckDbSecRepository(con)
        pipeline = BuildSecFilingsPipeline(repository=repository)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PipelineError as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
