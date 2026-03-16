#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# init_market_db.py
# -----------------------------------------------------------------------------
# Entry point principal d'initialisation de la base DuckDB.
#
# Pourquoi cette réécriture:
# - le SchemaManager principal initialise déjà le coeur historique du projet
#   (univers, symbol_reference, prix, FINRA, news)
# - mais les schémas SEC et fundamentals existent séparément dans le repo
# - ils doivent être initialisés ici aussi pour permettre un vrai bootstrap
#   fundamentals basé sur la SEC
#
# Objectifs de cette version:
# - conserver la compatibilité avec le codebase actuel
# - rester défensif si certains managers évoluent
# - initialiser:
#     1) schéma principal
#     2) schéma SEC si présent
#     3) schéma fundamentals si présent
# - valider ce qui peut être validé sans casser sur une méthode optionnelle
# =============================================================================

import argparse
import importlib
from pathlib import Path
from typing import Any

from stock_quant.app.dto.commands import InitDbCommand
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.
    """
    parser = argparse.ArgumentParser(description="Initialize stock-quant-oop DuckDB schema.")
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing tables before re-creating them.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def _resolve_manager_class(module_path: str, candidate_class_names: list[str]) -> type[Any] | None:
    """
    Essaie de résoudre une classe manager à partir d'un module.

    Pourquoi cette fonction:
    - les noms de classes peuvent varier légèrement dans le temps
    - on veut éviter de hardcoder un seul nom si le repo évolue
    - on garde une initialisation robuste pour SEC / fundamentals
    """
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        return None

    for class_name in candidate_class_names:
        manager_class = getattr(module, class_name, None)
        if manager_class is not None:
            return manager_class

    return None


def _call_if_exists(obj: Any, method_name: str, *args: Any, **kwargs: Any) -> bool:
    """
    Appelle une méthode seulement si elle existe.

    Retourne True si la méthode a été appelée, sinon False.
    """
    method = getattr(obj, method_name, None)
    if method is None:
        return False
    method(*args, **kwargs)
    return True


def _initialize_optional_manager(
    *,
    label: str,
    module_path: str,
    candidate_class_names: list[str],
    uow: DuckDbUnitOfWork,
    verbose: bool,
    initialize_kwargs: dict[str, Any] | None = None,
) -> None:
    """
    Initialise un manager optionnel si sa classe est trouvée.

    Cas visés:
    - SEC schema manager
    - Fundamentals schema manager

    Comportement:
    - si le module ou la classe n'existe pas: on log et on continue
    - si initialize() existe: on l'appelle
    - si validate() existe: on l'appelle ensuite
    """
    initialize_kwargs = initialize_kwargs or {}

    manager_class = _resolve_manager_class(
        module_path=module_path,
        candidate_class_names=candidate_class_names,
    )

    if manager_class is None:
        if verbose:
            print(f"[init_market_db] {label}: manager not found in {module_path}, skip")
        return

    manager = manager_class(uow)

    if verbose:
        print(f"[init_market_db] {label}: manager_class={manager.__class__.__name__}")

    initialized = _call_if_exists(manager, "initialize", **initialize_kwargs)
    if verbose:
        print(f"[init_market_db] {label}: initialize_called={initialized}")

    validated = _call_if_exists(manager, "validate")
    if verbose:
        print(f"[init_market_db] {label}: validate_called={validated}")


def _print_runtime_context(command: InitDbCommand, config: Any) -> None:
    """
    Affiche le contexte runtime en mode verbose.
    """
    print(f"[init_market_db] project_root={config.project_root}")
    print(f"[init_market_db] db_path={command.db_path}")
    print(f"[init_market_db] logs_dir={config.logs_dir}")
    print(f"[init_market_db] data_dir={config.data_dir}")
    print(f"[init_market_db] drop_existing={command.drop_existing}")


def _print_table_summary(uow: DuckDbUnitOfWork, verbose: bool) -> None:
    """
    Affiche un résumé des tables réellement présentes après init.

    Utile pour confirmer rapidement que:
    - les tables coeur existent
    - les tables SEC existent
    - les tables fundamentals existent
    """
    if not verbose:
        return

    if uow.connection is None:
        print("[init_market_db] table_summary: no active connection")
        return

    rows = uow.connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
    ).fetchall()

    print(f"[init_market_db] table_count={len(rows)}")
    for row in rows:
        print(f"[init_market_db] table={row[0]}")


def main() -> int:
    """
    Initialise la base du projet.

    Séquence:
    1) charge la config app
    2) initialise le schéma principal via SchemaManager
    3) initialise les schémas additionnels SEC / fundamentals si présents
    4) affiche un résumé des tables en mode verbose
    """
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    command = InitDbCommand(
        db_path=Path(config.db_path),
        verbose=bool(args.verbose),
        drop_existing=bool(args.drop_existing),
    )

    if command.verbose:
        _print_runtime_context(command=command, config=config)

    session_factory = DuckDbSessionFactory(command.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        # ---------------------------------------------------------------------
        # 1) Schéma principal du projet.
        # ---------------------------------------------------------------------
        schema_manager = SchemaManager(uow)
        schema_manager.initialize(drop_existing=command.drop_existing)
        schema_manager.validate()

        if command.verbose:
            print("[init_market_db] core_schema=OK")

        # ---------------------------------------------------------------------
        # 2) Schéma SEC.
        #
        # On ne lui passe pas drop_existing ici pour rester prudent:
        # - le manager SEC visible dans le repo est orienté migration douce
        # - il utilise CREATE TABLE IF NOT EXISTS / ALTER TABLE ADD COLUMN
        # ---------------------------------------------------------------------
        _initialize_optional_manager(
            label="sec_schema",
            module_path="stock_quant.infrastructure.db.sec_schema",
            candidate_class_names=[
                "SecSchemaManager",
            ],
            uow=uow,
            verbose=command.verbose,
        )

        # ---------------------------------------------------------------------
        # 3) Schéma fundamentals.
        #
        # Le nom exact de la classe peut varier; on garde plusieurs candidats.
        # ---------------------------------------------------------------------
        _initialize_optional_manager(
            label="fundamentals_schema",
            module_path="stock_quant.infrastructure.db.fundamentals_schema",
            candidate_class_names=[
                "FundamentalsSchemaManager",
                "FundamentalSchemaManager",
            ],
            uow=uow,
            verbose=command.verbose,
        )

        # ---------------------------------------------------------------------
        # 4) Résumé final.
        # ---------------------------------------------------------------------
        _print_table_summary(uow=uow, verbose=command.verbose)

    print(f"Schema initialized successfully: {command.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
