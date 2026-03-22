#!/usr/bin/env python3
"""
Patch minimal et ciblé pour rendre le repo portable vis-à-vis :
- du chemin de la base DuckDB
- des répertoires raw/sec externes
- des tmp/logs hors repo

Le script :
1. sauvegarde chaque fichier modifié en .bak
2. applique des remplacements précis
3. reste volontairement conservateur pour éviter de casser autre chose

IMPORTANT:
- On ne tente pas un refactor global complet ici.
- On corrige d'abord les points qui bloquent la reconstruction de DB.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import re

PROJECT_ROOT = Path("/home/marty/stock-quant-oop").resolve()

RUNTIME_DB = "~/stock-quant-oop-runtime/db/market.duckdb"
RUNTIME_TMP = "~/stock-quant-oop-runtime/tmp"
RUNTIME_LOGS = "~/stock-quant-oop-runtime/logs"
RAW_ROOT = "~/stock-quant-oop-raw/data/raw"
SEC_ROOT = "~/stock-quant-oop-raw/data/sec"


def backup_file(path: Path) -> None:
    """
    Crée une sauvegarde simple en suffixe .bak si elle n'existe pas déjà.
    """
    backup_path = path.with_name(path.name + ".bak")
    if not backup_path.exists():
        shutil.copy2(path, backup_path)
        print(f"BACKUP: {path} -> {backup_path}")
    else:
        print(f"BACKUP EXISTS: {backup_path}")


def rewrite_file(path: Path, new_text: str) -> None:
    """
    Réécrit le fichier si son contenu change.
    """
    old_text = path.read_text(encoding="utf-8")
    if old_text == new_text:
        print(f"UNCHANGED: {path}")
        return
    backup_file(path)
    path.write_text(new_text, encoding="utf-8")
    print(f"REWROTE: {path}")


def replace_literal(text: str, old: str, new: str) -> str:
    return text.replace(old, new)


def patch_settings_loader() -> None:
    """
    Ajoute support de STOCK_QUANT_DB_PATH.
    """
    path = PROJECT_ROOT / "stock_quant/infrastructure/config/settings_loader.py"
    text = path.read_text(encoding="utf-8")

    text = re.sub(
        r"from pathlib import Path\n",
        "from pathlib import Path\nimport os\n",
        text,
        count=1,
    )

    text = re.sub(
        r"""def build_app_config\(db_path: str \| None = None\) -> AppConfig:\n\s+project_root = Path\(__file__\)\.resolve\(\)\.parents\[3\]\n\s+resolved_db_path = Path\(db_path\)\.expanduser\(\) if db_path else None\n\s+return AppConfig\.from_project_root\(project_root=project_root, db_path=resolved_db_path\)\n""",
        """def build_app_config(db_path: str | None = None) -> AppConfig:
    project_root = Path(__file__).resolve().parents[3]

    # Priorité:
    # 1) argument CLI explicite
    # 2) variable d'environnement runtime
    # 3) fallback historique géré par AppConfig.from_project_root(...)
    env_db_path = os.environ.get("STOCK_QUANT_DB_PATH")
    effective_db_path = db_path or env_db_path
    resolved_db_path = Path(effective_db_path).expanduser() if effective_db_path else None

    return AppConfig.from_project_root(project_root=project_root, db_path=resolved_db_path)
""",
        text,
        count=1,
    )

    rewrite_file(path, text)


def patch_rebuild_database_from_scratch() -> None:
    """
    Corrige les defaults absolus du script principal de rebuild.
    """
    path = PROJECT_ROOT / "cli/ops/rebuild_database_from_scratch.py"
    text = path.read_text(encoding="utf-8")

    text = replace_literal(text, 'default="/home/marty/stock-quant-oop"', 'default="~/stock-quant-oop"')
    text = replace_literal(text, 'default="/home/marty/stock-quant-oop/market.duckdb"', f'default="{RUNTIME_DB}"')
    text = replace_literal(text, 'default="/home/marty/stock-quant-oop/tmp"', f'default="{RUNTIME_TMP}"')
    text = replace_literal(text, 'default="/home/marty/stock-quant-oop/data/raw/finra"', 'default="~/stock-quant-oop-raw/data/raw/finra"')

    rewrite_file(path, text)


def patch_run_sec_load_bulk_raw() -> None:
    """
    Corrige le default DB du loader SEC bulk raw.
    """
    path = PROJECT_ROOT / "cli/ops/run_sec_load_bulk_raw.py"
    text = path.read_text(encoding="utf-8")

    text = replace_literal(text, 'default="~/stock-quant-oop/market.duckdb"', f'default="{RUNTIME_DB}"')

    rewrite_file(path, text)


def patch_load_finra_daily_short_volume_raw() -> None:
    """
    Corrige les defaults hardcodés et le repo_root figé.
    """
    path = PROJECT_ROOT / "cli/raw/load_finra_daily_short_volume_raw.py"
    text = path.read_text(encoding="utf-8")

    text = replace_literal(
        text,
        'default="/home/marty/stock-quant-oop/data/raw/finra/daily_short_sale_volume"',
        'default="~/stock-quant-oop-raw/data/raw/finra/daily_short_sale_volume"',
    )
    text = replace_literal(
        text,
        'default="/home/marty/stock-quant-oop/tmp"',
        f'default="{RUNTIME_TMP}"',
    )
    text = replace_literal(
        text,
        'repo_root = Path("/home/marty/stock-quant-oop")',
        'repo_root = Path(__file__).resolve().parents[2]',
    )

    rewrite_file(path, text)


def patch_load_finra_short_interest_source_raw() -> None:
    """
    Corrige le default DB et le root FINRA short interest.
    """
    path = PROJECT_ROOT / "cli/raw/load_finra_short_interest_source_raw.py"
    text = path.read_text(encoding="utf-8")

    text = replace_literal(
        text,
        'default="~/stock-quant-oop/market.duckdb"',
        f'default="{RUNTIME_DB}"',
    )
    text = replace_literal(
        text,
        'default="/home/marty/stock-quant-oop/data/raw/finra/short_interest"',
        'default="~/stock-quant-oop-raw/data/raw/finra/short_interest"',
    )

    rewrite_file(path, text)


def patch_status_scripts() -> None:
    """
    Corrige quelques scripts de statut/outils qui pointent encore sur l'ancienne DB.
    """
    targets = [
        PROJECT_ROOT / "cli/core/pipeline_status.py",
        PROJECT_ROOT / "cli/tools/research_status.py",
        PROJECT_ROOT / "cli/ops/run_sec_download_history.py",
    ]
    for path in targets:
        text = path.read_text(encoding="utf-8")
        text = replace_literal(text, 'default="~/stock-quant-oop/market.duckdb"', f'default="{RUNTIME_DB}"')
        rewrite_file(path, text)


def patch_backtest_and_portfolio_scripts() -> None:
    """
    Corrige les scripts avec DB_PATH en dur.
    """
    targets = [
        "cli/core/run_backtest_momentum.py",
        "cli/core/run_backtest_momentum_reversed.py",
        "cli/core/run_backtest_lgbm.py",
        "cli/core/run_backtest_lgbm_ranker.py",
        "cli/core/run_backtest_lgbm_ranker_v2.py",
        "cli/core/run_backtest_lgbm_weighted.py",
        "cli/core/run_backtest_multifactor.py",
        "cli/core/run_grid_search_sma.py",
        "cli/core/run_portfolio_engine_ranker.py",
        "cli/core/run_portfolio_engine_ranker_v3.py",
        "cli/core/run_portfolio_engine_ranker_v3_20d.py",
        "cli/core/run_portfolio_engine_ranker_v3_20d_quality.py",
        "cli/core/run_portfolio_multi_horizon.py",
        "cli/core/train_model_lgbm.py",
        "cli/core/train_model_lgbm_ranker.py",
    ]

    for rel in targets:
        path = PROJECT_ROOT / rel
        if not path.exists():
            print(f"MISSING: {path}")
            continue

        text = path.read_text(encoding="utf-8")
        text = text.replace('"/home/marty/stock-quant-oop/market.duckdb"', f'"{RUNTIME_DB}"')
        rewrite_file(path, text)


def patch_trainers_model_paths() -> None:
    """
    Corrige les sorties modèles/metrics pour les trainers qui pointent
    en absolu dans le repo.
    """
    targets = [
        PROJECT_ROOT / "cli/core/train_lgbm_ranker_1d_aligned.py",
        PROJECT_ROOT / "cli/core/train_lgbm_ranker_20d_aligned.py",
        PROJECT_ROOT / "cli/core/train_lgbm_ranker_20d_quality.py",
    ]
    for path in targets:
        if not path.exists():
            print(f"MISSING: {path}")
            continue

        text = path.read_text(encoding="utf-8")
        text = text.replace('"/home/marty/stock-quant-oop/models/', '"~/stock-quant-oop-runtime/models/')
        rewrite_file(path, text)


def patch_runtime_logs_in_build_prices() -> None:
    """
    build_prices écrit dans logs/ relatif au cwd.
    On ne change pas encore la logique applicative profonde,
    mais on redirige vers ~/stock-quant-oop-runtime/logs.
    """
    path = PROJECT_ROOT / "cli/core/build_prices.py"
    text = path.read_text(encoding="utf-8")

    text = text.replace('"logs/build_prices.log"', f'"{RUNTIME_LOGS}/build_prices.log"')
    text = text.replace('"logs/provider_symbol_mapping.log"', f'"{RUNTIME_LOGS}/provider_symbol_mapping.log"')
    text = text.replace('"logs/provider_fetch_failures.log"', f'"{RUNTIME_LOGS}/provider_fetch_failures.log"')

    rewrite_file(path, text)


def main() -> None:
    patch_settings_loader()
    patch_rebuild_database_from_scratch()
    patch_run_sec_load_bulk_raw()
    patch_load_finra_daily_short_volume_raw()
    patch_load_finra_short_interest_source_raw()
    patch_status_scripts()
    patch_backtest_and_portfolio_scripts()
    patch_trainers_model_paths()
    patch_runtime_logs_in_build_prices()
    print("DONE")


if __name__ == "__main__":
    main()
