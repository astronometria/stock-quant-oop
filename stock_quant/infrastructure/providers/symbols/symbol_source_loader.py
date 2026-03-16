from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from stock_quant.domain.ports.providers import SymbolSourcePort


class SymbolSourceLoader(SymbolSourcePort):
    """
    Charge et normalise des fichiers locaux de symboles provenant de vraies sources.

    Sources supportées
    ------------------
    1. SEC company tickers déjà normalisé
       Colonnes attendues typiquement :
       - symbol
       - company_name
       - cik
       - exchange_raw
       - source_name

    2. Nasdaq Symbol Directory - nasdaqlisted
       Colonnes typiques :
       - Symbol
       - Security Name
       - Market Category
       - ETF
       - ...

    3. Nasdaq Symbol Directory - otherlisted
       Colonnes typiques :
       - ACT Symbol
       - Security Name
       - Exchange
       - ETF
       - ...

    Sortie
    ------
    Le DataFrame final est ramené vers le schéma canonique de staging :
    - symbol
    - company_name
    - cik
    - exchange_raw
    - security_type_raw
    - source_name
    - as_of_date
    - ingested_at

    Notes
    -----
    - on garde une logique SQL-first côté DB, mais la normalisation de colonnes
      a naturellement sa place ici dans le provider loader
    - on ne synthétise pas de proxy prix
    - on lit exclusivement des snapshots locaux téléchargés sur disque
    """

    def __init__(self, source_paths: list[str | Path] | None = None) -> None:
        self._source_paths = [
            Path(path).expanduser().resolve() for path in (source_paths or [])
        ]

    def fetch_symbols(self) -> Iterable[Any]:
        frame = self.fetch_symbols_frame()
        return frame.to_dict(orient="records")

    def fetch_symbols_frame(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for path in self._source_paths:
            frame = pd.read_csv(path)
            normalized = self._normalize_source_frame(
                frame=frame,
                source_path=path,
            )
            if normalized is not None and not normalized.empty:
                frames.append(normalized)

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(
            subset=["symbol", "company_name", "cik", "exchange_raw", "source_name", "as_of_date"]
        ).reset_index(drop=True)

        return merged

    def _normalize_source_frame(
        self,
        *,
        frame: pd.DataFrame,
        source_path: Path,
    ) -> pd.DataFrame:
        """
        Détecte le format source et applique la normalisation appropriée.
        """
        frame = frame.copy()
        frame.columns = [str(col).strip() for col in frame.columns]

        lower_columns = {str(col).strip().lower() for col in frame.columns}

        # ------------------------------------------------------------------
        # 1) SEC déjà presque canonique
        # ------------------------------------------------------------------
        if {"symbol", "company_name", "cik", "exchange_raw"}.issubset(lower_columns):
            return self._normalize_sec_frame(frame=frame, source_path=source_path)

        # ------------------------------------------------------------------
        # 2) Nasdaq listed
        # ------------------------------------------------------------------
        if "Symbol" in frame.columns and "Security Name" in frame.columns:
            return self._normalize_nasdaqlisted_frame(frame=frame, source_path=source_path)

        # ------------------------------------------------------------------
        # 3) Other listed
        # ------------------------------------------------------------------
        if "ACT Symbol" in frame.columns and "Security Name" in frame.columns:
            return self._normalize_otherlisted_frame(frame=frame, source_path=source_path)

        # ------------------------------------------------------------------
        # Format inconnu
        # ------------------------------------------------------------------
        return pd.DataFrame()

    def _normalize_sec_frame(self, *, frame: pd.DataFrame, source_path: Path) -> pd.DataFrame:
        """
        Normalisation d'un snapshot SEC.
        """
        rename_map = {
            "symbol": "symbol",
            "company_name": "company_name",
            "cik": "cik",
            "exchange_raw": "exchange_raw",
            "source_name": "source_name",
        }
        frame = frame.rename(columns=rename_map)

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["company_name"] = frame["company_name"].astype(str).str.strip()
        frame["cik"] = frame["cik"].astype(str).str.strip().str.zfill(10)

        if "exchange_raw" not in frame.columns:
            frame["exchange_raw"] = None

        if "source_name" not in frame.columns:
            frame["source_name"] = "sec_company_tickers"

        frame["security_type_raw"] = None
        frame["as_of_date"] = self._infer_as_of_date(source_path)
        frame["ingested_at"] = pd.Timestamp.utcnow()

        return self._finalize_canonical_frame(frame)

    def _normalize_nasdaqlisted_frame(
        self,
        *,
        frame: pd.DataFrame,
        source_path: Path,
    ) -> pd.DataFrame:
        """
        Normalisation du fichier nasdaqlisted.
        """
        frame = frame.rename(
            columns={
                "Symbol": "symbol",
                "Security Name": "company_name",
                "ETF": "etf_flag",
            }
        )

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["company_name"] = frame["company_name"].astype(str).str.strip()

        # Nasdaq listed => exchange explicite
        frame["exchange_raw"] = "NASDAQ"
        frame["cik"] = None
        frame["source_name"] = "nasdaq_symbol_directory"
        frame["security_type_raw"] = frame.apply(
            lambda row: self._infer_security_type(
                company_name=row.get("company_name"),
                etf_flag=row.get("etf_flag"),
            ),
            axis=1,
        )
        frame["as_of_date"] = self._infer_as_of_date(source_path)
        frame["ingested_at"] = pd.Timestamp.utcnow()

        return self._finalize_canonical_frame(frame)

    def _normalize_otherlisted_frame(
        self,
        *,
        frame: pd.DataFrame,
        source_path: Path,
    ) -> pd.DataFrame:
        """
        Normalisation du fichier otherlisted.
        """
        frame = frame.rename(
            columns={
                "ACT Symbol": "symbol",
                "Security Name": "company_name",
                "Exchange": "exchange_code",
                "ETF": "etf_flag",
            }
        )

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["company_name"] = frame["company_name"].astype(str).str.strip()
        frame["exchange_raw"] = frame["exchange_code"].map(
            {
                "A": "NYSE_MKT",
                "N": "NYSE",
                "P": "NYSE_ARCA",
                "Z": "BATS",
                "V": "IEX",
            }
        ).fillna(frame["exchange_code"].astype(str).str.strip().str.upper())

        frame["cik"] = None
        frame["source_name"] = "nasdaq_symbol_directory"
        frame["security_type_raw"] = frame.apply(
            lambda row: self._infer_security_type(
                company_name=row.get("company_name"),
                etf_flag=row.get("etf_flag"),
            ),
            axis=1,
        )
        frame["as_of_date"] = self._infer_as_of_date(source_path)
        frame["ingested_at"] = pd.Timestamp.utcnow()

        return self._finalize_canonical_frame(frame)

    def _finalize_canonical_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoyage final vers le schéma canonique attendu par le loader raw.
        """
        required = [
            "symbol",
            "company_name",
            "cik",
            "exchange_raw",
            "security_type_raw",
            "source_name",
            "as_of_date",
            "ingested_at",
        ]

        for col in required:
            if col not in frame.columns:
                frame[col] = None

        # Élimine les lignes vides / parasites éventuelles
        frame = frame[frame["symbol"].notna()].copy()
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame = frame[frame["symbol"] != ""].copy()
        frame = frame[~frame["symbol"].str.startswith("FILE CREATION TIME", na=False)].copy()

        frame["company_name"] = frame["company_name"].where(
            frame["company_name"].notna(),
            None,
        )
        frame["cik"] = frame["cik"].where(frame["cik"].notna(), None)
        frame["exchange_raw"] = frame["exchange_raw"].where(frame["exchange_raw"].notna(), None)
        frame["security_type_raw"] = frame["security_type_raw"].where(
            frame["security_type_raw"].notna(),
            None,
        )
        frame["source_name"] = frame["source_name"].astype(str).str.strip()

        return frame[required].reset_index(drop=True)

    def _infer_as_of_date(self, source_path: Path) -> date:
        """
        Déduit `as_of_date` depuis le nom du fichier si possible.
        Exemple :
        - nasdaqlisted_2026-03-16.csv
        - sec_company_tickers_2026-03-16.csv
        Sinon fallback à la date du jour.
        """
        stem = source_path.stem
        parts = stem.split("_")

        for part in reversed(parts):
            try:
                return date.fromisoformat(part)
            except Exception:
                continue

        return date.today()

    def _infer_security_type(self, *, company_name: object, etf_flag: object) -> str | None:
        """
        Déduit un `security_type_raw` simple et utile à partir du nom et du flag ETF.

        Ordre de priorité :
        - ETF explicite
        - Warrant / Right / Unit / Preferred / ADR
        - Common / Ordinary
        """
        if str(etf_flag).strip().upper() == "Y":
            return "ETF"

        name = str(company_name or "").strip().upper()

        if "WARRANT" in name or name.endswith(" W"):
            return "WARRANT"
        if "RIGHT" in name or " RIGHTS" in name:
            return "RIGHT"
        if "UNIT" in name or " UNITS" in name:
            return "UNIT"
        if "PREFERRED" in name or "PREF" in name:
            return "PREFERRED"
        if "DEPOSITARY SHARE" in name or "DEPOSITARY SHARES" in name:
            return "PREFERRED"
        if "AMERICAN DEPOSITARY SHARE" in name or "AMERICAN DEPOSITARY SHARES" in name:
            return "ADR"
        if "COMMON STOCK" in name:
            return "COMMON_STOCK"
        if "ORDINARY SHARE" in name or "ORDINARY SHARES" in name:
            return "ORDINARY_SHARES"
        if "COMMON SHARE" in name or "COMMON SHARES" in name:
            return "COMMON_SHARES"

        return None
