from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from stock_quant.domain.entities.company import CompanyMaster
from stock_quant.domain.entities.instrument import InstrumentMaster
from stock_quant.domain.entities.ticker_history import IdentifierMapEntry, TickerHistoryEntry


class MasterDataService:
    def build(
        self,
        market_universe_rows: list[dict[str, Any]],
        symbol_reference_rows: list[dict[str, Any]],
    ) -> tuple[list[CompanyMaster], list[InstrumentMaster], list[TickerHistoryEntry], list[IdentifierMapEntry], dict[str, int]]:
        companies: dict[str, CompanyMaster] = {}
        instruments: dict[str, InstrumentMaster] = {}
        ticker_history: list[TickerHistoryEntry] = []
        identifier_map: list[IdentifierMapEntry] = []

        symbol_ref_by_symbol: dict[str, dict[str, Any]] = {}
        for row in symbol_reference_rows:
            symbol = str(row.get("symbol", "")).strip().upper()
            if symbol:
                symbol_ref_by_symbol[symbol] = row

        for row in market_universe_rows:
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol:
                continue

            company_name = self._first_non_empty(
                row.get("company_name"),
                symbol_ref_by_symbol.get(symbol, {}).get("company_name"),
            )
            cik = self._first_non_empty(
                row.get("cik"),
                symbol_ref_by_symbol.get(symbol, {}).get("cik"),
            )
            exchange = self._first_non_empty(
                row.get("exchange_normalized"),
                row.get("exchange_raw"),
                symbol_ref_by_symbol.get(symbol, {}).get("exchange"),
            )
            security_type = self._first_non_empty(row.get("security_type"), "UNKNOWN")
            valid_from = row.get("as_of_date")

            company_id = self._build_company_id(cik=cik, company_name=company_name, symbol=symbol)
            instrument_id = self._build_instrument_id(symbol=symbol, exchange=exchange, security_type=security_type)

            if company_id not in companies:
                companies[company_id] = CompanyMaster(
                    company_id=company_id,
                    company_name=company_name,
                    cik=cik,
                    issuer_name_normalized=self._normalize_company_name(company_name),
                    country_code="US",
                    created_at=datetime.utcnow(),
                )

            if instrument_id not in instruments:
                instruments[instrument_id] = InstrumentMaster(
                    instrument_id=instrument_id,
                    company_id=company_id,
                    symbol=symbol,
                    exchange=exchange,
                    security_type=security_type,
                    share_class=None,
                    is_active=bool(row.get("include_in_universe", True)),
                    created_at=datetime.utcnow(),
                )

            ticker_history.append(
                TickerHistoryEntry(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    exchange=exchange,
                    valid_from=valid_from,
                    valid_to=None,
                    is_current=True,
                    created_at=datetime.utcnow(),
                )
            )

            identifier_map.append(
                IdentifierMapEntry(
                    instrument_id=instrument_id,
                    company_id=company_id,
                    identifier_type="ticker",
                    identifier_value=symbol,
                    is_primary=True,
                    created_at=datetime.utcnow(),
                )
            )

            if cik:
                identifier_map.append(
                    IdentifierMapEntry(
                        instrument_id=instrument_id,
                        company_id=company_id,
                        identifier_type="cik",
                        identifier_value=str(cik),
                        is_primary=True,
                        created_at=datetime.utcnow(),
                    )
                )

        ticker_history = self._dedupe_ticker_history(ticker_history)
        identifier_map = self._dedupe_identifier_map(identifier_map)

        metrics = {
            "market_universe_rows": len(market_universe_rows),
            "symbol_reference_rows": len(symbol_reference_rows),
            "company_master_rows": len(companies),
            "instrument_master_rows": len(instruments),
            "ticker_history_rows": len(ticker_history),
            "identifier_map_rows": len(identifier_map),
        }

        return (
            list(companies.values()),
            list(instruments.values()),
            ticker_history,
            identifier_map,
            metrics,
        )

    def _build_company_id(self, *, cik: str | None, company_name: str | None, symbol: str) -> str:
        if cik:
            return f"COMP:{str(cik).strip()}"
        key = f"{self._normalize_company_name(company_name)}|{symbol}"
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        return f"COMP:HASH:{digest}"

    def _build_instrument_id(self, *, symbol: str, exchange: str | None, security_type: str | None) -> str:
        key = f"{symbol}|{exchange or 'UNKNOWN'}|{security_type or 'UNKNOWN'}"
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        return f"INST:{digest}"

    def _normalize_company_name(self, value: str | None) -> str | None:
        if value is None:
            return None
        return " ".join(str(value).strip().upper().split())

    def _first_non_empty(self, *values: Any) -> str | None:
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    def _dedupe_ticker_history(self, rows: list[TickerHistoryEntry]) -> list[TickerHistoryEntry]:
        seen: set[tuple[str, str, str | None, object]] = set()
        out: list[TickerHistoryEntry] = []
        for row in rows:
            key = (row.instrument_id, row.symbol, row.exchange, row.valid_from)
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out

    def _dedupe_identifier_map(self, rows: list[IdentifierMapEntry]) -> list[IdentifierMapEntry]:
        seen: set[tuple[str, str, str]] = set()
        out: list[IdentifierMapEntry] = []
        for row in rows:
            key = (row.instrument_id, row.identifier_type, row.identifier_value)
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out
