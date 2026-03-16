from __future__ import annotations

import io
import json
import re

import pandas as pd
import requests
from tqdm import tqdm


SEC_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_TICKER_TXT_URL = "https://www.sec.gov/include/ticker.txt"


class SecCompanyTickerLoader:
    """
    Télécharge un mapping ticker <-> CIK officiel depuis la SEC.

    Stratégie
    ---------
    On tente, dans cet ordre :
    1. company_tickers_exchange.json
    2. company_tickers.json
    3. ticker.txt

    Pourquoi ce fallback :
    - certains environnements reçoivent 403 sur l'endpoint exchange
    - on veut rester 100 % source officielle SEC, sans proxy
    - on veut quand même pouvoir peupler symbol_reference_source_raw
      même si `exchange_raw` n'est pas disponible sur le fallback

    Sortie
    ------
    Un DataFrame normalisé avec les colonnes suivantes si possible :
    - symbol
    - company_name
    - cik
    - exchange_raw
    - source_name
    """

    def __init__(
        self,
        *,
        user_agent: str = "stock-quant-oop research pipeline marty@example.com",
        timeout_seconds: float = 120.0,
        chunk_size_bytes: int = 1024 * 1024,
        session: requests.Session | None = None,
    ) -> None:
        self._user_agent = str(user_agent).strip()
        self._timeout_seconds = float(timeout_seconds)
        self._chunk_size_bytes = int(chunk_size_bytes)
        self._session = session or requests.Session()

    def download_frame(self) -> pd.DataFrame:
        """
        Télécharge la meilleure source SEC disponible et la convertit
        en DataFrame pandas.
        """
        loaders = [
            ("company_tickers_exchange.json", SEC_TICKERS_EXCHANGE_URL, self._load_company_tickers_exchange_json),
            ("company_tickers.json", SEC_TICKERS_URL, self._load_company_tickers_json),
            ("ticker.txt", SEC_TICKER_TXT_URL, self._load_ticker_txt),
        ]

        last_error: Exception | None = None

        for source_name, url, loader in loaders:
            try:
                return loader(url=url, source_name=source_name)
            except Exception as exc:
                last_error = exc
                continue

        raise RuntimeError(f"all SEC ticker sources failed; last_error={last_error}")

    def _load_company_tickers_exchange_json(self, *, url: str, source_name: str) -> pd.DataFrame:
        """
        Charge company_tickers_exchange.json.
        """
        payload = self._download_json_payload(url)
        fields = payload.get("fields")
        data = payload.get("data")

        if not fields or not data:
            raise RuntimeError(f"invalid payload for {source_name}")

        frame = pd.DataFrame(data, columns=fields).reset_index(drop=True)

        rename_map = {
            "ticker": "symbol",
            "name": "company_name",
            "cik": "cik",
            "exchange": "exchange_raw",
        }
        frame = frame.rename(columns=rename_map)

        if "symbol" not in frame.columns:
            raise RuntimeError(f"symbol column missing in {source_name}")

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["cik"] = frame["cik"].astype(str).str.strip().str.zfill(10)
        frame["source_name"] = "sec_company_tickers_exchange"

        required = ["symbol", "company_name", "cik", "exchange_raw", "source_name"]
        for col in required:
            if col not in frame.columns:
                frame[col] = None

        return frame[required]

    def _load_company_tickers_json(self, *, url: str, source_name: str) -> pd.DataFrame:
        """
        Charge company_tickers.json.
        """
        payload = self._download_json_payload(url)

        if isinstance(payload, dict) and "data" in payload and "fields" in payload:
            fields = payload.get("fields")
            data = payload.get("data")
            frame = pd.DataFrame(data, columns=fields).reset_index(drop=True)
            rename_map = {
                "ticker": "symbol",
                "title": "company_name",
                "cik_str": "cik",
            }
            frame = frame.rename(columns=rename_map)
        elif isinstance(payload, dict):
            rows = []
            for _, item in payload.items():
                if not isinstance(item, dict):
                    continue
                rows.append(
                    {
                        "symbol": item.get("ticker"),
                        "company_name": item.get("title"),
                        "cik": item.get("cik_str"),
                    }
                )
            frame = pd.DataFrame(rows).reset_index(drop=True)
        else:
            raise RuntimeError(f"invalid payload type for {source_name}")

        if frame.empty:
            raise RuntimeError(f"empty payload for {source_name}")

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["cik"] = frame["cik"].astype(str).str.strip().str.zfill(10)
        frame["exchange_raw"] = None
        frame["source_name"] = "sec_company_tickers"

        required = ["symbol", "company_name", "cik", "exchange_raw", "source_name"]
        for col in required:
            if col not in frame.columns:
                frame[col] = None

        return frame[required]

    def _load_ticker_txt(self, *, url: str, source_name: str) -> pd.DataFrame:
        """
        Charge ticker.txt, format texte simple :
        <ticker><TAB><cik>
        """
        raw_text = self._download_text_payload(url)

        rows: list[dict[str, object]] = []
        for line in raw_text.splitlines():
            line = line.strip()
            if not line:
                continue

            parts = re.split(r"\s+", line)
            if len(parts) < 2:
                continue

            symbol = str(parts[0]).strip().upper()
            cik = str(parts[1]).strip().zfill(10)

            if not symbol:
                continue

            rows.append(
                {
                    "symbol": symbol,
                    "company_name": None,
                    "cik": cik,
                    "exchange_raw": None,
                    "source_name": "sec_ticker_txt",
                }
            )

        if not rows:
            raise RuntimeError(f"no usable rows in {source_name}")

        frame = pd.DataFrame(rows).drop_duplicates(subset=["symbol", "cik"]).reset_index(drop=True)
        return frame[["symbol", "company_name", "cik", "exchange_raw", "source_name"]]

    def _download_json_payload(self, url: str) -> dict:
        """
        Télécharge un payload JSON SEC avec tqdm.
        """
        raw_bytes = self._download_bytes(url)
        return json.loads(raw_bytes.decode("utf-8"))

    def _download_text_payload(self, url: str) -> str:
        """
        Télécharge un payload texte SEC avec tqdm.
        """
        raw_bytes = self._download_bytes(url)
        return raw_bytes.decode("utf-8")

    def _download_bytes(self, url: str) -> bytes:
        """
        Télécharge un endpoint SEC avec headers conformes et barre de progression.
        """
        headers = self._build_headers()

        with self._session.get(
            url,
            headers=headers,
            timeout=self._timeout_seconds,
            stream=True,
        ) as response:
            response.raise_for_status()

            content_length_header = response.headers.get("Content-Length")
            total_bytes = (
                int(content_length_header)
                if content_length_header and content_length_header.isdigit()
                else None
            )

            buffer = io.BytesIO()

            with tqdm(
                total=total_bytes,
                desc="sec_tickers",
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                dynamic_ncols=True,
                leave=True,
            ) as progress:
                for chunk in response.iter_content(chunk_size=self._chunk_size_bytes):
                    if not chunk:
                        continue
                    buffer.write(chunk)
                    progress.update(len(chunk))

        raw_bytes = buffer.getvalue()
        if not raw_bytes:
            raise RuntimeError(f"empty response body from {url}")

        return raw_bytes

    def _build_headers(self) -> dict[str, str]:
        """
        Construit les headers HTTP SEC-compliant.
        """
        headers = {
            "User-Agent": self._user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
            "Referer": "https://www.sec.gov/",
        }

        email_match = re.search(
            r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
            self._user_agent,
            re.IGNORECASE,
        )
        if email_match:
            headers["From"] = email_match.group(1)

        return headers
