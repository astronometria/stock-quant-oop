from __future__ import annotations

import io
import time

import pandas as pd
import requests
from tqdm import tqdm


NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"


class NasdaqSymbolDirectoryLoader:
    """
    Télécharge les fichiers officiels du Nasdaq Symbol Directory.

    Notes importantes
    -----------------
    - On utilise `www.nasdaqtrader.com`, qui répond correctement en HTTPS.
    - On garde une barre de progression tqdm propre et non spammante.
    - On ajoute un retry simple avec pause pour éviter les faux échecs réseau.
    - Ce loader ne touche pas la DB : il ne fait que télécharger et parser.
    """

    def __init__(
        self,
        *,
        timeout_seconds: float = 120.0,
        retries: int = 3,
        retry_sleep_seconds: float = 2.0,
        session: requests.Session | None = None,
    ) -> None:
        self._timeout_seconds = float(timeout_seconds)
        self._retries = int(retries)
        self._retry_sleep_seconds = float(retry_sleep_seconds)
        self._session = session or requests.Session()

    def download_frames(self) -> dict[str, pd.DataFrame]:
        """
        Télécharge et parse les deux fichiers Nasdaq :
        - nasdaqlisted.txt
        - otherlisted.txt

        Retourne un dict `name -> DataFrame`.
        """
        frames: dict[str, pd.DataFrame] = {}

        sources = {
            "nasdaqlisted": NASDAQ_LISTED_URL,
            "otherlisted": OTHER_LISTED_URL,
        }

        for name, url in tqdm(
            sources.items(),
            desc="nasdaq_symbols",
            unit="file",
            dynamic_ncols=True,
            leave=True,
        ):
            frames[name] = self._download_single_frame(url=url)

        return frames

    def _download_single_frame(self, *, url: str) -> pd.DataFrame:
        """
        Télécharge un fichier texte Nasdaq et le convertit en DataFrame pandas.

        Le format Nasdaq Symbol Directory est un texte pipe-delimited.
        La dernière ligne de type `File Creation Time` est ignorée.
        """
        last_error: Exception | None = None

        for attempt in range(1, self._retries + 1):
            try:
                response = self._session.get(
                    url,
                    timeout=self._timeout_seconds,
                )
                response.raise_for_status()

                raw_text = response.text
                if not raw_text.strip():
                    raise RuntimeError(f"empty response body from {url}")

                # Retire la ligne finale "File Creation Time" si présente,
                # car elle n'appartient pas réellement au dataset tabulaire.
                lines = raw_text.splitlines()
                cleaned_lines = [
                    line
                    for line in lines
                    if not line.startswith("File Creation Time")
                ]

                cleaned_text = "\n".join(cleaned_lines).strip()
                if not cleaned_text:
                    raise RuntimeError(f"no tabular content after cleaning {url}")

                frame = pd.read_csv(io.StringIO(cleaned_text), sep="|")
                frame.columns = [str(col).strip() for col in frame.columns]

                return frame.reset_index(drop=True)

            except Exception as exc:
                last_error = exc
                if attempt >= self._retries:
                    break
                time.sleep(self._retry_sleep_seconds)

        raise RuntimeError(f"failed to download Nasdaq symbol directory file: {url}; last_error={last_error}")
