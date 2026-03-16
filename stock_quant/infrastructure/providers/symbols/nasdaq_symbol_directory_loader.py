from __future__ import annotations

import io
import requests
import pandas as pd
from tqdm import tqdm


NASDAQ_LISTED_URL = "https://ftp.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://ftp.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"


class NasdaqSymbolDirectoryLoader:
    """
    Download NASDAQ symbol directory files.
    """

    def download_frames(self) -> dict[str, pd.DataFrame]:

        frames: dict[str, pd.DataFrame] = {}

        sources = {
            "nasdaqlisted": NASDAQ_LISTED_URL,
            "otherlisted": OTHER_LISTED_URL,
        }

        for name, url in tqdm(sources.items(), desc="NASDAQ sources", leave=False):

            response = requests.get(url, timeout=60)
            response.raise_for_status()

            raw = io.StringIO(response.text)

            frame = pd.read_csv(raw, sep="|")

            frames[name] = frame

        return frames
