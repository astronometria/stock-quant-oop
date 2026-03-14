from __future__ import annotations

from pathlib import Path


class RawPriceLoader:
    def load_from_csv(self, path: Path) -> None:
        raise NotImplementedError("À brancher en passe 3.")

    def load_from_zip(self, path: Path) -> None:
        raise NotImplementedError("À brancher en passe 3.")

    def load_from_directory(self, path: Path) -> None:
        raise NotImplementedError("À brancher en passe 3.")
