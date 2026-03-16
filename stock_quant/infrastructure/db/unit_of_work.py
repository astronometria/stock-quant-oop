from __future__ import annotations

from types import TracebackType

from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.shared.exceptions import RepositoryError


class DuckDbUnitOfWork:
    def __init__(self, session_factory: DuckDbSessionFactory) -> None:
        self.session_factory = session_factory
        self.connection = None

    def __enter__(self) -> "DuckDbUnitOfWork":
        self.connection = self.session_factory.create()
        return self

    def commit(self) -> None:
        if self.connection is None:
            raise RepositoryError("cannot commit without an active connection")
        self.connection.commit()

    def rollback(self) -> None:
        if self.connection is None:
            return
        try:
            self.connection.rollback()
        except Exception as exc:
            message = str(exc).lower()
            if "no transaction is active" not in message:
                raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        try:
            if self.connection is not None:
                if exc is None:
                    self.connection.commit()
                else:
                    self.rollback()
        finally:
            if self.connection is not None:
                self.connection.close()
                self.connection = None
        return False
