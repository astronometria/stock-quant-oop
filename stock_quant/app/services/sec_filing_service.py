from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from stock_quant.domain.entities.sec_filing import SecFiling


class SecFilingService:
    def build_filings(
        self,
        raw_index_rows: list[dict[str, Any]],
        cik_company_map: dict[str, str],
    ) -> tuple[list[SecFiling], dict[str, int]]:
        filings: list[SecFiling] = []

        for row in raw_index_rows:
            cik = str(row.get("cik", "")).strip().zfill(10)
            accession_number = str(row.get("accession_number", "")).strip()
            if not cik or not accession_number:
                continue

            filing_id = self._build_filing_id(cik=cik, accession_number=accession_number)
            company_id = cik_company_map.get(cik)

            accepted_at = row.get("accepted_at")
            available_at = accepted_at or datetime.utcnow()

            filings.append(
                SecFiling(
                    filing_id=filing_id,
                    company_id=company_id,
                    cik=cik,
                    form_type=str(row.get("form_type", "")).strip(),
                    filing_date=row.get("filing_date"),
                    accepted_at=accepted_at,
                    accession_number=accession_number,
                    filing_url=row.get("filing_url"),
                    primary_document=row.get("primary_document"),
                    available_at=available_at,
                    source_name="sec",
                    created_at=datetime.utcnow(),
                )
            )

        metrics = {
            "raw_index_rows": len(raw_index_rows),
            "sec_filing_rows": len(filings),
            "matched_company_ids": sum(1 for row in filings if row.company_id),
        }
        return filings, metrics

    def _build_filing_id(self, *, cik: str, accession_number: str) -> str:
        key = f"{cik}|{accession_number}"
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:20]
        return f"FILING:{digest}"
