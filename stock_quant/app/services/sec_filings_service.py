from __future__ import annotations

"""
Compatibility shim.

Why this file exists
--------------------
This module name used to carry overlapping fundamentals logic, which created
design duplication with stock_quant.app.services.fundamentals_service.

The canonical service is now:
    stock_quant.app.services.fundamentals_service.FundamentalsService

Any remaining imports through this file continue to work, but all new code
must import the canonical module directly.
"""

from stock_quant.app.services.fundamentals_service import FundamentalsService

__all__ = ["FundamentalsService"]
