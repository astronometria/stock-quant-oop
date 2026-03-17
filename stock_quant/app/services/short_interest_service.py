from __future__ import annotations

class ShortInterestService:
    """
    Backward-compatible service.

    Accepts both:
    - repository (new)
    - market_policy (legacy tests)
    """

    def __init__(self, repository=None, market_policy=None):
        self.repository = repository
        self.market_policy = market_policy
