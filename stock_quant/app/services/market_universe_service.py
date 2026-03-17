from __future__ import annotations

class MarketUniverseService:
    """
    Backward-compatible constructor.

    Accepts legacy policies but does not require them.
    """

    def __init__(
        self,
        exchange_policy=None,
        classification_policy=None,
        inclusion_policy=None,
        conflict_policy=None,
        repository=None,
    ):
        self.exchange_policy = exchange_policy
        self.classification_policy = classification_policy
        self.inclusion_policy = inclusion_policy
        self.conflict_policy = conflict_policy
        self.repository = repository
