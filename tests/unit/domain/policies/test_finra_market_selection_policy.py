from stock_quant.domain.policies.finra_market_selection_policy import FinraMarketSelectionPolicy


def test_regular_mode() -> None:
    policy = FinraMarketSelectionPolicy()
    assert policy.include_source_market("regular", "regular") is True
    assert policy.include_source_market("regular", "otc") is False


def test_otc_mode() -> None:
    policy = FinraMarketSelectionPolicy()
    assert policy.include_source_market("otc", "otc") is True
    assert policy.include_source_market("otc", "regular") is False


def test_both_mode() -> None:
    policy = FinraMarketSelectionPolicy()
    assert policy.include_source_market("both", "regular") is True
    assert policy.include_source_market("both", "otc") is True


def test_unknown_mode_rejects() -> None:
    policy = FinraMarketSelectionPolicy()
    assert policy.include_source_market("weird", "regular") is False
