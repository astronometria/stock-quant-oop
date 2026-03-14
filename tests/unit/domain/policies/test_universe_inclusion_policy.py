from stock_quant.domain.policies.universe_inclusion_policy import UniverseInclusionPolicy


def test_include_regular_common_stock() -> None:
    policy = UniverseInclusionPolicy()
    include, reason = policy.decide(
        symbol="AAPL",
        exchange_normalized="NASDAQ",
        security_type="COMMON_STOCK",
        is_common_stock=True,
        is_etf=False,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        allow_adr=True,
        is_adr=False,
    )
    assert include is True
    assert reason is None


def test_exclude_otc_security() -> None:
    policy = UniverseInclusionPolicy()
    include, reason = policy.decide(
        symbol="AAPL",
        exchange_normalized="OTC",
        security_type="COMMON_STOCK",
        is_common_stock=True,
        is_etf=False,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        allow_adr=True,
        is_adr=False,
    )
    assert include is False
    assert reason == "otc_security"


def test_exclude_etf() -> None:
    policy = UniverseInclusionPolicy()
    include, reason = policy.decide(
        symbol="SPY",
        exchange_normalized="NYSE_ARCA",
        security_type="ETF",
        is_common_stock=False,
        is_etf=True,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        allow_adr=True,
        is_adr=False,
    )
    assert include is False
    assert reason == "etf_excluded"


def test_exclude_warrant() -> None:
    policy = UniverseInclusionPolicy()
    include, reason = policy.decide(
        symbol="XYZW",
        exchange_normalized="NASDAQ",
        security_type="WARRANT",
        is_common_stock=False,
        is_etf=False,
        is_preferred=False,
        is_warrant=True,
        is_right=False,
        is_unit=False,
        allow_adr=True,
        is_adr=False,
    )
    assert include is False
    assert reason == "warrant_excluded"


def test_adr_can_be_allowed_or_rejected() -> None:
    policy = UniverseInclusionPolicy()

    include_yes, reason_yes = policy.decide(
        symbol="BABA",
        exchange_normalized="NYSE",
        security_type="ADR",
        is_common_stock=False,
        is_etf=False,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        allow_adr=True,
        is_adr=True,
    )
    assert include_yes is True
    assert reason_yes is None

    include_no, reason_no = policy.decide(
        symbol="BABA",
        exchange_normalized="NYSE",
        security_type="ADR",
        is_common_stock=False,
        is_etf=False,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        allow_adr=False,
        is_adr=True,
    )
    assert include_no is False
    assert reason_no == "adr_excluded"
