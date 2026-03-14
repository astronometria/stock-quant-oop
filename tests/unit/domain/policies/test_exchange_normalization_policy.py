from stock_quant.domain.policies.exchange_normalization_policy import ExchangeNormalizationPolicy


def test_normalize_known_nasdaq_variants() -> None:
    policy = ExchangeNormalizationPolicy()
    assert policy.normalize("NASDAQGS") == "NASDAQ"
    assert policy.normalize("NASDAQ") == "NASDAQ"
    assert policy.normalize("XNAS") == "NASDAQ"


def test_normalize_known_nyse_variants() -> None:
    policy = ExchangeNormalizationPolicy()
    assert policy.normalize("NYSE") == "NYSE"
    assert policy.normalize("XNYS") == "NYSE"
    assert policy.normalize("NYSEARCA") == "NYSE_ARCA"
    assert policy.normalize("AMEX") == "NYSE_AMERICAN"


def test_normalize_otc_variants() -> None:
    policy = ExchangeNormalizationPolicy()
    assert policy.normalize("OTCQX") == "OTC"
    assert policy.normalize("OTCQB") == "OTC"
    assert policy.normalize("PINK") == "OTC"
    assert policy.normalize("OTCMKTS") == "OTC"


def test_normalize_unknown_exchange_keeps_cleaned_value() -> None:
    policy = ExchangeNormalizationPolicy()
    assert policy.normalize("custom-exchange") == "CUSTOM EXCHANGE"


def test_normalize_empty_and_none() -> None:
    policy = ExchangeNormalizationPolicy()
    assert policy.normalize(None) is None
    assert policy.normalize("   ") is None
