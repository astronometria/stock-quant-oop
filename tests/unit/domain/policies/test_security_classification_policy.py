from stock_quant.domain.policies.security_classification_policy import SecurityClassificationPolicy


def test_classify_common_stock() -> None:
    policy = SecurityClassificationPolicy()
    result = policy.classify("Apple Inc.", "Common Stock")
    assert result["security_type"] == "COMMON_STOCK"
    assert result["is_common_stock"] is True
    assert result["is_etf"] is False
    assert result["is_adr"] is False


def test_classify_etf() -> None:
    policy = SecurityClassificationPolicy()
    result = policy.classify("SPDR S&P 500 ETF Trust", "ETF")
    assert result["security_type"] == "ETF"
    assert result["is_etf"] is True
    assert result["is_common_stock"] is False


def test_classify_adr() -> None:
    policy = SecurityClassificationPolicy()
    result = policy.classify("Alibaba Group Holding Ltd ADR", "ADR")
    assert result["security_type"] == "ADR"
    assert result["is_adr"] is True


def test_classify_warrant() -> None:
    policy = SecurityClassificationPolicy()
    result = policy.classify("Example Warrant Corp Warrant", "Warrant")
    assert result["security_type"] == "WARRANT"
    assert result["is_warrant"] is True
    assert result["is_common_stock"] is False


def test_classify_unit_and_right_and_preferred() -> None:
    policy = SecurityClassificationPolicy()

    unit_result = policy.classify("Example Capital Units", "Unit")
    assert unit_result["security_type"] == "UNIT"
    assert unit_result["is_unit"] is True

    right_result = policy.classify("Example Rights Offering", "Right")
    assert right_result["security_type"] == "RIGHT"
    assert right_result["is_right"] is True

    pref_result = policy.classify("Example Preferred Holdings", "Preferred")
    assert pref_result["security_type"] == "PREFERRED"
    assert pref_result["is_preferred"] is True
