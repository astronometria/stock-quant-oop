from stock_quant.domain.policies.news_symbol_candidate_policy import NewsSymbolCandidatePolicy


def test_normalize_text() -> None:
    policy = NewsSymbolCandidatePolicy()
    assert policy.normalize_text("Apple launches new AI tooling!") == "APPLE LAUNCHES NEW AI TOOLING"


def test_symbol_in_title() -> None:
    policy = NewsSymbolCandidatePolicy()
    title = policy.normalize_text("MSFT expands enterprise agreements")
    assert policy.symbol_in_title("MSFT", title) is True
    assert policy.symbol_in_title("AAPL", title) is False


def test_alias_in_title() -> None:
    policy = NewsSymbolCandidatePolicy()
    title = policy.normalize_text("Alibaba shares rise after stronger outlook")
    assert policy.alias_in_title("ALIBABA", title) is True
    assert policy.alias_in_title("MICROSOFT", title) is False


def test_can_use_alias_match_requires_min_length() -> None:
    policy = NewsSymbolCandidatePolicy()
    assert policy.can_use_alias_match("IBM") is False
    assert policy.can_use_alias_match("APPLE") is True


def test_score_match() -> None:
    policy = NewsSymbolCandidatePolicy()
    assert policy.score_match(match_type="symbol", matched_text="MSFT") == 1.0
    assert policy.score_match(match_type="alias", matched_text="MICROSOFT") == 0.9
    assert policy.score_match(match_type="alias", matched_text="APPLE") == 0.75
