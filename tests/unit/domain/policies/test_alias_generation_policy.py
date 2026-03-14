import json

from stock_quant.domain.policies.alias_generation_policy import AliasGenerationPolicy


def test_normalize_name() -> None:
    policy = AliasGenerationPolicy()
    assert policy.normalize_name("Apple, Inc.") == "APPLE INC"


def test_strip_suffixes() -> None:
    policy = AliasGenerationPolicy()
    normalized = policy.normalize_name("Microsoft Corporation")
    assert normalized == "MICROSOFT CORPORATION"
    assert policy.strip_suffixes(normalized) == "MICROSOFT"


def test_generate_aliases_common_company() -> None:
    policy = AliasGenerationPolicy()
    aliases = policy.generate_aliases("Apple Inc.")
    assert "APPLE INC" in aliases
    assert "APPLE" in aliases


def test_generate_aliases_adr_company() -> None:
    policy = AliasGenerationPolicy()
    aliases = policy.generate_aliases("Alibaba Group Holding Ltd ADR")
    assert "ALIBABA GROUP HOLDING LTD ADR" in aliases
    assert "ALIBABA" in aliases


def test_aliases_are_json_serializable() -> None:
    policy = AliasGenerationPolicy()
    aliases = policy.generate_aliases("Apple Inc.")
    payload = json.dumps(aliases)
    assert isinstance(payload, str)
