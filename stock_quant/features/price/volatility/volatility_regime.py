def build_sql():
    return """
    CASE
        WHEN STDDEV_SAMP(close) OVER w20 >
             AVG(STDDEV_SAMP(close) OVER w20) OVER w100
        THEN 1 ELSE 0
    END AS volatility_regime
    """
