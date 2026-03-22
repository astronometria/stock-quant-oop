def build_sql():
    return """
    -- volume momentum
    (volume / AVG(volume) OVER w20) AS volume_ratio,

    -- volume zscore
    (volume - AVG(volume) OVER w20) /
    NULLIF(STDDEV_SAMP(volume) OVER w20,0) AS volume_zscore,

    -- OBV approximation
    SUM(
        CASE
            WHEN close > LAG(close) OVER w THEN volume
            WHEN close < LAG(close) OVER w THEN -volume
            ELSE 0
        END
    ) OVER w AS obv
    """
