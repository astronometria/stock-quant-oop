def build_sql():
    return """
    -- zscore price
    (close - AVG(close) OVER w20) /
    NULLIF(STDDEV_SAMP(close) OVER w20,0) AS price_zscore,

    -- Bollinger bands
    AVG(close) OVER w20 + 2*STDDEV_SAMP(close) OVER w20 AS bb_upper,
    AVG(close) OVER w20 - 2*STDDEV_SAMP(close) OVER w20 AS bb_lower,

    -- distance to bands
    (close - AVG(close) OVER w20) /
    NULLIF(2*STDDEV_SAMP(close) OVER w20,0) AS bb_position
    """
