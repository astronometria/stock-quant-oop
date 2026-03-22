def build_sql():
    return """
    -- SMA
    AVG(close) OVER w20 AS sma_20,
    AVG(close) OVER w50 AS sma_50,
    AVG(close) OVER w200 AS sma_200,

    -- EMA approximation
    AVG(close) OVER w12 AS ema_12,
    AVG(close) OVER w26 AS ema_26,

    -- MACD
    (AVG(close) OVER w12 - AVG(close) OVER w26) AS macd_line,
    AVG(AVG(close) OVER w12 - AVG(close) OVER w26) OVER w9 AS macd_signal,

    -- slope (trend)
    AVG(close) OVER w20 - AVG(close) OVER w20_shift AS slope_20d,

    -- trend strength
    (AVG(close) OVER w20 - AVG(close) OVER w200) / NULLIF(AVG(close) OVER w200,0) AS trend_strength
    """
