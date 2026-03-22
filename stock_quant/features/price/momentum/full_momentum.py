def build_sql():
    return """
    -- returns
    (close / LAG(close,1) OVER w - 1) AS returns_1d,
    (close / LAG(close,5) OVER w - 1) AS returns_5d,
    (close / LAG(close,10) OVER w - 1) AS returns_10d,
    (close / LAG(close,20) OVER w - 1) AS returns_20d,
    (close / LAG(close,60) OVER w - 1) AS returns_60d,

    -- log return
    LN(close / LAG(close,1) OVER w) AS returns_log_1d,

    -- RSI
    100 - (100 / (1 + (
        AVG(GREATEST(close - LAG(close) OVER w,0)) OVER w14 /
        NULLIF(AVG(GREATEST(LAG(close) OVER w - close,0)) OVER w14,0)
    ))) AS rsi_14,

    -- Williams %R
    ( (MAX(high) OVER w14 - close) /
      NULLIF(MAX(high) OVER w14 - MIN(low) OVER w14,0)
    ) * -100 AS williams_r_14,

    -- stochastic
    (close - MIN(low) OVER w14) /
    NULLIF(MAX(high) OVER w14 - MIN(low) OVER w14,0) AS stoch_k_14
    """
