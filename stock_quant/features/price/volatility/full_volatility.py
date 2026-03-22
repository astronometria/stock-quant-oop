def build_sql():
    return """
    -- ATR
    AVG(high - low) OVER w14 AS atr_14,

    -- realized volatility
    STDDEV_SAMP(LN(close / LAG(close,1) OVER w)) OVER w20 AS realized_vol_20d,

    -- Parkinson volatility
    SQRT(AVG(POWER(LN(high/low),2)) OVER w20) AS parkinson_vol,

    -- Garman-Klass
    SQRT(AVG(
        POWER(LN(high/low),2)/2 -
        (2*LN(2)-1)*POWER(LN(close/open),2)
    ) OVER w20) AS garman_klass_vol
    """
