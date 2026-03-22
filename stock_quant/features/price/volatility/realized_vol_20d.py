def build_sql():
    return """
    STDDEV_SAMP(LN(close / LAG(close,1) OVER w)) OVER w20 AS realized_vol_20d
    """
