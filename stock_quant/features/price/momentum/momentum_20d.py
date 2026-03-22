def build_sql():
    return """
    (close / LAG(close, 20) OVER w - 1.0) AS momentum_20d
    """
