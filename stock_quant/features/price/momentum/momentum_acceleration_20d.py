def build_sql():
    return """
    (
        (close / LAG(close, 20) OVER w - 1.0)
        -
        (LAG(close, 20) OVER w / LAG(close, 40) OVER w - 1.0)
    ) AS momentum_acceleration_20d
    """
