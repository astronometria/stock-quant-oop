def build_sql():
    return """
    LN(close / LAG(close, 1) OVER w) AS returns_log_1d
    """
