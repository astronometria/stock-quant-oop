def build_sql():
    return """
    (
        AVG(close) OVER w50
        -
        AVG(close) OVER w50_shift
    ) AS slope_50d
    """
