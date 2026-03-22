def build_sql():
    return """
    (
        AVG(close) OVER w20
        -
        AVG(close) OVER w20_shift
    ) AS slope_20d
    """
