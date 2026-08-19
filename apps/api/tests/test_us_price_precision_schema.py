from sqlalchemy import Numeric

from app.db_models import USEquityFundamentalSnapshot, USEquityPriceBar, USEquityScreeningSnapshot


def test_us_market_columns_support_decimal_precision_without_changing_krx_tables() -> None:
    columns = [
        USEquityPriceBar.__table__.c.open_price,
        USEquityPriceBar.__table__.c.close_price,
        USEquityPriceBar.__table__.c.turnover_usd,
        USEquityFundamentalSnapshot.__table__.c.revenue_usd,
        USEquityScreeningSnapshot.__table__.c.composite_score,
    ]

    assert all(isinstance(column.type, Numeric) for column in columns)
    assert all(column.type.scale == 6 for column in columns)
