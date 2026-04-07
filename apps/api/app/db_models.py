from datetime import date, datetime

from sqlalchemy import JSON, BigInteger, Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Symbol(Base, TimestampMixin):
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), unique=True, index=True, nullable=False)
    standard_code: Mapped[str | None] = mapped_column(String(32))
    name_ko: Mapped[str] = mapped_column(String(255), nullable=False)
    market: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    instrument_type: Mapped[str] = mapped_column(String(32), default='equity', nullable=False)
    source: Mapped[str] = mapped_column(String(32), default='app', nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TrackedSymbol(Base):
    __tablename__ = 'tracked_symbols'
    __table_args__ = (UniqueConstraint('symbol_id', name='uq_tracked_symbols_symbol_id'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class QuoteSnapshot(Base):
    __tablename__ = 'quote_snapshots'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_state: Mapped[str] = mapped_column(String(32), nullable=False)
    price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    change: Mapped[int] = mapped_column(BigInteger, nullable=False)
    change_pct: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    turnover: Mapped[int] = mapped_column(BigInteger, nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(255))
    as_of: Mapped[str | None] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    is_live: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    fallback: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)


class CurrentQuoteCache(Base):
    __tablename__ = 'current_quote_cache'
    __table_args__ = (UniqueConstraint('symbol_id', name='uq_current_quote_cache_symbol_id'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_state: Mapped[str] = mapped_column(String(32), nullable=False)
    price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    change: Mapped[int] = mapped_column(BigInteger, nullable=False)
    change_pct: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    turnover: Mapped[int] = mapped_column(BigInteger, nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(255))
    as_of: Mapped[str | None] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    is_live: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    fallback: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class MarketStatusSnapshot(Base):
    __tablename__ = 'market_status_snapshots'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    is_trading_day: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255))
    server_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    session_open_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    session_close_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source: Mapped[str] = mapped_column(String(32), default='app', nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EODCollectionRun(Base):
    __tablename__ = 'eod_collection_runs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String(32), nullable=False, default='KRX')
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis')
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default='manual')
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='running', index=True)
    symbols_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_succeeded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    run_metadata: Mapped[dict | None] = mapped_column(JSON)


class DailyPriceBar(Base):
    __tablename__ = 'daily_price_bars'
    __table_args__ = (UniqueConstraint('symbol_id', 'trade_date', name='uq_daily_price_bars_symbol_trade_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    open_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    high_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    low_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    close_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    turnover: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis')
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DartDisclosureSyncRun(Base):
    __tablename__ = 'dart_disclosure_sync_runs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default='manual')
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='running', index=True)
    window_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    window_end: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    page_size: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    pages_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    financial_report_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    run_metadata: Mapped[dict | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DartDisclosureEvent(Base, TimestampMixin):
    __tablename__ = 'dart_disclosure_events'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rcept_no: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    corp_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    corp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    stock_code: Mapped[str | None] = mapped_column(String(16), index=True)
    corp_cls: Mapped[str | None] = mapped_column(String(8))
    report_nm: Mapped[str] = mapped_column(String(255), nullable=False)
    filing_category: Mapped[str] = mapped_column(String(64), nullable=False, default='other', index=True)
    is_financial_report: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    filer_name: Mapped[str | None] = mapped_column(String(255))
    receipt_date: Mapped[date | None] = mapped_column(Date, index=True)
    remark: Mapped[str | None] = mapped_column(String(255))
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    first_seen_run_id: Mapped[int | None] = mapped_column(
        ForeignKey('dart_disclosure_sync_runs.id', ondelete='SET NULL'),
        index=True,
    )
    last_seen_run_id: Mapped[int | None] = mapped_column(
        ForeignKey('dart_disclosure_sync_runs.id', ondelete='SET NULL'),
        index=True,
    )
    fundamentals_capture_status: Mapped[str] = mapped_column(String(32), nullable=False, default='pending', index=True)
    fundamentals_capture_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fundamentals_last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fundamentals_last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fundamentals_last_error: Mapped[str | None] = mapped_column(Text)


class DartFundamentalsCaptureRun(Base):
    __tablename__ = 'dart_fundamentals_capture_runs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default='manual')
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='running', index=True)
    limit_requested: Mapped[int | None] = mapped_column(Integer)
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fs_div: Mapped[str] = mapped_column(String(8), nullable=False, default='CFS')
    events_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_captured: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_skipped: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    run_metadata: Mapped[dict | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DartFundamentalsSnapshot(Base, TimestampMixin):
    __tablename__ = 'dart_fundamentals_snapshots'
    __table_args__ = (
        UniqueConstraint(
            'disclosure_event_id',
            'business_year',
            'report_code',
            'fs_div',
            name='uq_dart_fundamentals_snapshots_event_period_fs_div',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    disclosure_event_id: Mapped[int] = mapped_column(
        ForeignKey('dart_disclosure_events.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    capture_run_id: Mapped[int | None] = mapped_column(
        ForeignKey('dart_fundamentals_capture_runs.id', ondelete='SET NULL'),
        index=True,
    )
    rcept_no: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    corp_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    stock_code: Mapped[str | None] = mapped_column(String(16), index=True)
    corp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    business_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    report_code: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    filing_category: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    fs_div: Mapped[str] = mapped_column(String(8), nullable=False)
    statement_type: Mapped[str] = mapped_column(String(32), nullable=False)
    period_type: Mapped[str] = mapped_column(String(32), nullable=False)
    period_end_month: Mapped[int | None] = mapped_column(Integer)
    currency: Mapped[str | None] = mapped_column(String(16))
    source_status: Mapped[str | None] = mapped_column(String(8))
    source_message: Mapped[str | None] = mapped_column(Text)
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    response_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DartFundamentalsRow(Base, TimestampMixin):
    __tablename__ = 'dart_fundamentals_rows'
    __table_args__ = (
        UniqueConstraint('snapshot_id', 'row_key', name='uq_dart_fundamentals_rows_snapshot_row_key'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(
        ForeignKey('dart_fundamentals_snapshots.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    row_key: Mapped[str] = mapped_column(String(255), nullable=False)
    receipt_number: Mapped[str | None] = mapped_column(String(32))
    report_code: Mapped[str | None] = mapped_column(String(8))
    business_year: Mapped[str | None] = mapped_column(String(8))
    statement_type: Mapped[str | None] = mapped_column(String(32))
    statement_name: Mapped[str | None] = mapped_column(String(255))
    account_id: Mapped[str | None] = mapped_column(String(255))
    account_name: Mapped[str | None] = mapped_column(String(255))
    account_detail: Mapped[str | None] = mapped_column(String(255))
    current_period_name: Mapped[str | None] = mapped_column(String(255))
    current_amount: Mapped[int | None] = mapped_column(BigInteger)
    current_cumulative_amount: Mapped[int | None] = mapped_column(BigInteger)
    prior_period_name: Mapped[str | None] = mapped_column(String(255))
    prior_amount: Mapped[int | None] = mapped_column(BigInteger)
    prior_quarter_name: Mapped[str | None] = mapped_column(String(255))
    prior_quarter_amount: Mapped[int | None] = mapped_column(BigInteger)
    prior_cumulative_amount: Mapped[int | None] = mapped_column(BigInteger)
    pre_prior_period_name: Mapped[str | None] = mapped_column(String(255))
    pre_prior_amount: Mapped[int | None] = mapped_column(BigInteger)
    row_order: Mapped[int | None] = mapped_column(Integer)
    currency: Mapped[str | None] = mapped_column(String(16))
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class DartFundamentalsHighlight(Base, TimestampMixin):
    __tablename__ = 'dart_fundamentals_highlights'
    __table_args__ = (
        UniqueConstraint('snapshot_id', 'metric_key', name='uq_dart_fundamentals_highlights_snapshot_metric_key'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(
        ForeignKey('dart_fundamentals_snapshots.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    metric_key: Mapped[str] = mapped_column(String(64), nullable=False)
    metric_type: Mapped[str] = mapped_column(String(32), nullable=False)
    statement_type: Mapped[str | None] = mapped_column(String(32))
    statement_name: Mapped[str | None] = mapped_column(String(255))
    account_id: Mapped[str | None] = mapped_column(String(255))
    account_name: Mapped[str | None] = mapped_column(String(255))
    current_amount: Mapped[int | None] = mapped_column(BigInteger)
    current_cumulative_amount: Mapped[int | None] = mapped_column(BigInteger)
    prior_amount: Mapped[int | None] = mapped_column(BigInteger)
    prior_cumulative_amount: Mapped[int | None] = mapped_column(BigInteger)
    pre_prior_amount: Mapped[int | None] = mapped_column(BigInteger)
    currency: Mapped[str | None] = mapped_column(String(16))
    ratio_value: Mapped[float | None] = mapped_column(Numeric(18, 4))
