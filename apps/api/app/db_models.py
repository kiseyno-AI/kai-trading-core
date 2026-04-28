from datetime import date, datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.orders.domain import BrokerOrderStatus, OrderRequestSubmissionStatus, OrderRequestValidationStatus


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


class FavoriteSymbol(Base):
    __tablename__ = 'favorite_symbols'
    __table_args__ = (UniqueConstraint('symbol_id', name='uq_favorite_symbols_symbol_id'),)

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


class OrderRequest(Base, TimestampMixin):
    __tablename__ = 'order_requests'
    __table_args__ = (
        UniqueConstraint(
            'broker_name',
            'account_reference',
            'client_order_id',
            name='uq_order_requests_broker_account_client_order_id',
        ),
        Index(
            'ix_order_requests_account_submission_created',
            'account_reference',
            'submission_status',
            'created_at',
        ),
        Index(
            'ix_order_requests_account_validation_created',
            'account_reference',
            'validation_status',
            'created_at',
        ),
        Index('ix_order_requests_symbol_created', 'symbol', 'created_at'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_name: Mapped[str] = mapped_column(String(32), nullable=False, default='kis')
    client_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='manual')
    account_reference: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    order_type: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_quantity: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    requested_price: Mapped[float | None] = mapped_column(Numeric(20, 8))
    time_in_force: Mapped[str | None] = mapped_column(String(32))
    strategy_context_payload: Mapped[dict | None] = mapped_column(JSON)
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    validation_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=OrderRequestValidationStatus.PENDING.value,
    )
    validation_payload: Mapped[dict | None] = mapped_column(JSON)
    submission_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=OrderRequestSubmissionStatus.PENDING_SUBMIT.value,
    )
    submission_payload: Mapped[dict | None] = mapped_column(JSON)
    submission_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_submission_error: Mapped[str | None] = mapped_column(Text)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class BrokerOrder(Base, TimestampMixin):
    __tablename__ = 'broker_orders'
    __table_args__ = (
        UniqueConstraint(
            'broker_name',
            'account_reference',
            'client_order_id',
            name='uq_broker_orders_broker_account_client_order_id',
        ),
        UniqueConstraint(
            'broker_name',
            'account_reference',
            'broker_order_id',
            name='uq_broker_orders_broker_account_broker_order_id',
        ),
        Index(
            'ix_broker_orders_account_status_updated',
            'account_reference',
            'current_status',
            'updated_at',
        ),
        Index(
            'ix_broker_orders_symbol_status_updated',
            'symbol',
            'current_status',
            'updated_at',
        ),
        Index('ix_broker_orders_account_updated', 'account_reference', 'updated_at'),
        Index('ix_broker_orders_last_broker_update_at', 'last_broker_update_at'),
        Index('ix_broker_orders_last_reconciled_at', 'last_reconciled_at'),
        Index(
            'ix_broker_orders_account_reconcile_status',
            'account_reference',
            'last_reconcile_status',
            'last_reconciled_at',
        ),
        Index('ix_broker_orders_order_request_id', 'order_request_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_request_id: Mapped[int | None] = mapped_column(
        ForeignKey('order_requests.id', ondelete='SET NULL'),
        nullable=True,
    )
    broker_name: Mapped[str] = mapped_column(String(32), nullable=False, default='kis')
    broker_order_id: Mapped[str | None] = mapped_column(String(128))
    client_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    account_reference: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    order_type: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_quantity: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    requested_price: Mapped[float | None] = mapped_column(Numeric(20, 8))
    accepted_quantity: Mapped[float | None] = mapped_column(Numeric(20, 8))
    accepted_price: Mapped[float | None] = mapped_column(Numeric(20, 8))
    filled_quantity: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False, default=0)
    remaining_quantity: Mapped[float | None] = mapped_column(Numeric(20, 8))
    average_fill_price: Mapped[float | None] = mapped_column(Numeric(20, 8))
    time_in_force: Mapped[str | None] = mapped_column(String(32))
    current_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=BrokerOrderStatus.PENDING_SUBMIT.value,
    )
    status_reason: Mapped[str | None] = mapped_column(Text)
    first_submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_broker_update_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_fill_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconcile_status: Mapped[str | None] = mapped_column(String(32))
    last_reconcile_trigger: Mapped[str | None] = mapped_column(String(32))
    last_reconcile_payload: Mapped[dict | None] = mapped_column(JSON)
    raw_latest_payload: Mapped[dict | None] = mapped_column(JSON)
    snapshot_payload: Mapped[dict | None] = mapped_column(JSON)


class BrokerOrderEvent(Base):
    __tablename__ = 'broker_order_events'
    __table_args__ = (
        UniqueConstraint(
            'broker_order_row_id',
            'event_dedup_key',
            name='uq_broker_order_events_broker_order_dedup_key',
        ),
        Index(
            'ix_broker_order_events_order_timeline',
            'broker_order_row_id',
            'event_occurred_at',
            'id',
        ),
        Index(
            'ix_broker_order_events_account_recency',
            'account_reference',
            'event_occurred_at',
        ),
        Index(
            'ix_broker_order_events_symbol_recency',
            'symbol',
            'event_occurred_at',
        ),
        Index(
            'ix_broker_order_events_client_order_recency',
            'client_order_id',
            'event_occurred_at',
        ),
        Index(
            'ix_broker_order_events_broker_order_recency',
            'broker_order_id',
            'event_occurred_at',
        ),
        Index(
            'ix_broker_order_events_status_recency',
            'resulting_status',
            'event_occurred_at',
        ),
        Index('ix_broker_order_events_order_request_id', 'order_request_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_order_row_id: Mapped[int] = mapped_column(
        ForeignKey('broker_orders.id', ondelete='CASCADE'),
        nullable=False,
    )
    order_request_id: Mapped[int | None] = mapped_column(
        ForeignKey('order_requests.id', ondelete='SET NULL'),
        nullable=True,
    )
    broker_name: Mapped[str] = mapped_column(String(32), nullable=False, default='kis')
    broker_order_id: Mapped[str | None] = mapped_column(String(128))
    client_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    account_reference: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_sequence: Mapped[str | None] = mapped_column(String(64))
    broker_event_id: Mapped[str | None] = mapped_column(String(128))
    resulting_status: Mapped[str | None] = mapped_column(String(32))
    event_occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    quantity_delta: Mapped[float | None] = mapped_column(Numeric(20, 8))
    cumulative_filled_quantity: Mapped[float | None] = mapped_column(Numeric(20, 8))
    fill_price: Mapped[float | None] = mapped_column(Numeric(20, 8))
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    event_dedup_key: Mapped[str] = mapped_column(String(255), nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    normalized_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


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
    per: Mapped[float | None] = mapped_column(Numeric(18, 6))
    pbr: Mapped[float | None] = mapped_column(Numeric(18, 6))
    bps: Mapped[float | None] = mapped_column(Numeric(18, 6))
    eps: Mapped[float | None] = mapped_column(Numeric(18, 6))
    dividend_yield: Mapped[float | None] = mapped_column(Numeric(18, 6))
    dps: Mapped[float | None] = mapped_column(Numeric(18, 6))
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis')
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyPriceContext(Base, TimestampMixin):
    __tablename__ = 'daily_price_contexts'
    __table_args__ = (UniqueConstraint('symbol_id', 'market_date', name='uq_daily_price_contexts_symbol_market_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='pykrx')
    context_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyTechnicalSnapshot(Base, TimestampMixin):
    __tablename__ = 'daily_technical_snapshots'
    __table_args__ = (UniqueConstraint('symbol_id', 'market_date', name='uq_daily_technical_snapshots_symbol_market_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='pykrx')
    close_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    trend_state: Mapped[str] = mapped_column(String(32), nullable=False, default='insufficient_history', index=True)
    ma_alignment: Mapped[str] = mapped_column(String(32), nullable=False, default='insufficient_history', index=True)
    breakout_20d: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    breakdown_20d: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    return_5d: Mapped[float | None] = mapped_column(Numeric(12, 4))
    return_20d: Mapped[float | None] = mapped_column(Numeric(12, 4))
    volume_ratio_20d: Mapped[float | None] = mapped_column(Numeric(12, 4))
    atr_14: Mapped[float | None] = mapped_column(Numeric(14, 4))
    atr_pct_14: Mapped[float | None] = mapped_column(Numeric(12, 4))
    signal_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)



class DailyActionSummary(Base, TimestampMixin):
    __tablename__ = 'daily_action_summaries'
    __table_args__ = (UniqueConstraint('symbol_id', 'market_date', name='uq_daily_action_summaries_symbol_market_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    technical_snapshot_id: Mapped[int | None] = mapped_column(
        ForeignKey('daily_technical_snapshots.id', ondelete='SET NULL'),
        index=True,
    )
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='pykrx')
    action_label: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    bias_direction: Mapped[str] = mapped_column(String(16), nullable=False, default='neutral', index=True)
    signal_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    confidence_tier: Mapped[str] = mapped_column(String(16), nullable=False, default='low', index=True)
    risk_tier: Mapped[str] = mapped_column(String(16), nullable=False, default='medium', index=True)
    trigger_price: Mapped[float | None] = mapped_column(Numeric(14, 4))
    invalidation_price: Mapped[float | None] = mapped_column(Numeric(14, 4))
    support_price: Mapped[float | None] = mapped_column(Numeric(14, 4))
    resistance_price: Mapped[float | None] = mapped_column(Numeric(14, 4))
    reasons: Mapped[list] = mapped_column(JSON, nullable=False)
    summary_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyTrendingRank(Base, TimestampMixin):
    __tablename__ = 'daily_trending_ranks'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            'period',
            name='uq_daily_trending_ranks_symbol_market_date_period',
        ),
        Index('ix_daily_trending_ranks_market_period_absolute', 'market_date', 'period', 'absolute_rank'),
        Index('ix_daily_trending_ranks_market_period_up', 'market_date', 'period', 'up_rank'),
        Index('ix_daily_trending_ranks_market_period_down', 'market_date', 'period', 'down_rank'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    period: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='pykrx')
    metric_name: Mapped[str] = mapped_column(String(64), nullable=False)
    metric_value: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False)
    trend_direction: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    absolute_rank: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    up_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    down_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    price_payload: Mapped[dict | None] = mapped_column(JSON)
    technical_payload: Mapped[dict | None] = mapped_column(JSON)
    action_summary_payload: Mapped[dict | None] = mapped_column(JSON)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailySignalFeature(Base, TimestampMixin):
    __tablename__ = 'daily_signal_features'
    __table_args__ = (UniqueConstraint('symbol_id', 'market_date', name='uq_daily_signal_features_symbol_market_date'),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis_ranking')
    signal_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    volume_power_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    volume_power: Mapped[float | None] = mapped_column(Numeric(12, 4))
    overtime_volume_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    overtime_volume: Mapped[int | None] = mapped_column(BigInteger)
    overtime_fluctuation_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    overtime_fluctuation_rate: Mapped[float | None] = mapped_column(Numeric(12, 4))
    feature_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyMarketRiskSnapshot(Base, TimestampMixin):
    __tablename__ = 'daily_market_risk_snapshots'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_market_risk_snapshots_symbol_market_date',
        ),
        Index('ix_daily_market_risk_snapshots_market_warning', 'market_date', 'market_warning_code'),
        Index('ix_daily_market_risk_snapshots_market_halted', 'market_date', 'is_trade_halted'),
        Index('ix_daily_market_risk_snapshots_market_management', 'market_date', 'is_management_item'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis_quote2_snapshot_v1')
    market_code: Mapped[str] = mapped_column(String(8), nullable=False, default='J')
    source_endpoint: Mapped[str] = mapped_column(String(64), nullable=False, default='inquire-price-2')
    market_warning_code: Mapped[str | None] = mapped_column(String(8))
    market_warning_name: Mapped[str | None] = mapped_column(String(64))
    is_management_item: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    is_investment_caution: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_short_overheated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    short_overheat_class_code: Mapped[str | None] = mapped_column(String(8))
    is_short_sale_overheated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    vi_class_code: Mapped[str | None] = mapped_column(String(8))
    is_vi_triggered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_dishonest_disclosure: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_trade_halted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    snapshot_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyMarketRiskFilterResult(Base, TimestampMixin):
    __tablename__ = 'daily_market_risk_filter_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_market_risk_filter_results_symbol_market_date',
        ),
        Index('ix_daily_market_risk_filter_results_market_passed', 'market_date', 'passed_filter'),
        Index('ix_daily_market_risk_filter_results_market_watchlist', 'market_date', 'watchlist_flagged'),
        Index('ix_daily_market_risk_filter_results_market_state', 'market_date', 'risk_state'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    eod_run_id: Mapped[int | None] = mapped_column(ForeignKey('eod_collection_runs.id', ondelete='SET NULL'), index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis_market_risk_v1')
    risk_state: Mapped[str] = mapped_column(String(16), nullable=False, default='pass', index=True)
    passed_filter: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    watchlist_flagged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    market_warning_code: Mapped[str | None] = mapped_column(String(8))
    market_warning_name: Mapped[str | None] = mapped_column(String(64))
    is_management_item: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_investment_caution: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_short_overheated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    short_overheat_class_code: Mapped[str | None] = mapped_column(String(8))
    is_short_sale_overheated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    vi_class_code: Mapped[str | None] = mapped_column(String(8))
    is_vi_triggered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_dishonest_disclosure: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_trade_halted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    filter_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyPiotroskiFilterResult(Base, TimestampMixin):
    __tablename__ = 'daily_piotroski_filter_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_piotroski_filter_results_symbol_market_date',
        ),
        Index('ix_daily_piotroski_filter_results_market_passed', 'market_date', 'passed_filter'),
        Index('ix_daily_piotroski_filter_results_market_pbr_rank', 'market_date', 'pbr_percentile_rank'),
        Index('ix_daily_piotroski_filter_results_market_score', 'market_date', 'piotroski_score'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='strategy_piotroski_v1')
    pbr: Mapped[float | None] = mapped_column(Numeric(18, 6))
    valuation_available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    valuation_source_status: Mapped[str | None] = mapped_column(String(32))
    pbr_percentile_rank: Mapped[float | None] = mapped_column(Numeric(12, 6))
    is_low_pbr: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_profitability_issue: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_capital_impairment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_audit_issue: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    piotroski_score: Mapped[int | None] = mapped_column(Integer, index=True)
    passes_piotroski_score: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    momentum_percentile_rank: Mapped[float | None] = mapped_column(Numeric(12, 6))
    is_bottom_momentum: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    passed_filter: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    filter_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyLowRiskFactorResult(Base, TimestampMixin):
    __tablename__ = 'daily_low_risk_factor_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_low_risk_factor_results_symbol_market_date',
        ),
        Index('ix_daily_low_risk_factor_results_market_eligible', 'market_date', 'is_low_risk_eligible'),
        Index('ix_daily_low_risk_factor_results_market_score', 'market_date', 'low_risk_score'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='low_risk_v1')
    volatility_60d: Mapped[float | None] = mapped_column(Numeric(12, 6))
    volatility_120d: Mapped[float | None] = mapped_column(Numeric(12, 6))
    max_drawdown_120d: Mapped[float | None] = mapped_column(Numeric(12, 6))
    avg_trading_value_20d: Mapped[float | None] = mapped_column(Numeric(24, 4))
    volatility_60d_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    volatility_120d_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    max_drawdown_120d_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    liquidity_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    volatility_60d_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    volatility_120d_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    max_drawdown_120d_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    liquidity_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    passes_liquidity_floor: Mapped[bool | None] = mapped_column(Boolean)
    low_risk_score: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_risk_classification: Mapped[str] = mapped_column(String(32), nullable=False, default='insufficient_history', index=True)
    is_low_risk_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    history_days_available: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    factor_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyBetaFactorResult(Base, TimestampMixin):
    __tablename__ = 'daily_beta_factor_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_beta_factor_results_symbol_market_date',
        ),
        Index('ix_daily_beta_factor_results_market_eligible', 'market_date', 'is_low_beta_eligible'),
        Index('ix_daily_beta_factor_results_market_score', 'market_date', 'low_beta_score'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='beta_v1')
    benchmark_id: Mapped[str] = mapped_column(String(32), nullable=False, default='KRX_EQW')
    benchmark_market: Mapped[str] = mapped_column(String(32), nullable=False, default='KRX')
    beta_60d: Mapped[float | None] = mapped_column(Numeric(12, 6))
    beta_120d: Mapped[float | None] = mapped_column(Numeric(12, 6))
    beta_60d_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    beta_120d_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    beta_60d_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    beta_120d_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_beta_score: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_beta_classification: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default='insufficient_history',
        index=True,
    )
    is_low_beta_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    history_days_available: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    factor_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyValueFactorResult(Base, TimestampMixin):
    __tablename__ = 'daily_value_factor_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_value_factor_results_symbol_market_date',
        ),
        Index('ix_daily_value_factor_results_market_rank', 'market_date', 'value_rank'),
        Index('ix_daily_value_factor_results_market_score', 'market_date', 'value_score'),
        Index('ix_daily_value_factor_results_market_status', 'market_date', 'value_status'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='value_factor_v1')
    per: Mapped[float | None] = mapped_column(Numeric(18, 6))
    pbr: Mapped[float | None] = mapped_column(Numeric(18, 6))
    dividend_yield: Mapped[float | None] = mapped_column(Numeric(18, 6))
    per_status: Mapped[str] = mapped_column(String(32), nullable=False, default='missing', index=True)
    per_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    pbr_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    dividend_yield_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    per_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    pbr_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    dividend_yield_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    value_score: Mapped[float | None] = mapped_column(Numeric(12, 6))
    value_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    value_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    value_status: Mapped[str] = mapped_column(String(32), nullable=False, default='missing_metric', index=True)
    factor_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyAdvancedValueFactorResult(Base, TimestampMixin):
    __tablename__ = 'daily_advanced_value_factor_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_advanced_value_factor_results_symbol_market_date',
        ),
        Index('ix_daily_advanced_value_factor_results_market_rank', 'market_date', 'advanced_value_rank'),
        Index('ix_daily_advanced_value_factor_results_market_score', 'market_date', 'advanced_value_score'),
        Index(
            'ix_daily_advanced_value_factor_results_market_eligible',
            'market_date',
            'is_advanced_value_eligible',
        ),
        Index(
            'ix_daily_advanced_value_factor_results_market_classification',
            'market_date',
            'advanced_value_classification',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='advanced_value_v1')
    market_cap_proxy: Mapped[float | None] = mapped_column(Numeric(24, 4))
    enterprise_value: Mapped[float | None] = mapped_column(Numeric(24, 4))
    ebitda: Mapped[float | None] = mapped_column(Numeric(24, 4))
    free_cash_flow: Mapped[float | None] = mapped_column(Numeric(24, 4))
    fcf_yield: Mapped[float | None] = mapped_column(Numeric(18, 6))
    ev_to_ebitda: Mapped[float | None] = mapped_column(Numeric(18, 6))
    fcf_yield_status: Mapped[str] = mapped_column(String(48), nullable=False, default='missing_metric', index=True)
    ev_to_ebitda_status: Mapped[str] = mapped_column(
        String(48),
        nullable=False,
        default='missing_metric',
        index=True,
    )
    fcf_yield_valid_for_scoring: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    ev_to_ebitda_valid_for_scoring: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    fcf_yield_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    ev_to_ebitda_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    fcf_yield_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    ev_to_ebitda_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    advanced_value_score: Mapped[float | None] = mapped_column(Numeric(12, 6))
    advanced_value_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    advanced_value_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    advanced_value_components_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    advanced_value_classification: Mapped[str] = mapped_column(
        String(48),
        nullable=False,
        default='insufficient_components',
        index=True,
    )
    is_advanced_value_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    factor_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyCompositeFactorResult(Base, TimestampMixin):
    __tablename__ = 'daily_composite_factor_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_composite_factor_results_symbol_market_date',
        ),
        Index('ix_daily_composite_factor_results_market_rank', 'market_date', 'composite_rank'),
        Index('ix_daily_composite_factor_results_market_score', 'market_date', 'composite_score'),
        Index('ix_daily_composite_factor_results_market_eligible', 'market_date', 'is_composite_eligible'),
        Index(
            'ix_daily_composite_factor_results_market_classification',
            'market_date',
            'composite_classification',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='composite_v1')
    momentum_input: Mapped[float | None] = mapped_column(Numeric(12, 6))
    value_input: Mapped[float | None] = mapped_column(Numeric(12, 6))
    advanced_value_input: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_risk_input: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_beta_input: Mapped[float | None] = mapped_column(Numeric(12, 6))
    momentum_contribution: Mapped[float | None] = mapped_column(Numeric(12, 6))
    value_contribution: Mapped[float | None] = mapped_column(Numeric(12, 6))
    advanced_value_contribution: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_risk_contribution: Mapped[float | None] = mapped_column(Numeric(12, 6))
    low_beta_contribution: Mapped[float | None] = mapped_column(Numeric(12, 6))
    valid_factor_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_base_weight: Mapped[float] = mapped_column(Numeric(12, 6), nullable=False, default=0.0)
    has_required_inputs: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_hard_excluded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    composite_score: Mapped[float | None] = mapped_column(Numeric(12, 6))
    composite_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    composite_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    composite_classification: Mapped[str] = mapped_column(
        String(48),
        nullable=False,
        default='insufficient_inputs',
        index=True,
    )
    is_composite_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    factor_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyMomentumFactorResult(Base, TimestampMixin):
    __tablename__ = 'daily_momentum_factor_results'
    __table_args__ = (
        UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_momentum_factor_results_symbol_market_date',
        ),
        Index('ix_daily_momentum_factor_results_market_rank', 'market_date', 'momentum_rank'),
        Index('ix_daily_momentum_factor_results_market_percentile', 'market_date', 'momentum_percentile'),
        Index('ix_daily_momentum_factor_results_market_eligible', 'market_date', 'is_momentum_eligible'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='daily_price_bars')
    close_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ma_60: Mapped[float | None] = mapped_column(Numeric(18, 6))
    ma_120: Mapped[float | None] = mapped_column(Numeric(18, 6))
    momentum_12_1: Mapped[float | None] = mapped_column(Numeric(18, 6))
    return_5d: Mapped[float | None] = mapped_column(Numeric(18, 6))
    has_sufficient_history: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_price_above_ma120: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_ma60_above_ma120: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_recent_overheated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    momentum_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    momentum_percentile: Mapped[float | None] = mapped_column(Numeric(12, 6))
    is_momentum_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    factor_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DailyMarketParticipationContext(Base, TimestampMixin):
    __tablename__ = 'daily_market_participation_contexts'
    __table_args__ = (
        UniqueConstraint(
            'market',
            'market_date',
            name='uq_daily_market_participation_contexts_market_market_date',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String(32), nullable=False, default='KRX', index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default='kis_ranking')
    context_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AgentContextFeedback(Base, TimestampMixin):
    __tablename__ = 'agent_context_feedback'
    __table_args__ = (
        UniqueConstraint('market_date', 'symbol', 'agent_name', name='uq_agent_context_feedback_market_symbol_agent'),
        Index(
            'ix_agent_context_feedback_market_symbol_agent_received',
            'market_date',
            'symbol',
            'agent_name',
            'received_at',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    agent_name: Mapped[str] = mapped_column(String(64), nullable=False, default='openclaw', index=True)
    agent_version: Mapped[str | None] = mapped_column(String(64))
    run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    summary_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    feedback_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    normalized_feedback_payload: Mapped[dict | None] = mapped_column(JSON)
    details_payload: Mapped[dict | None] = mapped_column(JSON)
    breadth_context_payload: Mapped[dict | None] = mapped_column(JSON)
    leaders_laggards_payload: Mapped[dict | None] = mapped_column(JSON)
    price_action_payload: Mapped[dict | None] = mapped_column(JSON)
    trend_context_payload: Mapped[dict | None] = mapped_column(JSON)
    support_resistance_payload: Mapped[dict | None] = mapped_column(JSON)
    anomalies_and_gaps_payload: Mapped[dict | None] = mapped_column(JSON)
    data_gaps_payload: Mapped[dict | None] = mapped_column(JSON)
    fundamental_backdrop_payload: Mapped[dict | None] = mapped_column(JSON)
    conservative_take_payload: Mapped[dict | None] = mapped_column(JSON)
    improvement_notes_payload: Mapped[list | dict | None] = mapped_column(JSON)
    context_payload: Mapped[dict | None] = mapped_column(JSON)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)


class SignalDeciderDecisionSnapshot(Base, TimestampMixin):
    __tablename__ = 'signal_decider_decision_snapshots'
    __table_args__ = (
        Index(
            'ix_signal_decider_decision_snapshots_market_symbol_created',
            'market_date',
            'symbol',
            'created_at',
        ),
        Index(
            'ix_signal_decider_decision_snapshots_policy_action_created',
            'policy_version',
            'decision_action',
            'created_at',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feedback_id: Mapped[int | None] = mapped_column(
        ForeignKey('agent_context_feedback.id', ondelete='SET NULL'),
        index=True,
    )
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    agent_name: Mapped[str] = mapped_column(String(64), nullable=False, default='signal-decider', index=True)
    agent_version: Mapped[str | None] = mapped_column(String(64))
    decision_run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    feedback_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False, default='signal_decider_policy_v1', index=True)
    decision_action: Mapped[str] = mapped_column(String(16), nullable=False, default='wait', index=True)
    preferred_action: Mapped[str] = mapped_column(String(16), nullable=False, default='wait')
    next_day_open_action: Mapped[str] = mapped_column(String(16), nullable=False, default='wait')
    entry_price_basis: Mapped[str] = mapped_column(String(32), nullable=False, default='next_open')
    target_price: Mapped[int | None] = mapped_column(BigInteger)
    expected_return_pct: Mapped[float | None] = mapped_column(Numeric(12, 6))
    stop_loss_price: Mapped[int | None] = mapped_column(BigInteger)
    suggested_holding_period: Mapped[str | None] = mapped_column(String(64))
    confidence_score_10: Mapped[float | None] = mapped_column(Numeric(12, 6))
    conviction_score_10: Mapped[float | None] = mapped_column(Numeric(12, 6))
    thesis_summary: Mapped[str] = mapped_column(Text, nullable=False)
    supporting_evidence_payload: Mapped[list | dict] = mapped_column(JSON, nullable=False)
    decision_risks_payload: Mapped[list | dict] = mapped_column(JSON, nullable=False)
    invalidation_conditions_payload: Mapped[list | dict] = mapped_column(JSON, nullable=False)
    data_quality_warnings_payload: Mapped[list | dict] = mapped_column(JSON, nullable=False)
    specialist_disagreement_payload: Mapped[list | dict] = mapped_column(JSON, nullable=False)
    traceability_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    input_feature_snapshot_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    response_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    response_summary_payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class SignalDeciderRealizedOutcome(Base, TimestampMixin):
    __tablename__ = 'signal_decider_realized_outcomes'
    __table_args__ = (
        UniqueConstraint(
            'decision_snapshot_id',
            'horizon_days',
            name='uq_signal_decider_realized_outcomes_snapshot_horizon',
        ),
        Index(
            'ix_signal_decider_realized_outcomes_market_symbol_horizon',
            'market_date',
            'symbol',
            'horizon_days',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_snapshot_id: Mapped[int] = mapped_column(
        ForeignKey('signal_decider_decision_snapshots.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    feedback_id: Mapped[int | None] = mapped_column(
        ForeignKey('agent_context_feedback.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False, default='signal_decider_policy_v1', index=True)
    decision_action: Mapped[str] = mapped_column(String(16), nullable=False, default='wait', index=True)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    outcome_status: Mapped[str] = mapped_column(String(32), nullable=False, default='pending', index=True)
    entry_trade_date: Mapped[date | None] = mapped_column(Date, index=True)
    exit_trade_date: Mapped[date | None] = mapped_column(Date, index=True)
    bars_observed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    entry_price: Mapped[int | None] = mapped_column(BigInteger)
    exit_price: Mapped[int | None] = mapped_column(BigInteger)
    max_high_price: Mapped[int | None] = mapped_column(BigInteger)
    min_low_price: Mapped[int | None] = mapped_column(BigInteger)
    close_return_pct: Mapped[float | None] = mapped_column(Numeric(12, 6))
    max_upside_return_pct: Mapped[float | None] = mapped_column(Numeric(12, 6))
    max_drawdown_return_pct: Mapped[float | None] = mapped_column(Numeric(12, 6))
    target_price: Mapped[int | None] = mapped_column(BigInteger)
    stop_loss_price: Mapped[int | None] = mapped_column(BigInteger)
    expected_return_pct: Mapped[float | None] = mapped_column(Numeric(12, 6))
    target_hit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stop_loss_hit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    target_stop_collision: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    price_path_payload: Mapped[list | dict | None] = mapped_column(JSON)
    diagnostics_payload: Mapped[dict | None] = mapped_column(JSON)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SignalDeciderEvaluation(Base, TimestampMixin):
    __tablename__ = 'signal_decider_evaluations'
    __table_args__ = (
        UniqueConstraint(
            'decision_snapshot_id',
            'horizon_days',
            'evaluation_version',
            name='uq_signal_decider_evaluations_snapshot_horizon_version',
        ),
        Index(
            'ix_signal_decider_evaluations_market_symbol_horizon_version',
            'market_date',
            'symbol',
            'horizon_days',
            'evaluation_version',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_snapshot_id: Mapped[int] = mapped_column(
        ForeignKey('signal_decider_decision_snapshots.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    feedback_id: Mapped[int | None] = mapped_column(
        ForeignKey('agent_context_feedback.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )
    realized_outcome_id: Mapped[int | None] = mapped_column(
        ForeignKey('signal_decider_realized_outcomes.id', ondelete='SET NULL'),
        index=True,
    )
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False, default='signal_decider_policy_v1', index=True)
    decision_action: Mapped[str] = mapped_column(String(16), nullable=False, default='wait', index=True)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    evaluation_version: Mapped[str] = mapped_column(String(64), nullable=False, default='signal_decider_eval_v1', index=True)
    evaluation_status: Mapped[str] = mapped_column(String(32), nullable=False, default='pending', index=True)
    grade_label: Mapped[str] = mapped_column(String(64), nullable=False, default='pending_horizon', index=True)
    grade_score: Mapped[float | None] = mapped_column(Numeric(12, 6))
    missed_opportunity: Mapped[bool | None] = mapped_column(Boolean)
    counterfactual_action: Mapped[str | None] = mapped_column(String(16))
    rationale_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    diagnostics_payload: Mapped[dict | None] = mapped_column(JSON)
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SignalDeciderPolicyCandidate(Base, TimestampMixin):
    __tablename__ = 'signal_decider_policy_candidates'
    __table_args__ = (
        UniqueConstraint('candidate_version', name='uq_signal_decider_policy_candidates_version'),
        Index(
            'ix_signal_decider_policy_candidates_policy_status_generated',
            'base_policy_version',
            'status',
            'generated_at',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    base_policy_version: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    evaluation_version: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    market_date_from: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    market_date_to: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    focus_action: Mapped[str | None] = mapped_column(String(16), index=True)
    candidate_kind: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    feature_key: Mapped[str | None] = mapped_column(String(128), index=True)
    feature_value: Mapped[str | None] = mapped_column(String(128), index=True)
    affected_decisions_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    support_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    contradiction_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    support_ratio: Mapped[float | None] = mapped_column(Numeric(12, 6))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='generated', index=True)
    activation_mode: Mapped[str] = mapped_column(String(32), nullable=False, default='manual_only')
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    hypothesis: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    recommendation_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)


class AgentContextFeedbackFailure(Base, TimestampMixin):
    __tablename__ = 'agent_context_feedback_failures'
    __table_args__ = (
        UniqueConstraint('market_date', 'symbol', 'agent_name', name='uq_agent_context_feedback_failures_market_symbol_agent'),
        Index(
            'ix_agent_context_feedback_failures_market_symbol_agent_received',
            'market_date',
            'symbol',
            'agent_name',
            'received_at',
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    agent_name: Mapped[str] = mapped_column(String(64), nullable=False, default='openclaw', index=True)
    run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    failure_kind: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    summary_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    context_payload: Mapped[dict | None] = mapped_column(JSON)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)


class AgentContextFeedbackRun(Base, TimestampMixin):
    __tablename__ = 'agent_context_feedback_runs'
    __table_args__ = (
        Index('ix_agent_context_feedback_runs_market_date_status_started', 'market_date', 'status', 'started_at'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='running', index=True)
    symbols_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    request_payload: Mapped[dict | None] = mapped_column(JSON)
    summary_payload: Mapped[dict | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentFeedbackSessionCleanupFailure(Base, TimestampMixin):
    __tablename__ = 'agent_feedback_session_cleanup_failures'
    __table_args__ = (
        UniqueConstraint('session_key', name='uq_agent_feedback_session_cleanup_failures_session_key'),
        Index('ix_agent_feedback_session_cleanup_failures_next_attempt', 'next_attempt_at'),
        Index('ix_agent_feedback_session_cleanup_failures_market_agent', 'market_date', 'agent_name'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    agent_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    session_key: Mapped[str] = mapped_column(String(255), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default='openclaw')
    delete_transcript: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    emit_lifecycle_hooks: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cleanup_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_error: Mapped[str | None] = mapped_column(Text)
    cleanup_result: Mapped[dict | None] = mapped_column(JSON)

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


class DartFundamentalsRefreshRun(Base):
    __tablename__ = 'dart_fundamentals_refresh_runs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    trigger: Mapped[str] = mapped_column(String(64), nullable=False, default='manual')
    status: Mapped[str] = mapped_column(String(32), nullable=False, default='running', index=True)
    fs_div: Mapped[str] = mapped_column(String(8), nullable=False, default='CFS')
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    symbol_limit: Mapped[int | None] = mapped_column(Integer)
    symbol_offset: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_checked: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_missing: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_stale: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_fresh: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_refreshed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_detected: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_captured: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
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


class DartAuditSignal(Base, TimestampMixin):
    __tablename__ = 'dart_audit_signals'
    __table_args__ = (
        UniqueConstraint('rcept_no', name='uq_dart_audit_signals_rcept_no'),
        Index('ix_dart_audit_signals_stock_receipt', 'stock_code', 'receipt_date'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    disclosure_event_id: Mapped[int | None] = mapped_column(ForeignKey('dart_disclosure_events.id', ondelete='SET NULL'), index=True)
    rcept_no: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    corp_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    stock_code: Mapped[str | None] = mapped_column(String(16), index=True)
    corp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    receipt_date: Mapped[date | None] = mapped_column(Date, index=True)
    report_nm: Mapped[str | None] = mapped_column(String(255))
    opinion_code: Mapped[str | None] = mapped_column(String(32), index=True)
    opinion_label: Mapped[str | None] = mapped_column(String(64), index=True)
    has_audit_issue: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    source_section: Mapped[str | None] = mapped_column(String(255))
    evidence_text: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict | None] = mapped_column(JSON)


class DartDilutionSignal(Base, TimestampMixin):
    __tablename__ = 'dart_dilution_signals'
    __table_args__ = (
        UniqueConstraint('rcept_no', name='uq_dart_dilution_signals_rcept_no'),
        Index('ix_dart_dilution_signals_stock_receipt', 'stock_code', 'receipt_date'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    disclosure_event_id: Mapped[int | None] = mapped_column(ForeignKey('dart_disclosure_events.id', ondelete='SET NULL'), index=True)
    rcept_no: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    corp_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    stock_code: Mapped[str | None] = mapped_column(String(16), index=True)
    corp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    receipt_date: Mapped[date | None] = mapped_column(Date, index=True)
    report_nm: Mapped[str | None] = mapped_column(String(255))
    signal_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    has_dilution_signal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    security_type: Mapped[str | None] = mapped_column(String(64))
    reason: Mapped[str | None] = mapped_column(String(255))
    payload: Mapped[dict | None] = mapped_column(JSON)
