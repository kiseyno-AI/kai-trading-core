import pytest
from pydantic import ValidationError

from app.core.settings import Settings


def test_market_open_drive_settings_use_bounded_five_minute_refresh() -> None:
    configured = Settings(_env_file=None)

    assert configured.market_snapshot_open_drive_refresh_interval_seconds == 300
    assert configured.market_snapshot_open_drive_max_age_seconds == 360


@pytest.mark.parametrize(
    ('field_name', 'value'),
    (
        ('market_snapshot_open_drive_refresh_interval_seconds', 59),
        ('market_snapshot_open_drive_refresh_interval_seconds', 601),
        ('market_snapshot_open_drive_max_age_seconds', 59),
        ('market_snapshot_open_drive_max_age_seconds', 901),
    ),
)
def test_market_open_drive_settings_reject_unsafe_cadence_or_staleness(
    field_name: str,
    value: int,
) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field_name: value})


def test_market_open_drive_refresh_cadence_cannot_exceed_evidence_max_age() -> None:
    with pytest.raises(ValidationError, match='cannot exceed'):
        Settings(
            _env_file=None,
            market_snapshot_open_drive_refresh_interval_seconds=420,
            market_snapshot_open_drive_max_age_seconds=360,
        )
