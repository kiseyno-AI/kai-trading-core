from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from app.eod import price_bar_repair_state as state

PREFIX = (
    'Core startup blocked because repaired daily price bars still require '
    'derived-data rebuild verification: '
)
KST = timezone(timedelta(hours=9))


def _batch(batch_id: str, created_at: str | None) -> dict:
    return {'repair_batch_id': batch_id, 'created_at': created_at}


class PendingRebuildBlockMessageTests(unittest.TestCase):
    def test_existing_prefix_and_batch_list_are_preserved(self) -> None:
        message = state.format_pending_rebuild_block_message(
            [_batch('missing-bar-a', None), _batch('missing-bar-b', None)]
        )

        self.assertTrue(message.startswith(PREFIX + "['missing-bar-a', 'missing-bar-b']"))

    def test_message_reports_count_oldest_batch_and_age(self) -> None:
        pending = [
            _batch('newer', '2026-11-01T10:13:09.128307+09:00'),
            _batch('oldest', '2026-11-01T09:50:44.670401+09:00'),
        ]
        message = state.format_pending_rebuild_block_message(
            pending, now=datetime(2026, 11, 4, 9, 50, 44, tzinfo=KST)
        )

        self.assertIn('2 pending batch(es)', message)
        self.assertIn('2026-11-01T09:50:44.670401+09:00', message)
        self.assertIn('(72.0h ago)', message)

    def test_message_includes_the_closeout_steps_in_order(self) -> None:
        message = state.format_pending_rebuild_block_message([_batch('a', None)])

        rebuild = message.index('--rebuild-evaluations')
        verify = message.index('(verify only)')
        complete = message.index('--complete')
        self.assertLess(rebuild, verify)
        self.assertLess(verify, complete)
        self.assertIn('Core stays down while any batch is pending', message)

    def test_unparseable_or_missing_timestamps_do_not_break_the_message(self) -> None:
        message = state.format_pending_rebuild_block_message(
            [_batch('a', 'not-a-date'), _batch('b', None)]
        )

        self.assertNotIn('ago)', message)
        self.assertIn('--complete', message)

    def test_naive_timestamps_are_treated_as_utc(self) -> None:
        message = state.format_pending_rebuild_block_message(
            [_batch('a', '2026-11-01T00:00:00')],
            now=datetime(2026, 11, 1, 5, 0, tzinfo=timezone.utc),
        )

        self.assertIn('(5.0h ago)', message)

    def test_assert_raises_with_the_formatted_message_only_when_batches_are_pending(self) -> None:
        with patch.object(state, 'load_pending_price_bar_rebuild_batches', return_value=[]):
            self.assertIsNone(state.assert_no_pending_price_bar_rebuilds())

        pending = [_batch('missing-bar-x', '2026-11-01T09:50:44+09:00')]
        with patch.object(state, 'load_pending_price_bar_rebuild_batches', return_value=pending):
            with self.assertRaises(state.PendingPriceBarRebuildError) as raised:
                state.assert_no_pending_price_bar_rebuilds()

        self.assertTrue(str(raised.exception).startswith(PREFIX + "['missing-bar-x']"))
        self.assertIn('--complete', str(raised.exception))


if __name__ == '__main__':
    unittest.main()
