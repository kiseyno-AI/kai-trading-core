import logging
from unittest import TestCase

from app.core.app_logging import _HANDLER_MARKER, configure_application_logging


class ApplicationLoggingTests(TestCase):
    def setUp(self) -> None:
        self.app_logger = logging.getLogger('app')
        self.saved_level = self.app_logger.level
        self.saved_handlers = list(self.app_logger.handlers)
        for handler in self.saved_handlers:
            self.app_logger.removeHandler(handler)

    def tearDown(self) -> None:
        for handler in list(self.app_logger.handlers):
            self.app_logger.removeHandler(handler)
        for handler in self.saved_handlers:
            self.app_logger.addHandler(handler)
        self.app_logger.setLevel(self.saved_level)

    def test_info_records_from_app_modules_are_emitted(self) -> None:
        configure_application_logging('INFO')
        with self.assertLogs('app.eod.some_module', level='INFO') as captured:
            logging.getLogger('app.eod.some_module').info('stage started')
        self.assertEqual(len(captured.records), 1)
        self.assertTrue(logging.getLogger('app.eod.some_module').isEnabledFor(logging.INFO))

    def test_configuration_is_idempotent(self) -> None:
        configure_application_logging('INFO')
        configure_application_logging('INFO')
        marked = [h for h in self.app_logger.handlers if getattr(h, _HANDLER_MARKER, False)]
        self.assertEqual(len(marked), 1)

    def test_level_can_be_raised_and_invalid_values_fall_back_to_info(self) -> None:
        configure_application_logging('WARNING')
        self.assertFalse(logging.getLogger('app.x').isEnabledFor(logging.INFO))
        configure_application_logging('not-a-level')
        self.assertTrue(logging.getLogger('app.x').isEnabledFor(logging.INFO))

    def test_third_party_loggers_are_not_touched(self) -> None:
        before = logging.getLogger('sqlalchemy.engine').level
        configure_application_logging('DEBUG')
        self.assertEqual(logging.getLogger('sqlalchemy.engine').level, before)
