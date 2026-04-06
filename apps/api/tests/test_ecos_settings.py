import os
import unittest
from unittest.mock import patch

from app.core.settings import Settings


class ECOSSettingsTests(unittest.TestCase):
    def test_settings_reads_canonical_bok_ecos_env_names(self):
        with patch.dict(
            os.environ,
            {
                'BOK_ECOS_API_KEY': 'canonical-key',
                'BOK_ECOS_BASE_URL': 'https://ecos.example.test/api',
            },
            clear=False,
        ):
            settings = Settings(_env_file=None)

        self.assertEqual(settings.bok_ecos_api_key, 'canonical-key')
        self.assertEqual(settings.bok_ecos_base_url, 'https://ecos.example.test/api')

    def test_settings_accepts_legacy_bok_env_names_as_fallback(self):
        with patch.dict(
            os.environ,
            {
                'BOK_API_KEY': 'legacy-key',
                'BOK_BASE_URL': 'https://legacy.example.test/api',
            },
            clear=True,
        ):
            settings = Settings(_env_file=None)

        self.assertEqual(settings.bok_ecos_api_key, 'legacy-key')
        self.assertEqual(settings.bok_ecos_base_url, 'https://legacy.example.test/api')


if __name__ == '__main__':
    unittest.main()
