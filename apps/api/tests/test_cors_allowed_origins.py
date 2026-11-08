from __future__ import annotations

import unittest

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from app.core.settings import Settings
from app.main import _cors_allowed_origins


class CorsAllowedOriginsTests(unittest.TestCase):
    def test_default_keeps_the_historical_wildcard(self) -> None:
        self.assertEqual(Settings.model_fields['cors_allowed_origins'].default, '*')
        self.assertEqual(_cors_allowed_origins('*'), ['*'])
        self.assertEqual(_cors_allowed_origins(''), ['*'])
        self.assertEqual(_cors_allowed_origins(None), ['*'])

    def test_explicit_origins_are_split_and_trimmed(self) -> None:
        self.assertEqual(
            _cors_allowed_origins(' https://ui.example , http://127.0.0.1:8383 ,, '),
            ['https://ui.example', 'http://127.0.0.1:8383'],
        )

    def test_restricted_origins_only_answer_preflight_for_listed_origins(self) -> None:
        app = FastAPI()
        app.add_middleware(
            CORSMiddleware,
            allow_origins=_cors_allowed_origins('https://ui.example'),
            allow_methods=['*'],
            allow_headers=['*'],
        )

        @app.post('/x')
        def handler() -> dict:
            return {}

        client = TestClient(app)
        preflight = {'Access-Control-Request-Method': 'POST'}
        allowed = client.options('/x', headers={'Origin': 'https://ui.example', **preflight})
        blocked = client.options('/x', headers={'Origin': 'https://evil.example', **preflight})

        self.assertEqual(allowed.headers.get('access-control-allow-origin'), 'https://ui.example')
        self.assertNotIn('access-control-allow-origin', blocked.headers)


if __name__ == '__main__':
    unittest.main()
