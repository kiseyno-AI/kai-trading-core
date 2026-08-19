from app.brokers.kis.provider import kis_real_client


def test_real_domain_client_is_not_affected_by_mock_order_setting() -> None:
    assert kis_real_client.use_mock is False
