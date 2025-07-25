from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status":"ok"}

def test_data_endpoint():
    r = client.get(
        "/szse_etf_shares/data?startdate=2025-01-01&enddate=2025-07-25&limit=1"
    )
    assert r.status_code == 200
    payload = r.json()
    assert "data" in payload
    assert isinstance(payload["data"], list)
