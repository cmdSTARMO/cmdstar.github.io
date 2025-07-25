from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status":"ok"}

def test_data_endpoint():
    r = client.get("/data?startdate=2025-01-01&enddate=2025-07-25&limit=1")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
