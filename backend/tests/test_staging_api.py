from __future__ import annotations


async def test_create_is_idempotent(fastapi_client, seed_master_data):
    """API idempotency test.

    What this validates:
    - Same Idempotency-Key on create returns the exact same response payload.
    - Prevents duplicate staging rows when clients retry POST.
    """

    req = {
        "level": "allocation",
        "portfolio_id": seed_master_data["portfolio_id"],
        "instrument_id": seed_master_data["instrument_id"],
        "trade_date": "2026-01-02",
        "quantity": "10",
        "price": "100",
        "quote_currency": "USD",
        "report_currency": "USD",
    }

    headers = {"Idempotency-Key": "test-create-1"}
    r1 = await fastapi_client.post("/staging-transactions", json=req, headers=headers)
    assert r1.status_code == 200
    body1 = r1.json()

    r2 = await fastapi_client.post("/staging-transactions", json=req, headers=headers)
    assert r2.status_code == 200
    body2 = r2.json()

    assert body2 == body1
    assert body1["status"] == "entry"
    assert body1["lifecycle"] == "active"


async def test_patch_only_allows_entry(fastapi_client, seed_master_data):
    """Mutability rule test.

    What this validates:
    - Staging row can be modified while status=entry.
    - The PATCH increments entry_version.
    """

    req = {
        "level": "allocation",
        "portfolio_id": seed_master_data["portfolio_id"],
        "instrument_id": seed_master_data["instrument_id"],
        "trade_date": "2026-01-02",
        "quantity": "10",
        "price": "100",
        "quote_currency": "USD",
        "report_currency": "USD",
    }
    r1 = await fastapi_client.post("/staging-transactions", json=req)
    assert r1.status_code == 200
    staging_id = r1.json()["id"]
    v1 = r1.json()["entry_version"]

    r2 = await fastapi_client.patch(
        f"/staging-transactions/{staging_id}",
        json={"price": "101"},
        headers={"X-Actor": "test", "X-Change-Reason": "unit"},
    )
    assert r2.status_code == 200
    assert r2.json()["entry_version"] == v1 + 1
