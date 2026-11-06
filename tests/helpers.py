from __future__ import annotations

from fastapi.testclient import TestClient

from orderpipeline.api import create_app


def client_for(settings, **kw) -> TestClient:
    return TestClient(create_app(settings), **kw)


def place(client, key="k1", sku="SKU-MUG", qty=1, customer="c1", cid=None):
    headers = {"Idempotency-Key": key}
    if cid:
        headers["X-Correlation-ID"] = cid
    return client.post("/orders", json={"customer_id": customer, "sku": sku, "qty": qty},
                       headers=headers)
