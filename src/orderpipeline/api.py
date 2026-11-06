"""Orders API: idempotent order creation with a transactional outbox."""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from collections.abc import Iterator

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from orderpipeline import metrics, saga
from orderpipeline.config import Settings
from orderpipeline.db import connect, init_db, transaction
from orderpipeline.events import add_to_outbox, new_event
from orderpipeline.logs import correlation_id, log
from orderpipeline.queue.sqlite import SQLiteQueue

logger = logging.getLogger("orderpipeline.api")


class OrderIn(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)
    sku: str
    qty: int = Field(ge=1, le=10)


def _order_json(row) -> dict:
    return {
        "order_id": row["id"],
        "customer_id": row["customer_id"],
        "sku": row["sku"],
        "qty": row["qty"],
        "amount_cents": row["amount_cents"],
        "status": row["status"],
        "correlation_id": row["correlation_id"],
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    init_db(settings.db_path)
    app = FastAPI(title="orderpipeline orders API", version="1.0.0")

    def get_conn() -> Iterator:
        conn = connect(settings.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @app.middleware("http")
    async def correlation(request: Request, call_next):
        cid = request.headers.get("x-correlation-id") or uuid.uuid4().hex
        token = correlation_id.set(cid)
        try:
            response = await call_next(request)
        finally:
            correlation_id.reset(token)
        response.headers["x-correlation-id"] = cid
        return response

    @app.post("/orders", status_code=201)
    def create_order(
        body: OrderIn,
        response: Response,
        idempotency_key: str | None = Header(default=None),
        conn=Depends(get_conn),
    ):
        if not idempotency_key:
            raise HTTPException(400, "Idempotency-Key header is required")
        request_hash = hashlib.sha256(
            json.dumps(body.model_dump(), sort_keys=True).encode()
        ).hexdigest()
        cid = correlation_id.get()
        with transaction(conn) as c:
            existing = c.execute(
                "SELECT * FROM orders WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise HTTPException(422, "Idempotency-Key reused with a different body")
                response.status_code = 200
                response.headers["Idempotent-Replayed"] = "true"
                log(logger, "idempotent replay", order_id=existing["id"])
                return _order_json(existing)
            item = c.execute(
                "SELECT price_cents FROM inventory WHERE sku = ?", (body.sku,)
            ).fetchone()
            if item is None:
                raise HTTPException(422, f"unknown sku {body.sku}")
            order_id = "ord_" + uuid.uuid4().hex[:12]
            now = time.time()
            amount = item["price_cents"] * body.qty
            # One transaction: the order row, its first saga entry, and the
            # OrderPlaced event. Either all three exist or none do.
            c.execute(
                "INSERT INTO orders(id, customer_id, sku, qty, amount_cents, status,"
                " correlation_id, idempotency_key, request_hash, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (order_id, body.customer_id, body.sku, body.qty, amount, saga.PENDING, cid,
                 idempotency_key, request_hash, now, now),
            )
            c.execute(
                "INSERT INTO saga_log(order_id, from_state, to_state, reason, at)"
                " VALUES (?, NULL, ?, 'order placed', ?)",
                (order_id, saga.PENDING, now),
            )
            add_to_outbox(c, new_event("OrderPlaced", order_id, cid, {
                "sku": body.sku, "qty": body.qty, "amount_cents": amount,
            }))
            row = c.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        log(logger, "order placed", order_id=order_id, sku=body.sku, qty=body.qty,
            amount_cents=amount)
        return _order_json(row)

    @app.get("/orders/{order_id}")
    def get_order(order_id: str, conn=Depends(get_conn)):
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "order not found")
        out = _order_json(row)
        pay = conn.execute("SELECT * FROM payments WHERE order_id = ?", (order_id,)).fetchone()
        out["payment"] = dict(pay) if pay else None
        out["saga"] = saga.history(conn, order_id)
        return out

    @app.get("/inventory")
    def inventory(conn=Depends(get_conn)):
        rows = conn.execute("SELECT sku, price_cents, stock FROM inventory ORDER BY sku")
        return [dict(r) for r in rows]

    @app.get("/healthz")
    def healthz(conn=Depends(get_conn)):
        try:
            conn.execute("SELECT 1 FROM orders LIMIT 1").fetchall()
        except Exception as e:  # noqa: BLE001
            return JSONResponse({"status": "error", "error": str(e)}, status_code=503)
        return {"status": "ok"}

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus(conn=Depends(get_conn)):
        q = SQLiteQueue(settings.db_path)
        try:
            return PlainTextResponse(metrics.render(conn, q),
                                     media_type="text/plain; version=0.0.4")
        finally:
            q.close()

    return app
