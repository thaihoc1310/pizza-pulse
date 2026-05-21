from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query

from .config import Settings
from .kafka import KafkaPublishError
from .postgres import PostgresReadError, PostgresWriteError
from .schema import OrderValidationError
from .service import PizzaBackendService


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


@lru_cache(maxsize=1)
def get_service() -> PizzaBackendService:
    return PizzaBackendService(get_settings())


def create_app() -> FastAPI:
    api = FastAPI(
        title="Pizza Pulse Backend",
        version="0.1.0",
    )

    @api.get("/healthz")
    def healthz() -> dict[str, object]:
        return {"status": "ok", "config": get_settings().public_dict()}

    @api.get("/pizzas")
    def list_pizzas() -> dict[str, Any]:
        try:
            return get_service().list_pizzas()
        except PostgresReadError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @api.post("/orders", status_code=202)
    def publish_order(
        payload: dict[str, Any] = Body(...),
        persist_postgres: bool | None = Query(
            default=None,
            description="Override POSTGRES_WRITE_ENABLED for this request.",
        ),
    ) -> dict[str, Any]:
        try:
            result = get_service().publish_order(
                payload,
                persist_postgres=persist_postgres,
            )
        except OrderValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except (KafkaPublishError, PostgresWriteError) as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        return result.response_payload()

    return api


app = create_app()
