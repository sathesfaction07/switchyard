from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..resilience.errors import SchemaError


class CatalogRecord(BaseModel):
    """
    Expected shape of a catalog listing response.

    `extra="ignore"` is deliberate. Upstream adding a field is not a reason to fail a
    batch, and a strict model turns every harmless additive change into an outage on our
    side. Fields disappearing or changing type is what we actually want to hear about.
    """

    model_config = ConfigDict(extra="ignore")

    product_id: str = Field(min_length=1)
    status: Literal["active", "suppressed", "pending", "removed"]
    region: str = Field(min_length=1)
    updated_at: str = ""


class StatusRecord(BaseModel):
    """Minimal shape for a bare status probe, where only the verdict matters."""

    model_config = ConfigDict(extra="ignore")

    product_id: str = Field(min_length=1)
    status: str = Field(min_length=1)


SCHEMAS: dict[str, type[BaseModel]] = {
    "catalog": CatalogRecord,
    "status": StatusRecord,
}


def get_schema(name: str) -> type[BaseModel] | None:
    """
    Resolve a schema by name. `none` disables validation and returns None.

    Being able to turn validation off matters for the first run against an unfamiliar
    endpoint: you want to see what actually comes back before committing to a contract.
    """
    key = (name or "").strip().lower()
    if key in ("", "none", "off"):
        return None
    if key not in SCHEMAS:
        known = ", ".join(sorted(SCHEMAS))
        raise ValueError(f"unknown schema '{name}'; known schemas: {known}, or 'none'")
    return SCHEMAS[key]


def validate_payload(payload: dict, model: type[BaseModel] | None) -> dict:
    """
    Validate a response body against a declared model.

    Raises `SchemaError` carrying the field-level detail Pydantic produced, because
    "3 items invalid" is not actionable and

        status: Input should be 'active', 'suppressed', 'pending' or 'removed'

    tells you immediately that upstream started returning uppercase. Losing that detail
    to a generic message is the single most common way response validation ends up being
    switched off by whoever is on call.

    Passing `None` returns the payload untouched.
    """
    if model is None:
        return payload

    try:
        validated = model.model_validate(payload)
    except ValidationError as exc:
        raise SchemaError(f"{model.__name__}: {_summarise(exc)}") from exc

    return validated.model_dump()


def _summarise(exc: ValidationError, limit: int = 3) -> str:
    """
    Condense a ValidationError into one line, keeping the field names.

    Truncated at `limit` because a wholly unexpected payload can produce a dozen errors
    and the first few already tell you what happened.
    """
    parts: list[str] = []
    for error in exc.errors()[:limit]:
        location = ".".join(str(piece) for piece in error["loc"]) or "<root>"
        parts.append(f"{location}: {error['msg']}")
    remaining = len(exc.errors()) - limit
    if remaining > 0:
        parts.append(f"and {remaining} more")
    return "; ".join(parts)
