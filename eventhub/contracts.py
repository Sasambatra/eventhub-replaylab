import hashlib
import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Snapshot(StrictModel):
    booking_id: UUID
    revision: StrictInt = Field(ge=1)
    status: Literal["held", "confirmed", "cancelled", "expired"]


class Envelope(StrictModel):
    specversion: Literal["1.0"]
    id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    source: str = Field(min_length=1, max_length=200, pattern=r"^urn:[A-Za-z0-9:._-]+$")
    type: Literal["studio.booking.snapshot.v1"]
    subject: str = Field(max_length=200)
    time: datetime
    datacontenttype: Literal["application/json"] = "application/json"
    correlationid: str | None = Field(default=None, max_length=128)
    causationid: str | None = Field(default=None, max_length=128)
    data: Snapshot

    @field_validator("time")
    @classmethod
    def aware_time(cls, value):
        if value.tzinfo is None:
            raise ValueError("Time zone is required")
        return value

    @model_validator(mode="after")
    def matching_subject(self):
        if self.subject != f"booking/{self.data.booking_id}":
            raise ValueError("Subject must identify the snapshot booking")
        return self

    def payload(self):
        return self.model_dump(mode="json", exclude_none=True)


class ReplayStep(StrictModel):
    event_id: str = Field(min_length=1, max_length=128)
    fault: Literal["none", "delay", "return_500", "return_429", "commit_then_delay_response"] = "none"
    fault_attempts: int = Field(default=1, ge=0, le=3)


class Expected(StrictModel):
    status: Literal["held", "confirmed", "cancelled", "expired"]
    revision: StrictInt = Field(ge=1)


class CaseDefinition(StrictModel):
    case_version: Literal[1]
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=1000)
    seed: StrictInt = Field(default=42, ge=0, le=2**31)
    start_time: datetime
    events: list[Envelope] = Field(min_length=1, max_length=500)
    steps: list[ReplayStep] = Field(min_length=1, max_length=2000)
    expected: Expected
    expected_variants: dict[Literal["A", "B"], Literal["pass", "fail"]]

    @model_validator(mode="after")
    def consistency(self):
        if self.start_time.tzinfo is None:
            raise ValueError("Virtual clock requires a time zone")
        ids = [e.id for e in self.events]
        if len(ids) != len(set(ids)):
            raise ValueError("Event definitions must have unique IDs; repeat them through steps")
        if len({(e.source, e.subject) for e in self.events}) != 1:
            raise ValueError("The first case profile supports one aggregate")
        if any(step.event_id not in ids for step in self.steps):
            raise ValueError("Step references an undefined event")
        if set(self.expected_variants) != {"A", "B"}:
            raise ValueError("Both variant expectations are required")
        return self


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def checksum(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def strict_json(body):
    def pairs(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise ValueError("Duplicate JSON keys are forbidden")
            obj[key] = value
        return obj

    def invalid(_):
        raise ValueError("Non-finite JSON numbers are forbidden")

    value = json.loads(body, object_pairs_hook=pairs, parse_constant=invalid)

    def depth(obj, level=0):
        if level > 16:
            raise ValueError("JSON nesting exceeds the limit")
        if isinstance(obj, dict):
            for child in obj.values():
                depth(child, level + 1)
        elif isinstance(obj, list):
            for child in obj:
                depth(child, level + 1)

    depth(value)
    return value
