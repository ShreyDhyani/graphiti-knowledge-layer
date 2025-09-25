"""
model.py

Pydantic models for the core knowledge schema for Circular PDFs
(Designed to be lightweight and tolerant for a POC.)

Usage:
    from model import Circular, Policy, Organization
    c = Circular(title="Leave Policy", circular_number="HR-2025-07")
    node_payload = c.to_dict()  # returns mapping safe for Graphiti ingestion

The models are intentionally permissive (most fields optional) so the extractor
can populate partial data without failing validation.
"""
from __future__ import annotations

import uuid
from typing import Optional, List, Dict, Any
from datetime import date, datetime

from pydantic import BaseModel, Field


def _new_id() -> str:
    return str(uuid.uuid4())

class BaseEntity(BaseModel):
    id: str = Field(default_factory=_new_id)
    # free-form metadata for provenance (source file, page, chunk id, etc.)
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain mapping compatible with pydantic v1/v2."""
        # pydantic v2 provides model_dump(); fall back to dict()
        if hasattr(self, "model_dump"):
            return self.model_dump()
        return self.dict()


class Identifier(BaseModel):
    """Structured identifier used to link/correlate documents."""
    id: Optional[str] = None
    value: Optional[str] = None
    type: Optional[str] = None  # e.g., "circular_no", "memo_no"


class Organization(BaseEntity):
    name: Optional[str] = None
    type: Optional[str] = None  # e.g., 'ministry', 'department', 'company'
    parent_org_id: Optional[str] = None
    contact: Optional[Dict[str, str]] = None  # {'email':..., 'phone':...}


class Department(BaseEntity):
    name: Optional[str] = None
    org_id: Optional[str] = None
    location: Optional[str] = None
    contact: Optional[Dict[str, str]] = None


class Person(BaseEntity):
    name: Optional[str] = None
    role: Optional[str] = None
    affiliation_org_id: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class TemporalMarker(BaseModel):
    """Normalized date/time anchor used across entities."""
    id: Optional[str] = None
    date: Optional[date] = None
    datetime: Optional[datetime] = None
    label: Optional[str] = None  # e.g., 'issued_date', 'effective_from'
    raw: Optional[str] = None


class Clause(BaseEntity):
    circular_id: Optional[str] = None
    clause_number: Optional[str] = None
    text: Optional[str] = None
    page_ref: Optional[int] = None


class Policy(BaseEntity):
    name: Optional[str] = None
    circular_id: Optional[str] = None
    text: Optional[str] = None
    effective_from: Optional[date] = None
    effective_to: Optional[date] = None
    applies_to: Optional[List[str]] = None  # list of Dept/Org ids or tags
    tags: Optional[List[str]] = None


class Circular(BaseEntity):
    circular_number: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    full_text: Optional[str] = None
    issued_date: Optional[date] = None
    issued_by: Optional[str] = None  # org id
    version: Optional[str] = None
    status: Optional[str] = None  # 'active', 'superseded', etc.
    source_file: Optional[str] = None
    pages: Optional[int] = None
    identifiers: Optional[List[Identifier]] = None
    valid_from: Optional[date] = None
    valid_to: Optional[date] = None
    tags: Optional[List[str]] = None


# convenience export list
__all__ = [
    "BaseEntity",
    "Identifier",
    "Organization",
    "Department",
    "Person",
    "TemporalMarker",
    "Clause",
    "Policy",
    "Circular",
]
