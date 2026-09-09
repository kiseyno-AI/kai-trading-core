from typing import Literal

from pydantic import BaseModel, Field


class KISTokenResponse(BaseModel):
    access_token_present: bool
    token_type: Literal['Bearer']
    expires_in: int = Field(ge=0)
    cached: bool
    refresh_deferred: bool = False
    defer_reason: Literal['minimum_issue_interval', 'peer_refresh'] | None = None


class KISApprovalKeyResponse(BaseModel):
    approval_key: str | None = None
