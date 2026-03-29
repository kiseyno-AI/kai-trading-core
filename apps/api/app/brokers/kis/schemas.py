from pydantic import BaseModel


class KISTokenResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int


class KISApprovalKeyResponse(BaseModel):
    approval_key: str | None = None
