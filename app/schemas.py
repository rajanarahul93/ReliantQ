from pydantic import BaseModel, ConfigDict
from typing import Optional, Any, Dict
from uuid import UUID
from datetime import datetime
from app.models import JobStatus

class JobCreate(BaseModel):
    idempotency_key: str
    task_type: str
    payload: Dict[str, Any]
    priority: int = 1  # 0=Low, 1=Default, 2=High

class JobResponse(BaseModel):
    id: UUID
    idempotency_key: str
    task_type: str
    status: JobStatus
    retry_count: int
    created_at: datetime
    updated_at: datetime
    result: Optional[Any] = None

    model_config = ConfigDict(from_attributes=True)

class HealthResponse(BaseModel):
    status: str

class MetricsResponse(BaseModel):
    jobs_submitted_total: int
    jobs_completed_total: int
    jobs_failed_total: int
    queue_length: int
