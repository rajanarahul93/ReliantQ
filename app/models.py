import enum
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, JSON, Enum, ForeignKey, Index, UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRYING = "RETRYING"

class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key = Column(String, unique=True, nullable=False, index=True)
    task_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    result = Column(JSON, nullable=True)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    priority = Column(Integer, default=1)  # 0=Low, 1=Default, 2=High
    retry_count = Column(Integer, default=0)
    locked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    dead_letter = relationship("DeadLetterJob", back_populates="job", cascade="all, delete-orphan")

class DeadLetterJob(Base):
    __tablename__ = "dead_letter_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    last_error = Column(String, nullable=True)
    failed_at = Column(DateTime, default=datetime.utcnow)

    job = relationship("Job", back_populates="dead_letter")
