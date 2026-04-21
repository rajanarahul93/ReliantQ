import sys
import os
import uuid
import pytest
from unittest.mock import MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.models import Base, Job, JobStatus, DeadLetterJob
from app.tasks.process_job import process_job

# 1. SETUP MOCKED ENVIRONMENT
# Use SQLite for atomic verification of session logic
SQLALCHEMY_DATABASE_URL = "sqlite:///./temp_verify.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Mock Celery Application
mock_self = MagicMock()
mock_self.max_retries = 3
mock_self.retry.side_effect = Exception("Celery Retry Triggered")

# Patch the project to use our test session
import app.tasks.process_job
import app.tasks.reaper
app.tasks.process_job.SessionLocal = TestingSessionLocal
app.tasks.reaper.SessionLocal = TestingSessionLocal

def setup_function():
    Base.metadata.create_all(bind=engine)

def teardown_function():
    Base.metadata.drop_all(bind=engine)
    if os.path.exists("./temp_verify.db"):
        os.remove("./temp_verify.db")

def test_success_flow():
    """Validates: PENDING -> PROCESSING -> SUCCESS"""
    setup_function()
    db = TestingSessionLocal()
    
    # Create a job
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        idempotency_key="test-success",
        task_type="test",
        payload={"simulate_sleep": 0},
        status=JobStatus.PENDING
    )
    db.add(job)
    db.commit()
    db.close()
    
    # Run the task
    process_job(mock_self, str(job_id))
    
    # Verify final state
    db = TestingSessionLocal()
    updated_job = db.query(Job).filter(Job.id == job_id).one()
    assert updated_job.status == JobStatus.SUCCESS
    assert updated_job.result is not None
    db.close()
    teardown_function()

def test_failure_and_retry_flow():
    """Validates: PENDING -> PROCESSING -> RETRYING without session conflicts"""
    setup_function()
    db = TestingSessionLocal()
    
    # Create a job that will fail
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        idempotency_key="test-failure",
        task_type="test",
        payload={"simulate_failure": True, "simulate_sleep": 0},
        status=JobStatus.PENDING
    )
    db.add(job)
    db.commit()
    db.close()
    
    # Run the task (it should raise Exception from mock_self.retry)
    with pytest.raises(Exception, match="Celery Retry Triggered"):
        process_job(mock_self, str(job_id))
    
    # Verify retry state
    db = TestingSessionLocal()
    updated_job = db.query(Job).filter(Job.id == job_id).one()
    assert updated_job.status == JobStatus.RETRYING
    assert updated_job.retry_count == 1
    db.close()
    teardown_function()

def test_dlq_transition():
    """Validates: Moves to DLQ after exhaustion"""
    setup_function()
    db = TestingSessionLocal()
    
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        idempotency_key="test-dlq",
        task_type="test",
        payload={"simulate_failure": True, "simulate_sleep": 0},
        status=JobStatus.RETRYING,
        retry_count=3 # Max is 3
    )
    db.add(job)
    db.commit()
    db.close()
    
    # Run the task (it should raise Exception from mock_self.retry)
    with pytest.raises(Exception, match="Celery Retry Triggered"):
        process_job(mock_self, str(job_id))
    
    # Verify DLQ state
    db = TestingSessionLocal()
    updated_job = db.query(Job).filter(Job.id == job_id).one()
    assert updated_job.status == JobStatus.FAILURE
    assert updated_job.retry_count == 4
    
    dlq_entry = db.query(DeadLetterJob).filter(DeadLetterJob.job_id == job_id).first()
    assert dlq_entry is not None
    assert "Simulated task failure" in dlq_entry.last_error
    db.close()
    teardown_function()

if __name__ == "__main__":
    # Simple manual run if pytest isn't used
    setup_function()
    try:
        test_success_flow()
        print("SUCCESS Flow: PASSED")
        test_failure_and_retry_flow()
        print("RETRY Flow: PASSED")
        test_dlq_transition()
        print("DLQ Flow: PASSED")
    finally:
        teardown_function()
