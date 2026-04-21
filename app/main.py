from fastapi import FastAPI, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from sqlalchemy import select, insert, update
from app import models, schemas, database
from app.celery_app import celery_app
from app.config import settings
import logging
import redis
import uuid
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(request_id)s] - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Custom Logger filter to inject request_id
class RequestIDFilter(logging.Filter):
    def filter(self, record):
        record.request_id = getattr(record, 'request_id', 'SYSTEM')
        return True

logger.addFilter(RequestIDFilter())

app = FastAPI(title="Distributed Job Processing System")

@app.middleware("http")
async def add_request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    # In a real system, we'd use contextvars to pass this to the logger automatically
    # For simplicity, we'll just log it here
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = str(process_time)
    return response

redis_client = redis.from_url(settings.REDIS_URL)

@app.on_event("startup")
def on_startup():
    database.init_db()
    logger.info("Database initialized.", extra={"request_id": "STARTUP"})

@app.get("/health", response_model=schemas.HealthResponse)
def health_check():
    return {"status": "healthy"}

def get_queue_length():
    try:
        # Sum of all priority queues
        total = 0
        for q in ["high_priority", "default", "low_priority"]:
            total += redis_client.llen(q)
        return total
    except Exception:
        return 0

@app.get("/metrics", response_model=schemas.MetricsResponse)
def get_metrics(db: Session = Depends(database.get_db)):
    submitted = db.query(models.Job).count()
    completed = db.query(models.Job).filter(models.Job.status == models.JobStatus.SUCCESS).count()
    failed = db.query(models.Job).filter(models.Job.status == models.JobStatus.FAILURE).count()
    
    return {
        "jobs_submitted_total": submitted,
        "jobs_completed_total": completed,
        "jobs_failed_total": failed,
        "queue_length": get_queue_length()
    }

@app.post("/v1/jobs", status_code=status.HTTP_201_CREATED, response_model=schemas.JobResponse)
def submit_job(job_in: schemas.JobCreate, db: Session = Depends(database.get_db)):
    # 1. Backpressure Check
    if get_queue_length() >= settings.MAX_QUEUE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Queue is full. Please try again later."
        )

    # 2. Idempotency Check (Strict Fresh Read)
    # Use one_or_none to search for existing job in the current session
    existing_job = db.query(models.Job).filter(models.Job.idempotency_key == job_in.idempotency_key).one_or_none()
    
    if existing_job:
        logger.info(f"Duplicate job submission for key: {job_in.idempotency_key}. Enforcement of fresh-session read.")
        job_id = existing_job.id
        # Close current session even if it would be handled by middleware, to ensures NO identity map issues
        db.close()
        
        # Open a completely new session specifically for the response fetch
        with database.SessionLocal() as fresh_db:
            fresh_job = fresh_db.query(models.Job).filter(models.Job.id == job_id).one()
            return fresh_job

    # 3. Create Job Record
    new_job = models.Job(
        idempotency_key=job_in.idempotency_key,
        task_type=job_in.task_type,
        payload=job_in.payload,
        priority=job_in.priority,
        status=models.JobStatus.PENDING
    )
    db.add(new_job)
    try:
        db.commit()
        db.refresh(new_job)
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create job: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER__ERROR, detail="DB Error")

    # 4. Enqueue Job
    queue_name = "default"
    if new_job.priority >= 2:
        queue_name = "high_priority"
    elif new_job.priority <= 0:
        queue_name = "low_priority"

    celery_app.send_task(
        "process_job",
        args=[str(new_job.id)],
        queue=queue_name
    )
    
    return new_job

@app.get("/v1/jobs/{job_id}", response_model=schemas.JobResponse)
def get_job_status(job_id: str, db: Session = Depends(database.get_db)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job

@app.post("/v1/jobs/{job_id}/replay", response_model=schemas.JobResponse)
def replay_job(job_id: str, db: Session = Depends(database.get_db)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    
    if job.status != models.JobStatus.FAILURE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Only failed jobs can be replayed. Current status: {job.status}"
        )

    # Reset job state
    job.status = models.JobStatus.PENDING
    job.retry_count = 0
    job.updated_at = datetime.utcnow()
    
    # Delete DLQ entry if exists
    db.query(models.DeadLetterJob).filter(models.DeadLetterJob.job_id == job.id).delete()
    
    try:
        db.commit()
        db.refresh(job)
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to replay job: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="DB Error")

    # Re-enqueue
    queue_name = "default"
    if job.priority >= 2:
        queue_name = "high_priority"
    elif job.priority <= 0:
        queue_name = "low_priority"

    celery_app.send_task(
        "process_job",
        args=[str(job.id)],
        queue=queue_name
    )
    
    logger.info(f"Job {job.id} replayed and enqueued to {queue_name}")
    return job
