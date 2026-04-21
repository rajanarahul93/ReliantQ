from app.worker import celery_app
from app.database import SessionLocal
from app.models import Job, JobStatus, DeadLetterJob
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)

@celery_app.task(
    name="process_job",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 3},
    acks_late=True
)
def process_job(self, job_id: str):
    payload = None
    task_type = None
    priority = None
    current_retry_count = 0
    db_claim = SessionLocal()
    try:
        with db_claim.begin():
            job = db_claim.query(Job).filter(
                Job.id == job_id,
                Job.status.in_([JobStatus.PENDING, JobStatus.RETRYING])
            ).with_for_update(skip_locked=True).first()

            if not job:
                logger.info(f"Job {job_id} already being processed or not found.")
                return

            # Extract data into local variables (prevents lazy-loading issues)
            payload = dict(job.payload) if job.payload else {}
            task_type = job.task_type
            priority = job.priority
            current_retry_count = job.retry_count

            # Mark as PROCESSING
            job.status = JobStatus.PROCESSING
            job.locked_at = datetime.utcnow()
            job.updated_at = datetime.utcnow()
            
            logger.info(f"[CLAIMED] Job {job_id} (Type: {task_type}, Key: {job.idempotency_key})")
        
    except Exception as e:
        logger.error(f"Error during Phase 1 (Claim) for job {job_id}: {e}")
        # If claiming fails, we don't try to continue or finalize
        return
    finally:
        db_claim.close()

    # PHASE 2: EXECUTE LOGIC
    # No database session is active during this phase
    logger.info(f"[START] Executing job {job_id} logic.")
    try:
        result = simulate_task_execution(payload)
    except Exception as exc:
        # Handle execution failures in Phase 3
        handle_failure(self, job_id, exc, current_retry_count, priority)
        return

    # PHASE 3: FINALIZE (SUCCESS)
    # Use a fresh session to record the success
    db_finalize = SessionLocal()
    try:
        with db_finalize.begin():
            job = db_finalize.query(Job).filter(Job.id == job_id).first()
            if job:
                job.status = JobStatus.SUCCESS
                job.result = result
                job.updated_at = datetime.utcnow()
                logger.info(f"[SUCCESS] Job {job_id} completed successfully.")
    except Exception as e:
        logger.error(f"Error during Phase 3 (Finalize) for job {job_id}: {e}")
    finally:
        db_finalize.close()

def handle_failure(self, job_id: str, exc: Exception, current_retry_count: int, priority: int):
    """
    Independent helper to record failures and retries using a fresh session.
    """
    logger.error(f"[FATAL/RETRY] Job {job_id} failed: {exc}")
    
    db_fail = SessionLocal()
    try:
        with db_fail.begin():
            job = db_fail.query(Job).filter(Job.id == job_id).first()
            if job:
                job.retry_count = current_retry_count + 1
                job.updated_at = datetime.utcnow()
                
                # Determine if we should move to DLQ or mark for RETRYING
                if job.retry_count > self.max_retries:
                    job.status = JobStatus.FAILURE
                    dlq = DeadLetterJob(
                        job_id=job.id,
                        last_error=str(exc)
                    )
                    db_fail.add(dlq)
                    logger.warning(f"Job {job_id} exhausted retries. Moved to DLQ.")
                else:
                    job.status = JobStatus.RETRYING
                    logger.info(f"Job {job_id} marked for retry ({job.retry_count}/{self.max_retries})")
        
        # Trigger Celery retry mechanism
        raise self.retry(exc=exc)
    
    except self.Retry:
        # Re-raise Celery's Retry exception so it reaches the worker
        raise
    except Exception as e:
        logger.error(f"Error while recording failure for job {job_id}: {e}")
    finally:
        db_fail.close()

def simulate_task_execution(payload: dict):
    """
    Placeholder for actual task logic.
    Uses plain dictionary passed from phase 1.
    """
    # Simulate latency
    sleep_time = payload.get("simulate_sleep", 1)
    time.sleep(sleep_time)
    
    # Simulate failure
    if payload.get("simulate_failure"):
        raise ValueError("Simulated task failure")
        
    return {"status": "processed", "timestamp": datetime.utcnow().isoformat()}
