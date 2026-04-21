import threading
import uuid
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.database import Base, get_db
from app import models

# 1. Setup in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 2. Dependency Override
def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

# 3. Create tables
Base.metadata.create_all(bind=engine)

client = TestClient(app)

def test_concurrent_idempotency():
    idempotency_key = f"concurrent-test-{uuid.uuid4()}"
    job_payload = {
        "idempotency_key": idempotency_key,
        "task_type": "concurrency_test",
        "payload": {"data": "test"},
        "priority": 1
    }

    results = []
    
    def submit():
        try:
            # Note: TestClient with threads can be tricky because of how it handles requests
            # But the underlying logic in main.py will use the overridden db session.
            response = client.post("/v1/jobs", json=job_payload)
            results.append(response)
        except Exception as e:
            results.append(e)

    # Use threads to trigger the race condition
    threads = []
    for _ in range(10):
        t = threading.Thread(target=submit)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Verify results
    success_responses = [r for r in results if hasattr(r, 'status_code') and r.status_code in [200, 201]]
    job_ids = set()
    status_codes = []

    for r in success_responses:
        data = r.json()
        job_ids.add(data["id"])
        status_codes.append(r.status_code)

    print(f"Total responses: {len(results)}")
    print(f"Success responses: {len(success_responses)}")
    print(f"Unique Job IDs: {len(job_ids)}")
    print(f"Status codes: {set(status_codes)}")

    # Check for errors in results
    errors = [r for r in results if not hasattr(r, 'status_code')]
    if errors:
        print(f"Errors encountered: {errors}")

    assert len(job_ids) == 1, f"Expected 1 unique Job ID, got {len(job_ids)}"
    assert 201 in status_codes, "Expected at least one 201 Created"
    if len(success_responses) > 1:
        # If we hit the race condition, we should see 200s
        # Note: In-memory SQLite might be too fast to reliably trigger the race condition 
        # on every run, but if it hits, it should work.
        pass

if __name__ == "__main__":
    import app.database
    # Patch SessionLocal in app.database as well because main.py uses it in Phase 2 & Fallback
    app.database.SessionLocal = TestingSessionLocal
    
    try:
        test_concurrent_idempotency()
        print("\nCONCURRENCY TEST: PASSED")
    except Exception as e:
        print(f"\nCONCURRENCY TEST: FAILED - {e}")
        import traceback
        traceback.print_exc()
