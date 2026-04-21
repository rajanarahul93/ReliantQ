import asyncio
import httpx
import uuid
import time
import statistics

API_URL = "http://localhost:8000/v1/jobs"

async def submit_job(client, i):
    idempotency_key = f"load-test-{uuid.uuid4()}"
    payload = {
        "job_index": i,
        "simulate_sleep": 0.1,
        "simulate_failure": (i % 10 == 0) # 10% failure rate for testing DLQ/retries
    }
    
    start_time = time.time()
    try:
        response = await client.post(
            API_URL,
            json={
                "idempotency_key": idempotency_key,
                "task_type": "load_test_task",
                "payload": payload,
                "priority": i % 3 # Cycle through priorities
            },
            timeout=10.0
        )
        duration = time.time() - start_time
        return response.status_code, duration
    except Exception as e:
        return str(e), time.time() - start_time

async def run_load_test(num_jobs=1000, concurrency=50):
    async with httpx.AsyncClient() as client:
        tasks = []
        semaphore = asyncio.Semaphore(concurrency)
        
        async def sem_task(i):
            async with semaphore:
                return await submit_job(client, i)
        
        print(f"Starting load test: {num_jobs} jobs with concurrency {concurrency}...")
        start_time = time.time()
        
        results = await asyncio.gather(*(sem_task(i) for i in range(num_jobs)))
        
        total_duration = time.time() - start_time
        
        # Analyze results
        statuses = [r[0] for r in results]
        latencies = [r[1] for r in results]
        
        success_count = statuses.count(201)
        error_count = len(statuses) - success_count
        
        print("\n--- Load Test Results ---")
        print(f"Total Jobs: {num_jobs}")
        print(f"Total Time: {total_duration:.2f}s")
        print(f"Throughput: {num_jobs / total_duration:.2f} jobs/s")
        print(f"Success (201): {success_count}")
        print(f"Errors/Others: {error_count}")
        print(f"Avg Latency: {statistics.mean(latencies):.4f}s")
        print(f"P95 Latency: {statistics.quantiles(latencies, n=20)[18]:.4f}s")
        print("-------------------------\n")

if __name__ == "__main__":
    asyncio.run(run_load_test(1000, 50))
