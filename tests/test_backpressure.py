import asyncio
import httpx
import time

async def send_job(client, i):
    payload = {
        "idempotency_key": f"bp-test-{i}",
        "task_type": "demo",
        "payload": {},
        "priority": 2
    }
    try:
        response = await client.post("http://localhost:8000/v1/jobs", json=payload)
        return response.status_code
    except Exception as e:
        return str(e)

async def main():
    async with httpx.AsyncClient() as client:
        tasks = [send_job(client, i) for i in range(200)]
        results = await asyncio.gather(*tasks)
        
        status_counts = {}
        for res in results:
            status_counts[res] = status_counts.get(res, 0) + 1
        
        print(f"Results: {status_counts}")

if __name__ == "__main__":
    asyncio.run(main())
