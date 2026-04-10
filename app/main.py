import logging
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from typing import List
from sqlalchemy import func, select, update

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from db.database import datasets, setup_postgres, get_pg_engine, get_mongo_db
from core.processor import process_dataset
from core.logging import log_action

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application")
    setup_postgres()
    logger.info("Application ready")
    yield
    logger.info("Shutting down application")


app = FastAPI(title="AI Dataset Submission API", lifespan=lifespan)
executor = ThreadPoolExecutor(max_workers=10)


def _get_dataset_or_404(dataset_id: int):
    engine = get_pg_engine()
    with engine.connect() as conn:
        row = (
            conn.execute(
                select(datasets).where(
                    datasets.c.id == dataset_id,
                    datasets.c.is_active.is_(True),
                )
            )
            .mappings()
            .first()
        )
    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return dict(row)


@app.post("/datasets", summary="Submit one or more datasets")
async def submit_datasets(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided.")

    logger.info("Received dataset upload request with %s file(s)", len(files))

    tasks = []
    for upload in files:
        # Read the file bytes so we know the size
        content = await upload.read()
        size_mb = len(content) / (1024 * 1024)

        # Derive file_type from filename extension
        filename = upload.filename or ""
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        name = filename.rsplit(".", 1)[0] if "." in filename else filename

        # We treat the upload itself as the "URL" for storage and store it in a temp file
        import tempfile, os

        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        future = executor.submit(
            process_dataset,
            name,
            ext,
            size_mb,
            f"file://{tmp_path}",
        )
        tasks.append((name, future))

    results = []
    for name, future in tasks:
        try:
            result = future.result(timeout=120)
            results.append(result)
        except Exception as e:
            results.append({"name": name, "status": "error", "error": str(e)})

    return JSONResponse(content={"submitted": len(results), "results": results})


@app.post("/datasets/url", summary="Submit datasets by URL (batch)")
async def submit_datasets_by_url(payload: dict):
    datasets = payload.get("datasets", [])
    if not datasets:
        raise HTTPException(status_code=400, detail="No datasets provided.")

    logger.info(
        "Received dataset URL submission request with %s item(s)", len(datasets)
    )

    futures = []
    for ds in datasets:
        name = ds.get("name", "unknown")
        file_type = ds.get("file_type", "").lower()
        size_mb = float(ds.get("size_mb", 0))
        url = ds.get("url", "")

        if not url:
            futures.append((name, None, "No URL provided."))
            continue

        future = executor.submit(process_dataset, name, file_type, size_mb, url)
        futures.append((name, future, None))

    results = []
    for name, future, pre_error in futures:
        if pre_error:
            results.append({"name": name, "status": "error", "error": pre_error})
            continue
        try:
            result = future.result(timeout=120)
            results.append(result)
        except Exception as e:
            results.append({"name": name, "status": "error", "error": str(e)})

    return JSONResponse(content={"submitted": len(results), "results": results})


@app.get("/datasets/{dataset_id}", summary="Get dataset metadata and logs")
async def get_dataset(dataset_id: int):
    logger.info("Fetching dataset %s", dataset_id)
    dataset = _get_dataset_or_404(dataset_id)

    # Fetch logs from MongoDB
    db = get_mongo_db()
    cursor = db["logs"].find(
        {"dataset_id": dataset_id},
        {"_id": 0}, 
    )
    logs = [log async for log in cursor]
    for log in logs:
        if "timestamp" in log:
            log["timestamp"] = log["timestamp"].isoformat()

    for key in ("created_at", "updated_at"):
        if dataset.get(key):
            dataset[key] = dataset[key].isoformat()

    return {"dataset": dataset, "logs": logs}


@app.delete("/datasets/{dataset_id}", summary="Soft-delete a dataset")
def delete_dataset(dataset_id: int):
    logger.info("Soft deleting dataset %s", dataset_id)
    _get_dataset_or_404(dataset_id)  # ensures it exists and is active

    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(
            update(datasets)
            .where(datasets.c.id == dataset_id)
            .values(is_active=False, updated_at=func.now())
        )

    log_action(dataset_id, "soft delete", "success")

    return {"message": f"Dataset {dataset_id} has been soft-deleted.", "id": dataset_id}


@app.get("/health", summary="Health check")
def health():
    logger.debug("Health check requested")
    return {"status": "ok"}


@app.get("/mongo-health", summary="Check MongoDB connectivity")
async def mongo_health():
    db = get_mongo_db()
    try:
        await db.command("ping")
        return {"status": "ok", "mongodb": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {e}")
