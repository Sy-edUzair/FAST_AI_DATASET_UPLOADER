import os

import requests
from sqlalchemy import func, select, update

from db.database import datasets, get_pg_engine
from core.logging import log_action
from utils.priority_scoring import calculate_priority
from db.cloudinary import download_and_upload

ALLOWED_TYPES = {"csv", "json", "zip"}
MAX_MB = {"csv": 100, "json": 100, "zip": 500}


def _update_status(conn, dataset_id: int, status: str, cloud_url: str = None):
    values = {"status": status, "updated_at": func.now()}
    if cloud_url:
        values["cloud_storage_url"] = cloud_url

    conn.execute(update(datasets).where(datasets.c.id == dataset_id).values(**values))


def process_dataset(name: str, file_type: str, size_mb: float, url: str) -> dict:
    """
    Full pipeline for a single dataset.
    Returns a result dict so the API can build a response.
    Called in a separate thread for concurrency.
    """
    engine = get_pg_engine()
    dataset_id = None

    with engine.begin() as conn:
        try:
            #Insert row
            result = conn.execute(
                datasets.insert()
                .values(
                    name=name,
                    file_type=file_type,
                    size_mb=size_mb,
                    status="submitted",
                    priority_score=1,
                    priority_level="Low",
                )
                .returning(datasets.c.id)
            )
            dataset_id = result.scalar_one()

            log_action(dataset_id, "submission", "success")
            # Validate Filetype
            if file_type not in ALLOWED_TYPES:
                _update_status(conn, dataset_id, "error")
                log_action(
                    dataset_id,
                    "validation",
                    "failed",
                    f"File type '{file_type}' not allowed.",
                )
                return {
                    "id": dataset_id,
                    "name": name,
                    "status": "error",
                    "error": f"File type '{file_type}' not allowed.",
                }

            # 3. Validate size
            if size_mb > MAX_MB[file_type]:
                _update_status(conn, dataset_id, "error")
                log_action(
                    dataset_id,
                    "validation",
                    "failed",
                    f"Size {size_mb:.1f} MB exceeds limit {MAX_MB[file_type]} MB.",
                )
                return {
                    "id": dataset_id,
                    "name": name,
                    "status": "error",
                    "error": f"Size {size_mb:.1f} MB exceeds limit {MAX_MB[file_type]} MB.",
                }

            # 4. Duplicate check
            dup = conn.execute(
                select(datasets.c.id)
                .where(
                    datasets.c.name == name,
                    datasets.c.file_type == file_type,
                    datasets.c.is_active.is_(True),
                    datasets.c.id != dataset_id,
                )
                .limit(1)
            ).first()

            if dup:
                _update_status(conn, dataset_id, "error")
                log_action(
                    dataset_id,
                    "validation",
                    "failed",
                    f"Duplicate: same name+type already exists (id={dup.id}).",
                )
                return {
                    "id": dataset_id,
                    "name": name,
                    "status": "error",
                    "error": "Duplicate submission rejected.",
                }

            log_action(dataset_id, "validation", "success")

            # 5. Priority Scoring
            score, level = calculate_priority(file_type, size_mb)
            conn.execute(
                update(datasets)
                .where(datasets.c.id == dataset_id)
                .values(priority_score=score, priority_level=level, updated_at=func.now())
            )
            log_action(dataset_id, "priority calculation", "success")

            # 6. Validate the source before upload.
            if url.startswith("file://"):
                local_path = url[len("file://") :]
                if not os.path.exists(local_path):
                    _update_status(conn, dataset_id, "failed")
                    log_action(
                        dataset_id,
                        "cloud URL check",
                        "failed",
                        f"Local file not found: {local_path}",
                    )
                    return {
                        "id": dataset_id,
                        "name": name,
                        "status": "failed",
                        "error": f"Local file not found: {local_path}",
                    }
                log_action(dataset_id, "cloud URL check", "success")
            else:
                try:
                    head = requests.head(url, timeout=10, allow_redirects=True)
                    head.raise_for_status()
                    log_action(dataset_id, "cloud URL check", "success")
                except Exception as e:
                    _update_status(conn, dataset_id, "failed")
                    log_action(dataset_id, "cloud URL check", "failed", str(e))
                    return {
                        "id": dataset_id,
                        "name": name,
                        "status": "failed",
                        "error": f"URL not reachable: {e}",
                    }

            # 7. Download + upload to Cloudinary 
            log_action(dataset_id, "dataset download", "success")  # mark start
            try:
                cloud_url = download_and_upload(url, file_type, name)
                log_action(dataset_id, "cloud upload", "success")
            except RuntimeError as e:
                _update_status(conn, dataset_id, "failed")
                log_action(dataset_id, "cloud upload", "failed", str(e))
                return {"id": dataset_id, "name": name, "status": "failed", "error": str(e)}

            # 8. Final update
            _update_status(conn, dataset_id, "validated", cloud_url)

            return {
                "id": dataset_id,
                "name": name,
                "status": "validated",
                "priority_score": score,
                "priority_level": level,
                "cloud_url": cloud_url,
            }

        except Exception as e:
            # Catch-all exception
            if dataset_id:
                try:
                    _update_status(conn, dataset_id, "error")
                    log_action(dataset_id, "processing", "failed", str(e))
                except Exception:
                    pass
            return {"id": dataset_id, "name": name, "status": "error", "error": str(e)}
