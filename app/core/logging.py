import asyncio
import logging
import os
from datetime import datetime, timezone

from pymongo import MongoClient


logger = logging.getLogger(__name__)
_sync_mongo_client = None


def _get_sync_logs_collection():
    global _sync_mongo_client
    if _sync_mongo_client is None:
        _sync_mongo_client = MongoClient(
            os.environ["MONGODB_URL"], serverSelectionTimeoutMS=2000
        )
    return _sync_mongo_client["ai_datasets"]["logs"]


def _insert_log_sync(doc: dict):
    _get_sync_logs_collection().insert_one(doc)


async def log_action_async(
    dataset_id: int,
    action: str,
    status: str,  # "success" or "failed"
    error_message: str = None,
):
    doc = {
        "dataset_id": dataset_id,
        "action": action,
        "status": status,
        "error_message": error_message,
        "timestamp": datetime.now(timezone.utc),
    }
    try:
        await asyncio.to_thread(_insert_log_sync, doc)
    except Exception as e:
        logger.exception("MongoDB write failed for dataset %s action %s: %s", dataset_id, action, e)


def log_action(
    dataset_id: int,
    action: str,
    status: str,
    error_message: str = None,
):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            _insert_log_sync(
                {
                    "dataset_id": dataset_id,
                    "action": action,
                    "status": status,
                    "error_message": error_message,
                    "timestamp": datetime.now(timezone.utc),
                }
            )
        except Exception as e:
            logger.exception(
                "MongoDB write failed for dataset %s action %s: %s",
                dataset_id,
                action,
                e,
            )
        return

    asyncio.create_task(log_action_async(dataset_id, action, status, error_message))
