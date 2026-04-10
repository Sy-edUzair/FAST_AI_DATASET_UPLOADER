import os
import tempfile
import requests
import urllib3
import cloudinary
import cloudinary.uploader
from dotenv import load_dotenv

load_dotenv()

# Configure urllib3 globally for concurrent uploads to avoid pool exhaustion warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.HTTPWarning)

# Increase default pool size for requests/urllib3
requests.adapters.DEFAULT_POOLSIZE = 20
requests.adapters.DEFAULT_POOLBLOCK = False

cloudinary.config(
    secure=True,
)

CHUNK_SIZE = 1024 * 1024  # 1 MB streaming chunks
MAX_CSV_JSON = 100 * 1024 * 1024  # 100 MB
MAX_ZIP = 500 * 1024 * 1024  # 500 MB


def download_and_upload(url: str, file_type: str, dataset_name: str) -> str:
    max_bytes = MAX_ZIP if file_type == "zip" else MAX_CSV_JSON

    # Case 1: Local dataset file
    if url.startswith("file://"):
        local_path = url[len("file://") :]
        if not os.path.exists(local_path):
            raise RuntimeError(f"Local file not found: {local_path}")
        size = os.path.getsize(local_path)
        if size > max_bytes:
            raise RuntimeError(
                f"File size {size/(1024*1024):.1f} MB exceeds "
                f"limit {max_bytes//(1024*1024)} MB"
            )
        file_to_upload = local_path
        cleanup = False

    # Case 2: Online URL
    else:
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Download failed: {e}")

        suffix = f".{file_type}"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        file_to_upload = tmp.name
        cleanup = True

        try:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    downloaded += len(chunk)
                    if downloaded > max_bytes:
                        tmp.close()
                        os.remove(file_to_upload)
                        raise RuntimeError(
                            f"File exceeds size limit " f"({max_bytes//(1024*1024)} MB)"
                        )
                    tmp.write(chunk)
            tmp.close()
        except RuntimeError:
            raise
        except Exception as e:
            try:
                tmp.close()
                os.remove(file_to_upload)
            except OSError:
                pass
            raise RuntimeError(f"Download error: {e}")
    try:
        result = cloudinary.uploader.upload(
            file_to_upload,
            resource_type="raw",
            public_id=f"datasets/{dataset_name}",
            overwrite=True,
        )
    except Exception as e:
        raise RuntimeError(f"Cloudinary upload failed: {e}")
    finally:
        if cleanup:
            try:
                os.remove(file_to_upload)
            except OSError:
                pass

    return result["secure_url"]
