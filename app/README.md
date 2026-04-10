# AI Dataset Backend

FastAPI backend for submitting datasets from local file uploads or remote URLs, storing metadata in PostgreSQL, and writing processing logs to MongoDB.

## Prerequisites

- Python 3.11+ recommended
- PostgreSQL installed and running
- MongoDB installed and running
- Cloudinary account and credentials
- Postman for API testing

## Project Setup

1. Clone the repository and open it in VS Code or your terminal.
2. Create a virtual environment:

    ```bash
    python -m venv .venv
    ```

3. Activate the virtual environment:

    ```bash
    source .venv/bin/activate
    ```

4. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

5. Create a `.env` file in the project root.

### Example `.env`

```env
POSTGRES_DB="ai_datasets"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="1234"
DB_HOST=localhost
DB_PORT=5432
POSTGRES_URL="postgresql://postgres:1234@localhost:5432/ai_datasets"
CLOUDINARY_URL=cloudinary://<api_key>:<api_secret>@<cloud_name>
MONGO_DB_NAME="ai_dataset_logs"
MONGODB_URL=mongodb://localhost:27017/
```

## Start Required Services

Start PostgreSQL:

```bash
sudo systemctl start postgresql
sudo systemctl status postgresql --no-pager
```

Start MongoDB:

```bash
sudo systemctl start mongod
sudo systemctl status mongod --no-pager
```

Enable both on boot if you want them to start automatically:

```bash
sudo systemctl enable postgresql
sudo systemctl enable mongod
```

If you need to stop them later:

```bash
sudo systemctl stop postgresql
sudo systemctl stop mongod
```

## Run the API

From the repository root:

```bash
uvicorn app.main:app --reload
```

If you are already inside the `app/` directory:

```bash
uvicorn main:app --reload
```

Open the interactive docs:

```text
http://127.0.0.1:8000/docs
```

## Health Checks

- `GET /health` checks whether the API is up.
- `GET /mongo-health` pings MongoDB and returns whether the database is reachable.

## API Endpoints

### `POST /datasets`

Uploads one or more files directly.

- Content type: `multipart/form-data`
- Field name: `files`
- File upload only, not JSON

Example request in Postman:

- Body tab
- Select `form-data`
- Add key `files`
- Change the type to `File`
- Choose one file, or add multiple rows with the same `files` key for batch upload

Example response:

```json
{
   "submitted": 1,
   "results": [
      {
         "id": 1,
         "name": "my_dataset",
         "status": "validated",
         "priority_score": 8,
         "priority_level": "High",
         "cloud_url": "https://..."
      }
   ]
}
```

### `POST /datasets/url`

Submits one or more datasets by remote URL.

- Content type: `application/json`
- Body must contain a `datasets` array

Example body:

```json
{
   "datasets": [
      {
         "name": "customer_data",
         "file_type": "csv",
         "size_mb": 12.5,
         "url": "https://example.com/customer_data.csv"
      }
   ]
}
```

Important:

- The URL must be reachable from the server.
- The backend first checks the URL and then downloads/uploads it.

### `GET /datasets/{dataset_id}`

Returns dataset metadata and MongoDB logs for a dataset.

- No request body
- Put the dataset id in the URL path

### `DELETE /datasets/{dataset_id}`

Soft-deletes a dataset by marking it inactive.

- No request body
- Put the dataset id in the URL path

### `GET /health`

Simple application health check.

### `GET /mongo-health`

Checks whether MongoDB is reachable from the API.

Example success response:

```json
{
   "status": "ok",
   "mongodb": "connected"
}
```

## Postman Collection

Add your shared Postman collection link here once it is published:
```text
https://www.postman.com/sy-eduzair-1366980/syed-uzair-s-workspace/request/53747524-06861d93-8f0a-4510-b43a-affe82abae31?sideView=agentMode
```

```text
https://documenter.getpostman.com/view/53747524/2sBXirini3#0e519b12-41f0-4e36-85a5-72af132feb60
```

Suggested Postman requests to include in the collection:

- `GET /health`
- `GET /mongo-health`
- `POST /datasets`
- `POST /datasets/url`
- `GET /datasets/{dataset_id}`
- `DELETE /datasets/{dataset_id}`

## Troubleshooting

### MongoDB connection refused

If Compass or the API says connection refused, verify MongoDB is running:

```bash
sudo systemctl start mongod
ss -ltnp | grep 27017
```

### Upload warnings from Cloudinary

If you see connection pool warnings during batch uploads, they are usually a performance warning, not a hard failure. The uploads can still succeed.

### File upload failing with `file://`

The `/datasets` endpoint expects actual file uploads in Postman. It does not expect a JSON payload.

### Remote URL upload failing

Make sure the URL is publicly reachable and returns a valid file response.

## Project Layout

- `app/main.py`: FastAPI entrypoint and routes
- `app/core/processor.py`: dataset validation and processing flow
- `app/core/logging.py`: dataset action logging
- `app/db/database.py`: PostgreSQL and MongoDB setup
- `app/db/cloudinary.py`: download and Cloudinary upload helper
- `app/utils/priority_scoring.py`: priority calculation logic

## Notes

- PostgreSQL stores dataset metadata.
- MongoDB stores action logs.
- Cloudinary stores uploaded dataset files.
- The app creates the PostgreSQL table on startup.