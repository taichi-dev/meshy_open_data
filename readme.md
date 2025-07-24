# Meshy Open Data Helper Package

A comprehensive Python package for data operations with Databricks and S3, designed for the Meshy 3D platform. This package provides simplified interfaces for common data engineering tasks including SQL operations, workspace management, and cloud storage interactions.

## Features

### 🏗️ Databricks Integration (`DatabricksHelper`)
- **SQL Operations**: Execute queries against Databricks SQL warehouses with pandas/polars support
- **Workspace Management**: Create, modify, and delete notebooks, jobs, and workspace assets
- **Authentication Support**: Personal Access Token (PAT) authentication
- **File Operations**: Seamless file management between local filesystem and Databricks workspace/DBFS

### ☁️ S3 Storage (`S3IO`)
- **AWS S3 Integration**: Seamless AWS S3 operations
- **Async Operations**: High-performance asynchronous file operations
- **Batch Operations**: Efficient bulk upload/download capabilities

## Installation

[Pixi](https://pixi.sh) is a modern package management tool that provides reproducible environments. Install pixi first:

```bash
# Install pixi
curl -fsSL https://pixi.sh/install.sh | bash

# Then install dependencies and activate environment
pixi install
pixi shell
```

## Setup

### Environment Variables

```bash
# Databricks Configuration
export DATABRICKS_HOST="your-databricks-workspace-url"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_WAREHOUSE_ID="your-warehouse-http-path"
export DATABRICKS_CATALOG="your_catalog_name"
export DATABRICKS_SCHEMA="your_schema_name"

# MLflow (optional)
export MLFLOW_TRACKING_URI="databricks"

# S3 Configuration
export AWS_ACCESS_KEY_ID="your-aws-access-key"
export AWS_SECRET_ACCESS_KEY="your-aws-secret-key"
```

### How to Obtain Configuration Values

#### 🏗️ Databricks Configuration

**DATABRICKS_HOST**
- Go to your Databricks workspace
- Copy the URL from your browser (e.g., `https://your-workspace.cloud.databricks.com`)
- Remove the `https://` prefix when setting the environment variable

**DATABRICKS_TOKEN** (Personal Access Token)
1. In your Databricks workspace, click on your profile icon (top right)
2. Go to "User Settings"
3. Navigate to "Developer" → "Access tokens"
4. Click "Generate new token"
5. Set expiration and description, then click "Generate"
6. Copy the generated token immediately (it won't be shown again)

**DATABRICKS_WAREHOUSE_ID**
1. In your Databricks workspace, go to "SQL Warehouses" in the sidebar
2. Select or create a SQL warehouse
3. Click on the warehouse name to open its details
4. Go to "Connection details" tab
5. Copy the "HTTP Path" value (e.g., `/sql/1.0/warehouses/abc123def456`)

**DATABRICKS_CATALOG** and **DATABRICKS_SCHEMA**
- These are your Unity Catalog database identifiers
- Check with your Databricks administrator for the correct values
- Common defaults: catalog=`main`, schema=`default`

### Authentication Methods

#### 🏗️ Databricks Authentication

Use Personal Access Token (PAT) authentication:

```python
# Set environment variable
export DATABRICKS_TOKEN="your-personal-access-token"

# Or pass in configuration
config = DatabricksConfig.from_env()
config.token = "your-personal-access-token"
dbh = DatabricksHelper(config=config)
```

#### ☁️ S3 Authentication

Set your AWS credentials as environment variables:

```python
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

### 🔒 Security Best Practices

1. **Never commit credentials to version control**
2. **Use environment variables or secure secret management**
3. **Rotate tokens and keys regularly**

## Usage Examples

### 🏗️ Databricks Operations

#### Basic SQL Queries

```python
from meshy_open_data.helper.dbs import DatabricksHelper, DatabricksConfig

# Using default configuration from environment
with DatabricksHelper() as dbh:
    # Execute a simple query
    df = dbh.execute_sql("SELECT * FROM my_table LIMIT 100")
    print(df.head())
    
    # Use Polars instead of Pandas
    df_polars = dbh.execute_sql("SELECT * FROM my_table", return_polars=True)
    
    # Execute query with parameters
    df = dbh.execute_sql(
        "SELECT * FROM my_table WHERE created_date > ?",
        parameters=["2024-01-01"]
    )
```

#### Batch Processing Large Results

```python
# Process large datasets in batches
with DatabricksHelper() as dbh:
    for batch_df in dbh.fetch_sql_results(
        "SELECT * FROM large_table", 
        batch_size=10000
    ):
        # Process each batch
        process_batch(batch_df)
```

#### Table Operations

```python
with DatabricksHelper() as dbh:
    # Check if table exists
    if dbh.table_exists("my_table"):
        # Read entire table
        df = dbh.read_table("my_table")
        
        # Write data to table
        dbh.write_table(df, "new_table", mode="overwrite")
```

#### Custom Configuration

```python
# Custom Databricks configuration
config = DatabricksConfig(
    host="your-custom-host",
    token="your-token",
    warehouse_id="your-warehouse-id",
    catalog="your_catalog",
    schema="your_schema"
)

with DatabricksHelper(config=config) as dbh:
    df = dbh.execute_sql("SELECT COUNT(*) FROM my_table")
```

#### Workspace and Job Management

```python
# Get workspace client for advanced operations
dbh = DatabricksHelper()
ws_client = dbh.get_workspace_client()

# List existing jobs
jobs = list(ws_client.jobs.list())

# Create a new job
job_response = ws_client.jobs.create(
    name="my_data_job",
    tasks=[{
        "task_key": "main_task",
        "notebook_task": {
            "notebook_path": "/path/to/notebook",
        }
    }]
)

# Run the job
run_id = ws_client.jobs.run_now(job_id=job_response.job_id)

# Check run status
status = dbh.get_run_status(run_id.run_id)
print(f"Job status: {status}")
```

#### File Operations

```python
with DatabricksHelper() as dbh:
    # Upload local file to DBFS
    dbh.upload_to_workspace(
        local_path="/local/data/file.csv",
        workspace_path="dbfs:/mnt/data/file.csv"
    )
    
    # Download from DBFS to local
    dbh.download_from_workspace(
        workspace_path="dbfs:/mnt/results/output.parquet",
        local_path="/local/results/output.parquet"
    )
```



### ☁️ S3 Operations

#### Basic S3 Operations

```python
from meshy_open_data.helper.s3io import get_s3io

# Initialize S3 client
s3 = get_s3io()

# Check if file exists
if s3.doesKeyExist("data/my_file.csv"):
    print("File exists!")

# Upload file
success = s3.uploadFile("/local/path/data.csv", "remote/data.csv")

# Download file
s3.downloadFile("remote/data.csv", "/local/download/data.csv")

# List directory contents
files = s3.listdir("data/processed/")
print(f"Found {len(files)} files")
```

#### Working with Pandas DataFrames

```python
# Read Parquet file directly into DataFrame
df = s3.getTableParquet("data/analytics/user_metrics.parquet")
print(df.head())

# Upload DataFrame as Parquet
import pandas as pd
df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
parquet_bytes = df.to_parquet()
s3.uploadFile(parquet_bytes, "output/results.parquet")
```

#### Cached File Access

```python
# Using context manager for cached file access
with s3.getFileCached("large_dataset.json") as file_obj:
    import json
    data = json.load(file_obj)
    
# Force cache update
with s3.getFileCached("frequently_updated.csv", update_cache=True) as file_obj:
    import pandas as pd
    df = pd.read_csv(file_obj)
```

#### Asynchronous Operations

```python
import asyncio

async def process_files():
    s3 = get_s3io()
    
    # Prepare async client
    async with s3.prepAsyncCacheClient():
        # Check file existence
        exists = await s3.doesKeyExistAsync("data/file.json")
        
        if exists:
            # Read file asynchronously
            async with s3.asyncGetFileCached("data/file.json") as file_obj:
                import json
                data = json.loads(file_obj.read().decode())
                
        # Bulk upload files
        upload_tasks = [
            ("/local/file1.csv", "remote/file1.csv"),
            ("/local/file2.csv", "remote/file2.csv"),
        ]
        results = await s3.asyncUploadFiles(upload_tasks)
        print(f"Upload results: {results}")

# Run async operations
asyncio.run(process_files())
```





## Best Practices

### 🏗️ Databricks
- Use **context managers** (`with` statement) to ensure proper resource cleanup
- For quick queries, prefer **SQL warehouses** over clusters
- Set appropriate **timeouts** based on workload requirements
- Leverage **batch processing** for large datasets to avoid memory issues

### ☁️ S3
- Use **async operations** for better performance with multiple files
- Implement proper **error handling** for network operations
- Use **batch operations** for uploading multiple files efficiently

## Error Handling

```python
try:
    with DatabricksHelper() as dbh:
        df = dbh.execute_sql("SELECT * FROM non_existent_table")
except Exception as e:
    print(f"Query failed: {e}")

try:
    s3 = get_s3io()
    s3.downloadFile("missing_file.csv", "/local/path.csv")
except Exception as e:
    print(f"Download failed: {e}")
```

