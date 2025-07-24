# Meshy Open Data Helper Package

A comprehensive Python package for data operations with Databricks and S3, designed for the Meshy 3D platform. This package provides simplified interfaces for common data engineering tasks including SQL operations, workspace management, and cloud storage interactions.

## Features

### 🏗️ Databricks Integration (`DatabricksHelper`)
- **SQL Operations**: Execute queries against Databricks SQL warehouses with pandas/polars support
- **Workspace Management**: Create, modify, and delete notebooks, jobs, and workspace assets
- **Authentication Support**: Both Personal Access Token (PAT) and OAuth service principal authentication
- **Spark Context**: Access to distributed computing capabilities
- **File Operations**: Seamless file management between local filesystem and Databricks workspace/DBFS
- **Auto-ingestion Jobs**: Automated data pipeline creation for S3 to Databricks workflows

### ☁️ S3 Storage (`S3IO`)
- **Multi-backend Support**: AWS S3 and Ceph RGW (Rados Gateway) compatibility
- **Async Operations**: High-performance asynchronous file operations
- **Local Caching**: Optional local file caching for improved performance
- **Batch Operations**: Efficient bulk upload/download capabilities

## Installation

```bash
# Install required dependencies
pip install pandas polars boto3 aiobotocore aiofiles databricks-sql-connector databricks-sdk

# For Spark operations (optional)
pip install databricks-connect
```

## Configuration

### Environment Variables

```bash
# Databricks Configuration
export DATABRICKS_HOST="your-databricks-workspace-url"
export DATABRICKS_TOKEN="your-personal-access-token"  # For PAT auth
export DATABRICKS_WAREHOUSE_ID="your-warehouse-http-path"
export DATABRICKS_CLUSTER_ID="your-cluster-id"  # or "serverless"
export DATABRICKS_CATALOG="your_catalog_name"
export DATABRICKS_SCHEMA="your_schema_name"

# For OAuth authentication (alternative to PAT)
export DATABRICKS_CLIENT_ID="your-oauth-client-id"
export DATABRICKS_CLIENT_SECRET="your-oauth-client-secret"

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

**DATABRICKS_CLUSTER_ID**
1. Go to "Compute" in the Databricks sidebar
2. Select or create a cluster
3. Click on the cluster name
4. Copy the cluster ID from the URL or cluster details
5. Use `"serverless"` for serverless compute

**DATABRICKS_CATALOG** and **DATABRICKS_SCHEMA**
- These are your Unity Catalog database identifiers
- Check with your Databricks administrator for the correct values
- Common defaults: catalog=`main`, schema=`default`

**OAuth Credentials** (for service principals)
1. In Databricks workspace, go to "Settings" → "Identity and access"
2. Click "Service principals" → "Add service principal"
3. Create the service principal and note the Application ID (client_id)
4. Generate a secret for the service principal (client_secret)
5. Assign necessary permissions to the service principal

#### ☁️ AWS S3 Configuration

**AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY**
1. **IAM User Approach** (recommended for development):
   - Go to AWS IAM console
   - Create a new user or use existing one
   - Attach S3 permissions policy (e.g., `AmazonS3FullAccess`)
   - Go to "Security credentials" tab
   - Create access key → Choose "Application running outside AWS"
   - Download or copy the Access Key ID and Secret Access Key

2. **IAM Role Approach** (recommended for production):
   - Create an IAM role with S3 permissions
   - Attach the role to your EC2 instance/ECS task/Lambda function
   - No need to set these environment variables when using IAM roles

**LOCAL_CACHE_DIR** (optional)
- Any local directory path for caching S3 files
- Example: `/tmp/s3_cache` or `./cache`
- Directory will be created automatically if it doesn't exist

**RGW_HOSTS** (optional, for Ceph RGW)
- Contact your system administrator for RGW endpoint URLs
- Format: comma-separated HTTP URLs (e.g., `http://10.0.1.1:7480,http://10.0.1.2:7480`)

#### 🔒 Security Best Practices

1. **Never commit credentials to version control**
2. **Use environment variables or secure secret management**
3. **Rotate tokens and keys regularly**
4. **Use IAM roles instead of access keys when possible**
5. **Limit permissions to minimum required scope**
6. **Consider using tools like AWS Secrets Manager or HashiCorp Vault**

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
        },
        "existing_cluster_id": "your-cluster-id"
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

#### Spark Operations

```python
with DatabricksHelper() as dbh:
    # Get Spark session for custom operations
    spark = dbh.get_spark()
    
    # Read data using Spark
    df = spark.read.parquet("dbfs:/mnt/data/large_dataset")
    
    # Perform Spark transformations
    result = df.filter(df.status == "active").groupBy("category").count()
    
    # Convert to Pandas for further processing
    pandas_df = result.toPandas()
```

#### Auto-ingestion Jobs

```python
# Create automated S3 to Databricks ingestion job
with DatabricksHelper() as dbh:
    job_response = dbh.create_auto_ingestion_job(
        run_id="unique_run_id",
        s3_folder="s3://your-bucket/data/",
        table="ingested_data",
        file_format="json",
        interval="5 minutes"  # Optional: for scheduled ingestion
    )
    
    # Later, remove the job when no longer needed
    dbh.remove_auto_ingestion_job(job_response.job_id)
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

#### Local Caching

```python
# Enable local file caching for better performance
s3_cached = get_s3io(local_cache_dir="/tmp/s3_cache")

# Files will be cached locally on first access
with s3_cached.getFileCached("large_model.pkl") as file_obj:
    import pickle
    model = pickle.load(file_obj)
    
# Subsequent access will use local cache
with s3_cached.getFileCached("large_model.pkl") as file_obj:
    model = pickle.load(file_obj)  # Loads from local cache
```

## Authentication Methods

### Databricks Authentication

#### 1. Personal Access Token (PAT)
```python
# Set environment variable
export DATABRICKS_TOKEN="dapi1234567890abcdef"

# Or pass in configuration
config = DatabricksConfig.from_env()
config.token = "dapi1234567890abcdef"
dbh = DatabricksHelper(config=config)
```

#### 2. OAuth Service Principal (for automated systems)
```python
# Set environment variables
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Configuration will automatically use OAuth
dbh = DatabricksHelper()
```

**Important**: Do not specify both token and OAuth credentials simultaneously.

### S3 Authentication

The package uses AWS IAM roles by default. For explicit credentials:

```python
# Set environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

## Best Practices

### 🏗️ Databricks
- Use **context managers** (`with` statement) to ensure proper resource cleanup
- For quick queries, prefer **SQL warehouses** over clusters
- For long-running operations, use **dedicated clusters** instead of serverless
- Set appropriate **timeouts** based on workload requirements
- Use **Spark context** only when necessary to optimize resource usage
- Leverage **batch processing** for large datasets to avoid memory issues

### ☁️ S3
- Enable **local caching** for frequently accessed files
- Use **async operations** for better performance with multiple files
- Implement proper **error handling** for network operations
- Consider **RGW caching** for high-throughput scenarios

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

## Utility Functions

```python
# Convert iterables to SQL IN clause format
from meshy_open_data.helper.dbs import DatabricksHelper

user_ids = [1, 2, 3, 4, 5]
sql_set = DatabricksHelper.iterable_to_sql_set(user_ids)
query = f"SELECT * FROM users WHERE user_id IN {sql_set}"
```

## Contributing

When extending this package:
1. Follow the existing patterns for error handling and logging
2. Add comprehensive docstrings with usage examples
3. Implement proper resource cleanup in context managers
4. Add type hints for better IDE support
5. Consider both sync and async patterns where appropriate

## License

This project is licensed under the terms specified in the LICENSE file.