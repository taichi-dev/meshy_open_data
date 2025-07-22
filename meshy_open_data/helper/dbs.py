# -*- coding: utf-8 -*-

# -- stdlib --
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union

# -- third party --
import pandas as pd
import polars as pl
try:
    from databricks.connect import DatabricksSession
    HAS_DATABRICKS_CONNECT = True
except ImportError:
    HAS_DATABRICKS_CONNECT = False
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sdk.service.jobs import (
    CreateResponse,
    FileArrivalTriggerConfiguration,
    NotebookTask,
    Source,
    Task,
    TriggerSettings,
)
from databricks.sql import connect

if HAS_DATABRICKS_CONNECT:
    from pyspark.sql import DataFrame

# -- code --

@dataclass
class DatabricksConfig:
    """Configuration for Databricks connections"""

    host: str
    token: str
    # warehouse_id should include http_path as well
    # in general, serverless warehouse with SQL operation is much faster to startup,
    # hence prefered when reading data
    warehouse_id: Optional[str] = None
    cluster_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    catalog: str = "meshy_3d"
    schema: str = "silver"

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        """Load configuration from environment variables"""
        return cls(
            host=os.environ.get(
                "DATABRICKS_HOST", "dbc-83c969e1-3439.cloud.databricks.com"
            ),
            token=os.environ.get("DATABRICKS_TOKEN", ""),
            warehouse_id=os.environ.get(
                "DATABRICKS_WAREHOUSE_ID", "/sql/1.0/warehouses/91c66511ebfe8c9f"
            ),
            cluster_id=os.environ.get("DATABRICKS_CLUSTER_ID", "serverless"),
            client_id=os.environ.get("DATABRICKS_CLIENT_ID", ""),
            client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET", ""),
            catalog=os.environ.get("DATABRICKS_CATALOG", "meshy_3d"),
            schema=os.environ.get("DATABRICKS_SCHEMA", "silver"),
        )


class DatabricksHelper:
    """
    A simplified manager for Databricks operations, handling both data and workspace tasks.
    This class provides a unified interface for interacting with Databricks, supporting both
    SQL warehouse operations and workspace management tasks.

    Configuration:
    Accepts a DatabricksConfig object that specifies:
    - host: Databricks workspace URL
    - token: Personal Access Token (optional)
    - warehouse_id: SQL warehouse ID for queries
    - cluster_id: Compute cluster ID for Spark operations
    - client_id/secret: OAuth credentials
    - catalog/schema: Default database context

    Authentication:
        Specify ONE of the following authentication methods:
        1. PAT Authentication: Set DATABRICKS_TOKEN environment variable or pass token in config
           Example:
           - Environment variable: export DATABRICKS_TOKEN="dapi1234567890abcdef"
           - Config:
             ```python
             # Get default config first
             config = DatabricksConfig.from_env()
             # Override token
             config.token = "dapi1234567890abcdef"
             # Create helper instance with custom config
             dbh = DatabricksHelper(config=config)
             ```
        2. OAuth Authentication: Set both DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables
           or pass client_id and client_secret in config

        DO NOT specify both token and OAuth credentials (client_id and client_secret) as this will cause authentication conflicts on databricks

        Celery jobs (including the use of mlflow in celery) running on research k8s is set up to use OAUTH-m2m Authentication under celery_pipeline service principal.

        For usage outside of celery pipeline, please set your own personal access token with DATABRICKS_TOKEN env var.
        in addition, if you want to use mlflow, you will need to set MLFLOW_TRACKING_URI variable to `databricks` (yes, just a string literal `databricks`)

    Key Features:
    - SQL Operations: Execute queries against Databricks SQL warehouses
    - Workspace Management: Create, modify, and delete notebooks, jobs, and other workspace assets
    - Authentication Support: Handles both Personal Access Token (PAT) and OAuth service principal authentication
    - Resource Management: Manages connections to SQL warehouses and clusters
    - Data Operations: Supports both DataFrame and raw SQL operations
    - Spark Context: Provides access to Spark context for distributed computing
    - File Operations: Facilitates copying and managing files between local filesystem and Databricks workspace/DBFS

    Usage Examples:
    1. Execute SQL Query:
        ```python
        # Execute SQL query and get all results at once
        with DatabricksHelper() as dbh:
            df = dbh.execute_sql("SELECT * FROM my_table")

        # Execute SQL query and fetch results in batches
        with DatabricksHelper() as dbh:
            for batch_df in dbh.fetch_sql_results("SELECT * FROM my_table", batch_size=10000):
                # Process each batch of results
                process_batch(batch_df)
        ```

    2. Workspace Operations:
        The wrapper is used in celery pipeline to create auto ingestion jobs for
        condenser output s3. in addition, you get obtain a workspace client from the wrapper class
        to perform your own job operations, workspace management etc.

        ```python
        # Initialize helper and get workspace client
        dbh = DatabricksHelper()
        ws_client = dbh.get_workspace_client()

        # Create a new job
        ws_client.jobs.create(
            name="my_job",
            tasks=[{
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": "/path/to/notebook",
                },
                "existing_cluster_id": "cluster-id"
            }]
        )

        # List existing jobs
        jobs = ws_client.jobs.list()

        # Get job details
        job_details = ws_client.jobs.get(job_id="job-id")

        # Run a job
        run_id = ws_client.jobs.run_now(job_id="job-id")
        ```
        ```

    3. Using Spark Context:
        ```python
        with DatabricksHelper() as dbh:
            spark = dbh.get_spark_session()
            df = spark.read.parquet("dbfs:/path/to/data")
        ```

    4. File Operations:
        The wrapper provides methods to copy files between local filesystem and Databricks workspace/DBFS.

        ```python
        with DatabricksHelper() as dbh:
            # Upload local file to workspace
            dbh.upload_to_workspace(
                source_path="/local/path/file.csv",
                target_path="dbfs:/target/path/file.csv"
            )

            # Download file from workspace to local
            dbh.download_from_workspace(
                source_path="dbfs:/source/path/data.parquet",
                target_path="/local/path/data.parquet"
            )

            # Copy between workspace locations
            dbh.upload_to_workspace(
                source_path="dbfs:/source/path/",
                target_path="dbfs:/target/path/"
            )
        ```

    5. Custom Configuration:
        specify configuration to point to your own SQL warehouse or general purpose warehouse
        for running SQL or spark job (using local as driver-node on databricks cluster)
        ```python
        config = DatabricksConfig(
            host="your-host",
            token="your-token",
            warehouse_id="your-warehouse"
            cluster_id="your-cluster"
        )
        dbh = DatabricksHelper(config=config)
        ```

    6. Setup MLflow
        for celery_pipeline, it automatically use celery service principal
        for usage outside celery k8s, you will need to set MLFLOW_TRACKING_URI variable to `databricks`,
        and set DATABRICKS_TOKEN to your PAT

    Best Practices:
    - Use context manager ('with' statement) to ensure proper resource cleanup
    - For long-running operations, prefer non-serverless clusters
    - For quick queries, use SQL warehouses
    - Set appropriate timeouts for operations based on workload
    - When using Spark context, consider cluster configuration and resource requirements
    - Use Spark context only when needed to optimize resource usage, spark cluster will not start until action functions
    - Set proper inactivity shutddown when using personal non-serverless clusters
    """

    def __init__(self, config: DatabricksConfig = None):
        """Initialize the Databricks manager."""
        self.logger = logging.getLogger(__name__)

        self.config = config if config is not None else DatabricksConfig.from_env()

        self.use_token = False
        # if no client_id provided and token is present
        if self.config.token != "":
            self.use_token = True

        # we may not always need spark context
        self._init_connections()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _get_oauth_m2m_config(self):
        return Config(
            host=self.config.host,
            # these come from env var in k8s secrets
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            auth_type="oauth-m2m",
        )

    def credential_provider(self):
        config = self._get_oauth_m2m_config()
        return oauth_service_principal(config)

    def _init_workspace_client(self):
        # Initialize workspace client
        if self.use_token:
            self.ws_client = WorkspaceClient(
                host=self.config.host, token=self.config.token, auth_type="pat"
            )
        else:
            self.ws_client = WorkspaceClient(
                host=self.config.host,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                # need to specify auth_type when multiple method present
                auth_type="oauth-m2m",
            )

    def _init_connections(self):
        """Initialize connections"""
        try:
            self._init_workspace_client()
            self._init_sql_connection()
            self._init_spark_session()
        except Exception as e:
            self.logger.error(f"Connection failed: {str(e)}")
            raise

    def _init_sql_connection(self):
        # Initialize SQL warehouse connection if configured
        if self.config.warehouse_id:
            if self.use_token:
                self.sql_connection = connect(
                    server_hostname=self.config.host,
                    http_path=self.config.warehouse_id,
                    access_token=self.config.token,
                )
            else:
                self.sql_connection = connect(
                    server_hostname=self.config.host,
                    http_path=self.config.warehouse_id,
                    credentials_provider=self.credential_provider,
                )
        else:
            self.sql_connection = None

    def _init_spark_session(self):
        if not HAS_DATABRICKS_CONNECT:
            self.spark = None
            return

        if self.config.cluster_id and self.config.cluster_id != "serverless":
            if self.use_token:
                self.logger.info(
                    f"Connecting to cluster: {self.config.cluster_id} with token"
                )
                self.spark = DatabricksSession.builder.remote(
                    host=self.config.host,
                    token=self.config.token,
                    cluster_id=self.config.cluster_id,
                ).getOrCreate()
            else:
                # no token given, use all default environment variable
                os.environ["DATABRICKS_CLUSTER_ID"] = self.config.cluster_id
                self.spark = DatabricksSession.builder.getOrCreate()
        elif self.config.cluster_id == "serverless":
            if self.use_token:
                self.spark = (
                    DatabricksSession.builder.remote(
                        host=self.config.host, token=self.config.token
                    )
                    .serverless(True)
                    .getOrCreate()
                )
            else:
                self.spark = DatabricksSession.builder.remote(
                    serverless=True
                ).getOrCreate()

        else:
            self.spark = None

    def parse_table_name(self, table_name):
        """
        Parse table name with defaults for catalog and schema.
        """
        # Default catalog and schema
        catalog = self.config.catalog
        schema = self.config.schema
        # Split input table name
        parts = table_name.split(".")

        if len(parts) == 1:
            return catalog, schema, parts[0]
        elif len(parts) == 2:
            return catalog, parts[0], parts[1]
        elif len(parts) == 3:
            return parts[0], parts[1], parts[2]
        else:
            raise ValueError("Invalid table name format")

    # Data Operations
    def execute_sql(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        force_spark: bool = False,
        return_polars: bool = False
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Execute SQL query using SQL warehouse if available, fall back to Spark

        Note: Databricks does not support BEGIN/END, unlike some other database/warehouses.
        So there no transaction bundled at the database level.

        You can probably mitigate this by writing CTE and/or temporary tables

        """
        try:
            if self.config.warehouse_id and not force_spark:
                with self.sql_connection.cursor() as cursor:
                    # use default schema and catalog, in case they are not specified
                    if return_polars:
                        return pl.read_database(sql, cursor, execute_options={"parameters": parameters})

                    cursor.execute(f"use catalog {self.config.catalog};")
                    cursor.execute(f"use schema {self.config.schema};")
                    if parameters:
                        cursor.execute(sql, parameters)
                    else:
                        cursor.execute(sql)
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(result, columns=columns)

            if not HAS_DATABRICKS_CONNECT:
                raise Exception("Databricks connect is not installed")
            elif self.config.cluster_id:
                return self.spark.sql(sql).toPandas()
            else:
                raise ValueError("Neither warehouse_id nor cluster_id is configured")
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise

    def fetch_sql_results(
        self,
        sql: str, batch_size: int,
        parameters: Optional[Dict[str, Any]] = None,
        return_polars: bool = False
    ):
        """
        Execute SQL query using SQL, but as a generator returns smaller batches of results
        suitable for large dataframe fetch
        """
        if not self.config.warehouse_id:
            self.logger.error(f"No SQL warehouse selected.")
            raise ValueError("No SQL Warehouse selected")
        with self.sql_connection.cursor() as cursor:
            # use default schema and catalog, in case they are not specified
            cursor.execute(f"use catalog {self.config.catalog};")
            cursor.execute(f"use schema {self.config.schema};")
            if return_polars:
                for df in pl.read_database(sql,
                                           cursor,
                                           iter_batches=True,
                                           batch_size=batch_size,
                                           execute_options={"parameters": parameters}):
                    yield df

            else:
                if parameters:
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql)
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    else:
                        columns = [desc[0] for desc in cursor.description]
                        yield pd.DataFrame(rows, columns=columns)

    def table_exists(
        self, table: str, schema: Optional[str] = None, catalog: Optional[str] = None
    ) -> bool:
        """
        Check if a table exists in the specified schema and catalog.
        """
        default_catalog, default_schema, table = self.parse_table_name(table)
        catalog = catalog or default_catalog
        schema = schema or default_schema
        query = f"""
            SELECT COUNT(*)
            FROM {catalog}.information_schema.tables
            WHERE table_catalog = '{catalog}'
            AND table_schema = '{schema}'
            AND table_name = '{table}';
        """
        result = self.execute_sql(sql=query)
        return result.iloc[0, 0] > 0

    def read_table(
        self,
        table: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        force_spark: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        default_catalog, default_schema, table = self.parse_table_name(table)
        catalog = catalog or default_catalog
        schema = schema or default_schema
        query = f"select * from {catalog}.{schema}.{table};"
        return self.execute_sql(sql=query, force_spark=force_spark)

    def write_table(
        self,
        df,
        table: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        mode: str = "overwrite",
    ) -> None:
        if not self.spark:
            raise Exception("Spark cluster required for write operations")

        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        try:
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
            spark_df = (
                self.spark.createDataFrame(df) if isinstance(df, pd.DataFrame) else df
            )
            spark_df.write.mode(mode).saveAsTable(f"{catalog}.{schema}.{table}")
        except Exception as e:
            self.logger.error(f"Write error: {str(e)}")
            raise

    def get_spark(self):
        """provide spark session on general purpose cluster for custom operations."""
        if not HAS_DATABRICKS_CONNECT:
            raise Exception("Databricks connect is not installed")
        return self.spark

    def get_workspace_client(self) -> Optional[WorkspaceClient]:
        """provide a workspace client for more general workspace management tasks"""
        return self.ws_client

    def get_run_status(self, run_id: int) -> Dict:
        """Get the status of a job run."""
        try:
            run = self.ws_client.jobs.get_run(run_id)
            return {
                "run_id": run.run_id,
                "status": run.state.life_cycle_state,
                "result": run.state.result_state,
                "start_time": run.start_time,
                "end_time": run.end_time,
            }
        except Exception as e:
            self.logger.error(f"Error getting run status: {str(e)}")
            raise

    # Download and uploads from volumes
    def upload_to_workspace(
        self, local_path: Union[str, Path], workspace_path: str, overwrite: bool = False
    ):
        """Upload a file to the Databricks workspace"""
        try:
            local_path = str(local_path)
            self.ws_client.dbfs.copy(local_path, workspace_path, overwrite=overwrite)
        except Exception as e:
            self.logger.error(f"Upload failed: {str(e)}")
            raise

    def download_from_workspace(
        self, workspace_path: str, local_path: Union[str, Path], overwrite: bool = False
    ):
        """Download a file from the Databricks workspace"""
        try:
            local_path = str(local_path)
            self.ws_client.dbfs.copy(workspace_path, local_path, overwrite=overwrite)
        except Exception as e:
            self.logger.error(f"Download failed: {str(e)}")
            raise

    def _get_table_name(self, table_name):
        """
        replace most of the special characters with underscore,
        which is the preferred connecting char for database names
        """
        return re.sub(r"[^a-zA-Z0-9]", "_", table_name)

    # creating ingestion job for specific celery pipeline needs
    def create_auto_ingestion_job(
        self,
        run_id: str,
        s3_folder: str,
        table: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        interval="",
        file_format: str = "json",
    ) -> CreateResponse:
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        table = self._get_table_name(table)
        job_name = f"auto_loader_{run_id}"
        description = f"tmp data auto loader for celery pipeline {run_id}"
        notebook_path = (
            "/Workspace/Users/weifei@meshy.ai/auto_loaders/celery_data_autoload"
        )
        task_key = f"ingestion_{run_id}"
        base_parameters = {
            "s3_folder": s3_folder,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "trigger_interval": str(interval),
            "file_format": file_format,
        }

        if interval and self.config.cluster_id == "serverless":
            raise ValueError(
                "Serverless cluster does not accept ingestion jobs with trigger interval configurations"
            )

        existing_cluster_id = self.config.cluster_id
        # if no interval is set, use file arrival trigger, and serverless cluster
        if not interval:
            existing_cluster_id = None
            trigger = TriggerSettings(
                file_arrival=FileArrivalTriggerConfiguration(url=s3_folder)
            )

        task = Task(
            description=description,
            notebook_task=NotebookTask(
                base_parameters=base_parameters,
                notebook_path=notebook_path,
                source=Source("WORKSPACE"),
            ),
            task_key=task_key,
        )

        if existing_cluster_id:
            task.existing_cluster_id = existing_cluster_id

        j = self.ws_client.jobs.create(name=job_name, trigger=trigger, tasks=[task])
        return j

    def remove_auto_ingestion_job(self, job_id):
        """removes the auto ingestion job"""
        return self.ws_client.jobs.delete(job_id=job_id)

    def close(self):
        """Clean up connections"""
        try:
            if hasattr(self, "sql_connection"):
                self.sql_connection.close()
            if hasattr(self, "spark") and self.spark is not None:
                self.spark.stop()
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")
            raise

    @classmethod
    def iterable_to_sql_set(cls, iterable):
        """Convert an iterable to a string representation of a SQL set for IN condition."""
        if not iterable:
            return "NULL"  # Handle empty iterable case
        return f"({', '.join(repr(item) for item in iterable)})"
