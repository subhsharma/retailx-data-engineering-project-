from datetime import datetime, timedelta
import io

import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="retailx_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retailx", "production", "data-engineering"],
) as dag:

    @task
    def pipeline_start():
        print("RetailX pipeline started")

    @task
    def test_minio_connection():
        hook = S3Hook(aws_conn_id="retailx_minio")

        bucket = "retailx"

        expected_files = {
            "raw/customers.csv",
            "raw/products.csv",
            "raw/orders.csv",
            "raw/order_items.csv",
            "raw/payments.csv",
            "raw/stores.csv",
        }

        files = set(
            hook.list_keys(
                bucket_name=bucket,
                prefix="raw/",
            )
        )

        print(f"Files detected: {len(files)}")

        for file in sorted(files):
            print(file)

        missing_files = expected_files - files

        if missing_files:
            raise ValueError(
                f"Missing expected files: {sorted(missing_files)}"
            )

        print("All expected RetailX raw files are present")

    @task
    def inspect_customers():
        hook = S3Hook(aws_conn_id="retailx_minio")

        bucket = "retailx"
        key = "raw/customers.csv"

        obj = hook.get_key(
            key=key,
            bucket_name=bucket,
        )

        data = obj.get()["Body"].read()

        df = pd.read_csv(io.BytesIO(data))

        print("customers.csv loaded successfully")
        print(f"Rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        print("\nFirst 3 rows:")
        print(df.head(3).to_string(index=False))

    @task
    def transform_customers():
        hook = S3Hook(aws_conn_id="retailx_minio")

        bucket = "retailx"
        key = "raw/customers.csv"

        obj = hook.get_key(
            key=key,
            bucket_name=bucket,
        )

        data = obj.get()["Body"].read()

        df = pd.read_csv(io.BytesIO(data))

        # Clean column names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        # Remove duplicate customer records
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)

        print(f"Rows before transformation: {before}")
        print(f"Rows after transformation: {after}")
        print(f"Duplicates removed: {before - after}")

        print("\nTransformed columns:")
        print(df.columns.tolist())

        print("\nTransformed data:")
        print(df.head(5).to_string(index=False))

        return df.head(5).to_dict(orient="records")

    @task
    def save_processed_customers():
        hook = S3Hook(aws_conn_id="retailx_minio")

        bucket = "retailx"
        raw_key = "raw/customers.csv"
        processed_key = "processed/customers.csv"

        # Read raw data
        obj = hook.get_key(
            key=raw_key,
            bucket_name=bucket,
        )

        data = obj.get()["Body"].read()
        df = pd.read_csv(io.BytesIO(data))

        # Apply the same transformation
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        df = df.drop_duplicates()

        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload processed data to MinIO
        hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=processed_key,
            bucket_name=bucket,
            replace=True,
        )

        print(f"Processed file saved: {processed_key}")
        print(f"Processed rows: {len(df)}")

    @task
    def process_all_datasets():
        hook = S3Hook(aws_conn_id="retailx_minio")

        bucket = "retailx"

        datasets = [
            "customers",
            "products",
            "orders",
            "order_items",
            "payments",
            "stores",
        ]

        for dataset in datasets:

            raw_key = f"raw/{dataset}.csv"
            processed_key = f"processed/{dataset}.csv"

            print(f"\nProcessing: {raw_key}")

            # Read raw CSV from MinIO
            obj = hook.get_key(
                key=raw_key,
                bucket_name=bucket,
            )

            data = obj.get()["Body"].read()

            df = pd.read_csv(io.BytesIO(data))

            rows_before = len(df)

            # Clean column names
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
            )

            # Remove duplicate rows
            df = df.drop_duplicates()

            rows_after = len(df)

            # Convert transformed DataFrame to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            # Save to processed layer
            hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=processed_key,
                bucket_name=bucket,
                replace=True,
            )

            print(f"Processed: {dataset}")
            print(f"Rows before: {rows_before}")
            print(f"Rows after: {rows_after}")
            print(f"Duplicates removed: {rows_before - rows_after}")
            print(f"Saved to: {processed_key}")

        print("\nAll RetailX datasets processed successfully")

    @task
    def data_quality_check():
        hook = S3Hook(aws_conn_id="retailx_minio")
        bucket = "retailx"

        datasets = [
            "customers",
            "products",
            "orders",
            "order_items",
            "payments",
            "stores",
        ]

        print("\nStarting RetailX data quality checks...")

        for dataset in datasets:
            key = f"processed/{dataset}.csv"

            obj = hook.get_key(
                key=key,
                bucket_name=bucket,
            )

            if obj is None:
                raise ValueError(
                    f"Data quality failed: {key} does not exist"
                )

            data = obj.get()["Body"].read()

            if not data:
                raise ValueError(
                    f"Data quality failed: {key} is empty"
                )

            df = pd.read_csv(io.BytesIO(data))

            if df.empty:
                raise ValueError(
                    f"Data quality failed: {key} has 0 rows"
                )

            print(
                f"PASS | {dataset} | "
                f"Rows: {len(df)} | "
                f"Columns: {len(df.columns)}"
            )

        print("\nAll RetailX data quality checks passed")

    run_dbt = DockerOperator(
        task_id="run_dbt_build",
        image="retailx:1.0",
        command=[
            "dbt",
            "build",
            "--profiles-dir",
            "/dbt_profiles",
            "--project-dir",
            "/dbt_project",
            "--target-path",
            "/tmp/dbt_target",
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="retailx-cloud-data-platform_default",
        mount_tmp_dir=False,
        mounts=[
            {
                "source": r"C:\Users\yogesh\retailx-cloud-data-platform\retailx_dbt",
                "target": "/dbt_project",
                "type": "bind",
            },
            {
                "source": r"C:\Users\yogesh\retailx-cloud-data-platform\retailx_dbt\docker_profiles",
                "target": "/dbt_profiles",
                "type": "bind",
            },
        ],
        auto_remove="success",
)

    @task
    def pipeline_end():
        print("RetailX pipeline completed")
    @task
    def pipeline_end():
        print("RetailX pipeline completed")

    start = pipeline_start()
    minio_test = test_minio_connection()
    customers_test = inspect_customers()
    transform_test = transform_customers()
    save_processed = save_processed_customers()
    process_all = process_all_datasets()
    quality_check = data_quality_check()
    end = pipeline_end()

    start >> minio_test >> customers_test >> transform_test >> save_processed >> process_all >> quality_check >> run_dbt >> end