from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="retailx_first_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retailx", "learning"],
) as dag:

    @task
    def start_task():
        print("RetailX Airflow pipeline started")

    @task
    def process_task():
        print("Processing RetailX data")

    @task
    def finish_task():
        print("RetailX Airflow pipeline completed")

    start = start_task()
    process = process_task()
    finish = finish_task()

    start >> process >> finish