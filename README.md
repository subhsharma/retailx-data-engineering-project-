🚀 RetailX Data Engineering Platform

An end-to-end Data Engineering and Lakehouse platform built using MinIO, Apache Airflow, Apache Spark, Apache Iceberg, dbt, and Docker.

The project demonstrates how raw retail data can be ingested, processed, transformed, tested, and converted into analytics-ready Bronze, Silver, and Gold datasets through an automated data pipeline.

📌 Project Overview

RetailX Data Engineering Platform is a production-style data engineering project designed to simulate a modern retail analytics platform.

The platform takes raw retail CSV data stored in an S3-compatible object storage layer and processes it through an orchestrated pipeline.

The complete flow is:

Raw Retail CSV Data
        ↓
      MinIO
        ↓
    Airflow
        ↓
     Spark
        ↓
   Iceberg
        ↓
 dbt + Spark Thrift
        ↓
 ┌───────────────┐
 │    BRONZE     │
 │  Raw/Staging  │
 └───────┬───────┘
         ↓
 ┌───────────────┐
 │    SILVER     │
 │ Cleaned/Data  │
 │ Transformation│
 └───────┬───────┘
         ↓
 ┌───────────────┐
 │     GOLD      │
 │ Analytics/Data│
 │     Marts     │
 └───────────────┘
         ↓
 Analytics-Ready Data

🏗️ Architecture

flowchart TB
    A["Retail CSV Data"] --> B["MinIO<br/>S3-Compatible Object Storage"]
    B --> C["Apache Airflow<br/>Orchestration"]
    C --> D["Python / Processing Tasks"]
    D --> E["Apache Spark"]
    E --> F["Apache Iceberg<br/>Lakehouse Tables"]
    F --> G["dbt + Spark Thrift Server"]
    G --> H["BRONZE<br/>6 Staging Models"]
    H --> I["SILVER<br/>Intermediate + Transformed Data"]
    I --> J["GOLD<br/>Facts + Dimensions + Analytics"]
    J --> K["Analytics-Ready Data"]
    G -.-> L["dbt Tests<br/>25 Tests"]
    G -.-> M["Snapshot<br/>customers_snapshot"]
    G -.-> N["Incremental Model<br/>fct_orders_incremental"]

🧰 Technology Stack

Technology

Purpose

Python

Data generation, ingestion and processing

MinIO

S3-compatible object storage

Apache Airflow

Workflow orchestration

Apache Spark

Distributed data processing

Apache Iceberg

Lakehouse table format

dbt

SQL transformation, testing and modeling

Spark Thrift Server

SQL connectivity for dbt

Docker

Containerization

Git & GitHub

Version control and project management

📊 Dataset

The project uses synthetic retail data representing a small e-commerce/retail business.

Dataset

Records

Customers

501

Products

201

Orders

3,000

Order Items

9,080

Payments

3,000

Stores

20

The raw files are stored in MinIO under:

raw/
├── customers.csv
├── products.csv
├── orders.csv
├── order_items.csv
├── payments.csv
└── stores.csv

🗄️ Data Lakehouse Layers

🥉 Bronze Layer

The Bronze layer contains staging models that represent the raw source data in a structured form.

Bronze models

stg_customers

stg_products

stg_orders

stg_order_items

stg_payments

stg_stores

Total: 6 staging models

The purpose of this layer is to:

Bring raw data into the transformation environment

Standardize basic data types

Preserve source-level information

Provide a reliable foundation for downstream transformations

🥈 Silver Layer

The Silver layer contains cleaned and transformed datasets.

Transformations include:

Data cleaning

Type conversions

Business logic

Joining related datasets

Removing invalid records

Preparing reusable intermediate datasets

The Silver layer acts as the main transformation layer between raw/staging data and analytics-ready data.

🥇 Gold Layer

The Gold layer contains business-ready analytical models.

Gold models

Model

Rows

customer_360

500

monthly_sales

20

fct_order_items

9,080

fct_orders_incremental

3,000

dim_store

20

fct_payments

3,000

dim_customer

500

dim_date

1,096

store_performance

20

fct_orders

3,000

dim_product

200

product_performance

200

Gold layer contains

Fact tables

fct_orders

fct_order_items

fct_payments

fct_orders_incremental

Dimension tables

dim_customer

dim_product

dim_store

dim_date

Analytics models

customer_360

monthly_sales

store_performance

product_performance

These datasets are designed to be consumed directly by analytics and reporting workloads.

🔄 Pipeline Workflow

The pipeline is orchestrated using Apache Airflow.

A simplified workflow is:

Start
  ↓
Validate MinIO
  ↓
Validate Source Data
  ↓
Spark Processing
  ↓
Create / Update Iceberg Tables
  ↓
Run dbt Build
  ↓
Run dbt Tests
  ↓
Run Snapshot
  ↓
Run Incremental Model
  ↓
Validate Output
  ↓
End

Airflow manages the execution order and dependencies between the different stages.

🌊 Airflow Orchestration

The project contains Airflow DAGs responsible for automating the data pipeline.

Main DAG:

dags/retailx_pipeline.py

The DAG coordinates:

Source validation

Object-storage connectivity

Spark processing

Lakehouse processing

dbt transformations

Data quality tests

Snapshot execution

Incremental processing

Pipeline validation

The latest pipeline execution completed successfully.

⚡ Apache Spark

Apache Spark is used as the distributed processing engine.

Spark is responsible for processing the raw data and interacting with the lakehouse storage layer.

The project uses:

Apache Spark 3.5.x

Spark is also exposed through a Spark Thrift Server, allowing dbt to communicate with Spark using SQL.

Spark Thrift Server:

Port: 10000

Docker container:

retailx-spark-thrift

🧊 Apache Iceberg

Apache Iceberg provides the table format for the lakehouse layer.

It provides a structured table abstraction on top of object storage and enables the project to behave more like a modern analytical data platform.

The architecture uses:

MinIO
   ↓
Apache Iceberg
   ↓
Spark
   ↓
dbt

🪣 MinIO

MinIO is used as the project's local S3-compatible object storage.

It simulates cloud object storage while allowing the complete platform to run locally.

Container:

retailx-minio

Host ports:

API:       19020
Console:   19021

Raw data is stored in the project bucket under:

raw/

Using MinIO makes it possible to reproduce an object-storage-based data engineering architecture without depending on a paid cloud environment.

🔧 dbt

dbt is used for SQL-based transformation and data modeling.

The project uses:

dbt Core
dbt Spark

dbt is responsible for:

SQL transformations

Model dependency management

Testing

Snapshots

Incremental processing

Documentation structure

Layered data modeling

The project contains:

21 models
1 snapshot
25 tests

dbt is used to transform the Bronze data into Silver and Gold analytical models.

🔁 Incremental Processing

The project includes an incremental model:

fct_orders_incremental

Instead of rebuilding the entire target table every time, incremental processing allows new or changed data to be processed according to the model's incremental logic.

This demonstrates an important production data engineering concept:

Initial Load
     ↓
Full Dataset
     ↓
New / Changed Data
     ↓
Incremental Processing
     ↓
Updated Target

Incremental models are useful when working with large datasets where rebuilding the complete dataset on every run would be inefficient.

📸 dbt Snapshot

The project also implements a dbt snapshot:

customers_snapshot

The snapshot uses:

strategy = check
unique_key = customer_id

The snapshot tracks changes to selected customer attributes, including:

First name

Last name

Email

City

This demonstrates how historical changes can be captured rather than simply overwriting the current record.

🧪 Data Quality & Testing

Data quality is an important part of the project.

The dbt project contains:

25 tests

Tests are used to validate assumptions about the data and relationships between models.

Examples of testing concepts include:

Unique keys

Not-null constraints

Referential relationships

Data integrity

Model-level validation

The complete dbt test execution passed successfully.

🐳 Docker

The platform is containerized using Docker.

The project uses containers for components such as:

MinIO
Airflow
Spark
Spark Thrift Server
dbt

This makes the environment reproducible and avoids requiring every service to be installed directly on the host machine.

📁 Project Structure

retailx-airflow-pipeline/
│
├── dags/
│   ├── retailx_first_dag.py
│   └── retailx_pipeline.py
│
├── dbt/
│   ├── analyses/
│   ├── macros/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   │
│   ├── snapshots/
│   ├── seeds/
│   ├── tests/
│   ├── docker_profiles/
│   │   └── profiles.yml
│   │
│   ├── dbt_project.yml
│   ├── Dockerfile
│   └── README.md
│
├── spark-conf/
│   └── Spark configuration files
│
├── retailx_bronze_customers.py
├── retailx_bronze_ingestion.py
├── retailx_gold_layer.py
├── retailx_silver_customers.py
├── retailx_silver_layer.py
├── retailx_silver_order_items.py
├── retailx_silver_orders.py
├── retailx_silver_payments.py
├── retailx_silver_products.py
│
├── spark_iceberg_test.py
│
├── docker-compose.yaml
├── spark-iceberg-compose.yaml
├── .gitignore
└── README.md

🔐 Security

Sensitive configuration files are intentionally excluded from Git.

The project's .gitignore protects files such as:

.env
.env.*
target/
logs/
airflow.db

Local/user-specific dbt metadata and large build artifacts are also excluded from the GitHub repository.

No credentials or secrets are intentionally committed to the repository.

▶️ Running the Project

1. Clone the repository

git clone https://github.com/subhsharma/retailx-data-engineering-project-.git

cd retailx-data-engineering-project-

2. Start the required Docker services

Start the required infrastructure using Docker Compose.

docker compose up -d

Depending on the environment, Spark-related services can be started using the corresponding Spark Compose configuration.

3. Verify running containers

docker ps

Confirm that the required services are running.

4. Verify MinIO

MinIO should be accessible through its configured host ports.

The raw retail datasets should be available in the configured bucket.

5. Run the Airflow pipeline

Open the Airflow interface and trigger:

retailx_pipeline

The DAG executes the complete pipeline.

6. Verify dbt

The dbt workflow performs:

dbt build
    ↓
dbt tests
    ↓
snapshot
    ↓
incremental model

📈 Validation Results

The complete pipeline was successfully executed and validated.

Pipeline

Airflow DAG: SUCCESS

Connectivity

MinIO: SUCCESS
Spark Thrift: SUCCESS
dbt → Spark: SUCCESS

dbt

Models:     21
Tests:      25
Snapshot:   SUCCESS
Incremental: SUCCESS

Data Layers

Bronze: 6 staging models
Silver: Intermediate + transformed datasets
Gold:   12 analytics models

Key Gold Outputs

customer_360             → 500 rows
monthly_sales             → 20 rows
fct_order_items           → 9,080 rows
fct_orders_incremental    → 3,000 rows
dim_store                 → 20 rows
fct_payments              → 3,000 rows
dim_customer              → 500 rows
dim_date                  → 1,096 rows
store_performance         → 20 rows
fct_orders                → 3,000 rows
dim_product               → 200 rows
product_performance       → 200 rows

🧩 Key Engineering Challenges Solved

Building the project involved solving several practical integration problems.

1. Connecting MinIO with Spark

Spark needed to communicate with an S3-compatible storage system.

The solution involved configuring the appropriate Hadoop AWS and AWS SDK dependencies and configuring the S3A filesystem.

2. Spark + Iceberg Integration

The project required Spark to correctly recognize and interact with Iceberg tables.

This required configuring:

Iceberg Spark runtime

Catalog configuration

Warehouse location

Object-storage connectivity

3. dbt + Spark Thrift Server

dbt needed a SQL interface to communicate with Spark.

The Spark Thrift Server was configured to provide that interface.

Architecture:

dbt
 ↓
Spark Thrift Server
 ↓
Apache Spark
 ↓
Apache Iceberg
 ↓
MinIO

4. Airflow + dbt Execution

The Airflow pipeline needed to execute dbt inside the containerized environment.

A target-path configuration was required so that dbt's generated artifacts were written to a stable location during execution.

5. Data Quality

The project includes automated dbt tests to detect problems before downstream analytics consume the data.

This provides a basic data-quality layer within the pipeline.

🎯 What This Project Demonstrates

This project demonstrates practical knowledge of:

Data Engineering

ETL/ELT pipelines

Data ingestion

Data transformation

Data modeling

Data quality

Incremental processing

Historical data tracking

Lakehouse Architecture

Object storage

Apache Iceberg

Spark

Bronze/Silver/Gold architecture

Orchestration

Apache Airflow

DAG design

Task dependencies

Pipeline automation

Analytics Engineering

dbt

SQL transformations

Testing

Snapshots

Incremental models

Dimensional modeling

DevOps

Docker

Docker Compose

Containerized services

Environment configuration

Development Practices

Git

GitHub

Project structure

Configuration management

Reproducible environments

💼 Resume Description

RetailX Data Engineering Platform

Tech Stack: Python, Apache Airflow, Apache Spark, Apache Iceberg, MinIO, dbt, Docker, SQL, Git

Built an end-to-end retail data engineering platform using MinIO, Apache Airflow, Spark, Iceberg and dbt. Designed a Bronze/Silver/Gold lakehouse architecture, automated ingestion and transformation workflows with Airflow, implemented 21 dbt models, 25 data-quality tests, incremental processing and customer-history snapshots, and containerized the platform using Docker.

🗣️ Interview Explanation

If asked "Explain your RetailX project", you can explain it like this:

RetailX is an end-to-end retail data engineering and lakehouse project that I built to understand how different data engineering technologies work together.

I started with raw retail CSV data and stored it in MinIO, which provides S3-compatible object storage. Apache Airflow orchestrates the pipeline and triggers the required processing tasks.

Apache Spark processes the data and works with Apache Iceberg as the lakehouse table format. After that, dbt connects through the Spark Thrift Server and performs SQL-based transformations.

I designed the data platform using Bronze, Silver and Gold layers. Bronze contains six staging models, Silver contains cleaned and transformed datasets, and Gold contains fact tables, dimensions and analytical models.

I also implemented dbt tests, a customer snapshot for historical tracking and an incremental orders model. The final pipeline contains 21 dbt models and 25 tests, and the complete Airflow pipeline was successfully validated.

I containerized the major components using Docker so that the environment can be reproduced locally.

📚 Learning Outcomes

Through this project, I gained practical experience with:

Raw Data
   ↓
Object Storage
   ↓
Orchestration
   ↓
Distributed Processing
   ↓
Lakehouse Storage
   ↓
SQL Transformation
   ↓
Data Quality
   ↓
Analytics Modeling

More specifically:

How object storage fits into a data platform

How Airflow orchestrates data pipelines

How Spark processes data

How Iceberg provides a lakehouse table layer

How dbt manages SQL transformations

How incremental models work

How snapshots preserve historical changes

How data quality tests are integrated into pipelines

How Docker can be used to reproduce a data engineering environment

How multiple data engineering technologies integrate into a single platform

🚀 Future Improvements

Possible future improvements include:

Add a BI dashboard using Power BI or Apache Superset

Add CI/CD using GitHub Actions

Add automated dbt documentation deployment

Add data observability

Add Great Expectations or another data-quality framework

Add more realistic streaming data

Add Kafka for real-time ingestion

Add AWS deployment using S3, Glue and Athena

Add monitoring and alerting

Add partitioning and performance optimization

Add automated pipeline scheduling

Add role-based access control

🏁 Project Status

Status: ✅ Completed and Validated

The complete local pipeline has been executed successfully.

✅ MinIO connectivity
✅ Raw data ingestion
✅ Spark processing
✅ Apache Iceberg
✅ Airflow orchestration
✅ Spark Thrift Server
✅ dbt transformation
✅ Bronze layer
✅ Silver layer
✅ Gold layer
✅ dbt tests
✅ dbt snapshot
✅ Incremental model
✅ Docker containerization
✅ Git version control
✅ GitHub repository

👨‍💻 Author

Yogesh Bhardwaj

Data Engineer | Data Engineering | SQL | Python | Spark | dbt | Snowflake | AWS

⭐ If You Find This Project Useful

Feel free to explore the repository and use the architecture as a learning reference for building your own end-to-end data engineering projects.
