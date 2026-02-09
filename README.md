# Dynamic Airflow Orchestration for Salesforce & BigQuery

This project provides a robust and scalable data orchestration solution using Apache Airflow to extract data from Salesforce and ingest it into Google BigQuery. The DAGs are generated dynamically from YAML configuration files, allowing for easy extension and management without modifying the core Python code.

## Overview

The primary goal of this project is to create "bronze" tables in BigQuery with raw data extracted from various Salesforce objects. The process is orchestrated by Airflow, which leverages a Workato API for extraction and a Google Cloud Run job for ingestion.

### Key Features

- **Dynamic DAG Generation**: Airflow DAGs are built dynamically based on simple YAML configuration files. Adding a new Salesforce object to the pipeline is as simple as adding a few lines to a `.yaml` file.
- **Concurrency Control**: The number of parallel extractions from Salesforce is limited using Airflow Pools to avoid hitting API rate limits and to manage load.
- **Resilient Workflow**: The main DAG includes `start`, `end`, and `failure` states, ensuring that any task failure is caught and can be used to trigger alerts.
- **Containerized Environment**: The entire Airflow setup runs within Docker containers using `docker-compose`, ensuring a consistent and reproducible environment for local development.

## Architecture

The data pipeline follows a simple, sequential pattern for each Salesforce object defined in the configuration. The main DAG (`sfdc_bronze_ingestion`) orchestrates these steps:

1.  **Start**: A trigger task that initiates the entire workflow.
2.  **Extract**: A set of parallel tasks that call a Workato API via `HttpOperator` to extract data for different Salesforce objects. The number of concurrent extractions is limited to 3.
3.  **Ingest**: For each object, once the extraction is complete, a corresponding task triggers a Google Cloud Run job via `CloudRunExecuteJobOperator` to load the data into a BigQuery bronze table.
4.  **End/Failure**: The workflow concludes with an `end` task upon successful completion of all steps. If any task fails, a `failure` task is triggered instead, providing a hook for notifications.
---
```
[ start ]
    |
    +--> [ extract_lead ] ----> [ ingest_lead ] --+
    |                                             |
    +--> [ extract_account ] --> [ ingest_account ] --+--> [ end / failure ]
    |                                             |
    +--> [ extract_contact ] --> [ ingest_contact ] --+
    |
    ... (for all configured objects)
```
---

## Airflow UI
<img title="Airflow UI" alt="sfdc bronze ingestion dag" src="images/sfdc_bronze_ingestion.png">

---

## Running Locally with Docker Compose

Follow these steps to set up and run the Airflow environment on your local machine.

### Prerequisites

- **Docker & Docker Compose**: Ensure you have both installed and the Docker daemon is running.
- **Google Cloud SDK**: Install `gcloud` on your local machine to handle authentication with GCP.
- **GCP Project**: You need a Google Cloud Project with the **Cloud Run API** and **BigQuery API** enabled.
- **Workato API Access**: You must have access to the Salesforce extraction API endpoint and a valid API token.

### Step 1: Google Cloud Authentication

The Airflow container needs to authenticate with your Google Cloud account to trigger the Cloud Run jobs. We achieve this by mounting your local `gcloud` credentials into the container.

Run the following command on your host machine to generate your Application Default Credentials:
```bash
gcloud auth application-default login
```
This will open a browser window for you to log in and will store a credentials file in `~/.config/gcloud/application_default_credentials.json`. The `docker-compose.yml` file is already configured to mount this into the Airflow containers.

### Step 2: Build and Run the Airflow Containers

Once your GCP authentication is configured, you can start the Airflow environment.

```bash
docker-compose up --build -d
```
The first time you run this, it may take a few minutes to download the necessary Docker images and initialize the Airflow database.

### Step 3: Configure Airflow Connections

The DAGs rely on Airflow Connections to interact with external services. You must configure these in the Airflow UI.

1.  **Access the Airflow UI**: Open your browser and navigate to `http://localhost:8080`. The default username and password are `airflow`.

2.  **Create the Google Cloud Connection**:
    - Navigate to **Admin -> Connections**.
    - Click the `+` icon to add a new connection.
    - Set the following values:
        - **Connection Id**: `google_cloud_default`
        - **Connection Type**: `Google Cloud`
    - Leave all other fields (like `Keyfile Path`) blank. Airflow will automatically use the Application Default Credentials you configured in Step 1.
    - Click **Save**.

3.  **Create the Workato HTTP Connection**:
    - On the same **Connections** page, add another new connection.
    - Set the following values:
        - **Connection Id**: `workato_http_base`
        - **Connection Type**: `HTTP`
        - **Host**: `https://apim.workato.com`
    - Click **Save**.

    > **Note on API Token**: In the current implementation, the Workato API token is hardcoded in the DAG files (e.g., `dags/sfdc_bronze_ingestion_dag.py`). For production use, you should replace the hardcoded string with a more secure method, such as retrieving it from the Password field of the `workato_http_base` connection or using a secrets backend (e.g., HashiCorp Vault, GCP Secret Manager).

### Step 4: Configure the Airflow Pool

To limit concurrency, the `sfdc_bronze_ingestion` DAG uses a pool.

1.  In the Airflow UI, navigate to **Admin -> Pools**.
2.  Click the `+` icon to add a new pool.
3.  Set the following values:
    - **Pool**: `sfdc_extract_pool`
    - **Slots**: `3`
    - **Description**: `Limits concurrency for Salesforce extraction tasks.`
4.  Click **Save**.

### Step 5: Run the DAG

1.  On the Airflow UI homepage, find the `sfdc_bronze_ingestion` DAG.
2.  Click the toggle to un-pause it.
3.  Click the "play" button on the right to trigger a manual run.

## Customization

To add, remove, or modify the Salesforce objects being processed, simply edit the `dags/sfdc_bronze_config.yaml` file. The `sfdc_bronze_ingestion` DAG will automatically pick up the changes on its next run.

Next Test - 2 