# mod2-airflow
This repository contains the practical code and examples for the second class of the Fundamentals of Data Engineering with Python and SQL course. The focus is on introducing Apache Airflow, a powerful workflow orchestration tool widely used in modern data pipelines.

## Setup Instructions

### Step 1: Clone the Repository

If you haven't already cloned the repository, you can do so by running the following command:

```bash
git clone git@github.com:GADES-DATAENG/mod2-airflow.git
cd webinar
```

### Step 2: Create your .env file
Before starting the services, you need to build the .env file with some variables. Please check the .env.template file and use it as
a template for your .env file.
```bash
cp .env.template .env
```

### Step 3: Get your GCP service account JSON credentials file
After downloading your GCP service account JSON credentials file, just past it under the keys folder with the name `gcp-key.json`
If you don't need (or have) any GCP account yet, you can just create an empty file with the name `gcp-key.json`

### Step 4: Start the Services with Docker Compose
Once the image is built, you can start the services (Airflow, and other dependencies) using Docker Compose. Run the following command:
```bash
docker-compose up -d
```

This command will start all the containers defined in the `docker-compose.yml` file. It will set up Airflow, and any necessary services, including BigQuery integration.

### Step 6: Access the Services
- **Airflow Web UI**: You can access the Airflow web interface at http://localhost:8080
    - Default login credentials are
        - **Username**: `airflow`
        - **Password**: `airflow`

## Environment Setup
- The service account key file (`gcp-key.json`) should be inside the `keys` folder

Ensure that the key file is placed correctly in the repository folder as:
```bash
/mod2-airflow/gcp-key.json
```