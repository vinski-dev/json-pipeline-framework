JSON Pipeline Framework

A production-grade, idempotent data pipeline implementing the Medallion Architecture (Bronze, Silver, Gold) to extract, load, and transform JSON API data. This project leverages Apache Airflow for orchestration, dbt for data transformation, and GitHub Actions for continuous integration.

🏗️ Architecture & Tech Stack

Orchestration: Apache Airflow 2.7.1 (Dockerized)

Transformation: dbt (dbt-postgres)

Language: Python

Testing & Linting: pytest, flake8

CI/CD: GitHub Actions

Alerting: SMTP Email Integration (Gmail)

✨ Key Features

Medallion Architecture: Structured data progression from raw JSON ingestion to cleaned, business-ready models.

Idempotent Design: Safe to re-run pipeline steps without causing data duplication or inconsistencies.

Automated CI/CD Pipeline: GitHub Actions automatically lints Python code, runs DAG integrity tests, and verifies dbt compilation on every push to the main branch.

Automated Alerting: Airflow is configured with an SMTP backend to immediately dispatch email alerts containing error traces and direct logs links upon task failure.

Strict Dependency Management: Explicit version locking (e.g., pendulum<3.0, numpy<2.0.0) to guarantee binary compatibility across local and cloud environments.

📂 Project Structure

json-pipeline-framework/
├── .github/
│   └── workflows/
│       └── deploy_pipeline.yml   # GitHub Actions CI/CD configuration
├── dags/
│   └── random_user_pipeline.py   # Airflow DAG for API extraction and orchestration
├── include/
│   └── dbt_project/              # dbt models, profiles, and configuration
├── tests/
│   └── test_dag_integrity.py     # Pytest suite for catching Airflow import errors
├── logs/                         # Local Airflow execution logs (git-ignored)
├── docker-compose.yaml           # Airflow & Database container orchestration
├── .gitignore                    # Security and environment exclusion rules
└── README.md


🚀 Local Setup & Installation

1. Prerequisites

Docker & Docker Compose

Python 3.8+

Git

2. Environment Initialization

Clone the repository and set up your local testing environment:

git clone https://github.com/vinski-dev/json-pipeline-framework.git
cd json-pipeline-framework

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install dependencies with locked versions for stability
pip install apache-airflow==2.7.1 "pendulum<3.0" "numpy<2.0.0" pytest flake8 dbt-postgres pandas requests


3. Running Airflow

Start the local Airflow environment using Docker Compose:

docker-compose up -d


The Airflow UI will be available at http://localhost:8080.

4. Configuring Email Alerts

To enable failure alerting locally, configure your SMTP settings via environment variables (e.g., in your .env file) using an App Password:

AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com

AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com

AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password

AIRFLOW__SMTP__SMTP_PORT=465

AIRFLOW__SMTP__SMTP_SSL=True

🧪 Testing & CI/CD

This project enforces strict code quality checks before any code reaches production. You can run the CI checks locally before pushing:

1. Lint Python Code

flake8 dags/ --max-line-length=120 --ignore=E402,F401


2. Test DAG Integrity

pytest tests/test_dag_integrity.py


3. Compile dbt Models

dbt compile --project-dir include/dbt_project --profiles-dir include/dbt_project


When code is pushed to the main branch, the .github/workflows/deploy_pipeline.yml workflow automatically executes these exact steps on a fresh Ubuntu runner.