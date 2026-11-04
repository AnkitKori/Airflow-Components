# 🌀 Apache Airflow – Learning Journey

![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.x-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

This repository contains my notes, DAGs, and hands-on practice from learning **Apache Airflow**. It covers everything from setting up Airflow locally with Docker to building, scheduling, and monitoring data pipelines.

---

## 📌 Table of Contents
- [What is Apache Airflow?](#-what-is-apache-airflow)
- [Architecture](#-architecture)
- [Core Concepts](#-core-concepts)
- [Project Structure](#-project-structure)
- [Setup & Installation](#-setup--installation)
- [DAGs in this Repo](#-dags-in-this-repo)
- [Sample DAG](#-sample-dag)
- [Key Learnings](#-key-learnings)
- [Credits](#-credits)

---

## 🤔 What is Apache Airflow?
Apache Airflow is an open-source platform to **author, schedule, and monitor workflows** as code.
Pipelines are written in Python as **DAGs (Directed Acyclic Graphs)**, which makes them versionable,
testable, and easy to maintain.

**Common use cases:** ETL/ELT pipelines, ML workflows, data quality checks, report automation.

---

## 🏗️ Architecture

| Component | Role |
|-----------|------|
| **API Server** | Serves the Airflow UI and REST API (replaces the Airflow 2 webserver) |
| **Scheduler** | Decides when DAG runs and tasks should be triggered |
| **DAG Processor** | Parses DAG files from the `dags/` folder |
| **Executor** | Determines *how* tasks run (Local, Celery, Kubernetes) |
| **Workers** | Execute the actual task code |
| **Metadata Database** | Stores state of DAGs, runs, tasks, variables, connections |

---

## 📚 Core Concepts

- **DAG** – A collection of tasks with dependencies, defining a workflow
- **Task** – A single unit of work inside a DAG
- **Operators** – Templates for tasks (`PythonOperator`, `BashOperator`, `EmptyOperator`, etc.)
- **TaskFlow API** – Write tasks as Python functions using `@dag` and `@task` decorators
- **XComs** – Pass small pieces of data between tasks
- **Scheduling** – Run DAGs with cron expressions, presets (`@daily`), or timedeltas
- **Catchup & Backfill** – Run DAGs for past intervals
- **Branching** – Choose a path dynamically with `@task.branch`
- **Assets** – Trigger DAGs based on data updates (data-aware scheduling)
- **Variables & Connections** – Store config and credentials outside code
- **Hooks & Sensors** – Interact with external systems and wait for conditions

---

## 📁 Project Structure

```
airflow-learning/
├── dags/                  # All DAG files
│   ├── first_dag.py
│   ├── taskflow_dag.py
│   ├── xcom_dag.py
│   ├── branching_dag.py
│   └── asset_dag.py
├── plugins/               # Custom operators / hooks
├── config/                # Airflow config (airflow.cfg)
├── logs/                  # Task logs (git-ignored)
├── docker-compose.yaml    # Airflow services
├── requirements.txt       # Extra Python packages
└── README.md
```

---

## ⚙️ Setup & Installation

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- At least 4 GB RAM allocated to Docker

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>

# 2. Create required folders
mkdir -p ./dags ./logs ./plugins ./config

# 3. (Linux only) Set the Airflow user
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 4. Initialize the database
docker compose up airflow-init

# 5. Start all services
docker compose up -d
```

Open the UI at **http://localhost:8080**
Default login: `airflow` / `airflow`

### Stop Airflow
```bash
docker compose down
```

---

## 🗂️ DAGs in this Repo

| DAG | Description |
|-----|-------------|
| `first_dag.py` | Basic DAG with operators and task dependencies |
| `taskflow_dag.py` | Same pipeline rewritten with the TaskFlow API |
| `xcom_dag.py` | Sharing data between tasks using XComs |
| `branching_dag.py` | Conditional execution with branching |
| `asset_dag.py` | Producer/consumer DAGs using Assets |

---

## 🧪 Sample DAG

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="sample_etl_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning"],
)
def sample_etl():

    @task
    def extract():
        return {"name": "Airflow", "version": 3}

    @task
    def transform(data: dict):
        data["name"] = data["name"].upper()
        return data

    @task
    def load(data: dict):
        print(f"Loaded data: {data}")

    load(transform(extract()))

sample_etl()
```

---

## 💡 Key Learnings
- Writing pipelines as code makes them easy to version and review
- TaskFlow API makes DAGs cleaner and handles XComs automatically
- Using `catchup=False` avoids unexpected historical runs
- Docker Compose is the quickest way to run a full Airflow setup locally
- Assets enable event/data-driven pipelines instead of only time-based schedules

---

## 🙏 Credits
- **Ansh Lamba** – [YouTube Channel](<channel-link>) for the Airflow tutorial
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

---

⭐ If you found this helpful, consider giving the repo a star!