# ELT Pipeline for TMDB-Pipeline-Recommendation

TMDB-Pipeline-Recommendation is a Data Engineering project that builds a complete ELT pipeline to support:

- A movie recommendation system based on personal rating history
- Analytical dashboards for movie information

The project focuses on designing a full-fledged ELT pipeline, starting from data collection (Kaggle, TMDB API), transformation using Apache Spark following Lakehouse architecture, storage in PostgreSQL, data modeling with DBT, and visualization with Streamlit. Dagster is used as the data orchestrator.

## 🚀 Main Technologies & Tools Used

⚙️ Orchestration & Data Processing

<p>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="60" style="margin-right: 50px;" title="Python" />
  <img src="images/spark.png" width="100" style="margin-right: 50px;" title="Apache Spark" />
  <img src="images/dagster.png" width="80" style="margin-right: 20px;" title="Dagster" />
  <img src="images/dbt.png" width="100" title="dbt" />
</p>

☁️ Data Storage & Access

<p>
  <img src="https://min.io/resources/img/logo/MINIO_Bird.png" width="40" title="MinIO"/>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg" width="40" title="MySQL"/>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="40" title="PostgreSQL"/>
  <img src="images/polar.png" width="80" title="Polars" />
</p>

📊 Visualization

<p>
  <img src="https://streamlit.io/images/brand/streamlit-logo-primary-colormark-darktext.svg" width="140" title="Streamlit"/> 
</p>

---

## Streamlit Interface

![Streamlit UI](images/output.jpg)

---

## Project Overview

### 1. Data Pipeline Design

![Pipeline Diagram](images/pipeline.png)

**1. Data Sources – Collecting Data**

- Movie data is collected from two main sources:

  - `TMDB API`: Retrieves movie info from TMDB’s official API, including user-rated favorite movies.
  - `Kaggle`: Dataset (~1M) containing TMDB movie information.

- `MySQL`: Raw, unprocessed Kaggle dataset (~1M rows) is first loaded into MySQL.

**2. Lakehouse – Processing & Structuring Data**

- Centralized data processing is handled using:

  - `Apache Spark`: High-speed big data processing, structured into multiple layers:
    - **Bronze**: Stores raw ingested data
    - **Silver**: Cleaned and normalized data
    - **Gold**: Enriched and structured data ready for analytics

  - `Polars`: Used for lightweight, efficient pre-processing tasks.

  - `Spark MLlib`: Used for simple ML techniques or content-based recommendation.

**3. Data Warehouse – PostgreSQL**

- Once processed, data flows from Bronze → Silver → Gold, then into a PostgreSQL data warehouse.

  - `DBT`: Builds intermediate data models to simplify queries for the frontend.

**4. Streamlit – User Interface**

- Streamlit is used to create the user interface, with three main features:
  - **Recommendations**: Suggest movies based on behavior/content
  - **Visualizations**: Dashboards and charts from movie data
  - **Search Information**: Filter movies by rating, genre, release year

---

### 2. Data Lineage

Dagster is used as the **orchestrator**. It allows managing, scheduling, and visualizing data pipelines.

![Data lineage](images/lineage.jpg)

**Detailed Breakdown by Layer**

![Bronze Layer](images/bronze_layer.jpg)

![Silver Layer](images/silver.jpg)

![Gold Layer](images/gold.jpg)

![Warehouse Layer](images/warehouse.jpg)

---

## 3. Installation & Deployment Steps

### Prerequisites

- Docker & Docker Compose
- DBvear or any SQL management tool (for PostgreSQL and MySQL)
- Python 3

---

### Setup Steps

1. **Clone the Repository & Set Up:**
   ```bash
   git clone <repository-url>
   cd <repository-folder>
