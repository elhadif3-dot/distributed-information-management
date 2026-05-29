<div align="center">

![Distributed Information Management Banner](https://capsule-render.vercel.app/api?type=waving&color=0:FF3621,50:7C3AED,100:00A6ED&height=175&section=header&text=Distributed%20Information%20Management&fontSize=36&fontColor=ffffff&animation=fadeIn&fontAlignY=38&desc=PySpark%20analytics%20%7C%20Spark%20ML%20%7C%20Kafka%20streaming%20%7C%20Databricks&descSize=15&descAlignY=60)

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Distributed%20Analytics-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Notebook%20Workflows-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![CI](https://img.shields.io/badge/GitHub%20Actions-Validation-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)

</div>

# Distributed Information Management

Portfolio repository for distributed data processing projects built with PySpark and Databricks.

This portfolio analyzes large-scale TV set-top-box viewing data, demographic data, program metadata, and reference data. It focuses on distributed ETL, data quality, feature engineering, Spark ML, and streaming analytics over multi-table datasets.

## Recruiter Snapshot

| Area | Evidence In This Repository |
| --- | --- |
| Distributed ETL | Schema-driven ingestion from CSV and Parquet sources |
| Analytics | Multi-table joins, demographic segmentation, DMA and genre ranking |
| Spark ML | Feature pipelines, PCA visualization, KMeans household clustering |
| Streaming | Kafka ingestion with Spark Structured Streaming and micro-batch state |
| Engineering maturity | Sanitized notebooks, extracted Python scripts, validation workflow |

## Projects

| Project | Focus | Stack |
| --- | --- | --- |
| [Project 1 - TV Viewership Analytics](projects/project-1-tv-viewership-analytics) | PySpark ETL, anomaly detection, audience analytics, demographic segmentation | PySpark, Spark SQL, Databricks |
| [Project 2 - Household Clustering & Streaming](projects/project-2-household-clustering-streaming) | Feature engineering, PCA, KMeans clustering, subset analysis, Kafka streaming | PySpark ML, Spark Structured Streaming, Kafka, Databricks |

## Highlights

- Distributed data loading from CSV and Parquet sources.
- Explicit Spark schemas for viewing, demographic, reference, and program data.
- Data cleaning, deduplication, caching, and persistence for repeated analytical workloads.
- Feature engineering for suspicious broadcast detection.
- Aggregation-heavy analytics over households, devices, DMAs, genres, income, net worth, and programs.
- Spark ML workflows for demographic feature extraction, PCA visualization, and KMeans household clustering.
- Streaming analytics over Kafka events with incremental per-batch state.
- Clean GitHub-ready notebooks plus extracted Python scripts for easier review.

## Repository Structure

```text
projects/
  project-1-tv-viewership-analytics/
    notebooks/   Original notebook workflows, renamed and sanitized
    src/         Python scripts extracted from the notebooks
    README.md    Project-specific documentation
  project-2-household-clustering-streaming/
    notebooks/   Databricks notebook workflow, renamed and sanitized
    src/         Python script extracted from the notebook
    README.md    Project-specific documentation
docs/
  CV_PROJECT.md  Resume and LinkedIn-ready wording
requirements.txt
```

## Data Availability

The original datasets are not included in this repository because they were provided through a course Databricks environment. The code expects DBFS paths such as:

```text
dbfs:/mnt/coursedata2024/fwm-stb-data/
dbfs:/FileStore/ddm/10m_viewing
dbfs:/FileStore/ddm/ref_data
dbfs:/FileStore/project_b_data/
```

## Validation

The GitHub workflow validates that notebooks are valid JSON and that the extracted Python scripts compile. Full execution requires the original Databricks data environment.

<div align="center">

![Footer](https://capsule-render.vercel.app/api?type=waving&color=0:00A6ED,50:7C3AED,100:FF3621&height=95&section=footer)

</div>
