# Distributed Information Management

Portfolio repository for distributed data processing projects built with PySpark and Databricks.

The current project analyzes large-scale TV set-top-box viewing data, demographic data, program metadata, and reference data. It focuses on distributed ETL, data quality, feature engineering, and analytical queries over multi-table datasets.

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
