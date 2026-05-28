# Project 1 - TV Viewership Analytics with PySpark

Distributed analytics project built in PySpark on Databricks. The project processes TV set-top-box viewing data together with household demographics, program metadata, and device-to-household reference data.

## What This Project Does

Part 1 focuses on data preparation and suspicious broadcast detection:

- Loads structured CSV and Parquet data with explicit Spark schemas.
- Cleans and reduces the working dataframes to relevant columns.
- Joins viewing, reference, demographic, and program datasets.
- Engineers multiple suspicious-broadcast signals.
- Flags titles with a high ratio of suspicious broadcasts.

Part 2 focuses on distributed audience analytics:

- Finds the top viewed genres by household size.
- Identifies top DMAs by device and household reach.
- Analyzes popular programs among households with children.
- Compares family-program popularity against total household viewership.
- Builds a wealth-oriented DMA ranking using income and net worth indicators.

## Files

```text
notebooks/
  part1_data_quality_and_enrichment.ipynb
  part2_audience_analytics_queries.ipynb

src/
  part1_data_quality_and_enrichment.py
  part2_audience_analytics_queries.py
```

The notebooks are the primary runnable artifacts for Databricks. The Python files are extracted versions for easier code review on GitHub.

## Technical Stack

- Python
- PySpark DataFrame API
- Spark SQL functions
- Databricks notebooks
- CSV and Parquet data sources

## Key PySpark Techniques

- `StructType` schemas for reliable ingestion.
- `select`, `dropDuplicates`, `cache`, and `persist` for data preparation.
- Multi-stage joins across program, viewing, reference, and demographic data.
- `withColumn`, `when`, `array_intersect`, `split`, and aggregation functions for feature engineering.
- `groupBy`, `agg`, `countDistinct`, and sorted result windows for analytical ranking.

## Running

This project was designed for Databricks. To run it:

1. Upload the notebooks from `notebooks/` into a Databricks workspace.
2. Make sure the expected course datasets are available in DBFS.
3. Run Part 1 before Part 2 if you want to follow the project flow.

Full execution requires the original course data environment. Without those datasets, the repository still documents the implementation and the extracted scripts can be syntax-checked.

## Portfolio Summary

Built a PySpark analytics pipeline for large-scale TV viewership data, combining program metadata, household demographics, and device reference data to detect suspicious broadcasts and produce audience-segmentation insights across genres, DMAs, households with children, and wealth indicators.
