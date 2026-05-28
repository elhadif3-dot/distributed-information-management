# Project 2 - Household Clustering and Streaming Analytics

Distributed analytics project built with PySpark, Spark MLlib, Spark Structured Streaming, Kafka, and Databricks.

This project extends the TV viewing analytics work into machine-learning-based household segmentation and streaming event analysis.

## What This Project Does

Static analysis:

- Loads household demographic data from Parquet.
- Loads static viewing records with explicit Spark schemas.
- Builds a Spark ML feature pipeline for numerical and categorical demographic attributes.
- Normalizes numeric features with `MinMaxScaler`.
- Encodes categorical attributes using `StringIndexer` and `OneHotEncoder`.
- Combines features with `VectorAssembler`.
- Uses PCA to visualize demographic structure in two dimensions.
- Applies KMeans clustering to segment households.
- Computes each household's distance from its assigned cluster centroid.
- Creates full, every-third, and every-seventeenth subsets per cluster.
- Joins clustered households with viewing data to rank stations by cluster-specific popularity lift.

Streaming analysis:

- Reads viewing events from Kafka using Spark Structured Streaming.
- Parses streamed messages into a typed schema.
- Joins streaming records with the representative household subset.
- Processes micro-batches with `foreachBatch`.
- Maintains incremental state for station counts and cluster totals.
- Reports top stations by cluster across streaming batches.

## Files

```text
notebooks/
  household_clustering_and_streaming.ipynb

src/
  household_clustering_and_streaming.py
```

The notebook is the primary Databricks workflow. The Python file is extracted for easier GitHub review.

## Technical Stack

- Python
- PySpark DataFrame API
- Spark MLlib
- Spark Structured Streaming
- Kafka
- Databricks
- Matplotlib

## Key Techniques

- Schema-driven ingestion.
- Spark ML `Pipeline` composition.
- PCA dimensionality reduction.
- KMeans clustering.
- Window functions for ranking representative records per cluster.
- Cluster-based station popularity analysis.
- Streaming micro-batch processing with incremental state.

## Running

This project was designed for Databricks. To run it:

1. Upload `notebooks/household_clustering_and_streaming.ipynb` into Databricks.
2. Make sure the project datasets are available under `dbfs:/FileStore/project_b_data/`.
3. For the streaming section, make sure the Kafka topic and server are reachable from the Databricks cluster.

Full execution requires the original course data and Kafka environment.

## Portfolio Summary

Built a PySpark ML and streaming analytics workflow that clusters households from demographic features, ranks representative subsets, compares station popularity by cluster, and processes Kafka viewing events with Spark Structured Streaming.
