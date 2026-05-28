# Resume / LinkedIn Project Entry

## Distributed TV Viewership Analytics - PySpark and Databricks

**Technologies:** Python, PySpark, Spark SQL, Databricks, Distributed Data Processing

- Built a distributed analytics workflow for TV set-top-box data using PySpark and Databricks.
- Loaded CSV and Parquet datasets with explicit Spark schemas and joined viewing events, program metadata, demographic records, and device-household reference data.
- Engineered suspicious-broadcast indicators and flagged titles based on multiple behavioral and metadata-driven conditions.
- Implemented audience analytics queries for top genres, DMA reach, households with children, and wealth-based demographic segmentation.
- Optimized repeated analysis with dataframe deduplication, caching, persistence, and aggregation-focused transformations.

Short version:

Built a PySpark/Databricks pipeline for distributed TV viewership analytics, including data cleaning, multi-table joins, suspicious-broadcast detection, and audience segmentation by genre, DMA, family households, income, and net worth.

## Household Clustering and Streaming Analytics - PySpark ML and Kafka

**Technologies:** Python, PySpark, Spark MLlib, Spark Structured Streaming, Kafka, Databricks

- Built a PySpark ML pipeline for household demographic feature engineering using scaling, categorical indexing, one-hot encoding, and vector assembly.
- Applied PCA for visual exploration and KMeans clustering to segment households into behavioral/demographic groups.
- Created representative household subsets by ranking distance from cluster centroids and sampling full, every-third, and every-seventeenth households per cluster.
- Joined clustered households with static viewing data to identify top stations by cluster-specific popularity lift.
- Implemented Spark Structured Streaming from Kafka and maintained incremental per-batch state for streaming station analytics.

Short version:

Built a PySpark ML and streaming analytics workflow that clusters households from demographic features, ranks representative subsets, compares station popularity by cluster, and processes Kafka viewing events with Spark Structured Streaming.
