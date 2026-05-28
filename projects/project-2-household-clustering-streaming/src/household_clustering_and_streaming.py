"""
Project 2 - Household clustering, viewing analysis, and streaming analytics
Extracted from the original Databricks notebook for easier GitHub review.
"""

# %% Cell 1
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os,time
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("my_project_2").getOrCreate()

# %% Cell 2
demographic_df = spark.read.parquet("dbfs:/FileStore/project_b_data/proj_B_demographic/")
demographic_df.printSchema()
display(demographic_df.limit(10))

# %% Cell 3
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("event_time", StringType(), True),     
    StructField("station_num", IntegerType(), True),
    StructField("prog_code", StringType(), True),
    StructField("household_id", IntegerType(), True)
])

viewing_static_df = spark.read.schema(schema).option("header", True).csv("dbfs:/FileStore/project_b_data/viewing_static_csv/")

viewing_static_df.printSchema()
display(viewing_static_df.limit(10))

# %% Cell 4
#Imports
from pyspark.ml.feature import MinMaxScaler,StringIndexer,OneHotEncoder,VectorAssembler
from pyspark.ml import Pipeline

#remove nulls values
viewing_static_df = viewing_static_df.dropna()
demographic_df = demographic_df.dropna()

#First we define numerical and categorial columns and we exclude household_id
numerical_cols = ['household_size', 'num_adults', 'num_generations', 'length_residence', 
                  'home_market_value', 'net_worth']

categorical_cols = ['marital_status', 'race_code', 'dwelling_type', 'home_owner_status', 
                    'gender_individual', 'education_highest']

#Assemble numerical features to vector and normalize
numerical_assembler = VectorAssembler(inputCols=numerical_cols, outputCol="numerical_vector")
scaler = MinMaxScaler(inputCol="numerical_vector", outputCol="normalized_numerical_vector")

#We create indexes for categorical features
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_indexed",handleInvalid="keep") for col in categorical_cols]
#We one-hot encode categorical features
encoders = [OneHotEncoder(inputCol=f"{col}_indexed" , outputCol= f"{col}_encoded") for col in categorical_cols]

#Assemble encoded categorical features to vector
encoded_cols = [f"{col}_encoded" for col in categorical_cols]
encoded_assembler = VectorAssembler(inputCols=encoded_cols, outputCol="encoded_categorical_vector")

#Now we combine theese 2 vectors
final_assembler = VectorAssembler(inputCols=["normalized_numerical_vector", "encoded_categorical_vector"], outputCol="features")


#Create and fit the pipeline
pipeline_stages = [numerical_assembler,scaler] + indexers + encoders + [encoded_assembler,final_assembler]
pipeline = Pipeline(stages=pipeline_stages)
fitted_pipeline = pipeline.fit(demographic_df)
features_df = fitted_pipeline.transform(demographic_df)


result_df = features_df.withColumnRenamed("prediction", "features")\
    .select("household_id", "features")

display(result_df.limit(7))




# %% Cell 5
from pyspark.ml.feature import PCA
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
from pyspark.ml.functions import vector_to_array

#fit and transform PCA with k = 2 (2 components) 
pca = PCA(k=2, inputCol="features", outputCol="pca_features")
pca_model = pca.fit(result_df)
pca_df = pca_model.transform(result_df)

#transform the PCA vector to array to extract PC1 and PC2
pca_components = pca_df.select(
    col("household_id"),
    vector_to_array(col("pca_features"))[0].alias("pc1"),
    vector_to_array(col("pca_features"))[1].alias("pc2")
)
#show the first 7 rows 
display(pca_components.limit(7))

#converct to Pandas DF in order to plot
pca_data = pca_components.toPandas()
# Extract coordinates
x_coords = pca_data['pc1'].tolist()
y_coords = pca_data['pc2'].tolist()



# %% Cell 6
# Plot with Pink
plt.figure(figsize=(10, 8))
plt.scatter(x_coords, y_coords, c='pink')
plt.xlabel('First Principal Component')
plt.ylabel('Second Principal Component') 
plt.title('PCA Visualization of Household Demographics (k=2)')
plt.grid(True, alpha=0.3)
plt.show()

# %% Cell 7
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import *
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
import math

c = 6 # this is the clusters we observes in the  PCA

#fit and transform
kmeans = KMeans(k=c,seed=3,featuresCol='features', predictionCol= 'cluster')
kmeans_model = kmeans.fit(result_df)
cluster_df = kmeans_model.transform(result_df)

#Get Clusters Centers
cluster_centers = kmeans_model.clusterCenters()
dist_udf = udf(
    lambda v, idx: float(math.sqrt(Vectors.squared_distance(v, cluster_centers[idx]))),
    FloatType()
)

# Add distance column
result_df_k_means = cluster_df.withColumn(
    "distance_from_centroid", 
    dist_udf("features", "cluster")
)


final_df = result_df.join(
    result_df_k_means.select("household_id", "cluster", "distance_from_centroid"),
    on="household_id",
    how="left"
).cache()

#show 7 rows
display(final_df.select("household_id", "cluster", "distance_from_centroid").limit(7))





# %% Cell 8
# Add code cells here
from pyspark.sql.window import Window
from pyspark.sql.functions import *

#partition by cluster and order(rank) by distance
window_spec = Window.partitionBy("cluster").orderBy("distance_from_centroid")
#Add row numbers within each cluster (1st 2nd 3rd....17th , etc)
ranked_df = final_df.withColumn("rank_in_cluster", row_number().over(window_spec))


#create the full subset first
full_subset = ranked_df.withColumn("subset_type", lit("Full"))
#3rd subset
thirds_subset = ranked_df.where((col("rank_in_cluster") % 3 == 0)).withColumn("subset_type", lit("3rds"))
#17th subset
seventeenths_subset = ranked_df.where((col("rank_in_cluster") % 17 == 0)).withColumn("subset_type", lit("17ths"))

seventeenths_subset.cache()
full_subset.cache()
thirds_subset.cache()

# #combine all subsets in to one Dataframe
subset_union_df = full_subset.union(thirds_subset).union(seventeenths_subset)



# %% Cell 9
# Cluster's Viewing Analysis
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#first we join viewing data with the subset data
subset_viewing =subset_union_df.join(viewing_static_df.select('household_id', 'station_num'),
                                      on= 'household_id' ,how = 'inner')

#Count viewing events per station per cluster per subset
subset_station_counts = subset_viewing.groupBy("cluster", "subset_type", "station_num").agg(count('station_num').alias("count"))

#count viewing event per cluster per subset
subset_total_counts = subset_station_counts.groupBy("cluster", "subset_type").agg(sum("count").alias("total_count"))

#calculate popularity ratings for each subset
subset_ratings = subset_station_counts.join(subset_total_counts,  on = ["cluster", "subset_type"], how = 'inner').withColumn("subset_rating", (col("count") / col("total_count")) * 100)

#calculate the genral households ratings per station
#Count per station
general_counts = viewing_static_df.groupBy("station_num").agg(
    count("station_num").alias("cnt")
)

#Calculate total as DataFrame
total_general_df = general_counts.agg(
    sum("cnt").alias("total_cnt")
)  

# Broadcast join 
general_ratings = general_counts.crossJoin(
    broadcast(total_general_df)  
).withColumn(
    "general_rating",
    col("cnt") / col("total_cnt") * 100
).select("station_num", "general_rating")

#compute diff-rank for each station in each cluster in each subset
diff_rank_df = subset_ratings.join(general_ratings, on = "station_num", how = 'left').withColumn("diff_rank", col("subset_rating") - col("general_rating")) 

#select the top 7 stations by diff_rank per cluster per subset
window_top7 = Window.partitionBy("cluster", "subset_type").orderBy(col("diff_rank").desc())
top7 = diff_rank_df.withColumn("rank", row_number().over(window_top7)).where(col("rank") <= 7).select(
    "cluster", 
    "subset_type", 
    "station_num", 
    "diff_rank"
).drop("rank").orderBy(col("cluster"), col("subset_type"), col("diff_rank").desc())

top7.cache()

#display top 7 stations by diff_rank per cluster per subset
clusters = [row.cluster for row in top7.select("cluster").distinct().orderBy("cluster").collect()]

for cluster in sorted(clusters):
    for subset in ['Full', '3rds', '17ths']:
        print(f"Cluster {cluster}  {subset} Subset")
        display(top7.filter((col("cluster") == cluster) & (col("subset_type") == subset))
                .select("station_num","cluster", "subset_type", "diff_rank")
                .orderBy("diff_rank", ascending=False)
                .limit(7))
        






# %% Cell 10
SCHEMA = "device_id STRING, event_date INT, event_time INT, station_num STRING, prog_code STRING, household_id STRING"
kafka_server = "kafka.eastus.cloudapp.azure.com:29092"
topic = "view_data" 
OFFSETS_PER_TRIGGER = 50000

streaming_df = spark.readStream\
                  .format("kafka")\
                  .option("kafka.bootstrap.servers", kafka_server)\
                  .option("subscribe", topic)\
                  .option("startingOffsets", "earliest")\
                  .option("failOnDataLoss",False)\
                  .option("maxOffsetsPerTrigger", OFFSETS_PER_TRIGGER)\
                  .load()\
                  .select(from_csv(decode("value", "US-ASCII"), schema=SCHEMA).alias("value")).select("value.*")


# ########## QUERY EXAMPLE ##########

# station_counts = streaming_df.groupBy("station_num").count()

# count_viewings_per_station_query =station_counts.writeStream\
# .queryName('num_viewing')\
# .format("memory")\
# .outputMode("complete")\
# .start()

# time.sleep(10)

# for i in range(10):
#     print("Batch number: "+str(i))
#     print(count_viewings_per_station_query.status)
#     spark.sql('SELECT * FROM num_viewing ORDER BY count DESC LIMIT 10').show()
#     time.sleep(5)
    
# count_viewings_per_station_query.stop()

# %% Cell 11

from collections import defaultdict
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

#Join streamed df with thirds subset
subset_stream = streaming_df.join(thirds_subset, on="household_id", how="inner")

# INCREMENTAL STATE (running totals over all previous batches, 3rds-only)
subset_counts = defaultdict(lambda: defaultdict(int))  # cluster  station  running count
subset_totals = defaultdict(int)                       # cluster  running total views
overall_counts = defaultdict(int)                      # station  running count across ALL clusters (still 3rds-only)
overall_total = 0                                      # running total views (3rds-only)

#Batch counter
batch_counter = 0
min_required_batches = 3

#Foreach batch function
def process_batch(df, batch_id):
    global overall_total, batch_counter
    
    try:
        batch_counter += 1
        print(f"\n{'='*50}")
        print(f"PROCESSING BATCH {batch_id} (Batch #{batch_counter})")
        print(f"{'='*50}")
        
        if df.rdd.isEmpty():
            print("Empty batch; skipping analysis.")
            return
        
        # Get batch size for monitoring
        batch_size = df.count()
        print(f"Batch size: {batch_size:,} records")
        
        # Compute per-batch aggregates on executors;
        print("Computing batch aggregations...")
        
        batch_cluster_station = (
            df.groupBy("cluster", "station_num")
            .agg(F.count("*").alias("cnt"))
            .collect()
        )
        
        batch_cluster_totals = (
            df.groupBy("cluster")
            .agg(F.count("*").alias("cnt"))
            .collect()
        )
        
        batch_overall = (
            df.groupBy("station_num")
            .agg(F.count("*").alias("cnt"))
            .collect()
        )
        
        batch_total = df.agg(F.count("*").alias("cnt")).collect()[0]["cnt"]
        
        # Update running state (incremental over all previous batches)
        print("Updating incremental state...")
        
        for r in batch_cluster_station:
            subset_counts[r["cluster"]][r["station_num"]] += int(r["cnt"])
            
        for r in batch_cluster_totals:
            subset_totals[r["cluster"]] += int(r["cnt"])
            
        for r in batch_overall:
            overall_counts[r["station_num"]] += int(r["cnt"])
            
        overall_total += int(batch_total)
        
        # Show cumulative statistics
        print(f" Cumulative Statistics (Through Batch {batch_counter}):")
        print(f"   Total Records Processed: {overall_total:,}")
        print(f"   Active Clusters: {len(subset_totals)}")
        print(f"   Unique Stations: {len(overall_counts)}")
        
        # Build result rows from RUNNING state and show top-7 per cluster
        print(f"\nTOP STATIONS BY CLUSTER (Cumulative through Batch {batch_counter}):")
        
        rows = []
        if overall_total == 0:
            print("No data accumulated yet.")
            return
            
        for cl, station_map in subset_counts.items():
            cl_total = subset_totals.get(cl, 0)
            if cl_total == 0:
                continue
                
            for st, sc in station_map.items():
                subset_pct = (sc / cl_total) * 100.0
                global_pct = (overall_counts.get(st, 0) / overall_total) * 100.0
                diff_rank = subset_pct - global_pct
                rows.append(("3rds", cl, st, diff_rank))
        
        if not rows:
            print(" No rows to display yet.")
            return
            
        # Create DataFrame with original schema
        out_df = spark.createDataFrame(rows, 
            ["subset", "cluster", "station_num", "diff_rank"])
        
        w = Window.partitionBy("cluster").orderBy(F.desc("diff_rank"))
        
        result_df = (out_df.withColumn("rank", F.row_number().over(w))
                          .filter("rank <= 7")
                          .orderBy("cluster", "rank"))
        
        # Show results 
        print(f"\n Results:")
        result_df.show(50, truncate=False)
        
        # Additional analysis per batch
        print(f"\nBatch {batch_counter} Analysis:")
        clusters_in_batch = [r["cluster"] for r in batch_cluster_totals]
        print(f"Clusters active in this batch: {sorted(set(clusters_in_batch))}")
        
        # Show progress towards requirement
        if batch_counter >= min_required_batches:
            print(f"Requirement met: Processed {batch_counter} batches (minimum {min_required_batches})")
        else:
            remaining = min_required_batches - batch_counter
            print(f" Progress: {batch_counter}/{min_required_batches} batches completed ({remaining} more needed)")
            
    except Exception as e:
        print(f"Batch {batch_id} failed with error: {e}")
        import traceback
        traceback.print_exc()

# START STREAMING QUERY
print("Starting Spark Streaming Query...")
print(f"Kafka Server: {kafka_server}")
print(f"Max Offsets Per Trigger: {OFFSETS_PER_TRIGGER:,}")

query = subset_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_diff_rank_stream_{int(time.time())}") \
    .trigger(processingTime='10 seconds') \
    .start()

# Wait for at least 3 batches to process
print(f"\n Processing batches (minimum {min_required_batches} required)...")

try:
    # Wait longer to ensure we get at least 3 batches
    query.awaitTermination(120)  
finally:
    query.stop()
    print(f"\n Stream stopped. Total batches processed: {batch_counter}")
    
    if batch_counter >= min_required_batches:
        print("Successfully completed the streaming analysis requirement!")
    else:
        print(f"Warning: Only processed {batch_counter} batches (required: {min_required_batches})")


