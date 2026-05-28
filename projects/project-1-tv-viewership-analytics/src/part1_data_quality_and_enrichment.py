"""
Project 1 Part 1 - Data quality, enrichment, and broadcast anomaly detection
Extracted from the original Databricks notebook for easier GitHub review.
"""

# %% Cell 1
from pyspark import SparkContext
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("my_project_1").getOrCreate()


# %% Cell 2
from pyspark.sql.types import *
from pyspark.sql.functions import *

# %% Cell 3
# Read a CSV into a dataframe
# There is a smarter version, that will first check if there is a Parquet file and use it
def load_csv_file(filename, schema):
  # Reads the relevant file from distributed file system using the given schema

  allowed_files = {'Daily program data': ('Daily program data', "|"),
                   'demographic': ('demographic', "|")}

  if filename not in allowed_files.keys():
    print(f'You were trying to access unknown file \"{filename}\". Only valid options are {allowed_files.keys()}')
    return None

  filepath = allowed_files[filename][0]
  dataPath = f"dbfs:/mnt/coursedata2024/fwm-stb-data/{filepath}"
  delimiter = allowed_files[filename][1]

  df = spark.read.format("csv")\
    .option("header","false")\
    .option("delimiter",delimiter)\
    .schema(schema)\
    .load(dataPath)
  return df

# This dict holds the correct schemata for easily loading the CSVs
schemas_dict = {'Daily program data':
                  StructType([
                    StructField('prog_code', StringType()),
                    StructField('title', StringType()),
                    StructField('genre', StringType()),
                    StructField('air_date', StringType()),
                    StructField('air_time', StringType()),
                    StructField('Duration', FloatType())
                  ]),
                'viewing':
                  StructType([
                    StructField('device_id', StringType()),
                    StructField('event_date', StringType()),
                    StructField('event_time', IntegerType()),
                    StructField('mso_code', StringType()),
                    StructField('prog_code', StringType()),
                    StructField('station_num', StringType())
                  ]),
                'viewing_full':
                  StructType([
                    StructField('mso_code', StringType()),
                    StructField('device_id', StringType()),
                    StructField('event_date', IntegerType()),
                    StructField('event_time', IntegerType()),
                    StructField('station_num', StringType()),
                    StructField('prog_code', StringType())
                  ]),
                'demographic':
                  StructType([StructField('household_id',StringType()),
                    StructField('household_size',IntegerType()),
                    StructField('num_adults',IntegerType()),
                    StructField('num_generations',IntegerType()),
                    StructField('adult_range',StringType()),
                    StructField('marital_status',StringType()),
                    StructField('race_code',StringType()),
                    StructField('presence_children',StringType()),
                    StructField('num_children',IntegerType()),
                    StructField('age_children',StringType()), #format like range - 'bitwise'
                    StructField('age_range_children',StringType()),
                    StructField('dwelling_type',StringType()),
                    StructField('home_owner_status',StringType()),
                    StructField('length_residence',IntegerType()),
                    StructField('home_market_value',StringType()),
                    StructField('num_vehicles',IntegerType()),
                    StructField('vehicle_make',StringType()),
                    StructField('vehicle_model',StringType()),
                    StructField('vehicle_year',IntegerType()),
                    StructField('net_worth',IntegerType()),
                    StructField('income',StringType()),
                    StructField('gender_individual',StringType()),
                    StructField('age_individual',IntegerType()),
                    StructField('education_highest',StringType()),
                    StructField('occupation_highest',StringType()),
                    StructField('education_1',StringType()),
                    StructField('occupation_1',StringType()),
                    StructField('age_2',IntegerType()),
                    StructField('education_2',StringType()),
                    StructField('occupation_2',StringType()),
                    StructField('age_3',IntegerType()),
                    StructField('education_3',StringType()),
                    StructField('occupation_3',StringType()),
                    StructField('age_4',IntegerType()),
                    StructField('education_4',StringType()),
                    StructField('occupation_4',StringType()),
                    StructField('age_5',IntegerType()),
                    StructField('education_5',StringType()),
                    StructField('occupation_5',StringType()),
                    StructField('polit_party_regist',StringType()),
                    StructField('polit_party_input',StringType()),
                    StructField('household_clusters',StringType()),
                    StructField('insurance_groups',StringType()),
                    StructField('financial_groups',StringType()),
                    StructField('green_living',StringType())
                  ])
}

# %% Cell 4
# Databricks/Jupyter magic: %%time
# demographic data filename is 'demographic'
demo_df = load_csv_file('demographic', schemas_dict['demographic'])
demo_df.count()
demo_df.printSchema()
print(f'demo_df contains {demo_df.count()} records!')
display(demo_df.limit(12))

# %% Cell 5
# Databricks/Jupyter magic: %%time
# daily_program data filename is 'Daily program data'
daily_prog_df = load_csv_file('Daily program data', schemas_dict['Daily program data'])

daily_prog_df.printSchema()
print(f'daily_prog_df contains {daily_prog_df.count()} records!')
display(daily_prog_df.limit(6))

# %% Cell 6
dataPath = "dbfs:/FileStore/ddm/10m_viewing"

viewing10m_df = spark.read.format("csv")\
    .option("header","true")\
    .option("delimiter",",")\
    .schema(schemas_dict['viewing_full'])\
    .load(dataPath)

display(viewing10m_df.limit(6))
print(f'viewing10m_df contains {viewing10m_df.count()} rows!')

# %% Cell 7
# Read the new parquet
ref_data_schema = StructType([
    StructField('device_id', StringType()),
    StructField('dma', StringType()),
    StructField('dma_code', StringType()),
    StructField('household_id', IntegerType()),
    StructField('zipcode', IntegerType())
])

# Reading as a Parquet
dataPath = f"dbfs:/FileStore/ddm/ref_data"
ref_data = spark.read.format('parquet') \
                    .option("inferSchema","true")\
                    .load(dataPath)
                    
display(ref_data.limit(10))
print(f'ref_data contains {ref_data.count()} rows!')

# %% Cell 8
from pyspark.sql.functions import col, when, avg, lit, split, expr, udf
from pyspark.sql.types import BooleanType
from datetime import datetime



#Remove unused columns
demo_df = demo_df.drop(
    "dwelling_type", "length_residence", "home_market_value", "num_vehicles",
    "vehicle_model", "vehicle_year", "net_worth", "gender_individual", "race_code",
    "marital_status", "polit_party_regist", "polit_party_input", "occupation_highest"
)
ref_data = ref_data.drop("dma_code")  
viewing10m_df = viewing10m_df.drop("station_num", "mso_code")

# convert income to numeric values
demo_df = demo_df.withColumn(
    "income_numeric",
    when(col("income") == "A", 10)
    .when(col("income") == "B", 11)
    .when(col("income") == "C", 12)
    .when(col("income") == "D", 13)
    .when(col("income").rlike("^[0-9]+$"), col("income").cast("int"))
    .otherwise(None)
)





# %% Cell 9
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType
from functools import reduce
from pyspark.sql.window import Window
from pyspark import StorageLevel

#First we remove duplicates from the dataframes
daily_prog_df = daily_prog_df.dropDuplicates()
demo_df = demo_df.dropDuplicates()
ref_data = ref_data.dropDuplicates()
viewing10m_df = viewing10m_df.dropDuplicates()

#Condition 1: Duration > Avg , we calculate the avrage and add a columns that is a flag for this condition
avg_duration = daily_prog_df.select(avg("Duration")).first()[0]
daily_df = daily_prog_df.withColumn("cond1", (col("Duration") > avg_duration))

#Condition 4: Programs that Aired on friday the 13th (started or ended in some friday the 13th)
# Create proper datetime columns from air_date and air_time
daily_df = daily_df.withColumn(
    "start_datetime",
    to_timestamp(concat_ws(" ", col("air_date"), col("air_time")), "yyyyMMdd HHmmss")
)
daily_df = daily_df.withColumn("Duration", col("Duration").cast("int"))
# Calculate end datetime by adding duration to start time
daily_df = daily_df.withColumn(
    "end_datetime",
    expr("start_datetime + make_interval(0, 0, 0, 0, 0, 0, Duration * 60)")
)
# Find programs that started on Friday the 13th
friday13_start = daily_df.filter(
    (dayofmonth(col("start_datetime")) == 13) &
    (dayofweek(col("start_datetime")) == 6)
).select("prog_code").distinct()
# Find programs that ended on Friday the 13th (but didn't start on it)
friday13_end = daily_df.filter(
    (dayofmonth(col("start_datetime")) != 13) &
    (dayofmonth(col("end_datetime")) == 13) &
    (dayofweek(col("end_datetime")) == 6)
).select("prog_code").distinct()
# Combine both sets and create condition flag
friday13_prog_codes = friday13_start.union(friday13_end).distinct() \
    .withColumn("aired_on_friday_13th", lit(1))

daily_df = daily_df.join(friday13_prog_codes, on="prog_code", how="left") \
                   .fillna(0, subset=["aired_on_friday_13th"]) \
                   .withColumn("cond4", col("aired_on_friday_13th") == 1) \
                   .drop("start_datetime", "end_datetime", "aired_on_friday_13th")


#Condition 6: we flag programs with suspicious genres and add a flag column for this condition
sus_genres = ['Collectibles', 'Art', 'Snowmobile', 'Public affairs', 'Animated', 'Music']
# Split genres and trim whitespace
daily_df = daily_df.withColumn("genre_array", expr("transform(split(genre, ','), x -> trim(x))"))
# Check if any genre matches the suspicious list
daily_df = daily_df.withColumn("cond6", size(array_intersect(col("genre_array"), array(*[lit(g) for g in sus_genres]))) > 0)


#Condition 7: forbidden words in the title
target_words = ["better", "girls", "the", "call"]
daily_df = daily_df.withColumn("title_words", split(lower(col("title")), " "))
daily_df = daily_df.withColumn("cond7", size(array_intersect(col("title_words"), array(*[lit(w) for w in target_words]))) >= 2)



#Condition 3: devices in houses with 2 adults exactly and between theese 2 adults the age difference is no more then 6
demo_fixed = demo_df.withColumn("household_id_int", col("household_id").cast("int")) \
                    .withColumn("age_diff", abs(col("age_individual") - col("age_2")))
ref_fixed = ref_data.withColumn("household_id_int", col("household_id").cast("int"))
# Find devices belonging to households with 2 adults and small age difference(<=6)
valid_devices = ref_fixed.join(
    demo_fixed.filter((col("num_adults") == 2) & (col("age_diff") <= 6)), "household_id_int"
).select("device_id").distinct()
# Get program codes viewed by these devices
viewed_prog_codes = viewing10m_df.join(valid_devices, "device_id").select("prog_code").distinct()
daily_df = daily_df.join(viewed_prog_codes.withColumn("cond3", lit(True)), "prog_code", "left").fillna({'cond3': False})



#Condition 5: low income households(bewlow average) and more than 3 devices
avg_income = demo_fixed.select(avg("income_numeric")).first()[0]
device_counts = ref_fixed.groupBy("household_id_int").agg(count("device_id").alias("num_devices"))
# Find households with below-average income and more than 3 devices
low_income_households = demo_fixed.join(device_counts, "household_id_int") \
                                  .filter((col("num_devices") > 3) & (col("income_numeric") < avg_income)) \
                                  .select("household_id_int")
# Get devices and program codes for this condition
cond5_devices = ref_fixed.join(low_income_households, "household_id_int").select("device_id").distinct()
cond5_prog_codes = viewing10m_df.join(cond5_devices, "device_id").select("prog_code").distinct()

daily_df = daily_df.join(cond5_prog_codes.withColumn("cond5", lit(True)), "prog_code", "left").fillna({'cond5': False})


#Condition 2: flag programs viewed by households that own Toyota vehicles
toyota_devices = ref_fixed.join(
    demo_fixed.filter(col("vehicle_make") == "91"), "household_id_int"
).select("device_id").distinct()

cond2_prog_codes = viewing10m_df.join(toyota_devices, "device_id").select("prog_code").distinct()
daily_df = daily_df.join(cond2_prog_codes.withColumn("cond2", lit(True)), "prog_code", "left").fillna({'cond2': False})

# Cache the dataframe in memory and disk for reuse
daily_df = daily_df.persist(StorageLevel.MEMORY_AND_DISK)


# %% Cell 10
from pyspark.sql.functions import col, lit, count, when, expr, sum as spark_sum

# Calculate the total number of suspicious conditions met for each program 
condition_expr = " + ".join([f"int({c})" for c in ["cond1", "cond2", "cond3", "cond4", "cond5", "cond6", "cond7"]])
daily_df = daily_df.withColumn("num_true", expr(condition_expr))

# Flag broadcasts as malicious if they meet 4 or more suspicious conditions
daily_df = daily_df.withColumn("is_malicious", col("num_true") >= 4)

# Calculate maliciouss ratio for each program title across all broadcasts
title_stats = daily_df.groupBy("title").agg(
    count("*").alias("total_broadcasts"),
    spark_sum(when(col("is_malicious"), 1).otherwise(0)).alias("malicious_broadcasts")
).withColumn("malicious_ratio", col("malicious_broadcasts") / col("total_broadcasts"))

# Filter titles with high maliciouss ratio (>40% of broadcasts flagged)
malicious_titles = title_stats.filter(col("malicious_ratio") > 0.4)
# Display result
print(f"Number of titles identified as malicious: {malicious_titles.count():,}")
display(malicious_titles.orderBy(col("malicious_ratio").desc()).limit(20))

# Remove cached dataframe from memory to free up cluster resources
daily_df.unpersist()


