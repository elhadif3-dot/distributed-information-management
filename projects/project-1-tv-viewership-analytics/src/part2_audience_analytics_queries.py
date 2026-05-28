"""
Project 1 Part 2 - Distributed audience analytics queries
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
display(demo_df.limit(6))

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
                    
display(ref_data.limit(6))
print(f'ref_data contains {ref_data.count()} rows!')

# %% Cell 8
#leave only the used columnds , dedupe , and cache Dataframes
daily_prog_df = daily_prog_df.select('prog_code', 'title', 'genre').dropDuplicates().cache()
viewing10m_df = viewing10m_df.select('device_id','prog_code').dropDuplicates().persist()
ref_data = ref_data.select('device_id', 'dma' ,'household_id').dropDuplicates().cache()
demo_df = demo_df.select('household_id', 'household_size', 'presence_children', 'net_worth', 'income').dropDuplicates().cache()

# %% Cell 9
from pyspark.sql.functions import split, explode, sum as spark_sum, col
spark.conf.set("spark.sql.shuffle.partitions", "400")

print("Query 1: Top 5 Most Popular Genres by Household Size ")

# Join viewing data with reference data to map devices to households 
devices_progs_households = (viewing10m_df.select(col("device_id"), col("prog_code"))
      .join(ref_data.select(
              col("device_id"),
              col("household_id").cast("int")),
          "device_id",
          "inner"
      ))

## Join with demographic data to get household size information
devices_progs_households_sizes = devices_progs_households.join(
          demo_df.select(
              col("household_id").cast("int"),
              col("household_size")
          ),
          "household_id",
          "inner"
      )

# Now we Join to add genre data and calculate popularity
genres_popularity = (devices_progs_households_sizes.join(
          daily_prog_df.select(
              col("prog_code"),
              col("genre").alias("genres")
          ).distinct(), # remove duplicate program records
          "prog_code",
          "inner"
      )
                    
      .dropDuplicates(["household_id", "genres"]) #First dedupe - we remove duplicate household-program combinations to ensure each house is counted only ONCE per program
      .withColumn("genre", explode(split(col("genres"), ","))) #Split generes to handle multi generes programs
      .dropDuplicates(["household_id", "genre"])#2nd dedupe we remove duplicate household-genre combinationn to ensure each house is counted only ONCE per genere
      .groupBy("genre")# now we aggregate to calculate total viewership per genre, group by genre and sum household sizes to get total people who viewed
      .agg(spark_sum("household_size").alias("total_people")) 
)

#Order in decending order and aggrgate the total number of people who viewed the top 5 genres
top_genres = genres_popularity.orderBy(col("total_people").desc()).limit(5) 
total_people = top_genres.agg(spark_sum('total_people')).collect()[0][0]

#Display and print results
display(top_genres)
print(f'Total people who veiwed in the top 5 genres: {total_people:,}')

# %% Cell 10

from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum
spark.conf.set("spark.sql.shuffle.partitions", "400")


print(" Query 2: Top 5 Most Popular DMAs by Device Count")

# Join reference data with demographic data 
dma_household_mapping = ref_data.select(
    col("device_id"),
    col("household_id").cast("int").alias("household_id"), 
    col("dma")).join(demo_df.select(col("household_id").cast("int").alias("household_id")), 
    "household_id", 
    "inner"
)

# Count distinct devices per DMA 
dma_device_counts = dma_household_mapping.groupBy("dma") \
    .agg(countDistinct("device_id").alias("device_count"))


# Get DISTINCT household-DMA combinations to ensure we dosent double count households with multiple devices, then sum household sizes for total population size
dma_household_final = dma_household_mapping.select("household_id", "dma").distinct() \
    .join(demo_df.select(col("household_id").cast("int").alias("household_id"),col("household_size")), 
        "household_id"
    )

#Aggregate household sizes to get total population per DMA
dma_household_final = dma_household_final.groupBy("dma").agg(spark_sum("household_size").alias("total_people"))


# Join device counts with population data and rank by device count(descending)
top5_dmas_by_devices = dma_device_counts.join(dma_household_final, on="dma") \
    .orderBy(col("device_count").desc()).limit(5)

#Get total ppl before we say bye bye to total_people column
total_ppl = top5_dmas_by_devices.agg(spark_sum('total_people')).collect()[0][0]
#Display and print results
display(top5_dmas_by_devices.select("dma", "device_count"))
print(f'Total people in top 5 DMAs by devices: {total_ppl:,}')

# %% Cell 11

# Get viewing data aka which devices watched which programs
viewing_activity = viewing10m_df.select(col("device_id"), col("prog_code")
)

# Filter for households with children
device_household_mapping = (ref_data
    .filter(col("device_id").isNotNull())  # valid device IDs
    .select(
        col("device_id"),
        col("household_id").cast("int")  
    )
)

# Filter for households with children
family_demo = (demo_df
    .filter(col("presence_children") == 'Y') 
    .select(
        col("household_id").cast("int"),  
        col("household_size"),            
        col("presence_children")         
    )
)

# Get program titles and filter nulls
program_df = (daily_prog_df
    .filter(col("title").isNotNull())  # programs have titles
    .select(
        col("prog_code"), 
        col("title")
    )
)

#Join on device id to link viewing activity to households
viewing_with_households = (viewing_activity
    .join(
        device_household_mapping,
        "device_id",  
        "inner"       
    )
)

#Join to get only viewings from houses with childrens
family_viewing_data = (viewing_with_households
    .join(family_demo,
        "household_id",  
        "inner"         
)
)

#Join to get program titles
final_family_view_df = (family_viewing_data
    .join(
        program_df,
        "prog_code", 
        "inner"       
    )
)

# Dedupe household program combonations so each family is counted only once per program to prevent countiong families who watch the same program multiple times/devices
household_programs_distinct_df = (
    final_family_view_df.select(col("household_id"), col("title"), col("household_size")
    )
    .dropDuplicates(["household_id", "title"])  # One count per family per program
)

# Aggregate by program and sum household sizes to get total viewers
top_progs_by_family = (household_programs_distinct_df
    .groupBy(col("title"))
    .agg(spark_sum(col("household_size")).alias("total_viewers"))  
    .orderBy(col("total_viewers").desc())                          # rank by viewers
    .limit(5)                                                    
)


# Display and prints results
display(top_progs_by_family)

# Calculate total viewes across top 5 family programs
total_family_viewers = (top_progs_by_family
    .agg(spark_sum(col("total_viewers")))
    .collect()[0][0]
)
print(f"Total number of people in households with children who viewed top 5 programs: {total_family_viewers:,}")

# %% Cell 12
# Get viewing data aka which devices watched which programs
viewing_activity = viewing10m_df.select(col("device_id"), col("prog_code"))

# Map devices to households
device_household_mapping = (ref_data
    .filter(col("device_id").isNotNull())  # valid device IDs
    .select(
        col("device_id"),
        col("household_id").cast("int")  
    )
)

# Filter for households with children
family_demo = (demo_df
    .filter(col("presence_children") == 'Y') 
    .select(
        col("household_id").cast("int"),  
        col("household_size"),            
        col("presence_children")         
    )
)

# Get ALL household demographic data (for total viewership calculation)
all_demo = (demo_df
    .select(
        col("household_id").cast("int"),  
        col("household_size")
    )
)

# Get program titles and filter nulls
program_df = (daily_prog_df
    .filter(col("title").isNotNull())  # programs have titles
    .select(
        col("prog_code"), 
        col("title")
    )
)

# Find top 5 programs among households with children
# Join viewing activity with households
viewing_with_households = (viewing_activity
    .join(
        device_household_mapping,
        "device_id",  
        "inner"       
    )
)

# Join to get only viewings from houses with children
family_viewing_data = (viewing_with_households
    .join(family_demo,
        "household_id",  
        "inner"         
    )
)

# Join to get program titles for family viewing data
final_family_view_df = (family_viewing_data
    .join(
        program_df,
        "prog_code", 
        "inner"       
    )
)

# Dedupe household program combinations for families with children
household_programs_distinct_df = (
    final_family_view_df.select(col("household_id"), col("title"), col("household_size"))
    .dropDuplicates(["household_id", "title"])  # One count per family per program
)

# Aggregate by program and sum household sizes to get total viewers from families with children
top_progs_by_family = (household_programs_distinct_df
    .groupBy(col("title"))
    .agg(spark_sum(col("household_size")).alias("family_viewers"))  
    .orderBy(col("family_viewers").desc())                          
    .limit(5)                                                    
)

# Display top 5 programs popular among families with children
print("Top 5 programs popular among households with children:")
display(top_progs_by_family)

# Calculate total viewers for these specific programs across ALL households
# Get the titles of the top 5 programs
top_5_titles = [row['title'] for row in top_progs_by_family.collect()]

# Join all viewing data with all households (not just families with children)
all_viewing_data = (viewing_with_households
    .join(
        all_demo,
        "household_id",  
        "inner"         
    )
)

# Join to get program titles for all viewing data
final_all_view_df = (all_viewing_data
    .join(
        program_df,
        "prog_code", 
        "inner"       
    )
)

# Filter for only the top 5 programs and deduplicate
top_programs_all_viewers = (
    final_all_view_df
    .filter(col("title").isin(top_5_titles))
    .select(col("household_id"), col("title"), col("household_size"))
    .dropDuplicates(["household_id", "title"])  # One count per household per program
)

# Calculate total viewers across all households for these top 5 programs
total_all_viewers = (top_programs_all_viewers
    .agg(spark_sum(col("household_size")))
    .collect()[0][0]
)

# Print results
print(f"\nTotal number of people who viewed the top 5 programs (across ALL households): {total_all_viewers:,}")

#show the breakdown by program across all households
print("\nViewership breakdown across all households for these top 5 programs:")
all_viewers_breakdown = (top_programs_all_viewers
    .groupBy(col("title"))
    .agg(spark_sum(col("household_size")).alias("total_viewers_all_households"))
    .orderBy(col("total_viewers_all_households").desc())
)
display(all_viewers_breakdown)

# %% Cell 13
from pyspark.sql.functions import split, explode, trim

#I had some problems some time so i loaded the DF again and converted to int
demo_df = demo_df.withColumn(
    "household_id", 
    col("household_id").cast(IntegerType())  # Auto-removes leading zeros
)

ref_data = ref_data.withColumn(
    "household_id", 
    col("household_id").cast(IntegerType())  # Consistent with demo data
)


print("\n\nPart 2.2: Money and Corruption")
# Convert income using when clause (A=10, B=11, C=12, D=13)
demo_with_income = demo_df.withColumn(
    "income_numeric",
    when(col("income") == "A", 10)
    .when(col("income") == "B", 11)
    .when(col("income") == "C", 12)
    .when(col("income") == "D", 13)
    .otherwise(
        when(col("income").isNotNull() & (col("income") != ""), 
             col("income").cast(IntegerType()))
        .otherwise(None)
    )
)
#Here we didnt filter for nulls because its ok that a house won't have any income or networth, but if for somereason the logic is false we just add the line below 
#.filter(col("income_numeric").isNotNull() & col("net_worth").isNotNull())

#Get maximun values for normalization
max_stats = demo_with_income.agg(
    max("net_worth").alias("max_net_worth"),
    max("income_numeric").alias("max_income")
).collect()[0]

max_net_worth = max_stats["max_net_worth"]
max_income = max_stats["max_income"]

if max_net_worth is None or max_income is None or max_net_worth == 0 or max_income == 0: #for debbuging if somthing is wrong create empty df
    dma_wealth_scores = spark.createDataFrame([], StructType([
        StructField("dma", StringType()),
        StructField("avg_net_worth", DoubleType()),
        StructField("avg_income", DoubleType()),
        StructField("wealth_score", DoubleType())
    ]))
else:
    # Calculate wealth scores per DMA
    dma_wealth_scores = ref_data\
        .filter(col("dma") != "Unknown")\
        .select("household_id", "dma").distinct()\
        .join(
            demo_with_income.select("household_id", "net_worth", "income_numeric"), # Join the DMA data with the demographic data
            "household_id", 
            "inner"
        )\
        .groupBy("dma")\
        .agg( # For each DMA, calculate the average net worth and income.
            avg("net_worth").alias("avg_net_worth"),
            avg("income_numeric").alias("avg_income")
        )\
        .withColumn(
            #Wealth Score:
            # (avg_net_worth / max_net_worth) = Normalized net worth
            # (avg_income / max_income) = Normalized income 
            # We add them to get a combined score
            "wealth_score",
            (col("avg_net_worth") / lit(max_net_worth) + col("avg_income") / lit(max_income))
        )\
        .orderBy(col("wealth_score").desc())\
        .limit(10)

# Display the results of the wealth calculation
print("\nTop 10 Wealthiest DMAs:")
display(dma_wealth_scores)


# Assign Top 11 genres to each wealthy DMA:
# Create exploded genre mapping
prog_genre_exploded = daily_prog_df.select(
    "prog_code",
    explode(split(col("genre"), ",")).alias("genre")
).select("prog_code",trim(col("genre")).alias("genre")  # trim whitespace
).filter(col("genre") != "")  # remove empty genres

# Cache the exploded genres for faster results
prog_genre_exploded.cache()

# Create device to household mapping 
device_household_mapping = ref_data.select("device_id", "household_id").distinct()

# Join all the necessary data together to find out which genres are popular in which DMAs
# Calculate genre popularity per DMA using exploded genres
viewing_genre_dma = viewing10m_df.join(
    broadcast(prog_genre_exploded),  # Use exploded genres
    "prog_code",
    "inner"
).join(
    broadcast(device_household_mapping.join(
        ref_data_fixed.select("household_id", "dma").distinct(),
        on="household_id"
    )),
    "device_id",
    "inner"
).filter(col("dma") != "Unknown")

# Group by DMA and genre and count 
genre_popularity_by_dma = viewing_genre_dma\
    .groupBy("dma", "genre")\
    .count()\
    .withColumnRenamed("count", "popularity")

# Define result schema as we are told
result_schema = StructType([
    StructField("DMA NAME", StringType()),
    StructField("WEALTH SCORE", DoubleType()),
    StructField("ORDERED LIST OF GENRES", ArrayType(StringType()))
])

# Get the ordered list of wealthy DMAs
wealthy_dmas_ordered = dma_wealth_scores.collect()

#If we get empty results prints so we now it
if not wealthy_dmas_ordered:
    print("No wealthy DMAs found!")
    result_df = spark.createDataFrame([], result_schema)
else:
    # Process each DMA in order
    result_data = []
    used_genres = set()
    
    for idx, dma_row in enumerate(wealthy_dmas_ordered):
        dma_name = dma_row["dma"]
        wealth_score = dma_row["wealth_score"]
        # Get all genres for this DMA ordered by popularity
        dma_genres = genre_popularity_by_dma\
            .filter(col("dma") == dma_name)\
            .orderBy(col("popularity").desc())\
            .select("genre", "popularity").collect()
        
        # Filter out used genres and take up to 11
        available_genres = []
        for genre_row in dma_genres:
            genre = genre_row["genre"]
            if genre not in used_genres:
                available_genres.append(genre)
                used_genres.add(genre)
                if len(available_genres) >= 11:
                    break
        
       
        
        result_data.append((dma_name, wealth_score, available_genres))
    
    # Create result DataFrame
    result_df = spark.createDataFrame(result_data, result_schema)

print("\n\nTop 10 Wealthiest DMAs with Their Assigned Genres:")
display(result_df)

