from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FoodInspectionDataProcessing") \
    .getOrCreate()

print("INFO: SparkSession initialized.")

# Define file path (update this to your CSV file's location)
input_file = "food_inspections.csv"
output_file = "processed_food_inspections"

print(f"INFO: Input file path set to {input_file}.")
print(f"INFO: Output directory set to {output_file}.")

# Load the CSV file into a DataFrame
print("INFO: Loading data from CSV file...")
df = spark.read.csv(input_file, header=True, inferSchema=True)
print("INFO: Data loaded successfully.")
print("INFO: Schema of the input data:")
df.printSchema()

# Perform basic transformations
print("INFO: Filtering rows where 'Inspection Score' is greater than 80...")
filtered_df = df.filter(col("Inspection Score") > 80)
print("INFO: Filtering complete. Number of rows after filtering:", filtered_df.count())

print("INFO: Adding column 'Is High Risk Business' based on 'Violation Points'...")
transformed_df = filtered_df.withColumn(
    "Is High Risk Business", 
    (col("Violation Points") > 5).cast("boolean")
)
print("INFO: Column 'Is High Risk Business' added successfully.")

print("INFO: Grouping data by 'City' and counting inspections...")
grouped_df = transformed_df.groupBy("City").agg(
    count(lit(1)).alias("Inspection Count")
)
print("INFO: Grouping and aggregation complete.")

# Display a small sample of data
print("INFO: Sample of transformed data:")
transformed_df.show(5)

print("INFO: Sample of grouped data by city:")
grouped_df.show(5)

# Save the transformed data to Parquet format
print(f"INFO: Saving transformed data to {output_file}/transformed_data in Parquet format...")
transformed_df.write.parquet(f"{output_file}/transformed_data", mode="overwrite")
print(f"INFO: Transformed data saved successfully to {output_file}/transformed_data.")

print(f"INFO: Saving grouped data to {output_file}/grouped_by_city in Parquet format...")
grouped_df.write.parquet(f"{output_file}/grouped_by_city", mode="overwrite")
print(f"INFO: Grouped data saved successfully to {output_file}/grouped_by_city.")

# Stop the SparkSession
print("INFO: Stopping the SparkSession...")
spark.stop()
print("INFO: SparkSession stopped. Job completed successfully.")