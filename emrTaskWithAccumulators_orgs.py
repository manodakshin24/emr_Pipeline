import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform(data_source: str, output_uri: str) -> None:
    with SparkSession.builder.appName("My First Application").getOrCreate() as spark:
        # Initialize an accumulator for counting rows
        row_accumulator = spark.sparkContext.accumulator(0)

        start = time.time()

        # Load CSV file
        df = spark.read.option("header", "true").csv(data_source)

        # Rename specific columns and include all others unchanged
        df = df.select(
            col("Name").alias("Organization"),
            col("Number of employees").alias("Head Count"),
            col("Country").alias("HQ_Country"),
            *[col(c) for c in df.columns if c not in {"Name", "Number of employees", "Country"}]
        )

        # Use an action to trigger processing and count rows with the accumulator
        def update_accumulator(row):
            row_accumulator.add(1)

        # Perform a dummy action to iterate over rows and update the accumulator
        df.foreach(update_accumulator)

        # Log the accumulated row count
        print(f"Number of rows processed: {row_accumulator.value}")

        # Write out the results as a parquet file
        df.write.mode("overwrite").parquet(output_uri)

        end = time.time()
        print("Elapsed time: ", end - start, " seconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source')
    parser.add_argument('--output_uri')
    args = parser.parse_args()
    transform(args.data_source, args.output_uri)