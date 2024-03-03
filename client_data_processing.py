import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

def filter_data(df, countries_to_filter):
    return df.filter(col("country").isin(countries_to_filter))

def remove_personal_info(df):
    return df.drop("first_name", "last_name")

def remove_credit_card_info(df):
    return df.drop("cc_n")

def rename_columns(df):
    return df.withColumnRenamed("id", "client_identifier") \
             .withColumnRenamed("btc_a", "bitcoin_address") \
             .withColumnRenamed("cc_t", "credit_card_type")

def main(dataset_one_path, dataset_two_path, countries_to_filter, output_path):
    # Create or get a Spark session with the application name "KommatiParaProject"
    spark = SparkSession.builder.appName("KommatiParaProject").getOrCreate()

    # Load datasets into Spark DataFrames
    df1 = spark.read.csv(dataset_one_path, header=True, inferSchema=True)
    df2 = spark.read.csv(dataset_two_path, header=True, inferSchema=True)

    # Filter data based on countries (United Kingdom or Netherlands)
    filtered_df1 = filter_data(df1, countries_to_filter)

    # Remove personal identifiable information from the first dataset (excluding emails)
    filtered_df1 = remove_personal_info(filtered_df1)

    # Remove credit card numbers from the second dataset
    filtered_df2 = remove_credit_card_info(df2)

    # Join the datasets using the id field
    joined_df = filtered_df1.join(filtered_df2, "id")

    # Rename columns for better readability
    joined_df = rename_columns(joined_df)

    # Show the processed data (for testing purposes)
    joined_df.show()

    # Save the processed data to the client_data directory
    joined_df.write.mode("overwrite").csv(output_path)
    logging.info(f"Processed data saved to: {output_path}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) != 5:
        logging.error("Usage: python script.py <dataset_one_path> <dataset_two_path> <countries_to_filter> <output_path>")
        sys.exit(1)

    dataset_one_path = sys.argv[1]
    dataset_two_path = sys.argv[2]
    countries_to_filter = sys.argv[3].split(',')
    output_path = sys.argv[4]

    main(dataset_one_path, dataset_two_path, countries_to_filter, output_path)
