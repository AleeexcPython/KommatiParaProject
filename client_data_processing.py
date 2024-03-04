import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def process_datasets(dataset_one_path, dataset_two_path, countries_to_filter):
    # Create or get a Spark session with the application name "KommatiParaProject"
    spark = SparkSession.builder.appName("KommatiParaProject").getOrCreate()

    # Load datasets into Spark DataFrames
    df1 = spark.read.csv(dataset_one_path, header=True, inferSchema=True)
    df2 = spark.read.csv(dataset_two_path, header=True, inferSchema=True)
    print(df1)
    print(countries_to_filter)

    # Filter data based on countries
    countries_to_filter_split = countries_to_filter.split(',')
    filtered_df1 = df1.filter(col("country").isin(countries_to_filter_split))
    print(countries_to_filter_split)

    # Remove personal identifiable information from the first dataset (excluding emails)
    filtered_df1 = filtered_df1.drop("first_name", "last_name")

    # Remove credit card numbers from the second dataset
    filtered_df2 = df2.drop("cc_n")

    # Join the datasets using the id field
    joined_df = filtered_df1.join(filtered_df2, "id")

    # Rename columns for better readability
    joined_df = joined_df.withColumnRenamed("id", "client_identifier") \
        .withColumnRenamed("btc_a", "bitcoin_address") \
        .withColumnRenamed("cc_t", "credit_card_type")

    # Show the processed data (for testing purposes)
    joined_df.show()

    # Save the processed data to the client_data directory
    output_path = '/Users/alexchiu/PycharmProjects/KommatiParaProject/client_data'
    joined_df.write.mode("overwrite").csv(output_path)


if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 4:
        print("Usage: python script.py <dataset_one_path> <dataset_two_path> <countries_to_filter>")
        sys.exit(1)

    # Extract command line arguments
    dataset_one_path = sys.argv[1]
    dataset_two_path = sys.argv[2]
    countries_to_filter = sys.argv[3]

    # Process datasets
    process_datasets(dataset_one_path, dataset_two_path, countries_to_filter)

