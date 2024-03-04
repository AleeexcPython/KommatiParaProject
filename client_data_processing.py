import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Function to filter DataFrame based on specified countries
def filter_countries(df, countries_column, countries):
    return df.filter(col(countries_column).isin(countries))


# Function to remove personal information columns from the DataFrame
def remove_personal_info(df):
    return df.drop("first_name", "last_name")


# Function to remove credit card information column from the DataFrame
def remove_credit_card(df):
    return df.drop("cc_n")


# Function to rename DataFrame columns based on provided mapping
def rename_columns(df, column_mapping):
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df


# Main function to process datasets, filter, join, and perform transformations
def process_datasets(dataset_one_path, dataset_two_path, countries_to_filter, column_mapping):
    # Create a Spark session
    spark = SparkSession.builder.appName("KommatiParaProject").getOrCreate()

    # Read CSV files into DataFrames
    df1 = spark.read.csv(dataset_one_path, header=True, inferSchema=True)
    df2 = spark.read.csv(dataset_two_path, header=True, inferSchema=True)

    # Assuming the column name for countries is "country"
    countries_column_name = "country"

    # Apply filtering and transformations
    filtered_df1 = filter_countries(df1, countries_column_name, countries_to_filter)
    filtered_df1 = remove_personal_info(filtered_df1)

    filtered_df2 = remove_credit_card(df2)

    # Join DataFrames on 'id' column
    joined_df = filtered_df1.join(filtered_df2, "id")

    # Rename columns based on provided mapping
    joined_df = rename_columns(joined_df, column_mapping)

    # Show the resulting DataFrame
    joined_df.show()

    # Hardcoded output path for writing the result
    output_path = '/Users/alexchiu/PycharmProjects/KommatiParaProject/client_data'
    joined_df.write.mode("overwrite").csv(output_path)


# Entry point for the script
if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided
    if len(sys.argv) != 4:
        print("Usage: python script.py <dataset_one_path> <dataset_two_path> <countries>")
        sys.exit(1)

    # Assign command-line arguments to variables
    dataset_one_path = sys.argv[1]
    dataset_two_path = sys.argv[2]
    countries_to_filter = sys.argv[3].split(', ')

    # Define the column mapping for renaming
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }

    try:
        # Execute the data processing function
        process_datasets(dataset_one_path, dataset_two_path, countries_to_filter, column_mapping)
        logger.info("Data processing completed successfully.")
    except Exception as e:
        # Log an error message if an exception occurs
        logger.error(f"An error occurred: {str(e)}")
