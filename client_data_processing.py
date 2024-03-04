import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def filter_data(df, countries_column, countries):
    """
    Filter DataFrame based on a list of countries.

    Parameters:
    - df (DataFrame): Input DataFrame.
    - countries_column (str): Name of the column containing country information.
    - countries (list): List of country names for filtering.

    Returns:
    - DataFrame: Filtered DataFrame.
    """
    return df.filter(col(countries_column).isin(countries))


def remove_personal_info(df):
    """
    Remove personal information columns from the DataFrame.

    Parameters:
    - df (DataFrame): Input DataFrame.

    Returns:
    - DataFrame: DataFrame with personal information columns removed.
    """
    return df.drop("first_name", "last_name")


def remove_credit_card(df):
    """
    Remove credit card information column from the DataFrame.

    Parameters:
    - df (DataFrame): Input DataFrame.

    Returns:
    - DataFrame: DataFrame with credit card information column removed.
    """
    return df.drop("cc_n")


def rename_columns(df, column_mapping):
    """
    Rename columns in the DataFrame based on the provided mapping.

    Parameters:
    - df (DataFrame): Input DataFrame.
    - column_mapping (dict): Dictionary mapping old column names to new column names.

    Returns:
    - DataFrame: DataFrame with renamed columns.
    """
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df


def process_datasets(dataset_one_path, dataset_two_path, countries_to_filter, column_mapping):
    """
    Process datasets by filtering, removing personal information, and renaming columns.

    Parameters:
    - dataset_one_path (str): Path to the first dataset CSV file.
    - dataset_two_path (str): Path to the second dataset CSV file.
    - countries_to_filter (list): List of countries for filtering.
    - column_mapping (dict): Dictionary mapping old column names to new column names.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("KommatiParaProject").getOrCreate()

    # Read CSV files into DataFrames
    df1 = spark.read.csv(dataset_one_path, header=True, inferSchema=True)
    df2 = spark.read.csv(dataset_two_path, header=True, inferSchema=True)

    # Assuming the column name for countries is "country"
    countries_column_name = "country"

    # Apply filters and transformations
    filtered_df1 = filter_data(df1, countries_column_name, countries_to_filter)
    filtered_df1 = remove_personal_info(filtered_df1)

    filtered_df2 = remove_credit_card(df2)

    # Join DataFrames and rename columns
    joined_df = filtered_df1.join(filtered_df2, "id")
    joined_df = rename_columns(joined_df, column_mapping)

    # Display the processed data
    joined_df.show()

    # Hardcoded output path
    output_path = '/Users/alexchiu/PycharmProjects/KommatiParaProject/client_data'

    # Write the processed data to CSV
    joined_df.write.mode("overwrite").csv(output_path)


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) != 4:
        print("Usage: python script.py <dataset_one_path> <dataset_two_path> <countries>")
        sys.exit(1)

    # Extract command line arguments
    dataset_one_path = sys.argv[1]
    dataset_two_path = sys.argv[2]
    countries_to_filter = sys.argv[3].split(',')

    # Define the column mapping for renaming
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }

    try:
        # Process datasets and handle exceptions
        process_datasets(dataset_one_path, dataset_two_path, countries_to_filter, column_mapping)
        logger.info("Data processing completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
