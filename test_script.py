import chispa
from pyspark.sql import SparkSession
from client_data_processing import filter_data, remove_personal_info, remove_credit_card, rename_columns
def test_filter_data():
    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [("Alice", "US"), ("Bob", "CA"), ("Charlie", "UK")]
    columns = ["first_name", "country"]
    df = spark.createDataFrame(data, columns)

    result_df = filter_data(df, "country", ["US", "CA"])

    expected_data = [("Alice", "US"), ("Bob", "CA")]
    expected_df = spark.createDataFrame(expected_data, columns)

    chispa.assert_df_equality(result_df, expected_df)

def test_remove_personal_info():
    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [("Alice", "Smith", "US"), ("Bob", "Johnson", "CA")]
    columns = ["first_name", "last_name", "country"]
    df = spark.createDataFrame(data, columns)

    result_df = remove_personal_info(df)

    expected_data = [("US",), ("CA",)]
    expected_columns = ["country"]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    chispa.assert_df_equality(result_df, expected_df)

def test_remove_credit_card():
    # Create test data and expected result
    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [("1", "1234-5678-9012-3456")]
    columns = ["id", "cc_n"]
    df = spark.createDataFrame(data, columns)

    expected_data = [("1",)]
    expected_columns = ["id"]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Apply the remove_credit_card function
    result_df = remove_credit_card(df)

    # Assert the result using chispa
    chispa.assert_df_equality(result_df, expected_df)

def test_rename_columns():
    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [("1", "addr1", "visa")]
    columns = ["id", "btc_a", "cc_t"]
    df = spark.createDataFrame(data, columns)

    column_mapping = {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}

    expected_data = [("1", "addr1", "visa")]
    expected_columns = ["client_identifier", "bitcoin_address", "credit_card_type"]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    result_df = rename_columns(df, column_mapping)

    chispa.assert_df_equality(result_df, expected_df)


if __name__ == "__main__":
    # Run the tests
    test_filter_data()
    test_remove_personal_info()
    test_remove_credit_card()
    test_rename_columns()

