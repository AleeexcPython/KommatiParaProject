import chispa
from pyspark.sql import SparkSession
from client_data_processing import filter_data, remove_personal_info

#Test filter_data function
def test_filter_data():
    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [("Alice", "US"), ("Bob", "CA"), ("Charlie", "UK")]
    columns = ["first_name", "country"]
    df = spark.createDataFrame(data, columns)

    result_df = filter_data(df, "country", ["US", "CA"])

    expected_data = [("Alice", "US"), ("Bob", "CA")]
    expected_df = spark.createDataFrame(expected_data, columns)

    chispa.assert_df_equality(result_df, expected_df)

#Test remove_personal_info function
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
