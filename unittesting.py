# https://stackoverflow.com/questions/53217767/py4j-protocol-py4jerror-org-apache-spark-api-python-pythonutils-getencryptionen
# Uncomment this code snippet(next 2 lines) and provide the path directory to find spark on your local system,not required when running on cluster
import findspark
findspark.init("C:/Spark/")

from tasks import *
import pandas as pd
import logging
from pyspark.sql import SparkSession


# function to test iso8601_as_timedelta method
def test_iso8601_as_timedelta(obj, iso, in_str, equal_values):
    out = obj.iso8601_as_timedelta(iso)
    if equal_values:
        assert str(out) == in_str
    else:
        assert str(out) != in_str


# function to test valueToCategory method
def test_valueToCategory(obj, tot_cook_time, in_str, equal_values):
    out = obj.valueToCategory(tot_cook_time)
    if equal_values:
        assert out == in_str
    else:
        assert out != in_str


# function to test total_cook_time_mins method
def test_total_cook_time_mins(obj, prepTime, cookTime, input_val, equal_values):
    out = obj.total_cook_time_mins(prepTime, cookTime)
    if equal_values:
        assert out == input_val
    else:
        assert out != input_val


# function to test filter_and_compute method
def test_filter_and_compute(obj, df, logging):
    output_df = obj.filter_and_compute(df, logging)
    return output_df


if __name__ == "__main__":

    # testing 'iso8601_as_timedelta' function
    test1_obj = tasks()
    test_iso8601_as_timedelta(test1_obj, 'PT20M', '0:20:00', True)
    test_iso8601_as_timedelta(test1_obj, 'PT120M', '2:00:00', True)
    test_iso8601_as_timedelta(test1_obj, 'PT1H30M', '1:30:00', True)
    test_iso8601_as_timedelta(test1_obj, 'PT30M', '0:20:00', False)
    print("'iso8601_as_timedelta' function tested successfully")

    # testing 'valueToCategory' function
    test2_obj = tasks()
    test_valueToCategory(test2_obj, 25, 'easy', True)
    test_valueToCategory(test2_obj, 30, 'easy', False)
    test_valueToCategory(test2_obj, 30, 'medium', True)
    test_valueToCategory(test2_obj, 45, 'medium', True)
    test_valueToCategory(test2_obj, 60, 'medium', True)
    test_valueToCategory(test2_obj, 60, 'hard', False)
    test_valueToCategory(test2_obj, 100, 'hard', True)
    test_valueToCategory(test2_obj, -100, 'Unknown', True)
    print("'valueToCategory' function tested successfully")

    # testing 'valueToCategory' function
    test3_obj = tasks()
    test_total_cook_time_mins(test3_obj, 'PT', 'PT40M', 40, True)
    test_total_cook_time_mins(test3_obj, 'PT40M', 'PT10M', 40, False)
    test_total_cook_time_mins(test3_obj, 'PT10M', 'PT120M', 130, True)
    test_total_cook_time_mins(test3_obj, 'PT1H30M', 'PT30M', 120, True)
    print("'total_cook_time_mins' function tested successfully")

    # testing 'filter_and_compute' method
    data = [{'ingredients': 'beef chicken mutton chillies', 'prepTime': 'PT40M', 'cookTime': 'PT50M'},
            {'ingredients': 'BEEF pineapple coriander', 'prepTime': 'PT50M', 'cookTime': 'PT20M'},
            {'ingredients': 'chicken spices lemon', 'prepTime': 'PT20M', 'cookTime': 'PT40M'}]
    pd_df = pd.DataFrame.from_dict(data, orient='columns')

    # https://stackoverflow.com/questions/52943627/convert-a-pandas-dataframe-to-a-pyspark-dataframe
    # Pandas df to Spark df
    spark_session = SparkSession.builder.appName('unitesting').getOrCreate()
    df_sp = spark_session.createDataFrame(pd_df)
    # printing initial dataframe
    df_sp.show()

    test4_obj = tasks()
    out_df = test_filter_and_compute(test4_obj, df_sp, logging)
    # dataframe after completing the operations
    out_df.show()

    print("'filter_and_compute' function tested successfully")

