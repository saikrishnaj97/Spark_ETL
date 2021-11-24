################################################################
## Author     : Saikrishna Javvadi
## Description: Main file(with main method) which contains the method calls to execute inidvidual tasks
##
## References : Generic References used for the entire assignment can be found on the ETL_README file
##               References used for particular code blocks are along with the snippets of code
###################################################################

# https://stackoverflow.com/questions/53217767/py4j-protocol-py4jerror-org-apache-spark-api-python-pythonutils-getencryptionen
# Uncomment this code snippet(next 2 lines) and provide the path directory to find spark on your local system,not required when running on cluster
#import findspark
#findspark.init("C:/Spark/")

from pyspark.sql.types import StructType, StructField

from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark import StorageLevel
import sys
import os

from tasks import *

if __name__ == "__main__":

    # https://realpython.com/command-line-interfaces-python-argparse/
    if len(sys.argv) > 3:
        print('You have specified too many arguments')
        sys.exit()

    if len(sys.argv) < 3:
        print('You need to specify the path to be listed')
        sys.exit()

    # input data path
    input_dir = sys.argv[1]
    # output data path
    output_path = sys.argv[2]

    if not os.path.isdir(input_dir) or not os.path.isdir(output_path):
        print('The specified input/output directory/path does not exist')
        sys.exit()

    input_path = input_dir + '/*.json'

    # creating a Sparkconf object, setting the appname and assigning a 4 core processor to work individually with
    # 1 gigabyte of heap memory
    conf = SparkConf() \
        .setAppName("HelloFresh_Recipes") \
        .setMaster("local[4]") \
        .set("spark.executor.memory", "4g")

    # Creating object of SparkContext
    sc = SparkContext(conf=conf)
    sc.setLogLevel("INFO")

    # creating logger object
    # https://medium.com/@shantanualshi/logging-in-pyspark-36b0bd4dec55
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("pyspark script logger initialized")

    # Creating object of Spark SQLContext
    sql_c = SQLContext(sc)
    LOGGER.info("Spark SQLContext object successfully created")

    # creating an object of the tasks class
    obj = tasks()

    # Define custom schema
    df_schema = StructType([
        StructField("name", StringType(), True),
        StructField("ingredients", StringType(), True),
        StructField("url", StringType(), True),
        StructField("image", StringType(), True),
        StructField("cookTime", StringType(), True),
        StructField("recipeYield", StringType(), True),
        StructField("datePublished", StringType(), True),
        StructField("prepTime", StringType(), True),
        StructField("description", StringType(), True)
    ])

    recipes_df = obj.read_data(input_path, sql_c, df_schema, LOGGER)

    # printing the schema to have look at the columns and their datatypes
    print("##########Dataframe Schema####################")
    recipes_df.printSchema()
    print("##########Sample records####################")
    # Printing few sample records to check if the data is parsed correctly
    recipes_df.show(10)

    print("##########Sample records after Selecting columns####################")
    # selecting only the required columns for our task
    recipes_df = recipes_df.select("ingredients", "cookTime", "prepTime")

    # Printing few sample records
    recipes_df.show(10)

    # https://sparkbyexamples.com/pyspark/pyspark-drop-rows-with-null-values/
    # dropping rows with null values in any of the columns
    recipes_df.na.drop("any")

    print("##########Counts of Dataframe ####################")
    print((recipes_df.count(), len(recipes_df.columns)))

    # https://towardsdatascience.com/best-practices-for-caching-in-spark-sql-b22fb0f02d34
    # persisting the dataframe for faster computation later
    recipes_df.persist(StorageLevel.MEMORY_AND_DISK)

    final_recipes_df = obj.filter_and_compute(recipes_df, LOGGER)
    final_recipes_df.show()

    LOGGER.info("Successfully calculated and added 'total_cook_time_mins' and 'difficulty' columns to the dataframe")

    obj.groupbydifficulty_and_savecsv(final_recipes_df, output_path, LOGGER)

    # //ending the spark session
    sc.stop()
