################################################################
## Author : Saikrishna Javvadi
## Description : the file which contains all the individual tasks/functionalities inside a class
##                which can be accessed by creating an object of this class from caller.py file.
## References : Generic References used for the entire assignment can be found on the ETL_README file
##               References used for particular code blocks are along with the snippets of code
###################################################################


import isodate
import datetime
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lower, udf


class tasks:

    def read_data(self, input_path, sql_c,df_schema, LOGGER):
        LOGGER.info("reading json files from input directory to dataframe")
        # https://sparkbyexamples.com/pyspark/pyspark-read-json-file-into-dataframe/
        # reading JSON files into dataframe using Spark SQL
        recipes_df = sql_c.read.schema(df_schema).json(input_path)
        LOGGER.info("JSON files successfully read into dataframe from the input path")
        return recipes_df

    def filter_and_compute(self, recipes_df, LOGGER):

        LOGGER.info("Converting the values of ingredients column to lowercase")
        # https://www.titanwolf.org/Network/q/a57b77a0-d9ee-4d27-8740-42c0731bfbcb/y
        recipes_df = recipes_df.withColumn("ingredients", lower(col("ingredients")))
        LOGGER.info("Successfully Converted the values of ingredients column to lowercase")

        LOGGER.info("Filtering out ingredients that have beef ")
        beef_recipes_df = recipes_df.filter(recipes_df["ingredients"].contains("beef"))
        LOGGER.info("Successfully filtered out ingredients having beef")
        # beef_recipes_df.show()

        # initial count of the rows in df after filtering
        initial_count = beef_recipes_df.count()

        # print(beef_recipes_df.select("ingredients").rdd.flatMap(list).collect())

        LOGGER.info("Calculating and Adding two new columns 'total_cook_time_mins' and 'difficulty' to the dataframe")
        # https://stackoverflow.com/questions/33681487/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark
        udf_ValueToCategory = udf(self.valueToCategory, StringType())
        udf_total_cook_time_mins = udf(self.total_cook_time_mins, StringType())
        final_recipes_df = beef_recipes_df.withColumn("total_cook_time_mins",
                                                      udf_total_cook_time_mins("prepTime", "cookTime")) \
            .withColumn("difficulty", udf_ValueToCategory("total_cook_time_mins"))

        # final count of the rows in df after filtering
        final_count = final_recipes_df.count()

        #https://stackoverflow.com/questions/1569049/making-pythons-assert-throw-an-exception-that-i-choose
        try:
            assert initial_count == final_count, "Initial counts after filtering the data and and final counts after adding columns are matching"
        except AssertionError as e:
            raise Exception(e.args)

        LOGGER.info("Successfully calculated and added 'total_cook_time_mins' and 'difficulty' columns to the dataframe")

        return final_recipes_df

    # https://www.programcreek.com/python/example/88806/isodate.parse_duration
    # function to convert an ISO 8601 string to a timedelta
    def iso8601_as_timedelta(self, iso):
        # TODO: Decide what to do with months or years.
        try:
            duration = isodate.parse_duration(iso)
        except isodate.isoerror.ISO8601Error:
            raise ValueError("Invalid ISO duration")
        if type(duration) != datetime.timedelta:
            raise ValueError("Cannot support months or years")
        return duration

    def total_cook_time_mins(self, prepTime, cookTime):
        total_cook_time_secs = self.iso8601_as_timedelta(prepTime).total_seconds() \
                               + self.iso8601_as_timedelta(cookTime).total_seconds()
        total_cook_time_mins = total_cook_time_secs / 60.0
        return total_cook_time_mins

    def valueToCategory(self, total_cook_time_mins):
        if 0 <= total_cook_time_mins < 30:
            return 'easy'
        elif 30 <= total_cook_time_mins <= 60:
            return 'medium'
        elif total_cook_time_mins > 60:
            return 'hard'
        else:
            return 'Unknown'

    def groupbydifficulty_and_savecsv(self, df, output_path, LOGGER):

        LOGGER.info("Grouping by difficulty and calculating average")
        # https://stackoverflow.com/questions/51632126/pysparkhow-to-calculate-avg-and-count-in-a-single-groupby
        final_df = df.groupBy("difficulty").agg({'total_cook_time_mins': 'avg'})
        LOGGER.info("Successfully created new dataframe grouping by difficulty")

        LOGGER.info("Writing the final dataframe into output path")
        # https://stackoverflow.com/questions/31674530/write-single-csv-file-using-spark-csv
        # write to a CSV file
        final_df \
            .repartition(1) \
            .write.format("com.databricks.spark.csv") \
            .option("header", "true") \
            .mode('overwrite') \
            .save(output_path)

        LOGGER.info("Successfully written dataframe to csv file in output path")
