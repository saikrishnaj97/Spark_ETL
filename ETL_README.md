**ETL Description**

Python, Spark and Spark SQL are used for the given data engineering task.

Overall, There are three modules/python_files found in the 'Spark_ETL' directory
which are used to perform the all the tasks/functionalities and test them:
1)	caller.py – the file with the main method which contains the code for creation of spark context and method calls to perform individual tasks/functionalities.
2)	tasks.py – the file which contains all the individual tasks/functionalities inside a class which can be accessed by creating an object of this class from caller.py file.
3)  unittesting.py - the file contains the code for unit_testing different functionalities created. 
    (command to execute this file : 'python unittesting.py' )

Commands to execute  locally/on-cluster using python or spark-submit:
Python execution commands: \
- cd saikrishnaj97-data-engineering-test 
- python caller.py input_directory output_path \
  ( Sample : python caller.py "input" "output/output.csv")
  
Spark submit commands if running on a cluster:
- cd saikrishnaj97-data-engineering-test
- spark-submit caller.py input_directory output_path \
  ( Sample : spark-submit caller.py input output/output.csv)

**Detailed code execution work-flow:**
1) Create spark conf and then spark context.
2) Create a logger object.
3) Create an object of the class ‘tasks’ to access the methods in that class from the ‘caller’ class. 

_Extract phase:_ \
4) Call ‘read_data’ method to read data from input path to a json file.
5) Select only the columns that are required further for the next phase to save memory. \
6) drop columns with null values. (replacing the null values with mean of the column would have been better here, but  given the 
   time constraints I was not able to do this).   
7) Persist/cache the dataframe for faster computation later. 

_Transform phase:_ \
8) Call the ‘filter_and_compute’ method to extract only recipes that have beef as one of the ingredients and then calculate and add the total cook time and difficulty columns: 
- To do this firstly we convert all the values in ingredients column to lowercase to not miss out any data .
- From ingredients column, filter out rows that contain beef in them .
- Record the count after the filtering operation to validate later.
- create two user defined functions ‘total_cook_time_mins’ and ‘valueTocategory’ .
- ‘total_cook_time_mins’ is used to get the Total cooking time duration in minutes which can be obtained by adding cooktime and preparation time.
- cook time and preparation time are parsed firstly from ISO format to seconds using the ‘iso8601_as_timedelta’ function(udf) and then passed to ‘total_cook_time_mins’ method to combine them and get back in minutes.
- Then using the ‘valueTocategory’ method, we obtain the level of difficulty using the given criteria .
- Later, we append the total_cook_time_mins and difficulty columns obtained from the previous steps to the dataframe to use them next.
- Now obtain the row count of modified dataframe and validate the data to check if counts are matching. \
   
_Load Phase:_ \
9) Calculate average cooking time duration per difficulty level and persist dataset as CSV to output folder to have 2 columns: difficulty,avg_total_cooking_time.
- This is done by calling the groupbydifficulty_and_savecsv method in the tasks class.
- Using the dataframe obtained in previous step, we group by difficulty and calaculate average_total_cooking_time for each level of difficulty .
- save the obtained dataframe in a csv file in the specified output path.

**Versions Used** \
Java version : 1.8.0_281 (Java 8) \
Python version : 3.7.4 \
Spark version : 2.3.4 

**CI/CD Pipeline** \
As per my understanding the goals of CI/CD pipeline are : 
- Ensure that any changes made to the pipeline will not break any other processes. 
- Ensure that any changes made to the pipeline will not affect the processes or apps of customers.
- Ensure that changes do not degrade performance.

Thinking of these goals my ideal CI/CD pipeline would be to:
- Unit tests - does my logic work as intended? testing individual pieces of functionality.
- Simple integration test - can I successfully run all jobs in my pipeline? For a simple integration test, this could be ran in docker. This would test that the output from one job did not break another.
- Full integration test - can I successfully run my pipeline using the actual tech/hardware that it runs on in prod. Could feed in some static sample data sitting out in your storage and validate the output. 

_Schedule this pipeline to run periodically_ \
To schedule this pipeline we can use a workflow management system like Apache oozie( I was using oozie in my previous role as we had on-premise hadoop clusters) or Apache airflow.


**References** \
https://docs.python.org/3/howto/logging.html#logging-basic-tutorial \
https://spark.apache.org/docs/2.4.0/api/python/index.html \
https://spark.apache.org/docs/latest/api/python/ \
https://sparkbyexamples.com/ \
https://github.com/spark-examples/pyspark-examples \
http://renien.com/blog/accessing-pyspark-pycharm/ \
https://medium.com/@shantanualshi/logging-in-pyspark-36b0bd4dec55 \
https://www.tutorialspoint.com/python/index.htm \
https://www.reddit.com/r/dataengineering/ \
https://medium.com/@aieeshashafique/exploratory-data-analysis-using-pyspark-dataframe-in-python-bd55c02a2852
