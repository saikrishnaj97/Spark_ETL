# Exercise

## Task 1
Using Apache Spark and Python, read, pre-process and persist rows to ensure optimal structure and performance for further processing.  
The source events are located on the `input` folder. 

## Task 2
Using Apache Spark and Python read processed dataset from Task 1 and: 
1. Extract only recipes that have `beef` as one of the ingredients.
2. Calculate average cooking time duration per difficulty level.
3. Persist dataset as CSV to the `output` folder.  
  The dataset should have 2 columns: `difficulty,avg_total_cooking_time`.

Total cooking time duration can be calculated by formula:
```bash
total_cook_time = cookTime + prepTime
```  
Hint: times represent durations in ISO format.

Criteria for levels based on total cook time duration:
- easy - less than 30 mins
- medium - between 30 and 60 mins
- hard - more than 60 mins.

## Deliverables
- A deployable Spark Application written in Python.
- A separate `ETL_README.md` file with a brief explanation of the approach, data exploration and assumptions/considerations. 
- CSV output dataset from Task 2.

## Requirements
- Well structured, object-oriented, documented and maintainable code.
- Robust and resilient code. The application should be able to scale if data volume increases.
- Unit tests for the different components.
- Proper exception handling.
- Logging.
- Documentation.
- Solution is deployable and we can run it (locally and on a cluster) - an iPython notebook is not sufficient.

NOTE: If you are using code in your submission that was not written by you, please be sure to attribute it to it's original author.

## Bonus points
- Config management.
- Data quality checks (like input/output dataset validation).
- How would you implement CI/CD for this application?
- How would you diagnose and tune the application in case of performance problems?
- How would you schedule this pipeline to run periodically?
- We appreciate good combination of Software and Data Engineering.

