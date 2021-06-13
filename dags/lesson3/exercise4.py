import datetime

from airflow import DAG

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)

#
#The following DAF performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

#
# The following code loads trips data from S3 to RedShift. Use the s3_key
#       "data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
#       and the s3_bucket "udacity-dend"
#
copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    #s3_key="data-pipelines/divvy/unpartitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv"
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

#Perform a data quality check on the Trips table

check_trips = HasRowsOperator(
    task_id='check_trips_data',
    dag=dag,
    redshift_conn_id="redshift",
    table="trips",
)

#Using the FactsCalculatorOperator to create a Facts table in RedShift
calculate_facts = FactsCalculatorOperator(
                 task_id='calculate_facts_trips',
                 dag=dag,
                 redshift_conn_id="redshift",
                 origin_table="trips",
                 destination_table="fact_trips",
                 fact_column="tripduration",
                 groupby_column="bikeid",

)

#Task ordering for the DAG tasks you defined
copy_trips_task>>check_trips
check_trips>>calculate_facts
