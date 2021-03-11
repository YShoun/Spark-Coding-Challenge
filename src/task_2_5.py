#!/usr/bin/env python
# coding: utf-8
from datetime import timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# For this question, we assume we did the previous tasks in a seperate file and 
# the file SparkDataFrame.scala only contains part 2, task 1.
#
# Task 5 and Task 6 are empty tasks since there are no task 5 and 6 in the the questions.


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'SparkDataFrameDAG',
    default_args=default_args,
    description='DAG for 6 Spark Tasks',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    concurrency=4 # limit conccurency to 4
)

# Setting up tasks T1 -> T6

t1 = BashOperator(
    task_id='T1',
    bash_command='scala '+'/main/scala/'+'SparkDataFrame.scala', # run the scala file to load the data
    dag=dag,
)

t2 = SparkSqlOperator(
    task_id="T2"
    sql="SELECT MIN(price) AS min_price, MAX(price) as max_price, COUNT(1) AS row_count FROM Table0", 
    master="local")

t3 = SparkSQLOperator(
    task_id="T3"
    sql="SELECT AVG(bathrooms) AS avg_bathrooms, AVG(bedrooms) as avg_bedrooms " +
        "FROM Table1 " +
        "WHERE price > 5000 AND review_avg_score = 10",
    master="local")

t4 = SparkSQLOperator(
    task_id="T4"
    sql="SELECT accommodates "+
        "FROM Table2 "+
        "WHERE price = (SELECT MIN(price) FROM Table2) AND review_scores_rating = (SELECT MAX(review_scores_rating) FROM Table2)",
    master="local" )

t5 = BashOperator(
    task_id='T5',
    dag=dag
)
t6 = BashOperator(
    task_id='T6',
    dag=dag
)

t1 >> [t2, t3] >> [t4, t5, t6]