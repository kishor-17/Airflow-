from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("stock_view").getOrCreate()
my_file = Dataset("./DAGS/tmp/stockdata.csv")

def Reading_stock_data():
    df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(my_file.uri)
    print(df.show(5))

with DAG('stock_view', 
        start_date=datetime(2023, 10,5), 
        schedule=[my_file],
        catchup=False,
        description='Triggerd from pyspark_example.py',
        tags=['Value updation', 'Stocks'],
        ) as dag:
    
    reading_stock_data = PythonOperator(
        task_id="reading_stock_data",
        python_callable=Reading_stock_data,
    )
    reading_stock_data