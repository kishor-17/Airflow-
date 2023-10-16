from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_pyspark_task").getOrCreate()

def Reading_Data(**kwargs):
    # Reading data from csv file
    df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("DAGS/ecommerce.csv")
    
    limited_df = df.limit(100)
    kwargs['ti'].xcom_push(key='read_csv', value=limited_df.toPandas())
    print("Reading_data task ended...")

def Transform_Data(**kwargs):

    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='reading_csv_data', key='read_csv')
    
    if data is None:
        raise ValueError("No data retrieved from task_id : reading_csv_data ")

    # Create Spark DataFrame from Pandas DataFrame
    df = spark.createDataFrame(data)
    print(df.head(5))

    # Transforming data
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    stock_df = df.select("StockCode", "Description", "Quantity", "UnitPrice", "Country", year("InvoiceDate").alias("year"), month("InvoiceDate").alias("month"))
    sales_df = df.withColumn("TotalPrice", round(expr("UnitPrice * Quantity"),2)).select("InvoiceNo", "StockCode", "Quantity", "UnitPrice","InvoiceDate", "CustomerID", "Country", "TotalPrice",year("InvoiceDate").alias("year"), month("InvoiceDate").alias("month"))

    # Pushing data to xcom
    kwargs['ti'].xcom_push(key='stock_df', value=stock_df.toPandas())
    kwargs['ti'].xcom_push(key='sales_df', value=sales_df.toPandas())   

    print("Transform_data task ended...")

def Reading_Stock_Data(**kwargs):
    # Reading sales data from xcom
    ti = kwargs['ti']
    data = ti.xcom_pull(key='stock_df', task_ids='transform_csv_data')

    # Checking if data is None
    if data is None:
        raise ValueError("No data retrieved from task_id : transform_csv_data ")
    stock_df = spark.createDataFrame(data)
    print(stock_df.show(5))

    # Adding file location of store data to xcom
    my_file = Dataset("./DAGS/tmp/stockdata.csv")
    kwargs['ti'].xcom_push(key='my_file', value=my_file)

def Reading_Sales_Data(**kwargs):
    # Reading sales data from xcom
    ti = kwargs['ti']
    data = ti.xcom_pull(key='sales_df', task_ids='transform_csv_data')

    # Checking if data is None
    if data is None:
        raise ValueError("No data retrieved from task_id : transform_csv_data ")
    sales_df = spark.createDataFrame(data)
    print(sales_df.show(5))

def Storing_Sales_Data(**kwargs):

    # Pulling data from xcom
    ti = kwargs['ti']
    data = ti.xcom_pull(key='sales_df', task_ids='transform_csv_data')

    # Store data in partitioned format
    sales_df = spark.createDataFrame(data)
    sales_df.write.format("csv")\
        .option("header","true")\
        .option("mode","overwrite")\
        .option("path","DAGS/partition_by_year_month")\
        .partitionBy("year","month")\
        .save()

    print("Storing_Sales_Data task ended...")

def Sales_Data_Analysis():

    #Total sales in december month
    #Reading sales data from partitioned data
    sales_df = spark.read.format("csv")\
        .option("header","True")\
        .option("inferschema","True")\
        .option("mode","FAILFAST")\
        .load("DAGS/partition_by_year_month/year=2010/month=12")
    sales_df.count()

    total_sales = sales_df.agg(sum("TotalPrice").alias("Total_Sales"))
    print("Total Sales in December month is :", total_sales.show())
    print("Sales_Data_Analysis task ended...")

def Database_Stock_Updation( **kwargs):
    
    ti = kwargs['ti']
    data = ti.xcom_pull(key='stock_df', task_ids='transform_csv_data')
    stock_df = spark.createDataFrame(data)
    print(stock_df.show(5))

    my_file = kwargs['ti'].xcom_pull(key='my_file', task_ids='reading_stock_data')
    print(my_file)

    # Write DataFrame as a single CSV file
    stock_df.write.csv(my_file.uri, header=True, mode="overwrite")

    print("Database_Stock_Updation task ended...")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'email': ['20pw18@psgtech.ac.in'],
    'email_on_retry': True,
    'email_on_failure': True,
    'retry_delay': timedelta(seconds=5),
}

with DAG('pyspark_example', 
        start_date=datetime(2023, 10, 1), 
        schedule=None,
        catchup=False,
        description='Training models',
        tags=['training', 'models'],
        default_args=default_args,
        ) as dag:
    
    reading_csv_data = PythonOperator(
        task_id="reading_csv_data",
        python_callable=Reading_Data,
    )

    transform_csv_data = PythonOperator(
        task_id="transform_csv_data",
        python_callable=Transform_Data,
    )

    reading_stock_data = PythonOperator(
        task_id="reading_stock_data",
        python_callable=Reading_Stock_Data,
    )

    reading_sales_data = PythonOperator(
        task_id="reading_sales_data",
        python_callable=Reading_Sales_Data,
    )

    storing_sales_data = PythonOperator(
        task_id="storing_sales_data",
        python_callable=Storing_Sales_Data,
    )

    sales_data_analysis = PythonOperator(
        task_id="sales_data_analysis",
        python_callable=Sales_Data_Analysis,
    )

    database_stock_updation = PythonOperator(
        task_id="database_stock_updation",
        python_callable=Database_Stock_Updation,
    )

    trigger_stock_view_dag = TriggerDagRunOperator(
        task_id='trigger_stock_view_dag',
        trigger_dag_id="stock_view",
        execution_date="{{ execution_date }}",
    )

    reading_csv_data >> transform_csv_data >> [reading_stock_data, reading_sales_data]
    reading_stock_data >> database_stock_updation >> trigger_stock_view_dag
    reading_sales_data >> storing_sales_data >> sales_data_analysis