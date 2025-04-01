from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def extract_from_mysql(**context):
    """
    Extrae datos de MySQL usando el hook de Airflow.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_employees')
    tables = ['employees', 'departments', 'dept_emp', 'dept_manager', 'salaries', 'titles']
    data = {}
    
    for table in tables:
        df = mysql_hook.get_pandas_df(f"SELECT * FROM {table}")
        data[table] = df
    
    context['ti'].xcom_push(key='raw_data', value=data)

def transform_data(**context):
    """
    Procesa los datos y genera el reporte de cambios entre fechas.
    """
    raw_data = context['ti'].xcom_pull(key='raw_data')
    
    spark = SparkSession.builder \
        .appName("AirflowEmployeeChanges") \
        .getOrCreate()
    
    try:
        spark_data = {table: spark.createDataFrame(df) for table, df in raw_data.items()}
        
        start_date = datetime(1990, 1, 1).date()
        end_date = datetime(1999, 12, 31).date()

        # 1. Cambios de departamento
        dept_window = Window.partitionBy("emp_no").orderBy(F.desc("from_date"))
        dept_changes = (
            spark_data['dept_emp']
            .filter((F.col("from_date") >= start_date) & (F.col("from_date") <= end_date))
            .withColumn("rank", F.row_number().over(dept_window))
            .filter(F.col("rank") == 1)
            .join(spark_data['departments'], "dept_no")
            .select(
                "emp_no",
                F.lit("department_change").alias("change_type"),
                F.col("dept_name").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )

        # 2. Cambios de salario
        salary_changes = (
            spark_data['salaries']
            .filter((F.col("from_date") >= start_date) & (F.col("from_date") <= end_date))
            .withColumn("rank", F.row_number().over(Window.partitionBy("emp_no").orderBy(F.desc("from_date"))))
            .filter(F.col("rank") == 1)
            .select(
                "emp_no",
                F.lit("salary_change").alias("change_type"),
                F.col("salary").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )

        # 3. Cambios de título
        title_changes = (
            spark_data['titles']
            .filter((F.col("from_date") >= start_date) & (F.col("from_date") <= end_date))
            .withColumn("rank", F.row_number().over(Window.partitionBy("emp_no").orderBy(F.desc("from_date"))))
            .filter(F.col("rank") == 1)
            .select(
                "emp_no",
                F.lit("title_change").alias("change_type"),
                F.col("title").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )

        # Unir todos los cambios
        report_df = (
            dept_changes
            .union(salary_changes)
            .union(title_changes)
            .join(spark_data['employees'], "emp_no")
            .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
            .select("emp_no", "full_name", "change_type", "new_value", "change_date")
            .toPandas()
        )
        
        context['ti'].xcom_push(key='report_data', value=report_df.to_csv(index=False))
    
    finally:
        spark.stop()

def upload_to_s3(**context):
    """
    Sube el reporte a S3.
    """
    try:
        csv_data = context['ti'].xcom_pull(key='report_data')
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        file_name = f"reports/employee_changes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        s3_hook.load_string(
            string_data=csv_data,
            key=file_name,
            bucket_name='mi-bucket-pruebas-airflow',
            replace=True
        )
        logging.info(f"Reporte subido exitosamente a S3: {file_name}")
    except Exception as e:
        logging.error(f"Error al subir a S3: {str(e)}")
        raise

with DAG(
    'employee_changes_report',
    default_args=default_args,
    schedule='@monthly',  # Cambiado de schedule_interval a schedule
    catchup=False,
    tags=['reporting']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_from_mysql',
        python_callable=extract_from_mysql
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )