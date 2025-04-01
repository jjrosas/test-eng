from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging
import os

# Solución para el error de fork en macOS
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
os.environ['no_proxy'] = '*'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def load_and_clean(**context):
    """
    Carga y limpia datos desde MySQL usando Spark
    """
    from pyspark.sql import SparkSession
    from etl.data_cleaner import DataCleaner
    from etl.data_validator import DataValidator
    
    # Configuración de Spark con parámetros para macOS
    spark = SparkSession.builder \
        .appName("AirflowETL") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    try:
        # 1. Extraer desde MySQL
        mysql_hook = MySqlHook(mysql_conn_id='mysql_employees')
        tables = Variable.get("TABLES", deserialize_json=True)
        raw_data = {}
        
        for table in tables:
            pdf = mysql_hook.get_pandas_df(f"SELECT * FROM {table}")
            raw_data[table] = spark.createDataFrame(pdf)
        
        # 2. Limpieza
        clean_data = DataCleaner.clean_all_tables(raw_data)
        
        # 3. Validación
        for table_name, df in clean_data.items():
            DataValidator.validate_table(df, table_name)
        
        # Guardar datos limpios
        context['ti'].xcom_push(key='clean_data', value={k: v.toPandas() for k, v in clean_data.items()})
    
    finally:
        spark.stop()

def generate_changes_report(**context):
    """
    Genera el reporte de cambios usando datos limpios
    """
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.window import Window
    
    # Configuración segura para Spark en macOS
    spark = SparkSession.builder \
        .appName("AirflowChangesReport") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    try:
        clean_data = context['ti'].xcom_pull(key='clean_data')
        spark_data = {table: spark.createDataFrame(df) for table, df in clean_data.items()}
        
        # Fechas configurables
        start_date = datetime(1990, 1, 1).date()
        end_date = datetime(1999, 12, 31).date()
        
        # 1. Cambios de departamento
        dept_window = Window.partitionBy("emp_no").orderBy("from_date")
        dept_changes = (
            spark_data['dept_emp']
            .withColumn("prev_dept", F.lag("dept_no").over(dept_window))
            .filter(
                (F.col("from_date") >= start_date) & 
                (F.col("from_date") <= end_date) &
                (F.col("prev_dept").isNotNull())
            )
            .join(spark_data['departments'].alias("new"), "dept_no")
            .join(spark_data['departments'].alias("old"), 
                F.col("prev_dept") == F.col("old.dept_no"), "left")
            .select(
                "emp_no",
                F.lit("department_change").alias("change_type"),
                F.col("old.dept_name").alias("old_value"),
                F.col("new.dept_name").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )
        
        # 2. Cambios de salario
        salary_window = Window.partitionBy("emp_no").orderBy("from_date")
        salary_changes = (
            spark_data['salaries']
            .withColumn("prev_salary", F.lag("salary").over(salary_window))
            .filter(
                (F.col("from_date") >= start_date) & 
                (F.col("from_date") <= end_date) &
                (F.col("prev_salary").isNotNull())
            )
            .select(
                "emp_no",
                F.lit("salary_change").alias("change_type"),
                F.col("prev_salary").alias("old_value"),
                F.col("salary").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )
        
        # 3. Cambios de título
        title_changes = (
            spark_data['titles']
            .withColumn("prev_title", F.lag("title").over(Window.partitionBy("emp_no").orderBy("from_date")))
            .filter(
                (F.col("from_date") >= start_date) & 
                (F.col("from_date") <= end_date) &
                (F.col("prev_title").isNotNull())
            )
            .select(
                "emp_no",
                F.lit("title_change").alias("change_type"),
                F.col("prev_title").alias("old_value"),
                F.col("title").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )
        
        # Reporte final
        report_df = (
            dept_changes
            .union(salary_changes)
            .union(title_changes)
            .join(spark_data['employees'], "emp_no")
            .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
            .select(
                "emp_no", 
                "full_name", 
                "change_type", 
                "old_value", 
                "new_value",
                "change_date"
            )
            .orderBy("change_date", "emp_no")
        )
        
        context['ti'].xcom_push(key='changes_report', value=report_df.toPandas().to_csv(index=False))
    
    finally:
        spark.stop()

def upload_to_s3(**context):
    """Sube el reporte a S3"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    csv_data = context['ti'].xcom_pull(key='changes_report')
    
    file_name = f"employee_changes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    s3_hook.load_string(
        string_data=csv_data,
        key=f"reports/{file_name}",
        bucket_name=Variable.get("S3_BUCKET"),
        replace=True
    )
    logging.info(f"Reporte subido a S3: {file_name}")

with DAG(
    dag_id="employee_changes_report",
    default_args=default_args,
    schedule='@monthly',
    catchup=False,
    tags=['reporting']
) as dag:
    
    load_clean_task = PythonOperator(
        task_id='load_and_clean_data',
        python_callable=load_and_clean
    )
    
    report_task = PythonOperator(
        task_id='generate_changes_report',
        python_callable=generate_changes_report
    )
    
    upload_task = PythonOperator(
        task_id='upload_report_to_s3',
        python_callable=upload_to_s3
    )
    
    load_clean_task >> report_task >> upload_task