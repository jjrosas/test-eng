from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
import logging
import os

# Configuración para evitar errores en macOS
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
os.environ['no_proxy'] = '*'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': Variable.get("ALERT_EMAIL", "admin@example.com"),
    'email_on_retry': True
}

def get_previous_month_range(execution_date):
    """Obtiene el rango de fechas del mes anterior"""
    first_day = (execution_date.replace(day=1) - relativedelta(months=1)).date()
    last_day = (execution_date.replace(day=1) - timedelta(days=1)).date()
    return first_day, last_day

def on_failure_callback(context):
    """Maneja fallos en las tareas"""
    task_instance = context['task_instance']
    error_message = f"Task {task_instance.task_id} failed. Error: {context.get('exception')}"
    logging.error(error_message)

def extract_from_mysql(**context):
    """
    Extrae datos de MySQL para el mes anterior
    """
    try:
        execution_date = context['execution_date']
        start_date, end_date = get_previous_month_range(execution_date)
        
        mysql_hook = MySqlHook(mysql_conn_id='mysql_employees')
        tables = ['employees', 'departments', 'dept_emp', 'dept_manager', 'salaries', 'titles']
        data = {}
        
        for table in tables:
            # Filtramos por fechas para tablas que tienen campos de fecha
            if table in ['dept_emp', 'salaries', 'titles']:
                query = f"""
                SELECT * FROM {table} 
                WHERE from_date BETWEEN '{start_date}' AND '{end_date}'
                """
            else:
                query = f"SELECT * FROM {table}"
            
            df = mysql_hook.get_pandas_df(query)
            data[table] = df
        
        context['ti'].xcom_push(key='raw_data', value=data)
        context['ti'].xcom_push(
            key='date_range', 
            value={'start_date': str(start_date), 'end_date': str(end_date)}
        )
        
        logging.info(f"Datos extraídos para el período {start_date} a {end_date}")
    
    except Exception as e:
        logging.error(f"Error en extract_from_mysql: {str(e)}")
        raise AirflowException(f"Error en extract_from_mysql: {str(e)}")

def transform_data(**context):
    """
    Procesa los datos y genera el reporte de cambios del mes anterior
    """
    try:
        raw_data = context['ti'].xcom_pull(key='raw_data')
        date_range = context['ti'].xcom_pull(key='date_range')
        
        spark = SparkSession.builder \
            .appName("AirflowEmployeeChanges") \
            .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
            .getOrCreate()
        
        spark_data = {table: spark.createDataFrame(df) for table, df in raw_data.items()}
        
        # 1. Cambios de departamento
        dept_window = Window.partitionBy("emp_no").orderBy("from_date")
        dept_changes = (
            spark_data['dept_emp']
            .withColumn("prev_dept", F.lag("dept_no").over(dept_window))
            .filter(F.col("prev_dept").isNotNull())
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
            .filter(F.col("prev_salary").isNotNull())
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
            .filter(F.col("prev_title").isNotNull())
            .select(
                "emp_no",
                F.lit("title_change").alias("change_type"),
                F.col("prev_title").alias("old_value"),
                F.col("title").alias("new_value"),
                F.col("from_date").alias("change_date")
            )
        )

        # Reporte final con todos los cambios
        report_df = (
            dept_changes
            .union(salary_changes)
            .union(title_changes)
            .join(spark_data['employees'], "emp_no")
            .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
            .select("emp_no", "full_name", "change_type", "old_value", "new_value", "change_date")
            .orderBy("change_date", "emp_no")
            .toPandas()
        )
        
        context['ti'].xcom_push(key='report_data', value=report_df.to_csv(index=False))
        context['ti'].xcom_push(key='report_period')
    
    except Exception as e:
        logging.error(f"Error en transform_data: {str(e)}")
        raise AirflowException(f"Error en transform_data: {str(e)}")
    
    finally:
        if 'spark' in locals():
            spark.stop()

def upload_to_s3(**context):
    """
    Sube el reporte a S3 con nombre basado en el período
    """
    try:
        csv_data = context['ti'].xcom_pull(key='report_data')
        date_range = context['ti'].xcom_pull(key='date_range')
        report_period = context['ti'].xcom_pull(key='report_period')
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        # Nombre del archivo basado en el período del reporte
        file_name = f"reports/employee_changes_{date_range['start']}_to_{date_range['end']}.csv"
        
        s3_hook.load_string(
            string_data=csv_data,
            key=file_name,
            bucket_name=Variable.get("S3_BUCKET", "mi-bucket-pruebas-airflow"),
            replace=True
        )
        
        logging.info(f"{report_period} subido exitosamente a S3")
        context['ti'].xcom_push(key='s3_path', value=f"s3://{Variable.get('S3_BUCKET')}/{file_name}")
    
    except Exception as e:
        logging.error(f"Error en upload_to_s3: {str(e)}")
        raise AirflowException(f"Error en upload_to_s3: {str(e)}")

with DAG(
    'monthly_employee_changes_report',
    default_args=default_args,
    schedule_interval='0 3 1 * *',  # Ejecuta el día 1 de cada mes a las 3 AM
    catchup=False,
    tags=['reporting', 'monthly'],
    max_active_runs=1,
    on_failure_callback=on_failure_callback
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_from_mysql',
        python_callable=extract_from_mysql,
        on_failure_callback=on_failure_callback
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        on_failure_callback=on_failure_callback
    )
    
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        on_failure_callback=on_failure_callback
    )
    
    send_success_email = EmailOperator(
        task_id='send_success_email',
        to=Variable.get("REPORT_RECIPIENTS", "jjrosasmoreno@gmail.com"),
        subject="Reporte Mensual de Cambios - {{ ds }}",
        html_content="""
        <h1>Reporte Mensual de Cambios de Empleados</h1>
        <p>El reporte de cambios para {{ task_instance.xcom_pull(task_ids='transform_data', key='report_period') }} ha sido generado exitosamente.</p>
        <p>Ubicación del reporte: {{ task_instance.xcom_pull(task_ids='upload_to_s3', key='s3_path') }}</p>
        <p>Este es un mensaje automático, por favor no responder.</p>
        """,
        trigger_rule='all_success'
    )
    
    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to=Variable.get("ALERT_EMAIL", "admin@example.com"),
        subject="FALLA en Reporte Mensual de Cambios - {{ ds }}",
        html_content="""
        <h1>Falla en Generación de Reporte Mensual</h1>
        <p>El reporte de cambios para {{ ds }} no pudo ser generado.</p>
        <p>Por favor revisar los logs de Airflow para más detalles.</p>
        """,
        trigger_rule='one_failed'
    )
    
    # Flujo del DAG
    extract_task >> transform_task >> upload_task >> [send_success_email, send_failure_email]