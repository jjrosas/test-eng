
## 🚀 Configuración Inicial

1. **Clonar el repositorio**:
   ```bash
   git clone https://github.com/jjrosas/test-eng.git
   cd test-eng

2. **Configurar entorno virtual**
    python -m venv env
    source env/bin/activate 

3. **Instalar dependencias**
    pip install -r requirements.txt

4. **Configurar variables de entorno**
    export MYSQL_HOST=your_mysql_host
    export MYSQL_USER=your_mysql_user
    export MYSQL_PASSWORD=your_mysql_password
    export MYSQL_DB=employees
    export AWS_ACCESS_KEY_ID=your_aws_key
    export AWS_SECRET_ACCESS_KEY=your_aws_secret

## Pipeline de Datos
1. **Extracción y Limpieza de Datos**
    - Carga de tablas desde MySQL a DataFrames de Spark

    - Limpieza de datos:

        Manejo de valores nulos

        Consistencia de tipos de datos: se consideraron placeholders de fecha del tipo '9999-01-01' como curdate()

        Eliminación de duplicados

2. **Análisis de Datos**
    - Cálculo de costos anuales por departamento

    - Identificación de empleados con más de dos cambios de departamento

3. **Generación de Reportes**
    - Reporte Parquet (empleados actuales):

        emp_no

        full_name

        current_department

        current_title

        current_salary

        hire_date

        tenure_years (antigüedad)

    - Reporte CSV (cambios entre fechas):

        Resumen de cambios en empleados, departamentos y salarios

        Estructura personalizada que incluye:

            Tipo de cambio (departamento, salario, manager)

            Fecha de cambio

            Valores anteriores y nuevos

4. **Automatización con Airflow**
El DAG (monthly_employee_changes_report_dag.py) realiza:

    Conexión a MySQL

    Cálculo de cambios entre fechas

    Generación de reportes

    Almacenamiento en S3

    Programación: Primer día de cada mes a las 3:00 AM

    Características del DAG:

    Sistema de logs y alertas por email en fallos

    Retries configurados

    Manejo de dependencias entre tareas

Para ejecutarlo, es necesario:
    1. ejecutar airflow_data/dags/setup_connection.py para crear o actualizar las conexiones a mysql y s3
    2. iniciar los servicios de airflow
        airflow webserver -p 8080 & airflow scheduler
    3. hacer login en http://localhost:8080/
    4. en la solapa de DAGs, buscar el DAG 'monthly_employee_changes_report' y habilitarlo
    

