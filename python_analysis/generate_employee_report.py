from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def load_clean_data(spark, base_path="data/clean"):
    """Carga los DataFrames limpios desde el directorio donde save_clean_data los guarda."""
    return {
        "employees": spark.read.parquet(f"{base_path}/employees"),
        "departments": spark.read.parquet(f"{base_path}/departments"),
        "dept_emp": spark.read.parquet(f"{base_path}/dept_emp"),
        "salaries": spark.read.parquet(f"{base_path}/salaries"),
        "titles": spark.read.parquet(f"{base_path}/titles")
    }

def generate_employee_report(clean_data):
    """
    Genera un informe con:
    - emp_no
    - full_name (first_name + last_name)
    - current_department
    - current_title
    - current_salary
    - hire_date
    - tenure_years (antigüedad en años)
    """
    # 1. Departamento actual de cada empleado (el más reciente por to_date)
    current_dept = (
        clean_data["dept_emp"]
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("emp_no").orderBy(F.col("to_date").desc())
        ))
        .filter(F.col("rn") == 1)
        .join(clean_data["departments"], "dept_no", "left")
        .select("emp_no", F.col("dept_name").alias("current_department"))
    )

    # 2. Título actual de cada empleado (el más reciente por to_date)
    current_title = (
        clean_data["titles"]
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("emp_no").orderBy(F.col("to_date").desc())
        ))
        .filter(F.col("rn") == 1)
        .select("emp_no", F.col("title").alias("current_title"))
    )

    # 3. Salario actual de cada empleado (el más reciente por to_date)
    current_salary = (
        clean_data["salaries"]
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("emp_no").orderBy(F.col("to_date").desc())
        ))
        .filter(F.col("rn") == 1)
        .select("emp_no", F.col("salary").alias("current_salary"))
    )

    # 4. Construir el informe final
    report = (
        clean_data["employees"]
        .join(current_dept, "emp_no", "left")
        .join(current_title, "emp_no", "left")
        .join(current_salary, "emp_no", "left")
        .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
        .withColumn("tenure_years", F.round(
            F.datediff(F.current_date(), F.col("hire_date")) / 365, 2
        ))
        .select(
            "emp_no",
            "full_name",
            "current_department",
            "current_title",
            "current_salary",
            "hire_date",
            "tenure_years"
        )
    )
    return report

def save_report(report_df, output_path="output/employee_report.parquet"):
    """Guarda el informe en formato Parquet."""
    report_df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Reporte guardado en: {output_path}")

if __name__ == "__main__":
    # Configuración de Spark
    spark = SparkSession.builder \
        .appName("EmployeeReportGenerator") \
        .getOrCreate()

    try:
        # 1. Cargar datos limpios
        clean_data = load_clean_data(spark)

        # 2. Generar informe
        report = generate_employee_report(clean_data)

        # 3. Mostrar ejemplo (opcional)
        print("\nMuestra del informe generado:")
        report.show(5, truncate=False)

        # 4. Guardar informe
        save_report(report)

    finally:
        spark.stop()