import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Configurar paths
sys.path.append(str(Path(__file__).resolve().parent.parent))

def load_clean_data(spark):
    """Carga todos los datos limpios necesarios"""
    return {
        "employees": spark.read.parquet("data/clean/employees"),
        "dept_emp": spark.read.parquet("data/clean/dept_emp"),
        "dept_manager": spark.read.parquet("data/clean/dept_manager"),
        "salaries": spark.read.parquet("data/clean/salaries"),
        "departments": spark.read.parquet("data/clean/departments"),
        "titles": spark.read.parquet("data/clean/titles")
    }

def detect_employee_changes(spark, start_date, end_date):
    """Detecta todos los cambios relevantes en el período especificado"""
    clean_data = load_clean_data(spark)
    
    # Definir WindowSpec para analizar cambios secuenciales
    windowSpec = Window.partitionBy("emp_no").orderBy("from_date")
    
    # 1. Cambios de departamento CORREGIDOS
    dept_changes = (
        clean_data["dept_emp"]
        .withColumn("prev_dept", F.lag("dept_no").over(windowSpec))
        .withColumn("prev_to_date", F.lag("to_date").over(windowSpec))
        .filter(
            (F.col("from_date") >= start_date) & 
            (F.col("from_date") <= end_date) &
            (F.col("dept_no") != F.col("prev_dept")) &
            (F.col("prev_dept").isNotNull())
        )
        .join(clean_data["employees"], "emp_no")
        .join(clean_data["departments"], "dept_no")
        .join(
            clean_data["departments"]
            .withColumnRenamed("dept_no", "prev_dept_no")
            .withColumnRenamed("dept_name", "prev_dept_name"), 
            F.col("prev_dept") == F.col("prev_dept_no")
        )
        .select(
            "emp_no",
            F.concat_ws(" ", "first_name", "last_name").alias("employee_name"),
            "dept_name",
            F.col("from_date").alias("change_date"),
            F.lit("department_change").alias("change_type"),
            F.col("prev_dept_name").alias("old_value"),
            F.col("dept_name").alias("new_value")
        )
    )
    
    # 2. Cambios de salario
    salary_changes = (
        clean_data["salaries"]
        .filter((F.col("from_date") >= start_date) & (F.col("from_date") <= end_date))
        .join(clean_data["employees"], "emp_no")
        .groupBy("emp_no", "first_name", "last_name")
        .agg(
            F.first("salary").alias("old_salary"),
            F.last("salary").alias("new_salary"),
            F.first("from_date").alias("change_date")
        )
        .filter(F.col("old_salary") != F.col("new_salary"))
        .select(
            "emp_no",
            F.concat_ws(" ", "first_name", "last_name").alias("employee_name"),
            F.lit("N/A").alias("dept_name"),
            "change_date",
            F.lit("salary_change").alias("change_type"),
            F.col("old_salary").cast("string"),
            F.col("new_salary").cast("string")
        )
    )
    
    # 3. Cambios de manager en departamentos
    manager_changes = (
        clean_data["dept_manager"]
        .filter((F.col("from_date") >= start_date) & (F.col("from_date") <= end_date))
        .join(clean_data["employees"], "emp_no")
        .join(clean_data["departments"], "dept_no")
        .select(
            F.lit("N/A").alias("emp_no"),
            F.concat_ws(" ", "first_name", "last_name").alias("employee_name"),
            "dept_name",
            F.col("from_date").alias("change_date"),
            F.lit("manager_change").alias("change_type"),
            F.lit("N/A").alias("old_value"),
            F.lit("N/A").alias("new_value")
        )
    )
    
    # 4. Cambios de puesto/título
    title_changes = (
        clean_data["titles"]
        .filter((F.col("from_date") >= start_date) & (F.col("from_date") <= end_date))
        .join(clean_data["employees"], "emp_no")
        .select(
            "emp_no",
            F.concat_ws(" ", "first_name", "last_name").alias("employee_name"),
            F.lit("N/A").alias("dept_name"),
            F.col("from_date").alias("change_date"),
            F.lit("title_change").alias("change_type"),
            F.lit("N/A").alias("old_value"),
            F.col("title").alias("new_value")
        )
    )
    
    # Unir todos los cambios
    all_changes = (
        dept_changes
        .union(salary_changes)
        .union(manager_changes)
        .union(title_changes)
        .orderBy("change_date", "emp_no")
    )
    
    return all_changes

def generate_employee_changes_report(start_date, end_date, output_path):
    """Genera el informe completo de cambios"""
    from src.data_loader import create_spark_session
    
    spark = create_spark_session("EmployeeChangesReport")
    
    try:
        print(f"Generando reporte de cambios entre {start_date} y {end_date}...")
        
        changes_df = detect_employee_changes(spark, start_date, end_date)
        
        # Mostrar resumen
        print("\nResumen de cambios detectados:")
        changes_df.groupBy("change_type").count().show()
        
        # Guardar como CSV
        changes_df.write.mode("overwrite").csv(
            path=output_path,
            header=True,
            sep=",",
            dateFormat="yyyy-MM-dd"
        )
        
        print(f"\n✅ Reporte generado exitosamente en: {output_path}")
        
        return changes_df
    finally:
        spark.stop()

if __name__ == "__main__":
    # Ejemplo de uso - ajustar fechas según necesidad
    start_date = "1990-01-01"
    end_date = "1999-12-31"
    output_path = "output/employee_changes"
    
    generate_employee_changes_report(start_date, end_date, output_path)