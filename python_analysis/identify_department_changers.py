import sys
from pathlib import Path
from pyspark.sql import functions as F

# Configurar el path para importar src
project_root = str(Path(__file__).resolve().parent.parent)  # Ajusta según tu estructura
sys.path.append(project_root)

def load_clean_data(spark):
    """Carga los datos procesados desde archivos Parquet"""
    return {
        "dept_emp": spark.read.parquet("data/clean/dept_emp"),
        "employees": spark.read.parquet("data/clean/employees"),
        "departments": spark.read.parquet("data/clean/departments")
    }

def identify_department_changers():
    """Identifica empleados con más de 2 cambios de departamento usando datos limpios"""
    from src.data_loader import create_spark_session
    spark = create_spark_session("DepartmentChangesAnalysis")
    
    try:
        # 1. Cargar datos limpios
        print("Cargando datos procesados...")
        clean_data = load_clean_data(spark)
        
        # 2. Contar cambios de departamento por empleado
        department_changes = (
            clean_data["dept_emp"]
            .orderBy("emp_no", "from_date")
            .groupBy("emp_no")
            .agg(
                F.count("*").alias("total_cambios"),
                F.collect_list("dept_no").alias("departamentos"),
                F.collect_list("from_date").alias("fechas_cambio")
            )
            .filter(F.col("total_cambios") >= 2)  
            .orderBy(F.desc("total_cambios"))
        )

        # 3. Enriquecer con información de empleados
        result = (
            department_changes
            .join(clean_data["employees"], "emp_no")
            .select(
                "emp_no",
                "first_name",
                "last_name",
                "total_cambios",
                "departamentos",
                "fechas_cambio"
            )
        )

        # 4. Mostrar resultados
        print("Empleados con más de 2 cambios de departamento:")
        result.show(truncate=False, n=50)

        # 5. Guardar resultados
        output_path = "output/empleados_cambios_departamento"
        result.write.mode("overwrite").parquet(output_path)
        print(f"\n💾 Resultados guardados en: {output_path}")

        return result
        
    finally:
        spark.stop()

if __name__ == "__main__":
    identify_department_changers()