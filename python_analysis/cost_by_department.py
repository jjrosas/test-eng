from src.data_loader import create_spark_session
from pyspark.sql import functions as F

def load_clean_data(spark):
    """Carga los datos procesados desde archivos Parquet"""
    return {
        "salaries": spark.read.parquet("data/clean/salaries"),
        "dept_emp": spark.read.parquet("data/clean/dept_emp"),
        "departments": spark.read.parquet("data/clean/departments")
    }

def calculate_annual_costs():
    """Calcula y muestra los costos anuales por departamento"""
    spark = create_spark_session("CostAnalysis")
    
    try:
        # 1. Cargar datos limpios
        print("Cargando datos procesados...")
        clean_data = load_clean_data(spark)
        
        # 2. Obtener últimos salarios
        latest_salaries = clean_data["salaries"] \
            .orderBy("emp_no", F.desc("to_date")) \
            .dropDuplicates(["emp_no"])
        
        # 3. Filtrar asignaciones de departamento vigentes
        current_dept_emp = clean_data["dept_emp"] \
            .filter(F.col("to_date") >= F.current_date())
        
        # 4. Calcular costos
        annual_costs = latest_salaries \
            .join(current_dept_emp, "emp_no") \
            .join(clean_data["departments"], "dept_no") \
            .groupBy("dept_name") \
            .agg(F.sum("salary").alias("costo_anual")) \
            .orderBy(F.desc("costo_anual"))
        
        # 5. Mostrar y guardar resultados
        print("Costo anual por departamento:")
        annual_costs.show(truncate=False)
        
        # Guardar resultados (opcional)
        output_path = "output/results/annual_costs"
        annual_costs.write.mode("overwrite").parquet(output_path)
        print(f" Resultados guardados en: {output_path}")
        
        return annual_costs
        
    finally:
        spark.stop()

if __name__ == "__main__":
    calculate_annual_costs()