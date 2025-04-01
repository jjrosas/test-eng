from src.data_loader import create_spark_session, load_all_tables
from src.data_cleaner import DataCleaner
from src.data_validator import DataValidator
from src.utils import show_sample, save_clean_data

def run_etl():
    spark = create_spark_session("EmployeesETL")
    
    try:
        # 1. Extracción
        print(" Extrayendo datos...")
        raw_data = load_all_tables(spark)
        
        # 2. Transformación
        print(" Transformando datos...")
        clean_data = DataCleaner.clean_all_tables(raw_data)
        
        # 3. Validación
        print("Validando resultados...")
        for table_name, df in clean_data.items():
            DataValidator.validate_table(df, table_name)
            show_sample(df, table_name)
        
        # 4. Guardado (opcional)
        save_clean_data(clean_data, format="parquet")
        
        print("ETL completado exitosamente!")
        return clean_data
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()