from pyspark.sql import SparkSession
import os

def show_sample(df, table_name, n=3):
    """Muestra una muestra de datos limpios"""
    print(f"\n📊 Muestra de {table_name} (limpio):")
    df.show(n, truncate=False)

def save_clean_data(data_dict, format="parquet", path="data/clean"):
    """Guarda todos los DataFrames limpios"""
    os.makedirs(path, exist_ok=True)
    for name, df in data_dict.items():
        df.write.mode("overwrite").format(format).save(f"{path}/{name}")
    print(f"\n💾 Datos guardados en {format} en {path}")