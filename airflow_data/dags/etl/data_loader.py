from pyspark.sql import SparkSession
from config.settings import DB_CONFIG, JAR_PATH, TABLES

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "libs/mysql-connector-j-9.2.0.jar") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def load_table(spark, table_name):
    """Carga una tabla específica desde MySQL"""
    return spark.read \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", table_name) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .load()

def load_all_tables(spark):
    """Carga todas las tablas definidas en settings"""
    return {table: load_table(spark, table) for table in TABLES}