from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StringType

class DataCleaner:
    @staticmethod
    def _handle_date_placeholders(df, date_cols):
        """Reemplaza fechas placeholder (9999-01-01) con fecha actual"""
        for col_name in date_cols:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) == F.lit("9999-01-01"), F.current_date())
                 .otherwise(F.col(col_name))
            )
        return df

    @staticmethod
    def clean_employees(df):
        """Limpieza para la tabla employees"""
        return (
            df
            # Conversión de tipos
            .withColumn("birth_date", F.to_date("birth_date"))
            .withColumn("hire_date", F.to_date("hire_date"))
            # Manejo de nulos
            .fillna({
                "first_name": "UNKNOWN",
                "last_name": "UNKNOWN",
                "gender": "U"
            })
            # Eliminar duplicados
            .dropDuplicates(["emp_no"])
            # Validación de género
            .withColumn("gender", 
                F.when(F.col("gender").isin("M", "F"), F.col("gender"))
                 .otherwise("U"))
        )

    @staticmethod
    def clean_departments(df):
        """Limpieza para la tabla departments"""
        return (
            df
            # Eliminar espacios en nombres
            .withColumn("dept_name", F.trim("dept_name"))
            # Manejo de nulos
            .fillna({"dept_name": "DEPARTMENT_UNKNOWN"})
            # Eliminar duplicados
            .dropDuplicates(["dept_no"])
        )

    @staticmethod
    def clean_dept_emp(df):
        """Limpieza para la tabla dept_emp"""
        df = (
            df
            # Conversión de fechas
            .withColumn("from_date", F.to_date("from_date"))
            .withColumn("to_date", F.to_date("to_date"))
            # Validación de fechas
            .filter(F.col("from_date") <= F.col("to_date"))
            # Eliminar duplicados
            .dropDuplicates(["emp_no", "dept_no"])
        )
        return DataCleaner._handle_date_placeholders(df, ["to_date"])

    @staticmethod
    def clean_dept_manager(df):
        """Limpieza para la tabla dept_manager"""
        df = (
            df
            # Conversión de fechas
            .withColumn("from_date", F.to_date("from_date"))
            .withColumn("to_date", F.to_date("to_date"))
            # Validación de fechas
            .filter(F.col("from_date") <= F.col("to_date"))
            # Eliminar duplicados
            .dropDuplicates(["emp_no", "dept_no"])
        )
        return DataCleaner._handle_date_placeholders(df, ["to_date"])

    @staticmethod
    def clean_salaries(df):
        """Limpieza para la tabla salaries"""
        df = (
            df
            # Conversión de tipos
            .withColumn("salary", F.col("salary").cast(DecimalType(10, 2)))
            .withColumn("from_date", F.to_date("from_date"))
            .withColumn("to_date", F.to_date("to_date"))
            # Validación de rangos
            .filter(F.col("salary").between(1000, 1000000))
            # Validación de fechas
            .filter(F.col("from_date") <= F.col("to_date"))
            # Eliminar duplicados
            .dropDuplicates(["emp_no", "from_date"])
        )
        return DataCleaner._handle_date_placeholders(df, ["to_date"])

    @staticmethod
    def clean_titles(df):
        """Limpieza para la tabla titles"""
        df = (
            df
            # Normalización de texto
            .withColumn("title", F.initcap(F.trim("title")))
            # Conversión de fechas
            .withColumn("from_date", F.to_date("from_date"))
            .withColumn("to_date", F.to_date("to_date"))
            # Validación de fechas
            .filter(F.col("from_date") <= F.col("to_date"))
            # Eliminar duplicados
            .dropDuplicates(["emp_no", "title", "from_date"])
            # Manejo de nulos
            .fillna({"title": "TITLE_UNKNOWN"})
        )
        return DataCleaner._handle_date_placeholders(df, ["to_date"])

    @staticmethod
    def clean_all_tables(raw_data):
        """Aplica limpieza a todas las tablas"""
        return {
            "employees": DataCleaner.clean_employees(raw_data["employees"]),
            "departments": DataCleaner.clean_departments(raw_data["departments"]),
            "dept_emp": DataCleaner.clean_dept_emp(raw_data["dept_emp"]),
            "dept_manager": DataCleaner.clean_dept_manager(raw_data["dept_manager"]),
            "salaries": DataCleaner.clean_salaries(raw_data["salaries"]),
            "titles": DataCleaner.clean_titles(raw_data["titles"])
        }