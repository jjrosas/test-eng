from pyspark.sql import functions as F
from pyspark.sql.functions import col, count as spark_count, when, isnull, isnan

class DataValidator:
    @staticmethod
    def validate_table(df, table_name):
        """Ejecuta todas las validaciones para una tabla"""
        print(f"\n🔍 Validando {table_name.upper()}")
        
        # Validación de nulos
        null_report = DataValidator.check_nulls(df, table_name)
        
        # Validación de consistencia de fechas
        if {"from_date", "to_date"}.issubset(df.columns):
            DataValidator.check_date_consistency(df)
        
        # Validaciones específicas por tabla
        if table_name == "employees":
            DataValidator.check_gender_values(df)
            DataValidator.check_date_ranges(df, "birth_date", "1900-01-01", "2023-12-31")
        elif table_name == "salaries":
            DataValidator.check_value_range(df, "salary", 1000, 1000000)
        elif table_name in ["dept_emp", "dept_manager"]:
            DataValidator.check_date_consistency(df, "from_date", "to_date")
        
        return null_report

    @staticmethod
    def check_nulls(df, table_name):
        """Genera reporte de valores nulos/NaN por columna"""
        schema = df.schema
        null_exprs = []
        
        for field in schema:
            col_name = field.name
            col_type = field.dataType.typeName()
            
            if col_type in ["date", "timestamp"]:
                null_expr = spark_count(when(isnull(col(col_name)), col_name))
            elif col_type in ["double", "float"]:
                null_expr = spark_count(when(isnull(col(col_name)) | isnan(col(col_name)), col_name))
            else:
                null_expr = spark_count(when(isnull(col(col_name)), col_name))
            
            null_exprs.append(null_expr.alias(col_name))
        
        null_report = df.select(*null_exprs).collect()[0]
        report = {col_name: null_report[col_name] for col_name in df.columns}
        
        print(f"\n🔍 Reporte de nulos para {table_name}:")
        for col_name, cnt in report.items():
            print(f"- {col_name}: {cnt} nulos")
        
        return report

    @staticmethod
    def check_gender_values(df):
        """Valida que gender solo contenga M/F"""
        invalid_genders = df.filter(~F.col("gender").isin("M", "F"))
        if invalid_genders.count() > 0:
            print(f"⚠️ {invalid_genders.count()} registros con valores inválidos en 'gender':")
            invalid_genders.select("gender").distinct().show()

    @staticmethod
    def check_value_range(df, column, min_val, max_val):
        """Verifica que los valores estén en un rango"""
        out_of_range = df.filter(~F.col(column).between(min_val, max_val))
        if out_of_range.count() > 0:
            print(f"⚠️ {out_of_range.count()} registros fuera de rango en '{column}' ({min_val}-{max_val}):")
            out_of_range.select(column).summary().show()

    @staticmethod
    def check_date_ranges(df, date_col, min_date, max_date):
        """Valida que las fechas estén en un rango"""
        invalid_dates = df.filter(
            (F.col(date_col) < F.lit(min_date)) | 
            (F.col(date_col) > F.lit(max_date))
        )
        if invalid_dates.count() > 0:
            print(f"⚠️ {invalid_dates.count()} fechas inválidas en '{date_col}' (fuera de {min_date} a {max_date}):")
            invalid_dates.select(date_col).show(5, truncate=False)

    @staticmethod
    def check_date_consistency(df, date_from_col="from_date", date_to_col="to_date"):
        """Valida que date_to >= date_from"""
        invalid_dates = df.filter(
            (F.col(date_to_col).isNotNull() & 
             F.col(date_from_col).isNotNull()) & 
            (F.col(date_to_col) < F.col(date_from_col))
        )
        
        error_count = invalid_dates.count()
        if error_count > 0:
            print(f"⚠️ ERROR: {error_count} registros con {date_to_col} < {date_from_col}")
            print("Ejemplos de registros inválidos:")
            invalid_dates.select(date_from_col, date_to_col).show(5, truncate=False)
            return False
        return True