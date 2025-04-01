import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
JAR_PATH = str(BASE_DIR / "libs/mysql-connector-j-9.2.0.jar")

DB_CONFIG = {
    "url": "jdbc:mysql://localhost:3306/employees",
    "driver": "com.mysql.cj.jdbc.Driver",  # ¡Clave correcta!
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "")
}

# Tablas a procesar
TABLES = [
    "employees",
    "departments",
    "dept_emp",
    "dept_manager",
    "salaries",
    "titles"
]